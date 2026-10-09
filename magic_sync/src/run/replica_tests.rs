//! A replica as a host runs it: a message received is committed and the
//! server hears of it without a line of loop in the test; what the uplink
//! refuses goes again.

use super::*;
use crate::envelope::{self, Body, Down, Key as WireKey, Table, Up};
use crate::testkit::{from_wire, to_wire, Item, ItemRow};
use prost::Message as _;
use std::sync::{Arc, Mutex};

const TABLE: &str = "CREATE TABLE items (
    id BLOB PRIMARY KEY NOT NULL, partition_id BLOB NOT NULL, name TEXT NOT NULL, retired_at TEXT)";

/// What went up: who sent it, on which lane.
type Sent = Vec<(Uuid, &'static str, Up)>;

/// What went up, and whether the uplink is down.
#[derive(Clone, Default)]
struct Wire {
    sent: Arc<Mutex<Sent>>,
    down: Arc<Mutex<bool>>,
}

impl Uplink for Wire {
    async fn send(&self, replica: Uuid, lane: &'static str, message: Vec<u8>) -> Result<(), Error> {
        if *self.down.lock().unwrap() {
            return Err(Error::Transport {
                reason: "the network".into(),
                transient: true,
            });
        }
        let up = Up::decode(message.as_slice()).expect("an upload");
        self.sent.lock().unwrap().push((replica, lane, up));
        Ok(())
    }
}

impl Wire {
    fn acknowledged(&self) -> Vec<u64> {
        self.sent
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, _, up)| up.sequence > 0)
            .map(|(_, _, up)| up.sequence)
            .collect()
    }
}

async fn pool(pool: SqlitePool) -> SqlitePool {
    sqlx::raw_sql(crate::SQLITE_SCHEMA)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::raw_sql(TABLE).execute(&pool).await.unwrap();
    pool
}

fn codecs() -> &'static [Codec<()>] {
    Box::leak(Box::new([Codec::new::<Item, ItemRow>(
        to_wire, from_wire, None,
    )]))
}

fn rows(sequence: u64, name: &str) -> Vec<u8> {
    let id = Uuid::now_v7();
    Body::Down(Down {
        tables: vec![Table {
            table: "items".to_owned(),
            upserts: vec![ItemRow {
                id: id.to_string(),
                partition_id: Uuid::nil().to_string(),
                name: name.to_owned(),
            }
            .encode_to_vec()],
            keys: vec![WireKey {
                parts: vec![id.to_string()],
            }],
            ..Table::default()
        }],
        sequence,
        epoch: "e".to_owned(),
        ..Down::default()
    })
    .encoded()
}

fn settings() -> Settings {
    Settings {
        idle: Duration::from_millis(10),
        backoff: (Duration::from_millis(10), Duration::from_millis(20)),
        ..Settings::default()
    }
}

/// Runs `replica` until `done` holds, or fails after five seconds.
async fn until<I: Identity, U: Uplink>(replica: &Replica<(), I, U>, done: impl Fn() -> bool) {
    let (stop, mut stopped) = tokio::sync::watch::channel(false);
    let run = replica.run(async move {
        let _ = stopped.wait_for(|stop| *stop).await;
    });
    let check = async {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !done() {
            assert!(Instant::now() < deadline, "not done within five seconds");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        stop.send(true).unwrap();
    };
    tokio::join!(run, check);
}

#[sqlx::test(migrations = false)]
async fn what_the_replica_applied_reaches_the_server_even_through_an_outage(pool_: SqlitePool) {
    let pool = pool(pool_).await;
    let me = Uuid::now_v7();
    let wire = Wire::default();
    let replica = Replica::new(pool.clone(), codecs(), me, wire.clone(), settings());

    let received = replica.receive(&rows(4, "four")).await.unwrap();
    assert!(matches!(received, Received::Applied(_)));
    let names: Vec<String> = sqlx::query_scalar("SELECT name FROM items")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(names, ["four"], "committed before it returns");

    *wire.down.lock().unwrap() = true;
    replica.receive(&rows(5, "five")).await.unwrap();
    let outage = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        *wire.down.lock().unwrap() = false;
    };
    let (_, ()) = tokio::join!(outage, until(&replica, || wire.acknowledged() == [5]));
    assert!(wire
        .sent
        .lock()
        .unwrap()
        .iter()
        .all(|(r, lane, up)| *r == me && *lane == "main" && up.protocol == envelope::PROTOCOL));
}

/// A replica that does not know which one it is yet sends nothing up.
#[sqlx::test(migrations = false)]
async fn nothing_goes_up_until_the_replica_knows_who_it_is(pool_: SqlitePool) {
    struct Nobody;
    impl Identity for Nobody {
        async fn id(&self, _conn: &mut Conn) -> Result<Option<Uuid>, Error> {
            Ok(None)
        }
    }
    let pool = pool(pool_).await;
    let wire = Wire::default();
    let replica = Replica::new(pool, codecs(), Nobody, wire.clone(), settings());
    replica.receive(&rows(1, "one")).await.unwrap();
    assert_eq!(replica.send_up(true).await.unwrap(), 0);
    assert!(wire.sent.lock().unwrap().is_empty());
}
