//! A replica as a host runs it: a message received is committed and the
//! server hears of it without a line of loop in the test; what the uplink
//! refuses for a while goes again.

use super::*;
use crate::conformance::{codecs, Uplinked};
use crate::envelope::{self, Body, Down, Key as WireKey, Table};
use crate::testkit::ItemRow;
use prost::Message as _;

const TABLES: &str = "
CREATE TABLE items (id BLOB PRIMARY KEY NOT NULL, partition_id BLOB NOT NULL, name TEXT NOT NULL, retired_at TEXT);
CREATE TABLE shelves (aisle BLOB NOT NULL, slot INTEGER NOT NULL, label TEXT NOT NULL, revision INTEGER NOT NULL,
    PRIMARY KEY (aisle, slot));
CREATE TABLE prices (id BLOB PRIMARY KEY NOT NULL, cents INTEGER NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE tasks (id BLOB PRIMARY KEY NOT NULL, title TEXT NOT NULL, done INTEGER NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE readings (replica_id BLOB NOT NULL, id BLOB NOT NULL, value INTEGER NOT NULL, at INTEGER NOT NULL,
    PRIMARY KEY (replica_id, id));
CREATE TABLE events (id BLOB PRIMARY KEY NOT NULL, what TEXT NOT NULL);
";

async fn pool(pool: SqlitePool) -> SqlitePool {
    sqlx::raw_sql(TABLES).execute(&pool).await.unwrap();
    replica::install(&mut pool.acquire().await.unwrap(), &codecs())
        .await
        .unwrap();
    pool
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
        epoch: Uuid::nil().to_string(),
        ..Down::default()
    })
    .encoded()
}

fn settings() -> Settings {
    Settings {
        idle: Duration::from_millis(10),
        backoff: crate::run::Backoff::new(Duration::from_millis(10), Duration::from_millis(20)),
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

fn acknowledged(uplink: &Uplinked) -> Vec<u64> {
    uplink
        .sent
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, _, up)| up.sequence > 0)
        .map(|(_, _, up)| up.sequence)
        .collect()
}

#[sqlx::test(migrations = false)]
async fn what_the_replica_applied_reaches_the_server_even_through_an_outage(pool_: SqlitePool) {
    let pool = pool(pool_).await;
    let me = Uuid::now_v7();
    let uplink = Uplinked::default();
    let replica = Replica::new(pool.clone(), codecs(), me, uplink.clone(), settings())
        .await
        .unwrap();

    let received = replica.receive(&rows(4, "four")).await.unwrap();
    assert!(matches!(received, Received::Applied(_)));
    let names: Vec<String> = sqlx::query_scalar("SELECT name FROM items")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(names, ["four"], "committed before it returns");

    *uplink.refusing.lock().unwrap() = Some(true);
    replica.receive(&rows(5, "five")).await.unwrap();
    let outage = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        *uplink.refusing.lock().unwrap() = None;
    };
    let (_, ()) = tokio::join!(outage, until(&replica, || acknowledged(&uplink) == [5]));
    assert!(uplink
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
    let uplink = Uplinked::default();
    let replica = Replica::new(pool, codecs(), Nobody, uplink.clone(), settings())
        .await
        .unwrap();
    replica.receive(&rows(1, "one")).await.unwrap();
    assert!(!replica.send_lane(0, true).await.unwrap());
    assert!(uplink.sent.lock().unwrap().is_empty());
}

/// A replica refuses to start over a database it cannot run against.
#[sqlx::test(migrations = false)]
async fn a_replica_does_not_start_over_a_database_it_cannot_run_against(pool: SqlitePool) {
    sqlx::raw_sql(TABLES).execute(&pool).await.unwrap();
    let refused = Replica::new(pool, codecs(), Uuid::nil(), Uplinked::default(), settings()).await;
    assert!(refused.is_err());
}
