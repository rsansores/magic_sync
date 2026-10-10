//! The server as a host runs it: written rows reach the replicas without a
//! line of loop in the test, uploads are filed and answered, one process at a
//! time runs the relay, and a restart lets through what was set aside.

use super::*;
use crate::envelope::{self, Body, Table as WireTable, ToReplica};
use crate::testkit::records::{reading_from_wire, reading_to_wire, Reading, ReadingRow};
use crate::testkit::{from_wire, to_wire, Item, ItemRow, BUGGY};
use prost::Message as _;
use std::sync::{Arc, Mutex};

const SCHEMA: &str = "
CREATE SCHEMA fixture;
CREATE TABLE fixture.t_replicas (id UUID PRIMARY KEY, partition_id UUID NOT NULL, live BOOLEAN NOT NULL);
CREATE TABLE fixture.items (
    id UUID PRIMARY KEY, partition_id UUID NOT NULL, name TEXT NOT NULL, retired_at TIMESTAMPTZ);
CREATE VIEW items AS SELECT * FROM fixture.items WHERE retired_at IS NULL;
SELECT sync.mirror_table('fixture', 'items', '{}', 'partition_id');
CREATE TABLE fixture.readings (id UUID PRIMARY KEY, replica_id UUID NOT NULL, value TEXT NOT NULL);
";

const REPLICAS: Replicas = Replicas {
    relation: "fixture.t_replicas",
    table: "t_replicas",
    live: "live",
    partition: Some("partition_id"),
};

/// What every replica was sent, in order.
#[derive(Clone, Default)]
struct Wire(Arc<Mutex<Vec<(Uuid, Body)>>>);

impl Transport for Wire {
    async fn send(&self, replica: Uuid, message: Vec<u8>, _id: &str) -> Result<(), Error> {
        let message = ToReplica::decode(message.as_slice()).expect("a message");
        self.0
            .lock()
            .unwrap()
            .push((replica, message.body.expect("a body")));
        Ok(())
    }
}

impl Wire {
    /// The item names `replica` was sent, as often as it was sent each.
    fn items(&self, replica: Uuid) -> Vec<String> {
        let mut names = Vec::new();
        for (to, body) in self.0.lock().unwrap().iter() {
            let Body::Down(down) = body else { continue };
            if *to != replica {
                continue;
            }
            for table in &down.tables {
                for bytes in &table.upserts {
                    names.push(ItemRow::decode(bytes.as_slice()).expect("an item").name);
                }
            }
        }
        names
    }

    fn filed(&self, replica: Uuid) -> Vec<u64> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(to, _)| *to == replica)
            .filter_map(|(_, body)| match body {
                Body::Filed(filed) => Some(filed.uploads.clone()),
                _ => None,
            })
            .flatten()
            .collect()
    }
}

fn codecs() -> &'static [Codec<()>] {
    Box::leak(Box::new([
        Codec::new::<Item, ItemRow>(to_wire, from_wire, None),
        Codec::new::<Reading, ReadingRow>(reading_to_wire, reading_from_wire, None),
    ]))
}

fn settings() -> Settings {
    Settings {
        idle: Duration::from_millis(10),
        standby: Duration::from_millis(10),
        ..Settings::default()
    }
}

/// The schema, a partition and two live replicas in it.
async fn world(pool: &PgPool) -> (Uuid, [Uuid; 2]) {
    let mut conn = pool.acquire().await.unwrap();
    crate::plan::tests::install(&mut conn).await;
    sqlx::raw_sql(SCHEMA).execute(&mut *conn).await.unwrap();
    let partition = Uuid::now_v7();
    let replicas = [Uuid::now_v7(), Uuid::now_v7()];
    for replica in replicas {
        sqlx::query("INSERT INTO fixture.t_replicas VALUES ($1, $2, true)")
            .bind(replica)
            .bind(partition)
            .execute(&mut *conn)
            .await
            .unwrap();
    }
    (partition, replicas)
}

async fn item(pool: &PgPool, partition: Uuid, name: &str) {
    sqlx::query("INSERT INTO fixture.items (id, partition_id, name) VALUES ($1, $2, $3)")
        .bind(Uuid::now_v7())
        .bind(partition)
        .bind(name)
        .execute(pool)
        .await
        .unwrap();
}

/// Runs `servers` side by side until `done` holds, or fails after five
/// seconds.
async fn until<S: Senders, T: Transport + Send + 'static, F: Future<Output = bool>>(
    servers: &[&Server<S, T>],
    done: impl Fn() -> F,
) {
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let stopping = || {
        let mut stopped = stopped.clone();
        async move {
            let _ = stopped.wait_for(|stop| *stop).await;
        }
    };
    let runs = async {
        match servers {
            [one] => one.run(stopping()).await,
            [one, two] => {
                tokio::join!(one.run(stopping()), two.run(stopping()));
            }
            _ => unreachable!("one or two servers"),
        }
    };
    let check = async {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !done().await {
            assert!(Instant::now() < deadline, "not done within five seconds");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        stop.send(true).unwrap();
    };
    tokio::join!(runs, check);
}

#[sqlx::test(migrations = false)]
async fn what_is_written_reaches_the_replicas_and_what_comes_up_is_filed_and_answered(
    pool: PgPool,
) {
    let (partition, [a, b]) = world(&pool).await;
    let wire = Wire::default();
    let server = Server::new(
        pool.clone(),
        codecs(),
        REPLICAS,
        wire.clone(),
        Anyone(pool.clone()),
        settings(),
    )
    .await
    .expect("the server");

    item(&pool, partition, "one").await;
    until(&[&server], || async {
        wire.items(a) == ["one"] && wire.items(b) == ["one"]
    })
    .await;

    let upload = Up {
        replica_id: a.to_string(),
        tables: vec![WireTable {
            table: "readings".to_owned(),
            upserts: vec![ReadingRow {
                id: Uuid::now_v7().to_string(),
                replica_id: a.to_string(),
                value: "seven".to_owned(),
            }
            .encode_to_vec()],
            ..WireTable::default()
        }],
        uploads: vec![3],
        protocol: envelope::PROTOCOL,
        ..Up::default()
    };
    let filed = server.receive(a, &upload.encode_to_vec()).await.unwrap();
    assert_eq!(filed.len(), 1);
    assert_eq!(wire.filed(a), vec![3], "answered once filed");
    let stored: i64 = sqlx::query_scalar("SELECT count(*) FROM fixture.readings")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(stored, 1);

    let later = Up {
        protocol: envelope::PROTOCOL + 1,
        ..upload
    };
    let refused = server.receive(a, &later.encode_to_vec()).await;
    assert!(
        matches!(refused, Err(Error::Protocol { .. })),
        "a later protocol comes again once this build speaks it"
    );
}

/// Two processes run the server; one of them runs the relay, so nothing is
/// planned or sent twice.
#[sqlx::test(migrations = false)]
async fn one_process_at_a_time_runs_the_relay(pool: PgPool) {
    let (partition, [a, b]) = world(&pool).await;
    let wire = Wire::default();
    let mut servers = Vec::new();
    for _ in 0..2 {
        servers.push(
            Server::new(
                pool.clone(),
                codecs(),
                REPLICAS,
                wire.clone(),
                Anyone(pool.clone()),
                settings(),
            )
            .await
            .unwrap(),
        );
    }
    for n in 0..20 {
        item(&pool, partition, &format!("item {n}")).await;
    }
    until(&[&servers[0], &servers[1]], || async {
        wire.items(a).len() >= 20 && wire.items(b).len() >= 20
    })
    .await;
    assert_eq!(wire.items(a).len(), 20, "each sent once");
    assert_eq!(wire.items(b).len(), 20, "each sent once");
}

/// A row the build cannot encode waits in quarantine; the next process to run
/// the relay — after a deploy — sends it, with nobody touching the data.
#[sqlx::test(migrations = false)]
async fn what_was_set_aside_goes_once_a_fixed_build_runs_the_relay(pool: PgPool) {
    let (partition, [a, _]) = world(&pool).await;
    let wire = Wire::default();
    let server = || {
        Server::new(
            pool.clone(),
            codecs(),
            REPLICAS,
            wire.clone(),
            Anyone(pool.clone()),
            settings(),
        )
    };
    item(&pool, partition, "fine").await;
    item(&pool, partition, "poison").await;
    let quarantined = || async {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM sync.quarantine")
            .fetch_one(&pool)
            .await
            .unwrap()
    };

    let before = server().await.unwrap();
    until(&[&before], || async { quarantined().await == 2 }).await;
    assert_eq!(wire.items(a), ["fine"]);
    drop(before);

    BUGGY.set(false);
    let fixed = server().await.unwrap();
    // Sent and recorded, to both replicas.
    let held = || async {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM sync.held")
            .fetch_one(&pool)
            .await
            .unwrap()
    };
    until(&[&fixed], || async { held().await == 4 }).await;
    BUGGY.set(true);
    assert_eq!(wire.items(a), ["fine", "poison"]);
    // Out of the quarantine once each replica says it took it.
    let (epoch, sent): (Uuid, Vec<(Uuid, i64)>) = (
        sqlx::query_scalar("SELECT epoch FROM sync.epoch")
            .fetch_one(&pool)
            .await
            .unwrap(),
        sqlx::query_as("SELECT replica_id, last_sequence FROM sync.replicas")
            .fetch_all(&pool)
            .await
            .unwrap(),
    );
    for (replica, last) in sent {
        let ack = crate::envelope::Up {
            replica_id: replica.to_string(),
            epoch: epoch.to_string(),
            sequence: last as u64,
            ..crate::envelope::Up::default()
        };
        fixed.receive(replica, &ack.encode_to_vec()).await.unwrap();
    }
    assert_eq!(quarantined().await, 0);
}

/// A relay whose lock's connection is gone cannot record what it sent: the
/// record runs on that connection, so it dies with the lock, and the notes
/// wait for whoever runs the relay next.
#[sqlx::test(migrations = false)]
async fn a_relay_that_lost_its_lock_records_nothing(pool: PgPool) {
    let (partition, _) = world(&pool).await;
    let server = Server::new(
        pool.clone(),
        codecs(),
        REPLICAS,
        Wire::default(),
        Anyone(pool.clone()),
        settings(),
    )
    .await
    .unwrap();
    item(&pool, partition, "one").await;
    let mut lock = Lock::take(&pool, server.settings.lock_key)
        .await
        .unwrap()
        .expect("the lock");
    sqlx::query(
        "SELECT pg_terminate_backend(pid) FROM pg_locks \
          WHERE locktype = 'advisory' \
            AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
    )
    .execute(&pool)
    .await
    .unwrap();

    assert!(server.pass(&mut lock).await.is_err());
    let count = |sql: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>(sql)
                .fetch_one(&pool)
                .await
                .unwrap()
        }
    };
    assert_eq!(count("SELECT count(*) FROM sync.held").await, 0);
    assert!(count("SELECT count(*) FROM sync.notes WHERE done_at IS NULL").await > 0);
    assert_eq!(
        server.pass_now().await.unwrap(),
        Some(1),
        "the next relay does it"
    );
}

/// A database with none of the engine gets its schema and every mirrored
/// table's triggers; rows written before then reach every replica, by a
/// provisioning.
#[sqlx::test(migrations = false)]
async fn install_sets_up_a_database_that_has_none_of_the_engine(pool: PgPool) {
    sqlx::raw_sql("DROP SCHEMA IF EXISTS sync CASCADE")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::raw_sql(
        "CREATE SCHEMA fixture;
         CREATE TABLE fixture.t_replicas (id UUID PRIMARY KEY, partition_id UUID NOT NULL, live BOOLEAN NOT NULL);
         CREATE TABLE fixture.items (
             id UUID PRIMARY KEY, partition_id UUID NOT NULL, name TEXT NOT NULL, retired_at TIMESTAMPTZ);
         CREATE VIEW items AS SELECT * FROM fixture.items WHERE retired_at IS NULL;
         CREATE TABLE fixture.readings (id UUID PRIMARY KEY, replica_id UUID NOT NULL, value TEXT NOT NULL);",
    )
    .execute(&pool)
    .await
    .unwrap();
    let partition = Uuid::now_v7();
    let replica = Uuid::now_v7();
    sqlx::query("INSERT INTO fixture.t_replicas VALUES ($1, $2, true)")
        .bind(replica)
        .bind(partition)
        .execute(&pool)
        .await
        .unwrap();
    item(&pool, partition, "before").await;
    let refused = Server::new(
        pool.clone(),
        codecs(),
        REPLICAS,
        Wire::default(),
        Anyone(pool.clone()),
        settings(),
    )
    .await;
    assert!(refused.is_err(), "no schema: refused");

    let wire = Wire::default();
    let server = Server::install(
        pool.clone(),
        codecs(),
        REPLICAS,
        wire.clone(),
        Anyone(pool.clone()),
        settings(),
    )
    .await
    .expect("installed");
    Server::install(
        pool.clone(),
        codecs(),
        REPLICAS,
        Wire::default(),
        Anyone(pool.clone()),
        settings(),
    )
    .await
    .expect("idempotent");
    item(&pool, partition, "after").await;

    until(&[&server], || async {
        let mut names = wire.items(replica);
        names.sort();
        names.dedup();
        names == ["after", "before"]
    })
    .await;
}
