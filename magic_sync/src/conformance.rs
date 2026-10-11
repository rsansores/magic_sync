//! The tables the replica conformance suite runs on, as both sides hold
//! them — every key shape and version type a table may have, a table written
//! on both sides, and records — and the golden messages: what the server
//! really sends, captured on the postgres side into `tests/golden/` and
//! applied by the sqlite side's suite. The two sides never build together;
//! the golden file is what holds each to the other.

#[cfg(feature = "sqlite")]
use crate::row::Row;
use crate::testkit::{from_wire, to_wire, Item, ItemRow};
use crate::wire::Codec;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A catalog keyed by a uuid and an integer, versioned by an integer.
#[derive(sqlx::FromRow, Clone, Debug, PartialEq, Serialize, Deserialize, crate::Row)]
#[row(
    table = "shelves",
    schema = "fixture",
    mirror = "catalog",
    sync = "broadcast",
    key = "aisle, slot"
)]
pub struct Shelf {
    pub aisle: Uuid,
    pub slot: i64,
    pub label: String,
    #[row(version)]
    pub revision: i64,
}

/// A catalog versioned by an instant, with no retired column: a row a row
/// of the replica's own points at cannot be let go of.
#[derive(sqlx::FromRow, Clone, Debug, PartialEq, Serialize, Deserialize, crate::Row)]
#[row(
    table = "prices",
    schema = "fixture",
    mirror = "catalog",
    sync = "broadcast"
)]
pub struct Price {
    pub id: Uuid,
    pub cents: i64,
    #[row(version)]
    pub updated_at: DateTime<Utc>,
}

/// Written on both sides: the server's title, the replica's `done`. One the
/// server lets go of while the replica's edit is on its way is kept retired
/// until the edit is filed.
#[derive(sqlx::FromRow, Clone, Debug, PartialEq, Serialize, Deserialize, crate::Row)]
#[row(
    table = "tasks",
    schema = "fixture",
    mirror = "both",
    sync = "broadcast",
    retired = "retired_at"
)]
pub struct Task {
    pub id: Uuid,
    pub title: String,
    #[row(replica_owned)]
    pub done: bool,
    #[row(version)]
    pub updated_at: DateTime<Utc>,
}

/// State a replica reports: keyed by it, newest wins, editable.
#[derive(sqlx::FromRow, Clone, Debug, PartialEq, Serialize, Deserialize, crate::Row)]
#[row(
    table = "readings",
    schema = "fixture",
    mirror = "record",
    key = "replica_id, id",
    origin = "replica_id"
)]
pub struct Reading {
    pub replica_id: Uuid,
    pub id: Uuid,
    pub value: i64,
    #[row(version)]
    pub at: i64,
}

/// History a replica records: written once.
#[derive(sqlx::FromRow, Clone, Debug, PartialEq, Serialize, Deserialize, crate::Row)]
#[row(table = "events", schema = "fixture", mirror = "record")]
pub struct Event {
    pub id: Uuid,
    pub what: String,
}

/// Every table of the suite, parents first.
pub fn codecs() -> Vec<Codec<()>> {
    vec![
        Codec::new::<Item, ItemRow>(to_wire, from_wire, None),
        Codec::json::<Shelf>(None),
        Codec::json::<Price>(None),
        Codec::json::<Task>(None),
        Codec::json::<Reading>(None),
        Codec::json::<Event>(None),
    ]
}

/// A row of a JSON table, as the server sends it.
#[cfg(feature = "sqlite")]
pub fn json<R: Row + Serialize>(row: &R) -> Vec<u8> {
    let table = crate::row::Table::of::<R>();
    crate::wire::json_encode(R::TABLE, &table.columns, row).expect("a fixture row encodes")
}

/// Where the golden messages are kept.
pub const GOLDEN: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/to_replica.txt");

/// The golden messages, as `(step, bytes)`, in the order they were sent:
/// one line each, the step's name and the message in hex.
#[cfg(feature = "sqlite")]
pub fn golden(text: &str) -> Vec<(String, Vec<u8>)> {
    text.lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            let (step, hex) = line.split_once(' ').expect("a step and its bytes");
            let bytes = (0..hex.len())
                .step_by(2)
                .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).expect("hex"))
                .collect();
            (step.to_owned(), bytes)
        })
        .collect()
}

/// What went up: who sent it, on which lane.
#[cfg(feature = "sqlite")]
pub type Sent = (Uuid, &'static str, crate::envelope::Up);

/// An uplink that keeps what it is given, refusing — for a while or for
/// good — while told to.
#[cfg(feature = "sqlite")]
#[derive(Clone, Default)]
pub struct Uplinked {
    pub sent: std::sync::Arc<std::sync::Mutex<Vec<Sent>>>,
    /// `Some(transient)`: refused, as a network would or for good.
    pub refusing: std::sync::Arc<std::sync::Mutex<Option<bool>>>,
    /// The lanes it refuses; every lane when empty.
    pub refused_lanes: std::sync::Arc<std::sync::Mutex<Vec<&'static str>>>,
}

#[cfg(feature = "sqlite")]
impl crate::run::replica::Uplink for Uplinked {
    async fn send(
        &self,
        replica: Uuid,
        lane: &'static str,
        message: Vec<u8>,
    ) -> Result<(), crate::Error> {
        use prost::Message as _;
        let lanes = self.refused_lanes.lock().unwrap().clone();
        if let Some(transient) = *self.refusing.lock().unwrap() {
            if lanes.is_empty() || lanes.contains(&lane) {
                return Err(crate::Error::Transport {
                    reason: "refused".into(),
                    transient,
                });
            }
        }
        let up = crate::envelope::Up::decode(message.as_slice()).expect("an upload");
        self.sent.lock().unwrap().push((replica, lane, up));
        Ok(())
    }
}

#[cfg(feature = "sqlite")]
impl Uplinked {
    /// What went up since last asked.
    pub fn taken(&self) -> Vec<(&'static str, crate::envelope::Up)> {
        self.sent
            .lock()
            .unwrap()
            .drain(..)
            .map(|(_, lane, up)| (lane, up))
            .collect()
    }
}

/// The server's side of the golden messages: a server with every table of
/// the suite, one replica, and a pass of the real relay per step, each
/// message it sends that replica kept. What varies from run to run — the
/// installation's epoch, the snapshot ids a pass draws — is pinned, so the
/// same build sends the same bytes.
#[cfg(all(test, feature = "postgres"))]
mod server {
    use super::*;
    use crate::envelope::{Body, Limits, ToReplica};
    use crate::plan::{Engine, Replicas};
    use crate::relay::{Relay, Transport};
    use prost::Message as _;
    use std::sync::Mutex;

    pub const EPOCH: &str = "0192f6a0-0000-7000-8000-00000000e90c";
    pub const REPLICA: &str = "0192f6a0-0000-7000-8000-0000000000aa";

    const SCHEMA: &str = "
    CREATE SCHEMA fixture;
    CREATE TABLE fixture.t_replicas (id UUID PRIMARY KEY, live BOOLEAN NOT NULL);
    CREATE TABLE fixture.items (
        id UUID PRIMARY KEY, partition_id UUID NOT NULL, name TEXT NOT NULL, retired_at TIMESTAMPTZ);
    CREATE VIEW items AS SELECT * FROM fixture.items;
    CREATE TABLE fixture.shelves (aisle UUID NOT NULL, slot BIGINT NOT NULL, label TEXT NOT NULL,
        revision BIGINT NOT NULL, PRIMARY KEY (aisle, slot));
    CREATE TABLE fixture.prices (id UUID PRIMARY KEY, cents BIGINT NOT NULL, updated_at TIMESTAMPTZ NOT NULL);
    CREATE TABLE fixture.tasks (id UUID PRIMARY KEY, title TEXT NOT NULL, done BOOLEAN NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL, retired_at TIMESTAMPTZ);
    ";

    /// Each step's statements, run before its pass.
    pub const STEPS: &[(&str, &str)] = &[
        (
            "rows",
            "INSERT INTO fixture.items (id, partition_id, name) VALUES
               ('0192f6a0-0000-7000-8000-000000000a01', '00000000-0000-0000-0000-000000000000', 'a'),
               ('0192f6a0-0000-7000-8000-000000000a02', '00000000-0000-0000-0000-000000000000', 'b');
             INSERT INTO fixture.shelves VALUES
               ('0192f6a0-0000-7000-8000-000000000b01', 1, 'top', 1800000000000),
               ('0192f6a0-0000-7000-8000-000000000b01', 2, 'bottom', 1800000000000);
             INSERT INTO fixture.prices VALUES
               ('0192f6a0-0000-7000-8000-000000000c01', 9007199254740993, '2027-01-15 08:00:00.123456+00');
             INSERT INTO fixture.tasks (id, title, done, updated_at) VALUES
               ('0192f6a0-0000-7000-8000-000000000d01', 'count', false, '2027-01-15 08:00:00+00');",
        ),
        (
            "update",
            "UPDATE fixture.items SET name = 'a2' WHERE name = 'a';
             UPDATE fixture.shelves SET label = 'top2', revision = 1800000000001 WHERE slot = 1;
             UPDATE fixture.prices SET cents = 250, updated_at = '2027-01-15 08:00:01+00';
             UPDATE fixture.tasks SET title = 'recount', updated_at = '2027-01-15 08:00:01+00';",
        ),
        (
            "delete",
            "DELETE FROM fixture.items WHERE name = 'b'; DELETE FROM fixture.shelves WHERE slot = 2;",
        ),
        (
            "snapshot",
            "INSERT INTO sync.notes (kind, replica_id) VALUES ('resync', '0192f6a0-0000-7000-8000-0000000000aa');",
        ),
        (
            "emptied",
            "DELETE FROM fixture.items; DELETE FROM fixture.shelves; DELETE FROM fixture.prices;
             DELETE FROM fixture.tasks;",
        ),
        (
            "empty_snapshot",
            "INSERT INTO sync.notes (kind, replica_id) VALUES ('resync', '0192f6a0-0000-7000-8000-0000000000aa');",
        ),
    ];

    /// What the relay sends the replica.
    #[derive(Default)]
    struct Kept(Mutex<Vec<Vec<u8>>>);

    impl Transport for Kept {
        async fn send(
            &self,
            _replica: Uuid,
            message: Vec<u8>,
            _id: &str,
        ) -> Result<(), crate::Error> {
            self.0.lock().unwrap().push(message);
            Ok(())
        }
    }

    /// Every message of every step, as `step hex` lines.
    async fn sent(pool: crate::DbPool) -> String {
        let mut conn = pool.acquire().await.unwrap();
        crate::plan::tests::install(&mut conn).await;
        sqlx::raw_sql(SCHEMA).execute(&mut *conn).await.unwrap();
        sqlx::query("UPDATE sync.epoch SET epoch = $1::uuid")
            .bind(EPOCH)
            .execute(&mut *conn)
            .await
            .unwrap();
        sqlx::query("INSERT INTO fixture.t_replicas VALUES ($1::uuid, true)")
            .bind(REPLICA)
            .execute(&mut *conn)
            .await
            .unwrap();
        let codecs = codecs();
        let tables: Vec<&crate::row::Table> = codecs
            .iter()
            .filter(|c| c.sent_down())
            .map(|c| &c.table)
            .collect();
        let replicas = Replicas {
            relation: "fixture.t_replicas",
            table: "t_replicas",
            live: "live",
            partition: None,
        };
        drop(conn);
        let mut tx = pool.begin().await.unwrap();
        let engine = Engine::install(&mut tx, &tables, replicas).await.unwrap();
        tx.commit().await.unwrap();
        let relay = Relay {
            engine: &engine,
            codecs: &codecs,
            limits: Limits {
                rows: 100,
                bytes: 1 << 20,
            },
            batch: 100,
            timeout: std::time::Duration::from_secs(5),
            backoff: crate::run::Backoff::new(
                std::time::Duration::from_secs(1),
                std::time::Duration::from_secs(60),
            ),
            bound: crate::relay::Bound {
                messages: 1_000,
                bytes: 1 << 30,
            },
        };
        let mut snapshots: Vec<String> = Vec::new();
        let mut out = String::new();
        for (step, sql) in STEPS {
            sqlx::raw_sql(sql).execute(&pool).await.unwrap();
            loop {
                let kept = Kept::default();
                let mut conn = pool.acquire().await.unwrap();
                let mut tx = sqlx::Connection::begin_with(
                    &mut *conn,
                    "BEGIN ISOLATION LEVEL REPEATABLE READ",
                )
                .await
                .unwrap();
                let done = relay.record(&mut tx).await.unwrap().done;
                tx.commit().await.unwrap();
                relay.publish(&mut conn, &kept).await.unwrap();
                if done == 0 {
                    break;
                }
                for bytes in kept.0.into_inner().unwrap() {
                    let mut message = ToReplica::decode(bytes.as_slice()).unwrap();
                    let snapshot = match &mut message.body {
                        Some(Body::Down(m)) => Some(&mut m.snapshot_id),
                        Some(Body::EndSnapshot(m)) => Some(&mut m.snapshot_id),
                        _ => None,
                    };
                    if let Some(id) = snapshot.filter(|id| !id.is_empty()) {
                        let at = snapshots.iter().position(|s| s == id).unwrap_or_else(|| {
                            snapshots.push(id.clone());
                            snapshots.len() - 1
                        });
                        *id = format!("0192f6a0-0000-7000-8000-{:012x}", 0x5000 + at);
                    }
                    let hex: String = message
                        .encode_to_vec()
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect();
                    out.push_str(&format!("{step} {hex}\n"));
                }
            }
        }
        out
    }

    /// The golden messages are what this build's server sends: regenerated
    /// here, they are the ones checked in, which the sqlite side's suite
    /// applies. `MAGIC_SYNC_BLESS=1` writes them anew.
    #[sqlx::test(migrations = false)]
    async fn the_golden_messages_are_what_the_server_sends(pool: crate::DbPool) {
        let header = "# What the server sends one replica, step by step: `step hex` per message.\n\
                      # Generated by conformance::server; applied by the replica conformance suite.\n";
        let generated = format!("{header}{}", sent(pool).await);
        if std::env::var_os("MAGIC_SYNC_BLESS").is_some() {
            std::fs::create_dir_all(std::path::Path::new(GOLDEN).parent().unwrap()).unwrap();
            std::fs::write(GOLDEN, &generated).unwrap();
        }
        let checked_in = std::fs::read_to_string(GOLDEN).unwrap_or_default();
        assert!(
            checked_in == generated,
            "tests/golden/to_replica.txt is not what the server sends: run this test with \
             MAGIC_SYNC_BLESS=1, then the sqlite suite against the result, and commit it"
        );
    }
}
