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

/// Says which messages up a transport can never take.
#[cfg(feature = "sqlite")]
pub type CannotTake = fn(&crate::envelope::Up) -> bool;

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
    /// Refuses for good every message this says it cannot take.
    pub cannot_take: std::sync::Arc<std::sync::Mutex<Option<CannotTake>>>,
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
        if self
            .cannot_take
            .lock()
            .unwrap()
            .is_some_and(|cannot| cannot(&up))
        {
            return Err(crate::Error::Transport {
                reason: "cannot take it".into(),
                transient: false,
            });
        }
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

/// `pool`'s database, through connections set as a replica's must be
/// ([`crate::replica::check`]).
#[cfg(feature = "sqlite")]
pub async fn settled(pool: sqlx::SqlitePool) -> sqlx::SqlitePool {
    let options = (*pool.connect_options())
        .clone()
        .pragma("recursive_triggers", "ON");
    sqlx::SqlitePool::connect_with(options)
        .await
        .expect("the same database")
}

/// Where the golden uploads are kept: what the replica sends, captured on
/// the sqlite side, filed by the postgres side's server.
pub const GOLDEN_UP: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/to_server.txt");

/// Where the messages no server sends are kept, each one a replica must
/// take as malformed: acknowledged, nothing applied.
pub const MALFORMED: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/malformed.txt");

/// The replica the golden messages are for, and the server installations
/// they come from.
pub const REPLICA: &str = "0192f6a0-0000-7000-8000-0000000000aa";
pub const EPOCH: &str = "0192f6a0-0000-7000-8000-00000000e90c";
#[cfg(feature = "sqlite")]
pub const NEXT_EPOCH: &str = "0192f6a0-0000-7000-8000-00000000e90d";

/// The number the golden upload is sent under, pinned: a replica draws a
/// random one.
#[cfg(feature = "sqlite")]
pub const UPLOAD: u64 = 0x0192_f6a0_0000_7001;

/// `bytes` as a golden line's hex.
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Holds `text` against the golden file `path`; `MAGIC_SYNC_BLESS=1` writes
/// it.
pub fn matches_golden(path: &str, text: &str) {
    if std::env::var_os("MAGIC_SYNC_BLESS").is_some() {
        std::fs::create_dir_all(std::path::Path::new(path).parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }
    let checked_in = std::fs::read_to_string(path).unwrap_or_default();
    assert!(
        checked_in == text,
        "{path} is not what this build sends: run its test with MAGIC_SYNC_BLESS=1, then the \
         other side's suite against the result, and commit it"
    );
}

/// Messages no server sends, each named for what is wrong with it: a
/// replica of any build takes each as malformed (PROTOCOL.md 4.0, 4.1, 4.2).
/// They follow a replica provisioned by a snapshot of nothing, `EndSnapshot`
/// 100 of [`EPOCH`].
pub fn malformed() -> Vec<(&'static str, Vec<u8>)> {
    use crate::envelope::{Body, Down, EndSnapshot, ToReplica};
    use prost::Message as _;
    let down = |sequence: u64, previous: u64, snapshot: &str, epoch: &str| {
        Body::Down(Down {
            snapshot_id: snapshot.to_owned(),
            tables: Vec::new(),
            sequence,
            epoch: epoch.to_owned(),
            previous,
        })
    };
    let message = |protocol: u32, body: Option<Body>| ToReplica { protocol, body }.encode_to_vec();
    let mut truncated = message(1, Some(down(101, 100, "", EPOCH)));
    truncated.truncate(truncated.len() - 3);
    vec![
        ("no_message", vec![0xff, 0xff, 0xff]),
        ("truncated", truncated),
        ("no_protocol", message(0, Some(down(101, 100, "", EPOCH)))),
        ("no_body", message(1, None)),
        ("unknown_body", vec![0x08, 0x01, 0x4a, 0x02, 0x08, 0x01]),
        ("sequence_zero", message(1, Some(down(0, 0, "s", EPOCH)))),
        (
            "sequence_too_large",
            message(1, Some(down(1 << 63, 100, "", EPOCH))),
        ),
        ("epoch_no_uuid", message(1, Some(down(101, 100, "", "e")))),
        (
            "follows_itself",
            message(1, Some(down(101, 101, "", EPOCH))),
        ),
        (
            "follows_a_later_one",
            message(1, Some(down(101, 102, "", EPOCH))),
        ),
        ("keys_not_one_per_row", {
            let mut down = down(101, 100, "", EPOCH);
            if let Body::Down(d) = &mut down {
                d.tables.push(crate::envelope::Table {
                    table: "items".to_owned(),
                    upserts: vec![Vec::new()],
                    ..crate::envelope::Table::default()
                });
            }
            message(1, Some(down))
        }),
        (
            "follows_nothing_and_begins_nothing",
            message(1, Some(down(101, 0, "", EPOCH))),
        ),
        (
            "of_a_snapshot_not_in_progress",
            message(
                1,
                Some(down(
                    101,
                    100,
                    "0192f6a0-0000-7000-8000-000000005fff",
                    EPOCH,
                )),
            ),
        ),
        (
            "ends_a_snapshot_not_in_progress",
            message(
                1,
                Some(Body::EndSnapshot(EndSnapshot {
                    snapshot_id: "0192f6a0-0000-7000-8000-000000005fff".to_owned(),
                    tables: vec!["items".to_owned()],
                    sequence: 101,
                    epoch: EPOCH.to_owned(),
                    previous: 100,
                })),
            ),
        ),
    ]
}

/// The malformed corpus is what [`malformed`] builds: a replica in another
/// language reads its cases from the file.
#[test]
fn the_malformed_corpus_is_the_one_checked_in() {
    let header = "# Messages no server sends: `case hex` per message. A replica provisioned by an\n\
                  # EndSnapshot 100 of epoch 0192f6a0-0000-7000-8000-00000000e90c, of nothing, takes each\n\
                  # as malformed: it acknowledges it and applies nothing (PROTOCOL.md 4.0, 4.1, 4.2).\n";
    let text: String = malformed()
        .iter()
        .map(|(case, bytes)| format!("{case} {}\n", hex(bytes)))
        .collect();
    matches_golden(MALFORMED, &format!("{header}{text}"));
}

/// The server's side of the golden messages: a server with every table of
/// the suite and one replica, run step by step — the real relay's passes,
/// and the real filing of the golden upload — each message it sends that
/// replica kept. What varies from run to run — the snapshot ids a pass
/// draws — is pinned, so the same build sends the same bytes.
#[cfg(all(test, feature = "postgres"))]
mod server {
    use super::*;
    use crate::envelope::{Body, Limits, ToReplica};
    use crate::plan::Replicas;
    use crate::run::server::{Anyone, Server, Settings, Transport};
    use prost::Message as _;
    use std::sync::{Arc, Mutex};

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
    CREATE TABLE fixture.readings (replica_id UUID NOT NULL, id UUID NOT NULL, value BIGINT NOT NULL,
        at BIGINT NOT NULL, PRIMARY KEY (replica_id, id));
    CREATE TABLE fixture.events (id UUID PRIMARY KEY, what TEXT NOT NULL);
    ";

    const RESYNC: &str =
        "INSERT INTO sync.notes (kind, replica_id) VALUES ('resync', '0192f6a0-0000-7000-8000-0000000000aa');";

    /// Each step: its name, the statements run before its pass, and how many
    /// of its messages the transport takes before it fails (`None`: all).
    /// The replica's suite reads the steps by name.
    pub const STEPS: &[(&str, &str, Option<usize>)] = &[
        // The first messages to a replica: its provisioning, of several
        // messages (two rows each).
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
            None,
        ),
        (
            "update",
            "UPDATE fixture.items SET name = 'a2' WHERE name = 'a';
             UPDATE fixture.shelves SET label = 'top2', revision = 1800000000001 WHERE slot = 1;
             UPDATE fixture.prices SET cents = 250, updated_at = '2027-01-15 08:00:01+00';
             UPDATE fixture.tasks SET title = 'recount', updated_at = '2027-01-15 08:00:01+00';",
            None,
        ),
        // A row the replica refuses (its own check): it keeps what it had.
        (
            "refused",
            "UPDATE fixture.shelves SET label = 'refused', revision = 1800000000002 WHERE slot = 1;",
            None,
        ),
        // A row a row of the replica's own points at: retired, not deleted.
        (
            "delete",
            "DELETE FROM fixture.items WHERE name = 'b'; DELETE FROM fixture.shelves WHERE slot = 2;",
            None,
        ),
        // A snapshot cut short by the transport, superseded by the next.
        ("cut", RESYNC, Some(1)),
        ("snapshot", RESYNC, None),
        // A message the transport loses: the next follows one the replica
        // never got.
        ("lost", "UPDATE fixture.items SET name = 'a3' WHERE name = 'a2';", None),
        ("gap", "UPDATE fixture.items SET name = 'a4' WHERE name = 'a3';", None),
        ("healed", RESYNC, None),
        // The replica's upload, filed, answered.
        ("filed", "", None),
        // The server installed again: a new epoch begins with a snapshot.
        (
            "epoch",
            "UPDATE sync.epoch SET epoch = '0192f6a0-0000-7000-8000-00000000e90d';
             DELETE FROM sync.outbox; DELETE FROM sync.held; DELETE FROM sync.gone;
             UPDATE sync.chains SET last_sequence = NULL, snapshot_sequence = NULL, snapshot_end = NULL;
             INSERT INTO sync.notes (kind, replica_id) VALUES ('resync', '0192f6a0-0000-7000-8000-0000000000aa');",
            None,
        ),
        (
            "emptied",
            "DELETE FROM fixture.items; DELETE FROM fixture.shelves; DELETE FROM fixture.prices;
             DELETE FROM fixture.tasks;",
            None,
        ),
        ("empty_snapshot", RESYNC, None),
    ];

    /// What the server sends the replica, as long as it takes them.
    #[derive(Clone, Default)]
    struct Kept {
        sent: Arc<Mutex<Vec<Vec<u8>>>>,
        takes: Arc<Mutex<Option<usize>>>,
    }

    impl Transport for Kept {
        async fn send(
            &self,
            _replica: Uuid,
            message: Vec<u8>,
            _id: &str,
        ) -> Result<(), crate::Error> {
            let mut takes = self.takes.lock().unwrap();
            if *takes == Some(0) {
                return Err(crate::Error::Transport {
                    reason: "cut".into(),
                    transient: true,
                });
            }
            if let Some(n) = takes.as_mut() {
                *n -= 1;
            }
            self.sent.lock().unwrap().push(message);
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
        drop(conn);
        let replicas = Replicas::relation("fixture.t_replicas")
            .table("t_replicas")
            .live("live");
        let kept = Kept::default();
        let settings = Settings {
            limits: Limits {
                rows: 2,
                bytes: 1 << 20,
            },
            ..Settings::default()
        };
        let server = Server::install(
            pool.clone(),
            codecs(),
            replicas,
            kept.clone(),
            Anyone::new(pool.clone()),
            settings,
        )
        .await
        .unwrap();
        let uploads = std::fs::read_to_string(GOLDEN_UP).expect("the golden uploads");
        let mut snapshots: Vec<String> = Vec::new();
        let mut out = String::new();
        for (step, sql, takes) in STEPS {
            sqlx::raw_sql(sql).execute(&pool).await.unwrap();
            *kept.takes.lock().unwrap() = *takes;
            if *step == "filed" {
                for (_, bytes) in golden(&uploads) {
                    server
                        .receive(REPLICA.parse().unwrap(), &bytes)
                        .await
                        .unwrap();
                }
                let filed: i64 = sqlx::query_scalar(
                    "SELECT (SELECT count(*) FROM fixture.readings) + (SELECT count(*) FROM fixture.events)",
                )
                .fetch_one(&pool)
                .await
                .unwrap();
                assert_eq!(filed, 2, "the golden upload is filed");
            }
            loop {
                sqlx::query("UPDATE sync.replicas SET retry_after = NULL")
                    .execute(&pool)
                    .await
                    .unwrap();
                let before = kept.sent.lock().unwrap().len();
                server.pass_now().await.unwrap();
                if kept.sent.lock().unwrap().len() == before || takes.is_some() {
                    break;
                }
            }
            for bytes in std::mem::take(&mut *kept.sent.lock().unwrap()) {
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
                out.push_str(&format!("{step} {}\n", hex(&message.encode_to_vec())));
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
                      # Generated by conformance::server; applied by the replica conformance suite,\n\
                      # which reads each step by its name (the transport loses the `lost` one).\n";
        matches_golden(GOLDEN, &format!("{header}{}", sent(pool).await));
    }
}
