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

/// A setting: a replica's screen, whose level the server may wish.
#[derive(sqlx::FromRow, Clone, Debug, PartialEq, Serialize, Deserialize, crate::Row)]
#[row(
    table = "screens",
    schema = "fixture",
    mirror = "setting",
    key = "replica_id, id",
    origin = "replica_id"
)]
pub struct Screen {
    pub replica_id: Uuid,
    pub id: Uuid,
    #[row(desired)]
    pub level: i64,
    #[row(version)]
    pub revision: i64,
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

/// Every table of the golden corpora, parents first: the suite's, and a
/// setting, whose wishes travel as the engine's own table.
pub fn golden_codecs() -> Vec<Codec<()>> {
    let mut codecs = codecs();
    codecs.push(Codec::json::<Screen>(None));
    codecs
}

/// A row of a JSON table, as the server sends it.
#[cfg(feature = "sqlite")]
pub fn json<R: Row + Serialize>(row: &R) -> Vec<u8> {
    let table = crate::row::Table::of::<R>();
    crate::wire::json_encode(R::TABLE, &table.columns, row).expect("a fixture row encodes")
}

/// Where the golden messages are kept.
pub const GOLDEN: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/to_replica.txt");

/// The messages of a golden file, as `(name, outcome, bytes)`, in order:
/// one line each — its name, what the other side makes of it, and the
/// message in hex. Lines of what a replica then holds (`holds …`) are
/// [`holds`]'.
pub fn golden(text: &str) -> Vec<(String, String, Vec<u8>)> {
    text.lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#') && !line.starts_with("holds "))
        .map(|line| {
            let mut fields = line.splitn(3, ' ');
            let mut next = || fields.next().expect("a name, an outcome and the bytes");
            let (name, outcome, hex) = (next(), next(), next());
            let bytes = (0..hex.len())
                .step_by(2)
                .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).expect("hex"))
                .collect();
            (name.to_owned(), outcome.to_owned(), bytes)
        })
        .collect()
}

/// What a golden file says a replica holds after each step, as
/// `(step, json)`: the lines `holds step json`.
#[cfg(feature = "sqlite")]
pub fn holds(text: &str) -> Vec<(String, serde_json::Value)> {
    text.lines()
        .filter_map(|line| line.strip_prefix("holds "))
        .map(|line| {
            let (step, json) = line.split_once(' ').expect("a step and what it holds");
            let json = serde_json::from_str(json).expect("what it holds, as json");
            (step.to_owned(), json)
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
    /// The lanes whose sends never return.
    pub hanging: std::sync::Arc<std::sync::Mutex<Vec<&'static str>>>,
    /// Every send it was asked to make.
    pub attempts: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    /// Every message it was handed, taken or not, since last asked.
    pub offered: std::sync::Arc<std::sync::Mutex<Vec<crate::envelope::Up>>>,
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
        self.attempts
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let up = crate::envelope::Up::decode(message.as_slice()).expect("an upload");
        self.offered.lock().unwrap().push(up.clone());
        if self.hanging.lock().unwrap().contains(&lane) {
            std::future::pending::<()>().await;
        }
        let lanes = self.refused_lanes.lock().unwrap().clone();
        if let Some(transient) = *self.refusing.lock().unwrap() {
            if lanes.is_empty() || lanes.contains(&lane) {
                return Err(crate::Error::transport(transient, "refused"));
            }
        }
        if self
            .cannot_take
            .lock()
            .unwrap()
            .is_some_and(|cannot| cannot(&up))
        {
            return Err(crate::Error::transport(false, "cannot take it"));
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

/// The seal the messages built by hand carry: a server's is its own
/// HMAC, which a replica cannot check, and gives back as it came.
pub const SEAL: [u8; 16] = [0x5e; 16];

/// The nonce the messages built by hand carry at `sequence`: a server draws
/// its own at random.
pub fn nonce_of(sequence: u64) -> u64 {
    0x5eed_0000_0000_0000 | sequence
}

/// What a replica makes of a message, as the corpora name it beside the
/// bytes: `applied`, `ended`, `filed`, `already` (at or below the last
/// applied), `gap` or `malformed`.
pub type Outcome = &'static str;

/// Messages no server sends — and, between them, the few that set the
/// scene — each named for what it is and given with what a replica of any
/// build makes of it (PROTOCOL.md 3, 4.0, 4.1, 4.2), in order: they follow a
/// replica provisioned by a snapshot of nothing, `EndSnapshot` 100 of
/// [`EPOCH`], nonce `nonce_of(100)`.
pub fn malformed() -> Vec<(&'static str, Outcome, Vec<u8>)> {
    use crate::envelope::{Body, Down, EndSnapshot, Table, ToReplica};
    use prost::Message as _;
    let chained = |sequence: u64, previous: u64, snapshot: &str, epoch: &str| Down {
        snapshot_id: snapshot.to_owned(),
        sequence,
        epoch: epoch.to_owned(),
        previous,
        nonce: nonce_of(sequence),
        previous_nonce: if previous == 0 { 0 } else { nonce_of(previous) },
        mac: SEAL.to_vec(),
        ..Down::default()
    };
    let down = |sequence: u64, previous: u64, snapshot: &str, epoch: &str| {
        Body::Down(chained(sequence, previous, snapshot, epoch))
    };
    let end = |sequence: u64, previous: u64, snapshot: &str| {
        Body::EndSnapshot(EndSnapshot {
            snapshot_id: snapshot.to_owned(),
            tables: vec!["items".to_owned()],
            sequence,
            epoch: EPOCH.to_owned(),
            previous,
            nonce: nonce_of(sequence),
            previous_nonce: if previous == 0 { 0 } else { nonce_of(previous) },
            mac: SEAL.to_vec(),
        })
    };
    let message = |protocol: u32, body: Option<Body>| ToReplica { protocol, body }.encode_to_vec();
    let one = |body: Body| message(1, Some(body));
    let after = |previous: u64, change: fn(&mut Down)| {
        let mut d = chained(previous + 1, previous, "", EPOCH);
        change(&mut d);
        one(Body::Down(d))
    };
    let with = |change: fn(&mut Down)| after(100, change);
    let mut truncated = one(down(101, 100, "", EPOCH));
    truncated.truncate(truncated.len() - 3);
    let snapshot = "0192f6a0-0000-7000-8000-000000005eee";
    let other = "0192f6a0-0000-7000-8000-000000005fff";
    vec![
        ("no_message", "malformed", vec![0xff, 0xff, 0xff]),
        ("truncated", "malformed", truncated),
        (
            "no_protocol",
            "malformed",
            message(0, Some(down(101, 100, "", EPOCH))),
        ),
        ("no_body", "malformed", message(1, None)),
        (
            "unknown_body",
            "malformed",
            vec![0x08, 0x01, 0x4a, 0x02, 0x08, 0x01],
        ),
        ("sequence_zero", "malformed", one(down(0, 0, "s", EPOCH))),
        (
            "sequence_too_large",
            "malformed",
            one(down(1 << 63, 100, "", EPOCH)),
        ),
        ("epoch_no_uuid", "malformed", one(down(101, 100, "", "e"))),
        (
            "follows_itself",
            "malformed",
            one(down(101, 101, "", EPOCH)),
        ),
        (
            "follows_a_later_one",
            "malformed",
            one(down(101, 102, "", EPOCH)),
        ),
        ("no_nonce", "malformed", with(|d| d.nonce = 0)),
        ("nonce_too_large", "malformed", with(|d| d.nonce = 1 << 63)),
        (
            "follows_a_nonce_too_large",
            "malformed",
            with(|d| d.previous_nonce = 1 << 63),
        ),
        ("no_mac", "malformed", with(|d| d.mac = Vec::new())),
        ("mac_too_short", "malformed", with(|d| d.mac.truncate(15))),
        ("mac_too_long", "malformed", with(|d| d.mac.push(0x5e))),
        (
            "snapshot_no_uuid",
            "malformed",
            one(down(101, 0, "snapshot", EPOCH)),
        ),
        (
            "follows_without_its_nonce",
            "malformed",
            with(|d| d.previous_nonce = 0),
        ),
        (
            "follows_nothing_with_a_nonce",
            "malformed",
            one(Body::Down(Down {
                previous_nonce: 7,
                ..chained(101, 0, "s", EPOCH)
            })),
        ),
        (
            "keys_not_one_per_row",
            "malformed",
            with(|d| {
                d.tables.push(Table {
                    table: "items".to_owned(),
                    upserts: vec![Vec::new()],
                    ..Table::default()
                });
            }),
        ),
        (
            "follows_nothing_and_begins_nothing",
            "malformed",
            one(down(101, 0, "", EPOCH)),
        ),
        (
            "of_a_snapshot_not_in_progress",
            "malformed",
            one(down(101, 100, other, EPOCH)),
        ),
        (
            "ends_a_snapshot_not_in_progress",
            "malformed",
            one(end(101, 100, other)),
        ),
        (
            "ends_a_snapshot_of_no_id",
            "malformed",
            one(end(101, 100, "")),
        ),
        // More than a message carries, as a replica takes them by default.
        (
            "more_rows_than_a_message_carries",
            "malformed",
            with(|d| {
                d.tables.push(Table {
                    table: "items".to_owned(),
                    deletes: vec![crate::envelope::Key::default(); 501],
                    ..Table::default()
                });
            }),
        ),
        (
            "more_tables_than_a_message_carries",
            "malformed",
            with(|d| {
                d.tables = vec![
                    Table {
                        table: "items".to_owned(),
                        ..Table::default()
                    };
                    501
                ];
            }),
        ),
        (
            "ends_more_tables_than_a_message_carries",
            "malformed",
            one(Body::EndSnapshot(EndSnapshot {
                snapshot_id: snapshot.to_owned(),
                tables: vec!["items".to_owned(); 501],
                sequence: 101,
                epoch: EPOCH.to_owned(),
                nonce: nonce_of(101),
                mac: SEAL.to_vec(),
                ..EndSnapshot::default()
            })),
        ),
        (
            "files_more_uploads_than_a_message_carries",
            "malformed",
            one(Body::Filed(crate::envelope::Filed {
                uploads: (1..=501).collect(),
            })),
        ),
        // A key that does not parse is left out, and the rest applied.
        (
            "a_delete_of_a_key_that_does_not_parse",
            "applied",
            with(|d| {
                d.tables.push(Table {
                    table: "items".to_owned(),
                    deletes: vec![crate::envelope::Key {
                        parts: vec!["not a uuid".to_owned()],
                    }],
                    ..Table::default()
                });
            }),
        ),
        // A row beside a key not its own is rejected, and the rest applied.
        (
            "a_row_beside_another_key",
            "applied",
            after(101, |d| {
                let row = ItemRow {
                    id: "0192f6a0-0000-7000-8000-0000000000b1".to_owned(),
                    partition_id: Uuid::nil().to_string(),
                    name: "beside".to_owned(),
                };
                d.tables.push(Table {
                    table: "items".to_owned(),
                    upserts: vec![row.encode_to_vec()],
                    keys: vec![crate::envelope::Key {
                        parts: vec!["0192f6a0-0000-7000-8000-0000000000b2".to_owned()],
                    }],
                    ..Table::default()
                });
            }),
        ),
        (
            "begins_a_snapshot",
            "applied",
            one(down(103, 0, snapshot, EPOCH)),
        ),
        (
            "follows_the_snapshot_and_is_not_of_it",
            "malformed",
            one(down(104, 103, "", EPOCH)),
        ),
        (
            "follows_the_snapshot_and_is_of_another",
            "malformed",
            one(down(104, 103, other, EPOCH)),
        ),
        ("ends_the_snapshot", "ended", one(end(104, 103, snapshot))),
    ]
}

/// The malformed corpus is what [`malformed`] builds: a replica in another
/// language reads its cases from the file.
#[test]
fn the_malformed_corpus_is_the_one_checked_in() {
    let header = "# Messages no server sends, and between them the few that set the scene:\n\
                  # `case outcome hex` per message, in order. A replica provisioned by an\n\
                  # EndSnapshot 100 of epoch 0192f6a0-0000-7000-8000-00000000e90c, nonce\n\
                  # 0x5eed000000000064, sealed with sixteen bytes 0x5e, of nothing, makes of each\n\
                  # what `outcome` says: `malformed`\n\
                  # is acknowledged and applies nothing (PROTOCOL.md 3, 4.0, 4.1, 4.2). It\n\
                  # takes 500 rows a message, and 500 tables or uploads, at most.\n";
    let text: String = malformed()
        .iter()
        .map(|(case, outcome, bytes)| format!("{case} {outcome} {}\n", hex(bytes)))
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
    CREATE TABLE fixture.screens (replica_id UUID NOT NULL, id UUID NOT NULL, level BIGINT NOT NULL,
        revision BIGINT NOT NULL, PRIMARY KEY (replica_id, id));
    ";

    /// Where what a replica says of itself, and does to two-way rows and its
    /// own, is kept with what the server makes of each: built as a replica
    /// would send it, filed by the server's suite.
    const VERDICTS: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/golden/to_server_verdicts.txt"
    );

    /// The wishes of the verdicts: one the replica holds, one withdrawn
    /// before it answered; and the setting rows they are of.
    const WISHED: &str = "0192f6a0-0000-7000-8000-00000000f001";
    const WITHDRAWN: &str = "0192f6a0-0000-7000-8000-00000000f003";
    const SCREEN: &str = "0192f6a0-0000-7000-8000-00000000f002";
    const OTHER_SCREEN: &str = "0192f6a0-0000-7000-8000-00000000f004";

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
        // The transport delivers the first message again.
        ("again", "", None),
        (
            "update",
            "UPDATE fixture.items SET name = 'a2' WHERE name = 'a';
             UPDATE fixture.shelves SET label = 'top2', revision = 1800000000001 WHERE slot = 1;
             UPDATE fixture.prices SET cents = 250, updated_at = '2027-01-15 08:00:01+00';
             UPDATE fixture.tasks SET title = 'recount', updated_at = '2027-01-15 08:00:01+00';
             INSERT INTO sync.sync_wishes (id, replica_id, relation, row_key, wanted, version,
                                           if_unchanged, note)
             VALUES ('0192f6a0-0000-7000-8000-00000000f001', '0192f6a0-0000-7000-8000-0000000000aa',
                     'screens',
                     '0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f002',
                     '{\"replica_id\":\"0192f6a0-0000-7000-8000-0000000000aa\",\"id\":\"0192f6a0-0000-7000-8000-00000000f002\",\"level\":70,\"revision\":3}',
                     7, false, 'ops'),
                    ('0192f6a0-0000-7000-8000-00000000f005', '0192f6a0-0000-7000-8000-0000000000aa',
                     'screens',
                     '0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f004',
                     '{\"replica_id\":\"0192f6a0-0000-7000-8000-0000000000aa\",\"id\":\"0192f6a0-0000-7000-8000-00000000f004\",\"level\":30,\"revision\":1}',
                     9, false, NULL);",
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
        // A snapshot, which names a setting the server records the replica
        // holding, and the replica does not: it notes it deleted.
        (
            "snapshot",
            "INSERT INTO fixture.screens VALUES ('0192f6a0-0000-7000-8000-0000000000aa',
               '0192f6a0-0000-7000-8000-00000000f004', 1, 1);
             INSERT INTO sync.notes (kind, replica_id) VALUES ('resync', '0192f6a0-0000-7000-8000-0000000000aa');",
            None,
        ),
        // A message the transport loses: the next follows one the replica
        // never got. Then, in the gap, a message of another epoch, and one
        // that follows the last applied: neither applies until a snapshot.
        ("lost", "UPDATE fixture.items SET name = 'a3' WHERE name = 'a2';", None),
        ("gap", "UPDATE fixture.items SET name = 'a4' WHERE name = 'a3';", None),
        ("healed", RESYNC, None),
        // One wish withdrawn: the replica lets go of it.
        (
            "withdrawn",
            "DELETE FROM sync.sync_wishes WHERE id = '0192f6a0-0000-7000-8000-00000000f001';",
            None,
        ),
        // The replica's upload, filed, answered.
        ("filed", "", None),
        // A row whose version is older than the one the replica holds — a
        // server restored, or its clock set back: taken all the same.
        (
            "older",
            "UPDATE fixture.prices SET cents = 125, updated_at = '2027-01-15 07:00:00+00';",
            None,
        ),
        // A message with a field this build does not know, top level and in
        // its body: what it knows of it applies.
        (
            "unknown_field",
            "UPDATE fixture.items SET name = 'a5' WHERE name = 'a4';",
            None,
        ),
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
             DELETE FROM fixture.tasks; DELETE FROM sync.sync_wishes;",
            None,
        ),
        ("empty_snapshot", RESYNC, None),
    ];

    /// What the replica the golden file's header describes holds after each
    /// step, and what it then does of its own, as the file's `holds` lines
    /// say it: written by hand, from what the protocol says, never from
    /// what a build does.
    const HOLDS: &[(&str, &str)] = &[
        (
            "rows",
            r#"{"answers":[],"items":["a","b"],"noted":[],"prices":["9007199254740993"],"rejected":[],"shelves":["top 1800000000000","bottom 1800000000000"],"tasks":["count 1"],"wishes":[]}"#,
        ),
        (
            "again",
            r#"{"answers":[],"items":["a","b"],"noted":[],"prices":["9007199254740993"],"rejected":[],"shelves":["top 1800000000000","bottom 1800000000000"],"tasks":["count 1"],"wishes":[]}"#,
        ),
        (
            "update",
            r#"{"answers":[],"items":["a2","b"],"noted":[],"prices":["250"],"rejected":[],"shelves":["top2 1800000000001","bottom 1800000000000"],"tasks":["recount 1"],"wishes":["screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f002 7","screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f004 9"]}"#,
        ),
        (
            "refused",
            r#"{"answers":[],"items":["a2","b"],"noted":[],"prices":["250"],"rejected":["shelves 0192f6a0-0000-7000-8000-000000000b01|1"],"shelves":["top2 1800000000001","bottom 1800000000000"],"tasks":["recount 1"],"wishes":["screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f002 7","screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f004 9"]}"#,
        ),
        (
            "delete",
            r#"{"answers":[],"items":["a2","b retired"],"noted":[],"prices":["250"],"rejected":["shelves 0192f6a0-0000-7000-8000-000000000b01|1"],"shelves":["top2 1800000000001"],"tasks":["recount 1"],"wishes":["screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f002 7","screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f004 9"]}"#,
        ),
        (
            "cut",
            r#"{"answers":[],"items":["a2","b retired"],"noted":[],"prices":["250"],"rejected":["shelves 0192f6a0-0000-7000-8000-000000000b01|1"],"shelves":["top2 1800000000001"],"tasks":["recount 1"],"wishes":["screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f002 7","screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f004 9"]}"#,
        ),
        (
            "snapshot",
            r#"{"answers":[],"items":["a2","b retired"],"noted":["screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f004"],"prices":["250"],"rejected":["shelves 0192f6a0-0000-7000-8000-000000000b01|1"],"shelves":["top2 1800000000001"],"tasks":["recount 1"],"wishes":["screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f002 7","screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f004 9"]}"#,
        ),
        (
            "lost",
            r#"{"answers":[],"items":["a2","b retired"],"noted":["screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f004"],"prices":["250"],"rejected":["shelves 0192f6a0-0000-7000-8000-000000000b01|1"],"shelves":["top2 1800000000001"],"tasks":["recount 1"],"wishes":["screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f002 7","screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f004 9"]}"#,
        ),
        (
            "gap",
            r#"{"answers":[],"items":["a2","b retired"],"noted":["screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f004"],"prices":["250"],"rejected":["shelves 0192f6a0-0000-7000-8000-000000000b01|1"],"shelves":["top2 1800000000001"],"tasks":["recount 1"],"wishes":["screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f002 7","screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f004 9"]}"#,
        ),
        (
            "healed",
            r#"{"answers":[],"items":["a4","b retired"],"noted":["screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f004"],"prices":["250"],"rejected":["shelves 0192f6a0-0000-7000-8000-000000000b01|1"],"shelves":["top2 1800000000001"],"tasks":["recount 1"],"wishes":["screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f002 7","screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f004 9"]}"#,
        ),
        (
            "withdrawn",
            r#"{"answers":[],"items":["a4","b retired"],"noted":["screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f004"],"prices":["250"],"rejected":["shelves 0192f6a0-0000-7000-8000-000000000b01|1"],"shelves":["top2 1800000000001"],"tasks":["recount 1"],"wishes":["screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f004 9"]}"#,
        ),
        (
            "filed",
            r#"{"answers":["screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f004 9 waiting"],"items":["a4","b retired"],"noted":[],"prices":["250"],"rejected":["shelves 0192f6a0-0000-7000-8000-000000000b01|1"],"shelves":["top2 1800000000001"],"tasks":["recount 1"],"wishes":["screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f004 9"]}"#,
        ),
        (
            "older",
            r#"{"answers":["screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f004 9 waiting"],"items":["a4","b retired"],"noted":[],"prices":["125"],"rejected":["shelves 0192f6a0-0000-7000-8000-000000000b01|1"],"shelves":["top2 1800000000001"],"tasks":["recount 1"],"wishes":["screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f004 9"]}"#,
        ),
        (
            "unknown_field",
            r#"{"answers":["screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f004 9 waiting"],"items":["a5","b retired"],"noted":[],"prices":["125"],"rejected":["shelves 0192f6a0-0000-7000-8000-000000000b01|1"],"shelves":["top2 1800000000001"],"tasks":["recount 1"],"wishes":["screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f004 9"]}"#,
        ),
        (
            "epoch",
            r#"{"answers":["screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f004 9 waiting"],"items":["a5","b retired"],"noted":["screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f002 restated","sync_wishes 0192f6a0-0000-7000-8000-00000000f005 restated"],"prices":["125"],"rejected":["shelves 0192f6a0-0000-7000-8000-000000000b01|1"],"shelves":["top2 1800000000001"],"tasks":["recount 1"],"wishes":["screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f004 9"]}"#,
        ),
        (
            "emptied",
            r#"{"answers":[],"items":["b retired"],"noted":["screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f002 restated","sync_wishes 0192f6a0-0000-7000-8000-00000000f005 restated"],"prices":[],"rejected":[],"shelves":[],"tasks":[],"wishes":[]}"#,
        ),
        (
            "empty_snapshot",
            r#"{"answers":[],"items":["b retired"],"noted":["screens 0192f6a0-0000-7000-8000-0000000000aa|0192f6a0-0000-7000-8000-00000000f002 restated","sync_wishes 0192f6a0-0000-7000-8000-00000000f005 restated"],"prices":[],"rejected":[],"shelves":[],"tasks":[],"wishes":[]}"#,
        ),
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
            _audience: &str,
            _replica: Uuid,
            message: Vec<u8>,
            _id: &str,
        ) -> Result<(), crate::Error> {
            let mut takes = self.takes.lock().unwrap();
            if *takes == Some(0) {
                return Err(crate::Error::transport(true, "cut"));
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
        // Pinned, with the nonces: the same build seals the same bytes.
        sqlx::query("UPDATE sync.epoch SET epoch = $1::uuid, secret = '\\x5eed'")
            .bind(EPOCH)
            .execute(&mut *conn)
            .await
            .unwrap();
        crate::envelope::PINNED_NONCES.set(Some(nonce_of(1)));
        sqlx::query("INSERT INTO fixture.t_replicas VALUES ($1::uuid, true)")
            .bind(REPLICA)
            .execute(&mut *conn)
            .await
            .unwrap();
        drop(conn);
        let replicas = Replicas::new("fixture.t_replicas", "t_replicas").live("live");
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
            vec![crate::plan::Audience::new(
                "test",
                replicas,
                golden_codecs(),
            )],
            kept.clone(),
            Anyone::new(pool.clone()),
            settings,
        )
        .await
        .unwrap();
        // The uploads are a replica's of default limits: rows and
        // rejections past the two a message down carries here.
        let filer = Server::new(
            pool.clone(),
            vec![crate::plan::Audience::new(
                "test",
                replicas,
                golden_codecs(),
            )],
            kept.clone(),
            Anyone::new(pool.clone()),
            Settings::default(),
        )
        .await
        .unwrap();
        let uploads = std::fs::read_to_string(GOLDEN_UP).expect("the golden uploads");
        let mut snapshots: Vec<String> = Vec::new();
        let mut out = String::new();
        let line = |out: &mut String, step: &str, outcome: &str, bytes: &[u8]| {
            out.push_str(&format!("{step} {outcome} {}\n", hex(bytes)));
        };
        // The first message, delivered again; the last the replica applied.
        let mut first: Option<Vec<u8>> = None;
        let mut applied = (0, 0);
        for (step, sql, takes) in STEPS {
            let holds = HOLDS
                .iter()
                .find(|(s, _)| s == step)
                .expect("what it holds")
                .1;
            if *step == "again" {
                line(
                    &mut out,
                    step,
                    "already",
                    first.as_ref().expect("a first message"),
                );
                out.push_str(&format!("holds {step} {holds}\n"));
                continue;
            }
            sqlx::raw_sql(sql).execute(&pool).await.unwrap();
            *kept.takes.lock().unwrap() = *takes;
            if *step == "filed" {
                file_uploads(&filer, &pool, &uploads).await;
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
                // The bytes the transport took are the message's own, as
                // this build encodes it, however the outbox kept them.
                assert_eq!(message.encode_to_vec(), bytes, "{step}: as encoded");
                let (snapshot, place) = match &mut message.body {
                    Some(Body::Down(m)) => (Some(&mut m.snapshot_id), Some((m.sequence, m.nonce))),
                    Some(Body::EndSnapshot(m)) => {
                        (Some(&mut m.snapshot_id), Some((m.sequence, m.nonce)))
                    }
                    _ => (None, None),
                };
                if let Some(id) = snapshot.filter(|id| !id.is_empty()) {
                    let at = snapshots.iter().position(|s| s == id).unwrap_or_else(|| {
                        snapshots.push(id.clone());
                        snapshots.len() - 1
                    });
                    *id = format!("0192f6a0-0000-7000-8000-{:012x}", 0x5000 + at);
                }
                let outcome = match (&message.body, *step) {
                    (_, "lost") => "lost",
                    (_, "gap") => "gap",
                    (Some(Body::Down(_)), _) => "applied",
                    (Some(Body::EndSnapshot(_)), _) => "ended",
                    _ => "filed",
                };
                if let Some(place) = place.filter(|_| matches!(outcome, "applied" | "ended")) {
                    applied = place;
                }
                let mut bytes = message.encode_to_vec();
                if *step == "unknown_field" {
                    bytes = with_unknown_fields(&message);
                }
                first.get_or_insert_with(|| bytes.clone());
                line(&mut out, step, outcome, &bytes);
            }
            if *step == "gap" {
                let (sequence, nonce) = applied;
                let follows = |epoch: &str| {
                    Body::Down(crate::envelope::Down {
                        tables: Vec::new(),
                        sequence: sequence + 1_000,
                        epoch: epoch.to_owned(),
                        previous: sequence,
                        previous_nonce: nonce,
                        nonce: nonce_of(sequence + 1_000),
                        mac: SEAL.to_vec(),
                        ..crate::envelope::Down::default()
                    })
                    .encoded()
                };
                line(
                    &mut out,
                    step,
                    "gap",
                    &follows("0192f6a0-0000-7000-8000-0000000e90ff"),
                );
                line(&mut out, step, "gap", &follows(EPOCH));
            }
            out.push_str(&format!("holds {step} {holds}\n"));
        }
        out
    }

    /// `message` with a field this build does not know added to it, and to
    /// its body.
    fn with_unknown_fields(message: &ToReplica) -> Vec<u8> {
        const UNKNOWN: [u8; 2] = [15 << 3, 1];
        let mut body = match &message.body {
            Some(Body::Down(m)) => m.encode_to_vec(),
            Some(Body::EndSnapshot(m)) => m.encode_to_vec(),
            _ => panic!("a Down or an EndSnapshot"),
        };
        body.extend(UNKNOWN);
        let tag = match &message.body {
            Some(Body::Down(_)) => 2 << 3 | 2,
            _ => 3 << 3 | 2,
        };
        let mut bytes = vec![1 << 3, u8::try_from(message.protocol).unwrap(), tag];
        prost::encoding::encode_varint(body.len() as u64, &mut bytes);
        bytes.extend(body);
        bytes.extend(UNKNOWN);
        bytes
    }

    /// Files each golden upload, and holds the server to what its line says
    /// it makes of it: `filed`, its rows filed and answered; `confirmed`,
    /// what it applied recorded; `quarantined`, the row it refused set aside
    /// for it; `agrees`, its digests compared with nothing to provision;
    /// `ignored`, nothing of what it says of itself taken.
    async fn file_uploads(server: &Server<Anyone, Kept>, pool: &crate::DbPool, uploads: &str) {
        let replica: Uuid = REPLICA.parse().unwrap();
        let scalar = |sql: &'static str| async move {
            sqlx::query_scalar::<_, i64>(sql)
                .bind(replica)
                .fetch_one(pool)
                .await
                .unwrap()
        };
        let resyncs = "SELECT count(*) FROM sync.notes WHERE kind = 'resync' AND replica_id = $1";
        let applied =
            "SELECT coalesce(applied_sequence, 0) FROM sync.replicas WHERE replica_id = $1";
        for (name, outcomes, bytes) in golden(uploads) {
            let up = crate::envelope::Up::decode(bytes.as_slice()).unwrap();
            let (provisioned, confirmed) = (scalar(resyncs).await, scalar(applied).await);
            server.receive(replica, &bytes).await.unwrap();
            let outcomes: Vec<&str> = outcomes.split(',').collect();
            for outcome in &outcomes {
                match *outcome {
                    "filed" => {
                        let filed: i64 = sqlx::query_scalar(
                            "SELECT (SELECT count(*) FROM fixture.readings) \
                                  + (SELECT count(*) FROM fixture.events)",
                        )
                        .fetch_one(pool)
                        .await
                        .unwrap();
                        assert_eq!(filed, 2, "{name}: its rows filed");
                    }
                    "confirmed" => assert_eq!(
                        scalar(applied).await,
                        i64::try_from(up.sequence).unwrap(),
                        "{name}: what it applied, recorded"
                    ),
                    "quarantined" => assert_eq!(
                        scalar(
                            "SELECT count(*) FROM sync.quarantine \
                              WHERE replica_id = $1 AND cause = 'replica'"
                        )
                        .await,
                        i64::try_from(up.rejected.len()).unwrap(),
                        "{name}: what it refused, set aside"
                    ),
                    "agrees" => assert_eq!(
                        scalar(
                            "SELECT count(*) FROM sync.replicas \
                              WHERE replica_id = $1 AND compared_at IS NOT NULL"
                        )
                        .await,
                        1,
                        "{name}: its digests compared"
                    ),
                    "ignored" => {}
                    other => panic!("{name}: an outcome this suite does not know: {other}"),
                }
            }
            if outcomes.contains(&"ignored") {
                assert_eq!(scalar(applied).await, confirmed, "{name}: nothing recorded");
            }
            assert_eq!(
                scalar(resyncs).await,
                provisioned,
                "{name}: nothing provisioned again"
            );
        }
    }

    /// The golden messages are what this build's server sends: regenerated
    /// here, they are the ones checked in, which the sqlite side's suite
    /// applies. `MAGIC_SYNC_BLESS=1` writes them anew.
    #[sqlx::test(migrations = false)]
    async fn the_golden_messages_are_what_the_server_sends(pool: crate::DbPool) {
        let header = "# What the server sends one replica, step by step: `step outcome hex` per\n\
                      # message, `outcome` what the replica makes of it — applied, ended, filed,\n\
                      # already, gap — or `lost`, which the transport loses. After a step,\n\
                      # `holds step json`: what the replica then holds, per table its rows as\n\
                      # text (items: name, and ` retired`; shelves: label and revision; prices:\n\
                      # cents; tasks: title and done; wishes: the setting's table, its row's\n\
                      # key and the wish's version; answers: the wish, the version answered and\n\
                      # the outcome), its rejections as `table key`, and what it noted of its\n\
                      # setting and its answers for the server, as `table key`, and ` restated`.\n\
                      # The replica: it holds a setting, `screens`, which the server wishes two\n\
                      # of at `update` and withdraws one of at `withdrawn` — the other of a row\n\
                      # the replica does not hold, which the server records it holding at\n\
                      # `snapshot`; its own check refuses a shelf labelled `refused`; after\n\
                      # `rows` it marks its task done (its own column), has that filed, and\n\
                      # writes a receipt pointing at item b; at `filed`, it writes a screen of\n\
                      # its own, answers the wish it holds, and the golden upload's rows are\n\
                      # its own, sent under its number.\n\
                      # Generated by conformance::server; applied by the replica conformance\n\
                      # suite, which reads each step by its name.\n";
        matches_golden(GOLDEN, &format!("{header}{}", sent(pool).await));
    }

    /// What the server makes of what a replica says, and of what it does to
    /// rows written on both sides and its own — each upload as a replica
    /// sends it, after a provisioning, a change and a take-back it applied:
    /// the server's own outcome, held against the line's.
    #[sqlx::test(migrations = false)]
    async fn the_server_makes_of_each_upload_what_the_verdicts_say(pool: crate::DbPool) {
        use crate::envelope::{Key, Rejected, Table, Up};
        let replica: Uuid = REPLICA.parse().unwrap();
        let mut conn = pool.acquire().await.unwrap();
        crate::plan::tests::install(&mut conn).await;
        sqlx::raw_sql(SCHEMA).execute(&mut *conn).await.unwrap();
        sqlx::query("UPDATE sync.epoch SET epoch = $1::uuid, secret = '\\x5eed'")
            .bind(EPOCH)
            .execute(&mut *conn)
            .await
            .unwrap();
        crate::envelope::PINNED_NONCES.set(Some(nonce_of(1)));
        sqlx::raw_sql(&format!(
            "INSERT INTO fixture.t_replicas VALUES ('{REPLICA}', true);
             INSERT INTO fixture.prices VALUES
               ('0192f6a0-0000-7000-8000-000000000c01', 250, '2027-01-15 08:00:00+00');
             INSERT INTO fixture.tasks (id, title, done, updated_at) VALUES
               ('0192f6a0-0000-7000-8000-000000000d01', 'count', false, '2027-01-15 08:00:00+00');
             INSERT INTO fixture.readings VALUES
               ('{REPLICA}', '0192f6a0-0000-7000-8000-000000000e01', 7, 1);
             INSERT INTO sync.sync_wishes (id, replica_id, relation, row_key, wanted, version,
                                           if_unchanged)
             VALUES ('{WISHED}', '{REPLICA}', 'screens', '{REPLICA}|{SCREEN}',
                     '{{\"replica_id\":\"{REPLICA}\",\"id\":\"{SCREEN}\",\"level\":70,\"revision\":1}}', 7, false),
                    ('{WITHDRAWN}', '{REPLICA}', 'screens', '{REPLICA}|{OTHER_SCREEN}',
                     '{{\"replica_id\":\"{REPLICA}\",\"id\":\"{OTHER_SCREEN}\",\"level\":70,\"revision\":1}}',
                     8, false);"
        ))
        .execute(&mut *conn)
        .await
        .unwrap();
        let kept = Kept::default();
        let server = Server::install(
            pool.clone(),
            vec![crate::plan::Audience::new(
                "test",
                Replicas::new("fixture.t_replicas", "t_replicas").live("live"),
                golden_codecs(),
            )],
            kept.clone(),
            Anyone::new(pool.clone()),
            Settings::default(),
        )
        .await
        .unwrap();
        let pass = || async { while server.pass_now().await.unwrap().unwrap_or_default() > 0 {} };
        pass().await;
        sqlx::raw_sql(&format!(
            "DELETE FROM fixture.prices; DELETE FROM sync.sync_wishes WHERE id = '{WITHDRAWN}';"
        ))
        .execute(&mut *conn)
        .await
        .unwrap();
        pass().await;
        // What it was sent: its provisioning, ended; the price taken back.
        let mut sent = Vec::new();
        for bytes in std::mem::take(&mut *kept.sent.lock().unwrap()) {
            match ToReplica::decode(bytes.as_slice()).unwrap().body {
                Some(Body::Down(m)) => sent.push((
                    m.sequence,
                    !m.tables.is_empty() && m.tables.iter().all(|t| t.upserts.is_empty()),
                )),
                Some(Body::EndSnapshot(m)) => sent.push((m.sequence, false)),
                _ => {}
            }
        }
        let first = sent.first().expect("a provisioning").0;
        let ended: i64 = sqlx::query_scalar("SELECT snapshot_end FROM sync.chains")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        let took = sent
            .iter()
            .find(|(_, takes)| *takes)
            .expect("a take-back")
            .0;
        let last = sent.last().expect("a message").0;
        let key = |parts: &[&str]| Key {
            parts: parts.iter().map(|p| (*p).to_owned()).collect(),
        };
        let confirming = |sequence: u64| crate::testkit::confirmation(&pool, replica, sequence);
        let up = |number: u64| Up {
            uploads: vec![number],
            protocol: crate::envelope::PROTOCOL,
            ..Up::default()
        };
        let rejection = |table: &str, parts: &[&str], sequence: u64| Up {
            rejected: vec![Rejected {
                held_back: false,
                table: table.to_owned(),
                key: Some(key(parts)),
                reason: "a check of its own".to_owned(),
                epoch: EPOCH.to_owned(),
                sequence,
            }],
            ..up(0)
        };
        let task = Task {
            id: "0192f6a0-0000-7000-8000-000000000d01".parse().unwrap(),
            title: "count".to_owned(),
            done: true,
            updated_at: "2027-01-15T08:00:00Z".parse().unwrap(),
        };
        let edit = Up {
            tables: vec![Table {
                table: "tasks".to_owned(),
                upserts: vec![crate::wire::json_encode(
                    "tasks",
                    &crate::row::Table::of::<Task>().columns,
                    &task,
                )
                .unwrap()],
                keys: vec![key(&["0192f6a0-0000-7000-8000-000000000d01"])],
                deletes: Vec::new(),
                restated: false,
                recorded: Vec::new(),
            }],
            ..up(0x0192_f6a0_0000_7002)
        };
        let delete = Up {
            tables: vec![Table {
                table: "readings".to_owned(),
                deletes: vec![key(&[REPLICA, "0192f6a0-0000-7000-8000-000000000e01"])],
                ..Table::default()
            }],
            ..up(0x0192_f6a0_0000_7003)
        };
        let elsewhere = Up {
            epoch: "0192f6a0-0000-7000-8000-0000000e90ff".to_owned(),
            sequence: 1,
            nonce: 1,
            mac: SEAL.to_vec(),
            ..up(0)
        };
        // A replica says what it refused with the last message it applied.
        let refused_delete = Up {
            rejected: rejection("prices", &["0192f6a0-0000-7000-8000-000000000c01"], took).rejected,
            ..confirming(last).await
        };
        // A replica holds back a row of its own it deleted, the delete on
        // its way: the task as the provisioning carried it.
        let carried: i64 =
            sqlx::query_scalar("SELECT sequence FROM sync.held WHERE relation = 'tasks'")
                .fetch_one(&mut *conn)
                .await
                .unwrap();
        let held_back = Up {
            rejected: vec![Rejected {
                held_back: true,
                reason: "deleted here, and the delete not filed yet".to_owned(),
                ..rejection(
                    "tasks",
                    &["0192f6a0-0000-7000-8000-000000000d01"],
                    u64::try_from(carried).unwrap(),
                )
                .rejected[0]
                    .clone()
            }],
            ..up(0)
        };
        // Its answer to a wish, as it sends one up: the wish as it holds
        // it, and its own columns.
        let answer = |id: &str, screen: &str, version: u64, outcome: &str, number: u64| Up {
            tables: vec![Table {
                table: "sync_wishes".to_owned(),
                upserts: vec![crate::envelope::Wish {
                    id: id.to_owned(),
                    table: "screens".to_owned(),
                    key: Some(key(&[REPLICA, screen])),
                    wanted: b"{}".to_vec(),
                    version,
                    answered: version,
                    outcome: outcome.to_owned(),
                    answered_at: "2027-01-15T08:00:02.000000Z".to_owned(),
                    ..crate::envelope::Wish::default()
                }
                .encode_to_vec()],
                keys: vec![key(&[id])],
                deletes: Vec::new(),
                restated: false,
                recorded: Vec::new(),
            }],
            ..up(number)
        };
        // A setting of its own, as it is: re-stated, as a snapshot begins,
        // or not.
        let setting = |level: i64, revision: i64, restated: bool, number: u64| Up {
            tables: vec![Table {
                table: "screens".to_owned(),
                upserts: vec![crate::wire::json_encode(
                    "screens",
                    &crate::row::Table::of::<Screen>().columns,
                    &Screen {
                        replica_id: replica,
                        id: SCREEN.parse().unwrap(),
                        level,
                        revision,
                    },
                )
                .unwrap()],
                keys: vec![key(&[REPLICA, SCREEN])],
                deletes: Vec::new(),
                restated,
                recorded: Vec::new(),
            }],
            ..up(number)
        };
        let lines: Vec<(&str, &str, Up)> = vec![
            ("refused_delete", "confirmed,quarantined", refused_delete),
            // Once it confirmed its provisioning, an answer to a wish withdrawn
            // is the server's withdrawal winning, not an edit kept for later.
            (
                "answer",
                "filed,answered",
                answer(WISHED, SCREEN, 7, "applied", 0x0192_f6a0_0000_7004),
            ),
            // An answer of the same wish held back by the transport: the
            // final one stands.
            (
                "answer_late",
                "filed,answered",
                answer(WISHED, SCREEN, 7, "waiting", 0x0192_f6a0_0000_7008),
            ),
            (
                "answer_withdrawn",
                "filed,unheld",
                answer(WITHDRAWN, OTHER_SCREEN, 8, "applied", 0x0192_f6a0_0000_7005),
            ),
            (
                "setting",
                "filed,set",
                setting(40, 9, false, 0x0192_f6a0_0000_7006),
            ),
            (
                "setting_older",
                "filed,kept",
                setting(20, 3, false, 0x0192_f6a0_0000_7007),
            ),
            (
                "setting_restated",
                "filed,set",
                setting(20, 3, true, 0x0192_f6a0_0000_7009),
            ),
            ("edit", "filed,edited", edit),
            ("delete", "filed,gone", delete),
            (
                "refused_sweep",
                "quarantined",
                rejection(
                    "tasks",
                    &["0192f6a0-0000-7000-8000-00000000dead"],
                    u64::try_from(ended).unwrap(),
                ),
            ),
            ("held_back", "held_back", held_back),
            ("gap", "provisioned", Up { gap: last, ..up(0) }),
            ("behind", "provisioned", confirming(first).await),
            ("elsewhere", "provisioned", elsewhere),
            ("unknown", "restored", confirming(last + 1_000).await),
        ];
        let header =
            "# What the server makes of what a replica says, and does: `name outcomes hex`\n\
                      # per upload — confirmed: what it applied recorded; filed: answered;\n\
                      # edited: its column of a two-way row written; gone: its own row deleted;\n\
                      # quarantined: the row it refused set aside; held_back: the row of its\n\
                      # own it held back set aside, with no alarm; answered: its answer to a\n\
                      # wish kept beside it; unheld: its answer to a wish withdrawn left out;\n\
                      # set: its setting written; kept: an older version of its setting, not\n\
                      # re-stated, left out; provisioned: provisioned\n\
                      # again; restored: a new epoch, every replica provisioned again. Each\n\
                      # after a provisioning, a change and a take-back the replica applied.\n\
                      # Generated and filed by conformance::server.\n";
        let text: String = lines
            .iter()
            .map(|(name, outcomes, up)| format!("{name} {outcomes} {}\n", hex(&up.encode_to_vec())))
            .collect();
        matches_golden(VERDICTS, &format!("{header}{text}"));

        let scalar = |sql: &'static str| {
            let pool = pool.clone();
            async move {
                sqlx::query_scalar::<_, i64>(sql)
                    .bind(replica)
                    .fetch_one(&pool)
                    .await
                    .unwrap()
            }
        };
        let resyncs = "SELECT count(*) FROM sync.notes WHERE kind = 'resync' AND replica_id = $1 \
                         AND done_at IS NULL";
        let quarantined = "SELECT count(*) FROM sync.quarantine \
                             WHERE replica_id = $1 AND cause = 'replica'";
        let set_aside = "SELECT count(*) FROM sync.quarantine \
                           WHERE replica_id = $1 AND cause = 'held_back'";
        for (name, outcomes, bytes) in golden(&std::fs::read_to_string(VERDICTS).unwrap()) {
            // Each judged on its own: no provisioning pending, none asked lately.
            sqlx::raw_sql(
                "UPDATE sync.notes SET done_at = now() WHERE kind = 'resync';
                 UPDATE sync.replicas SET asked_at = NULL, asks = 0",
            )
            .execute(&pool)
            .await
            .unwrap();
            let before = (scalar(quarantined).await, scalar(set_aside).await);
            let up = Up::decode(bytes.as_slice()).unwrap();
            server.receive(replica, &bytes).await.unwrap();
            let outcomes: Vec<&str> = outcomes.split(',').collect();
            for outcome in &outcomes {
                let holds = match *outcome {
                    "confirmed" => {
                        scalar("SELECT applied_sequence FROM sync.replicas WHERE replica_id = $1")
                            .await
                            == i64::try_from(up.sequence).unwrap()
                    }
                    "filed" => kept.sent.lock().unwrap().iter().any(|bytes| {
                        matches!(ToReplica::decode(bytes.as_slice()).unwrap().body,
                                 Some(Body::Filed(f)) if f.uploads == up.uploads)
                    }),
                    "edited" => {
                        scalar("SELECT count(*) FROM fixture.tasks WHERE done AND $1 IS NOT NULL")
                            .await
                            == 1
                    }
                    "gone" => {
                        scalar("SELECT count(*) FROM fixture.readings WHERE replica_id = $1").await
                            == 0
                    }
                    "quarantined" => scalar(quarantined).await == before.0 + 1,
                    "held_back" => scalar(set_aside).await == before.1 + 1,
                    "answered" => {
                        scalar(
                            "SELECT count(*) FROM sync.sync_wishes WHERE replica_id = $1 \
                               AND answered = version AND outcome = 'applied'",
                        )
                        .await
                            == 1
                    }
                    "unheld" => {
                        scalar(
                            "SELECT count(*) FROM sync.sync_wishes WHERE replica_id = $1 \
                               AND answered IS NOT NULL",
                        )
                        .await
                            == 1
                            && scalar("SELECT count(*) FROM sync.inbox WHERE $1 IS NOT NULL").await
                                == 0
                    }
                    "set" | "kept" => {
                        let screen = crate::wire::json_decode::<Screen>(
                            "screens",
                            &crate::row::Table::of::<Screen>().columns,
                            &up.tables[0].upserts[0],
                        )
                        .unwrap();
                        (sqlx::query_as::<_, (i64, i64)>(
                            "SELECT level, revision FROM fixture.screens WHERE replica_id = $1",
                        )
                        .bind(replica)
                        .fetch_one(&pool)
                        .await
                        .unwrap()
                            == (screen.level, screen.revision))
                            == (*outcome == "set")
                    }
                    "provisioned" => scalar(resyncs).await == 1,
                    "restored" => {
                        sqlx::query_scalar::<_, bool>("SELECT epoch <> $1::uuid FROM sync.epoch")
                            .bind(EPOCH)
                            .fetch_one(&pool)
                            .await
                            .unwrap()
                    }
                    other => panic!("{name}: an outcome this suite does not know: {other}"),
                };
                assert!(holds, "{name}: not {outcome}");
            }
            if !outcomes.contains(&"provisioned") && !outcomes.contains(&"restored") {
                assert_eq!(
                    scalar(resyncs).await,
                    0,
                    "{name}: nothing provisioned again"
                );
            }
        }
    }
}
