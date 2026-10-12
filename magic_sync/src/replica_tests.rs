//! The replica conformance suite: a replica as a host runs it, driven by the
//! bytes the server sends — snapshots whole, empty and superseded, messages
//! redelivered or numbered as no server numbers them, bodies it does not
//! know, deletes the database refuses, versions of every type in either
//! order, every key shape, tables written on both sides, records — each
//! checked by what the replica then holds, what it reports, and that nothing
//! a message carries is ever an error. Then what goes up: numbered so that no
//! answer forgets a write it did not carry, within the transport's bytes, on
//! lanes that do not wait for one another.

use super::*;
use crate::conformance::{
    codecs, json, nonce_of, settled, Event, Price, Reading, Shelf, Task, Uplinked,
};
use crate::envelope::{
    Backlog, Down, EndSnapshot, Filed, Key as WireKey, Limits, Table, ToReplica,
};
use crate::run::replica::{Replica, Settings};
use crate::testkit::ItemRow;
use chrono::{TimeZone, Utc};
use sqlx::SqlitePool;
use std::collections::{BTreeMap, BTreeSet};

const TABLES: &str = "
CREATE TABLE items (id BLOB PRIMARY KEY NOT NULL, partition_id BLOB NOT NULL, name TEXT NOT NULL, retired_at TEXT);
CREATE TABLE shelves (aisle BLOB NOT NULL, slot INTEGER NOT NULL, label TEXT NOT NULL, revision INTEGER NOT NULL,
    PRIMARY KEY (aisle, slot));
CREATE TABLE prices (id BLOB PRIMARY KEY NOT NULL, cents INTEGER NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE tasks (id BLOB PRIMARY KEY NOT NULL, title TEXT NOT NULL, done INTEGER NOT NULL, updated_at TEXT NOT NULL,
    retired_at TEXT);
CREATE TABLE readings (replica_id BLOB NOT NULL, id BLOB NOT NULL, value INTEGER NOT NULL, at INTEGER NOT NULL,
    PRIMARY KEY (replica_id, id));
CREATE TABLE events (id BLOB PRIMARY KEY NOT NULL, what TEXT NOT NULL);
-- The replica's own rows, pointing at the server's: one key checked at once,
-- one at commit.
CREATE TABLE receipts (id INTEGER PRIMARY KEY, item_id BLOB NOT NULL REFERENCES items (id));
CREATE TABLE quotes (id INTEGER PRIMARY KEY,
    price_id BLOB NOT NULL REFERENCES prices (id) DEFERRABLE INITIALLY DEFERRED);
";

const EPOCH: &str = "0192f6a0-0000-7000-8000-000000000001";
const OTHER_EPOCH: &str = "0192f6a0-0000-7000-8000-000000000002";

struct World {
    pool: SqlitePool,
    me: Uuid,
    uplink: Uplinked,
    replica: Replica<(), Uuid, Uplinked>,
}

async fn world(pool: SqlitePool) -> World {
    world_with(pool, Settings::default()).await
}

async fn world_with(pool: SqlitePool, settings: Settings) -> World {
    world_over(pool, TABLES, settings).await
}

async fn world_over(pool: SqlitePool, tables: &str, settings: Settings) -> World {
    let pool = settled(pool).await;
    sqlx::raw_sql(tables).execute(&pool).await.expect("tables");
    install(&mut pool.acquire().await.unwrap(), &codecs())
        .await
        .expect("installed");
    let me = Uuid::now_v7();
    let uplink = Uplinked::default();
    let replica = Replica::new(pool.clone(), codecs(), me, uplink.clone(), settings)
        .await
        .expect("a replica");
    let w = World {
        pool,
        me,
        uplink,
        replica,
    };
    let provisioned = w.receive(&ending(BASE, 0, "provisioned", &[])).await;
    assert!(
        matches!(provisioned, Received::SnapshotEnded),
        "{provisioned:?}"
    );
    w
}

impl World {
    /// `bytes` received as a host receives them: nothing a message carries
    /// is an error.
    async fn receive(&self, bytes: &[u8]) -> Received {
        match self.replica.receive(bytes).await {
            Ok(received) => received,
            Err(e) => panic!("a message's data made an error, and would come again for ever: {e}"),
        }
    }

    async fn texts(&self, sql: &str) -> Vec<String> {
        sqlx::query_scalar(sql)
            .fetch_all(&self.pool)
            .await
            .expect(sql)
    }

    async fn count(&self, sql: &str) -> i64 {
        sqlx::query_scalar(sql)
            .fetch_one(&self.pool)
            .await
            .expect(sql)
    }

    async fn exec(&self, sql: &str) -> Result<(), sqlx::Error> {
        sqlx::raw_sql(sql).execute(&self.pool).await.map(|_| ())
    }

    /// The item names held and not retired.
    async fn items(&self) -> Vec<String> {
        self.texts("SELECT name FROM items WHERE retired_at IS NULL ORDER BY name")
            .await
    }

    /// The rows refused, as `table key`.
    async fn rejected(&self) -> Vec<String> {
        self.texts("SELECT table_name || ' ' || row_key FROM sync_rejections ORDER BY 1")
            .await
    }

    /// One message up on `lane`, as the uplink took it.
    async fn up(&self, lane: usize, digests: bool) -> Option<Up> {
        self.replica.send_lane(lane, digests).await.expect("sent");
        let mut taken = self.uplink.taken();
        assert!(taken.len() <= 1);
        taken.pop().map(|(_, up)| up)
    }
}

fn key(parts: &[&str]) -> WireKey {
    WireKey {
        parts: parts.iter().map(|p| (*p).to_owned()).collect(),
    }
}

fn item(id: Uuid, name: &str) -> (WireKey, Vec<u8>) {
    let row = ItemRow {
        id: id.to_string(),
        partition_id: Uuid::nil().to_string(),
        name: name.to_owned(),
    };
    (key(&[&id.to_string()]), row.encode_to_vec())
}

fn upserts(table: &str, rows: Vec<(WireKey, Vec<u8>)>) -> Table {
    let (keys, upserts) = rows.into_iter().unzip();
    Table {
        table: table.to_owned(),
        upserts,
        deletes: Vec::new(),
        keys,
    }
}

fn deletes(table: &str, keys: Vec<WireKey>) -> Table {
    Table {
        table: table.to_owned(),
        deletes: keys,
        ..Table::default()
    }
}

/// Where the suite's replica stands when a test begins: provisioned, by the
/// end of a snapshot of nothing, numbered so. A test's message `n` is the
/// server's `BASE + n`.
const BASE: u64 = 100;

/// Message `n` of a test, following message `n - 1` (or the provisioning).
fn down(n: u64, snapshot: &str, tables: Vec<Table>) -> Vec<u8> {
    numbered(BASE + n, BASE + n - 1, snapshot, tables)
}

/// Message `n`, beginning the snapshot `snapshot`: it follows nothing.
fn begin(n: u64, snapshot: &str, tables: Vec<Table>) -> Vec<u8> {
    numbered(BASE + n, 0, snapshot, tables)
}

/// The snapshot a test calls `name`, as a server would name it: a uuid, the
/// same for the same name.
fn snapshot_named(name: &str) -> String {
    if name.is_empty() {
        return String::new();
    }
    let spelled = name.bytes().fold(7u128, |sum, byte| {
        sum.wrapping_mul(31).wrapping_add(u128::from(byte))
    });
    Uuid::from_u128(spelled).to_string()
}

fn numbered(sequence: u64, previous: u64, snapshot: &str, tables: Vec<Table>) -> Vec<u8> {
    Body::Down(Down {
        snapshot_id: snapshot_named(snapshot),
        tables,
        sequence,
        epoch: EPOCH.to_owned(),
        previous,
        nonce: nonce_of(sequence),
        previous_nonce: if previous == 0 { 0 } else { nonce_of(previous) },
        mac: vec![0x5e; 16],
    })
    .encoded()
}

/// Message `n`, ending the snapshot in progress, `snapshot`.
fn end(n: u64, snapshot: &str, tables: &[&str]) -> Vec<u8> {
    ending(BASE + n, BASE + n - 1, snapshot, tables)
}

fn ending(sequence: u64, previous: u64, snapshot: &str, tables: &[&str]) -> Vec<u8> {
    Body::EndSnapshot(EndSnapshot {
        snapshot_id: snapshot_named(snapshot),
        tables: tables.iter().map(|t| (*t).to_owned()).collect(),
        sequence,
        epoch: EPOCH.to_owned(),
        previous,
        nonce: nonce_of(sequence),
        previous_nonce: if previous == 0 { 0 } else { nonce_of(previous) },
        mac: vec![0x5e; 16],
    })
    .encoded()
}

fn ids<const N: usize>() -> [Uuid; N] {
    std::array::from_fn(|_| Uuid::now_v7())
}

fn at(seconds: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(1_800_000_000 + seconds, 0).unwrap()
}

// ---------------------------------------------------------------------------
// What the server really sends.

/// The replica the golden messages are for, as the golden file's header
/// says it is: the suite's, with a check of its own that refuses a shelf
/// labelled `refused`.
async fn golden_replica(pool: SqlitePool) -> World {
    let w = world(settled(pool).await).await;
    w.exec(
        "CREATE TRIGGER no_refused BEFORE UPDATE ON shelves WHEN NEW.label = 'refused' \
         BEGIN SELECT RAISE(ABORT, 'a label this replica refuses'); END",
    )
    .await
    .unwrap();
    w
}

/// What `w` holds, as the golden file's `holds` lines say it.
async fn golden_holds(w: &World) -> serde_json::Value {
    let all = |sql: &'static str| w.texts(sql);
    serde_json::json!({
        "items": all(
            "SELECT name || CASE WHEN retired_at IS NULL THEN '' ELSE ' retired' END \
             FROM items ORDER BY name",
        )
        .await,
        "shelves": all("SELECT label || ' ' || revision FROM shelves ORDER BY slot").await,
        "prices": all("SELECT CAST(cents AS TEXT) FROM prices").await,
        "tasks": all("SELECT title || ' ' || done FROM tasks").await,
        "rejected": w.rejected().await,
    })
}

/// The golden messages of `step`, delivered to `w` as the golden file says
/// — but `lost` ones — each made what its line says, and each but a gap
/// delivered again, changing nothing; then what the replica does of its own
/// after the step, as the file's header says.
async fn follow_golden(w: &World, messages: &[(String, String, Vec<u8>)], step: &str) {
    use crate::conformance::{REPLICA, UPLOAD};
    for (_, expected, bytes) in messages.iter().filter(|(s, _, _)| s == step) {
        if expected == "lost" {
            continue;
        }
        if step == "filed" {
            // The replica's own upload, sent under the golden number.
            w.exec(&format!(
                "INSERT INTO readings (replica_id, id, value, at) \
                 VALUES (x'{}', x'0192f6a0000070008000000000000e01', 7, 1); \
                 UPDATE sync_notes SET upload = {}, sent = writes, \
                   sent_at = '2000-01-01T00:00:00.000Z';",
                REPLICA.replace('-', ""),
                i64::try_from(UPLOAD).unwrap()
            ))
            .await
            .unwrap();
        }
        let got = w.receive(bytes).await;
        assert_eq!(outcome(&got), expected, "{step}: {got:?}");
        if expected != "gap" {
            let again = w.receive(bytes).await;
            assert!(
                matches!(again, Received::AlreadyApplied | Received::Filed),
                "{step}, delivered again: {again:?}"
            );
        }
    }
    if step == "rows" {
        // The replica's own column, kept through the server's edits — and
        // filed, so the server's deletes let the row go; and a row of its
        // own pointing at the server's.
        w.exec("UPDATE tasks SET done = 1").await.unwrap();
        let up = w.up(0, false).await.expect("the edit");
        w.receive(
            &Body::Filed(Filed {
                uploads: up.uploads,
            })
            .encoded(),
        )
        .await;
        w.exec(
            "INSERT INTO receipts (id, item_id) \
             VALUES (1, x'0192f6a0000070008000000000000a02')",
        )
        .await
        .unwrap();
    }
}

/// The steps of the golden messages, in order.
fn golden_steps(messages: &[(String, String, Vec<u8>)]) -> Vec<String> {
    let mut steps: Vec<String> = Vec::new();
    for (step, _, _) in messages {
        if steps.last() != Some(step) {
            steps.push(step.clone());
        }
    }
    steps
}

/// The server's own messages (`tests/golden`, captured from the postgres
/// side's relay) leave the replica holding, step by step, what the file
/// says — its encodings, its numbering, its snapshots whole, cut short and
/// superseded, a message lost on the way and the gap it leaves, a new
/// epoch, an answer to an upload, a version older than the one held, a
/// field it does not know — as a replica with a check of its own and a row
/// of its own pointing at the server's holds it: a row it refuses kept as
/// it was, one it points at retired. Every message delivered twice changes
/// nothing the second time.
#[sqlx::test(migrations = false)]
async fn the_servers_own_messages_leave_the_replica_holding_what_the_file_says(pool: SqlitePool) {
    use crate::conformance::{golden, holds, GOLDEN, NEXT_EPOCH};
    let w = golden_replica(pool).await;
    let text = std::fs::read_to_string(GOLDEN).expect("the golden messages");
    let messages = golden(&text);
    let expected: BTreeMap<String, serde_json::Value> = holds(&text).into_iter().collect();
    for step in golden_steps(&messages) {
        follow_golden(&w, &messages, &step).await;
        if step == "filed" {
            assert_eq!(w.count("SELECT count(*) FROM sync_notes").await, 0, "filed");
        }
        let held = golden_holds(&w).await;
        if std::env::var_os("MAGIC_SYNC_HOLDS").is_some() {
            println!("holds {step} {held}");
            continue;
        }
        let expected = expected.get(&step).expect("what it holds after the step");
        assert_eq!(&held, expected, "after {step}");
    }
    let applied: String = sqlx::query_scalar("SELECT epoch FROM sync_applied")
        .fetch_one(&w.pool)
        .await
        .unwrap();
    assert_eq!(applied, NEXT_EPOCH);
    assert!(w.rejected().await.is_empty(), "{:?}", w.rejected().await);
}

/// What the replica of the golden messages sends up, once it followed them
/// up to the upload's step, is the golden uploads the server's suite files:
/// rows of its own, keyed by it, and history, under a number of its own,
/// with what it refused and what it applied, sealed as the server sealed
/// it; then its digests; then the same with a seal the server never made.
/// `MAGIC_SYNC_BLESS=1` writes them anew.
#[sqlx::test(migrations = false)]
async fn the_golden_uploads_are_what_the_replica_sends(pool: SqlitePool) {
    use crate::conformance::{golden, hex, matches_golden, GOLDEN, GOLDEN_UP, REPLICA, UPLOAD};
    let w = golden_replica(pool).await;
    let text = std::fs::read_to_string(GOLDEN).expect("the golden messages");
    let messages = golden(&text);
    for step in golden_steps(&messages)
        .iter()
        .take_while(|step| *step != "filed")
    {
        follow_golden(&w, &messages, step).await;
    }
    w.exec(&format!(
        "INSERT INTO readings (replica_id, id, value, at) \
         VALUES (x'{}', x'0192f6a0000070008000000000000e01', 7, 1); \
         INSERT INTO events (id, what) VALUES (x'0192f6a0000070008000000000000e02', 'opened');",
        REPLICA.replace('-', "")
    ))
    .await
    .unwrap();
    let mut upload = w.up(0, false).await.expect("the upload");
    assert_eq!(upload.uploads.len(), 1);
    assert!(!upload.rejected.is_empty() && upload.sequence > 0);
    upload.uploads = vec![UPLOAD];
    let digests = w.up(0, true).await.expect("the digests");
    assert!(!digests.digests.is_empty());
    let mut forged = digests.clone();
    forged.mac = vec![0; forged.mac.len()];
    let header = "# What a replica sends up: `name outcomes hex` per message, `outcomes` what\n\
                  # the server makes of it — filed: its rows filed and answered; confirmed: what\n\
                  # it applied recorded; quarantined: what it refused set aside; agrees: its\n\
                  # digests compared, nothing provisioned; ignored: nothing it says of itself\n\
                  # taken. Generated by the replica's suite, from the golden messages up to\n\
                  # their `filed` step; filed by the server's (conformance::server) at that\n\
                  # step, whose answer is in it.\n";
    let lines = [
        ("upload", "filed,confirmed,quarantined", &upload),
        ("digests", "confirmed,agrees", &digests),
        ("forged", "ignored", &forged),
    ];
    let text: String = lines
        .iter()
        .map(|(name, outcomes, up)| format!("{name} {outcomes} {}\n", hex(&up.encode_to_vec())))
        .collect();
    matches_golden(GOLDEN_UP, &format!("{header}{text}"));
    assert_eq!(golden(&text).len(), 3);
}

/// What a replica made of a message, as the corpora name it.
fn outcome(received: &Received) -> &'static str {
    match received {
        Received::Applied(_) => "applied",
        Received::SnapshotEnded => "ended",
        Received::Filed => "filed",
        Received::AlreadyApplied => "already",
        Received::Gap => "gap",
        Received::Malformed(_) => "malformed",
    }
}

/// Every message no server sends — the corpus in `tests/golden` — is what
/// its line says to the replica: a malformed one acknowledged, nothing
/// applied, and the replica goes on from where it was.
#[sqlx::test(migrations = false)]
async fn every_message_of_the_malformed_corpus_is_what_its_line_says(pool: SqlitePool) {
    let pool = settled(pool).await;
    use crate::conformance::{golden, EPOCH as SERVER, MALFORMED};
    let w = world(pool).await;
    let provisioned = Body::EndSnapshot(EndSnapshot {
        snapshot_id: SERVER.to_owned(),
        sequence: 100,
        epoch: SERVER.to_owned(),
        nonce: nonce_of(100),
        ..EndSnapshot::default()
    })
    .encoded();
    w.receive(&provisioned).await;
    let corpus = golden(&std::fs::read_to_string(MALFORMED).expect("the corpus"));
    assert!(corpus.len() >= 20);
    let mut last = 100;
    for (case, expected, bytes) in corpus {
        let got = w.receive(&bytes).await;
        assert_eq!(outcome(&got), expected, "{case}: {got:?}");
        let applied: (String, i64) = sqlx::query_as("SELECT epoch, sequence FROM sync_applied")
            .fetch_one(&w.pool)
            .await
            .unwrap();
        if expected == "malformed" {
            assert_eq!(
                applied,
                (SERVER.to_owned(), last),
                "{case}: nothing applied"
            );
        }
        last = applied.1;
    }
    let last = u64::try_from(last).unwrap();
    let next = Body::Down(Down {
        tables: vec![upserts("items", vec![item(Uuid::now_v7(), "after")])],
        sequence: last + 1,
        previous: last,
        epoch: SERVER.to_owned(),
        nonce: nonce_of(last + 1),
        previous_nonce: nonce_of(last),
        ..Down::default()
    })
    .encoded();
    assert!(matches!(w.receive(&next).await, Received::Applied(_)));
}

// ---------------------------------------------------------------------------
// Snapshots.

#[sqlx::test(migrations = false)]
async fn a_snapshot_keeps_what_it_names_and_lets_go_of_the_rest(pool: SqlitePool) {
    let w = world(pool).await;
    let [old, a, b] = ids();
    w.receive(&down(1, "", vec![upserts("items", vec![item(old, "old")])]))
        .await;

    let s = Uuid::now_v7().to_string();
    w.receive(&begin(2, &s, vec![upserts("items", vec![item(a, "a")])]))
        .await;
    w.receive(&down(3, &s, vec![upserts("items", vec![item(b, "b")])]))
        .await;
    assert_eq!(
        w.items().await,
        ["a", "b", "old"],
        "nothing goes before the end"
    );
    let ended = w.receive(&end(4, &s, &["items"])).await;
    assert!(matches!(ended, Received::SnapshotEnded), "{ended:?}");
    assert_eq!(w.items().await, ["a", "b"]);
}

/// A snapshot of nothing is only its end: every row of its tables goes.
#[sqlx::test(migrations = false)]
async fn a_snapshot_that_names_no_row_lets_go_of_every_row(pool: SqlitePool) {
    let w = world(pool).await;
    let [a] = ids();
    w.receive(&down(1, "", vec![upserts("items", vec![item(a, "a")])]))
        .await;
    w.receive(&ending(
        BASE + 2,
        0,
        &Uuid::now_v7().to_string(),
        &["items"],
    ))
    .await;
    assert!(w.items().await.is_empty());
}

/// A table a snapshot covers and names no row of is emptied, while the
/// tables it named rows of keep them.
#[sqlx::test(migrations = false)]
async fn a_table_a_snapshot_names_no_row_of_is_emptied(pool: SqlitePool) {
    let w = world(pool).await;
    let [a, aisle] = ids();
    let shelf = Shelf {
        aisle,
        slot: 1,
        label: "top".into(),
        revision: 1,
    };
    let shelf_key = key(&[&aisle.to_string(), "1"]);
    w.receive(&down(
        1,
        "",
        vec![upserts("shelves", vec![(shelf_key, json(&shelf))])],
    ))
    .await;
    let s = Uuid::now_v7().to_string();
    w.receive(&begin(2, &s, vec![upserts("items", vec![item(a, "a")])]))
        .await;
    w.receive(&end(3, &s, &["items", "shelves"])).await;
    assert_eq!(w.items().await, ["a"]);
    assert_eq!(w.count("SELECT count(*) FROM shelves").await, 0);
}

/// A message that follows nothing begins a snapshot, and supersedes the one
/// in progress, whatever their ids say; a message of the superseded one is
/// one no server sends after it, and the end of the newer sweeps what only
/// the older named.
#[sqlx::test(migrations = false)]
async fn a_snapshot_begun_supersedes_the_one_in_progress(pool: SqlitePool) {
    let w = world(pool).await;
    let [a, b] = ids();
    let newer_id = Uuid::now_v7().to_string();
    let older_id = Uuid::now_v7().to_string();
    // The later id begins first: an id's clock says nothing of the order.
    w.receive(&begin(
        1,
        &older_id,
        vec![upserts("items", vec![item(a, "a")])],
    ))
    .await;
    w.receive(&begin(
        2,
        &newer_id,
        vec![upserts("items", vec![item(b, "b")])],
    ))
    .await;
    let stale = w.receive(&end(3, &older_id, &["items"])).await;
    assert!(matches!(stale, Received::Malformed(_)), "{stale:?}");
    assert_eq!(
        w.items().await,
        ["a", "b"],
        "the superseded end sweeps nothing"
    );
    w.receive(&end(3, &newer_id, &["items"])).await;
    assert_eq!(w.items().await, ["b"]);
}

/// An end delivered again changes nothing, and a message of a snapshot
/// that ended is one no server sends.
#[sqlx::test(migrations = false)]
async fn an_end_delivered_again_changes_nothing(pool: SqlitePool) {
    let w = world(pool).await;
    let [a, b] = ids();
    let s = Uuid::now_v7().to_string();
    w.receive(&begin(1, &s, vec![upserts("items", vec![item(a, "a")])]))
        .await;
    w.receive(&end(2, &s, &["items"])).await;
    let again = w.receive(&end(2, &s, &["items"])).await;
    assert!(matches!(again, Received::AlreadyApplied), "{again:?}");
    let late = w
        .receive(&down(3, &s, vec![upserts("items", vec![item(b, "b")])]))
        .await;
    assert!(matches!(late, Received::Malformed(_)), "{late:?}");
    assert_eq!(w.items().await, ["a"]);
}

/// A server installed again begins with a snapshot, which the replica
/// follows whatever came before; a message of that epoch that follows one
/// the replica never applied is a gap.
#[sqlx::test(migrations = false)]
async fn a_new_epoch_begins_with_a_snapshot(pool: SqlitePool) {
    let w = world(pool).await;
    let [a, b] = ids();
    let s = Uuid::now_v7().to_string();
    w.receive(&begin(1, &s, vec![upserts("items", vec![item(a, "a")])]))
        .await;
    let other = |bytes: Vec<u8>| {
        let mut message = ToReplica::decode(bytes.as_slice()).unwrap();
        match &mut message.body {
            Some(Body::Down(m)) => m.epoch = OTHER_EPOCH.to_owned(),
            Some(Body::EndSnapshot(m)) => m.epoch = OTHER_EPOCH.to_owned(),
            _ => {}
        }
        message.encode_to_vec()
    };
    let followed = w
        .receive(&other(down(
            2,
            "",
            vec![upserts("items", vec![item(b, "b")])],
        )))
        .await;
    assert!(matches!(followed, Received::Gap), "{followed:?}");
    let t = Uuid::now_v7().to_string();
    w.receive(&other(ending(5, 0, &t, &["items"]))).await;
    assert!(w.items().await.is_empty(), "named nothing in this epoch");
    let applied: (String, i64) = sqlx::query_as("SELECT epoch, sequence FROM sync_applied")
        .fetch_one(&w.pool)
        .await
        .unwrap();
    assert_eq!(applied, (OTHER_EPOCH.to_owned(), 5));
}

/// What the replica refused of a table it holds from no server, under a
/// server that is gone, is forgotten once a server installed again has
/// provisioned it: nothing would ever report it, and no snapshot names the
/// table to clear it.
#[sqlx::test(migrations = false)]
async fn a_rejection_of_an_unnamed_table_under_a_gone_server_is_cleared(pool: SqlitePool) {
    let w = world(pool).await;
    let a = Uuid::now_v7();
    w.receive(&down(1, "", vec![upserts("mystery", vec![item(a, "x")])]))
        .await;
    assert_eq!(w.rejected().await, [format!("mystery {a}")]);
    let mut provision =
        ToReplica::decode(ending(5, 0, &Uuid::now_v7().to_string(), &["items"]).as_slice())
            .unwrap();
    let Some(Body::EndSnapshot(end)) = &mut provision.body else {
        panic!("an end");
    };
    end.epoch = OTHER_EPOCH.to_owned();
    let ended = w.receive(&provision.encode_to_vec()).await;
    assert!(matches!(ended, Received::SnapshotEnded), "{ended:?}");
    assert!(w.rejected().await.is_empty());
}

/// A message that follows one the replica never applied — lost on the way,
/// or a replica restored from an older copy — is a gap, and so is every
/// message after it until a snapshot begins, even one that follows the last
/// applied: nothing is applied meanwhile. The replica says the newest
/// message it could not follow, once, and again whenever that grows; then
/// it follows the snapshot.
#[sqlx::test(migrations = false)]
async fn a_replica_that_missed_a_message_applies_nothing_until_a_snapshot(pool: SqlitePool) {
    let w = world(pool).await;
    let [a, b, c] = ids();
    w.receive(&down(1, "", vec![upserts("items", vec![item(a, "a")])]))
        .await;
    w.up(0, false).await;
    // Message 2 is lost.
    let after = w
        .receive(&down(3, "", vec![upserts("items", vec![item(b, "b")])]))
        .await;
    assert!(matches!(after, Received::Gap), "{after:?}");
    let next = w
        .receive(&down(4, "", vec![upserts("items", vec![item(c, "c")])]))
        .await;
    assert!(matches!(next, Received::Gap), "{next:?}");
    assert_eq!(w.items().await, ["a"]);
    let told = w.up(0, false).await.expect("the gap said");
    assert_eq!(told.gap, BASE + 4, "the newest it could not follow");
    assert_eq!(
        (told.epoch.as_str(), told.sequence, told.nonce),
        (EPOCH, BASE + 1, nonce_of(BASE + 1))
    );
    assert!(w.up(0, false).await.is_none(), "said once");
    // One that follows the last applied: still nothing until a snapshot.
    let chained = w
        .receive(&numbered(
            BASE + 5,
            BASE + 1,
            "",
            vec![upserts("items", vec![item(c, "c")])],
        ))
        .await;
    assert!(matches!(chained, Received::Gap), "{chained:?}");
    assert_eq!(w.items().await, ["a"]);
    let grown = w.up(0, false).await.expect("said again: it grew");
    assert_eq!(grown.gap, BASE + 5);
    let s = Uuid::now_v7().to_string();
    w.receive(&begin(6, &s, vec![upserts("items", vec![item(c, "c")])]))
        .await;
    w.receive(&end(7, &s, &["items"])).await;
    assert_eq!(w.items().await, ["c"]);
    let up = w.up(0, false).await.expect("what it applied");
    assert_eq!(up.gap, 0);
    assert_eq!((up.sequence, up.nonce), (BASE + 7, nonce_of(BASE + 7)));
}

/// A message whose number is the last applied's, of the same epoch, and
/// whose nonce is another — a server restored from an older copy, numbering
/// again — is not taken: the replica says again what it applied, nonce and
/// all, so the server finds out. And what follows it does not follow the
/// last applied.
#[sqlx::test(migrations = false)]
async fn a_message_numbered_again_with_another_nonce_is_not_taken(pool: SqlitePool) {
    let w = world(pool).await;
    let [a, b] = ids();
    w.receive(&down(1, "", vec![upserts("items", vec![item(a, "a")])]))
        .await;
    w.up(0, false).await.expect("what it applied");
    let again = Body::Down(Down {
        tables: vec![upserts("items", vec![item(b, "restored")])],
        sequence: BASE + 1,
        epoch: EPOCH.to_owned(),
        previous: BASE,
        nonce: 7,
        previous_nonce: nonce_of(BASE),
        ..Down::default()
    })
    .encoded();
    assert!(matches!(w.receive(&again).await, Received::AlreadyApplied));
    let told = w.up(0, false).await.expect("said again");
    assert_eq!((told.sequence, told.nonce), (BASE + 1, nonce_of(BASE + 1)));
    let following = Body::Down(Down {
        tables: vec![upserts("items", vec![item(b, "restored")])],
        sequence: BASE + 2,
        epoch: EPOCH.to_owned(),
        previous: BASE + 1,
        nonce: 8,
        previous_nonce: 7,
        ..Down::default()
    })
    .encoded();
    assert!(matches!(w.receive(&following).await, Received::Gap));
    assert_eq!(w.items().await, ["a"]);
}

// ---------------------------------------------------------------------------
// Numbers, versions, bodies: what a replica does with every message.

#[sqlx::test(migrations = false)]
async fn a_message_at_or_below_the_last_applied_changes_nothing(pool: SqlitePool) {
    let w = world(pool).await;
    let [a, b, c] = ids();
    w.receive(&down(1, "", vec![upserts("items", vec![item(a, "one")])]))
        .await;
    w.receive(&down(2, "", vec![upserts("items", vec![item(b, "two")])]))
        .await;
    let past = w
        .receive(&down(1, "", vec![upserts("items", vec![item(c, "again")])]))
        .await;
    assert!(matches!(past, Received::AlreadyApplied), "{past:?}");
    assert_eq!(w.items().await, ["one", "two"]);
}

/// Numbers no server sends — 0, past 2^63 - 1, a message that follows
/// itself or a later one, one that follows nothing and begins no snapshot —
/// and an epoch that is no uuid, or no protocol, make the message
/// malformed: acknowledged, and nothing applied.
#[sqlx::test(migrations = false)]
async fn a_message_numbered_as_no_server_numbers_one_is_malformed(pool: SqlitePool) {
    let w = world(pool).await;
    let [a, b, c] = ids();
    let max = u64::try_from(i64::MAX).unwrap();
    for (sequence, previous) in [
        (0, 0),
        (max + 1, BASE),
        (u64::MAX, BASE),
        (BASE + 1, BASE + 1),
        (BASE + 1, BASE + 2),
        (BASE + 1, 0),
    ] {
        let got = w
            .receive(&numbered(
                sequence,
                previous,
                "",
                vec![upserts("items", vec![item(a, "a")])],
            ))
            .await;
        assert!(
            matches!(got, Received::Malformed(_)),
            "{sequence} after {previous}: {got:?}"
        );
    }
    let no_epoch = Body::Down(Down {
        tables: vec![upserts("items", vec![item(b, "b")])],
        sequence: BASE + 1,
        previous: BASE,
        epoch: "e".into(),
        ..Down::default()
    })
    .encoded();
    assert!(matches!(w.receive(&no_epoch).await, Received::Malformed(_)));
    let mut no_protocol = ToReplica::decode(down(1, "", Vec::new()).as_slice()).unwrap();
    no_protocol.protocol = 0;
    assert!(matches!(
        w.receive(&no_protocol.encode_to_vec()).await,
        Received::Malformed(_)
    ));
    assert!(w.items().await.is_empty());
    // The largest number is one.
    w.receive(&numbered(
        max,
        BASE,
        "",
        vec![upserts("items", vec![item(c, "c")])],
    ))
    .await;
    assert_eq!(w.items().await, ["c"]);
}

/// What a message names is bounded before it is stored or logged: an epoch
/// or a snapshot that is no uuid is malformed whatever its length, and what
/// the replica says of it is short.
#[sqlx::test(migrations = false)]
async fn an_epoch_or_snapshot_no_server_would_name_is_malformed_and_short(pool: SqlitePool) {
    let w = world(pool).await;
    let huge = "x".repeat(1 << 20);
    let message = |epoch: &str, snapshot: &str, previous: u64| {
        Body::Down(Down {
            snapshot_id: snapshot.to_owned(),
            tables: vec![upserts("items", vec![item(Uuid::now_v7(), "a")])],
            sequence: BASE + 1,
            epoch: epoch.to_owned(),
            previous,
            nonce: nonce_of(BASE + 1),
            previous_nonce: if previous == 0 { 0 } else { nonce_of(previous) },
            mac: vec![0x5e; 16],
        })
        .encoded()
    };
    for (bytes, what) in [
        (message(&huge, "", BASE), "epoch"),
        (message(EPOCH, &huge, 0), "snapshot"),
        (message(EPOCH, "snapshot", 0), "snapshot"),
    ] {
        let Received::Malformed(why) = w.receive(&bytes).await else {
            panic!("{what}: taken");
        };
        assert!(why.len() < 600, "{what}: {} bytes", why.len());
    }
    assert!(w.items().await.is_empty());
    assert_eq!(
        w.count("SELECT count(*) FROM sync_applied WHERE snapshot_id IS NOT NULL")
            .await,
        0
    );
}

/// A body this build does not know is malformed in its own protocol, and
/// waits for an update in a later one; bytes that are no message at all are
/// malformed.
#[sqlx::test(migrations = false)]
async fn a_body_this_build_does_not_know_waits_only_in_a_later_protocol(pool: SqlitePool) {
    let w = world(pool).await;
    // protocol = 1, then field 9 (a body a later build might send).
    let unknown = |protocol: u8| vec![0x08, protocol, 0x4a, 0x02, 0x08, 0x01];
    let ours = w.receive(&unknown(1)).await;
    assert!(matches!(ours, Received::Malformed(_)), "{ours:?}");
    let later = w.replica.receive(&unknown(2)).await;
    assert!(
        matches!(later, Err(ref e @ Error::Protocol { theirs: 2, .. }) if e.is_transient()),
        "{later:?}"
    );
    assert!(matches!(
        w.receive(&[0xff, 0xff, 0xff]).await,
        Received::Malformed(_)
    ));
}

/// Whatever bytes arrive — a real message cut short, a byte flipped —
/// receiving them is never an error, and leaves the replica able to go on.
#[sqlx::test(migrations = false)]
async fn no_bytes_make_receiving_an_error(pool: SqlitePool) {
    let w = world(pool).await;
    let [a, b, aisle] = ids();
    let shelf = Shelf {
        aisle,
        slot: 3,
        label: "x".into(),
        revision: 2,
    };
    let message = begin(
        7,
        &Uuid::now_v7().to_string(),
        vec![
            upserts("items", vec![item(a, "a")]),
            upserts(
                "shelves",
                vec![(key(&[&aisle.to_string(), "3"]), json(&shelf))],
            ),
            deletes("items", vec![key(&[&b.to_string()])]),
        ],
    );
    let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
    let mut next = |below: usize| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        usize::try_from(seed % below as u64).unwrap()
    };
    for round in 0..300 {
        let mut bytes = message.clone();
        match round % 3 {
            0 => bytes.truncate(next(bytes.len())),
            1 => {
                let at = next(bytes.len());
                bytes[at] ^= 1 << next(8);
            }
            _ => {
                let at = next(bytes.len());
                bytes[at] = u8::try_from(next(256)).unwrap();
            }
        }
        match w.replica.receive(&bytes).await {
            Ok(_) | Err(Error::Protocol { .. }) => {}
            Err(e) => panic!("round {round}: {e}"),
        }
    }
    // Whatever the rounds left it at, a snapshot that begins is followed.
    w.receive(&numbered(
        u64::MAX >> 1,
        0,
        "after",
        vec![upserts("items", vec![item(b, "after")])],
    ))
    .await;
    assert!(w.items().await.contains(&"after".to_owned()));
}

/// What the server sends is taken whatever its version, an older one
/// included — a server restored from a copy, or whose clock was set back,
/// sends what it has, and the chain already orders its messages — for an
/// integer version and for an instant.
#[sqlx::test(migrations = false)]
async fn the_servers_version_is_taken_older_or_newer_whatever_its_type(pool: SqlitePool) {
    let w = world(pool).await;
    let [aisle, price] = ids();
    let shelf = |revision: i64, label: &str| {
        let row = Shelf {
            aisle,
            slot: 1,
            label: label.into(),
            revision,
        };
        upserts(
            "shelves",
            vec![(key(&[&aisle.to_string(), "1"]), json(&row))],
        )
    };
    let price_at = |seconds: i64, cents: i64| {
        let row = Price {
            id: price,
            cents,
            updated_at: at(seconds),
        };
        upserts("prices", vec![(key(&[&price.to_string()]), json(&row))])
    };
    let mut sequence = 0;
    let mut send = |table: Table| {
        sequence += 1;
        down(sequence, "", vec![table])
    };
    let label = "SELECT label FROM shelves";
    let cents = "SELECT CAST(cents AS TEXT) FROM prices";

    // Versions as a host keeps them: milliseconds since the epoch.
    let ms = 1_800_000_000_000;
    w.receive(&send(shelf(ms + 5, "five"))).await;
    w.receive(&send(shelf(ms + 3, "three"))).await;
    assert_eq!(w.texts(label).await, ["three"], "older: taken");
    w.receive(&send(shelf(ms + 10, "ten"))).await;
    assert_eq!(w.texts(label).await, ["ten"], "newer: taken");
    w.receive(&send(shelf(ms + 10, "ten again"))).await;
    assert_eq!(w.texts(label).await, ["ten again"], "equal: taken");

    w.receive(&send(price_at(60, 600))).await;
    w.receive(&send(price_at(30, 300))).await;
    assert_eq!(w.texts(cents).await, ["300"]);
    w.receive(&send(price_at(61, 610))).await;
    assert_eq!(w.texts(cents).await, ["610"]);
}

/// Keys of every shape — two parts, an integer among them — are taken, let
/// go of and swept by what they are, and written in one form wherever they
/// are compared: a uuid that came uppercase or without hyphens included.
#[sqlx::test(migrations = false)]
async fn keys_of_every_shape_are_read_and_written_in_one_form(pool: SqlitePool) {
    let w = world(pool).await;
    let [aisle, a] = ids();
    let shelf = |slot: i64| {
        let row = Shelf {
            aisle,
            slot,
            label: format!("slot {slot}"),
            revision: 1,
        };
        (key(&[&aisle.to_string(), &slot.to_string()]), json(&row))
    };
    w.receive(&down(
        1,
        "",
        vec![upserts("shelves", vec![shelf(1), shelf(2), shelf(3)])],
    ))
    .await;
    let shouted = aisle.simple().to_string().to_uppercase();
    w.receive(&down(
        2,
        "",
        vec![deletes("shelves", vec![key(&[&shouted, "+2"])])],
    ))
    .await;
    assert_eq!(
        w.texts("SELECT label FROM shelves ORDER BY slot").await,
        ["slot 1", "slot 3"]
    );
    let s = Uuid::now_v7().to_string();
    w.receive(&begin(3, &s, vec![upserts("shelves", vec![shelf(3)])]))
        .await;
    w.receive(&end(4, &s, &["shelves"])).await;
    assert_eq!(w.texts("SELECT label FROM shelves").await, ["slot 3"]);
    // A row that does not decode is reported by its key, in that one form.
    let broken = upserts("items", vec![(key(&[&a.simple().to_string()]), vec![0xff])]);
    w.receive(&down(5, "", vec![broken])).await;
    assert_eq!(w.rejected().await, [format!("items {a}")]);
    let up = w.up(0, true).await.expect("digests");
    let shelves = up.digests.iter().find(|d| d.table == "shelves").unwrap();
    assert_eq!(
        shelves.fingerprint,
        envelope::fingerprint([format!("{aisle}|3")])
    );
}

// ---------------------------------------------------------------------------
// Rows the replica cannot take, or let go of.

#[sqlx::test(migrations = false)]
async fn a_row_the_replica_cannot_take_is_reported_until_it_takes_it(pool: SqlitePool) {
    let w = world(pool).await;
    let [good, broken] = ids();
    w.receive(&down(
        1,
        "",
        vec![upserts(
            "items",
            vec![
                item(good, "good"),
                (key(&[&broken.to_string()]), vec![0xff, 0xff]),
            ],
        )],
    ))
    .await;
    assert_eq!(w.items().await, ["good"]);
    assert_eq!(w.rejected().await, [format!("items {broken}")]);
    let up = w.up(0, false).await.expect("the refusal");
    assert_eq!(up.rejected.len(), 1);
    assert_eq!(up.rejected[0].key, Some(key(&[&broken.to_string()])));
    assert_eq!(up.sequence, BASE + 1, "confirmed with its refusal");
    assert!(w.up(0, false).await.is_none(), "said once");

    w.receive(&down(
        2,
        "",
        vec![upserts("items", vec![item(broken, "fixed")])],
    ))
    .await;
    assert!(w.rejected().await.is_empty());
    assert_eq!(w.items().await, ["fixed", "good"]);
}

#[sqlx::test(migrations = false)]
async fn a_table_this_build_does_not_mirror_is_rejected_row_by_row(pool: SqlitePool) {
    let w = world(pool).await;
    let [a, b] = ids();
    w.receive(&down(
        1,
        "",
        vec![upserts("newer", vec![item(a, "a"), item(b, "b")])],
    ))
    .await;
    assert_eq!(
        w.rejected().await,
        BTreeSet::from([format!("newer {a}"), format!("newer {b}")])
            .into_iter()
            .collect::<Vec<_>>()
    );
    let up = w.up(0, false).await.expect("the rejections");
    assert_eq!(up.rejected.len(), 2);
    assert_eq!(up.sequence, BASE + 1, "confirmed once reported");
    assert!(
        w.rejected().await.is_empty(),
        "no snapshot clears a table this build does not hold: forgotten once told"
    );
}

/// What a server says is kept within bounds, whatever it says: a reason is
/// cut, a table this build does not know named or keyed longer than any
/// table is not kept, and a rejection is told however long its reason was.
#[sqlx::test(migrations = false)]
async fn what_a_server_says_is_kept_within_bounds(pool: SqlitePool) {
    let w = world(pool).await;
    w.exec(&format!(
        "CREATE TRIGGER wordy BEFORE INSERT ON items BEGIN SELECT RAISE(ABORT, '{}'); END",
        "é".repeat(5000)
    ))
    .await
    .unwrap();
    let [a, b] = ids();
    let long = "t".repeat(10_000);
    let mut keyed = upserts("unknown", vec![item(b, "b")]);
    keyed.keys[0].parts[0] = "k".repeat(10_000);
    w.receive(&down(
        1,
        "",
        vec![
            upserts("items", vec![item(a, "a")]),
            upserts(&long, vec![item(b, "b")]),
            keyed,
        ],
    ))
    .await;
    assert_eq!(w.rejected().await, [format!("items {a}")]);
    let longest = w
        .count("SELECT max(length(CAST(reason AS BLOB))) FROM sync_rejections")
        .await;
    assert!((500..=512).contains(&longest), "{longest}");
    let up = w.up(0, false).await.expect("the rejection");
    assert_eq!(
        (up.rejected.len(), up.sequence),
        (1, BASE + 1),
        "told, and the message confirmed"
    );
}

/// Shapes no server sends cost nothing: rows with keys not one per row are
/// a malformed message, applied not at all; a snapshot's end that names a
/// table again and again sweeps it once.
#[sqlx::test(migrations = false)]
async fn shapes_no_server_sends_cost_nothing(pool: SqlitePool) {
    let w = world(pool).await;
    let [a, b] = ids();
    let mut unkeyed = upserts("items", vec![item(a, "a"), item(b, "b")]);
    unkeyed.keys.pop();
    let received = w.receive(&down(1, "", vec![unkeyed])).await;
    assert!(matches!(received, Received::Malformed(_)), "{received:?}");
    assert!(w.items().await.is_empty());
    assert_eq!(
        w.count("SELECT sequence FROM sync_applied").await,
        i64::try_from(BASE).unwrap(),
        "not applied: the next one is a gap"
    );

    let s = Uuid::now_v7().to_string();
    w.receive(&begin(1, &s, vec![upserts("items", vec![item(a, "a")])]))
        .await;
    let tables = vec!["items"; 200_000];
    let started = std::time::Instant::now();
    assert!(matches!(
        w.receive(&end(2, &s, &tables)).await,
        Received::SnapshotEnded
    ));
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(w.items().await, ["a"]);
}

/// A row a trigger refuses — as a bug in this build would — is the only one
/// left out, and so is a delete it refuses: the rest of the message is
/// taken.
#[sqlx::test(migrations = false)]
async fn what_a_trigger_refuses_is_the_only_thing_rejected(pool: SqlitePool) {
    let w = world(pool).await;
    let [good, poison, kept, gone] = ids();
    w.receive(&down(
        1,
        "",
        vec![upserts(
            "items",
            vec![item(kept, "kept"), item(gone, "gone")],
        )],
    ))
    .await;
    w.exec(
        "CREATE TRIGGER a_bug BEFORE INSERT ON items WHEN NEW.name = 'poison' \
         BEGIN SELECT RAISE(ABORT, 'this build cannot take it'); END; \
         CREATE TRIGGER another BEFORE DELETE ON items WHEN OLD.name = 'kept' \
         BEGIN SELECT RAISE(ABORT, 'this build cannot let it go'); END;",
    )
    .await
    .unwrap();
    w.receive(&down(
        2,
        "",
        vec![
            upserts("items", vec![item(good, "good"), item(poison, "poison")]),
            deletes(
                "items",
                vec![key(&[&kept.to_string()]), key(&[&gone.to_string()])],
            ),
        ],
    ))
    .await;
    assert_eq!(w.items().await, ["good", "kept"]);
    assert_eq!(
        w.rejected().await,
        [format!("items {kept}"), format!("items {poison}")]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
    );
}

/// A row whose refusal rolls back the whole transaction — a trigger's
/// `RAISE(ROLLBACK)` — never fails the message for ever: the message is
/// applied again, row by row, without it, and it is rejected; so is a
/// delete, or a sweep, refused so.
#[sqlx::test(migrations = false)]
async fn a_row_that_rolls_back_the_whole_message_is_rejected_and_the_rest_taken(pool: SqlitePool) {
    let w = world(pool).await;
    let [good, poison, kept, gone, swept, stays] = ids();
    w.receive(&down(
        1,
        "",
        vec![upserts(
            "items",
            vec![item(kept, "kept"), item(gone, "gone"), item(swept, "swept")],
        )],
    ))
    .await;
    w.exec(
        "CREATE TRIGGER a_bug BEFORE INSERT ON items WHEN NEW.name = 'poison' \
         BEGIN SELECT RAISE(ROLLBACK, 'this build cannot take it'); END; \
         CREATE TRIGGER another BEFORE DELETE ON items WHEN OLD.name IN ('kept', 'swept') \
         BEGIN SELECT RAISE(ROLLBACK, 'this build cannot let it go'); END;",
    )
    .await
    .unwrap();
    let received = w
        .receive(&down(
            2,
            "",
            vec![
                upserts("items", vec![item(good, "good"), item(poison, "poison")]),
                deletes(
                    "items",
                    vec![key(&[&kept.to_string()]), key(&[&gone.to_string()])],
                ),
            ],
        ))
        .await;
    let Received::Applied(applied) = received else {
        panic!("{received:?}");
    };
    assert_eq!(applied[0].keys, [vec![crate::row::KeyPart::Uuid(good)]]);
    assert_eq!(w.items().await, ["good", "kept", "swept"]);
    let mut rejected = vec![format!("items {kept}"), format!("items {poison}")];
    rejected.sort();
    assert_eq!(w.rejected().await, rejected);
    let s = Uuid::now_v7().to_string();
    w.receive(&begin(
        3,
        &s,
        vec![upserts("items", vec![item(stays, "stays")])],
    ))
    .await;
    assert!(matches!(
        w.receive(&end(4, &s, &["items"])).await,
        Received::SnapshotEnded
    ));
    assert_eq!(w.items().await, ["kept", "stays", "swept"]);
    let mut rejected = vec![format!("items {kept}"), format!("items {swept}")];
    rejected.sort();
    assert_eq!(w.rejected().await, rejected, "the sweep's, since it began");
    let reasons = w.texts("SELECT reason FROM sync_rejections").await;
    assert!(
        reasons
            .iter()
            .all(|r| r.ends_with("this build cannot let it go")),
        "{reasons:?}"
    );
    assert_eq!(
        w.count("SELECT sequence FROM sync_applied").await,
        i64::try_from(BASE + 4).unwrap()
    );
}

/// A failure of the replica's own storage — the disk full in the middle of
/// a message — fails the message, whole: nothing of it applied, nothing
/// rejected, and it is taken when it comes again.
#[sqlx::test(migrations = false)]
async fn a_disk_full_while_applying_leaves_the_message_to_come_again(pool: SqlitePool) {
    let w = world(pool).await;
    let pages: i64 = w.count("PRAGMA page_count").await;
    let options = (*w.pool.connect_options())
        .clone()
        .pragma("max_page_count", (pages + 2).to_string());
    let full = SqlitePool::connect_with(options).await.unwrap();
    let cramped = Replica::new(full, codecs(), w.me, w.uplink.clone(), Settings::default())
        .await
        .unwrap();
    let rows: Vec<(WireKey, Vec<u8>)> = (0..2000)
        .map(|n| item(Uuid::now_v7(), &format!("{n:0>200}")))
        .collect();
    let message = down(1, "", vec![upserts("items", rows)]);
    match cramped.receive(&message).await {
        Err(e) => assert!(e.is_transient(), "{e}"),
        Ok(received) => panic!("applied on a full disk: {received:?}"),
    }
    assert_eq!(w.count("SELECT count(*) FROM items").await, 0);
    assert!(w.rejected().await.is_empty());
    assert_eq!(
        w.count("SELECT sequence FROM sync_applied").await,
        i64::try_from(BASE).unwrap()
    );
    w.receive(&message).await;
    assert_eq!(w.count("SELECT count(*) FROM items").await, 2000);
}

/// A message larger than one statement binds is written in as many as it
/// takes, never as rows a statement refused: rows, and deletes, past
/// SQLite's limit of variables.
#[sqlx::test(migrations = false)]
async fn a_message_past_a_statements_variables_is_taken_whole(pool: SqlitePool) {
    let w = world(pool).await;
    let ids: Vec<Uuid> = (0..crate::dialect::MAX_BINDS + 10)
        .map(|_| Uuid::now_v7())
        .collect();
    let rows = ids.iter().map(|id| item(*id, "n")).collect();
    w.receive(&down(1, "", vec![upserts("items", rows)])).await;
    assert_eq!(
        w.count("SELECT count(*) FROM items").await,
        i64::try_from(ids.len()).unwrap()
    );
    // Keyed by two columns: half as many keys a statement.
    let aisle = Uuid::now_v7();
    let shelves: Vec<(WireKey, Vec<u8>)> = (0..crate::dialect::MAX_BINDS / 2 + 10)
        .map(|slot| {
            let shelf = Shelf {
                aisle,
                slot: i64::try_from(slot).unwrap(),
                label: String::new(),
                revision: 1,
            };
            (key(&[&aisle.to_string(), &slot.to_string()]), json(&shelf))
        })
        .collect();
    let slots: Vec<WireKey> = shelves.iter().map(|(k, _)| k.clone()).collect();
    w.receive(&down(2, "", vec![upserts("shelves", shelves)]))
        .await;
    let gone = ids.iter().map(|id| key(&[&id.to_string()])).collect();
    w.receive(&down(
        3,
        "",
        vec![deletes("items", gone), deletes("shelves", slots)],
    ))
    .await;
    assert_eq!(w.count("SELECT count(*) FROM items").await, 0);
    assert_eq!(w.count("SELECT count(*) FROM shelves").await, 0);
    assert!(w.rejected().await.is_empty());
}

/// A row a row of the replica's own points at is retired where the table
/// can retire it, and otherwise rejected — whether the key pointing at it is
/// checked at once or at commit, the message commits.
#[sqlx::test(migrations = false)]
async fn a_row_still_pointed_at_is_retired_or_rejected_never_a_failed_message(pool: SqlitePool) {
    let w = world(pool).await;
    let [item_id, price_id, free] = ids();
    let price = Price {
        id: price_id,
        cents: 100,
        updated_at: at(0),
    };
    w.receive(&down(
        1,
        "",
        vec![
            upserts("items", vec![item(item_id, "pointed"), item(free, "free")]),
            upserts(
                "prices",
                vec![(key(&[&price_id.to_string()]), json(&price))],
            ),
        ],
    ))
    .await;
    sqlx::query("INSERT INTO receipts (item_id) VALUES ($1)")
        .bind(item_id)
        .execute(&w.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO quotes (price_id) VALUES ($1)")
        .bind(price_id)
        .execute(&w.pool)
        .await
        .unwrap();
    w.receive(&down(
        2,
        "",
        vec![
            deletes("prices", vec![key(&[&price_id.to_string()])]),
            deletes(
                "items",
                vec![key(&[&item_id.to_string()]), key(&[&free.to_string()])],
            ),
        ],
    ))
    .await;
    assert!(w.items().await.is_empty());
    assert_eq!(
        w.texts("SELECT name FROM items WHERE retired_at IS NOT NULL")
            .await,
        ["pointed"],
        "retired, still there for the receipt"
    );
    assert_eq!(w.count("SELECT count(*) FROM prices").await, 1);
    assert_eq!(w.rejected().await, [format!("prices {price_id}")]);
    // Sent again, the retired row is live again.
    w.receive(&down(
        3,
        "",
        vec![upserts("items", vec![item(item_id, "back")])],
    ))
    .await;
    assert_eq!(w.items().await, ["back"]);
}

/// A mirrored table whose foreign key is deferred fails the commit of a
/// message that sends a row without its parent, and that message would come
/// again for ever: the row is refused, as one the database refuses at once,
/// and the rest of the message is applied — however many rows are in it.
#[sqlx::test(migrations = false)]
async fn a_row_whose_deferred_parent_is_missing_is_refused_not_the_message(pool: SqlitePool) {
    let tables = TABLES.replace(
        "CREATE TABLE prices (id BLOB PRIMARY KEY NOT NULL,",
        "CREATE TABLE prices (id BLOB PRIMARY KEY NOT NULL \
         REFERENCES items (id) DEFERRABLE INITIALLY DEFERRED,",
    );
    let w = world_over(pool, &tables, Settings::default()).await;
    let parent = Uuid::from_u128(1);
    let price = |id: Uuid| {
        let row = Price {
            id,
            cents: 1,
            updated_at: at(0),
        };
        (key(&[&id.to_string()]), json(&row))
    };
    let orphans: Vec<Uuid> = (10..510).map(Uuid::from_u128).collect();
    let mut rows = vec![price(parent)];
    rows.extend(orphans.iter().map(|id| price(*id)));
    w.receive(&down(
        1,
        "",
        vec![
            upserts("items", vec![item(parent, "parent")]),
            upserts("prices", rows),
        ],
    ))
    .await;
    assert_eq!(
        w.texts("SELECT lower(hex(id)) FROM prices").await.len(),
        1,
        "the price with a parent"
    );
    assert_eq!(w.count("SELECT count(*) FROM sync_rejections").await, 500);
    assert_eq!(w.items().await, ["parent"]);
    // Sent again with its parent, a refused row is taken and no more reported.
    w.receive(&down(
        2,
        "",
        vec![
            upserts("items", vec![item(orphans[0], "late")]),
            upserts("prices", vec![price(orphans[0])]),
        ],
    ))
    .await;
    assert_eq!(w.count("SELECT count(*) FROM prices").await, 2);
    assert_eq!(w.count("SELECT count(*) FROM sync_rejections").await, 499);
}

/// A row the replica's own rows point at through a key that would cascade
/// or set them null is kept, retired, like one a key would refuse to let
/// go: letting it go would change rows of the replica's that the server
/// never sent.
#[sqlx::test(migrations = false)]
async fn a_row_let_go_never_changes_the_replicas_own_rows_through_their_keys(pool: SqlitePool) {
    let w = world(pool).await;
    w.exec(
        "CREATE TABLE tags (id INTEGER PRIMARY KEY, item_id BLOB REFERENCES items (id) ON DELETE CASCADE);
         CREATE TABLE marks (id INTEGER PRIMARY KEY, item_id BLOB REFERENCES items (id) ON DELETE SET NULL);
         CREATE TABLE stamps (id INTEGER PRIMARY KEY,
             item_id BLOB DEFAULT x'00' REFERENCES items (id) ON DELETE SET DEFAULT);",
    )
    .await
    .unwrap();
    let [tagged, marked, stamped, free] = ids();
    w.receive(&down(
        1,
        "",
        vec![upserts(
            "items",
            vec![
                item(tagged, "tagged"),
                item(marked, "marked"),
                item(stamped, "stamped"),
                item(free, "free"),
            ],
        )],
    ))
    .await;
    for (table, id) in [("tags", tagged), ("marks", marked), ("stamps", stamped)] {
        sqlx::query(&format!("INSERT INTO {table} (item_id) VALUES ($1)"))
            .bind(id)
            .execute(&w.pool)
            .await
            .unwrap();
    }
    let gone = [tagged, marked, stamped, free].map(|id| key(&[&id.to_string()]));
    w.receive(&down(2, "", vec![deletes("items", gone.to_vec())]))
        .await;
    assert_eq!(
        w.texts("SELECT name FROM items WHERE retired_at IS NOT NULL ORDER BY name")
            .await,
        ["marked", "stamped", "tagged"]
    );
    assert_eq!(
        w.count("SELECT count(*) FROM items").await,
        3,
        "free is gone"
    );
    let pointing = w
        .count(
            "SELECT (SELECT count(*) FROM tags WHERE item_id IS NOT NULL) \
                  + (SELECT count(*) FROM marks WHERE item_id IS NOT NULL) \
                  + (SELECT count(*) FROM stamps WHERE item_id <> x'00')",
        )
        .await;
    assert_eq!(pointing, 3, "the replica's own rows as they were");
    assert!(w.rejected().await.is_empty());
}

/// A row an `INSERT OR REPLACE` deletes to make room — another row with the
/// same value of a unique column — goes up as gone: the replica's
/// connections fire triggers on what a conflict replaces.
#[sqlx::test(migrations = false)]
async fn a_row_a_replace_deletes_goes_up_as_gone(pool: SqlitePool) {
    let w = world(pool).await;
    w.exec("CREATE UNIQUE INDEX one_reading_a_value ON readings (value)")
        .await
        .unwrap();
    let [first, second] = ids();
    for (id, at) in [(first, 1), (second, 2)] {
        sqlx::query(
            "INSERT OR REPLACE INTO readings (replica_id, id, value, at) VALUES ($1, $2, 7, $3)",
        )
        .bind(w.me)
        .bind(id)
        .bind(at)
        .execute(&w.pool)
        .await
        .unwrap();
    }
    let up = w.up(0, false).await.expect("the writes");
    let readings = up.tables.iter().filter(|t| t.table == "readings");
    let deleted: Vec<String> = readings
        .clone()
        .flat_map(|t| t.deletes.iter().map(|k| k.parts.join("|")))
        .collect();
    let written: Vec<String> = readings
        .flat_map(|t| t.keys.iter().map(|k| k.parts.join("|")))
        .collect();
    assert_eq!(deleted, [format!("{}|{first}", w.me)]);
    assert_eq!(written, [format!("{}|{second}", w.me)]);
}

/// Rejections from before a snapshot began are forgotten when it ends: it
/// sent again everything the replica should hold.
#[sqlx::test(migrations = false)]
async fn a_snapshot_forgets_the_rejections_from_before_it(pool: SqlitePool) {
    let w = world(pool).await;
    let [gone, still] = ids();
    w.receive(&down(
        1,
        "",
        vec![upserts(
            "items",
            vec![
                (key(&[&gone.to_string()]), vec![0xff]),
                (key(&[&still.to_string()]), vec![0xff]),
            ],
        )],
    ))
    .await;
    let s = Uuid::now_v7().to_string();
    w.receive(&begin(
        2,
        &s,
        vec![upserts(
            "items",
            vec![(key(&[&still.to_string()]), vec![0xff])],
        )],
    ))
    .await;
    w.receive(&end(3, &s, &["items"])).await;
    assert_eq!(w.rejected().await, [format!("items {still}")]);
    // Let go of by the server, it is no longer one to report.
    w.receive(&down(
        4,
        "",
        vec![deletes("items", vec![key(&[&still.to_string()])])],
    ))
    .await;
    assert!(w.rejected().await.is_empty());
}

/// A snapshot names every row it carried, the ones the replica refused as
/// well: one it held keeps its old copy through the sweep, and its
/// rejection stands, reported with the message that refused it.
#[sqlx::test(migrations = false)]
async fn a_snapshot_keeps_a_row_it_carried_and_the_replica_refused(pool: SqlitePool) {
    let w = world(pool).await;
    let [a] = ids();
    w.receive(&down(1, "", vec![upserts("items", vec![item(a, "old")])]))
        .await;
    let s = Uuid::now_v7().to_string();
    let broken = (key(&[&a.to_string()]), vec![0xff, 0xff]);
    w.receive(&begin(2, &s, vec![upserts("items", vec![broken])]))
        .await;
    w.receive(&end(3, &s, &["items"])).await;
    assert_eq!(w.items().await, ["old"], "not swept");
    assert_eq!(
        w.rejected().await,
        [format!("items {a}")],
        "the rejection stands"
    );
    let up = w.up(0, false).await.expect("the refusal");
    assert_eq!(up.rejected.len(), 1);
    assert_eq!(
        (up.rejected[0].epoch.as_str(), up.rejected[0].sequence),
        (EPOCH, BASE + 2),
        "named by the message that refused it"
    );
}

// ---------------------------------------------------------------------------
// Tables written on both sides, and records.

/// A row of a two-way table begins on the server; the replica edits only
/// its own columns, and only those edits go up — the engine's own writes
/// note nothing.
#[sqlx::test(migrations = false)]
async fn a_two_way_row_keeps_the_replicas_columns_and_sends_only_its_edits(pool: SqlitePool) {
    let w = world(pool).await;
    let [id, other] = ids();
    let task = |title: &str, seconds: i64| {
        let row = Task {
            id,
            title: title.into(),
            done: false,
            updated_at: at(seconds),
        };
        upserts("tasks", vec![(key(&[&id.to_string()]), json(&row))])
    };
    w.receive(&down(1, "", vec![task("write", 0)])).await;
    assert_eq!(
        w.count("SELECT count(*) FROM sync_notes").await,
        0,
        "no echo"
    );
    w.up(0, false).await;

    sqlx::query("UPDATE tasks SET done = 1 WHERE id = $1")
        .bind(id)
        .execute(&w.pool)
        .await
        .unwrap();
    w.receive(&down(2, "", vec![task("rewrite", 10)])).await;
    assert_eq!(
        w.texts("SELECT title || ' ' || done FROM tasks").await,
        ["rewrite 1"]
    );

    let insert = sqlx::query(
        "INSERT INTO tasks (id, title, done, updated_at) VALUES ($1, 'mine', 0, '2026-01-01')",
    )
    .bind(other)
    .execute(&w.pool)
    .await;
    assert!(insert.is_err(), "a row of it begins on the server");

    let up = w.up(0, false).await.expect("the edit");
    assert_eq!(up.tables.len(), 1);
    let sent: serde_json::Value = serde_json::from_slice(&up.tables[0].upserts[0]).unwrap();
    assert_eq!(
        (&sent["title"], &sent["done"]),
        (&serde_json::json!("rewrite"), &serde_json::json!(true))
    );
    assert_eq!(up.tables[0].keys, [key(&[&id.to_string()])]);

    // The server's columns are its own, and so is letting a row go: the
    // replica's database refuses either.
    for refused in ["UPDATE tasks SET title = 'mine'", "DELETE FROM tasks"] {
        assert!(w.exec(refused).await.is_err(), "{refused}");
    }

    // Let go of while its edit is on its way: kept retired until the edit
    // is filed, then let go of.
    w.receive(&down(
        3,
        "",
        vec![deletes("tasks", vec![key(&[&id.to_string()])])],
    ))
    .await;
    assert_eq!(
        w.count("SELECT count(*) FROM tasks WHERE retired_at IS NOT NULL")
            .await,
        1,
        "held retired for its edit"
    );
    assert!(
        w.rejected().await.is_empty(),
        "let go of, as far as the server is told"
    );
    w.receive(
        &Body::Filed(Filed {
            uploads: up.uploads.clone(),
        })
        .encoded(),
    )
    .await;
    assert_eq!(
        w.count("SELECT count(*) FROM tasks").await,
        0,
        "filed: let go of"
    );
    assert_eq!(
        w.count("SELECT count(*) FROM sync_notes").await,
        0,
        "letting go of the server's row notes nothing"
    );
}

/// A record is the replica's: the server sends none of it and lets go of
/// none of it. History is written once; state is edited, and a changed key
/// goes up as the old one gone.
#[sqlx::test(migrations = false)]
async fn a_record_is_the_replicas_and_history_is_written_once(pool: SqlitePool) {
    let w = world(pool).await;
    let [event, reading, moved] = ids();
    let me = w.me;
    sqlx::query("INSERT INTO events (id, what) VALUES ($1, 'opened')")
        .bind(event)
        .execute(&w.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO readings (replica_id, id, value, at) VALUES ($1, $2, 1, 1)")
        .bind(me)
        .bind(reading)
        .execute(&w.pool)
        .await
        .unwrap();
    let edit = sqlx::query("UPDATE events SET what = 'closed' WHERE id = $1")
        .bind(event)
        .execute(&w.pool)
        .await;
    assert!(edit.is_err(), "history is written once");

    let theirs = Reading {
        replica_id: me,
        id: reading,
        value: 99,
        at: 99,
    };
    let reading_key = key(&[&me.to_string(), &reading.to_string()]);
    let s = Uuid::now_v7().to_string();
    w.receive(&begin(
        1,
        &s,
        vec![
            upserts("readings", vec![(reading_key.clone(), json(&theirs))]),
            deletes("events", vec![key(&[&event.to_string()])]),
        ],
    ))
    .await;
    w.receive(&end(2, &s, &["readings", "events"])).await;
    assert_eq!(w.count("SELECT count(*) FROM events").await, 1);
    assert_eq!(
        w.texts("SELECT CAST(value AS TEXT) FROM readings").await,
        ["1"]
    );
    assert_eq!(w.rejected().await, [format!("readings {me}|{reading}")]);

    sqlx::query("UPDATE readings SET id = $2, value = 2, at = 2 WHERE id = $1")
        .bind(reading)
        .bind(moved)
        .execute(&w.pool)
        .await
        .unwrap();
    let up = w.up(0, false).await.expect("the writes");
    let tables: Vec<(&str, usize, usize)> = up
        .tables
        .iter()
        .map(|t| (t.table.as_str(), t.upserts.len(), t.deletes.len()))
        .collect();
    assert_eq!(
        tables,
        [("readings", 1, 0), ("events", 1, 0), ("readings", 0, 1)],
        "upserts parents first, then deletes children first"
    );
    let gone = up.tables.iter().find(|t| !t.deletes.is_empty()).unwrap();
    assert_eq!(gone.deletes, [reading_key]);
}

/// A key stored in a form the engine does not read back is refused when it
/// is written, rather than going up as a row that seems gone.
#[sqlx::test(migrations = false)]
async fn a_key_stored_in_a_form_the_engine_cannot_read_is_refused(pool: SqlitePool) {
    let w = world(pool).await;
    let as_text = sqlx::query("INSERT INTO events (id, what) VALUES ($1, 'x')")
        .bind(Uuid::now_v7().to_string())
        .execute(&w.pool)
        .await;
    assert!(as_text.is_err());
    let as_blob = sqlx::query("INSERT INTO events (id, what) VALUES ($1, 'x')")
        .bind(Uuid::now_v7())
        .execute(&w.pool)
        .await;
    assert!(as_blob.is_ok());
    let theirs = w.exec("UPDATE items SET name = 'mine'").await;
    assert!(theirs.is_ok(), "no row, nothing refused");
    let [a] = ids();
    w.receive(&down(1, "", vec![upserts("items", vec![item(a, "a")])]))
        .await;
    assert!(
        w.exec("UPDATE items SET name = 'mine'").await.is_err(),
        "the server's rows are the server's"
    );
}

// ---------------------------------------------------------------------------
// What goes up.

#[sqlx::test(migrations = false)]
async fn what_the_replica_wrote_is_pending_until_the_server_filed_it(pool: SqlitePool) {
    let w = world(pool).await;
    for _ in 0..2 {
        sqlx::query("INSERT INTO events (id, what) VALUES ($1, 'x')")
            .bind(Uuid::now_v7())
            .execute(&w.pool)
            .await
            .unwrap();
    }
    let pending = |unsent, unfiled| {
        vec![UpProgress {
            table: "events".to_owned(),
            unsent,
            unfiled,
            set_aside: 0,
        }]
    };
    let mut conn = w.pool.acquire().await.unwrap();
    assert_eq!(progress(&mut conn).await.unwrap(), pending(2, 0));
    let up = w.up(0, false).await.expect("the rows");
    assert_eq!(up.tables[0].upserts.len(), 2);
    assert_eq!(progress(&mut conn).await.unwrap(), pending(0, 2));
    assert!(
        w.up(0, false)
            .await
            .and_then(|up| up.tables.first().cloned())
            .is_none(),
        "not sent again while the server may still answer"
    );
    let filed = w
        .receive(
            &Body::Filed(Filed {
                uploads: up.uploads.clone(),
            })
            .encoded(),
        )
        .await;
    assert!(matches!(filed, Received::Filed));
    assert!(progress(&mut conn).await.unwrap().is_empty());
}

/// Whatever the order uploads are sent, lost, sent again and answered in —
/// late answers, answers that overtake the transport's own, and answers
/// meant for another replica included — and however often a row is written
/// meanwhile, a row's note is forgotten only once the server filed an upload
/// that carried the row as its last write left it; and once the answers
/// come, every note is.
#[sqlx::test(migrations = false)]
async fn no_answer_forgets_a_write_it_did_not_carry(pool: SqlitePool) {
    let pool = settled(pool).await;
    let settings = Outbox {
        limits: Limits {
            rows: 3,
            bytes: 1 << 20,
        },
        resend_after: std::time::Duration::from_secs(3600),
        ..Outbox::default()
    };
    let lanes = Settings::default().lanes;
    let w = world(pool).await;
    for walk in 1..=5_u64 {
        w.exec("DELETE FROM sync_notes; DELETE FROM readings")
            .await
            .unwrap();
        forgets_nothing_unfiled(&w, &lanes, &settings, walk).await;
    }
}

/// The schedule that once lost a write: the first upload is lost, the
/// second carries only the newer write, both go again together, and the
/// second's late answer arrives.
#[sqlx::test(migrations = false)]
async fn a_late_answer_forgets_only_what_its_upload_carried(pool: SqlitePool) {
    let w = world(pool).await;
    let lanes = Settings::default().lanes;
    let settings = Outbox::default();
    let write =
        || sqlx::query("INSERT INTO events (id, what) VALUES ($1, 'x')").bind(Uuid::now_v7());
    let send = || async {
        let mut tx = w.pool.begin().await.unwrap();
        let outgoing = outbox(&mut tx, &codecs(), &lanes, 0, &settings, None, None)
            .await
            .unwrap()
            .unwrap();
        delivered(&mut tx, &outgoing).await.unwrap();
        tx.commit().await.unwrap();
        outgoing
    };
    write().execute(&w.pool).await.unwrap();
    let lost = send().await;
    write().execute(&w.pool).await.unwrap();
    let late = send().await;
    assert_eq!((lost.items(), late.items()), (1, 1));
    w.exec("UPDATE sync_notes SET sent_at = '2000-01-01T00:00:00.000Z'")
        .await
        .unwrap();
    assert_eq!(send().await.items(), 2, "both go again");
    w.receive(
        &Body::Filed(Filed {
            uploads: late.up.uploads.clone(),
        })
        .encoded(),
    )
    .await;
    assert_eq!(
        w.count("SELECT count(*) FROM sync_notes").await,
        2,
        "the answer to the second upload names neither as they were sent again"
    );
}

/// A row written many times is one note, and goes up once, as its last
/// write left it; one written again while its upload is on its way is not
/// forgotten by that upload's answer, and goes again.
#[sqlx::test(migrations = false)]
async fn a_row_written_often_goes_up_once_as_it_is_last(pool: SqlitePool) {
    let w = world(pool).await;
    let id = Uuid::now_v7();
    let w = &w;
    let write = |value: i64| {
        let sql = format!(
            "INSERT INTO readings (replica_id, id, value, at) VALUES (x'{}', x'{}', {value}, {value}) \
             ON CONFLICT (replica_id, id) DO UPDATE SET value = excluded.value, at = excluded.at",
            w.me.simple(),
            id.simple()
        );
        async move { w.exec(&sql).await }
    };
    for value in 1..=5 {
        write(value).await.unwrap();
    }
    assert_eq!(w.count("SELECT count(*) FROM sync_notes").await, 1);
    let up = w.up(0, false).await.expect("the row");
    assert_eq!(up.tables[0].upserts.len(), 1);
    let sent: serde_json::Value = serde_json::from_slice(&up.tables[0].upserts[0]).unwrap();
    assert_eq!(sent["value"], "5", "as its last write left it");
    write(6).await.unwrap();
    w.receive(
        &Body::Filed(Filed {
            uploads: up.uploads.clone(),
        })
        .encoded(),
    )
    .await;
    assert_eq!(
        w.count("SELECT count(*) FROM sync_notes").await,
        1,
        "written since: the answer does not forget it"
    );
    let again = w.up(0, false).await.expect("the row, again");
    let sent: serde_json::Value = serde_json::from_slice(&again.tables[0].upserts[0]).unwrap();
    assert_eq!(sent["value"], "6");
    assert_ne!(again.uploads, up.uploads, "a fresh number");
}

/// The server's answer may reach the replica before the replica records
/// that the transport took the upload: it is not lost, and nothing goes
/// again. An answer of a number no upload of this replica carries — meant
/// for another, or for this one before it was restored — forgets nothing.
#[sqlx::test(migrations = false)]
async fn an_answer_that_overtakes_the_send_or_is_not_ours_is_read_right(pool: SqlitePool) {
    let w = world(pool).await;
    let lanes = Settings::default().lanes;
    sqlx::query("INSERT INTO events (id, what) VALUES ($1, 'x')")
        .bind(Uuid::now_v7())
        .execute(&w.pool)
        .await
        .unwrap();
    let mut tx = w.pool.begin().await.unwrap();
    let outgoing = outbox(
        &mut tx,
        &codecs(),
        &lanes,
        0,
        &Outbox::default(),
        None,
        None,
    )
    .await
    .unwrap()
    .unwrap();
    tx.commit().await.unwrap();
    let number = outgoing.up.uploads[0];
    assert!(number > 0 && i64::try_from(number).is_ok());
    for stranger in [number ^ 1, number.wrapping_add(1), 1] {
        w.receive(
            &Body::Filed(Filed {
                uploads: vec![stranger],
            })
            .encoded(),
        )
        .await;
    }
    assert_eq!(
        w.count("SELECT count(*) FROM sync_notes").await,
        1,
        "none of ours"
    );
    w.receive(
        &Body::Filed(Filed {
            uploads: vec![number],
        })
        .encoded(),
    )
    .await;
    let mut tx = w.pool.begin().await.unwrap();
    delivered(&mut tx, &outgoing).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(w.count("SELECT count(*) FROM sync_notes").await, 0, "filed");
    assert!(
        w.up(0, false).await.is_none_or(|up| up.tables.is_empty()),
        "nothing goes again"
    );
}

/// The server's answer to the upload numbered `number`.
async fn answer(w: &World, number: u64) {
    w.receive(
        &Body::Filed(Filed {
            uploads: vec![number],
        })
        .encoded(),
    )
    .await;
}

/// One seeded walk of [`no_answer_forgets_a_write_it_did_not_carry`].
async fn forgets_nothing_unfiled(w: &World, lanes: &[Lane], settings: &Outbox, walk: u64) {
    let mut seed: u64 = 0x9e37_79b9_7f4a_7c15 ^ walk.wrapping_mul(0x2545_f491_4f6c_dd1d);
    let mut next = |below: usize| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        usize::try_from(seed % below as u64).unwrap()
    };
    let rows: [Uuid; 4] = ids();
    let key = |id: &Uuid| format!("{}|{id}", w.me);
    // Per row, the writes made to it, and the latest write a filed upload
    // carried.
    let mut written: HashMap<String, u64> = HashMap::new();
    let mut filed: HashMap<String, u64> = HashMap::new();
    // The sends the server got, each with its number and, per row it
    // carried, the writes made to it by then.
    let mut received: Vec<(u64, Vec<(String, u64)>)> = Vec::new();
    for step in 0..300 {
        let last = step >= 250;
        match if last { 3 + next(3) } else { next(8) } {
            0 | 1 => {
                let id = rows[next(rows.len())];
                w.exec(&format!(
                    "INSERT INTO readings (replica_id, id, value, at) VALUES (x'{}', x'{}', 1, {step}) \
                     ON CONFLICT (replica_id, id) DO UPDATE SET value = value + 1, at = {step}",
                    w.me.simple(),
                    id.simple()
                ))
                .await
                .unwrap();
                *written.entry(key(&id)).or_default() += 1;
            }
            2 | 3 => {
                let mut tx = w.pool.begin().await.unwrap();
                let outgoing = outbox(&mut tx, &codecs(), lanes, 0, settings, None, None)
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
                let Some(outgoing) = outgoing else { continue };
                let Some(&number) = outgoing.up.uploads.first() else {
                    continue;
                };
                let carried: Vec<(String, u64)> = outgoing
                    .up
                    .tables
                    .iter()
                    .flat_map(|t| t.keys.iter().chain(&t.deletes))
                    .map(|k| {
                        let k = k.parts.join("|");
                        let at = written.get(&k).copied().unwrap_or_default();
                        (k, at)
                    })
                    .collect();
                // Lost on the way, or taken by the server — whose answer
                // may come before the replica records the send.
                if last || next(2) == 0 {
                    if !last && next(3) == 0 {
                        for (k, at) in &carried {
                            let seen = filed.entry(k.clone()).or_default();
                            *seen = (*seen).max(*at);
                        }
                        answer(w, number).await;
                    } else {
                        received.push((number, carried));
                    }
                }
                let mut tx = w.pool.begin().await.unwrap();
                delivered(&mut tx, &outgoing).await.unwrap();
                tx.commit().await.unwrap();
            }
            4 => {
                // Time passes: whatever was sent and not answered goes again.
                w.exec("UPDATE sync_notes SET sent_at = '2000-01-01T00:00:00.000Z' WHERE sent_at IS NOT NULL")
                    .await
                    .unwrap();
            }
            5 if !received.is_empty() => {
                // An answer, of any send the server got, in any order.
                let (number, carried) = received.remove(next(received.len()));
                for (k, at) in carried {
                    let seen = filed.entry(k).or_default();
                    *seen = (*seen).max(at);
                }
                answer(w, number).await;
            }
            6 => {
                // An answer meant for another replica.
                answer(w, u64::try_from(next(usize::MAX)).unwrap() | 1).await;
            }
            _ => {}
        }
        let noted: HashSet<String> = sqlx::query_scalar("SELECT row_key FROM sync_notes")
            .fetch_all(&w.pool)
            .await
            .unwrap()
            .into_iter()
            .collect();
        for (row, writes) in &written {
            if !noted.contains(row) {
                assert!(
                    filed.get(row).is_some_and(|at| at >= writes),
                    "walk {walk}, step {step}: {row} forgotten, and no filed upload carried its \
                     last write ({writes}; filed {:?})",
                    filed.get(row)
                );
            }
        }
    }
    assert_eq!(
        w.count("SELECT count(*) FROM sync_notes").await,
        0,
        "walk {walk}: once the answers come, every note goes"
    );
}

/// The upload's number as a message carries it: a packed field of one.
fn number_field(up: &Up) -> usize {
    up.uploads
        .first()
        .map_or(0, |n| framed(prost::encoding::encoded_len_varint(*n)))
}

/// What the next upload's first row would have added to `up`'s part of its
/// table.
fn joining(up: &Up, next: &Up) -> usize {
    let part = up.tables[0].encoded_len();
    let row = &next.tables[0];
    let added = framed(row.upserts[0].len()) + framed(row.keys[0].encoded_len());
    framed(part + added) - framed(part)
}

/// A message up fills the transport's bytes to the byte and never outgrows
/// them: each one that left a row for the next had no room for it, with
/// nine bytes kept for its number. A row no message can carry is set aside
/// — and counted — rather than offered for ever; one that fills a message
/// alone goes.
#[sqlx::test(migrations = false)]
async fn a_message_up_fills_the_transport_and_a_row_too_large_is_set_aside(pool: SqlitePool) {
    let limits = Limits::new(100, 5000).unwrap();
    let settings = Settings {
        outbox: Outbox {
            limits,
            ..Outbox::default()
        },
        lanes: vec![Lane::new("main", &[]), Lane::new("history", &["events"])],
        ..Settings::default()
    };
    let w = world_with(pool, settings).await;
    let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
    let mut sizes: Vec<usize> = (0..60)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            1 + usize::try_from(seed % 900).unwrap()
        })
        .collect();
    sizes.extend([4850, 5000]);
    for size in &sizes {
        sqlx::query("INSERT INTO events (id, what) VALUES ($1, $2)")
            .bind(Uuid::now_v7())
            .bind("w".repeat(*size))
            .execute(&w.pool)
            .await
            .unwrap();
    }
    let mut ups = Vec::new();
    while let Some(up) = w.up(1, false).await {
        ups.push(up);
    }
    for (up, next) in ups.iter().zip(ups.iter().skip(1)) {
        let used = up.encoded_len();
        assert!(used <= limits.bytes(), "{used} bytes");
        let room = limits.bytes() + number_field(up) - used - framed(9);
        assert!(
            room < joining(up, next),
            "{room} bytes of room left, and the next row takes {}",
            joining(up, next)
        );
    }
    let rows: usize = ups.iter().map(|up| up.tables[0].upserts.len()).sum();
    assert_eq!(rows, sizes.len() - 1);
    let mut conn = w.pool.acquire().await.unwrap();
    let progress = progress(&mut conn).await.unwrap();
    assert_eq!(
        (progress[0].set_aside, progress[0].unfiled),
        (1, i64::try_from(rows).unwrap())
    );
}

/// On the main lane too a message fills the transport's bytes, short only
/// of what it keeps for its number, its confirmation and its backlog.
#[sqlx::test(migrations = false)]
async fn a_message_up_the_main_lane_fills_the_transport_too(pool: SqlitePool) {
    let limits = Limits::new(100, 5000).unwrap();
    let settings = Settings {
        outbox: Outbox {
            limits,
            ..Outbox::default()
        },
        ..Settings::default()
    };
    let w = world_with(pool, settings).await;
    for n in 0..40 {
        sqlx::query("INSERT INTO events (id, what) VALUES ($1, $2)")
            .bind(Uuid::now_v7())
            .bind("w".repeat(100 + n * 17))
            .execute(&w.pool)
            .await
            .unwrap();
    }
    let mut ups = Vec::new();
    while let Some(up) = w.up(0, false).await {
        if up.tables.is_empty() {
            break;
        }
        ups.push(up);
    }
    for (up, next) in ups.iter().zip(ups.iter().skip(1)) {
        let short = limits.bytes() - up.encoded_len();
        assert!(
            short < joining(up, next) + 64,
            "{short} bytes short, and the next row takes {}",
            joining(up, next)
        );
    }
}

/// Deletes fill a message up too, to the byte, and never outgrow it.
#[sqlx::test(migrations = false)]
async fn deletes_up_fill_the_transport_to_the_byte(pool: SqlitePool) {
    let limits = Limits::new(500, 5000).unwrap();
    let settings = Settings {
        outbox: Outbox {
            limits,
            ..Outbox::default()
        },
        lanes: vec![Lane::new("main", &[]), Lane::new("state", &["readings"])],
        ..Settings::default()
    };
    let w = world_with(pool, settings).await;
    for n in 0..200 {
        sqlx::query("INSERT INTO readings (replica_id, id, value, at) VALUES ($1, $2, 1, $3)")
            .bind(w.me)
            .bind(Uuid::now_v7())
            .bind(n)
            .execute(&w.pool)
            .await
            .unwrap();
    }
    w.exec("DELETE FROM sync_notes; DELETE FROM readings")
        .await
        .unwrap();
    let mut ups = Vec::new();
    while let Some(up) = w.up(1, false).await {
        ups.push(up);
    }
    let deleted: usize = ups.iter().map(|up| up.tables[0].deletes.len()).sum();
    assert_eq!(deleted, 200);
    for up in ups.iter().take(ups.len() - 1) {
        let used = up.encoded_len();
        assert!(used <= limits.bytes(), "{used} bytes");
        let room = limits.bytes() + number_field(up) - used - framed(9);
        let part = up.tables[0].encoded_len();
        let next = framed(up.tables[0].deletes[0].encoded_len());
        assert!(
            room < framed(part + next) - framed(part),
            "{room} bytes of room left"
        );
    }
}

/// The edges, to the byte, on a lane with nothing but rows: a row that
/// fills a message alone goes, one a byte larger is set aside; a second row
/// that fills what the first left goes with it, one a byte larger waits.
#[sqlx::test(migrations = false)]
async fn a_message_up_is_full_to_the_byte(pool: SqlitePool) {
    let limits = Limits::new(100, 5000).unwrap();
    let settings = Settings {
        outbox: Outbox {
            limits,
            ..Outbox::default()
        },
        lanes: vec![Lane::new("main", &[]), Lane::new("history", &["events"])],
        ..Settings::default()
    };
    let w = world_with(pool, settings).await;
    let id = Uuid::from_u128(7);
    let row = |n: usize| {
        json(&Event {
            id,
            what: "w".repeat(n),
        })
        .len()
    };
    let key = framed(wire_of(&vec![crate::row::KeyPart::Uuid(id)]).encoded_len());
    let part = |rows: &[usize]| framed(6) + rows.iter().map(|r| framed(*r) + key).sum::<usize>();
    let bare = Up {
        protocol: envelope::PROTOCOL,
        ..Up::default()
    }
    .encoded_len();
    // What a message of these rows takes, as the outbox counts it.
    let message = |rows: &[usize]| bare + framed(9) + framed(part(rows));
    let fills = |taken: &[usize]| {
        (1..5000)
            .find(|n| message(&[taken, &[row(*n)]].concat()) == limits.bytes())
            .expect("a size that fills the message to the byte")
    };
    let alone = fills(&[]);
    let first = row(300);
    let second = fills(&[first]);
    let write = |n: usize| {
        let pool = w.pool.clone();
        async move {
            sqlx::query("INSERT INTO events (id, what) VALUES ($1, $2)")
                .bind(Uuid::now_v7())
                .bind("w".repeat(n))
                .execute(&pool)
                .await
                .unwrap();
        }
    };
    let carried = |up: Option<Up>| {
        up.map_or(Vec::new(), |up| {
            up.tables
                .iter()
                .flat_map(|t| t.upserts.iter().map(Vec::len))
                .collect::<Vec<_>>()
        })
    };
    for (sizes, expected) in [
        (vec![alone], vec![vec![row(alone)]]),
        (vec![alone + 1], vec![]),
        (vec![300, second], vec![vec![first, row(second)]]),
        (
            vec![300, second + 1],
            vec![vec![first], vec![row(second + 1)]],
        ),
    ] {
        for size in &sizes {
            write(*size).await;
        }
        let mut sent = Vec::new();
        loop {
            let rows = carried(w.up(1, false).await);
            if rows.is_empty() {
                break;
            }
            sent.push(rows);
        }
        assert_eq!(sent, expected, "{sizes:?}");
        w.exec("DELETE FROM sync_notes").await.unwrap();
    }
}

/// A message the uplink refuses for good does not come back for ever: its
/// rows are set aside, the rest of what the replica has to say goes, and
/// the rows are offered again when the replica runs again.
#[sqlx::test(migrations = false)]
async fn rows_the_uplink_refuses_for_good_are_set_aside_until_the_next_run(pool: SqlitePool) {
    let w = world(pool).await;
    sqlx::query("INSERT INTO events (id, what) VALUES ($1, 'x')")
        .bind(Uuid::now_v7())
        .execute(&w.pool)
        .await
        .unwrap();
    events(&w, &["bad"]).await;
    *w.uplink.cannot_take.lock().unwrap() = Some(|up: &Up| {
        up.tables
            .iter()
            .flat_map(|t| &t.upserts)
            .any(|row| String::from_utf8_lossy(row).contains("\"what\":\"bad\""))
    });
    for _ in 0..8 {
        w.replica.send_lane(0, false).await.unwrap();
    }
    *w.uplink.cannot_take.lock().unwrap() = None;
    w.uplink.taken();
    let mut conn = w.pool.acquire().await.unwrap();
    let progress = progress(&mut conn).await.unwrap();
    assert_eq!((progress[0].set_aside, progress[0].unfiled), (1, 1));
    drop(conn);

    let again = Replica::new(
        w.pool.clone(),
        codecs(),
        w.me,
        w.uplink.clone(),
        Settings::default(),
    )
    .await
    .unwrap();
    again.send_lane(0, false).await.unwrap();
    let (_, up) = w.uplink.taken().pop().unwrap();
    assert_eq!(up.tables[0].upserts.len(), 1, "offered again");
}

/// A message the uplink refuses for good is asked again by halves until
/// what it refuses is alone: that row is set aside, and that rejection
/// taken as told — so it holds back no confirmation — and nothing beside
/// either is.
#[sqlx::test(migrations = false)]
async fn what_the_uplink_refuses_for_good_is_found_and_only_it_set_aside(pool: SqlitePool) {
    let w = world(pool).await;
    for what in ["a", "b", "bad", "c", "d", "e", "f"] {
        sqlx::query("INSERT INTO events (id, what) VALUES ($1, $2)")
            .bind(Uuid::now_v7())
            .bind(what)
            .execute(&w.pool)
            .await
            .unwrap();
    }
    let [good] = ids();
    let odd = Uuid::from_u128(0xbad);
    let broken = |id: Uuid| (key(&[&id.to_string()]), vec![0xff]);
    w.receive(&down(
        1,
        "",
        vec![upserts("items", vec![broken(good), broken(odd)])],
    ))
    .await;
    *w.uplink.cannot_take.lock().unwrap() = Some(|up: &Up| {
        let bad = String::from("\"what\":\"bad\"");
        up.tables
            .iter()
            .flat_map(|t| &t.upserts)
            .any(|row| String::from_utf8_lossy(row).contains(&bad))
            || up.rejected.iter().any(|r| {
                r.key
                    .as_ref()
                    .is_some_and(|k| k.parts == ["00000000-0000-0000-0000-000000000bad"])
            })
    });
    let mut taken = Vec::new();
    for _ in 0..30 {
        w.replica.send_lane(0, false).await.unwrap();
        taken.extend(w.uplink.taken());
    }
    let rows: usize = taken
        .iter()
        .flat_map(|(_, up)| &up.tables)
        .map(|t| t.upserts.len())
        .sum();
    assert_eq!(rows, 6, "every row but the one refused");
    let told: usize = taken.iter().map(|(_, up)| up.rejected.len()).sum();
    assert_eq!(told, 1, "one rejection told, the other refused alone");
    assert!(
        taken.iter().any(|(_, up)| up.sequence == BASE + 1),
        "confirmed once both are done with"
    );
    let mut conn = w.pool.acquire().await.unwrap();
    let progress = progress(&mut conn).await.unwrap();
    assert_eq!((progress[0].set_aside, progress[0].unfiled), (1, 6));
    assert_eq!(
        w.count("SELECT count(*) FROM sync_rejections WHERE reported_at IS NULL")
            .await,
        0
    );
}

async fn events(w: &World, names: &[&str]) {
    for what in names {
        sqlx::query("INSERT INTO events (id, what) VALUES ($1, $2)")
            .bind(Uuid::now_v7())
            .bind(what)
            .execute(&w.pool)
            .await
            .unwrap();
    }
}

/// A transport that refuses every message for good — a revoked credential, a
/// host that answers 403 — says nothing of any one row: nothing is set aside
/// on its word, whatever the number of rows, and it goes on being asked.
async fn nothing_is_blamed_when_everything_is_refused(pool: SqlitePool, names: &[&str]) {
    let w = world(pool).await;
    w.exec("DELETE FROM sync_notes").await.unwrap();
    events(&w, names).await;
    *w.uplink.refusing.lock().unwrap() = Some(false);
    let mut alarms = 0;
    for _ in 0..20 {
        alarms += usize::from(w.replica.send_lane(0, false).await.is_err());
    }
    assert!(alarms > 0, "{names:?}: the lane backs off");
    assert_eq!(
        w.count("SELECT count(*) FROM sync_notes WHERE set_aside IS NOT NULL")
            .await,
        0,
        "{names:?}"
    );
    *w.uplink.refusing.lock().unwrap() = None;
    let mut sent = 0;
    for _ in 0..10 {
        let _ = w.replica.send_lane(0, false).await;
        sent += w
            .uplink
            .taken()
            .iter()
            .flat_map(|(_, up)| &up.tables)
            .map(|t| t.upserts.len())
            .sum::<usize>();
    }
    assert_eq!(
        sent,
        names.len(),
        "all of it goes once the transport takes it"
    );
}

#[sqlx::test(migrations = false)]
async fn an_uplink_that_refuses_everything_for_good_blames_no_lone_row(pool: SqlitePool) {
    nothing_is_blamed_when_everything_is_refused(pool, &["only"]).await;
}

#[sqlx::test(migrations = false)]
async fn an_uplink_that_refuses_everything_for_good_blames_none_of_many_rows(pool: SqlitePool) {
    nothing_is_blamed_when_everything_is_refused(pool, &["a", "b", "c", "d", "e"]).await;
}

/// The row at the head of the queue that the transport refuses is found
/// all the same: with nothing yet taken to prove the transport works, the
/// ones behind it are tried, and then it is the one set aside.
#[sqlx::test(migrations = false)]
async fn a_refused_row_at_the_head_of_the_queue_is_set_aside_once_the_others_go(pool: SqlitePool) {
    let w = world(pool).await;
    w.exec("DELETE FROM sync_notes").await.unwrap();
    events(&w, &["bad", "b", "c", "d", "e"]).await;
    *w.uplink.cannot_take.lock().unwrap() = Some(|up: &Up| {
        up.tables
            .iter()
            .flat_map(|t| &t.upserts)
            .any(|row| String::from_utf8_lossy(row).contains("\"what\":\"bad\""))
    });
    for _ in 0..12 {
        let _ = w.replica.send_lane(0, false).await;
    }
    let sent: usize = w
        .uplink
        .taken()
        .iter()
        .flat_map(|(_, up)| &up.tables)
        .map(|t| t.upserts.len())
        .sum();
    assert_eq!(sent, 4);
    assert_eq!(
        w.texts("SELECT set_aside FROM sync_notes WHERE set_aside IS NOT NULL")
            .await
            .len(),
        1
    );
}

/// A message with nothing of the replica's writes in it — the confirmation of
/// what it applied — that the transport refuses for good has no row to
/// blame: it is the alarm and the backoff, not a silent loop.
#[sqlx::test(migrations = false)]
async fn a_message_with_no_rows_the_uplink_refuses_for_good_blames_nothing(pool: SqlitePool) {
    let w = world(pool).await;
    let [id] = ids();
    w.receive(&down(1, "", vec![upserts("items", vec![item(id, "a")])]))
        .await;
    *w.uplink.refusing.lock().unwrap() = Some(false);
    for _ in 0..3 {
        assert!(w.replica.send_lane(0, false).await.is_err());
    }
    *w.uplink.refusing.lock().unwrap() = None;
    let up = w.up(0, false).await.expect("the confirmation still goes");
    assert_eq!(up.sequence, BASE + 1);
}

/// The digests are due again an hour after they went, not after a message
/// that carried them and was refused.
#[sqlx::test(migrations = false)]
async fn digests_in_a_message_the_uplink_refuses_are_not_counted_as_sent(pool: SqlitePool) {
    let w = world(pool).await;
    events(&w, &["a", "bad", "c"]).await;
    *w.uplink.cannot_take.lock().unwrap() = Some(|up: &Up| {
        up.tables
            .iter()
            .flat_map(|t| &t.upserts)
            .any(|row| String::from_utf8_lossy(row).contains("\"what\":\"bad\""))
    });
    let went = w.replica.send_lane(0, true).await.unwrap();
    assert!(went.message);
    assert!(!went.digests, "refused with the rows");
    let mut digests = false;
    for _ in 0..8 {
        digests |= w.replica.send_lane(0, true).await.unwrap().digests;
    }
    assert!(digests, "they go with the message that is taken");
}

/// A row this build cannot encode is set aside, with why, and counted: the
/// rest of its table goes.
#[sqlx::test(migrations = false)]
async fn a_row_that_does_not_encode_is_set_aside_and_the_rest_goes(pool: SqlitePool) {
    fn to_wire(event: Event) -> Result<ItemRow, crate::wire::MapError> {
        if event.what == "poison" {
            return Err(crate::wire::MapError::new(
                "a value this build cannot encode",
            ));
        }
        Ok(ItemRow {
            id: event.id.to_string(),
            partition_id: String::new(),
            name: event.what,
        })
    }
    fn from_wire(row: ItemRow) -> Result<Event, crate::wire::MapError> {
        Ok(Event {
            id: row.id.parse()?,
            what: row.name,
        })
    }
    let w = world(pool).await;
    let mut codecs = codecs();
    *codecs.last_mut().unwrap() = Codec::new::<Event, ItemRow>(to_wire, from_wire, None);
    let replica = Replica::new(
        w.pool.clone(),
        codecs,
        w.me,
        w.uplink.clone(),
        Settings::default(),
    )
    .await
    .unwrap();
    for what in ["fine", "poison", "also fine"] {
        sqlx::query("INSERT INTO events (id, what) VALUES ($1, $2)")
            .bind(Uuid::now_v7())
            .bind(what)
            .execute(&w.pool)
            .await
            .unwrap();
    }
    replica.send_lane(0, false).await.unwrap();
    let (_, up) = w.uplink.taken().pop().unwrap();
    assert_eq!(up.tables[0].upserts.len(), 2);
    assert_eq!(up.backlog.unwrap().tables[0].set_aside, 1);
    assert_eq!(
        w.texts("SELECT set_aside FROM sync_notes WHERE set_aside IS NOT NULL")
            .await,
        ["`events` does not encode: a value this build cannot encode"]
    );
}

/// Digests go with the last message applied, which goes only once every
/// refusal of it is told: until then they wait, rather than go unanchored.
#[sqlx::test(migrations = false)]
async fn digests_wait_for_the_confirmation_they_are_read_against(pool: SqlitePool) {
    let settings = Settings {
        outbox: Outbox {
            rejections: std::num::NonZeroUsize::MIN,
            ..Outbox::default()
        },
        ..Settings::default()
    };
    let w = world_with(pool, settings).await;
    let [a, b] = ids();
    let broken = |id: Uuid| (key(&[&id.to_string()]), vec![0xff]);
    w.receive(&down(
        1,
        "",
        vec![upserts("items", vec![broken(a), broken(b)])],
    ))
    .await;
    let first = w.up(0, true).await.unwrap();
    assert_eq!(
        (first.rejected.len(), first.sequence, first.digests.len()),
        (1, 0, 0)
    );
    let last = w.up(0, true).await.unwrap();
    assert_eq!((last.rejected.len(), last.sequence), (1, BASE + 1));
    assert!(!last.digests.is_empty());
}

/// Digests read before a message was applied are not sent with it: they
/// wait to be read again, against what the replica now holds.
#[sqlx::test(migrations = false)]
async fn digests_go_only_with_the_message_they_were_read_against(pool: SqlitePool) {
    let w = world(pool).await;
    let mut conn = w.pool.acquire().await.unwrap();
    let stale = held(&mut conn, &codecs()).await.unwrap();
    drop(conn);
    w.receive(&down(
        1,
        "",
        vec![upserts("items", vec![item(Uuid::now_v7(), "a")])],
    ))
    .await;
    let lanes = Settings::default().lanes;
    let mut tx = w.pool.begin().await.unwrap();
    let up = outbox(
        &mut tx,
        &codecs(),
        &lanes,
        0,
        &Outbox::default(),
        Some(stale),
        None,
    )
    .await
    .unwrap()
    .unwrap()
    .up;
    tx.rollback().await.unwrap();
    assert!(up.digests.is_empty());
    assert_eq!(up.sequence, BASE + 1);
    let mut conn = w.pool.acquire().await.unwrap();
    let fresh = held(&mut conn, &codecs()).await.unwrap();
    let mut tx = w.pool.begin().await.unwrap();
    let small = Outbox {
        limits: Limits::new(10, 5000).unwrap(),
        ..Outbox::default()
    };
    let up = outbox(&mut tx, &codecs(), &lanes, 0, &small, Some(fresh), None)
        .await
        .unwrap()
        .unwrap()
        .up;
    assert_eq!(
        up.digests
            .iter()
            .find(|d| d.table == "items")
            .unwrap()
            .count,
        1
    );
}

/// Once the server heard what was applied, an upload of rows says it no
/// more: the last applied goes when it is news, or with digests or a gap.
#[sqlx::test(migrations = false)]
async fn the_last_applied_goes_only_when_it_is_news(pool: SqlitePool) {
    let w = world(pool).await;
    w.receive(&down(
        1,
        "",
        vec![upserts("items", vec![item(Uuid::now_v7(), "a")])],
    ))
    .await;
    assert_eq!(w.up(0, false).await.unwrap().sequence, BASE + 1);
    sqlx::query("INSERT INTO events (id, what) VALUES ($1, 'x')")
        .bind(Uuid::now_v7())
        .execute(&w.pool)
        .await
        .unwrap();
    let up = w.up(0, false).await.unwrap();
    assert_eq!((up.tables.len(), up.sequence), (1, 0));
}

/// Each lane keeps its own pace: one the uplink fails on does not hold back
/// another.
#[sqlx::test(migrations = false)]
async fn a_lane_that_fails_holds_back_no_other(pool: SqlitePool) {
    let pool = settled(pool).await;
    let settings = Settings {
        lanes: vec![
            Lane {
                name: "main",
                tables: &[],
            },
            Lane {
                name: "fast",
                tables: &["readings"],
            },
        ],
        idle: std::time::Duration::from_millis(10),
        backoff: crate::run::Backoff::new(
            std::time::Duration::from_millis(10),
            std::time::Duration::from_millis(20),
        ),
        ..Settings::default()
    };
    let w = world_with(pool, settings).await;
    *w.uplink.refusing.lock().unwrap() = Some(true);
    w.uplink.refused_lanes.lock().unwrap().push("main");
    sqlx::query("INSERT INTO readings (replica_id, id, value, at) VALUES ($1, $2, 1, 1)")
        .bind(w.me)
        .bind(Uuid::now_v7())
        .execute(&w.pool)
        .await
        .unwrap();
    let [a] = ids();
    w.receive(&down(1, "", vec![upserts("items", vec![item(a, "a")])]))
        .await;
    let (stop, mut stopped) = tokio::sync::watch::channel(false);
    let run = w.replica.run(async move {
        let _ = stopped.wait_for(|s| *s).await;
    });
    let watch = async {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let sent = w.uplink.sent.lock().unwrap().clone();
            if sent
                .iter()
                .any(|(_, lane, up)| *lane == "fast" && !up.tables.is_empty())
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the fast lane waited"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        stop.send(true).unwrap();
    };
    tokio::join!(run, watch);
}

/// What the replica tells the server it applied is of one epoch: a message
/// of a new one applied while the old one's confirmation was on its way is
/// confirmed in turn.
#[sqlx::test(migrations = false)]
async fn a_confirmation_is_of_its_epoch(pool: SqlitePool) {
    let w = world(pool).await;
    let [a, b] = ids();
    w.receive(&down(1, "", vec![upserts("items", vec![item(a, "a")])]))
        .await;
    let lanes = Settings::default().lanes;
    let mut tx = w.pool.begin().await.unwrap();
    let outgoing = outbox(
        &mut tx,
        &codecs(),
        &lanes,
        0,
        &Outbox::default(),
        None,
        None,
    )
    .await
    .unwrap()
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(outgoing.up.sequence, BASE + 1);
    let s = Uuid::now_v7().to_string();
    let mut message =
        ToReplica::decode(begin(1, &s, vec![upserts("items", vec![item(b, "b")])]).as_slice())
            .unwrap();
    if let Some(Body::Down(down)) = &mut message.body {
        down.epoch = OTHER_EPOCH.to_owned();
    }
    w.receive(&message.encode_to_vec()).await;
    let mut tx = w.pool.begin().await.unwrap();
    delivered(&mut tx, &outgoing).await.unwrap();
    tx.commit().await.unwrap();
    let up = w.up(0, false).await.expect("the new epoch's confirmation");
    assert_eq!((up.epoch.as_str(), up.sequence), (OTHER_EPOCH, BASE + 1));
}

/// A replica does not confirm a message while refusals of it are still to
/// report: the server would take the refused rows for applied.
#[sqlx::test(migrations = false)]
async fn a_replica_confirms_only_once_every_refusal_is_reported(pool: SqlitePool) {
    let pool = settled(pool).await;
    let settings = Settings {
        outbox: Outbox {
            rejections: std::num::NonZeroUsize::MIN,
            ..Outbox::default()
        },
        ..Settings::default()
    };
    let w = world_with(pool, settings).await;
    let [a, b] = ids();
    let broken = |id: Uuid| (key(&[&id.to_string()]), vec![0xff]);
    w.receive(&down(
        1,
        "",
        vec![upserts("items", vec![broken(a), broken(b)])],
    ))
    .await;
    let first = w.up(0, false).await.unwrap();
    assert_eq!((first.rejected.len(), first.sequence), (1, 0));
    let last = w.up(0, false).await.unwrap();
    assert_eq!((last.rejected.len(), last.sequence), (1, BASE + 1));
}

/// The replica tells the server what it wrote and the server has not filed:
/// with the rows it sends, at once when that starts or empties, again to a
/// server installed again, and while it grows no more often than told.
#[sqlx::test(migrations = false)]
async fn the_backlog_is_said_when_it_changes(pool: SqlitePool) {
    let w = world(pool).await;
    let lanes = vec![
        Lane {
            name: "main",
            tables: &[],
        },
        Lane {
            name: "fast",
            tables: &["events"],
        },
    ];
    let mut settings = Outbox::default();
    let write =
        || sqlx::query("INSERT INTO events (id, what) VALUES ($1, 'x')").bind(Uuid::now_v7());
    let said = |o: &Option<Outgoing>| {
        o.as_ref()
            .and_then(|o| o.up.backlog.clone())
            .map(|b: Backlog| {
                b.tables
                    .into_iter()
                    .map(|t| (t.table, t.unsent, t.unfiled))
                    .collect::<Vec<_>>()
            })
    };
    let turn = |lane: usize, settings: Outbox| {
        let pool = w.pool.clone();
        let lanes = lanes.clone();
        async move {
            let mut tx = pool.begin().await.unwrap();
            let outgoing = outbox(&mut tx, &codecs(), &lanes, lane, &settings, None, None)
                .await
                .unwrap();
            if let Some(o) = &outgoing {
                delivered(&mut tx, o).await.unwrap();
            }
            tx.commit().await.unwrap();
            outgoing
        }
    };

    write().execute(&w.pool).await.unwrap();
    let main = turn(0, settings.clone()).await;
    assert_eq!(
        said(&main),
        Some(vec![("events".into(), 1, 0)]),
        "it starts"
    );
    write().execute(&w.pool).await.unwrap();
    assert!(
        turn(0, settings.clone()).await.is_none(),
        "it grows, said a moment ago"
    );
    settings.backlog_every = std::time::Duration::ZERO;
    let main = turn(0, settings.clone()).await;
    assert_eq!(said(&main), Some(vec![("events".into(), 2, 0)]), "when due");
    let again = turn(0, settings.clone()).await;
    assert!(again.is_some(), "unchanged, said again when due");

    let fast = turn(1, settings.clone()).await.expect("the rows");
    assert!(fast.up.backlog.is_none(), "the main lane says it");
    w.receive(
        &Body::Filed(Filed {
            uploads: fast.up.uploads.clone(),
        })
        .encoded(),
    )
    .await;
    settings.backlog_every = std::time::Duration::from_secs(3600);
    let main = turn(0, settings.clone()).await;
    assert_eq!(said(&main), Some(vec![]), "it empties, said at once");
    assert!(turn(0, settings.clone()).await.is_none());
    let zero = Outbox {
        backlog_every: std::time::Duration::ZERO,
        ..settings.clone()
    };
    assert!(
        turn(0, zero).await.is_none(),
        "empty, said: not said again however due"
    );

    write().execute(&w.pool).await.unwrap();
    turn(0, settings.clone()).await.expect("it starts again");
    let [a] = ids();
    w.receive(&down(1, "", vec![upserts("items", vec![item(a, "a")])]))
        .await;
    let s = Uuid::now_v7().to_string();
    let mut message = ToReplica::decode(begin(2, &s, vec![]).as_slice()).unwrap();
    if let Some(Body::Down(down)) = &mut message.body {
        down.epoch = OTHER_EPOCH.to_owned();
    }
    w.receive(&message.encode_to_vec()).await;
    let told = turn(0, settings.clone())
        .await
        .expect("a new server hears it");
    assert!(told.up.backlog.is_some());
}

/// A clock set back makes nothing wait for it to come round again: an upload
/// sent "in the future" goes again.
#[sqlx::test(migrations = false)]
async fn a_clock_set_back_holds_back_nothing(pool: SqlitePool) {
    let w = world(pool).await;
    sqlx::query("INSERT INTO events (id, what) VALUES ($1, 'x')")
        .bind(Uuid::now_v7())
        .execute(&w.pool)
        .await
        .unwrap();
    w.up(0, false).await.expect("sent");
    w.exec(
        "UPDATE sync_notes SET sent_at = '2999-01-01T00:00:00.000Z'; \
            UPDATE sync_backlog SET reported_at = 32503680000000",
    )
    .await
    .unwrap();
    let up = w.up(0, false).await.expect("sent again");
    assert_eq!(up.tables[0].upserts.len(), 1);
    // A backlog told "in the future", unchanged and not empty: said again.
    w.exec("UPDATE sync_backlog SET reported_at = 32503680000000")
        .await
        .unwrap();
    let up = w.up(0, false).await.expect("the backlog again");
    assert!(up.tables.is_empty() && up.backlog.is_some(), "{up:?}");
}

/// A row the replica deleted goes up as its key only for a table keyed by
/// its replica: history it let go of is not the server's to delete, and its
/// note is done.
#[sqlx::test(migrations = false)]
async fn only_a_table_keyed_by_its_replica_sends_its_deletes(pool: SqlitePool) {
    let w = world(pool).await;
    let [event, reading] = ids();
    sqlx::query("INSERT INTO events (id, what) VALUES ($1, 'x')")
        .bind(event)
        .execute(&w.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO readings (replica_id, id, value, at) VALUES ($1, $2, 1, 1)")
        .bind(w.me)
        .bind(reading)
        .execute(&w.pool)
        .await
        .unwrap();
    w.exec("DELETE FROM events; DELETE FROM readings")
        .await
        .unwrap();
    let up = w.up(0, false).await.expect("the deletes");
    let sent: Vec<(&str, usize, usize)> = up
        .tables
        .iter()
        .map(|t| (t.table.as_str(), t.upserts.len(), t.deletes.len()))
        .collect();
    assert_eq!(sent, [("readings", 0, 1)]);
    assert_eq!(
        w.count("SELECT count(*) FROM sync_notes WHERE relation = 'events'")
            .await,
        0,
        "done here"
    );
}

/// Whatever rows, rejections and digests there are, every message up fits
/// the transport's bytes — framing, the backlog and the confirmation
/// included — and every row goes, once.
#[sqlx::test(migrations = false)]
async fn every_message_up_fits_whatever_it_carries(pool: SqlitePool) {
    let pool = settled(pool).await;
    let limits = Limits {
        rows: 500,
        bytes: envelope::ENVELOPE_ROOM + 700,
    };
    let settings = Settings {
        outbox: Outbox {
            limits,
            ..Outbox::default()
        },
        ..Settings::default()
    };
    let w = world_with(pool, settings).await;
    let mut seed: u64 = 0x5851_f42d_4c95_7f2d;
    let mut next = |below: usize| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        usize::try_from(seed % below as u64).unwrap()
    };
    // Rejections whose reasons are long: they share the room with the rows.
    let broken: Vec<(WireKey, Vec<u8>)> = (0..40)
        .map(|_| (key(&[&Uuid::now_v7().to_string()]), vec![0xff]))
        .collect();
    w.receive(&down(1, "", vec![upserts("items", broken)]))
        .await;
    w.exec(&format!(
        "UPDATE sync_rejections SET reason = '{}'",
        "r".repeat(150)
    ))
    .await
    .unwrap();
    let mut written = 0;
    for _ in 0..60 {
        sqlx::query("INSERT INTO events (id, what) VALUES ($1, $2)")
            .bind(Uuid::now_v7())
            .bind("w".repeat(1 + next(500)))
            .execute(&w.pool)
            .await
            .unwrap();
        written += 1;
    }
    let (mut rows, mut rejected) = (0, 0);
    for round in 0..200 {
        let Some(up) = w.up(0, round % 3 == 0).await else {
            break;
        };
        assert!(
            up.encoded_len() <= limits.bytes,
            "{} bytes, over {}",
            up.encoded_len(),
            limits.bytes
        );
        rows += up.tables.iter().map(|t| t.upserts.len()).sum::<usize>();
        rejected += up.rejected.len();
    }
    assert_eq!((rows, rejected), (written, 40));
}

/// The tables the suite runs on are their rows, column for column.
#[sqlx::test(migrations = false)]
async fn the_suites_tables_are_their_rows(pool: SqlitePool) {
    let pool = settled(pool).await;
    sqlx::raw_sql(TABLES).execute(&pool).await.unwrap();
    let codecs = codecs();
    let tables: Vec<&crate::row::Table> = codecs.iter().map(|c| &c.table).collect();
    let mut conn = pool.acquire().await.unwrap();
    crate::row::parity(&mut conn, &tables)
        .await
        .expect("every table its row");
}

/// A table that is not its row is refused, with every way it is not: a
/// column the row lacks, a field the table lacks, a nullability, a type, a
/// primary key.
#[sqlx::test(migrations = false)]
async fn a_table_that_is_not_its_row_is_refused_with_every_difference(pool: SqlitePool) {
    let pool = settled(pool).await;
    sqlx::raw_sql(
        "CREATE TABLE events (id BLOB NOT NULL, what BLOB, extra TEXT, PRIMARY KEY (id, extra))",
    )
    .execute(&pool)
    .await
    .unwrap();
    let table = crate::row::Table::of::<Event>();
    let mut conn = pool.acquire().await.unwrap();
    let why = crate::row::parity(&mut conn, &[&table])
        .await
        .expect_err("refused")
        .to_string();
    for difference in [
        "`extra` is not a field of the row",
        "`what` is nullable in the table and no Option in the row",
        "`what` is BLOB in the table, which its field does not decode",
        "the primary key is [\"extra\", \"id\"], and the row's key [\"id\"]",
    ] {
        assert!(why.contains(difference), "{difference} — in: {why}");
    }
    sqlx::raw_sql("DROP TABLE events; CREATE TABLE events (id BLOB PRIMARY KEY NOT NULL)")
        .execute(&mut *conn)
        .await
        .unwrap();
    let why = crate::row::parity(&mut conn, &[&table])
        .await
        .expect_err("refused")
        .to_string();
    assert!(
        why.contains("the field `what` is no column of the table"),
        "{why}"
    );
}

/// Rows by key are the active ones: on a replica, less the retired.
#[sqlx::test(migrations = false)]
async fn rows_by_key_are_the_ones_not_retired(pool: SqlitePool) {
    let w = world(pool).await;
    let [kept, retired, aisle] = ids();
    let shelf = Shelf {
        aisle,
        slot: 4,
        label: "four".into(),
        revision: 1,
    };
    w.receive(&down(
        1,
        "",
        vec![
            upserts("items", vec![item(kept, "kept"), item(retired, "retired")]),
            upserts(
                "shelves",
                vec![(key(&[&aisle.to_string(), "4"]), json(&shelf))],
            ),
        ],
    ))
    .await;
    sqlx::query("INSERT INTO receipts (item_id) VALUES ($1)")
        .bind(retired)
        .execute(&w.pool)
        .await
        .unwrap();
    w.receive(&down(
        2,
        "",
        vec![deletes("items", vec![key(&[&retired.to_string()])])],
    ))
    .await;
    let mut conn = w.pool.acquire().await.unwrap();
    let both = [
        vec![crate::row::KeyPart::Uuid(kept)],
        vec![crate::row::KeyPart::Uuid(retired)],
    ];
    let items = crate::row::get_many::<crate::testkit::Item>(&mut conn, &both)
        .await
        .unwrap();
    assert_eq!(
        items.iter().map(|i| i.name.as_str()).collect::<Vec<_>>(),
        ["kept"]
    );
    let got = crate::row::get::<Shelf>(
        &mut conn,
        &vec![
            crate::row::KeyPart::Uuid(aisle),
            crate::row::KeyPart::Int(4),
        ],
    )
    .await
    .unwrap();
    assert_eq!(got, Some(shelf));
}

// ---------------------------------------------------------------------------
// The database the engine runs against.

async fn live_triggers(conn: &mut sqlx::SqliteConnection) -> Vec<(String, String)> {
    sqlx::query_as("SELECT name, sql FROM sqlite_master WHERE type = 'trigger' ORDER BY name")
        .fetch_all(conn)
        .await
        .unwrap()
}

/// A host's migration prints the triggers again whenever a table changes, so
/// the script replaces what is there rather than failing on it.
#[sqlx::test(migrations = false)]
async fn the_triggers_script_runs_again_over_what_it_wrote(pool: SqlitePool) {
    let w = world(pool).await;
    let mut conn = w.pool.acquire().await.unwrap();
    let script = triggers(&codecs());
    let before = live_triggers(&mut conn).await;

    sqlx::raw_sql(&script)
        .execute(&mut *conn)
        .await
        .expect("a second run");
    sqlx::raw_sql(&script)
        .execute(&mut *conn)
        .await
        .expect("a third run");
    assert_eq!(live_triggers(&mut conn).await, before);

    sqlx::raw_sql("ALTER TABLE readings ADD COLUMN later TEXT")
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::raw_sql(&script)
        .execute(&mut *conn)
        .await
        .expect("after a column is added");
    assert_eq!(live_triggers(&mut conn).await, before);
}

/// The engine's triggers are checked as they are, not by name: one that
/// differs, one missing, and one left from a table that no longer travels
/// are each refused; installing puts them right, in one transaction.
#[sqlx::test(migrations = false)]
async fn only_the_engines_own_triggers_pass_the_check(pool: SqlitePool) {
    let w = world(pool).await;
    let mut conn = w.pool.acquire().await.unwrap();
    check(&mut conn, &codecs()).await.expect("as installed");
    let script = triggers(&codecs());
    assert!(script.contains("sync_note_readings_update"));

    let cases = [
        (
            "DROP TRIGGER sync_note_readings_update; CREATE TRIGGER sync_note_readings_update \
             AFTER UPDATE ON readings BEGIN INSERT INTO sync_notes (relation, row_key) VALUES ('readings', NEW.id); END",
            "the trigger `sync_note_readings_update` is not the one replica::triggers writes",
        ),
        (
            "DROP TRIGGER sync_note_events_delete",
            "the trigger `sync_note_events_delete` is missing",
        ),
        (
            "CREATE TRIGGER sync_note_gone_insert AFTER INSERT ON receipts BEGIN SELECT 1; END",
            "the trigger `sync_note_gone_insert` is not one of the engine's",
        ),
        (
            "CREATE TRIGGER by_hand AFTER INSERT ON receipts BEGIN \
             INSERT INTO sync_notes (relation, row_key) VALUES ('receipts', NEW.id); END",
            "the trigger `by_hand` is not one of the engine's",
        ),
    ];
    for (case, why) in cases {
        sqlx::raw_sql(case).execute(&mut *conn).await.unwrap();
        match check(&mut conn, &codecs()).await {
            Err(Error::Misuse(refused)) => assert!(refused.contains(why), "{case}: {refused}"),
            other => panic!("{case}: {other:?}"),
        }
        if case.contains("by_hand") {
            sqlx::raw_sql("DROP TRIGGER by_hand")
                .execute(&mut *conn)
                .await
                .unwrap();
        }
        install(&mut conn, &codecs()).await.expect("put right");
    }

    // A host's own trigger may be named as it likes: install leaves it.
    sqlx::raw_sql("CREATE TRIGGER sync_audit AFTER INSERT ON receipts BEGIN SELECT 1; END")
        .execute(&mut *conn)
        .await
        .unwrap();
    install(&mut conn, &codecs())
        .await
        .expect("installed again");
    let audit: i64 =
        sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE name = 'sync_audit'")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    assert_eq!(audit, 1, "the host's own trigger stays");
}

/// A connection the engine cannot keep its word on is refused, naming the
/// setting: commits that are not durable, a journal that cannot roll back,
/// triggers that miss what a conflict replaces.
#[sqlx::test(migrations = false)]
async fn a_connection_the_engine_cannot_keep_its_word_on_is_refused(pool: SqlitePool) {
    let w = world(pool).await;
    for (set, why) in [
        ("PRAGMA synchronous = NORMAL", "`synchronous` below FULL"),
        (
            "PRAGMA journal_mode = MEMORY",
            "`journal_mode` OFF or MEMORY",
        ),
        ("PRAGMA journal_mode = OFF", "`journal_mode` OFF or MEMORY"),
        (
            "PRAGMA recursive_triggers = OFF",
            "`recursive_triggers` off",
        ),
    ] {
        let mut conn = w.pool.acquire().await.unwrap();
        sqlx::raw_sql(set).execute(&mut *conn).await.unwrap();
        match check(&mut conn, &codecs()).await {
            Err(Error::Misuse(refused)) => assert!(refused.contains(why), "{set}: {refused}"),
            other => panic!("{set}: {other:?}"),
        }
        conn.detach();
    }
}

/// Each released step of the engine's tables is as it was released, and
/// brings a database to its version: a step is never edited, a later
/// version is a step of its own.
#[sqlx::test(migrations = false)]
async fn every_step_of_the_engines_tables_is_as_released(pool: SqlitePool) {
    let pool = settled(pool).await;
    let released: [u64; crate::REPLICA_SCHEMA_VERSION as usize] = [0x7f10_e071_c3e4_898e];
    let fnv = |text: &str| {
        text.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
        })
    };
    let mut conn = pool.acquire().await.unwrap();
    for (version, pinned) in (1..=crate::REPLICA_SCHEMA_VERSION).zip(released) {
        let step = migration(version).expect("a released step");
        assert_eq!(
            fnv(&step),
            pinned,
            "step {version} was edited: {:#x}",
            fnv(&step)
        );
        sqlx::raw_sql(&step).execute(&mut *conn).await.unwrap();
        assert_eq!(schema_version(&mut conn).await.unwrap(), version);
    }
    assert_eq!(migration(0), None);
    assert_eq!(migration(crate::REPLICA_SCHEMA_VERSION + 1), None);
    sqlx::raw_sql(TABLES).execute(&mut *conn).await.unwrap();
    install(&mut conn, &codecs())
        .await
        .expect("nothing left to migrate");
}

/// A database whose engine tables are of another version is refused, with
/// what brings it to this build's: an install for one behind, never for one
/// a later build wrote.
#[sqlx::test(migrations = false)]
async fn a_database_of_another_version_is_refused_with_the_way_forward(pool: SqlitePool) {
    let pool = settled(pool).await;
    let mut conn = pool.acquire().await.unwrap();
    sqlx::raw_sql(TABLES).execute(&mut *conn).await.unwrap();
    let Err(Error::Misuse(why)) = check(&mut conn, &codecs()).await else {
        panic!("a database without the engine passed the check");
    };
    assert!(
        why.contains("replica::install") && why.contains("migration"),
        "{why}"
    );

    install(&mut conn, &codecs()).await.expect("installed");
    sqlx::raw_sql("UPDATE sync_version SET version = version + 1")
        .execute(&mut *conn)
        .await
        .unwrap();
    let Err(Error::Misuse(why)) = check(&mut conn, &codecs()).await else {
        panic!("a later build's database passed the check");
    };
    assert!(
        why.contains("later build") && !why.contains("install"),
        "{why}"
    );
}

/// An install that fails leaves nothing behind: the engine's tables and
/// triggers go in together, or not at all.
#[sqlx::test(migrations = false)]
async fn an_install_that_fails_leaves_nothing_half_done(pool: SqlitePool) {
    let pool = settled(pool).await;
    let mut conn = pool.acquire().await.unwrap();
    // `tasks` is missing: its triggers cannot be created.
    sqlx::raw_sql(&TABLES.replace("CREATE TABLE tasks", "CREATE TABLE not_tasks"))
        .execute(&mut *conn)
        .await
        .unwrap();
    match install(&mut conn, &codecs()).await {
        Err(Error::Database(e)) => assert!(e.to_string().contains("no such table"), "{e}"),
        other => panic!("{other:?}"),
    }
    let left: i64 =
        sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE name LIKE 'sync%'")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    assert_eq!(left, 0);
}

/// Lanes a replica cannot send up with are refused before it runs.
#[test]
fn lanes_that_would_strand_a_write_are_refused() {
    let lane = |name, tables| Lane { name, tables };
    let refused: [&[Lane]; 5] = [
        &[],
        &[lane("a", &[]), lane("b", &[])],
        &[lane("a", &["events"]), lane("b", &["events"])],
        &[lane("main", &[]), lane("a", &["items"])],
        &[lane("a", &["events"])],
    ];
    for lanes in refused {
        assert!(check_settings(&codecs(), lanes).is_err(), "{lanes:?}");
    }
    let fine = [lane("main", &[]), lane("fast", &["readings"])];
    check_settings(&codecs(), &fine).unwrap();
    let every_table_named = [lane("a", &["tasks", "readings"]), lane("b", &["events"])];
    check_settings(&codecs(), &every_table_named).unwrap();
}

/// A table that travels as JSON keeps a 64-bit integer exact for a replica
/// that reads numbers as doubles, and names columns as the table does.
#[test]
fn a_json_row_carries_its_columns_by_name_and_big_integers_as_text() {
    let shelf = Shelf {
        aisle: Uuid::nil(),
        slot: i64::MAX,
        label: "x".into(),
        revision: (1 << 53) + 1,
    };
    let row: serde_json::Value = serde_json::from_slice(&json(&shelf)).unwrap();
    assert_eq!(row["slot"], serde_json::json!(i64::MAX.to_string()));
    assert_eq!(row["revision"], serde_json::json!("9007199254740993"));
    let event: serde_json::Value = serde_json::from_slice(&json(&Event {
        id: Uuid::nil(),
        what: "w".into(),
    }))
    .unwrap();
    assert_eq!(event, serde_json::json!({"id": Uuid::nil(), "what": "w"}));
}

/// A host spawns what a replica runs, so its futures are `Send` — which one
/// statement holding a borrow across an await quietly stops them being.
#[sqlx::test(migrations = false)]
async fn what_a_host_spawns_can_be_spawned(pool: SqlitePool) {
    let pool = settled(pool).await;
    fn spawnable<T: Send>(_: T) {}
    let w = world(pool).await;
    spawnable(w.replica.receive(&[]));
    spawnable(w.replica.send_lane(0, true));
    spawnable(w.replica.run(std::future::pending()));
    spawnable(Replica::new(
        w.pool.clone(),
        codecs(),
        w.me,
        w.uplink.clone(),
        Settings::default(),
    ));
    let mut conn = w.pool.acquire().await.unwrap();
    spawnable(receive(&mut conn, &codecs(), &[], &Care::default()));
    let settings = Outbox::default();
    spawnable(outbox(
        &mut conn,
        &codecs(),
        &Settings::default().lanes,
        0,
        &settings,
        None,
        None,
    ));
    spawnable(install(&mut conn, &codecs()));
}

/// A host writes whole rows on a replica only of a record: a table the
/// server writes is refused before the database is asked, with how a
/// replica edits a two-way row.
#[sqlx::test(migrations = false)]
async fn a_host_writes_whole_rows_only_of_a_record(pool: SqlitePool) {
    let w = world(pool).await;
    let mut conn = w.pool.acquire().await.unwrap();
    let task = Task {
        id: Uuid::now_v7(),
        title: "t".into(),
        done: false,
        updated_at: at(0),
    };
    match crate::row::upsert(&mut conn, vec![task]).await {
        Err(Error::Misuse(why)) => assert!(why.contains("UPDATE of those"), "{why}"),
        other => panic!("{other:?}"),
    }
    let price = Price {
        id: Uuid::now_v7(),
        cents: 1,
        updated_at: at(0),
    };
    match crate::row::insert_ignore(&mut conn, vec![price]).await {
        Err(Error::Misuse(why)) => assert!(why.contains("is the server's"), "{why}"),
        other => panic!("{other:?}"),
    }
    let event = Event {
        id: Uuid::now_v7(),
        what: "mine".into(),
    };
    let written = crate::row::upsert(&mut conn, vec![event.clone()])
        .await
        .unwrap();
    assert_eq!(written, [crate::row::Row::key(&event)]);
    let other = Event {
        id: Uuid::now_v7(),
        what: "other".into(),
    };
    let inserted = crate::row::insert_ignore(&mut conn, vec![event, other.clone()])
        .await
        .unwrap();
    assert_eq!(
        inserted,
        [crate::row::Row::key(&other)],
        "the one it held left as it was"
    );
}

/// A mapping's failure says why, as text and as debug output.
#[test]
fn a_mapping_that_fails_says_why() {
    let failed = crate::wire::MapError::new("no such colour");
    assert_eq!(failed.to_string(), "no such colour");
    assert_eq!(format!("{failed:?}"), "MapError(\"no such colour\")");
}

/// A send the server has not answered goes again once `resend_after` has
/// passed, to the millisecond: not before, however short it is.
#[sqlx::test(migrations = false)]
async fn an_unanswered_send_goes_again_after_its_wait_and_not_before(pool: SqlitePool) {
    let settings = Settings {
        outbox: Outbox {
            resend_after: std::time::Duration::from_millis(400),
            ..Outbox::default()
        },
        ..Settings::default()
    };
    let w = world_with(pool, settings).await;
    sqlx::query("INSERT INTO events (id, what) VALUES ($1, 'once')")
        .bind(Uuid::now_v7())
        .execute(&w.pool)
        .await
        .unwrap();
    let rows = |up: Option<Up>| up.map_or(0, |up| up.tables.len());
    assert_eq!(rows(w.up(0, false).await), 1);
    assert_eq!(rows(w.up(0, false).await), 0, "not yet");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_eq!(rows(w.up(0, false).await), 1, "unanswered, again");
}

/// Of two-way rows let go of with their edits on the way, the one the
/// database will not retire is rejected and kept as it is, and only the
/// other waits, retired, to be let go of once filed.
#[sqlx::test(migrations = false)]
async fn a_two_way_row_that_cannot_be_held_for_its_edit_is_rejected_alone(pool: SqlitePool) {
    let w = world(pool).await;
    let [stuck, held] = ids();
    let task = |id: Uuid, title: &str| {
        let row = Task {
            id,
            title: title.into(),
            done: false,
            updated_at: at(0),
        };
        (key(&[&id.to_string()]), json(&row))
    };
    w.receive(&down(
        1,
        "",
        vec![upserts(
            "tasks",
            vec![task(stuck, "stuck"), task(held, "held")],
        )],
    ))
    .await;
    w.exec(
        "CREATE TRIGGER unretired BEFORE UPDATE OF retired_at ON tasks WHEN OLD.title = 'stuck' \
         BEGIN SELECT RAISE(ABORT, 'not this one'); END;
         UPDATE tasks SET done = 1;",
    )
    .await
    .unwrap();
    let up = w.up(0, false).await.expect("the edits");
    w.receive(&down(
        2,
        "",
        vec![deletes(
            "tasks",
            vec![key(&[&stuck.to_string()]), key(&[&held.to_string()])],
        )],
    ))
    .await;
    assert_eq!(w.rejected().await, [format!("tasks {stuck}")]);
    assert_eq!(
        w.texts("SELECT row_key FROM sync_leaving").await,
        [held.to_string()]
    );
    w.receive(
        &Body::Filed(Filed {
            uploads: up.uploads,
        })
        .encoded(),
    )
    .await;
    assert_eq!(
        w.texts("SELECT title FROM tasks").await,
        ["stuck"],
        "the held one let go of once filed"
    );
}

/// A row whose refusal rolls the whole message back is found by applying it
/// again, so a message of many costs one rerun for each: five hundred of five
/// hundred and five are all refused, and the message applied, in seconds.
#[sqlx::test(migrations = false)]
async fn five_hundred_rows_that_each_roll_the_message_back_are_all_refused(pool: SqlitePool) {
    let w = world(pool).await;
    w.exec(
        "CREATE TRIGGER doomed BEFORE INSERT ON items WHEN NEW.name = 'doom' \
         BEGIN SELECT RAISE(ROLLBACK, 'doomed'); END",
    )
    .await
    .unwrap();
    let rows: Vec<_> = (0..505u128)
        .map(|n| {
            item(
                Uuid::from_u128(n + 1),
                if n % 101 < 100 { "doom" } else { "fine" },
            )
        })
        .collect();
    let started = std::time::Instant::now();
    w.receive(&down(1, "", vec![upserts("items", rows)])).await;
    assert_eq!(w.items().await.len(), 5);
    assert_eq!(w.rejected().await.len(), 500);
    assert!(
        started.elapsed() < std::time::Duration::from_secs(60),
        "{:?}",
        started.elapsed()
    );
}

/// A row held retired for its edits, that the database then refuses to let
/// go of once they are filed, is not stuck without a word: it is rejected,
/// for the server to hear of and the operator to see.
#[sqlx::test(migrations = false)]
async fn a_row_the_database_will_not_let_go_of_once_filed_is_rejected(pool: SqlitePool) {
    let w = world(pool).await;
    let [id] = ids();
    let row = Task {
        id,
        title: "held".into(),
        done: false,
        updated_at: at(0),
    };
    w.receive(&down(
        1,
        "",
        vec![upserts(
            "tasks",
            vec![(key(&[&id.to_string()]), json(&row))],
        )],
    ))
    .await;
    w.exec(
        "UPDATE tasks SET done = 1;
         CREATE TRIGGER stays BEFORE DELETE ON tasks BEGIN SELECT RAISE(ABORT, 'it stays'); END;",
    )
    .await
    .unwrap();
    let up = w.up(0, false).await.expect("the edit");
    w.receive(&down(
        2,
        "",
        vec![deletes("tasks", vec![key(&[&id.to_string()])])],
    ))
    .await;
    assert!(w.rejected().await.is_empty(), "held retired for its edit");
    w.receive(
        &Body::Filed(Filed {
            uploads: up.uploads,
        })
        .encoded(),
    )
    .await;
    assert_eq!(w.rejected().await, [format!("tasks {id}")]);
    let up = w.up(0, false).await.expect("the rejection goes up");
    assert_eq!(up.rejected.len(), 1);
    assert_eq!(up.rejected[0].sequence, BASE + 2);
}

/// A full disk while a message is applied row by row — after a refusal
/// rolled it back — still fails the message, whole, rather than rejecting
/// the row it happened on.
#[sqlx::test(migrations = false)]
async fn a_disk_full_while_applying_row_by_row_fails_the_message(pool: SqlitePool) {
    let w = world(pool).await;
    w.exec(
        "CREATE TRIGGER doomed BEFORE INSERT ON items WHEN NEW.name = 'doom' \
         BEGIN SELECT RAISE(ROLLBACK, 'doomed'); END",
    )
    .await
    .unwrap();
    let pages: i64 = w.count("PRAGMA page_count").await;
    let options = (*w.pool.connect_options())
        .clone()
        .pragma("max_page_count", (pages + 2).to_string());
    let full = SqlitePool::connect_with(options).await.unwrap();
    let cramped = Replica::new(full, codecs(), w.me, w.uplink.clone(), Settings::default())
        .await
        .unwrap();
    let mut rows: Vec<(WireKey, Vec<u8>)> = (0..2000)
        .map(|n| item(Uuid::now_v7(), &format!("{n:0>200}")))
        .collect();
    rows.insert(0, item(Uuid::now_v7(), "doom"));
    match cramped
        .receive(&down(1, "", vec![upserts("items", rows)]))
        .await
    {
        Err(e) => assert!(e.is_transient(), "{e}"),
        Ok(received) => panic!("applied on a full disk: {received:?}"),
    }
    assert_eq!(w.count("SELECT count(*) FROM items").await, 0);
    assert!(w.rejected().await.is_empty());
}

/// A table this build does not know is kept to report up to the cap, to
/// the byte, and not past it.
#[sqlx::test(migrations = false)]
async fn an_unknown_tables_name_and_key_are_kept_to_the_byte(pool: SqlitePool) {
    let w = world(pool).await;
    let at_cap = "t".repeat(TEXT_CAP);
    let past = "t".repeat(TEXT_CAP + 1);
    let keyed = |table: &str, key_text: String| Table {
        table: table.to_owned(),
        upserts: vec![vec![1]],
        keys: vec![key(&[&key_text])],
        ..Table::default()
    };
    w.receive(&down(
        1,
        "",
        vec![
            keyed(&at_cap, "k".into()),
            keyed(&past, "k".into()),
            keyed("u", "k".repeat(TEXT_CAP)),
            keyed("v", "k".repeat(TEXT_CAP + 1)),
        ],
    ))
    .await;
    assert_eq!(
        w.texts("SELECT table_name FROM sync_rejections ORDER BY 1")
            .await,
        [at_cap, "u".to_owned()]
    );
}

/// What the database fails with that is not the rows' — a host's trigger
/// that reads a table there is not — fails the message, rather than
/// refusing every row it touched: the next build may fix it.
#[sqlx::test(migrations = false)]
async fn a_failure_that_is_not_the_rows_fails_the_message(pool: SqlitePool) {
    let w = world(pool).await;
    w.exec(
        "CREATE TRIGGER broken BEFORE INSERT ON items \
         BEGIN SELECT count(*) FROM no_such_table; END",
    )
    .await
    .unwrap();
    let message = down(
        1,
        "",
        vec![upserts("items", vec![item(Uuid::now_v7(), "a")])],
    );
    match w.replica.receive(&message).await {
        Err(Error::Database(e)) => assert!(e.to_string().contains("no such table"), "{e}"),
        other => panic!("{other:?}"),
    }
    assert!(w.rejected().await.is_empty());
    w.exec("DROP TRIGGER broken").await.unwrap();
    w.receive(&message).await;
    assert_eq!(w.items().await, ["a"]);
}

/// A table takes a replica's deletes only when keyed by it — alone or with
/// other columns — and history does not.
#[test]
fn a_table_takes_deletes_only_when_keyed_by_its_replica() {
    #[derive(
        sqlx::FromRow, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, crate::Row,
    )]
    #[row(
        table = "beacons",
        mirror = "record",
        key = "replica_id",
        origin = "replica_id"
    )]
    struct Beacon {
        replica_id: Uuid,
        #[row(version)]
        seen: i64,
    }
    assert!(Codec::<()>::json::<Beacon>(None).takes_deletes());
    assert!(Codec::<()>::json::<Reading>(None).takes_deletes());
    assert!(!Codec::<()>::json::<Event>(None).takes_deletes());
}

#[path = "replica_walk_tests.rs"]
mod walk;

/// A replica left at its defaults never builds an upload a server left at
/// its own drops as beyond its limits: both take the one default.
#[test]
fn a_replicas_default_limits_are_the_servers() {
    assert_eq!(Outbox::default().limits, Limits::default());
}
