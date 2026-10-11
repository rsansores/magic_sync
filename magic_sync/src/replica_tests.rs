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
use crate::conformance::{codecs, json, Event, Price, Reading, Shelf, Task, Uplinked};
use crate::envelope::{Backlog, Down, EndSnapshot, Filed, Key as WireKey, Table, ToReplica};
use crate::run::replica::{Replica, Settings};
use crate::testkit::ItemRow;
use chrono::{TimeZone, Utc};
use sqlx::SqlitePool;

const TABLES: &str = "
CREATE TABLE items (id BLOB PRIMARY KEY NOT NULL, partition_id BLOB NOT NULL, name TEXT NOT NULL, retired_at TEXT);
CREATE TABLE shelves (aisle BLOB NOT NULL, slot INTEGER NOT NULL, label TEXT NOT NULL, revision INTEGER NOT NULL,
    PRIMARY KEY (aisle, slot));
CREATE TABLE prices (id BLOB PRIMARY KEY NOT NULL, cents INTEGER NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE tasks (id BLOB PRIMARY KEY NOT NULL, title TEXT NOT NULL, done INTEGER NOT NULL, updated_at TEXT NOT NULL);
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
    sqlx::raw_sql(TABLES).execute(&pool).await.expect("tables");
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

fn numbered(sequence: u64, previous: u64, snapshot: &str, tables: Vec<Table>) -> Vec<u8> {
    Body::Down(Down {
        snapshot_id: snapshot.to_owned(),
        tables,
        sequence,
        epoch: EPOCH.to_owned(),
        previous,
    })
    .encoded()
}

/// Message `n`, ending the snapshot in progress, `snapshot`.
fn end(n: u64, snapshot: &str, tables: &[&str]) -> Vec<u8> {
    ending(BASE + n, BASE + n - 1, snapshot, tables)
}

fn ending(sequence: u64, previous: u64, snapshot: &str, tables: &[&str]) -> Vec<u8> {
    Body::EndSnapshot(EndSnapshot {
        snapshot_id: snapshot.to_owned(),
        tables: tables.iter().map(|t| (*t).to_owned()).collect(),
        sequence,
        epoch: EPOCH.to_owned(),
        previous,
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

/// The server's own messages (`tests/golden`, captured from the postgres
/// side's relay) leave the replica holding, step by step, what the server
/// holds — its encodings, its numbering and its snapshots as they are.
#[sqlx::test(migrations = false)]
async fn the_servers_own_messages_leave_the_replica_holding_what_it_holds(pool: SqlitePool) {
    use crate::conformance::{golden, GOLDEN};
    let w = world(pool).await;
    let messages = golden(&std::fs::read_to_string(GOLDEN).expect("the golden messages"));
    let mut steps: Vec<&str> = Vec::new();
    for (step, _) in &messages {
        if steps.last() != Some(&step.as_str()) {
            steps.push(step);
        }
    }
    let stray = || async {
        // A row the server no longer has, held here: a snapshot sweeps it.
        sqlx::raw_sql(&format!(
            "INSERT INTO sync_applying VALUES (1); \
             INSERT INTO items (id, partition_id, name) VALUES (x'{}', x'00', 'stray'); \
             DELETE FROM sync_applying;",
            Uuid::now_v7().simple()
        ))
        .execute(&w.pool)
        .await
        .unwrap();
    };
    let state = || async {
        let all = |sql: &'static str| w.texts(sql);
        (
            all("SELECT name FROM items ORDER BY name").await,
            all("SELECT label || ' ' || revision FROM shelves ORDER BY slot").await,
            all("SELECT CAST(cents AS TEXT) FROM prices").await,
            all("SELECT title || ' ' || done FROM tasks").await,
        )
    };
    let owned = |v: &[&str]| v.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
    for step in steps {
        if step.contains("snapshot") {
            stray().await;
        }
        for (_, bytes) in messages.iter().filter(|(s, _)| s == step) {
            let received = w.receive(bytes).await;
            assert!(
                matches!(received, Received::Applied(_) | Received::SnapshotEnded),
                "{step}: {received:?}"
            );
        }
        let expected = match step {
            "rows" => (
                owned(&["a", "b"]),
                owned(&["top 1800000000000", "bottom 1800000000000"]),
                owned(&["9007199254740993"]),
                owned(&["count 0"]),
            ),
            "update" => (
                owned(&["a2", "b"]),
                owned(&["top2 1800000000001", "bottom 1800000000000"]),
                owned(&["250"]),
                owned(&["recount 1"]),
            ),
            "delete" | "snapshot" => (
                owned(&["a2"]),
                owned(&["top2 1800000000001"]),
                owned(&["250"]),
                owned(&["recount 1"]),
            ),
            "emptied" | "empty_snapshot" => (vec![], vec![], vec![], vec![]),
            other => panic!("a step this suite does not know: {other}"),
        };
        assert_eq!(state().await, expected, "after {step}");
        if step == "rows" {
            // The replica's own column, kept through the server's edits.
            w.exec("UPDATE tasks SET done = 1").await.unwrap();
        }
    }
    assert!(w.rejected().await.is_empty(), "{:?}", w.rejected().await);
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

/// A message that follows one the replica never applied — lost on the way,
/// or a replica restored from an older copy — is a gap: it applies nothing
/// more, says so to the server once, and with every message up after, until
/// a snapshot begins; then it follows that.
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
    assert!(told.gap);
    assert_eq!((told.epoch.as_str(), told.sequence), (EPOCH, BASE + 1));
    assert!(w.up(0, false).await.is_none(), "said once");
    let s = Uuid::now_v7().to_string();
    w.receive(&begin(5, &s, vec![upserts("items", vec![item(c, "c")])]))
        .await;
    w.receive(&end(6, &s, &["items"])).await;
    assert_eq!(w.items().await, ["c"]);
    let up = w.up(0, false).await.expect("what it applied");
    assert!(!up.gap);
    assert_eq!(up.sequence, BASE + 6);
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

/// The newest version wins whichever arrives first, for an integer version
/// and for an instant — and an equal one is applied.
#[sqlx::test(migrations = false)]
async fn the_newest_version_wins_whatever_its_type_and_order(pool: SqlitePool) {
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
    assert_eq!(w.texts(label).await, ["five"], "older: kept the newer");
    w.receive(&send(shelf(ms + 10, "ten"))).await;
    assert_eq!(w.texts(label).await, ["ten"], "newer: taken");
    w.receive(&send(shelf(ms + 10, "ten again"))).await;
    assert_eq!(w.texts(label).await, ["ten again"], "equal: taken");

    w.receive(&send(price_at(60, 600))).await;
    w.receive(&send(price_at(30, 300))).await;
    assert_eq!(w.texts(cents).await, ["600"]);
    w.receive(&send(price_at(61, 610))).await;
    assert_eq!(w.texts(cents).await, ["610"]);
    // Stored by another writer in another form, the instant still compares.
    w.exec(&format!(
        "UPDATE prices SET updated_at = '{}'",
        at(120).format("%Y-%m-%d %H:%M:%S+00:00")
    ))
    .await
    .unwrap_err();
    sqlx::raw_sql("INSERT INTO sync_applying VALUES (1)")
        .execute(&w.pool)
        .await
        .unwrap();
    w.exec(&format!(
        "UPDATE prices SET updated_at = '{}'",
        at(120).format("%Y-%m-%d %H:%M:%S+00:00")
    ))
    .await
    .unwrap();
    w.exec("DELETE FROM sync_applying").await.unwrap();
    w.receive(&send(price_at(90, 900))).await;
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
    assert_eq!(w.rejected().await.len(), 2);
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

    w.receive(&down(
        3,
        "",
        vec![deletes("tasks", vec![key(&[&id.to_string()])])],
    ))
    .await;
    assert_eq!(w.count("SELECT count(*) FROM tasks").await, 0);
    assert_eq!(
        w.count("SELECT count(*) FROM sync_notes WHERE upload IS NULL")
            .await,
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
        let outgoing = outbox(&mut tx, &codecs(), &lanes, 0, &settings, false)
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
    assert_eq!((lost.rows(), late.rows()), (1, 1));
    w.exec("UPDATE sync_notes SET sent_at = '2000-01-01T00:00:00.000Z'")
        .await
        .unwrap();
    assert_eq!(send().await.rows(), 2, "both go again");
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
    let outgoing = outbox(&mut tx, &codecs(), &lanes, 0, &Outbox::default(), false)
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
                let outgoing = outbox(&mut tx, &codecs(), lanes, 0, settings, false)
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

/// A message up never outgrows the transport's bytes: what does not fit
/// waits for the next, and a row no message can carry is set aside — and
/// counted — rather than offered for ever.
#[sqlx::test(migrations = false)]
async fn a_message_up_fits_the_transport_and_a_row_too_large_is_set_aside(pool: SqlitePool) {
    let limits = Limits {
        rows: 100,
        bytes: envelope::ENVELOPE_ROOM + 600,
    };
    let settings = Settings {
        outbox: Outbox {
            limits,
            ..Outbox::default()
        },
        ..Settings::default()
    };
    let w = world_with(pool, settings).await;
    for what in [
        "a".repeat(200),
        "b".repeat(200),
        "c".repeat(200),
        "d".repeat(2000),
    ] {
        sqlx::query("INSERT INTO events (id, what) VALUES ($1, $2)")
            .bind(Uuid::now_v7())
            .bind(what)
            .execute(&w.pool)
            .await
            .unwrap();
    }
    let mut rows = 0;
    while let Some(up) = w.up(0, false).await {
        assert!(
            up.encoded_len() <= limits.bytes,
            "{} bytes",
            up.encoded_len()
        );
        rows += up.tables.iter().map(|t| t.upserts.len()).sum::<usize>();
        if up.tables.is_empty() {
            break;
        }
    }
    assert_eq!(rows, 3);
    let mut conn = w.pool.acquire().await.unwrap();
    let progress = progress(&mut conn).await.unwrap();
    assert_eq!((progress[0].set_aside, progress[0].unfiled), (1, 3));
}

/// A message the uplink refuses for good does not come back for ever: its
/// rows are set aside, the rest of what the replica has to say goes, and
/// the rows are offered again when the replica runs again.
#[sqlx::test(migrations = false)]
async fn rows_the_uplink_refuses_for_good_are_set_aside_until_the_next_run(pool: SqlitePool) {
    let w = world(pool.clone()).await;
    sqlx::query("INSERT INTO events (id, what) VALUES ($1, 'x')")
        .bind(Uuid::now_v7())
        .execute(&w.pool)
        .await
        .unwrap();
    *w.uplink.refusing.lock().unwrap() = Some(false);
    assert!(w.replica.send_lane(0, false).await.unwrap(), "set aside");
    *w.uplink.refusing.lock().unwrap() = None;
    let up = w.up(0, false).await.expect("the backlog");
    assert!(up.tables.is_empty());
    assert_eq!(up.backlog.unwrap().tables[0].set_aside, 1);

    let again = Replica::new(pool, codecs(), w.me, w.uplink.clone(), Settings::default())
        .await
        .unwrap();
    again.send_lane(0, false).await.unwrap();
    let (_, up) = w.uplink.taken().pop().unwrap();
    assert_eq!(up.tables[0].upserts.len(), 1, "offered again");
}

/// Each lane keeps its own pace: one the uplink fails on does not hold back
/// another.
#[sqlx::test(migrations = false)]
async fn a_lane_that_fails_holds_back_no_other(pool: SqlitePool) {
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
    let outgoing = outbox(&mut tx, &codecs(), &lanes, 0, &Outbox::default(), false)
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
            let outgoing = outbox(&mut tx, &codecs(), &lanes, lane, &settings, false)
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
            UPDATE sync_backlog SET reported_at = 32503680000",
    )
    .await
    .unwrap();
    let up = w.up(0, false).await.expect("sent again");
    assert_eq!(up.tables[0].upserts.len(), 1);
    // A backlog told "in the future", unchanged and not empty: said again.
    w.exec("UPDATE sync_backlog SET reported_at = 32503680000")
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
        "DROP TRIGGER sync_note_readings_update; CREATE TRIGGER sync_note_readings_update \
         AFTER UPDATE ON readings BEGIN INSERT INTO sync_notes (relation, row_key) VALUES ('readings', NEW.id); END",
        "DROP TRIGGER sync_note_events_delete",
        "CREATE TRIGGER sync_note_gone_insert AFTER INSERT ON receipts BEGIN SELECT 1; END",
        "CREATE TRIGGER by_hand AFTER INSERT ON receipts BEGIN \
         INSERT INTO sync_notes (relation, row_key) VALUES ('receipts', NEW.id); END",
    ];
    for case in cases {
        sqlx::raw_sql(case).execute(&mut *conn).await.unwrap();
        let refused = check(&mut conn, &codecs()).await;
        assert!(refused.is_err(), "{case}");
        if case.contains("by_hand") {
            sqlx::raw_sql("DROP TRIGGER by_hand")
                .execute(&mut *conn)
                .await
                .unwrap();
        }
        install(&mut conn, &codecs()).await.expect("put right");
    }
    sqlx::raw_sql("PRAGMA synchronous = OFF")
        .execute(&mut *conn)
        .await
        .unwrap();
    assert!(
        check(&mut conn, &codecs()).await.is_err(),
        "commits not durable"
    );
}

/// A database whose engine tables are of another version is refused, with
/// what brings it to this build's: an install for one behind, never for one
/// a later build wrote.
#[sqlx::test(migrations = false)]
async fn a_database_of_another_version_is_refused_with_the_way_forward(pool: SqlitePool) {
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
    let mut conn = pool.acquire().await.unwrap();
    // `tasks` is missing: its triggers cannot be created.
    sqlx::raw_sql(&TABLES.replace("CREATE TABLE tasks", "CREATE TABLE not_tasks"))
        .execute(&mut *conn)
        .await
        .unwrap();
    assert!(install(&mut conn, &codecs()).await.is_err());
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
    spawnable(receive(&mut conn, &codecs(), &[]));
    let settings = Outbox::default();
    spawnable(outbox(
        &mut conn,
        &codecs(),
        &Settings::default().lanes,
        0,
        &settings,
        true,
    ));
    spawnable(install(&mut conn, &codecs()));
}
