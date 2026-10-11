//! A replica walked at random against a model of it: a server sends what a
//! server sends — rows, deletes, snapshots, new epochs — through a transport
//! that loses and redelivers; the host writes, rewrites and deletes its own
//! rows; the uplink fails for a while and for good; and the power goes, in
//! the middle of a message, of an upload, of a write, after a send. After
//! every step the replica holds what the protocol's model of it holds,
//! confirms nothing it has not told every refusal of, keeps what it stores
//! bounded, and has lost no write: each is either still noted or filed with
//! its latest content. At the end, with the faults gone, everything it wrote
//! is filed, once, as it is.

use super::*;
use crate::model::{self, Model, Rules};
use crate::testkit::Walk;
use std::collections::{BTreeMap, VecDeque};

/// Rows the walk's server names: few, so the same rows come back.
const ITEMS: u128 = 6;
const PRICES: u128 = 3;
const READINGS: u128 = 4;

/// What the replica's database refuses: an item named `bad…` (with `ABORT`)
/// or `doom…` (with `ROLLBACK`, the whole message), and the let-go of one
/// named `keep…`; and a price a quote of its own points at.
const BUGS: &str = "
CREATE TRIGGER bad_item BEFORE INSERT ON items WHEN NEW.name LIKE 'bad%'
BEGIN SELECT RAISE(ABORT, 'a bad item'); END;
CREATE TRIGGER bad_item_again BEFORE UPDATE ON items WHEN NEW.name LIKE 'bad%'
BEGIN SELECT RAISE(ABORT, 'a bad item'); END;
CREATE TRIGGER doomed_item BEFORE INSERT ON items WHEN NEW.name LIKE 'doom%'
BEGIN SELECT RAISE(ROLLBACK, 'a doomed item'); END;
CREATE TRIGGER doomed_item_again BEFORE UPDATE ON items WHEN NEW.name LIKE 'doom%'
BEGIN SELECT RAISE(ROLLBACK, 'a doomed item'); END;
CREATE TRIGGER kept_item BEFORE DELETE ON items WHEN OLD.name LIKE 'keep%'
BEGIN SELECT RAISE(ABORT, 'an item kept'); END;
";

fn name_of(bytes: &[u8]) -> String {
    ItemRow::decode(bytes)
        .map(|row| row.name)
        .unwrap_or_default()
}

fn cents_of(bytes: &[u8]) -> i64 {
    let value: serde_json::Value = serde_json::from_slice(bytes).expect("a price");
    value["cents"]
        .as_str()
        .expect("cents as text")
        .parse()
        .unwrap()
}

/// The model's rules: what the walk's triggers and quotes refuse.
const RULES: Rules = Rules {
    refuses: |table, bytes| {
        table == "items" && {
            let name = name_of(bytes);
            name.starts_with("bad") || name.starts_with("doom")
        }
    },
    parents: |_, _| Vec::new(),
};

/// The walk's server: what it holds, what it sent this replica, and the
/// transport between them.
struct Server {
    epoch: String,
    /// The last message it numbered for this replica.
    last: u64,
    items: BTreeMap<Uuid, String>,
    prices: BTreeMap<Uuid, (i64, i64)>,
    /// What the transport holds for the replica, in order.
    queue: VecDeque<Vec<u8>>,
    /// Everything delivered of its epoch, to deliver again: one of an
    /// installation since replaced is no message a replica must follow.
    delivered: Vec<Vec<u8>>,
    /// A snapshot is owed: the replica said it has a gap.
    owed: bool,
    /// What the replica filed here: its readings, newest wins, and events.
    readings: BTreeMap<String, (i64, i64)>,
    events: BTreeSet<String>,
    /// The last applied it was told, of its epoch.
    confirmed: u64,
}

impl Server {
    fn numbered(&mut self, snapshot: &str, tables: Vec<Table>, begins: bool) -> Vec<u8> {
        let previous = if begins { 0 } else { self.last };
        self.last += 1;
        Body::Down(Down {
            snapshot_id: snapshot.to_owned(),
            tables,
            sequence: self.last,
            epoch: self.epoch.clone(),
            previous,
        })
        .encoded()
    }

    fn price(id: Uuid, cents: i64, version: i64) -> (WireKey, Vec<u8>) {
        let price = Price {
            id,
            cents,
            updated_at: at(version),
        };
        (key(&[&id.to_string()]), json(&price))
    }

    /// A message of changes, as a pass would send them.
    fn changes(&mut self, walk: &mut Walk) {
        let mut items = Vec::new();
        let mut gone = Vec::new();
        for _ in 0..1 + walk.below(3) {
            let id = Uuid::from_u128(1 + walk.below(ITEMS as usize) as u128);
            if walk.below(4) == 0 {
                self.items.remove(&id);
                gone.push(key(&[&id.to_string()]));
                continue;
            }
            let name = ["fine", "fine", "fine", "bad", "doom", "keep"][walk.below(6)];
            let name = format!("{name}{}", walk.below(100));
            self.items.insert(id, name.clone());
            gone.retain(|k| k.parts[0] != id.to_string());
            items.push(item(id, &name));
        }
        // One row a message, at most, so the order of a row's own changes
        // is the order of the message.
        items.dedup_by_key(|(k, _)| k.parts.clone());
        let mut seen = BTreeSet::new();
        items.retain(|(k, _)| seen.insert(k.parts.clone()));
        items.retain(|(k, _)| !gone.iter().any(|g| g.parts == k.parts));
        let mut prices = Vec::new();
        let mut unpriced = Vec::new();
        if walk.below(2) == 0 {
            let id = Uuid::from_u128(100 + walk.below(PRICES as usize) as u128);
            if walk.below(4) == 0 {
                self.prices.remove(&id);
                unpriced.push(key(&[&id.to_string()]));
            } else {
                // Newer every time, whatever was let go of meanwhile.
                let version = i64::try_from(self.last).unwrap();
                let cents = i64::try_from(walk.below(1000)).unwrap();
                self.prices.insert(id, (cents, version));
                prices.push(Self::price(id, cents, version));
            }
        }
        let tables = vec![
            upserts("items", items),
            upserts("prices", prices),
            deletes("prices", unpriced),
            deletes("items", gone),
        ];
        let bytes = self.numbered("", tables, false);
        self.queue.push_back(bytes);
    }

    /// A snapshot of everything it holds, in messages of a few rows, after
    /// whatever the transport holds — or in place of it, when it drops what
    /// it was about to send.
    fn snapshot(&mut self, walk: &mut Walk) {
        let id = walk.uuid().to_string();
        let mut rows: Vec<Table> = self
            .items
            .iter()
            .map(|(id, name)| upserts("items", vec![item(*id, name)]))
            .chain(
                self.prices
                    .iter()
                    .map(|(id, (c, v))| upserts("prices", vec![Self::price(*id, *c, *v)])),
            )
            .collect();
        // Prices before items, as a server lists its tables: here either
        // order holds, nothing points between them.
        rows.rotate_left(self.items.len());
        let mut begins = true;
        for chunk in rows.chunks(2) {
            let bytes = self.numbered(&id, chunk.to_vec(), begins);
            self.queue.push_back(bytes);
            begins = false;
        }
        let end = Body::EndSnapshot(EndSnapshot {
            snapshot_id: id.clone(),
            tables: vec!["prices".to_owned(), "items".to_owned()],
            sequence: self.last + 1,
            epoch: self.epoch.clone(),
            previous: if begins { 0 } else { self.last },
        })
        .encoded();
        self.last += 1;
        self.queue.push_back(end);
        self.owed = false;
    }

    /// Files what the replica sent up: rows as a server files them, and
    /// what it says of itself. Answers the upload's number, to answer.
    fn file(&mut self, up: &Up, me: Uuid) -> Option<u64> {
        for table in &up.tables {
            for row in &table.upserts {
                let value: serde_json::Value = serde_json::from_slice(row).expect("json");
                match table.table.as_str() {
                    "readings" => {
                        let key = format!(
                            "{}|{}",
                            value["replica_id"].as_str().unwrap(),
                            value["id"].as_str().unwrap()
                        );
                        assert!(key.starts_with(&me.to_string()), "another replica's row");
                        let new = (
                            value["value"].as_str().unwrap().parse().unwrap(),
                            value["at"].as_str().unwrap().parse().unwrap(),
                        );
                        let stored = self.readings.entry(key).or_insert(new);
                        if new.1 >= stored.1 {
                            *stored = new;
                        }
                    }
                    "events" => {
                        self.events.insert(value["id"].as_str().unwrap().to_owned());
                    }
                    other => panic!("an upload of `{other}`"),
                }
            }
            for key in &table.deletes {
                assert_eq!(table.table, "readings", "deletes of history");
                self.readings.remove(&key.parts.join("|"));
            }
        }
        if up.gap {
            self.owed = true;
        }
        if up.epoch == self.epoch {
            self.confirmed = self.confirmed.max(up.sequence);
        }
        up.uploads.first().copied()
    }
}

/// What the host wrote: each reading's latest value and version, or `None`
/// once deleted, and every version it ever had; every event.
#[derive(Default)]
struct Host {
    readings: BTreeMap<String, Option<(i64, i64)>>,
    versions: BTreeMap<String, BTreeSet<(i64, i64)>>,
    events: BTreeSet<String>,
    clock: i64,
}

/// A replica as a host runs it, over a pool it may lose at any moment.
struct Node {
    pool: SqlitePool,
    replica: Replica<(), Uuid, Uplinked>,
}

fn settings() -> Settings {
    Settings {
        outbox: Outbox {
            limits: Limits::new(3, 5000).unwrap(),
            rejections: std::num::NonZeroUsize::new(2).unwrap(),
            ..Outbox::default()
        },
        lanes: vec![Lane::new("main", &[]), Lane::new("history", &["events"])],
        ..Settings::default()
    }
}

async fn boot(file: &sqlx::sqlite::SqliteConnectOptions, me: Uuid, uplink: &Uplinked) -> Node {
    let pool = SqlitePool::connect_with(file.clone()).await.unwrap();
    let replica = Replica::new(pool.clone(), codecs(), me, uplink.clone(), settings())
        .await
        .unwrap();
    Node { pool, replica }
}

fn same(model: &model::Received, real: &Received) -> bool {
    matches!(
        (model, real),
        (model::Received::Applied, Received::Applied(_))
            | (model::Received::Ended, Received::SnapshotEnded)
            | (model::Received::Filed, Received::Filed)
            | (model::Received::Already, Received::AlreadyApplied)
            | (model::Received::Gap, Received::Gap)
            | (model::Received::Malformed, Received::Malformed(_))
    )
}

/// The rows the model says the replica pins: an item named `keep…`, a
/// price a quote points at.
async fn pinned(pool: &SqlitePool, model: &Model) -> BTreeSet<model::Row> {
    let mut pinned: BTreeSet<model::Row> = model
        .rows
        .iter()
        .filter(|((table, _), bytes)| table == "items" && name_of(bytes).starts_with("keep"))
        .map(|(row, _)| row.clone())
        .collect();
    let quoted: Vec<Uuid> = sqlx::query_scalar("SELECT price_id FROM quotes")
        .fetch_all(pool)
        .await
        .unwrap();
    pinned.extend(
        quoted
            .into_iter()
            .map(|id| ("prices".to_owned(), id.to_string())),
    );
    pinned
}

/// What the replica holds of the server's rows, its rejections, and where
/// it stands, against the model.
async fn holds_what_the_model_holds(pool: &SqlitePool, model: &Model, at: &str) {
    let items: Vec<(Uuid, String)> =
        sqlx::query_as("SELECT id, name FROM items WHERE retired_at IS NULL")
            .fetch_all(pool)
            .await
            .unwrap();
    let prices: Vec<(Uuid, i64)> = sqlx::query_as("SELECT id, cents FROM prices")
        .fetch_all(pool)
        .await
        .unwrap();
    let held: BTreeMap<model::Row, String> = items
        .into_iter()
        .map(|(id, name)| (("items".to_owned(), id.to_string()), name))
        .chain(
            prices
                .into_iter()
                .map(|(id, cents)| (("prices".to_owned(), id.to_string()), cents.to_string())),
        )
        .collect();
    let modelled: BTreeMap<model::Row, String> = model
        .rows
        .iter()
        .map(|(row, bytes)| {
            let content = if row.0 == "items" {
                name_of(bytes)
            } else {
                cents_of(bytes).to_string()
            };
            (row.clone(), content)
        })
        .collect();
    assert_eq!(held, modelled, "{at}: the rows held");
    let rejected: BTreeSet<model::Row> =
        sqlx::query_as("SELECT table_name, row_key FROM sync_rejections")
            .fetch_all(pool)
            .await
            .unwrap()
            .into_iter()
            .collect();
    let modelled: BTreeSet<model::Row> = model.rejections.keys().cloned().collect();
    assert_eq!(rejected, modelled, "{at}: the rows rejected");
    let applied: (String, i64, bool) =
        sqlx::query_as("SELECT epoch, sequence, gap = 1 FROM sync_applied")
            .fetch_one(pool)
            .await
            .unwrap();
    let modelled = model.applied.clone().expect("provisioned");
    assert_eq!(
        (applied.0, u64::try_from(applied.1).unwrap(), applied.2),
        (modelled.0, modelled.1, model.gap),
        "{at}: where it stands"
    );
}

/// What the replica stores of its own bookkeeping stays bounded, and it
/// confirms nothing it has not told every refusal of.
async fn keeps_its_word(pool: &SqlitePool, host: &Host, model: &Model, at: &str) {
    let notes = count(pool, "SELECT count(*) FROM sync_notes").await;
    let rows = i64::try_from(host.readings.len() + host.events.len()).unwrap();
    assert!(notes <= rows, "{at}: {notes} notes of {rows} rows");
    let rejections = count(pool, "SELECT count(*) FROM sync_rejections").await;
    assert!(rejections <= i64::try_from(model.rejections.len()).unwrap());
    let named = count(pool, "SELECT count(*) FROM sync_snapshot_keys").await;
    assert!(
        named <= i64::try_from(ITEMS + PRICES).unwrap(),
        "{at}: {named} named"
    );
    assert_eq!(count(pool, "SELECT count(*) FROM sync_leaving").await, 0);
    let untold = count(
        pool,
        "SELECT count(*) FROM sync_rejections r JOIN sync_applied a ON r.epoch = a.epoch \
          WHERE r.sequence <= a.acknowledged AND r.reported_at IS NULL",
    )
    .await;
    assert_eq!(untold, 0, "{at}: confirmed with a refusal untold");
}

/// No write is lost: each is filed with its latest content, or still noted
/// to go up; and nothing filed is what the host never wrote.
async fn loses_no_write(pool: &SqlitePool, host: &Host, server: &Server, at: &str) {
    let noted: BTreeSet<String> = sqlx::query_scalar("SELECT row_key FROM sync_notes")
        .fetch_all(pool)
        .await
        .unwrap()
        .into_iter()
        .collect();
    for (key, latest) in &host.readings {
        let filed = server.readings.get(key).copied();
        assert!(
            noted.contains(key) || filed == *latest,
            "{at}: reading {key} is {latest:?}, filed {filed:?}, and no longer noted"
        );
    }
    for (key, filed) in &server.readings {
        assert!(
            host.versions.get(key).is_some_and(|v| v.contains(filed)),
            "{at}: reading {key} filed as {filed:?}, never written so"
        );
    }
    for event in &host.events {
        let key = event.clone();
        assert!(
            noted.contains(&key) || server.events.contains(&key),
            "{at}: event {key} lost"
        );
    }
    assert!(
        server.events.is_subset(&host.events),
        "{at}: an event never written"
    );
}

async fn count(pool: &SqlitePool, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(pool).await.expect(sql)
}

/// One seeded walk.
async fn walk(pool: SqlitePool, seed: u64) {
    let w = world(pool).await;
    w.exec(BUGS).await.unwrap();
    let file = (*w.pool.connect_options()).clone();
    let (me, uplink) = (w.me, w.uplink.clone());
    drop(w);
    let mut walk = Walk(seed);
    let mut node = boot(&file, me, &uplink).await;
    let mut model = Model::default();
    let provisioned = ending(BASE, 0, "provisioned", &[]);
    model.receive(&provisioned, &RULES);
    let mut server = Server {
        epoch: EPOCH.to_owned(),
        last: BASE,
        items: BTreeMap::new(),
        prices: BTreeMap::new(),
        queue: VecDeque::new(),
        delivered: vec![provisioned],
        owed: false,
        readings: BTreeMap::new(),
        events: BTreeSet::new(),
        confirmed: 0,
    };
    let mut host = Host::default();
    // Answers the server owes, and may send late or never.
    let mut answers: Vec<u64> = Vec::new();
    for step in 0..400 {
        let at = format!("seed {seed:#x}, step {step}");
        let quiet = step >= 300;
        if quiet && step == 300 {
            // The faults are over: what was set aside goes again once the
            // replica runs again, and everything unanswered goes again.
            *uplink.refusing.lock().unwrap() = None;
            *uplink.cannot_take.lock().unwrap() = None;
            node = boot(&file, me, &uplink).await;
        }
        let action = if quiet {
            20 + walk.below(4)
        } else {
            walk.below(24)
        };
        match action {
            // The server changes rows.
            0..=2 => server.changes(&mut walk),
            // It provisions the replica again; or, installed again, in a
            // new epoch, dropping what the transport held.
            3 if walk.below(4) == 0 => {
                server.epoch = walk.uuid().to_string();
                server.queue.clear();
                server.delivered.clear();
                server.snapshot(&mut walk);
            }
            3 => server.snapshot(&mut walk),
            // The transport loses a message, or delivers an old one again.
            4 if walk.below(3) == 0 => {
                server.queue.pop_front();
            }
            4 => {
                if let Some(old) = walk.pick(&(0..server.delivered.len()).collect::<Vec<_>>()) {
                    let bytes = server.delivered[old].clone();
                    model.receive(&bytes, &RULES);
                    node.replica.receive(&bytes).await.expect("redelivered");
                }
            }
            // The power goes in the middle of a message: nothing of it
            // stays, and the transport delivers it again.
            5 => {
                if let Some(bytes) = server.queue.front() {
                    let mut conn = node.pool.acquire().await.unwrap();
                    let mut tx = sqlx::Connection::begin_with(&mut *conn, "BEGIN IMMEDIATE")
                        .await
                        .unwrap();
                    let _ = receive(&mut tx, &codecs(), bytes, &Care::default()).await;
                    drop(tx);
                    conn.close_on_drop();
                }
                node = boot(&file, me, &uplink).await;
            }
            // The host writes a reading, deletes one, records an event,
            // quotes a price, or loses a write to the power.
            6..=8 => {
                let id = Uuid::from_u128(200 + walk.below(READINGS as usize) as u128);
                let key = format!("{me}|{id}");
                host.clock += 1;
                if walk.below(5) == 0 {
                    sqlx::query("DELETE FROM readings WHERE replica_id = $1 AND id = $2")
                        .bind(me)
                        .bind(id)
                        .execute(&node.pool)
                        .await
                        .unwrap();
                    if host.readings.contains_key(&key) {
                        host.readings.insert(key, None);
                    }
                } else {
                    let value = i64::try_from(walk.below(1000)).unwrap();
                    sqlx::query(
                        "INSERT INTO readings (replica_id, id, value, at) VALUES ($1, $2, $3, $4) \
                         ON CONFLICT (replica_id, id) DO UPDATE SET value = excluded.value, at = excluded.at",
                    )
                    .bind(me)
                    .bind(id)
                    .bind(value)
                    .bind(host.clock)
                    .execute(&node.pool)
                    .await
                    .unwrap();
                    host.readings.insert(key.clone(), Some((value, host.clock)));
                    host.versions
                        .entry(key)
                        .or_default()
                        .insert((value, host.clock));
                }
            }
            9 => {
                let id = walk.uuid();
                let what = if walk.below(6) == 0 { "evil" } else { "fine" };
                sqlx::query("INSERT INTO events (id, what) VALUES ($1, $2)")
                    .bind(id)
                    .bind(what)
                    .execute(&node.pool)
                    .await
                    .unwrap();
                host.events.insert(id.to_string());
            }
            10 => {
                let held: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM prices")
                    .fetch_all(&node.pool)
                    .await
                    .unwrap();
                if walk.below(2) == 0 {
                    if let Some(id) = walk.pick(&held) {
                        sqlx::query("INSERT INTO quotes (price_id) VALUES ($1)")
                            .bind(id)
                            .execute(&node.pool)
                            .await
                            .unwrap();
                    }
                } else {
                    sqlx::query("DELETE FROM quotes WHERE id = (SELECT min(id) FROM quotes)")
                        .execute(&node.pool)
                        .await
                        .unwrap();
                }
            }
            11 => {
                let mut tx = node.pool.begin().await.unwrap();
                sqlx::query("INSERT INTO events (id, what) VALUES ($1, 'never')")
                    .bind(walk.uuid())
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                drop(tx);
            }
            // The uplink fails for a while, refuses one row for good, or
            // comes back.
            12 => *uplink.refusing.lock().unwrap() = Some(true),
            13 => {
                *uplink.refusing.lock().unwrap() = None;
                *uplink.cannot_take.lock().unwrap() = Some(|up: &Up| {
                    up.tables
                        .iter()
                        .flat_map(|t| &t.upserts)
                        .any(|row| String::from_utf8_lossy(row).contains("\"evil\""))
                });
            }
            // The power goes after an upload is made, and either before it
            // went or after the transport took it.
            14 => {
                let lane = walk.below(2);
                let settings = settings();
                let mut tx = node.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
                let outgoing = outbox(
                    &mut tx,
                    &codecs(),
                    &settings.lanes,
                    lane,
                    &settings.outbox,
                    None,
                    None,
                )
                .await
                .unwrap();
                tx.commit().await.unwrap();
                if let (Some(outgoing), 0) = (outgoing, walk.below(2)) {
                    answers.extend(server.file(&outgoing.up, me));
                }
                node = boot(&file, me, &uplink).await;
            }
            // Time passes: what was sent and not answered goes again.
            15 => {
                sqlx::query(
                    "UPDATE sync_notes SET sent_at = '2000-01-01T00:00:00.000Z' WHERE sent_at IS NOT NULL",
                )
                .execute(&node.pool)
                .await
                .unwrap();
            }
            // The server answers an upload, of any it filed, in any order.
            16 if !answers.is_empty() => {
                let number = answers.remove(walk.below(answers.len()));
                if walk.below(5) > 0 {
                    server.queue.push_back(
                        Body::Filed(Filed {
                            uploads: vec![number],
                        })
                        .encoded(),
                    );
                }
            }
            // The replica sends up, on either lane, digests now and then.
            17 | 20 | 21 => {
                let lane = walk.below(2);
                let went = node.replica.send_lane(lane, walk.below(5) == 0).await;
                assert!(quiet || went.is_ok() || uplink.refusing.lock().unwrap().is_some());
                for (_, up) in uplink.taken() {
                    answers.extend(server.file(&up, me));
                }
                if server.owed {
                    server.snapshot(&mut walk);
                }
            }
            // The transport delivers the next message.
            _ => {
                if quiet {
                    if let Some(number) = answers.pop() {
                        server.queue.push_back(
                            Body::Filed(Filed {
                                uploads: vec![number],
                            })
                            .encoded(),
                        );
                    }
                }
                if let Some(bytes) = server.queue.pop_front() {
                    model.pinned = pinned(&node.pool, &model).await;
                    let expected = model.receive(&bytes, &RULES);
                    let received = node.replica.receive(&bytes).await.expect("applied");
                    assert!(
                        same(&expected, &received),
                        "{at}: {expected:?}, and the replica {received:?}"
                    );
                    server.delivered.push(bytes);
                }
            }
        }
        holds_what_the_model_holds(&node.pool, &model, &at).await;
        keeps_its_word(&node.pool, &host, &model, &at).await;
        loses_no_write(&node.pool, &host, &server, &at).await;
    }
    // Quiet: everything goes, and is answered, until nothing is left.
    for round in 0..200 {
        while let Some(bytes) = server.queue.pop_front() {
            model.pinned = pinned(&node.pool, &model).await;
            model.receive(&bytes, &RULES);
            node.replica.receive(&bytes).await.expect("applied");
        }
        let mut said = false;
        for lane in 0..2 {
            said |= node.replica.send_lane(lane, false).await.unwrap().message;
        }
        for (_, up) in uplink.taken() {
            answers.extend(server.file(&up, me));
        }
        if server.owed {
            server.snapshot(&mut walk);
        }
        for number in answers.drain(..) {
            server.queue.push_back(
                Body::Filed(Filed {
                    uploads: vec![number],
                })
                .encoded(),
            );
        }
        if !said
            && server.queue.is_empty()
            && count(&node.pool, "SELECT count(*) FROM sync_notes").await == 0
        {
            break;
        }
        if round % 10 == 9 {
            sqlx::query("UPDATE sync_notes SET sent_at = '2000-01-01T00:00:00.000Z' WHERE sent_at IS NOT NULL")
                .execute(&node.pool)
                .await
                .unwrap();
        }
    }
    let at = format!("seed {seed:#x}, the end");
    holds_what_the_model_holds(&node.pool, &model, &at).await;
    assert_eq!(
        count(&node.pool, "SELECT count(*) FROM sync_notes").await,
        0,
        "{at}: all filed"
    );
    let latest: BTreeMap<String, (i64, i64)> = host
        .readings
        .iter()
        .filter_map(|(k, v)| v.map(|v| (k.clone(), v)))
        .collect();
    assert_eq!(server.readings, latest, "{at}: every reading as it is");
    assert_eq!(server.events, host.events, "{at}: every event, once");
    let applied = model.applied.clone().unwrap();
    assert!(
        model.gap || (server.epoch == applied.0 && server.confirmed == applied.1),
        "{at}: confirmed {} of {:?}",
        server.confirmed,
        applied
    );
}

/// The walk, over seeds enough to meet every fault beside every other.
#[sqlx::test(migrations = false)]
async fn a_replica_walked_at_random_holds_what_the_protocol_says_and_loses_no_write(
    pool: SqlitePool,
) {
    let seeds: u64 = std::env::var("MAGIC_SYNC_WALKS")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(8);
    for seed in 1..=seeds {
        // Each walk on a database of its own.
        let options = (*pool.connect_options())
            .clone()
            .filename(
                std::env::temp_dir()
                    .join(format!("magic_sync_walk_{}_{seed}.db", std::process::id())),
            )
            .create_if_missing(true);
        let walk_pool = SqlitePool::connect_with(options.clone()).await.unwrap();
        walk(walk_pool, seed.wrapping_mul(0x9e37_79b9_7f4a_7c15)).await;
        let path = options.get_filename().to_owned();
        for suffix in ["", "-wal", "-shm", "-journal"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }
}
