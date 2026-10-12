//! A replica walked at random against a model of it: a server sends what a
//! server sends — rows of a tree that points at itself, under names unique
//! among the live ones and given again once let go of; prices of items and
//! shelves labelled in either case, through keys checked at commit; deletes,
//! snapshots, new epochs, each pass packed as a server packs it, a table's
//! rows that come together in one part, a few rows a message — and now and
//! then what none sends: a row beside another's key, one that does not
//! decode, a key twice, rows of a table the replica does not hold from it,
//! more rows than a message carries, a row of twenty thousand bytes, a table
//! named past what is kept; through a transport that loses, and redelivers,
//! of this epoch or an earlier one; the host writes, rewrites and deletes its
//! own rows — receipts, quotes and tags pointing at the server's, readings
//! written while a message is applied; the uplink fails for a while and for
//! good; and the power goes, in the middle of a message, of an upload, of a
//! write, after a send. After every step the replica holds what the
//! protocol's model of it holds — retired rows let go of once nothing keeps
//! them, its edits held for as long as the host's writes say they are not
//! filed — digests what the model holds, confirms nothing it has not told
//! every refusal of, keeps what it stores bounded, and has lost no write:
//! each is either still noted or filed with its latest content. At the end,
//! with the faults gone, everything it wrote is filed, once, as it is.

use super::*;
use crate::model::{self, Model, Rules};
use crate::testkit::Walk;
use std::collections::{BTreeMap, VecDeque};

/// Rows the walk's server names: few, so the same rows come back. A price
/// is of the item of its id.
const ITEMS: u128 = 6;
const SHELVES: usize = 3;
const PRICES: u128 = 3;
const READINGS: u128 = 4;
const TASKS: u128 = 3;

/// The walk's tables: the suite's, its items a tree — each under a parent,
/// the root under itself — named uniquely among the live ones; a price of
/// an item, checked at commit; shelves labelled uniquely whatever the case,
/// and the replica's own tags of a shelf's label, checked at commit.
fn tables() -> String {
    tree_tables()
        .replace(
            "CREATE TABLE prices (id BLOB PRIMARY KEY NOT NULL,",
            "CREATE TABLE prices (id BLOB PRIMARY KEY NOT NULL \
             REFERENCES items (id) DEFERRABLE INITIALLY DEFERRED,",
        )
        .replace(
            "label TEXT NOT NULL,",
            "label TEXT NOT NULL UNIQUE COLLATE NOCASE,",
        )
        + "CREATE UNIQUE INDEX items_name ON items (name) WHERE retired_at IS NULL;
           CREATE TABLE tags (id INTEGER PRIMARY KEY, \
             label TEXT NOT NULL REFERENCES shelves (label) DEFERRABLE INITIALLY DEFERRED);"
}

/// The aisle of the walk's shelves.
const AISLE: Uuid = Uuid::from_u128(0xa151e);

fn label_of(bytes: &[u8]) -> String {
    let value: serde_json::Value = serde_json::from_slice(bytes).expect("a shelf");
    value["label"].as_str().expect("a label").to_owned()
}

/// What the replica's database refuses: an item named `bad…` (with `ABORT`)
/// or `doom…` (with `ROLLBACK`, the whole message), and the let-go of one
/// named `keep…` (with `ABORT`) or `gone…` (with `ROLLBACK`); and a price a
/// quote of its own points at.
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
CREATE TRIGGER doomed_delete BEFORE DELETE ON items WHEN OLD.name LIKE 'gone%'
BEGIN SELECT RAISE(ROLLBACK, 'an item doomed to stay'); END;
";

fn name_of(bytes: &[u8]) -> String {
    ItemRow::decode(bytes)
        .map(|row| row.name)
        .unwrap_or_default()
}

fn title_of(bytes: &[u8]) -> String {
    let value: serde_json::Value = serde_json::from_slice(bytes).expect("a task");
    value["title"].as_str().expect("a title").to_owned()
}

fn cents_of(bytes: &[u8]) -> i64 {
    let value: serde_json::Value = serde_json::from_slice(bytes).expect("a price");
    value["cents"]
        .as_str()
        .expect("cents as text")
        .parse()
        .unwrap()
}

/// An item, as the walk's server sends it: `None` when the bytes are none.
fn item_of(bytes: &[u8]) -> Option<ItemRow> {
    ItemRow::decode(bytes).ok()
}

/// The model's rules: what the walk's triggers, keys and quotes refuse.
const RULES: Rules = Rules {
    refuses: |table, bytes| {
        table == "items"
            && item_of(bytes)
                .is_none_or(|row| row.name.starts_with("bad") || row.name.starts_with("doom"))
    },
    keeps: |table, bytes| {
        table == "items"
            && item_of(bytes)
                .is_some_and(|row| row.name.starts_with("keep") || row.name.starts_with("gone"))
    },
    parents: |table, bytes| match table {
        "items" => item_of(bytes)
            .map(|row| ("items".to_owned(), row.partition_id))
            .into_iter()
            .collect(),
        "prices" => {
            let value: serde_json::Value = serde_json::from_slice(bytes).expect("a price");
            let id = value["id"].as_str().expect("an id").to_owned();
            vec![("items".to_owned(), id)]
        }
        _ => Vec::new(),
    },
    retires: |table| matches!(table, "items" | "tasks"),
    key: |table, bytes| {
        if table == "items" {
            item_of(bytes).map(|row| row.id)
        } else {
            None
        }
    },
    holds: |table| matches!(table, "items" | "shelves" | "prices" | "tasks"),
    unique: |table, bytes| match table {
        "items" => item_of(bytes).map(|row| row.name),
        "shelves" => Some(label_of(bytes).to_lowercase()),
        _ => None,
    },
    checked: true,
};

/// The walk's server: what it holds, what it sent this replica, and the
/// transport between them.
#[derive(Clone)]
struct Server {
    epoch: String,
    /// The last message it numbered for this replica, and its nonce.
    last: u64,
    nonce: u64,
    /// What its nonces are drawn from: another after a restore.
    salt: u64,
    /// The message that began the last snapshot it wrote the replica.
    began: u64,
    /// Items: their name and parent.
    items: BTreeMap<Uuid, (String, Uuid)>,
    /// The names of items it let go of, to give a new one.
    gone_names: Vec<String>,
    /// Shelves, by slot: their label.
    shelves: BTreeMap<i64, String>,
    prices: BTreeMap<Uuid, (i64, i64)>,
    /// Tasks, written on both sides: their title and version, here.
    tasks: BTreeMap<Uuid, (String, i64)>,
    /// What the transport holds for the replica, in order.
    queue: VecDeque<Vec<u8>>,
    /// Everything delivered of its epoch, to deliver again; and of the
    /// epochs before it, which a transport may deliver again too.
    delivered: Vec<Vec<u8>>,
    past: Vec<Vec<u8>>,
    /// A snapshot is owed: the replica said it has a gap, or that it
    /// applied less than it said before.
    owed: bool,
    /// The replica said it applied a message this server has no record of
    /// sending: it was restored from an older copy, and takes a new epoch.
    restored: bool,
    /// What the replica filed here: its readings, newest wins, and events.
    readings: BTreeMap<String, (i64, i64)>,
    events: BTreeSet<String>,
    /// What the replica sent of the tasks, as the server last received it —
    /// filed, or kept to be filed once it has the task again: whether each
    /// is done.
    done: BTreeMap<Uuid, bool>,
    /// The last applied it was told, of its epoch.
    confirmed: u64,
}

impl Server {
    /// The next message's place in the chain: its sequence and nonce, and
    /// what it follows — nothing, when it `begins` a snapshot.
    fn next(&mut self, begins: bool) -> (u64, u64, u64, u64) {
        let previous = if begins {
            (0, 0)
        } else {
            (self.last, self.nonce)
        };
        self.last += 1;
        self.nonce = (nonce_of(self.last) ^ self.salt) & u64::try_from(i64::MAX).unwrap();
        if begins {
            self.began = self.last;
        }
        (self.last, self.nonce, previous.0, previous.1)
    }

    fn numbered(&mut self, snapshot: &str, tables: Vec<Table>, begins: bool) -> Vec<u8> {
        let (sequence, nonce, previous, previous_nonce) = self.next(begins);
        Body::Down(Down {
            snapshot_id: snapshot.to_owned(),
            tables,
            sequence,
            epoch: self.epoch.clone(),
            previous,
            nonce,
            previous_nonce,
            mac: crate::conformance::SEAL.to_vec(),
        })
        .encoded()
    }

    fn shelf(slot: i64, label: &str) -> (WireKey, Vec<u8>) {
        let shelf = Shelf {
            aisle: AISLE,
            slot,
            label: label.to_owned(),
            revision: 1,
        };
        (key(&[&AISLE.to_string(), &slot.to_string()]), json(&shelf))
    }

    fn price(id: Uuid, cents: i64, version: i64) -> (WireKey, Vec<u8>) {
        let price = Price {
            id,
            cents,
            updated_at: at(version),
        };
        (key(&[&id.to_string()]), json(&price))
    }

    fn task(id: Uuid, title: &str, version: i64) -> (WireKey, Vec<u8>) {
        let task = Task {
            id,
            title: title.to_owned(),
            done: false,
            updated_at: at(version),
        };
        (key(&[&id.to_string()]), json(&task))
    }

    /// The messages of a pass's changes, as a server packs them — upserts
    /// parents first, then deletes children first, a table's rows that
    /// come together in one part, a few rows a message — and now and then
    /// what no server sends.
    fn changes(&mut self, walk: &mut Walk) {
        let mut items = Vec::new();
        let mut gone = Vec::new();
        for _ in 0..1 + walk.below(3) {
            let n = 1 + walk.below(ITEMS as usize);
            let id = Uuid::from_u128(n as u128);
            if walk.below(4) == 0 {
                gone.push(key(&[&id.to_string()]));
                continue;
            }
            let name = ["fine", "fine", "fine", "bad", "doom", "keep", "gone"][walk.below(7)];
            let mut name = format!("{name}{}", walk.below(100));
            // Now and then the name of an item let go of: a retired one may
            // still hold it here.
            if walk.below(3) == 0 {
                if let Some(old) = walk.pick(&(0..self.gone_names.len()).collect::<Vec<_>>()) {
                    name = self.gone_names[old].clone();
                }
            }
            // Under an item named before it, or itself: a tree.
            let parent = Uuid::from_u128(1 + walk.below(n) as u128);
            items.push((id, name, parent));
        }
        // A key let go of and sent again in one part is held — a pass of
        // items alone, whose deletes then join the part of its rows;
        // otherwise a pass sends a row or lets it go, never both.
        let held_again = walk.below(4) == 0;
        if !held_again {
            items.retain(|(id, _, _)| !gone.iter().any(|g| g.parts[0] == id.to_string()));
        }
        // Its own keys hold: an item with a price goes with it, but in a
        // pass of items alone, which keeps it.
        let mut unpriced = Vec::new();
        gone.retain(|g| {
            let id: Uuid = g.parts[0].parse().unwrap();
            if !self.prices.contains_key(&id) {
                return true;
            }
            if held_again {
                return false;
            }
            self.prices.remove(&id);
            unpriced.push(key(&[&id.to_string()]));
            true
        });
        for key in &gone {
            if let Some((name, _)) = self.items.remove(&key.parts[0].parse().unwrap()) {
                self.gone_names.push(name);
            }
        }
        for (id, name, parent) in &items {
            self.items.insert(*id, (name.clone(), *parent));
        }
        let mut items: Vec<(WireKey, Vec<u8>)> = items
            .iter()
            .map(|(id, name, parent)| node(*id, *parent, name))
            .collect();
        if walk.below(10) == 0 {
            if let Some(first) = items.first_mut() {
                let other = Uuid::from_u128(1 + walk.below(ITEMS as usize) as u128);
                first.0 = key(&[&other.to_string()]);
            }
        }
        if walk.below(12) == 0 {
            let id = Uuid::from_u128(1 + walk.below(ITEMS as usize) as u128);
            items.push((key(&[&id.to_string()]), vec![0xff, 0xff]));
        }
        let mut strays = Vec::new();
        if walk.below(8) == 0 {
            let ghost =
                |walk: &mut Walk| key(&[&Uuid::from_u128(500 + walk.below(3) as u128).to_string()]);
            let mut part = upserts("ghosts", vec![(ghost(walk), vec![1, 2, 3])]);
            part.deletes = vec![ghost(walk)];
            strays.push(part.clone());
            // A table named again, further on: logged once.
            strays.push(part);
        }
        if walk.below(8) == 0 {
            let id = Uuid::from_u128(600 + walk.below(3) as u128);
            let reading = (
                key(&[&Uuid::nil().to_string(), &id.to_string()]),
                b"{}".to_vec(),
            );
            strays.push(upserts("readings", vec![reading]));
        }
        let mut shelves = Vec::new();
        let mut unshelved = Vec::new();
        if !held_again && walk.below(2) == 0 {
            let slot = walk.below(SHELVES);
            let at = i64::try_from(slot).unwrap();
            if walk.below(4) == 0 {
                self.shelves.remove(&at);
                unshelved.push(key(&[&AISLE.to_string(), &at.to_string()]));
            } else {
                // Each slot's labels its own, in either case, or another.
                let label = [format!("s{slot}"), format!("S{slot}"), format!("t{slot}")]
                    [walk.below(3)]
                .clone();
                self.shelves.insert(at, label.clone());
                shelves.push(Self::shelf(at, &label));
            }
        }
        let mut prices = Vec::new();
        if !held_again && walk.below(2) == 0 {
            let id = Uuid::from_u128(1 + walk.below(PRICES as usize) as u128);
            if !self.items.contains_key(&id) {
                // Of no item it holds.
            } else if walk.below(4) == 0 {
                if self.prices.remove(&id).is_some() {
                    unpriced.push(key(&[&id.to_string()]));
                }
            } else {
                // Newer, whatever was let go of meanwhile; now and then
                // older, as a server restored from a copy, or whose clock
                // was set back, sends it.
                let version = if walk.below(5) == 0 {
                    i64::try_from(walk.below(10)).unwrap()
                } else {
                    i64::try_from(self.last).unwrap()
                };
                let cents = i64::try_from(walk.below(1000)).unwrap();
                self.prices.insert(id, (cents, version));
                prices.push(Self::price(id, cents, version));
            }
        }
        let mut tasks = Vec::new();
        let mut untasked = Vec::new();
        if !held_again && walk.below(3) == 0 {
            let id = Uuid::from_u128(300 + walk.below(TASKS as usize) as u128);
            if walk.below(4) == 0 {
                self.tasks.remove(&id);
                untasked.push(key(&[&id.to_string()]));
            } else {
                let version = i64::try_from(self.last).unwrap();
                let title = format!("task{}", walk.below(100));
                self.tasks.insert(id, (title.clone(), version));
                tasks.push(Self::task(id, &title, version));
            }
        }
        // The rows in the order a pass lists them.
        let row =
            |table: &str, (key, bytes): (WireKey, Vec<u8>)| (table.to_owned(), Some(bytes), key);
        let gone_row = |table: &str, key: WireKey| (table.to_owned(), None, key);
        let mut rows: Vec<(String, Option<Vec<u8>>, WireKey)> = Vec::new();
        if held_again {
            rows.extend(gone.drain(..).map(|k| gone_row("items", k)));
        }
        rows.extend(items.into_iter().map(|r| row("items", r)));
        rows.extend(shelves.into_iter().map(|r| row("shelves", r)));
        rows.extend(prices.into_iter().map(|r| row("prices", r)));
        rows.extend(tasks.into_iter().map(|r| row("tasks", r)));
        rows.extend(untasked.into_iter().map(|k| gone_row("tasks", k)));
        rows.extend(unpriced.into_iter().map(|k| gone_row("prices", k)));
        rows.extend(unshelved.into_iter().map(|k| gone_row("shelves", k)));
        rows.extend(gone.into_iter().map(|k| gone_row("items", k)));
        // A few rows a message — or, when a key is let go of and sent again,
        // all in one — each table's rows that come together in one part.
        let most = if held_again {
            usize::MAX
        } else {
            1 + walk.below(6)
        };
        let mut messages: Vec<Vec<Table>> = vec![Vec::new()];
        let mut count = 0;
        for (table, bytes, key) in rows {
            if count == most {
                messages.push(Vec::new());
                count = 0;
            }
            count += 1;
            let message = messages.last_mut().expect("one at least");
            if message.last().is_none_or(|part| part.table != table) {
                message.push(Table {
                    table,
                    ..Table::default()
                });
            }
            let part = message.last_mut().expect("just pushed");
            match bytes {
                Some(bytes) => {
                    part.upserts.push(bytes);
                    part.keys.push(key);
                }
                None => part.deletes.push(key),
            }
        }
        messages.last_mut().expect("one at least").extend(strays);
        for tables in messages {
            let bytes = self.numbered("", tables, false);
            self.queue.push_back(bytes);
        }
    }

    /// What no server sends, and the replica must survive: a message of
    /// more rows than it takes, a row larger than most, a table named longer
    /// than it keeps.
    fn hostile(&mut self, walk: &mut Walk) {
        let tables = match walk.below(3) {
            0 => vec![deletes("ghosts", vec![key(&["g"]); 501])],
            1 => {
                let id = Uuid::from_u128(1 + walk.below(ITEMS as usize) as u128);
                let name = format!("fine{}", "x".repeat(20_000 + walk.below(1000)));
                self.items.insert(id, (name.clone(), id));
                vec![upserts("items", vec![node(id, id, &name)])]
            }
            _ => {
                let long = "g".repeat(600);
                vec![upserts(&long, vec![(key(&[&long]), vec![1])])]
            }
        };
        let bytes = self.numbered("", tables, false);
        self.queue.push_back(bytes);
    }

    /// A snapshot of everything it holds, in messages of a few rows, after
    /// whatever the transport holds — or in place of it, when it drops what
    /// it was about to send.
    fn snapshot(&mut self, walk: &mut Walk) {
        let id = walk.uuid().to_string();
        let rows: Vec<Table> = self
            .items
            .iter()
            .map(|(id, (name, parent))| upserts("items", vec![node(*id, *parent, name)]))
            .chain(
                self.shelves
                    .iter()
                    .map(|(slot, label)| upserts("shelves", vec![Self::shelf(*slot, label)])),
            )
            .chain(
                self.prices
                    .iter()
                    .map(|(id, (c, v))| upserts("prices", vec![Self::price(*id, *c, *v)])),
            )
            .chain(
                self.tasks
                    .iter()
                    .map(|(id, (t, v))| upserts("tasks", vec![Self::task(*id, t, *v)])),
            )
            .collect();
        let mut begins = true;
        for chunk in rows.chunks(2) {
            let bytes = self.numbered(&id, chunk.to_vec(), begins);
            self.queue.push_back(bytes);
            begins = false;
        }
        let (sequence, nonce, previous, previous_nonce) = self.next(begins);
        let end = Body::EndSnapshot(EndSnapshot {
            snapshot_id: id.clone(),
            tables: ["items", "shelves", "prices", "tasks", "ghosts"]
                [..4 + usize::from(walk.below(4) == 0)]
                .iter()
                .map(|t| (*t).to_owned())
                .collect(),
            sequence,
            epoch: self.epoch.clone(),
            previous,
            nonce,
            previous_nonce,
            mac: crate::conformance::SEAL.to_vec(),
        })
        .encoded();
        self.queue.push_back(end);
        self.owed = false;
    }

    /// Installed again, or found restored: a new epoch, which begins with a
    /// snapshot, and nothing of the old one sent any more.
    fn anew(&mut self, walk: &mut Walk) {
        self.epoch = walk.uuid().to_string();
        self.queue.clear();
        self.past.append(&mut self.delivered);
        (self.confirmed, self.restored) = (0, false);
        self.snapshot(walk);
    }

    /// Answers what the replica said: a new epoch if it found this server
    /// restored, a snapshot if one is owed.
    fn answer(&mut self, walk: &mut Walk) {
        if self.restored {
            self.anew(walk);
        } else if self.owed {
            self.snapshot(walk);
        }
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
                    "tasks" => {
                        // Only the replica's own column — of a task the
                        // server let go of, kept until it has it again.
                        let id: Uuid = value["id"].as_str().unwrap().parse().unwrap();
                        self.done.insert(id, value["done"].as_bool().unwrap());
                    }
                    other => panic!("an upload of `{other}`"),
                }
            }
            for key in &table.deletes {
                assert_eq!(table.table, "readings", "deletes of history");
                self.readings.remove(&key.parts.join("|"));
            }
        }
        // As a server files a gap: a snapshot is owed unless one that began
        // after the message it could not follow is on its way.
        if up.gap > 0 && self.began <= up.gap {
            self.owed = true;
        }
        // As a server files what it applied: a message past the last it
        // sent, or the last with another nonce, it has no record of — it was
        // restored itself; less than it said before is a replica restored
        // from an older copy, owed a snapshot unless one written since is on
        // its way.
        let unknown =
            up.sequence > self.last || (up.sequence == self.last && up.nonce != self.nonce);
        let behind = self.began <= self.confirmed;
        if up.epoch != self.epoch && up.sequence > 0 && self.confirmed > 0 && behind {
            // An epoch it never had, after one of its own: it was restored
            // from a copy older than that epoch.
            self.owed = true;
        } else if up.epoch == self.epoch && up.sequence > 0 && unknown {
            self.restored = true;
        } else if up.epoch == self.epoch && up.sequence > 0 {
            if up.sequence < self.confirmed && behind {
                self.owed = true;
            }
            self.confirmed = self.confirmed.max(up.sequence);
        }
        up.uploads.first().copied()
    }
}

/// What the host wrote of each task's own column that the server has not
/// filed, as PROTOCOL.md section 7 says a replica keeps it: per task, how
/// many writes, and the upload that last carried it with how many it
/// carried — filed once the server answers that upload, unless written
/// since.
#[derive(Clone, Default)]
struct Edits(BTreeMap<Uuid, (u64, Option<(u64, u64)>)>);

impl Edits {
    fn wrote(&mut self, task: Uuid) {
        self.0.entry(task).or_insert((0, None)).0 += 1;
    }

    /// `up` was made: what it carries of the tasks goes under its number.
    fn carried(&mut self, up: &Up) {
        let Some(number) = up.uploads.first() else {
            return;
        };
        for part in up.tables.iter().filter(|t| t.table == "tasks") {
            for key in &part.keys {
                if let Some((writes, sent)) = self.0.get_mut(&key.parts[0].parse().unwrap()) {
                    *sent = Some((*number, *writes));
                }
            }
        }
    }

    fn filed(&mut self, numbers: &[u64]) {
        self.0.retain(|_, (writes, sent)| {
            !sent.is_some_and(|(number, carried)| numbers.contains(&number) && carried == *writes)
        });
    }
}

/// Writes a reading of `me`, as the host does.
async fn write_reading(pool: &SqlitePool, me: Uuid, id: Uuid, value: i64, clock: i64) {
    sqlx::query(
        "INSERT INTO readings (replica_id, id, value, at) VALUES ($1, $2, $3, $4) \
         ON CONFLICT (replica_id, id) DO UPDATE SET value = excluded.value, at = excluded.at",
    )
    .bind(me)
    .bind(id)
    .bind(value)
    .bind(clock)
    .execute(pool)
    .await
    .unwrap();
}

/// What the host wrote: each reading's latest value and version, or `None`
/// once deleted, and every version it ever had; every event.
#[derive(Default)]
struct Host {
    readings: BTreeMap<String, Option<(i64, i64)>>,
    versions: BTreeMap<String, BTreeSet<(i64, i64)>>,
    events: BTreeSet<String>,
    /// The latest the host set each task's own column to.
    done: BTreeMap<Uuid, bool>,
    /// What of that is not filed yet.
    edits: Edits,
    clock: i64,
}

impl Host {
    /// The reading `key` was written `value`, now.
    fn read(&mut self, key: String, value: i64) {
        self.readings.insert(key.clone(), Some((value, self.clock)));
        self.versions
            .entry(key)
            .or_default()
            .insert((value, self.clock));
    }
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

/// Whether the replica made of a message what the model made of it — and
/// when it applied it, took the same rows of each table.
fn same(model: &Model, made: &model::Received, real: &Received) -> bool {
    match (made, real) {
        (model::Received::Applied, Received::Applied(applied)) => {
            let real: Vec<(String, BTreeSet<String>)> = applied
                .iter()
                .map(|a| (a.table.clone(), a.keys.iter().map(key_text).collect()))
                .collect();
            let modelled: Vec<(String, BTreeSet<String>)> = model
                .taken
                .iter()
                .map(|(table, keys)| (table.clone(), keys.iter().cloned().collect()))
                .collect();
            real == modelled
        }
        _ => matches!(
            (made, real),
            (model::Received::Ended, Received::SnapshotEnded)
                | (model::Received::Filed, Received::Filed)
                | (model::Received::Already, Received::AlreadyApplied)
                | (model::Received::Gap, Received::Gap)
                | (model::Received::Malformed, Received::Malformed(_))
        ),
    }
}

/// The rows the replica's own keep, as the model is told: a price a quote
/// points at, a shelf a tag names, which it cannot let go — nor the shelf's
/// label change; and what it retires — an item a receipt points at, a task
/// whose edit the server has not filed.
async fn pins(pool: &SqlitePool, edits: &Edits, model: &mut Model) {
    let ids = |sql: &'static str| async move {
        sqlx::query_scalar::<_, Uuid>(sql)
            .fetch_all(pool)
            .await
            .unwrap()
            .into_iter()
            .map(|id| id.to_string())
            .collect::<BTreeSet<String>>()
    };
    // The shelf's label first: compared as the key compares, whatever the
    // case.
    let tagged: BTreeSet<model::Row> = sqlx::query_as::<_, (Uuid, i64)>(
        "SELECT aisle, slot FROM shelves s WHERE EXISTS (SELECT 1 FROM tags t WHERE s.label = t.label)",
    )
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(|(aisle, slot)| ("shelves".to_owned(), format!("{aisle}|{slot}")))
    .collect();
    model.pinned = ids("SELECT price_id FROM quotes")
        .await
        .into_iter()
        .map(|id| ("prices".to_owned(), id))
        .chain(tagged.iter().cloned())
        .collect();
    model.anchored = tagged;
    model.retiring = ids("SELECT item_id FROM receipts")
        .await
        .into_iter()
        .map(|id| ("items".to_owned(), id))
        .chain(
            edits
                .0
                .keys()
                .map(|id| ("tasks".to_owned(), id.to_string())),
        )
        .collect();
}

/// `bytes` delivered to the replica and to the model, the model told what
/// the replica's own rows keep: what each made of it.
async fn deliver(
    node: &Node,
    edits: &mut Edits,
    model: &mut Model,
    bytes: &[u8],
) -> (model::Received, Received) {
    if let Ok(ToReplica {
        body: Some(Body::Filed(filed)),
        ..
    }) = ToReplica::decode(bytes)
    {
        edits.filed(&filed.uploads);
    }
    pins(&node.pool, edits, model).await;
    let before = model.rejections.clone();
    let expected = model.receive(bytes, &RULES);
    let again = &node.replica.again;
    let tries = again.load(std::sync::atomic::Ordering::Relaxed);
    let received = node.replica.receive(bytes).await.expect("applied");
    // Applied again only for a row at fault, which it then rejects.
    let culprit = model
        .rejections
        .iter()
        .any(|(row, r)| before.get(row) != Some(r));
    assert!(
        culprit || again.load(std::sync::atomic::Ordering::Relaxed) == tries,
        "applied again with no row at fault: {expected:?}"
    );
    (expected, received)
}

/// The uplink took `up`: the engine forgets a rejection it told of a row
/// it holds no copy of, as PROTOCOL.md section 6 lets it — the model, which
/// keeps it, is told so.
fn forgets_what_it_told(model: &mut Model, up: &Up) {
    for rejected in &up.rejected {
        let key = rejected.key.as_ref().map(|k| k.parts.join("|"));
        let row = (rejected.table.clone(), key.unwrap_or_default());
        let held = model.rows.contains_key(&row) || model.retired.contains_key(&row);
        if !held
            && model
                .rejections
                .get(&row)
                .is_some_and(|r| r.sequence == rejected.sequence)
        {
            model.rejections.remove(&row);
        }
    }
}

/// The server's rows the replica holds, live or `retired`, each with what
/// tells it: an item's name, a price's cents, a task's title.
async fn held_rows(pool: &SqlitePool, retired: bool) -> BTreeMap<model::Row, String> {
    let which = if retired { "IS NOT NULL" } else { "IS NULL" };
    let mut held = BTreeMap::new();
    for (table, column, retires) in [
        ("items", "name", true),
        ("prices", "CAST(cents AS TEXT)", false),
        ("tasks", "title", true),
    ] {
        if retired && !retires {
            continue;
        }
        let filter = if retires {
            format!(" WHERE retired_at {which}")
        } else {
            String::new()
        };
        let rows: Vec<(Uuid, String)> =
            sqlx::query_as(&format!("SELECT id, {column} FROM {table}{filter}"))
                .fetch_all(pool)
                .await
                .unwrap();
        held.extend(
            rows.into_iter()
                .map(|(id, content)| ((table.to_owned(), id.to_string()), content)),
        );
    }
    if !retired {
        let shelves: Vec<(Uuid, i64, String)> =
            sqlx::query_as("SELECT aisle, slot, label FROM shelves")
                .fetch_all(pool)
                .await
                .unwrap();
        held.extend(shelves.into_iter().map(|(aisle, slot, label)| {
            (("shelves".to_owned(), format!("{aisle}|{slot}")), label)
        }));
    }
    held
}

/// The digests `up` carries are what the model holds — but of a table with
/// writes the server has not filed, which go without one (PROTOCOL.md
/// section 8).
fn digests_are_the_models(model: &Model, edits: &Edits, up: &Up, at: &str) {
    if up.digests.is_empty() {
        return;
    }
    let tables: Vec<&str> = ["items", "shelves", "prices", "tasks"]
        .into_iter()
        .filter(|table| *table != "tasks" || edits.0.is_empty())
        .collect();
    let modelled = model.digests(&tables);
    assert_eq!(
        (up.epoch.as_str(), up.sequence),
        (modelled.epoch.as_str(), modelled.sequence),
        "{at}: digests read against"
    );
    let digest = |d: &crate::envelope::Digest| (d.table.clone(), d.count, d.fingerprint);
    let mut sent: Vec<_> = up.digests.iter().map(digest).collect();
    let mut expected: Vec<_> = modelled.digests.iter().map(digest).collect();
    sent.sort();
    expected.sort();
    assert_eq!(sent, expected, "{at}: the digests");
}

/// What the replica holds of the server's rows, its rejections, and where
/// it stands, against the model.
async fn holds_what_the_model_holds(pool: &SqlitePool, model: &Model, at: &str) {
    for (retired, modelled) in [(false, &model.rows), (true, &model.retired)] {
        let held = held_rows(pool, retired).await;
        let modelled: BTreeMap<model::Row, String> = modelled
            .iter()
            .map(|(row, bytes)| {
                let content = match row.0.as_str() {
                    "items" => name_of(bytes),
                    "prices" => cents_of(bytes).to_string(),
                    "shelves" => label_of(bytes),
                    _ => title_of(bytes),
                };
                (row.clone(), content)
            })
            .collect();
        assert_eq!(held, modelled, "{at}: the rows held, retired {retired}");
    }
    let rejected: BTreeSet<model::Row> =
        sqlx::query_as("SELECT table_name, row_key FROM sync_rejections")
            .fetch_all(pool)
            .await
            .unwrap()
            .into_iter()
            .collect();
    let modelled: BTreeSet<model::Row> = model.rejections.keys().cloned().collect();
    assert_eq!(rejected, modelled, "{at}: the rows rejected");
    let applied: (String, i64, i64) =
        sqlx::query_as("SELECT epoch, sequence, gap FROM sync_applied")
            .fetch_one(pool)
            .await
            .unwrap();
    let modelled = model.applied.clone().expect("provisioned");
    assert_eq!(
        (
            applied.0,
            u64::try_from(applied.1).unwrap(),
            u64::try_from(applied.2).unwrap()
        ),
        (modelled.0, modelled.1, model.gap),
        "{at}: where it stands"
    );
}

/// What the replica stores of its own bookkeeping stays bounded, and it
/// confirms nothing it has not told every refusal of.
async fn keeps_its_word(pool: &SqlitePool, host: &Host, model: &Model, at: &str) {
    let notes = count(pool, "SELECT count(*) FROM sync_notes").await;
    let rows = i64::try_from(host.readings.len() + host.events.len() + host.done.len()).unwrap();
    assert!(notes <= rows, "{at}: {notes} notes of {rows} rows");
    let rejections = count(pool, "SELECT count(*) FROM sync_rejections").await;
    assert!(rejections <= i64::try_from(model.rejections.len()).unwrap());
    let named = count(pool, "SELECT count(*) FROM sync_snapshot_keys").await;
    assert!(
        named <= i64::try_from(ITEMS + PRICES + TASKS).unwrap() + i64::try_from(SHELVES).unwrap(),
        "{at}: {named} named"
    );
    let leaving = count(pool, "SELECT count(*) FROM sync_leaving").await;
    assert!(
        leaving <= i64::try_from(TASKS).unwrap(),
        "{at}: {leaving} leaving"
    );
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
    let noted_tasks: BTreeSet<String> =
        sqlx::query_scalar("SELECT row_key FROM sync_notes WHERE relation = 'tasks'")
            .fetch_all(pool)
            .await
            .unwrap()
            .into_iter()
            .collect();
    let unfiled: BTreeSet<String> = host.edits.0.keys().map(Uuid::to_string).collect();
    assert_eq!(
        noted_tasks, unfiled,
        "{at}: the tasks whose edits are not filed"
    );
    for (id, done) in &host.done {
        assert!(
            noted_tasks.contains(&id.to_string()) || server.done.get(id) == Some(done),
            "{at}: task {id} set {done}, received {:?}, and no longer noted",
            server.done.get(id)
        );
    }
}

async fn count(pool: &SqlitePool, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(pool).await.expect(sql)
}

/// One seeded walk.
async fn walk(pool: SqlitePool, seed: u64) {
    let w = world_over(pool, &tables(), Settings::default()).await;
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
        nonce: nonce_of(BASE),
        salt: 0,
        began: BASE,
        items: BTreeMap::new(),
        gone_names: Vec::new(),
        shelves: BTreeMap::new(),
        prices: BTreeMap::new(),
        tasks: BTreeMap::new(),
        queue: VecDeque::new(),
        delivered: vec![provisioned],
        past: Vec::new(),
        owed: false,
        restored: false,
        readings: BTreeMap::new(),
        events: BTreeSet::new(),
        done: BTreeMap::new(),
        confirmed: 0,
    };
    let mut host = Host::default();
    // Answers the server owes, and may send late or never.
    let mut answers: Vec<u64> = Vec::new();
    // A copy of the replica's database, and the model as it stood then: the
    // host restores it now and then — never over writes of its own made
    // since, which a restore would lose for good.
    let copy_path = file.get_filename().with_extension("copy");
    let mut copy: Option<(Model, Edits)> = None;
    // A copy of the server's database: what it sends, and what it recorded
    // sending — never what it filed, which a restore would lose for good.
    let mut server_copy: Option<Server> = None;
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
            walk.below(32)
        };
        if matches!(action, 6..=9 | 24) {
            copy = None;
        }
        match action {
            // The server changes rows.
            0..=2 => server.changes(&mut walk),
            // It provisions the replica again; or, installed again, in a
            // new epoch, dropping what the transport held.
            3 if walk.below(4) == 0 => server.anew(&mut walk),
            3 => server.snapshot(&mut walk),
            // The transport loses a message, or delivers an old one again.
            4 if walk.below(3) == 0 => {
                server.queue.pop_front();
            }
            4 => {
                let from = if walk.below(4) == 0 {
                    &server.past
                } else {
                    &server.delivered
                };
                if let Some(old) = walk.pick(&(0..from.len()).collect::<Vec<_>>()) {
                    let bytes = from[old].clone();
                    let (expected, received) =
                        deliver(&node, &mut host.edits, &mut model, &bytes).await;
                    assert!(
                        same(&model, &expected, &received),
                        "{at}: again, {expected:?} of {:?}, and the replica {received:?}",
                        model.taken
                    );
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
                    let limits = Limits::default();
                    let _ = receive(&mut tx, &codecs(), bytes, &limits, &Care::default()).await;
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
                    write_reading(&node.pool, me, id, value, host.clock).await;
                    host.read(key, value);
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
                if let Some(outgoing) = &outgoing {
                    host.edits.carried(&outgoing.up);
                }
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
                for up in uplink.offered.lock().unwrap().drain(..) {
                    host.edits.carried(&up);
                    digests_are_the_models(&model, &host.edits, &up, &at);
                }
                assert!(
                    // Refused for good with nothing else to try it beside,
                    // a row is not blamed: the lane goes again later.
                    quiet
                        || went.is_ok()
                        || uplink.refusing.lock().unwrap().is_some()
                        || uplink.cannot_take.lock().unwrap().is_some(),
                    "{at}: {went:?}"
                );
                for (_, up) in uplink.taken() {
                    forgets_what_it_told(&mut model, &up);
                    answers.extend(server.file(&up, me));
                }
                server.answer(&mut walk);
            }
            // The host edits its own column of a task.
            24 => {
                let held: Vec<Uuid> =
                    sqlx::query_scalar("SELECT id FROM tasks WHERE retired_at IS NULL")
                        .fetch_all(&node.pool)
                        .await
                        .unwrap();
                if let Some(id) = walk.pick(&held) {
                    let done = walk.below(2) == 0;
                    sqlx::query("UPDATE tasks SET done = $2 WHERE id = $1")
                        .bind(id)
                        .bind(done)
                        .execute(&node.pool)
                        .await
                        .unwrap();
                    host.done.insert(id, done);
                    host.edits.wrote(id);
                }
            }
            // A receipt of the replica's own points at an item, or goes.
            25 => {
                let held: Vec<Uuid> =
                    sqlx::query_scalar("SELECT id FROM items WHERE retired_at IS NULL")
                        .fetch_all(&node.pool)
                        .await
                        .unwrap();
                if walk.below(2) == 0 {
                    if let Some(id) = walk.pick(&held) {
                        sqlx::query("INSERT INTO receipts (item_id) VALUES ($1)")
                            .bind(id)
                            .execute(&node.pool)
                            .await
                            .unwrap();
                    }
                } else {
                    sqlx::query("DELETE FROM receipts WHERE id = (SELECT min(id) FROM receipts)")
                        .execute(&node.pool)
                        .await
                        .unwrap();
                }
            }
            // The host copies the replica's database, or restores the copy
            // over it: the replica holds, and applied, what it did then.
            26 => match copy.take() {
                Some((copied, edits)) if walk.below(2) == 0 => {
                    node.pool.close().await;
                    let path = file.get_filename();
                    for suffix in ["-wal", "-shm"] {
                        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
                    }
                    std::fs::copy(&copy_path, path).unwrap();
                    node = boot(&file, me, &uplink).await;
                    model = copied;
                    host.edits = edits;
                }
                _ => {
                    let _ = std::fs::remove_file(&copy_path);
                    sqlx::query("VACUUM INTO $1")
                        .bind(copy_path.to_str().unwrap())
                        .execute(&node.pool)
                        .await
                        .unwrap();
                    copy = Some((model.clone(), host.edits.clone()));
                }
            },
            // The server's database is copied, or restored from the copy:
            // it numbers its messages again, with nonces of its own.
            28 => match server_copy.take() {
                Some(mut copied) if walk.below(2) == 0 => {
                    copied.queue = std::mem::take(&mut server.queue);
                    copied.delivered = std::mem::take(&mut server.delivered);
                    copied.readings = std::mem::take(&mut server.readings);
                    copied.events = std::mem::take(&mut server.events);
                    copied.done = std::mem::take(&mut server.done);
                    copied.salt = walk.below(1 << 20) as u64 + 1;
                    server = copied;
                }
                // A copy with a snapshot on its way would send it again,
                // which this server does not model.
                _ if server.began <= server.confirmed => server_copy = Some(server.clone()),
                _ => {}
            },
            // The next message comes in a later protocol: nothing is done,
            // and the transport keeps it, for a replica updated since.
            29 => {
                if let Some(bytes) = server.queue.front() {
                    let mut later = ToReplica::decode(bytes.as_slice()).unwrap();
                    later.protocol += 1;
                    let later = later.encode_to_vec();
                    pins(&node.pool, &host.edits, &mut model).await;
                    assert_eq!(model.receive(&later, &RULES), model::Received::Waits);
                    let waits = node.replica.receive(&later).await;
                    assert!(
                        matches!(waits, Err(Error::Protocol { .. })),
                        "{at}: {waits:?}"
                    );
                }
            }
            // A tag of the replica's own names a shelf's label, in any case;
            // or goes.
            30 => {
                let labels: Vec<String> = sqlx::query_scalar("SELECT label FROM shelves")
                    .fetch_all(&node.pool)
                    .await
                    .unwrap();
                match walk.pick(&(0..labels.len()).collect::<Vec<_>>()) {
                    Some(at) if walk.below(2) == 0 => {
                        let label = if walk.below(2) == 0 {
                            labels[at].to_uppercase()
                        } else {
                            labels[at].to_lowercase()
                        };
                        sqlx::query("INSERT INTO tags (label) VALUES ($1)")
                            .bind(label)
                            .execute(&node.pool)
                            .await
                            .unwrap();
                    }
                    _ => {
                        sqlx::query("DELETE FROM tags WHERE id = (SELECT min(id) FROM tags)")
                            .execute(&node.pool)
                            .await
                            .unwrap();
                    }
                }
            }
            // The server sends what none sends: too many rows, a row of
            // twenty thousand bytes, a table named past what is kept.
            31 => server.hostile(&mut walk),
            // The transport loses the first message of a snapshot.
            27 => {
                let begins = |bytes: &Vec<u8>| match ToReplica::decode(bytes.as_slice()) {
                    Ok(ToReplica {
                        body: Some(Body::Down(d)),
                        ..
                    }) => d.previous == 0,
                    Ok(ToReplica {
                        body: Some(Body::EndSnapshot(e)),
                        ..
                    }) => e.previous == 0,
                    _ => false,
                };
                if let Some(at) = server.queue.iter().position(begins) {
                    server.queue.remove(at);
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
                    // The host writes a reading meanwhile, now and then.
                    let during = (walk.below(4) == 0).then(|| {
                        host.clock += 1;
                        let id = Uuid::from_u128(200 + walk.below(READINGS as usize) as u128);
                        (id, i64::try_from(walk.below(1000)).unwrap(), host.clock)
                    });
                    let writing = async {
                        if let Some((id, value, clock)) = during {
                            write_reading(&node.pool, me, id, value, clock).await;
                        }
                    };
                    let ((expected, received), ()) =
                        tokio::join!(deliver(&node, &mut host.edits, &mut model, &bytes), writing);
                    if let Some((id, value, _)) = during {
                        host.read(format!("{me}|{id}"), value);
                        copy = None;
                    }
                    assert!(
                        same(&model, &expected, &received),
                        "{at}: {expected:?} of {:?}, and the replica {received:?}",
                        model.taken
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
            deliver(&node, &mut host.edits, &mut model, &bytes).await;
        }
        let mut said = false;
        for lane in 0..2 {
            // Digests now and then, as a replica says them hourly.
            let digests = round % 10 == 5;
            said |= node.replica.send_lane(lane, digests).await.unwrap().message;
            for up in uplink.offered.lock().unwrap().drain(..) {
                host.edits.carried(&up);
                digests_are_the_models(&model, &host.edits, &up, "the end");
            }
        }
        for (_, up) in uplink.taken() {
            forgets_what_it_told(&mut model, &up);
            answers.extend(server.file(&up, me));
        }
        server.answer(&mut walk);
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
    let _ = std::fs::remove_file(&copy_path);
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
    assert_eq!(
        count(&node.pool, "SELECT count(*) FROM sync_leaving").await,
        0,
        "{at}: every task let go of"
    );
    for (id, done) in &host.done {
        assert_eq!(server.done.get(id), Some(done), "{at}: task {id}");
    }
    let applied = model.applied.clone().unwrap();
    assert_eq!(
        model.gap, 0,
        "{at}: it missed a message, and was never provisioned again"
    );
    assert!(
        server.epoch == applied.0 && server.confirmed == applied.1,
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
