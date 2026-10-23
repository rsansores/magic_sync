//! Rows routed by a view of the host's: documents held by the devices at
//! the sites a rule grants them. What the view reads — the rules, which
//! site each device is at — is the host's to write however it likes, and
//! every write of it moves documents between devices; a write the view does
//! not read moves nothing. And the engine's own routes, `assigned` and
//! `grouped`, written as views: routed by them, the devices are sent what
//! they are sent without.

use super::*;
use crate::envelope::{Body, ToReplica};
use crate::plan::{Audience, Replicas};
use prost::Message as _;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

pub(crate) const SCHEMA: &str = "
CREATE SCHEMA fixture;
CREATE TABLE fixture.devices (
    id UUID PRIMARY KEY, company_id UUID NOT NULL, site_id UUID, live BOOLEAN NOT NULL DEFAULT true);
CREATE TABLE fixture.sites (id UUID PRIMARY KEY, company_id UUID NOT NULL, name TEXT NOT NULL);
CREATE TABLE fixture.documents (
    id UUID PRIMARY KEY, company_id UUID NOT NULL, title TEXT NOT NULL, archived_at TIMESTAMPTZ);
-- A rule grants a document to a site, or keeps it from it. Its note is the
-- host's own, which no view reads.
CREATE TABLE fixture.rules (
    document_id UUID NOT NULL REFERENCES fixture.documents (id) ON DELETE CASCADE,
    site_id UUID NOT NULL REFERENCES fixture.sites (id), company_id UUID NOT NULL,
    granted BOOLEAN NOT NULL DEFAULT true, note TEXT, PRIMARY KEY (document_id, site_id));
CREATE VIEW fixture.live_documents AS SELECT * FROM fixture.documents WHERE archived_at IS NULL;
-- Who holds a document: every device at a site a rule grants it.
CREATE VIEW fixture.document_devices AS
    SELECT r.document_id, d.id AS device_id, r.company_id
      FROM fixture.rules r JOIN fixture.devices d ON d.site_id = r.site_id
     WHERE r.granted;
-- The engine's other routes, and each as a view: a task goes to the device
-- it names; a manual to every device given a shelf it is on.
CREATE TABLE fixture.tasks (
    id UUID PRIMARY KEY, company_id UUID NOT NULL, device_id UUID REFERENCES fixture.devices (id),
    title TEXT NOT NULL);
CREATE TABLE fixture.manuals (id UUID PRIMARY KEY, company_id UUID NOT NULL, title TEXT NOT NULL);
CREATE TABLE fixture.shelves (id UUID PRIMARY KEY, company_id UUID NOT NULL);
CREATE TABLE fixture.shelf_manuals (
    shelf_id UUID NOT NULL REFERENCES fixture.shelves (id) ON DELETE CASCADE,
    manual_id UUID NOT NULL REFERENCES fixture.manuals (id), company_id UUID NOT NULL,
    PRIMARY KEY (shelf_id, manual_id));
CREATE TABLE fixture.device_shelves (
    shelf_id UUID NOT NULL REFERENCES fixture.shelves (id) ON DELETE CASCADE,
    device_id UUID NOT NULL REFERENCES fixture.devices (id), company_id UUID NOT NULL,
    PRIMARY KEY (shelf_id, device_id));
CREATE VIEW fixture.task_devices AS
    SELECT t.id AS task_id, t.device_id, t.company_id FROM fixture.tasks t;
CREATE VIEW fixture.manual_devices AS
    SELECT m.manual_id, a.device_id, m.company_id
      FROM fixture.shelf_manuals m JOIN fixture.device_shelves a ON a.shelf_id = m.shelf_id;
";

pub(crate) const DEVICES: Replicas = Replicas::new("fixture.devices", "devices")
    .live("live")
    .partitioned_by("company_id");

/// A document: held by every device at a site a rule grants it.
#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(table = "documents", schema = "fixture", mirror = "catalog")]
#[row(sync = "routed(view = fixture.document_devices, row = document_id, replica = device_id)")]
#[row(active = "fixture.live_documents", unmirrored = "archived_at")]
pub(crate) struct Document {
    pub(crate) id: Uuid,
    pub(crate) company_id: Uuid,
    pub(crate) title: String,
}

/// A task: to the device it names.
#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(
    table = "tasks",
    schema = "fixture",
    mirror = "catalog",
    sync = "assigned(device_id)"
)]
pub(crate) struct Task {
    pub(crate) id: Uuid,
    pub(crate) company_id: Uuid,
    pub(crate) device_id: Option<Uuid>,
    pub(crate) title: String,
}

/// A task, routed by a view that names the device it names.
#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(table = "tasks", schema = "fixture", mirror = "catalog")]
#[row(sync = "routed(view = fixture.task_devices, row = task_id, replica = device_id)")]
pub(crate) struct RoutedTask {
    pub(crate) id: Uuid,
    pub(crate) company_id: Uuid,
    pub(crate) device_id: Option<Uuid>,
    pub(crate) title: String,
}

/// A manual: to every device given a shelf it is on.
#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(table = "manuals", schema = "fixture", mirror = "catalog")]
#[row(sync = "grouped(members = shelf_manuals, assigned = device_shelves)")]
pub(crate) struct Manual {
    pub(crate) id: Uuid,
    pub(crate) company_id: Uuid,
    pub(crate) title: String,
}

/// A manual, routed by a view that pairs it with the devices given a shelf
/// it is on.
#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(table = "manuals", schema = "fixture", mirror = "catalog")]
#[row(sync = "routed(view = fixture.manual_devices, row = manual_id, replica = device_id)")]
pub(crate) struct RoutedManual {
    pub(crate) id: Uuid,
    pub(crate) company_id: Uuid,
    pub(crate) title: String,
}

/// Two companies: A, with the sites north and south and a device at each,
/// and B, with the site east and a device there.
pub(crate) const A: &str = "01900000-0000-7000-8000-00000000000a";
pub(crate) const B: &str = "01900000-0000-7000-8000-00000000000b";
pub(crate) const NORTH: &str = "01900000-0000-7000-8000-000000000101";
pub(crate) const SOUTH: &str = "01900000-0000-7000-8000-000000000102";
pub(crate) const EAST: &str = "01900000-0000-7000-8000-000000000103";
pub(crate) const A1: &str = "01900000-0000-7000-8000-000000000201";
pub(crate) const A2: &str = "01900000-0000-7000-8000-000000000202";
pub(crate) const B1: &str = "01900000-0000-7000-8000-000000000203";
const SAFETY: &str = "01900000-0000-7000-8000-000000000301";
const MENU: &str = "01900000-0000-7000-8000-000000000302";
const ROTA: &str = "01900000-0000-7000-8000-000000000303";

/// The companies, their sites and their devices.
pub(crate) fn sites() -> String {
    format!(
        "INSERT INTO fixture.sites VALUES ('{NORTH}', '{A}', 'north'), ('{SOUTH}', '{A}', 'south'), \
                                         ('{EAST}', '{B}', 'east'); \
         INSERT INTO fixture.devices (id, company_id, site_id) \
              VALUES ('{A1}', '{A}', '{NORTH}'), ('{A2}', '{A}', '{SOUTH}'), ('{B1}', '{B}', '{EAST}');"
    )
}

fn uuid(id: &str) -> Uuid {
    id.parse().expect("a uuid")
}

/// What each device was sent, each message once.
#[derive(Clone, Default)]
struct Wire(Arc<Mutex<Vec<(Uuid, Body)>>>, Arc<Mutex<BTreeSet<String>>>);

impl Transport for Wire {
    async fn send(
        &self,
        _audience: &str,
        replica: Uuid,
        message: Vec<u8>,
        id: &str,
    ) -> Result<(), Error> {
        let message = ToReplica::decode(message.as_slice()).expect("a message");
        if self.1.lock().unwrap().insert(id.to_owned()) {
            self.0
                .lock()
                .unwrap()
                .push((replica, message.body.expect("a body")));
        }
        Ok(())
    }
}

/// Rows by table, then by key, as JSON.
type Held = BTreeMap<String, BTreeMap<String, serde_json::Value>>;

/// What a message told a device: a table, a key, and whether to hold it.
type Told = BTreeSet<(String, String, bool)>;

impl Wire {
    /// What `replica` holds once it applied everything it was sent.
    fn held(&self, replica: Uuid) -> Held {
        let mut held = Held::new();
        let mut named: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (to, body) in self.0.lock().unwrap().iter() {
            if *to != replica {
                continue;
            }
            match body {
                Body::Down(down) => {
                    for table in &down.tables {
                        let rows = held.entry(table.table.clone()).or_default();
                        for (bytes, key) in table.upserts.iter().zip(&table.keys) {
                            let key = key.parts.join("|");
                            if !down.snapshot_id.is_empty() {
                                named
                                    .entry(table.table.clone())
                                    .or_default()
                                    .insert(key.clone());
                            }
                            rows.insert(key, serde_json::from_slice(bytes).expect("json"));
                        }
                        for key in &table.deletes {
                            rows.remove(&key.parts.join("|"));
                        }
                    }
                }
                Body::EndSnapshot(end) => {
                    for table in &end.tables {
                        let named = named.remove(table).unwrap_or_default();
                        held.entry(table.clone())
                            .or_default()
                            .retain(|key, _| named.contains(key));
                    }
                }
                Body::Filed(_) => {}
            }
        }
        held.retain(|_, rows| !rows.is_empty());
        held
    }

    /// How many messages were sent, to anyone.
    fn count(&self) -> usize {
        self.0.lock().unwrap().len()
    }

    /// What each replica was told by the messages sent from the `from`th
    /// on: every row it was sent or told to let go of, and every table a
    /// snapshot it was sent ended.
    fn told(&self, from: usize) -> BTreeMap<Uuid, Told> {
        let mut out: BTreeMap<Uuid, Told> = BTreeMap::new();
        for (to, body) in self.0.lock().unwrap().iter().skip(from) {
            let told = out.entry(*to).or_default();
            match body {
                Body::Down(down) => {
                    for table in &down.tables {
                        for key in &table.keys {
                            told.insert((table.table.clone(), key.parts.join("|"), true));
                        }
                        for key in &table.deletes {
                            told.insert((table.table.clone(), key.parts.join("|"), false));
                        }
                    }
                }
                Body::EndSnapshot(end) => {
                    told.extend(end.tables.iter().map(|t| (t.clone(), String::new(), false)));
                }
                Body::Filed(_) => {}
            }
        }
        out
    }
}

struct World {
    pool: PgPool,
    wire: Wire,
    server: Server<Anyone, Wire>,
}

impl World {
    /// The fixture's companies, sites and devices, served the tables of
    /// `codecs`.
    async fn new(pool: PgPool, codecs: Vec<Codec<()>>) -> Self {
        sqlx::raw_sql(&format!("{SCHEMA}{}", sites()))
            .execute(&pool)
            .await
            .expect("the fixture");
        let wire = Wire::default();
        let server = Server::install(
            pool.clone(),
            vec![Audience::new("devices", DEVICES, codecs)],
            wire.clone(),
            Anyone::new(pool.clone()),
            Settings::default(),
        )
        .await
        .expect("the server");
        let world = World { pool, wire, server };
        world.settle().await;
        world
    }

    async fn exec(&self, sql: &str) {
        sqlx::raw_sql(sql).execute(&self.pool).await.expect(sql);
    }

    async fn settle(&self) {
        while self.server.pass_now().await.unwrap().unwrap_or_default() > 0 {}
    }

    /// The titles of the documents `id` holds.
    fn titles(&self, id: &str) -> BTreeSet<String> {
        self.wire
            .held(uuid(id))
            .get("documents")
            .into_iter()
            .flat_map(|rows| rows.values())
            .map(|row| row["title"].as_str().expect("text").to_owned())
            .collect()
    }

    /// The notes written since the note `after`, as their kind and
    /// partition, oldest first.
    async fn notes_since(&self, after: i64) -> Vec<(String, Option<Uuid>)> {
        sqlx::query_as("SELECT kind, partition_id FROM sync.notes WHERE id > $1 ORDER BY id")
            .bind(after)
            .fetch_all(&self.pool)
            .await
            .expect("the notes")
    }

    async fn last_note(&self) -> i64 {
        sqlx::query_scalar("SELECT coalesce(max(id), 0) FROM sync.notes")
            .fetch_one(&self.pool)
            .await
            .expect("the last note")
    }
}

fn set(titles: &[&str]) -> BTreeSet<String> {
    titles.iter().map(|t| (*t).to_owned()).collect()
}

fn documents() -> Vec<Codec<()>> {
    vec![Codec::json::<Document>(None)]
}

/// A rule moved to another site takes its document from the devices of the
/// one and gives it to the devices of the other; a device moved to another
/// site takes what that site is granted and lets go of the rest; a rule
/// that stops granting takes its document back. What none of it touched —
/// another company's devices — is sent nothing.
#[sqlx::test(migrations = false)]
async fn a_change_of_what_the_view_reads_moves_rows_between_replicas(pool: PgPool) {
    let world = World::new(pool, documents()).await;
    let to_b1 = |world: &World| {
        world
            .wire
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|(to, _)| *to == uuid(B1))
            .count()
    };
    let b1_provisioned = to_b1(&world);
    world
        .exec(&format!(
            "INSERT INTO fixture.documents (id, company_id, title) \
                  VALUES ('{SAFETY}', '{A}', 'safety'), ('{MENU}', '{A}', 'menu'); \
             INSERT INTO fixture.rules (document_id, site_id, company_id) \
                  VALUES ('{SAFETY}', '{NORTH}', '{A}'), ('{MENU}', '{SOUTH}', '{A}');"
        ))
        .await;
    world.settle().await;
    assert_eq!(world.titles(A1), set(&["safety"]));
    assert_eq!(world.titles(A2), set(&["menu"]));

    world
        .exec(&format!(
            "UPDATE fixture.rules SET site_id = '{SOUTH}' WHERE document_id = '{SAFETY}'"
        ))
        .await;
    world.settle().await;
    assert_eq!(world.titles(A1), set(&[]), "the rule left its site");
    assert_eq!(world.titles(A2), set(&["menu", "safety"]));

    world
        .exec(&format!(
            "UPDATE fixture.devices SET site_id = '{SOUTH}' WHERE id = '{A1}'"
        ))
        .await;
    world.settle().await;
    assert_eq!(
        world.titles(A1),
        set(&["menu", "safety"]),
        "the device came to the site"
    );

    world
        .exec(&format!(
            "UPDATE fixture.rules SET granted = false WHERE document_id = '{MENU}'"
        ))
        .await;
    world.settle().await;
    assert_eq!(world.titles(A1), set(&["safety"]), "no longer granted");
    assert_eq!(world.titles(A2), set(&["safety"]));

    world
        .exec(&format!(
            "DELETE FROM fixture.rules WHERE document_id = '{SAFETY}'"
        ))
        .await;
    world.settle().await;
    assert_eq!(world.titles(A1), set(&[]));
    assert_eq!(world.titles(A2), set(&[]));
    assert_eq!(
        to_b1(&world),
        b1_provisioned,
        "another company's device is sent nothing"
    );
}

/// A change of the routed row itself is planned by its key, as any row's:
/// sent again to the devices that hold it, and nothing else — its view is
/// not asked for the rest. The active rows gate what travels at all: an
/// archived document is taken back wherever the view pairs it.
#[sqlx::test(migrations = false)]
async fn a_change_of_a_routed_row_is_planned_by_its_key(pool: PgPool) {
    let world = World::new(pool, documents()).await;
    world
        .exec(&format!(
            "INSERT INTO fixture.documents (id, company_id, title) \
                  VALUES ('{SAFETY}', '{A}', 'safety'), ('{MENU}', '{A}', 'menu'); \
             INSERT INTO fixture.rules (document_id, site_id, company_id) \
                  VALUES ('{SAFETY}', '{NORTH}', '{A}'), ('{MENU}', '{NORTH}', '{A}');"
        ))
        .await;
    world.settle().await;
    let (before, sent) = (world.last_note().await, world.wire.count());

    world
        .exec(&format!(
            "UPDATE fixture.documents SET title = 'safety, revised' WHERE id = '{SAFETY}'"
        ))
        .await;
    assert_eq!(
        world.notes_since(before).await,
        [("row".to_owned(), Some(uuid(A)))],
        "a change of the row, by its key"
    );
    world.settle().await;
    assert_eq!(
        world.wire.told(sent),
        BTreeMap::from([(
            uuid(A1),
            Told::from([("documents".to_owned(), SAFETY.to_owned(), true)])
        )]),
        "the row alone, to the device that holds it"
    );
    assert_eq!(world.titles(A1), set(&["menu", "safety, revised"]));

    world
        .exec(&format!(
            "UPDATE fixture.documents SET archived_at = now() WHERE id = '{MENU}'"
        ))
        .await;
    world.settle().await;
    assert_eq!(
        world.titles(A1),
        set(&["safety, revised"]),
        "archived, it is no row of the active ones"
    );
}

/// A statement that rewrites what the view reads, however many rows, notes
/// each partition it touched once; one that touches only what no view reads
/// notes nothing, and sends nothing; a truncate notes every partition.
#[sqlx::test(migrations = false)]
async fn a_statement_notes_once_each_partition_it_touched(pool: PgPool) {
    let world = World::new(pool, documents()).await;
    world
        .exec(&format!(
            "INSERT INTO fixture.documents (id, company_id, title) \
                  VALUES ('{SAFETY}', '{A}', 'safety'), ('{MENU}', '{A}', 'menu'), \
                         ('{ROTA}', '{B}', 'rota');"
        ))
        .await;
    let rules = format!(
        "INSERT INTO fixture.rules (document_id, site_id, company_id) \
              VALUES ('{SAFETY}', '{NORTH}', '{A}'), ('{SAFETY}', '{SOUTH}', '{A}'), \
                     ('{MENU}', '{SOUTH}', '{A}'), ('{ROTA}', '{EAST}', '{B}')"
    );
    let partitions = |notes: Vec<(String, Option<Uuid>)>| -> Vec<Option<Uuid>> {
        let mut rerouted: Vec<Option<Uuid>> = notes
            .into_iter()
            .filter(|(kind, _)| kind == "rerouted")
            .map(|(_, partition)| partition)
            .collect();
        rerouted.sort_unstable();
        rerouted
    };
    let both = vec![Some(uuid(A)), Some(uuid(B))];

    let before = world.last_note().await;
    world.exec(&rules).await;
    assert_eq!(
        partitions(world.notes_since(before).await),
        both,
        "an insert"
    );
    world.settle().await;
    assert_eq!(world.titles(A1), set(&["safety"]));
    assert_eq!(world.titles(A2), set(&["menu", "safety"]));
    assert_eq!(world.titles(B1), set(&["rota"]));

    let (before, sent) = (world.last_note().await, world.wire.count());
    world
        .exec("UPDATE fixture.rules SET note = 'checked'")
        .await;
    assert_eq!(
        world.notes_since(before).await,
        [],
        "the views read no note"
    );
    world.settle().await;
    assert_eq!(world.wire.count(), sent, "nothing to send");

    let before = world.last_note().await;
    world
        .exec("UPDATE fixture.rules SET granted = NOT granted")
        .await;
    assert_eq!(
        partitions(world.notes_since(before).await),
        both,
        "an update"
    );
    world.settle().await;
    for device in [A1, A2, B1] {
        assert_eq!(world.titles(device), set(&[]), "none granted");
    }

    // The rules rebuilt in one transaction, as a host rebuilds a table it
    // derives: a statement each, a note a partition each.
    let before = world.last_note().await;
    world
        .exec(&format!(
            "BEGIN; DELETE FROM fixture.rules; {rules}; COMMIT;"
        ))
        .await;
    assert_eq!(
        partitions(world.notes_since(before).await),
        [Some(uuid(A)), Some(uuid(A)), Some(uuid(B)), Some(uuid(B))],
        "a delete and an insert"
    );
    world.settle().await;
    assert_eq!(world.titles(A2), set(&["menu", "safety"]));

    let before = world.last_note().await;
    world.exec("TRUNCATE fixture.rules").await;
    assert_eq!(
        partitions(world.notes_since(before).await),
        [None],
        "every partition"
    );
    world.settle().await;
    for device in [A1, A2, B1] {
        assert_eq!(world.titles(device), set(&[]), "no rule left");
    }
}

/// The writes of the proof below, in order: each moves a task or a manual
/// some way the engine's own routes see.
fn steps() -> Vec<String> {
    const T1: &str = "01900000-0000-7000-8000-000000000401";
    const T2: &str = "01900000-0000-7000-8000-000000000402";
    const T3: &str = "01900000-0000-7000-8000-000000000403";
    const M1: &str = "01900000-0000-7000-8000-000000000501";
    const M2: &str = "01900000-0000-7000-8000-000000000502";
    const S1: &str = "01900000-0000-7000-8000-000000000601";
    const S2: &str = "01900000-0000-7000-8000-000000000602";
    vec![
        format!(
            "INSERT INTO fixture.tasks VALUES ('{T1}', '{A}', '{A1}', 'open'), \
                 ('{T2}', '{A}', '{A2}', 'close'), ('{T3}', '{B}', NULL, 'count'); \
             INSERT INTO fixture.manuals VALUES ('{M1}', '{A}', 'fryer'), ('{M2}', '{A}', 'till'); \
             INSERT INTO fixture.shelves VALUES ('{S1}', '{A}'), ('{S2}', '{A}'); \
             INSERT INTO fixture.shelf_manuals VALUES ('{S1}', '{M1}', '{A}'), ('{S1}', '{M2}', '{A}'), \
                 ('{S2}', '{M2}', '{A}'); \
             INSERT INTO fixture.device_shelves VALUES ('{S1}', '{A1}', '{A}'), ('{S2}', '{A2}', '{A}');"
        ),
        format!("UPDATE fixture.tasks SET device_id = '{A2}' WHERE id = '{T1}'"),
        format!("UPDATE fixture.tasks SET device_id = NULL WHERE id = '{T2}'"),
        format!("UPDATE fixture.tasks SET device_id = '{B1}' WHERE id = '{T3}'"),
        format!("INSERT INTO fixture.device_shelves VALUES ('{S1}', '{A2}', '{A}')"),
        format!("DELETE FROM fixture.shelf_manuals WHERE shelf_id = '{S1}' AND manual_id = '{M2}'"),
        format!("INSERT INTO fixture.device_shelves VALUES ('{S2}', '{A1}', '{A}')"),
        format!("UPDATE fixture.manuals SET title = 'till, new' WHERE id = '{M2}'"),
        format!("DELETE FROM fixture.shelves WHERE id = '{S1}'"),
        format!("UPDATE fixture.tasks SET title = 'open early' WHERE id = '{T1}'"),
        format!("DELETE FROM fixture.tasks WHERE id = '{T1}'"),
    ]
}

/// `assigned` and `grouped` are routes a view can say: a task routed by a
/// view of the device it names, and a manual by a view of the devices given
/// a shelf it is on, go where they go without one — step by step, each
/// device told the same rows to hold and to let go of, holding the same.
#[sqlx::test(migrations = false)]
async fn assigned_and_grouped_are_routes_a_view_says(pool: PgPool) {
    async fn told(pool: &PgPool, codecs: Vec<Codec<()>>) -> Vec<(BTreeMap<Uuid, Told>, Vec<Held>)> {
        sqlx::raw_sql("DROP SCHEMA IF EXISTS fixture CASCADE; DROP SCHEMA IF EXISTS sync CASCADE;")
            .execute(pool)
            .await
            .expect("a clean database");
        let world = World::new(pool.clone(), codecs).await;
        let mut out = Vec::new();
        for step in steps() {
            let sent = world.wire.count();
            world.exec(&step).await;
            world.settle().await;
            let held = [A1, A2, B1].map(|d| world.wire.held(uuid(d))).to_vec();
            out.push((world.wire.told(sent), held));
        }
        out
    }
    let own = told(
        &pool,
        vec![Codec::json::<Task>(None), Codec::json::<Manual>(None)],
    )
    .await;
    let routed = told(
        &pool,
        vec![
            Codec::json::<RoutedTask>(None),
            Codec::json::<RoutedManual>(None),
        ],
    )
    .await;
    for (n, (own, routed)) in own.iter().zip(&routed).enumerate() {
        assert_eq!(own, routed, "step {n}: {}", steps()[n]);
    }
    assert!(
        own.iter().all(|(told, _)| !told.is_empty()),
        "every step sends something: each is a move the routes see"
    );
}
