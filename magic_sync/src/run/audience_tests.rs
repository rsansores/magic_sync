//! Two kinds of replica served by one server: a fleet of devices that holds
//! every table as it is, and an ERP that holds some of them, the districts
//! in a shape of its own — its list's code joined in, and only while the
//! list is live.

use super::*;
use crate::envelope::{Body, ToReplica};
use crate::plan::{Audience, Replicas};
use prost::Message as _;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

const SCHEMA: &str = "
CREATE SCHEMA fixture;
CREATE TABLE fixture.devices (id UUID PRIMARY KEY, partition_id UUID NOT NULL, live BOOLEAN NOT NULL);
CREATE TABLE fixture.erps (id UUID PRIMARY KEY, partition_id UUID NOT NULL, live BOOLEAN NOT NULL);
CREATE TABLE fixture.regions (id UUID PRIMARY KEY, partition_id UUID NOT NULL, name TEXT NOT NULL);
CREATE INDEX ON fixture.regions (partition_id);
CREATE TABLE fixture.lists (
    id UUID PRIMARY KEY, partition_id UUID NOT NULL, code TEXT NOT NULL, live BOOLEAN NOT NULL);
CREATE INDEX ON fixture.lists (partition_id);
CREATE TABLE fixture.districts (
    id UUID PRIMARY KEY, partition_id UUID NOT NULL,
    region_id UUID NOT NULL REFERENCES fixture.regions (id),
    list_id UUID REFERENCES fixture.lists (id),
    name TEXT NOT NULL);
CREATE INDEX ON fixture.districts (partition_id);
CREATE INDEX ON fixture.districts (region_id);
CREATE INDEX ON fixture.districts (list_id);
CREATE VIEW fixture.erp_districts AS
    SELECT d.id, d.partition_id, d.region_id, d.name, l.code AS zone
      FROM fixture.districts d JOIN fixture.lists l ON l.id = d.list_id
     WHERE l.live;
";

const DEVICES: Replicas = Replicas::new("fixture.devices", "devices")
    .live("live")
    .partitioned_by("partition_id");

const ERPS: Replicas = Replicas::new("fixture.erps", "erps")
    .live("live")
    .partitioned_by("partition_id");

#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(
    table = "regions",
    schema = "fixture",
    mirror = "catalog",
    sync = "broadcast"
)]
struct Region {
    id: Uuid,
    partition_id: Uuid,
    name: String,
}

#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(
    table = "lists",
    schema = "fixture",
    mirror = "catalog",
    sync = "broadcast"
)]
struct List {
    id: Uuid,
    partition_id: Uuid,
    code: String,
    live: bool,
}

#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(
    table = "districts",
    schema = "fixture",
    mirror = "catalog",
    sync = "broadcast"
)]
struct District {
    id: Uuid,
    partition_id: Uuid,
    region_id: Uuid,
    list_id: Option<Uuid>,
    name: String,
}

/// The ERP's district: its list's code joined in, while the list is live.
#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(
    table = "districts",
    schema = "fixture",
    mirror = "catalog",
    sync = "broadcast",
    from = "fixture.erp_districts"
)]
struct ErpDistrict {
    id: Uuid,
    partition_id: Uuid,
    region_id: Uuid,
    name: String,
    zone: String,
}

fn audiences() -> Vec<Audience<()>> {
    vec![
        Audience::new(
            "devices",
            DEVICES,
            vec![
                Codec::json::<Region>(None),
                Codec::json::<List>(None),
                Codec::json::<District>(None),
            ],
        ),
        Audience::new(
            "erp",
            ERPS,
            vec![
                Codec::json::<Region>(None),
                Codec::json::<ErpDistrict>(None),
            ],
        ),
    ]
}

/// What each replica was sent, by audience, each message once.
#[derive(Clone, Default)]
struct Wire(
    Arc<Mutex<Vec<(String, Uuid, Body)>>>,
    Arc<Mutex<BTreeSet<String>>>,
);

impl Transport for Wire {
    async fn send(
        &self,
        audience: &str,
        replica: Uuid,
        message: Vec<u8>,
        id: &str,
    ) -> Result<(), Error> {
        let message = ToReplica::decode(message.as_slice()).expect("a message");
        if self.1.lock().unwrap().insert(id.to_owned()) {
            self.0.lock().unwrap().push((
                audience.to_owned(),
                replica,
                message.body.expect("a body"),
            ));
        }
        Ok(())
    }
}

/// Rows by table, then by key, as JSON.
type Held = BTreeMap<String, BTreeMap<String, serde_json::Value>>;

impl Wire {
    /// What `replica` holds once it applied everything it was sent.
    fn held(&self, replica: Uuid) -> Held {
        let mut held = Held::new();
        let mut named: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (_, to, body) in self.0.lock().unwrap().iter() {
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

    /// The audiences each replica was sent messages under.
    fn audiences(&self) -> BTreeSet<(String, Uuid)> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|(audience, replica, _)| (audience.clone(), *replica))
            .collect()
    }
}

struct World {
    pool: PgPool,
    wire: Wire,
    server: Server<Anyone, Wire>,
    partition: Uuid,
    device: Uuid,
    erp: Uuid,
}

impl World {
    async fn new(pool: PgPool) -> Self {
        let mut conn = pool.acquire().await.unwrap();
        sqlx::raw_sql(SCHEMA).execute(&mut *conn).await.unwrap();
        let (partition, device, erp) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
        for (table, id) in [("devices", device), ("erps", erp)] {
            sqlx::query(&format!(
                "INSERT INTO fixture.{table} VALUES ($1, $2, true)"
            ))
            .bind(id)
            .bind(partition)
            .execute(&mut *conn)
            .await
            .unwrap();
        }
        let wire = Wire::default();
        let server = Server::install(
            pool.clone(),
            audiences(),
            wire.clone(),
            Anyone::new(pool.clone()),
            Settings::default(),
        )
        .await
        .expect("the server");
        World {
            pool,
            wire,
            server,
            partition,
            device,
            erp,
        }
    }

    async fn exec(&self, sql: &str) {
        sqlx::raw_sql(&sql.replace("{p}", &self.partition.to_string()))
            .execute(&self.pool)
            .await
            .expect(sql);
    }

    async fn settle(&self) {
        while self.server.pass_now().await.unwrap().unwrap_or_default() > 0 {}
    }

    fn names(&self, replica: Uuid, table: &str, field: &str) -> BTreeSet<String> {
        self.wire
            .held(replica)
            .get(table)
            .into_iter()
            .flat_map(|rows| rows.values())
            .map(|row| row[field].as_str().expect("text").to_owned())
            .collect()
    }
}

const REGION: &str = "01900000-0000-7000-8000-000000000001";
const LIST: &str = "01900000-0000-7000-8000-000000000002";
const NORTH: &str = "01900000-0000-7000-8000-000000000003";
const SOUTH: &str = "01900000-0000-7000-8000-000000000004";

fn set(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|n| (*n).to_owned()).collect()
}

/// Each audience holds its own tables, in its own shape, from one stream
/// of changes: the devices every table as it is; the ERP no lists, and the
/// districts with their list's code, only while they name a live list —
/// one that stops is let go of by the ERP alone.
#[sqlx::test(migrations = false)]
async fn each_audience_holds_its_tables_in_its_own_shape(pool: PgPool) {
    let world = World::new(pool).await;
    world
        .exec(&format!(
            "INSERT INTO fixture.regions VALUES ('{REGION}', '{{p}}', 'coast'); \
             INSERT INTO fixture.lists VALUES ('{LIST}', '{{p}}', 'A', true); \
             INSERT INTO fixture.districts VALUES ('{NORTH}', '{{p}}', '{REGION}', '{LIST}', 'north'); \
             INSERT INTO fixture.districts VALUES ('{SOUTH}', '{{p}}', '{REGION}', NULL, 'south');"
        ))
        .await;
    world.settle().await;

    let device = world.wire.held(world.device);
    assert_eq!(
        device.keys().collect::<Vec<_>>(),
        ["districts", "lists", "regions"]
    );
    assert_eq!(
        world.names(world.device, "districts", "name"),
        set(&["north", "south"])
    );
    assert_eq!(
        device["districts"][NORTH]["list_id"], LIST,
        "the row as it is"
    );

    let erp = world.wire.held(world.erp);
    assert_eq!(erp.keys().collect::<Vec<_>>(), ["districts", "regions"]);
    assert_eq!(
        erp["districts"].keys().collect::<Vec<_>>(),
        [NORTH],
        "only a district of a live list"
    );
    let north = &erp["districts"][NORTH];
    assert_eq!(north["zone"], "A", "its list's code joined in");
    assert!(north.get("list_id").is_none(), "in the ERP's shape");

    world
        .exec(&format!(
            "UPDATE fixture.districts SET list_id = '{LIST}' WHERE id = '{SOUTH}'; \
             UPDATE fixture.districts SET list_id = NULL WHERE id = '{NORTH}';"
        ))
        .await;
    world.settle().await;
    assert_eq!(
        world.names(world.erp, "districts", "name"),
        set(&["south"]),
        "north left the ERP's districts, south came in"
    );
    assert_eq!(
        world.names(world.device, "districts", "name"),
        set(&["north", "south"]),
        "the devices hold both"
    );

    world
        .exec(&format!(
            "DELETE FROM fixture.districts WHERE id = '{SOUTH}';"
        ))
        .await;
    world.settle().await;
    assert_eq!(world.names(world.erp, "districts", "name"), set(&[]));
    assert_eq!(
        world.names(world.device, "districts", "name"),
        set(&["north"])
    );

    assert_eq!(
        world.wire.audiences(),
        BTreeSet::from([
            ("devices".to_owned(), world.device),
            ("erp".to_owned(), world.erp),
        ]),
        "each replica is sent to under its audience"
    );
}

/// The district's one trigger notes what any audience needs of it: what it
/// points at for every audience, its list included, which the ERP does not
/// hold.
#[sqlx::test(migrations = false)]
async fn a_table_two_audiences_hold_has_one_trigger_for_both(pool: PgPool) {
    let world = World::new(pool).await;
    let mut conn = world.pool.acquire().await.unwrap();
    let printed = crate::plan::Engine::triggers(&mut conn, &audiences())
        .await
        .expect("the triggers");
    let districts: Vec<&str> = printed
        .lines()
        .filter(|l| l.contains("\"districts\"") && l.contains("note_row"))
        .collect();
    assert_eq!(districts.len(), 1, "{printed}");
    assert!(
        districts[0].contains("'regions'") && districts[0].contains("'lists'"),
        "{}",
        districts[0]
    );
}

/// A row read from a relation of its own is held to that relation, column
/// for column, when the server starts.
#[sqlx::test(migrations = false)]
async fn a_row_read_from_a_view_is_held_to_it(pool: PgPool) {
    sqlx::raw_sql(SCHEMA).execute(&pool).await.unwrap();
    sqlx::raw_sql(
        "DROP VIEW fixture.erp_districts; CREATE VIEW fixture.erp_districts AS \
             SELECT d.id, d.partition_id, d.region_id, d.name \
               FROM fixture.districts d JOIN fixture.lists l ON l.id = d.list_id",
    )
    .execute(&pool)
    .await
    .unwrap();
    let refused = Server::install(
        pool.clone(),
        audiences(),
        Wire::default(),
        Anyone::new(pool.clone()),
        Settings::default(),
    )
    .await;
    match refused {
        Err(Error::Misuse(why)) => assert!(
            why.contains("`zone` is no column of the relation it is read from"),
            "{why}"
        ),
        other => panic!("not refused: {other:?}"),
    }
}
