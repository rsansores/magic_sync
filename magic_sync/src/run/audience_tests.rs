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

/// The ERP's district: its list's code joined in, while the list is live —
/// so a change of the list is a change of it.
#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(
    table = "districts",
    schema = "fixture",
    mirror = "catalog",
    sync = "broadcast",
    from = "fixture.erp_districts",
    depends = "list_id"
)]
struct ErpDistrict {
    id: Uuid,
    partition_id: Uuid,
    region_id: Uuid,
    name: String,
    zone: String,
}

fn erp() -> Audience<()> {
    Audience::new(
        "erp",
        ERPS,
        vec![
            Codec::json::<Region>(None),
            Codec::json::<ErpDistrict>(None),
        ],
    )
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
        erp(),
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
        Self::serving(pool, audiences()).await
    }

    async fn serving(pool: PgPool, audiences: Vec<Audience<()>>) -> Self {
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
            audiences,
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

/// What the engine answers about itself says which audience each replica
/// and each table is of, lists a table once for each audience that holds
/// it, and narrows to one audience when asked.
#[sqlx::test(migrations = false)]
async fn the_status_says_which_audience_each_replica_and_table_is_of(pool: PgPool) {
    use crate::status::{RowState, Scope};
    let world = World::new(pool).await;
    world
        .exec(&format!(
            "INSERT INTO fixture.regions VALUES ('{REGION}', '{{p}}', 'coast'); \
             INSERT INTO fixture.lists VALUES ('{LIST}', '{{p}}', 'A', true); \
             INSERT INTO fixture.districts VALUES ('{NORTH}', '{{p}}', '{REGION}', '{LIST}', 'north');"
        ))
        .await;
    world.settle().await;
    let engine = world.server.engine();
    let mut conn = world.pool.acquire().await.unwrap();

    let overview = engine.overview(&mut conn, Scope::All, 10).await.unwrap();
    let mut replicas: Vec<(Uuid, &str)> = overview
        .replicas
        .first
        .iter()
        .map(|r| (r.replica, r.audience.as_str()))
        .collect();
    replicas.sort_unstable();
    let mut expected = vec![(world.device, "devices"), (world.erp, "erp")];
    expected.sort_unstable();
    assert_eq!(replicas, expected);
    let tables: Vec<(&str, &str)> = overview
        .tables
        .iter()
        .map(|t| (t.audience.as_str(), t.table.as_str()))
        .collect();
    assert_eq!(
        tables,
        [
            ("devices", "regions"),
            ("devices", "lists"),
            ("devices", "districts"),
            ("erp", "regions"),
            ("erp", "districts"),
        ]
    );

    let erp = engine
        .overview(&mut conn, Scope::Audience("erp"), 10)
        .await
        .unwrap();
    assert_eq!(erp.replicas.total, 1);
    assert_eq!(erp.replicas.first[0].replica, world.erp);
    assert!(erp.tables.iter().all(|t| t.audience == "erp"));
    assert!(matches!(
        engine
            .overview(&mut conn, Scope::Audience("nobody"), 10)
            .await,
        Err(Error::Misuse(_))
    ));

    let progress = engine
        .progress(&mut conn, Scope::Replicas(&[world.erp]))
        .await
        .unwrap();
    assert_eq!(progress.replicas.len(), 1);
    assert_eq!(progress.replicas[0].audience, "erp");
    let tables: Vec<&str> = progress.replicas[0]
        .tables
        .iter()
        .map(|t| t.table.as_str())
        .collect();
    assert_eq!(tables, ["regions", "districts"]);

    let places = engine
        .row_progress(&mut conn, Scope::All, "districts", NORTH)
        .await
        .unwrap();
    let mut seen: Vec<(Uuid, &str)> = places
        .places
        .iter()
        .map(|p| (p.replica, p.audience.as_str()))
        .collect();
    seen.sort_unstable();
    assert_eq!(
        seen, expected,
        "the row's place on every audience's replicas"
    );
    assert!(places.places.iter().all(|p| p.state != RowState::Leaving));
    let lists = engine
        .row_progress(&mut conn, Scope::All, "lists", LIST)
        .await
        .unwrap();
    assert_eq!(
        lists.places.iter().map(|p| p.replica).collect::<Vec<_>>(),
        [world.device],
        "only the devices hold the lists"
    );
}

/// A replica's digests are compared with what its own audience records of
/// it: a table its audience does not hold is its own business, and a table
/// it holds otherwise than recorded provisions it again — no other replica.
#[sqlx::test(migrations = false)]
async fn digests_are_compared_against_the_replicas_audience(pool: PgPool) {
    use crate::envelope::{fingerprint, Digest, Up};
    let world = World::new(pool).await;
    world
        .exec(&format!(
            "INSERT INTO fixture.regions VALUES ('{REGION}', '{{p}}', 'coast'); \
             INSERT INTO fixture.lists VALUES ('{LIST}', '{{p}}', 'A', true); \
             INSERT INTO fixture.districts VALUES ('{NORTH}', '{{p}}', '{REGION}', '{LIST}', 'north');"
        ))
        .await;
    world.settle().await;
    let last: i64 =
        sqlx::query_scalar("SELECT last_sequence FROM sync.chains WHERE replica_id = $1")
            .bind(world.erp)
            .fetch_one(&world.pool)
            .await
            .unwrap();
    let last = u64::try_from(last).unwrap();
    let digest = |table: &str, keys: &[&str]| {
        let keys: Vec<String> = keys.iter().map(|k| (*k).to_owned()).collect();
        Digest {
            table: table.to_owned(),
            count: keys.len() as u64,
            fingerprint: fingerprint(keys),
        }
    };
    let resyncs = || async {
        sqlx::query_scalar::<_, Uuid>(
            "SELECT replica_id FROM sync.notes WHERE kind = 'resync' AND done_at IS NULL",
        )
        .fetch_all(&world.pool)
        .await
        .unwrap()
    };
    let say = |digests: Vec<Digest>| {
        let world = &world;
        async move {
            sqlx::query("UPDATE sync.replicas SET compared_at = now() - interval '11 minutes'")
                .execute(&world.pool)
                .await
                .unwrap();
            let up = Up {
                digests,
                ..crate::testkit::confirmation(&world.pool, world.erp, last).await
            };
            world
                .server
                .receive(world.erp, &up.encode_to_vec())
                .await
                .unwrap();
        }
    };

    say(vec![
        digest("regions", &[REGION]),
        digest("districts", &[NORTH]),
        digest("lists", &["whatever it keeps"]),
    ])
    .await;
    assert!(
        resyncs().await.is_empty(),
        "holds what its audience records"
    );

    say(vec![digest("regions", &[REGION]), digest("districts", &[])]).await;
    assert_eq!(resyncs().await, [world.erp], "it alone provisioned again");
}

/// A change of a list is a change of every ERP district that copies its
/// code, sent again to the ERP alone: the devices hold the list itself, and
/// their districts did not change. A list that stops being live takes its
/// districts out of the ERP's.
#[sqlx::test(migrations = false)]
async fn a_change_of_a_parent_is_a_change_of_what_copies_it(pool: PgPool) {
    let world = World::new(pool).await;
    world
        .exec(&format!(
            "INSERT INTO fixture.regions VALUES ('{REGION}', '{{p}}', 'coast'); \
             INSERT INTO fixture.lists VALUES ('{LIST}', '{{p}}', 'A', true); \
             INSERT INTO fixture.districts VALUES ('{NORTH}', '{{p}}', '{REGION}', '{LIST}', 'north'); \
             INSERT INTO fixture.districts VALUES ('{SOUTH}', '{{p}}', '{REGION}', '{LIST}', 'south');"
        ))
        .await;
    world.settle().await;
    let sent = |replica: Uuid| {
        world
            .wire
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, to, _)| *to == replica)
            .count()
    };
    let device_before = sent(world.device);

    world
        .exec(&format!(
            "UPDATE fixture.lists SET code = 'B' WHERE id = '{LIST}';"
        ))
        .await;
    world.settle().await;
    assert_eq!(
        world.names(world.erp, "districts", "zone"),
        set(&["B"]),
        "both districts again, with the list's new code"
    );
    assert_eq!(sent(world.device), device_before + 1, "the list alone");
    let device = world.wire.held(world.device);
    assert_eq!(device["lists"][LIST]["code"], "B");
    assert!(
        world
            .wire
            .0
            .lock()
            .unwrap()
            .iter()
            .rev()
            .take(1)
            .all(|(_, to, body)| {
                *to != world.device
                    || matches!(body, Body::Down(down)
                    if down.tables.iter().map(|t| t.table.as_str()).eq(["lists"]))
            }),
        "no district went to the devices"
    );

    world
        .exec(&format!(
            "UPDATE fixture.lists SET live = false WHERE id = '{LIST}';"
        ))
        .await;
    world.settle().await;
    assert_eq!(world.names(world.erp, "districts", "name"), set(&[]));
    assert_eq!(
        world.names(world.device, "districts", "name"),
        set(&["north", "south"])
    );
}

/// A parent no audience holds is noted all the same, for the rows that copy
/// it, and its notes plan nothing else.
#[sqlx::test(migrations = false)]
async fn a_parent_no_audience_holds_is_noted_for_what_copies_it(pool: PgPool) {
    let world = World::serving(pool, vec![erp()]).await;
    let noted: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_trigger \
          WHERE tgname = 'sync_note_row' AND tgrelid = 'fixture.lists'::regclass",
    )
    .fetch_one(&world.pool)
    .await
    .unwrap();
    assert_eq!(noted, 1);
    world
        .exec(&format!(
            "INSERT INTO fixture.regions VALUES ('{REGION}', '{{p}}', 'coast'); \
             INSERT INTO fixture.lists VALUES ('{LIST}', '{{p}}', 'A', true); \
             INSERT INTO fixture.districts VALUES ('{NORTH}', '{{p}}', '{REGION}', '{LIST}', 'north');"
        ))
        .await;
    world.settle().await;
    assert_eq!(world.names(world.erp, "districts", "zone"), set(&["A"]));
    world
        .exec(&format!(
            "UPDATE fixture.lists SET code = 'C' WHERE id = '{LIST}';"
        ))
        .await;
    world.settle().await;
    assert_eq!(world.names(world.erp, "districts", "zone"), set(&["C"]));
    let pending: i64 = sqlx::query_scalar("SELECT count(*) FROM sync.notes WHERE done_at IS NULL")
        .fetch_one(&world.pool)
        .await
        .unwrap();
    assert_eq!(pending, 0, "the list's notes are done");
}

/// A row may depend only on a column that is a whole foreign key.
#[sqlx::test(migrations = false)]
async fn a_dependency_that_is_no_foreign_key_is_refused(pool: PgPool) {
    #[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
    #[row(
        table = "districts",
        schema = "fixture",
        mirror = "catalog",
        sync = "broadcast",
        depends = "name"
    )]
    struct Wrong {
        id: Uuid,
        partition_id: Uuid,
        region_id: Uuid,
        list_id: Option<Uuid>,
        name: String,
    }
    sqlx::raw_sql(SCHEMA).execute(&pool).await.unwrap();
    let refused = Server::install(
        pool.clone(),
        vec![Audience::new(
            "erp",
            ERPS,
            vec![Codec::json::<Region>(None), Codec::json::<Wrong>(None)],
        )],
        Wire::default(),
        Anyone::new(pool.clone()),
        Settings::default(),
    )
    .await;
    match refused {
        Err(Error::Misuse(why)) => assert!(
            why.contains("depends on `name`, which is not the whole of a foreign key"),
            "{why}"
        ),
        other => panic!("not refused: {other:?}"),
    }
}
