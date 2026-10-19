//! Two kinds of replica served by one server: a fleet of devices that holds
//! every table as it is, and an ERP that holds some of them, the districts
//! in a shape of its own — its list's code joined in, and only while the
//! list is live.

use super::*;
use crate::conformance::audiences::{
    both as audiences, device_codecs, erp, erp_codecs, Region, DEVICES, ERPS,
    SERVER_SCHEMA as SCHEMA,
};
use crate::envelope::{Body, ToReplica};
use crate::plan::{Audience, Replicas};
use prost::Message as _;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

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

/// What a replica sends up is filed by the audience the relay plans it in,
/// and answered under its name: two audiences over one relation, told apart
/// by which of its rows each counts live.
#[sqlx::test(migrations = false)]
async fn an_upload_is_filed_by_the_audience_its_sender_is_planned_in(pool: PgPool) {
    use crate::envelope::Up;
    /// Every sender the server asked about, with its audience.
    #[derive(Clone)]
    struct Asked(PgPool, Arc<Mutex<Vec<(String, Uuid)>>>);
    impl Senders for Asked {
        type Known = ();
        async fn known(&self, audience: &str, replica: Uuid) -> Result<Option<()>, Error> {
            self.1.lock().unwrap().push((audience.to_owned(), replica));
            Ok(Some(()))
        }
        async fn filing(
            &self,
            _: &(),
        ) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, Error> {
            Ok(self.0.begin().await?)
        }
    }
    sqlx::raw_sql(SCHEMA).execute(&pool).await.unwrap();
    sqlx::raw_sql("ALTER TABLE fixture.devices ADD COLUMN kind TEXT NOT NULL DEFAULT 'till'")
        .execute(&pool)
        .await
        .unwrap();
    let (partition, till, kiosk) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    for (id, kind) in [(till, "till"), (kiosk, "kiosk")] {
        sqlx::query("INSERT INTO fixture.devices VALUES ($1, $2, true, $3)")
            .bind(id)
            .bind(partition)
            .bind(kind)
            .execute(&pool)
            .await
            .unwrap();
    }
    let tills = Replicas::new("fixture.devices", "devices")
        .live("live AND kind = 'till'")
        .partitioned_by("partition_id");
    let kiosks = Replicas::new("fixture.devices", "devices")
        .live("live AND kind = 'kiosk'")
        .partitioned_by("partition_id");
    let wire = Wire::default();
    let asked = Asked(pool.clone(), Arc::default());
    let server = Server::install(
        pool.clone(),
        vec![
            Audience::new("tills", tills, device_codecs()),
            Audience::new("kiosks", kiosks, erp_codecs()),
        ],
        wire.clone(),
        asked.clone(),
        Settings::default(),
    )
    .await
    .expect("the server");
    while server.pass_now().await.unwrap().unwrap_or_default() > 0 {}
    let up = Up {
        uploads: vec![1],
        protocol: crate::envelope::PROTOCOL,
        ..Up::default()
    };
    server.receive(kiosk, &up.encode_to_vec()).await.unwrap();
    assert_eq!(
        asked.1.lock().unwrap().clone(),
        [("kiosks".to_owned(), kiosk)]
    );
    let answered: Vec<String> = wire
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, to, body)| *to == kiosk && matches!(body, Body::Filed(_)))
        .map(|(audience, _, _)| audience.clone())
        .collect();
    assert_eq!(answered, ["kiosks"]);
    assert_eq!(
        wire.audiences(),
        BTreeSet::from([("kiosks".to_owned(), kiosk), ("tills".to_owned(), till)])
    );
}

/// An audience renamed takes its replicas over, provisioning each again; an
/// audience no longer declared lets its replicas go; and a replica of an
/// engine from before audiences that no audience counts live is let go of.
#[sqlx::test(migrations = false)]
async fn replicas_of_an_audience_no_longer_declared_are_taken_over_or_let_go(pool: PgPool) {
    let world = World::new(pool).await;
    world
        .exec(&format!(
            "INSERT INTO fixture.regions VALUES ('{REGION}', '{{p}}', 'coast');"
        ))
        .await;
    world.settle().await;
    let audience_of = |replica: Uuid| {
        let pool = world.pool.clone();
        async move {
            sqlx::query_as::<_, (bool, Option<String>)>(
                "SELECT live, audience FROM sync.replicas WHERE replica_id = $1",
            )
            .bind(replica)
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    let held = |replica: Uuid| {
        let pool = world.pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM sync.held WHERE replica_id = $1")
                .bind(replica)
                .fetch_one(&pool)
                .await
                .unwrap()
        }
    };

    // Renamed: the ERP's replica is the books', sent everything again.
    let wire = Wire::default();
    let renamed = Server::install(
        world.pool.clone(),
        vec![
            Audience::new("devices", DEVICES, device_codecs()),
            Audience::new("books", ERPS, erp_codecs()),
        ],
        wire.clone(),
        Anyone::new(world.pool.clone()),
        Settings::default(),
    )
    .await
    .unwrap();
    while renamed.pass_now().await.unwrap().unwrap_or_default() > 0 {}
    assert_eq!(
        audience_of(world.erp).await,
        (true, Some("books".to_owned()))
    );
    assert_eq!(
        wire.audiences(),
        BTreeSet::from([("books".to_owned(), world.erp)])
    );
    assert_eq!(
        wire.held(world.erp)["regions"].len(),
        1,
        "provisioned again"
    );
    let declared: Vec<String> = sqlx::query_scalar("SELECT name FROM sync.audiences ORDER BY 1")
        .fetch_all(&world.pool)
        .await
        .unwrap();
    assert_eq!(declared, ["books", "devices"]);

    // Removed: let go of.
    let removed = Server::install(
        world.pool.clone(),
        vec![Audience::new("devices", DEVICES, device_codecs())],
        Wire::default(),
        Anyone::new(world.pool.clone()),
        Settings::default(),
    )
    .await
    .unwrap();
    while removed.pass_now().await.unwrap().unwrap_or_default() > 0 {}
    assert!(!audience_of(world.erp).await.0);
    assert_eq!(held(world.erp).await, 0);

    // Recorded before there were audiences, and live in none now.
    sqlx::raw_sql(&format!(
        "UPDATE sync.replicas SET audience = NULL WHERE replica_id = '{}'; \
         UPDATE fixture.devices SET live = false WHERE id = '{}';",
        world.device, world.device
    ))
    .execute(&world.pool)
    .await
    .unwrap();
    while removed.pass_now().await.unwrap().unwrap_or_default() > 0 {}
    assert!(!audience_of(world.device).await.0);
    assert_eq!(held(world.device).await, 0);
}

/// On the first run after an engine of one audience, what that engine ran
/// with is compared by the audience that takes its replicas over, wherever
/// it is declared: unchanged, nothing is provisioned again; changed, its
/// replicas are.
#[sqlx::test(migrations = false)]
async fn what_an_engine_of_one_audience_ran_with_is_its_replicas_audiences(pool: PgPool) {
    let world = World::serving(
        pool,
        vec![Audience::new("devices", DEVICES, device_codecs())],
    )
    .await;
    world
        .exec(&format!(
            "INSERT INTO fixture.regions VALUES ('{REGION}', '{{p}}', 'coast');"
        ))
        .await;
    world.settle().await;
    let snapshots_to = |wire: &Wire, replica: Uuid| {
        wire.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, to, body)| *to == replica && matches!(body, Body::EndSnapshot(_)))
            .count()
    };
    // The ERP is new the first time, and the second its own declaration
    // stands.
    for (ran, provisioned, erp_provisioned) in [
        (
            "(SELECT declared FROM sync.audiences WHERE name = 'devices')",
            0,
            1,
        ),
        ("'what it ran with before'", 1, 0),
    ] {
        sqlx::raw_sql(&format!(
            "UPDATE sync.version SET declared = {ran}; \
             DELETE FROM sync.audiences WHERE name = 'devices'; \
             UPDATE sync.replicas SET audience = NULL WHERE replica_id = '{}';",
            world.device
        ))
        .execute(&world.pool)
        .await
        .unwrap();
        let wire = Wire::default();
        // The new audience declared first.
        let upgraded = Server::install(
            world.pool.clone(),
            vec![erp(), Audience::new("devices", DEVICES, device_codecs())],
            wire.clone(),
            Anyone::new(world.pool.clone()),
            Settings::default(),
        )
        .await
        .unwrap();
        while upgraded.pass_now().await.unwrap().unwrap_or_default() > 0 {}
        assert_eq!(
            snapshots_to(&wire, world.device),
            provisioned,
            "ran with {ran}"
        );
        assert_eq!(snapshots_to(&wire, world.erp), erp_provisioned);
    }
}

/// Two audiences that declare one table's key in other orders are refused:
/// its one trigger writes the key in one.
#[sqlx::test(migrations = false)]
async fn one_key_in_two_orders_is_refused(pool: PgPool) {
    #[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
    #[row(
        table = "pairs",
        schema = "fixture",
        mirror = "catalog",
        sync = "broadcast",
        key = "a, b"
    )]
    struct Ab {
        a: Uuid,
        b: Uuid,
        partition_id: Uuid,
    }
    #[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
    #[row(
        table = "pairs",
        schema = "fixture",
        mirror = "catalog",
        sync = "broadcast",
        key = "b, a"
    )]
    struct Ba {
        a: Uuid,
        b: Uuid,
        partition_id: Uuid,
    }
    sqlx::raw_sql(&format!(
        "{SCHEMA} CREATE TABLE fixture.pairs (a UUID NOT NULL, b UUID NOT NULL, \
         partition_id UUID NOT NULL, PRIMARY KEY (a, b)); \
         CREATE INDEX ON fixture.pairs (partition_id);"
    ))
    .execute(&pool)
    .await
    .unwrap();
    let refused = Server::install(
        pool.clone(),
        vec![
            Audience::new("devices", DEVICES, vec![Codec::json::<Ab>(None)]),
            Audience::new("erp", ERPS, vec![Codec::json::<Ba>(None)]),
        ],
        Wire::default(),
        Anyone::new(pool.clone()),
        Settings::default(),
    )
    .await;
    match refused {
        Err(Error::Misuse(why)) => assert!(why.contains("declare its key in one order"), "{why}"),
        other => panic!("not refused: {other:?}"),
    }
}

/// A partitioned audience may copy a parent that has no partition column,
/// and that it holds itself, a column of it the replicas own: one trigger
/// notes every change of the parent, and the copies go again.
#[sqlx::test(migrations = false)]
async fn a_parent_without_the_partition_column_may_be_copied(pool: PgPool) {
    /// A zone, shared by every partition, with a note each replica keeps.
    #[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
    #[row(table = "zones", schema = "fixture", mirror = "both")]
    struct Zone {
        id: Uuid,
        code: String,
        #[row(replica_owned)]
        note: String,
    }
    #[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
    #[row(
        table = "districts",
        schema = "fixture",
        mirror = "catalog",
        sync = "broadcast",
        from = "fixture.zoned",
        depends = "zone_id",
        unmirrored = "list_id"
    )]
    struct Zoned {
        id: Uuid,
        partition_id: Uuid,
        region_id: Uuid,
        zone_id: Option<Uuid>,
        name: String,
        zone: String,
    }
    sqlx::raw_sql(&format!(
        "{SCHEMA} CREATE TABLE fixture.zones (id UUID PRIMARY KEY, code TEXT NOT NULL, note TEXT NOT NULL); \
         ALTER TABLE fixture.districts ADD COLUMN zone_id UUID REFERENCES fixture.zones (id); \
         CREATE INDEX ON fixture.districts (zone_id); \
         CREATE VIEW fixture.zoned AS \
             SELECT d.id, d.partition_id, d.region_id, d.zone_id, d.name, z.code || z.note AS zone \
               FROM fixture.districts d JOIN fixture.zones z ON z.id = d.zone_id;"
    ))
    .execute(&pool)
    .await
    .unwrap();
    let audiences = || -> Vec<Audience<()>> {
        vec![Audience::new(
            "erp",
            ERPS,
            vec![
                Codec::json::<Region>(None),
                Codec::json::<Zone>(None),
                Codec::json::<Zoned>(None),
            ],
        )]
    };
    let mut conn = pool.acquire().await.unwrap();
    crate::plan::tests::install(&mut conn).await;
    let printed = crate::plan::Engine::triggers(&mut conn, &audiences())
        .await
        .expect("a partitioned audience copies a parent with no partition column");
    let zones = printed
        .lines()
        .find(|l| l.contains("\"zones\"") && l.contains("note_row"))
        .expect("the zones are noted");
    assert!(
        !zones.contains("'note'"),
        "a change of what the replicas own is a change too: {zones}"
    );
}

/// A replica counts only the changes its own audience plans: one of a table
/// another audience holds alone keeps it from nothing.
#[sqlx::test(migrations = false)]
async fn a_replica_waits_only_on_what_its_audience_plans(pool: PgPool) {
    use crate::status::{ReplicaState, Scope};
    let world = World::serving(
        pool,
        vec![
            Audience::new("devices", DEVICES, device_codecs()),
            Audience::new("erp", ERPS, vec![Codec::json::<Region>(None)]),
        ],
    )
    .await;
    world
        .exec(&format!(
            "INSERT INTO fixture.regions VALUES ('{REGION}', '{{p}}', 'coast');"
        ))
        .await;
    world.settle().await;
    world
        .exec(&format!(
            "INSERT INTO fixture.lists VALUES ('{LIST}', '{{p}}', 'A', true);"
        ))
        .await;
    let engine = world.server.engine();
    let mut conn = world.pool.acquire().await.unwrap();
    let overview = engine.overview(&mut conn, Scope::All, 10).await.unwrap();
    let state = |replica: Uuid| {
        overview
            .replicas
            .first
            .iter()
            .find(|r| r.replica == replica)
            .map(|r| (r.state, r.pending))
            .unwrap()
    };
    assert_eq!(state(world.device), (ReplicaState::CatchingUp, 1));
    assert_eq!(state(world.erp).1, 0, "the lists are not the ERP's");
    let erp = engine
        .health(&mut conn, Scope::Audience("erp"), 10)
        .await
        .unwrap();
    assert_eq!(erp.pending, 0);
}
