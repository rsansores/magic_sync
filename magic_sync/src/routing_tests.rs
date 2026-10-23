//! Routed tables, as the planner reads them: the view checked against the
//! row's key and the replicas, what it reads found in the catalog — column
//! by column, through the views and the SQL-standard functions it reads —
//! and refused where the catalog cannot see; the triggers that note a
//! change of it, printed in one order and held to it when the server
//! starts; and where the view sends rows.

use super::*;
use crate::run::server::routed_tests::{
    sites, Document, RoutedManual, RoutedTask, A, A1, A2, B, B1, DEVICES, EAST, NORTH, SCHEMA,
    SOUTH,
};
use crate::run::server::routed_tests::{Manual, Task};
use sqlx::Connection as _;

const SAFETY: &str = "01900000-0000-7000-8000-000000000311";
const MENU: &str = "01900000-0000-7000-8000-000000000312";
const ROTA: &str = "01900000-0000-7000-8000-000000000313";
const OLD: &str = "01900000-0000-7000-8000-000000000314";

fn uuid(id: &str) -> Uuid {
    id.parse().expect("a uuid")
}

/// The engine's schema and the fixture's, its companies, sites and devices.
async fn fixture(pool: &crate::DbPool) -> sqlx::pool::PoolConnection<crate::Db> {
    let mut conn = pool.acquire().await.expect("a connection");
    super::tests::install(&mut conn).await;
    sqlx::raw_sql(&format!("{SCHEMA}{}", sites()))
        .execute(&mut *conn)
        .await
        .expect("the fixture");
    conn
}

/// The documents, routed by `view`, its columns `row` and `replica`.
fn routed_by(view: &'static str, row: &'static [&'static str], replica: &'static str) -> Table {
    let mut table = Table::of::<Document>();
    table.route = Route::Routed { view, row, replica };
    table
}

/// The engine of `tables`, installed, its replicas seen.
async fn engine(conn: &mut Conn, tables: &[&Table]) -> Engine {
    let mut tx = conn.begin().await.expect("a transaction");
    let engine = Engine::install_one(&mut tx, tables, DEVICES)
        .await
        .expect("the engine");
    engine.observe(&mut tx).await.expect("observed");
    tx.commit().await.expect("installed");
    engine
}

/// Why `tables` cannot be planned.
async fn refused(conn: &mut Conn, tables: &[&Table]) -> String {
    let mut tx = conn.begin().await.expect("a transaction");
    refusal(Engine::install_one(&mut tx, tables, DEVICES).await)
}

fn refusal(result: Result<Engine, Error>) -> String {
    match result {
        Err(Error::Misuse(why)) => why,
        Err(e) => panic!("refused for another reason: {e}"),
        Ok(_) => panic!("not refused"),
    }
}

/// What the plan of the documents reads, by table, with its columns.
fn reads(engine: &Engine) -> BTreeMap<String, Vec<String>> {
    let planner = engine.only();
    let routing = planner.plans[planner.index("documents").expect("the documents")]
        .routing
        .as_ref()
        .expect("routed");
    routing
        .reads
        .iter()
        .map(|(on, columns)| (on.sql(), columns.iter().cloned().collect()))
        .collect()
}

fn columns(names: &[&str]) -> Vec<String> {
    names.iter().map(|n| (*n).to_owned()).collect()
}

/// Every pair of a device and a document its view pairs, of a live document
/// and in the device's company, is wanted, and no other: an archived one,
/// or one a rule of another company pairs with a device, is not. Asked
/// about some keys, or some replicas, the view answers for those.
#[sqlx::test(migrations = false)]
async fn a_routed_table_is_wanted_where_its_view_pairs_it(pool: crate::DbPool) {
    let mut conn = fixture(&pool).await;
    sqlx::raw_sql(&format!(
        "INSERT INTO fixture.documents (id, company_id, title, archived_at) \
              VALUES ('{SAFETY}', '{A}', 'safety', NULL), ('{MENU}', '{A}', 'menu', NULL), \
                     ('{ROTA}', '{B}', 'rota', NULL), ('{OLD}', '{A}', 'old', now()); \
         INSERT INTO fixture.rules (document_id, site_id, company_id) \
              VALUES ('{SAFETY}', '{NORTH}', '{A}'), ('{MENU}', '{NORTH}', '{A}'), \
                     ('{MENU}', '{SOUTH}', '{A}'), ('{OLD}', '{NORTH}', '{A}'), \
                     ('{ROTA}', '{EAST}', '{B}'), ('{ROTA}', '{NORTH}', '{B}');"
    ))
    .execute(&mut *conn)
    .await
    .expect("rules");
    let table = Table::of::<Document>();
    let engine = engine(&mut conn, &[&table]).await;
    let planner = engine.only();
    let i = planner.index("documents").expect("the documents");
    let pairs = |pairs: &[(&str, &str)]| -> BTreeSet<(Uuid, String)> {
        pairs
            .iter()
            .map(|(device, document)| (uuid(device), (*document).to_owned()))
            .collect()
    };
    assert_eq!(
        planner.wanted(&mut conn, i, None, None).await.unwrap(),
        pairs(&[(A1, SAFETY), (A1, MENU), (A2, MENU), (B1, ROTA)]),
        "no archived document, no pair across companies"
    );
    assert_eq!(
        planner
            .wanted(&mut conn, i, Some(&[MENU.to_owned()]), None)
            .await
            .unwrap(),
        pairs(&[(A1, MENU), (A2, MENU)])
    );
    assert_eq!(
        planner
            .wanted(&mut conn, i, None, Some(&[uuid(A2)]))
            .await
            .unwrap(),
        pairs(&[(A2, MENU)])
    );
}

/// A view may answer a pair more than once: the row is owed once.
#[sqlx::test(migrations = false)]
async fn a_pair_the_view_answers_twice_is_owed_once(pool: crate::DbPool) {
    let mut conn = fixture(&pool).await;
    sqlx::raw_sql(&format!(
        "CREATE VIEW fixture.twice AS \
             SELECT * FROM fixture.document_devices UNION ALL SELECT * FROM fixture.document_devices; \
         INSERT INTO fixture.documents (id, company_id, title) VALUES ('{SAFETY}', '{A}', 'safety'); \
         INSERT INTO fixture.rules (document_id, site_id, company_id) \
              VALUES ('{SAFETY}', '{NORTH}', '{A}'), ('{SAFETY}', '{SOUTH}', '{A}');"
    ))
    .execute(&mut *conn)
    .await
    .expect("a view that repeats itself");
    let table = routed_by(r#""fixture"."twice""#, &["document_id"], "device_id");
    let engine = engine(&mut conn, &[&table]).await;
    let planner = engine.only();
    let wanted = planner.wanted_sql(0, "true").expect("a rule");
    let (rows, pairs): (i64, i64) = sqlx::query_as(&format!(
        "SELECT count(*), count(DISTINCT (w.replica_id, w.key)) FROM ({wanted}) w"
    ))
    .fetch_one(&mut *conn)
    .await
    .expect("wanted");
    assert_eq!((rows, pairs), (2, 2), "each pair once, of the view's four");
}

/// A view that cannot route the table is refused when the server starts,
/// naming what it lacks: no such relation, no column to hold the key, or
/// one of another type, no uuid column naming the replica or the
/// partition.
#[sqlx::test(migrations = false)]
async fn a_view_that_cannot_route_the_table_is_refused_naming_what_it_lacks(pool: crate::DbPool) {
    let mut conn = fixture(&pool).await;
    sqlx::raw_sql(
        "CREATE VIEW fixture.no_key AS SELECT d.id AS device_id, d.company_id FROM fixture.devices d; \
         CREATE VIEW fixture.int_key AS SELECT 1 AS document_id, d.id AS device_id, d.company_id \
             FROM fixture.devices d; \
         CREATE VIEW fixture.text_replica AS \
             SELECT r.document_id, r.site_id::text AS device_id, r.company_id FROM fixture.rules r; \
         CREATE VIEW fixture.no_partition AS \
             SELECT r.document_id, d.id AS device_id FROM fixture.rules r JOIN fixture.devices d USING (site_id);",
    )
    .execute(&mut *conn)
    .await
    .expect("views");
    for (table, refusal) in [
        (
            routed_by(r#""fixture"."nowhere""#, &["document_id"], "device_id"),
            r#"`documents` is routed by "fixture"."nowhere", which is no table or view of the database"#,
        ),
        (
            routed_by(r#""fixture"."no_key""#, &["document_id"], "device_id"),
            "which has no column `document_id` to hold its key column `id`",
        ),
        (
            routed_by(r#""fixture"."int_key""#, &["document_id"], "device_id"),
            "whose column `document_id` is integer, and the key column `id` it holds uuid",
        ),
        (
            routed_by(r#""fixture"."text_replica""#, &["document_id"], "device_id"),
            "which has no uuid column `device_id` to name the replica of each pair",
        ),
        (
            routed_by(r#""fixture"."no_partition""#, &["document_id"], "device_id"),
            "which has no uuid column `company_id` to name the partition of each pair",
        ),
    ] {
        let why = refused(&mut conn, &[&table]).await;
        assert!(why.contains(refusal), "{why}");
    }
}

/// A view that calls a function whose body the catalog does not look into
/// — PL/pgSQL, SQL written as a string, behind an operator too — or that
/// reads a materialized view, is refused when the server starts, naming
/// it: what it reads could change, and no trigger would say.
#[sqlx::test(migrations = false)]
async fn what_the_catalog_cannot_see_into_is_refused_naming_it(pool: crate::DbPool) {
    let mut conn = fixture(&pool).await;
    sqlx::raw_sql(
        "CREATE FUNCTION fixture.open_site(site UUID) RETURNS BOOLEAN LANGUAGE plpgsql STABLE AS $$ \
             BEGIN RETURN EXISTS (SELECT 1 FROM fixture.sites s WHERE s.id = site); END $$; \
         CREATE FUNCTION fixture.quoted(site UUID) RETURNS BOOLEAN LANGUAGE sql STABLE AS \
             'SELECT EXISTS (SELECT 1 FROM fixture.sites s WHERE s.id = site)'; \
         CREATE FUNCTION fixture.same(a UUID, b UUID) RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE AS $$ \
             BEGIN RETURN a = b; END $$; \
         CREATE OPERATOR fixture.=== (LEFTARG = UUID, RIGHTARG = UUID, FUNCTION = fixture.same); \
         CREATE MATERIALIZED VIEW fixture.placed AS SELECT id, site_id, company_id FROM fixture.devices; \
         CREATE VIEW fixture.by_plpgsql AS SELECT r.document_id, d.id AS device_id, r.company_id \
             FROM fixture.rules r JOIN fixture.devices d USING (site_id) WHERE fixture.open_site(r.site_id); \
         CREATE VIEW fixture.by_string AS SELECT r.document_id, d.id AS device_id, r.company_id \
             FROM fixture.rules r JOIN fixture.devices d USING (site_id) WHERE fixture.quoted(r.site_id); \
         CREATE VIEW fixture.by_operator AS SELECT r.document_id, d.id AS device_id, r.company_id \
             FROM fixture.rules r JOIN fixture.devices d ON d.site_id OPERATOR(fixture.===) r.site_id; \
         CREATE VIEW fixture.inner_plpgsql AS SELECT * FROM fixture.by_plpgsql; \
         CREATE VIEW fixture.by_matview AS SELECT r.document_id, p.id AS device_id, r.company_id \
             FROM fixture.rules r JOIN fixture.placed p USING (site_id);",
    )
    .execute(&mut *conn)
    .await
    .expect("views");
    for (table, refusal) in [
        (
            routed_by(r#""fixture"."by_plpgsql""#, &["document_id"], "device_id"),
            "whose the view fixture.by_plpgsql calls fixture.open_site(uuid), a function whose \
             body PostgreSQL does not look into",
        ),
        (
            routed_by(r#""fixture"."by_string""#, &["document_id"], "device_id"),
            "calls fixture.quoted(uuid), a function whose body PostgreSQL does not look into",
        ),
        (
            routed_by(r#""fixture"."by_operator""#, &["document_id"], "device_id"),
            "the operator fixture.===(uuid,uuid), calls fixture.same(uuid,uuid)",
        ),
        (
            routed_by(
                r#""fixture"."inner_plpgsql""#,
                &["document_id"],
                "device_id",
            ),
            "whose the view fixture.by_plpgsql calls fixture.open_site(uuid)",
        ),
        (
            routed_by(r#""fixture"."by_matview""#, &["document_id"], "device_id"),
            r#"whose the view fixture.by_matview reads "fixture"."placed", a materialized view"#,
        ),
        (
            routed_by(r#""fixture"."placed""#, &["id"], "site_id"),
            "which is a materialized view",
        ),
    ] {
        let why = refused(&mut conn, &[&table]).await;
        assert!(why.contains(refusal), "{why}");
    }
    let why = refused(
        &mut conn,
        &[&routed_by(
            r#""fixture"."by_plpgsql""#,
            &["document_id"],
            "device_id",
        )],
    )
    .await;
    assert!(why.contains("BEGIN ATOMIC"), "says what to write: {why}");
}

/// A view that calls a function of PostgreSQL's or of an extension's that
/// is not immutable — directly, through an operator, as a SQL value, or in
/// the body of a SQL-standard function — is refused when the server
/// starts, naming it: what it answers changes with no write. What a view
/// compares, casts and tests uuids, integers, text and booleans with is
/// immutable, and starts.
#[sqlx::test(migrations = false)]
async fn a_function_that_is_not_immutable_is_refused_naming_it(pool: crate::DbPool) {
    let mut conn = fixture(&pool).await;
    sqlx::raw_sql(
        "CREATE EXTENSION IF NOT EXISTS pgcrypto; \
         ALTER TABLE fixture.rules ADD COLUMN until TIMESTAMPTZ, ADD COLUMN rank INTEGER; \
         CREATE FUNCTION fixture.current(at TIMESTAMPTZ) RETURNS BOOLEAN LANGUAGE sql STABLE \
             RETURN at > now(); \
         CREATE VIEW fixture.by_now AS SELECT r.document_id, d.id AS device_id, r.company_id \
             FROM fixture.rules r JOIN fixture.devices d USING (site_id) WHERE r.until > now(); \
         CREATE VIEW fixture.by_date AS SELECT r.document_id, d.id AS device_id, r.company_id \
             FROM fixture.rules r JOIN fixture.devices d USING (site_id) \
            WHERE current_date > '2026-01-01'; \
         CREATE VIEW fixture.by_setting AS SELECT r.document_id, d.id AS device_id, r.company_id \
             FROM fixture.rules r JOIN fixture.devices d USING (site_id) \
            WHERE current_setting('app.company', true) IS NULL; \
         CREATE VIEW fixture.by_chance AS SELECT r.document_id, d.id AS device_id, r.company_id \
             FROM fixture.rules r JOIN fixture.devices d USING (site_id) WHERE random() < 2; \
         CREATE VIEW fixture.by_operator AS SELECT r.document_id, d.id AS device_id, r.company_id \
             FROM fixture.rules r JOIN fixture.devices d USING (site_id) \
            WHERE r.until > '2026-01-01'::date; \
         CREATE VIEW fixture.by_extension AS SELECT r.document_id, d.id AS device_id, r.company_id \
             FROM fixture.rules r JOIN fixture.devices d USING (site_id) \
            WHERE length(gen_random_bytes(1)) = 1; \
         CREATE VIEW fixture.by_function AS SELECT r.document_id, d.id AS device_id, r.company_id \
             FROM fixture.rules r JOIN fixture.devices d USING (site_id) WHERE fixture.current(r.until);",
    )
    .execute(&mut *conn)
    .await
    .expect("views");
    for (table, refusal) in [
        (
            routed_by(r#""fixture"."by_now""#, &["document_id"], "device_id"),
            "whose the view fixture.by_now calls now(), a function that is not immutable: what \
             it answers changes with no write a trigger sees",
        ),
        (
            routed_by(r#""fixture"."by_date""#, &["document_id"], "device_id"),
            "calls current_date, a function that is not immutable",
        ),
        (
            routed_by(r#""fixture"."by_setting""#, &["document_id"], "device_id"),
            "calls current_setting(text,boolean), a function that is not immutable",
        ),
        (
            routed_by(r#""fixture"."by_chance""#, &["document_id"], "device_id"),
            "calls random(), a function that is not immutable",
        ),
        (
            routed_by(r#""fixture"."by_operator""#, &["document_id"], "device_id"),
            "calls timestamptz_gt_date(timestamp with time zone,date), a function that is not \
             immutable",
        ),
        (
            routed_by(r#""fixture"."by_extension""#, &["document_id"], "device_id"),
            "gen_random_bytes(integer), a function that is not immutable",
        ),
        (
            routed_by(r#""fixture"."by_function""#, &["document_id"], "device_id"),
            "whose the function fixture.current(timestamp with time zone) calls now()",
        ),
    ] {
        let why = refused(&mut conn, &[&table]).await;
        assert!(why.contains(refusal), "{why}");
    }

    sqlx::raw_sql(
        "CREATE OR REPLACE VIEW fixture.document_devices AS \
             SELECT r.document_id, d.id AS device_id, r.company_id \
               FROM fixture.rules r JOIN fixture.devices d ON d.site_id = r.site_id \
              WHERE r.granted AND NOT (d.live IS FALSE) AND r.site_id::text <> '' \
                AND coalesce(r.rank, 0)::bigint < 10 AND r.rank IS DISTINCT FROM -1 \
                AND r.note || '' >= '' AND r.company_id IN (d.company_id) \
                AND (r.until IS NULL OR r.until = r.until);",
    )
    .execute(&mut *conn)
    .await
    .expect("a view of comparisons and casts");
    let table = Table::of::<Document>();
    engine(&mut conn, &[&table]).await;
}

/// A view that reads a partitioned table, a partition, or a table that
/// inherits or is inherited is refused when the server starts, naming it: a
/// trigger of statements fires on the table a statement names, and a write
/// of one table of the tree changes the rows of another.
#[sqlx::test(migrations = false)]
async fn a_table_of_a_partitioned_or_inheritance_tree_is_refused_naming_it(pool: crate::DbPool) {
    let mut conn = fixture(&pool).await;
    sqlx::raw_sql(&format!(
        "CREATE TABLE fixture.zones (site_id UUID NOT NULL, company_id UUID NOT NULL) \
             PARTITION BY LIST (company_id); \
         CREATE TABLE fixture.zones_a PARTITION OF fixture.zones FOR VALUES IN ('{A}'); \
         CREATE TABLE fixture.places (site_id UUID NOT NULL, company_id UUID NOT NULL); \
         CREATE TABLE fixture.kiosks (kind TEXT) INHERITS (fixture.places); \
         CREATE VIEW fixture.by_zone AS SELECT r.document_id, d.id AS device_id, r.company_id \
             FROM fixture.rules r JOIN fixture.devices d ON d.site_id = r.site_id \
             JOIN fixture.zones z ON z.site_id = r.site_id; \
         CREATE VIEW fixture.by_zone_a AS SELECT r.document_id, d.id AS device_id, r.company_id \
             FROM fixture.rules r JOIN fixture.devices d ON d.site_id = r.site_id \
             JOIN fixture.zones_a z ON z.site_id = r.site_id; \
         CREATE VIEW fixture.by_place AS SELECT r.document_id, d.id AS device_id, r.company_id \
             FROM fixture.rules r JOIN fixture.devices d ON d.site_id = r.site_id \
             JOIN fixture.places p ON p.site_id = r.site_id; \
         CREATE VIEW fixture.by_kiosk AS SELECT r.document_id, d.id AS device_id, r.company_id \
             FROM fixture.rules r JOIN fixture.devices d ON d.site_id = r.site_id \
             JOIN fixture.kiosks k ON k.site_id = r.site_id;"
    ))
    .execute(&mut *conn)
    .await
    .expect("trees");
    for (table, refusal) in [
        (
            routed_by(r#""fixture"."by_zone""#, &["document_id"], "device_id"),
            r#"whose the view fixture.by_zone reads "fixture"."zones", a partitioned table"#,
        ),
        (
            routed_by(r#""fixture"."by_zone_a""#, &["document_id"], "device_id"),
            r#"reads "fixture"."zones_a", a table of an inheritance tree"#,
        ),
        (
            routed_by(r#""fixture"."by_place""#, &["document_id"], "device_id"),
            r#"reads "fixture"."places", a table of an inheritance tree"#,
        ),
        (
            routed_by(r#""fixture"."by_kiosk""#, &["document_id"], "device_id"),
            r#"reads "fixture"."kiosks", a table of an inheritance tree"#,
        ),
    ] {
        let why = refused(&mut conn, &[&table]).await;
        assert!(why.contains(refusal), "{why}");
    }
}

/// The routed table's own rows are planned by their key, off a trigger
/// that notes no write of the columns its replicas write alone: a view that
/// reads one of those is refused when the server starts, naming it, and a
/// view that reads none starts.
#[sqlx::test(migrations = false)]
async fn a_view_that_reads_a_column_the_replicas_write_is_refused(pool: crate::DbPool) {
    let mut conn = fixture(&pool).await;
    let mut table = Table::of::<Document>();
    table.mirror = crate::row::Mirror::Both;
    table
        .columns
        .iter_mut()
        .find(|c| c.name == "title")
        .expect("the title")
        .replica_owned = true;
    engine(&mut conn, &[&table]).await;

    sqlx::raw_sql(
        "CREATE OR REPLACE VIEW fixture.document_devices AS \
             SELECT r.document_id, d.id AS device_id, r.company_id \
               FROM fixture.rules r JOIN fixture.devices d ON d.site_id = r.site_id \
               JOIN fixture.documents doc ON doc.id = r.document_id \
              WHERE r.granted AND doc.title <> '';",
    )
    .execute(&mut *conn)
    .await
    .expect("a view that reads the title");
    let why = refused(&mut conn, &[&table]).await;
    assert!(
        why.contains(
            "which reads `title` of `documents` itself, a column its replicas write \
             (`replica_owned`)"
        ),
        "{why}"
    );
}

/// A view named alone — `view`, not `schema.view` — is the one the
/// connection's search path finds, and routes as one named whole.
#[sqlx::test(migrations = false)]
async fn a_view_named_alone_is_the_one_the_search_path_finds(pool: crate::DbPool) {
    let mut conn = fixture(&pool).await;
    sqlx::raw_sql(&format!(
        "CREATE VIEW public.devices_of AS \
             SELECT r.document_id, d.id AS device_id, r.company_id \
               FROM fixture.rules r JOIN fixture.devices d ON d.site_id = r.site_id; \
         INSERT INTO fixture.documents (id, company_id, title) VALUES ('{SAFETY}', '{A}', 'safety'); \
         INSERT INTO fixture.rules (document_id, site_id, company_id) VALUES ('{SAFETY}', '{NORTH}', '{A}');"
    ))
    .execute(&mut *conn)
    .await
    .expect("a view on the search path");
    let table = routed_by(r#""devices_of""#, &["document_id"], "device_id");
    let engine = engine(&mut conn, &[&table]).await;
    assert_eq!(
        reads(&engine),
        BTreeMap::from([
            (
                r#""fixture"."devices""#.to_owned(),
                columns(&["id", "site_id"])
            ),
            (
                r#""fixture"."rules""#.to_owned(),
                columns(&["company_id", "document_id", "site_id"])
            ),
        ])
    );
    let planner = engine.only();
    let i = planner.index("documents").expect("the documents");
    assert_eq!(
        planner.wanted(&mut conn, i, None, None).await.unwrap(),
        BTreeSet::from([(uuid(A1), SAFETY.to_owned())])
    );
}

/// A row with the partition column goes to the replicas of its partition
/// alone, as a broadcast row does: a view that pairs a row of one company
/// with a device of another sends that device nothing.
#[sqlx::test(migrations = false)]
async fn a_row_is_wanted_only_in_its_own_partition(pool: crate::DbPool) {
    let mut conn = fixture(&pool).await;
    sqlx::raw_sql(&format!(
        "CREATE VIEW fixture.everywhere AS \
             SELECT doc.id AS document_id, d.id AS device_id, d.company_id \
               FROM fixture.documents doc CROSS JOIN fixture.devices d; \
         INSERT INTO fixture.documents (id, company_id, title) \
              VALUES ('{SAFETY}', '{A}', 'safety'), ('{ROTA}', '{B}', 'rota');"
    ))
    .execute(&mut *conn)
    .await
    .expect("a view across companies");
    let table = routed_by(r#""fixture"."everywhere""#, &["document_id"], "device_id");
    let engine = engine(&mut conn, &[&table]).await;
    let planner = engine.only();
    let i = planner.index("documents").expect("the documents");
    assert_eq!(
        planner.wanted(&mut conn, i, None, None).await.unwrap(),
        BTreeSet::from([
            (uuid(A1), SAFETY.to_owned()),
            (uuid(A2), SAFETY.to_owned()),
            (uuid(B1), ROTA.to_owned()),
        ]),
        "no document of A to B1, nor of B to A1 or A2"
    );
}

/// A function written SQL-standard — `BEGIN ATOMIC … END`, or `RETURN …` —
/// is read through, as a view is: what its body reads is what the view
/// reads.
#[sqlx::test(migrations = false)]
async fn what_a_sql_standard_function_reads_is_read_through(pool: crate::DbPool) {
    let mut conn = fixture(&pool).await;
    sqlx::raw_sql(
        "CREATE FUNCTION fixture.site_of(device UUID) RETURNS UUID LANGUAGE sql STABLE \
         BEGIN ATOMIC SELECT d.site_id FROM fixture.devices d WHERE d.id = device; END; \
         CREATE FUNCTION fixture.open(site UUID) RETURNS BOOLEAN LANGUAGE sql STABLE \
             RETURN EXISTS (SELECT 1 FROM fixture.sites s WHERE s.id = site AND s.name <> ''); \
         CREATE OR REPLACE VIEW fixture.document_devices AS \
             SELECT r.document_id, d.id AS device_id, r.company_id \
               FROM fixture.rules r JOIN fixture.devices d ON fixture.site_of(d.id) = r.site_id \
              WHERE r.granted AND fixture.open(r.site_id);",
    )
    .execute(&mut *conn)
    .await
    .expect("functions");
    let table = Table::of::<Document>();
    let engine = engine(&mut conn, &[&table]).await;
    assert_eq!(
        reads(&engine),
        BTreeMap::from([
            (
                r#""fixture"."devices""#.to_owned(),
                columns(&["id", "site_id"])
            ),
            (
                r#""fixture"."rules""#.to_owned(),
                columns(&["company_id", "document_id", "granted", "site_id"])
            ),
            (r#""fixture"."sites""#.to_owned(), columns(&["id", "name"])),
        ])
    );
}

/// What PostgreSQL ships, and what an extension installed, reads no table
/// of the host's: a view may call either, immutable.
#[sqlx::test(migrations = false)]
async fn what_postgres_and_its_extensions_ship_is_no_dependency(pool: crate::DbPool) {
    let mut conn = fixture(&pool).await;
    sqlx::raw_sql(
        "CREATE EXTENSION IF NOT EXISTS pgcrypto; \
         CREATE OR REPLACE VIEW fixture.document_devices AS \
             SELECT r.document_id, d.id AS device_id, r.company_id \
               FROM fixture.rules r JOIN fixture.devices d ON d.site_id = r.site_id \
              WHERE r.granted AND lower(coalesce(r.note, '')) = '' \
                AND length(digest('x', 'sha256')) = 32;",
    )
    .execute(&mut *conn)
    .await
    .expect("a view of builtins");
    let table = Table::of::<Document>();
    let engine = engine(&mut conn, &[&table]).await;
    assert_eq!(
        reads(&engine).keys().collect::<Vec<_>>(),
        [r#""fixture"."devices""#, r#""fixture"."rules""#]
    );
}

/// What a view reads is read column by column — a column it does not read
/// is no column a write of re-routes — and never the routed table itself,
/// whose rows are planned by their key. A view it reads is read whole:
/// every column that view reads; and a whole row read anywhere reads every
/// column of the tables read there.
#[sqlx::test(migrations = false)]
async fn what_a_view_reads_is_read_column_by_column_but_its_own_table(pool: crate::DbPool) {
    let conn = fixture(&pool).await;
    let table = Table::of::<Document>();
    let engine_of = |sql: &'static str| {
        let pool = pool.clone();
        let table = &table;
        async move {
            let mut conn = pool.acquire().await.expect("a connection");
            sqlx::raw_sql(sql).execute(&mut *conn).await.expect(sql);
            let engine = engine(&mut conn, &[table]).await;
            reads(&engine)
        }
    };
    let rules = r#""fixture"."rules""#.to_owned();
    let devices = r#""fixture"."devices""#.to_owned();
    assert_eq!(
        engine_of("SELECT 1").await,
        BTreeMap::from([
            (devices.clone(), columns(&["id", "site_id"])),
            (
                rules.clone(),
                columns(&["company_id", "document_id", "granted", "site_id"])
            ),
        ]),
        "no note, which it does not read"
    );
    assert_eq!(
        engine_of(
            "CREATE OR REPLACE VIEW fixture.document_devices AS \
                 SELECT r.document_id, d.id AS device_id, r.company_id \
                   FROM fixture.rules r JOIN fixture.devices d ON d.site_id = r.site_id \
                   JOIN fixture.documents doc ON doc.id = r.document_id \
                  WHERE r.granted AND doc.title <> '';"
        )
        .await
        .keys()
        .collect::<Vec<_>>(),
        [&devices, &rules],
        "not the documents"
    );
    assert_eq!(
        engine_of(
            "CREATE VIEW fixture.granted AS SELECT r.* FROM fixture.rules r WHERE r.granted; \
             CREATE OR REPLACE VIEW fixture.document_devices AS \
                 SELECT g.document_id, d.id AS device_id, g.company_id \
                   FROM fixture.granted g JOIN fixture.devices d ON d.site_id = g.site_id;"
        )
        .await[&rules],
        columns(&["company_id", "document_id", "granted", "note", "site_id"]),
        "the note too, which the view it reads reads"
    );
    assert_eq!(
        engine_of(
            "CREATE OR REPLACE VIEW fixture.document_devices AS \
                 SELECT r.document_id, d.id AS device_id, r.company_id \
                   FROM fixture.rules r JOIN fixture.devices d ON d.site_id = r.site_id \
                  WHERE r.granted AND r IS NOT NULL;"
        )
        .await[&rules],
        columns(&["company_id", "document_id", "granted", "note", "site_id"]),
        "a whole row: every column"
    );
    drop(conn);
}

/// Every table a view reads gets a trigger for each statement that can
/// change it, noting a re-route: printed in one order, its columns in one
/// order, the same text twice; none on the routed table itself.
#[sqlx::test(migrations = false)]
async fn the_triggers_of_what_a_view_reads_are_printed_in_one_order(pool: crate::DbPool) {
    let mut conn = fixture(&pool).await;
    let audiences: [Audience<()>; 1] = [Audience::new(
        "devices",
        DEVICES,
        vec![Codec::json::<Document>(None)],
    )];
    let printed = Engine::triggers(&mut conn, &audiences)
        .await
        .expect("the triggers");
    assert_eq!(
        printed,
        Engine::triggers(&mut conn, &audiences)
            .await
            .expect("the triggers"),
        "the same text, printed again"
    );
    let reroutes: Vec<&str> = printed
        .lines()
        .filter(|l| l.contains("note_reroute"))
        .collect();
    assert_eq!(
        reroutes,
        [
            r#"CREATE TRIGGER sync_reroute_insert AFTER INSERT ON "fixture"."devices" REFERENCING NEW TABLE AS sync_new FOR EACH STATEMENT EXECUTE FUNCTION sync.note_reroute('"fixture"."devices"', 'company_id');"#,
            r#"CREATE TRIGGER sync_reroute_update AFTER UPDATE ON "fixture"."devices" REFERENCING OLD TABLE AS sync_old NEW TABLE AS sync_new FOR EACH STATEMENT EXECUTE FUNCTION sync.note_reroute('"fixture"."devices"', 'company_id', 'id', 'site_id');"#,
            r#"CREATE TRIGGER sync_reroute_delete AFTER DELETE ON "fixture"."devices" REFERENCING OLD TABLE AS sync_old FOR EACH STATEMENT EXECUTE FUNCTION sync.note_reroute('"fixture"."devices"', 'company_id');"#,
            r#"CREATE TRIGGER sync_reroute_truncate AFTER TRUNCATE ON "fixture"."devices" FOR EACH STATEMENT EXECUTE FUNCTION sync.note_reroute('"fixture"."devices"', 'company_id');"#,
            r#"CREATE TRIGGER sync_reroute_insert AFTER INSERT ON "fixture"."rules" REFERENCING NEW TABLE AS sync_new FOR EACH STATEMENT EXECUTE FUNCTION sync.note_reroute('"fixture"."rules"', 'company_id');"#,
            r#"CREATE TRIGGER sync_reroute_update AFTER UPDATE ON "fixture"."rules" REFERENCING OLD TABLE AS sync_old NEW TABLE AS sync_new FOR EACH STATEMENT EXECUTE FUNCTION sync.note_reroute('"fixture"."rules"', 'company_id', 'company_id', 'document_id', 'granted', 'site_id');"#,
            r#"CREATE TRIGGER sync_reroute_delete AFTER DELETE ON "fixture"."rules" REFERENCING OLD TABLE AS sync_old FOR EACH STATEMENT EXECUTE FUNCTION sync.note_reroute('"fixture"."rules"', 'company_id');"#,
            r#"CREATE TRIGGER sync_reroute_truncate AFTER TRUNCATE ON "fixture"."rules" FOR EACH STATEMENT EXECUTE FUNCTION sync.note_reroute('"fixture"."rules"', 'company_id');"#,
        ]
    );
    assert!(
        !printed
            .lines()
            .any(|l| l.contains(r#""fixture"."documents""#) && l.contains("note_reroute")),
        "{printed}"
    );
}

/// The triggers are held to what the views read when the server starts: a
/// view that reads one table more is refused until that table's triggers
/// are written, and one that reads one less, until they are dropped.
#[sqlx::test(migrations = false)]
async fn the_engine_starts_only_on_the_triggers_its_views_need(pool: crate::DbPool) {
    let mut conn = fixture(&pool).await;
    let table = Table::of::<Document>();
    engine(&mut conn, &[&table]).await;
    Engine::new_one(&mut conn, &[&table], DEVICES)
        .await
        .expect("installed as it needs them");

    sqlx::raw_sql(
        "CREATE OR REPLACE VIEW fixture.document_devices AS \
             SELECT r.document_id, d.id AS device_id, r.company_id \
               FROM fixture.rules r JOIN fixture.devices d ON d.site_id = r.site_id \
               JOIN fixture.sites s ON s.id = r.site_id \
              WHERE r.granted AND s.name <> 'closed';",
    )
    .execute(&mut *conn)
    .await
    .expect("a view that reads the sites too");
    let why = refusal(Engine::new_one(&mut conn, &[&table], DEVICES).await);
    assert!(
        why.contains(
            r#"is missing: CREATE TRIGGER sync_reroute_update AFTER UPDATE ON "fixture"."sites""#
        ),
        "{why}"
    );

    engine(&mut conn, &[&table]).await;
    sqlx::raw_sql(
        "CREATE OR REPLACE VIEW fixture.document_devices AS \
             SELECT r.document_id, d.id AS device_id, r.company_id \
               FROM fixture.rules r JOIN fixture.devices d ON d.site_id = r.site_id;",
    )
    .execute(&mut *conn)
    .await
    .expect("a view that reads neither the sites nor whether a rule grants");
    let why = refusal(Engine::new_one(&mut conn, &[&table], DEVICES).await);
    assert!(
        why.contains(r#"`sync_reroute_insert` on "fixture"."sites" notes for no table this engine declares: drop it"#)
            && why.contains(r#"differs, and is: CREATE TRIGGER sync_reroute_update AFTER UPDATE ON "fixture"."rules""#),
        "{why}"
    );
}

/// A view written otherwise routes rows otherwise: the first pass after
/// it changed provisions every replica again, as a declaration changed.
#[sqlx::test(migrations = false)]
async fn a_view_written_otherwise_provisions_every_replica_again(pool: crate::DbPool) {
    let mut conn = fixture(&pool).await;
    let table = Table::of::<Document>();
    engine(&mut conn, &[&table]).await;
    sqlx::raw_sql("UPDATE sync.notes SET done_at = now() WHERE kind = 'resync'")
        .execute(&mut *conn)
        .await
        .expect("provisioned");
    async fn resyncs(conn: &mut Conn) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM sync.notes WHERE kind = 'resync' AND done_at IS NULL",
        )
        .fetch_one(conn)
        .await
        .expect("provisionings")
    }
    engine(&mut conn, &[&table]).await;
    assert_eq!(resyncs(&mut conn).await, 0, "nothing changed");

    sqlx::raw_sql(
        "CREATE OR REPLACE VIEW fixture.document_devices AS \
             SELECT r.document_id, d.id AS device_id, r.company_id \
               FROM fixture.rules r JOIN fixture.devices d ON d.site_id = r.site_id \
              WHERE r.granted AND d.live;",
    )
    .execute(&mut *conn)
    .await
    .expect("the view written otherwise");
    engine(&mut conn, &[&table]).await;
    assert_eq!(
        resyncs(&mut conn).await,
        3,
        "every live device, both companies"
    );
}

/// `assigned` and `grouped` are routes a view says: over the same rows,
/// the view of the device a task names and the view of the devices given a
/// shelf a manual is on want what the engine's own routes want, write after
/// write.
#[sqlx::test(migrations = false)]
async fn a_view_wants_what_assigned_and_grouped_want(pool: crate::DbPool) {
    let mut conn = fixture(&pool).await;
    let own = [Table::of::<Task>(), Table::of::<Manual>()];
    let routed = [Table::of::<RoutedTask>(), Table::of::<RoutedManual>()];
    let own = engine(&mut conn, &own.iter().collect::<Vec<_>>()).await;
    let routed = Engine::read(
        &mut conn,
        &[Engine::one(&routed.iter().collect::<Vec<_>>(), DEVICES)],
    )
    .await
    .expect("the routed engine");
    async fn wanted(engine: &Engine, conn: &mut Conn, table: &str) -> BTreeSet<(Uuid, String)> {
        let planner = engine.only();
        let i = planner.index(table).expect(table);
        planner.wanted(conn, i, None, None).await.expect("wanted")
    }
    let writes = [
        "INSERT INTO fixture.tasks VALUES \
             ('01900000-0000-7000-8000-000000000411', '{A}', '{A1}', 'open'), \
             ('01900000-0000-7000-8000-000000000412', '{A}', '{A2}', 'close'), \
             ('01900000-0000-7000-8000-000000000413', '{B}', NULL, 'count'); \
         INSERT INTO fixture.manuals VALUES \
             ('01900000-0000-7000-8000-000000000511', '{A}', 'fryer'), \
             ('01900000-0000-7000-8000-000000000512', '{A}', 'till'); \
         INSERT INTO fixture.shelves VALUES \
             ('01900000-0000-7000-8000-000000000611', '{A}'), ('01900000-0000-7000-8000-000000000612', '{A}'); \
         INSERT INTO fixture.shelf_manuals VALUES \
             ('01900000-0000-7000-8000-000000000611', '01900000-0000-7000-8000-000000000511', '{A}'), \
             ('01900000-0000-7000-8000-000000000611', '01900000-0000-7000-8000-000000000512', '{A}'), \
             ('01900000-0000-7000-8000-000000000612', '01900000-0000-7000-8000-000000000512', '{A}'); \
         INSERT INTO fixture.device_shelves VALUES \
             ('01900000-0000-7000-8000-000000000611', '{A1}', '{A}'), \
             ('01900000-0000-7000-8000-000000000612', '{A2}', '{A}');",
        "UPDATE fixture.tasks SET device_id = '{A2}' WHERE title = 'open'; \
         UPDATE fixture.tasks SET device_id = '{B1}' WHERE title = 'count'",
        "INSERT INTO fixture.device_shelves VALUES ('01900000-0000-7000-8000-000000000611', '{A2}', '{A}')",
        "DELETE FROM fixture.shelf_manuals WHERE manual_id = '01900000-0000-7000-8000-000000000512' \
            AND shelf_id = '01900000-0000-7000-8000-000000000611'",
        "DELETE FROM fixture.shelves WHERE id = '01900000-0000-7000-8000-000000000612'",
        "DELETE FROM fixture.tasks WHERE title = 'close'",
    ];
    for write in writes {
        let write = write
            .replace("{A}", A)
            .replace("{B}", B)
            .replace("{A1}", A1)
            .replace("{A2}", A2)
            .replace("{B1}", B1);
        sqlx::raw_sql(&write)
            .execute(&mut *conn)
            .await
            .expect("a write");
        for table in ["tasks", "manuals"] {
            let ours = wanted(&own, &mut conn, table).await;
            assert!(!ours.is_empty() || table == "manuals", "{write}");
            assert_eq!(
                wanted(&routed, &mut conn, table).await,
                ours,
                "{table}, after {write}"
            );
        }
    }
}

/// A change of a table no view reads any more — its trigger outlived the
/// declaration — is refused where it is planned, as a row's is, and done
/// by the next look at the replicas before any pass plans it.
#[sqlx::test(migrations = false)]
async fn a_re_route_through_a_table_no_view_reads_is_done_not_planned(pool: crate::DbPool) {
    let mut conn = fixture(&pool).await;
    let table = Table::of::<Document>();
    let engine = engine(&mut conn, &[&table]).await;
    let change = Change::Rerouted {
        table: r#""fixture"."gone""#.to_owned(),
        partition: None,
    };
    let mut tx = conn
        .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("a transaction");
    assert!(matches!(
        engine.only().plan(&mut tx, &[Note { id: 1, change }]).await,
        Err(Error::Misuse(why)) if why.contains("no routed table's view reads")
    ));
    drop(tx);
    sqlx::raw_sql(
        r#"INSERT INTO sync.notes (kind, relation) VALUES ('rerouted', '"fixture"."gone"'),
                                                         ('rerouted', '"fixture"."rules"')"#,
    )
    .execute(&mut *conn)
    .await
    .expect("notes");
    engine.observe(&mut conn).await.expect("observed");
    let pending: Vec<String> = sqlx::query_scalar(
        "SELECT relation FROM sync.notes WHERE kind = 'rerouted' AND done_at IS NULL",
    )
    .fetch_all(&mut *conn)
    .await
    .expect("pending");
    assert_eq!(pending, [r#""fixture"."rules""#]);
}

/// A database whose engine predates routed tables is brought to the step
/// that knows them, its pending notes kept, and notes a re-route after.
#[sqlx::test(migrations = false)]
async fn a_schema_from_before_routed_tables_is_upgraded_keeping_its_notes(pool: crate::DbPool) {
    let mut conn = pool.acquire().await.expect("a connection");
    sqlx::raw_sql("DROP SCHEMA IF EXISTS sync CASCADE")
        .execute(&mut *conn)
        .await
        .expect("drop the schema");
    for step in 1..=3 {
        sqlx::raw_sql(&migration(step).expect("a released step"))
            .execute(&mut *conn)
            .await
            .expect("the step");
    }
    sqlx::raw_sql(
        "INSERT INTO sync.notes (kind, relation, row_key) VALUES ('row', 'documents', 'k')",
    )
    .execute(&mut *conn)
    .await
    .expect("a note of version 3");
    let mut tx = conn.begin().await.expect("a transaction");
    migrate(&mut tx).await.expect("upgraded");
    tx.commit().await.expect("upgraded");
    assert_eq!(version(&mut conn).await.unwrap(), Some(4));
    sqlx::raw_sql(&format!(
        r#"INSERT INTO sync.notes (kind, relation, partition_id) VALUES ('rerouted', '"fixture"."rules"', '{A}')"#
    ))
    .execute(&mut *conn)
    .await
    .expect("a re-route, noted");
    let mut tx = conn
        .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("a transaction");
    let notes = pending(&mut tx, 10).await.expect("pending");
    assert!(
        matches!(notes.as_slice(), [
            Note { change: Change::Row { table, .. }, .. },
            Note { change: Change::Rerouted { table: through, partition: Some(partition) }, .. },
        ] if table == "documents" && through == r#""fixture"."rules""# && *partition == uuid(A)),
        "{notes:?}"
    );
}
