//! The engine against a schema that has every strategy, driven by a seeded
//! random walk of the writes an office makes: after every step, each replica
//! holds exactly what its strategies say, and never a row whose parent it
//! lacks.
//!
//! Replicas are modelled as the set of `(table, key)` they hold: what the engine
//! sends is applied to it, what it takes back removed. What each replica should
//! hold is computed here from the rows directly, independently of the engine.

use super::*;
use crate::row::{Column, Mirror, Wire};
use crate::testkit::Walk;

pub(super) const SCHEMA: &str = "
CREATE SCHEMA fixture;
CREATE TABLE fixture.t_replicas (
    id UUID PRIMARY KEY, partition_id UUID NOT NULL, live BOOLEAN NOT NULL,
    retired_at TIMESTAMPTZ);
CREATE TABLE fixture.t_kinds (
    id UUID PRIMARY KEY, partition_id UUID NOT NULL, name TEXT NOT NULL,
    retired_at TIMESTAMPTZ);
CREATE TABLE fixture.t_products (
    id UUID PRIMARY KEY, partition_id UUID NOT NULL, kind_id UUID REFERENCES fixture.t_kinds (id),
    name TEXT NOT NULL, retired_at TIMESTAMPTZ);
CREATE TABLE fixture.t_customers (
    id UUID PRIMARY KEY, partition_id UUID NOT NULL, name TEXT NOT NULL,
    retired_at TIMESTAMPTZ);
CREATE TABLE fixture.t_lists (id UUID PRIMARY KEY, partition_id UUID NOT NULL);
CREATE TABLE fixture.t_list_members (
    list_id UUID NOT NULL REFERENCES fixture.t_lists (id),
    customer_id UUID NOT NULL REFERENCES fixture.t_customers (id),
    partition_id UUID NOT NULL, PRIMARY KEY (list_id, customer_id));
CREATE TABLE fixture.t_replica_lists (
    list_id UUID NOT NULL REFERENCES fixture.t_lists (id),
    replica_id UUID NOT NULL REFERENCES fixture.t_replicas (id),
    partition_id UUID NOT NULL, PRIMARY KEY (list_id, replica_id));
CREATE TABLE fixture.t_emails (
    id UUID PRIMARY KEY, partition_id UUID NOT NULL,
    customer_id UUID NOT NULL REFERENCES fixture.t_customers (id), address TEXT NOT NULL,
    retired_at TIMESTAMPTZ);
CREATE TABLE fixture.t_phones (
    customer_id UUID NOT NULL REFERENCES fixture.t_customers (id), number INTEGER NOT NULL,
    partition_id UUID NOT NULL, kind_id UUID REFERENCES fixture.t_kinds (id),
    retired_at TIMESTAMPTZ, PRIMARY KEY (customer_id, number));
CREATE TABLE fixture.t_orders (
    id UUID PRIMARY KEY, partition_id UUID NOT NULL,
    replica_id UUID REFERENCES fixture.t_replicas (id),
    customer_id UUID REFERENCES fixture.t_customers (id), retired_at TIMESTAMPTZ);
CREATE VIEW t_replicas AS SELECT * FROM fixture.t_replicas WHERE retired_at IS NULL;
CREATE VIEW t_kinds AS SELECT * FROM fixture.t_kinds WHERE retired_at IS NULL;
CREATE VIEW t_products AS SELECT * FROM fixture.t_products WHERE retired_at IS NULL;
CREATE VIEW t_customers AS SELECT * FROM fixture.t_customers WHERE retired_at IS NULL;
CREATE VIEW t_emails AS SELECT * FROM fixture.t_emails WHERE retired_at IS NULL;
CREATE VIEW t_orders AS SELECT * FROM fixture.t_orders WHERE retired_at IS NULL;
CREATE VIEW t_phones AS SELECT * FROM fixture.t_phones WHERE retired_at IS NULL;
CREATE VIEW t_list_members AS SELECT * FROM fixture.t_list_members;
CREATE VIEW t_replica_lists AS SELECT * FROM fixture.t_replica_lists;
SELECT sync.mirror_table('fixture', 't_kinds', '{}', 'partition_id');
SELECT sync.mirror_table('fixture', 't_products', '{}', 'partition_id');
SELECT sync.mirror_table('fixture', 't_customers', '{}', 'partition_id');
SELECT sync.mirror_table('fixture', 't_emails', '{}', 'partition_id');
SELECT sync.mirror_table('fixture', 't_orders', '{}', 'partition_id');
SELECT sync.mirror_table('fixture', 't_phones', '{}', 'partition_id');
SELECT sync.mirror_group('fixture', 't_list_members', 'list_id', 'members', 'partition_id');
SELECT sync.mirror_group('fixture', 't_replica_lists', 'replica_id', 'assigned', 'partition_id');
";

fn table(name: &'static str, sync: Sync) -> Table {
    Table {
        name,
        schema: "fixture",
        message: "",
        rust_path: "",
        key: &["id"],
        mirror: Mirror::Catalog,
        sync,
        active: Some(name),
        retired: Some("retired_at"),
        origin: None,
        version: None,
        columns: vec![Column {
            name: "id",
            field: "id",
            nullable: false,
            generated: false,
            replica_owned: false,
            wire: Wire::String,
            compatible: |_| true,
        }],
    }
}

/// A follower keyed by its parent and a number, pointing at a kind too.
fn phones() -> Table {
    let column = |name: &'static str, wire: Wire| Column {
        name,
        field: name,
        nullable: false,
        generated: false,
        replica_owned: false,
        wire,
        compatible: |_| true,
    };
    Table {
        key: &["customer_id", "number"],
        columns: vec![
            column("customer_id", Wire::String),
            column("number", Wire::Int32),
        ],
        ..table("t_phones", Sync::Follows("customer_id"))
    }
}

pub(super) fn tables() -> Vec<Table> {
    vec![
        phones(),
        table("t_orders", Sync::Assigned("replica_id")),
        table("t_emails", Sync::Follows("customer_id")),
        table(
            "t_customers",
            Sync::Grouped {
                members: "t_list_members",
                assigned: "t_replica_lists",
            },
        ),
        table("t_products", Sync::Broadcast),
        table("t_kinds", Sync::Referenced),
    ]
}

pub(super) const REPLICAS: Replicas = Replicas {
    relation: "t_replicas",
    table: "t_replicas",
    live: "live",
    partition: Some("partition_id"),
};

/// What each replica holds, as the engine told it.
type Holdings = BTreeMap<Uuid, BTreeSet<(&'static str, String)>>;

/// Drains the outbox through the engine into the modelled replicas.
async fn sync(conn: &mut Conn, engine: &Engine, holdings: &mut Holdings) -> Vec<Delta> {
    let mut deltas = Vec::new();
    loop {
        let pending = pending(conn, 1000).await.expect("pending");
        if pending.is_empty() {
            return deltas;
        }
        let (ids, changes): (Vec<i64>, Vec<Change>) =
            pending.into_iter().map(|n| (n.id, n.change)).unzip();
        let delta = engine.plan(conn, &changes).await.expect("plan");
        for replica in Engine::replicas(&delta) {
            let held = holdings.entry(replica).or_default();
            if delta.snapshots.contains(&replica) {
                held.clear();
            }
            for (i, keys) in delta.upserts.get(&replica).into_iter().flatten() {
                for key in keys {
                    held.insert((engine.table(*i), key.clone()));
                }
            }
            for (i, keys) in delta.deletes.get(&replica).into_iter().flatten() {
                for key in keys {
                    held.remove(&(engine.table(*i), key.clone()));
                }
            }
        }
        engine.record(conn, &delta).await.expect("record");
        mark_done(conn, &ids).await.expect("mark");
        deltas.push(delta);
    }
}

async fn ids(conn: &mut Conn, sql: &str, bind: Uuid) -> BTreeSet<String> {
    sqlx::query_scalar::<_, Uuid>(sql)
        .bind(bind)
        .fetch_all(&mut *conn)
        .await
        .expect(sql)
        .into_iter()
        .map(|id| id.to_string())
        .collect()
}

/// What `replica` should hold, read off the rows.
async fn expected(conn: &mut Conn, replica: Uuid) -> BTreeSet<(&'static str, String)> {
    let live: bool = sqlx::query_scalar("SELECT live FROM fixture.t_replicas WHERE id = $1")
        .bind(replica)
        .fetch_one(&mut *conn)
        .await
        .expect("replica");
    if !live {
        return BTreeSet::new();
    }
    let products = ids(
        conn,
        "SELECT p.id FROM fixture.t_products p JOIN fixture.t_replicas e ON e.partition_id = p.partition_id \
         WHERE e.id = $1 AND p.retired_at IS NULL",
        replica,
    )
    .await;
    let listed = ids(
        conn,
        "SELECT DISTINCT c.id FROM fixture.t_customers c \
         JOIN fixture.t_list_members m ON m.customer_id = c.id \
         JOIN fixture.t_replica_lists a ON a.list_id = m.list_id \
         WHERE a.replica_id = $1 AND c.retired_at IS NULL",
        replica,
    )
    .await;
    let orders = ids(
        conn,
        "SELECT id FROM fixture.t_orders WHERE replica_id = $1 AND retired_at IS NULL",
        replica,
    )
    .await;
    // What those point at comes with them.
    let kinds = ids(
        conn,
        "SELECT DISTINCT p.kind_id FROM fixture.t_products p JOIN fixture.t_replicas e ON e.partition_id = p.partition_id \
         WHERE e.id = $1 AND p.retired_at IS NULL AND p.kind_id IS NOT NULL",
        replica,
    )
    .await;
    let ordered_customers = ids(
        conn,
        "SELECT DISTINCT customer_id FROM fixture.t_orders \
         WHERE replica_id = $1 AND retired_at IS NULL AND customer_id IS NOT NULL",
        replica,
    )
    .await;
    // A customer's emails go wherever the customer goes.
    let customers = &listed | &ordered_customers;
    let emails: BTreeSet<String> = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM fixture.t_emails WHERE retired_at IS NULL AND customer_id::text = ANY($1)",
    )
    .bind(customers.iter().cloned().collect::<Vec<_>>())
    .fetch_all(&mut *conn)
    .await
    .expect("emails")
    .into_iter()
    .map(|id| id.to_string())
    .collect();
    // A customer's phones too, and the kind a phone points at.
    let phone_rows: Vec<(Uuid, i32, Option<Uuid>)> = sqlx::query_as(
        "SELECT customer_id, number, kind_id FROM fixture.t_phones \
          WHERE retired_at IS NULL AND customer_id::text = ANY($1)",
    )
    .bind(customers.iter().cloned().collect::<Vec<_>>())
    .fetch_all(&mut *conn)
    .await
    .expect("phones");
    let phones: BTreeSet<String> = phone_rows
        .iter()
        .map(|(c, n, _)| format!("{c}|{n}"))
        .collect();
    let kinds = &kinds
        | &phone_rows
            .iter()
            .filter_map(|(_, _, u)| u.map(|u| u.to_string()))
            .collect::<BTreeSet<_>>();
    let mut out = BTreeSet::new();
    for (table, keys) in [
        ("t_products", products),
        ("t_customers", customers),
        ("t_emails", emails),
        ("t_orders", orders),
        ("t_kinds", kinds),
        ("t_phones", phones),
    ] {
        out.extend(keys.into_iter().map(|k| (table, k)));
    }
    out
}

async fn all(conn: &mut Conn, table: &str, partition: Uuid) -> Vec<Uuid> {
    sqlx::query_scalar(&format!(
        "SELECT id FROM fixture.{table} WHERE partition_id = $1 ORDER BY id"
    ))
    .bind(partition)
    .fetch_all(&mut *conn)
    .await
    .expect(table)
}

async fn exec(conn: &mut Conn, sql: &str, binds: &[Uuid]) {
    let mut query = sqlx::query(sql);
    for bind in binds {
        query = query.bind(*bind);
    }
    query.execute(&mut *conn).await.expect(sql);
}

/// One office write, picked at random among those the schema allows.
async fn step(conn: &mut Conn, walk: &mut Walk, partition: Uuid) -> String {
    let replicas = all(conn, "t_replicas", partition).await;
    let kinds = all(conn, "t_kinds", partition).await;
    let products = all(conn, "t_products", partition).await;
    let customers = all(conn, "t_customers", partition).await;
    let lists = all(conn, "t_lists", partition).await;
    let emails = all(conn, "t_emails", partition).await;
    let orders = all(conn, "t_orders", partition).await;
    let phones: Vec<(Uuid, i32)> = sqlx::query_as(
        "SELECT customer_id, number FROM fixture.t_phones WHERE partition_id = $1 ORDER BY 1, 2",
    )
    .bind(partition)
    .fetch_all(&mut *conn)
    .await
    .expect("phones");
    let phone = (!phones.is_empty()).then(|| phones[walk.below(phones.len())]);
    let new = Uuid::now_v7();
    let choice = walk.below(30);
    let (sql, binds): (&str, Vec<Uuid>) = match choice {
        0 => (
            "INSERT INTO fixture.t_kinds (id, partition_id, name) VALUES ($1, $2, 'u')",
            vec![new, partition],
        ),
        1 => match walk.pick(&kinds) {
            Some(kind) => (
                "INSERT INTO fixture.t_products (id, partition_id, kind_id, name) VALUES ($1, $2, $3, 'p')",
                vec![new, partition, kind],
            ),
            None => ("INSERT INTO fixture.t_products (id, partition_id, name) VALUES ($1, $2, 'p')", vec![new, partition]),
        },
        2 => match (walk.pick(&products), walk.pick(&kinds)) {
            (Some(p), Some(u)) => ("UPDATE fixture.t_products SET kind_id = $2 WHERE id = $1", vec![p, u]),
            _ => return "skip".into(),
        },
        3 => match walk.pick(&products) {
            Some(p) => (
                "UPDATE fixture.t_products SET retired_at = CASE WHEN retired_at IS NULL THEN now() END WHERE id = $1",
                vec![p],
            ),
            None => return "skip".into(),
        },
        4 => match walk.pick(&products) {
            Some(p) => ("UPDATE fixture.t_products SET name = name || '.' WHERE id = $1", vec![p]),
            None => return "skip".into(),
        },
        5 => match walk.pick(&kinds) {
            Some(u) => (
                "UPDATE fixture.t_kinds SET retired_at = CASE WHEN retired_at IS NULL THEN now() END WHERE id = $1",
                vec![u],
            ),
            None => return "skip".into(),
        },
        6 => (
            "INSERT INTO fixture.t_customers (id, partition_id, name) VALUES ($1, $2, 'c')",
            vec![new, partition],
        ),
        7 => match walk.pick(&customers) {
            Some(c) => (
                "UPDATE fixture.t_customers SET retired_at = CASE WHEN retired_at IS NULL THEN now() END WHERE id = $1",
                vec![c],
            ),
            None => return "skip".into(),
        },
        8 => ("INSERT INTO fixture.t_lists (id, partition_id) VALUES ($1, $2)", vec![new, partition]),
        9 | 10 => match (walk.pick(&lists), walk.pick(&customers)) {
            (Some(l), Some(c)) => (
                "INSERT INTO fixture.t_list_members (list_id, customer_id, partition_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                vec![l, c, partition],
            ),
            _ => return "skip".into(),
        },
        11 => match (walk.pick(&lists), walk.pick(&customers)) {
            (Some(l), Some(c)) => (
                "DELETE FROM fixture.t_list_members WHERE list_id = $1 AND customer_id = $2",
                vec![l, c],
            ),
            _ => return "skip".into(),
        },
        12 => match (walk.pick(&lists), walk.pick(&replicas)) {
            (Some(l), Some(e)) => (
                "INSERT INTO fixture.t_replica_lists (list_id, replica_id, partition_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                vec![l, e, partition],
            ),
            _ => return "skip".into(),
        },
        13 => match (walk.pick(&lists), walk.pick(&replicas)) {
            (Some(l), Some(e)) => (
                "DELETE FROM fixture.t_replica_lists WHERE list_id = $1 AND replica_id = $2",
                vec![l, e],
            ),
            _ => return "skip".into(),
        },
        14 => match walk.pick(&customers) {
            Some(c) => (
                "INSERT INTO fixture.t_emails (id, partition_id, customer_id, address) VALUES ($1, $2, $3, 'e')",
                vec![new, partition, c],
            ),
            None => return "skip".into(),
        },
        15 => match (walk.pick(&emails), walk.pick(&customers)) {
            (Some(m), Some(c)) => ("UPDATE fixture.t_emails SET customer_id = $2 WHERE id = $1", vec![m, c]),
            _ => return "skip".into(),
        },
        16 => match (walk.pick(&replicas), walk.pick(&customers)) {
            (Some(e), Some(c)) => (
                "INSERT INTO fixture.t_orders (id, partition_id, replica_id, customer_id) VALUES ($1, $2, $3, $4)",
                vec![new, partition, e, c],
            ),
            _ => return "skip".into(),
        },
        17 => match (walk.pick(&orders), walk.pick(&replicas)) {
            (Some(o), Some(e)) => ("UPDATE fixture.t_orders SET replica_id = $2 WHERE id = $1", vec![o, e]),
            _ => return "skip".into(),
        },
        18 => match walk.pick(&orders) {
            Some(o) => (
                "UPDATE fixture.t_orders SET retired_at = CASE WHEN retired_at IS NULL THEN now() END WHERE id = $1",
                vec![o],
            ),
            None => return "skip".into(),
        },
        19 => match walk.pick(&orders) {
            Some(o) => ("DELETE FROM fixture.t_orders WHERE id = $1", vec![o]),
            None => return "skip".into(),
        },
        20 => match walk.pick(&emails) {
            Some(m) => ("DELETE FROM fixture.t_emails WHERE id = $1", vec![m]),
            None => return "skip".into(),
        },
        21 => match walk.pick(&emails) {
            Some(m) => (
                "UPDATE fixture.t_emails SET retired_at = CASE WHEN retired_at IS NULL THEN now() END WHERE id = $1",
                vec![m],
            ),
            None => return "skip".into(),
        },
        22 => match walk.pick(&products) {
            Some(p) => ("DELETE FROM fixture.t_products WHERE id = $1", vec![p]),
            None => return "skip".into(),
        },
        23 => match walk.pick(&products) {
            Some(p) => ("UPDATE fixture.t_products SET kind_id = NULL WHERE id = $1", vec![p]),
            None => return "skip".into(),
        },
        24 => match walk.pick(&kinds) {
            Some(u) => (
                "DELETE FROM fixture.t_kinds WHERE id = $1 \
                   AND NOT EXISTS (SELECT 1 FROM fixture.t_products WHERE kind_id = $1) \
                   AND NOT EXISTS (SELECT 1 FROM fixture.t_phones WHERE kind_id = $1)",
                vec![u],
            ),
            None => return "skip".into(),
        },
        25 => match (walk.pick(&customers), walk.pick(&kinds)) {
            (Some(c), kind) => {
                let number = walk.below(3) as i32;
                sqlx::query(
                    "INSERT INTO fixture.t_phones (customer_id, number, partition_id, kind_id) \
                     VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
                )
                .bind(c)
                .bind(number)
                .bind(partition)
                .bind(kind)
                .execute(&mut *conn)
                .await
                .expect("a phone");
                return format!("{choice}: phone {c}|{number}");
            }
            _ => return "skip".into(),
        },
        26..=28 => match phone {
            Some((c, n)) => {
                let (sql, kind) = match choice {
                    26 => (
                        "UPDATE fixture.t_phones SET retired_at = CASE WHEN retired_at IS NULL THEN now() END \
                          WHERE customer_id = $1 AND number = $2",
                        None,
                    ),
                    27 => (
                        "UPDATE fixture.t_phones SET kind_id = $3 WHERE customer_id = $1 AND number = $2",
                        walk.pick(&kinds),
                    ),
                    _ => ("DELETE FROM fixture.t_phones WHERE customer_id = $1 AND number = $2", None),
                };
                let mut query = sqlx::query(sql).bind(c).bind(n);
                if choice == 27 {
                    query = query.bind(kind);
                }
                query.execute(&mut *conn).await.expect(sql);
                return format!("{choice}: {sql} {c}|{n}");
            }
            None => return "skip".into(),
        },
        _ => match walk.pick(&replicas) {
            Some(e) => (
                "INSERT INTO sync.notes (partition_id, kind, replica_id) VALUES ($2, 'resync', $1)",
                vec![e, partition],
            ),
            None => return "skip".into(),
        },
    };
    exec(conn, sql, &binds).await;
    format!("{choice}: {sql}")
}

/// Every row a replica holds has the rows it points at.
async fn assert_closed(conn: &mut Conn, holdings: &Holdings) {
    for (replica, held) in holdings {
        for (child, parent, sql) in [
            (
                "t_products",
                "t_kinds",
                "SELECT kind_id FROM fixture.t_products WHERE id = $1",
            ),
            (
                "t_orders",
                "t_customers",
                "SELECT customer_id FROM fixture.t_orders WHERE id = $1",
            ),
            (
                "t_emails",
                "t_customers",
                "SELECT customer_id FROM fixture.t_emails WHERE id = $1",
            ),
        ] {
            for (table, key) in held.iter().filter(|(t, _)| *t == child) {
                let parent_key: Option<Option<Uuid>> = sqlx::query_scalar(sql)
                    .bind(Uuid::parse_str(key).expect("a uuid"))
                    .fetch_optional(&mut *conn)
                    .await
                    .expect(sql);
                if let Some(Some(parent_key)) = parent_key {
                    assert!(
                        held.contains(&(parent, parent_key.to_string())),
                        "{replica} holds {table} {key} without its {parent} {parent_key}"
                    );
                }
            }
        }
    }
}

/// Every phone a replica holds has its customer and its kind.
async fn assert_phones_closed(conn: &mut Conn, holdings: &Holdings) {
    for (replica, held) in holdings {
        for (_, key) in held.iter().filter(|(t, _)| *t == "t_phones") {
            let (customer, number) = key.split_once('|').expect("a composite key");
            let kind: Option<Option<Uuid>> = sqlx::query_scalar(
                "SELECT kind_id FROM fixture.t_phones WHERE customer_id = $1::uuid AND number = $2::int",
            )
            .bind(customer)
            .bind(number)
            .fetch_optional(&mut *conn)
            .await
            .expect("the phone");
            assert!(
                held.contains(&("t_customers", customer.to_owned())),
                "{replica} holds phone {key} without its customer"
            );
            if let Some(Some(kind)) = kind {
                assert!(
                    held.contains(&("t_kinds", kind.to_string())),
                    "{replica} holds phone {key} without its kind {kind}"
                );
            }
        }
    }
}

/// Two partitions: three replicas for the first — the last not live — and
/// two for the second.
async fn setup(pool: &crate::DbPool) -> (sqlx::pool::PoolConnection<crate::Db>, Engine, Vec<Uuid>) {
    let mut conn = pool.acquire().await.expect("a connection");
    super::tests::install(&mut conn).await;
    sqlx::raw_sql(SCHEMA)
        .execute(&mut *conn)
        .await
        .expect("schema");
    let partitions = vec![Uuid::now_v7(), Uuid::now_v7()];
    for (partition, live) in [
        (partitions[0], true),
        (partitions[0], true),
        (partitions[0], false),
        (partitions[1], true),
        (partitions[1], true),
    ] {
        sqlx::query("INSERT INTO fixture.t_replicas (id, partition_id, live) VALUES ($1, $2, $3)")
            .bind(Uuid::now_v7())
            .bind(partition)
            .bind(live)
            .execute(&mut *conn)
            .await
            .expect("a replica");
    }
    let tables = tables();
    let refs: Vec<&Table> = tables.iter().collect();
    let engine = Engine::new(&mut conn, &refs, REPLICAS)
        .await
        .expect("engine");
    (conn, engine, partitions)
}

#[sqlx::test(migrations = false)]
async fn every_replica_holds_exactly_what_its_strategies_say(pool: crate::DbPool) {
    let (mut conn, engine, partitions) = setup(&pool).await;
    for seed in SEEDS {
        walk_from(&mut conn, &engine, &partitions, seed).await;
    }
}

/// Seeds the walk replays; a failure names the one it came from.
const SEEDS: [u64; 8] = [
    0x9E37_79B9_7F4A_7C15,
    0xD1B5_4A32_D192_ED03,
    0x2545_F491_4F6C_DD1D,
    0x94D0_49BB_1331_11EB,
    0xBF58_476D_1CE4_E5B9,
    0x0123_4567_89AB_CDEF,
    0xFEDC_BA98_7654_3210,
    0x1357_9BDF_2468_ACE0,
];

async fn walk_from(conn: &mut Conn, engine: &Engine, partitions: &[Uuid], seed: u64) {
    // What the previous walk left behind: every replica starts from a snapshot.
    let replicas_now: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM fixture.t_replicas")
        .fetch_all(&mut *conn)
        .await
        .expect("replicas");
    for replica in &replicas_now {
        sqlx::query("INSERT INTO sync.notes (partition_id, kind, replica_id) SELECT partition_id, 'resync', id FROM fixture.t_replicas WHERE id = $1")
            .bind(replica)
            .execute(&mut *conn)
            .await
            .expect("resync");
    }
    let mut holdings = Holdings::new();
    sync(conn, engine, &mut holdings).await;
    let mut walk = Walk(seed);
    let replicas = replicas_now;
    let mut history = Vec::new();
    for n in 0..400 {
        let partition = partitions[walk.below(partitions.len())];
        history.push(step(conn, &mut walk, partition).await);
        if walk.below(3) == 0 || n == 399 {
            sync(conn, engine, &mut holdings).await;
            for replica in &replicas {
                let held = holdings.get(replica).cloned().unwrap_or_default();
                let should = expected(conn, *replica).await;
                assert!(
                    held == should,
                    "seed {seed:#x}, after step {n}, {replica}:\n  holds   {:?}\n  missing {:?}\n  extra   {:?}\n  last steps: {:#?}",
                    held.len(),
                    should.difference(&held).collect::<Vec<_>>(),
                    held.difference(&should).collect::<Vec<_>>(),
                    &history[history.len().saturating_sub(6)..],
                );
            }
            assert_closed(conn, &holdings).await;
            assert_phones_closed(conn, &holdings).await;
        }
    }
}

#[sqlx::test(migrations = false)]
async fn a_changed_row_goes_again_to_every_replica_holding_it(pool: crate::DbPool) {
    let (mut conn, engine, partitions) = setup(&pool).await;
    let product = Uuid::now_v7();
    exec(
        &mut conn,
        "INSERT INTO fixture.t_products (id, partition_id, name) VALUES ($1, $2, 'p')",
        &[product, partitions[1]],
    )
    .await;
    let mut holdings = Holdings::new();
    sync(&mut conn, &engine, &mut holdings).await;
    exec(
        &mut conn,
        "UPDATE fixture.t_products SET name = 'q' WHERE id = $1",
        &[product],
    )
    .await;
    let deltas = sync(&mut conn, &engine, &mut holdings).await;
    let sent: usize = deltas
        .iter()
        .flat_map(|d| d.upserts.values())
        .flat_map(|t| t.values())
        .filter(|keys| keys.contains(&product.to_string()))
        .count();
    assert_eq!(
        sent, 2,
        "both live holdings of the partition get the new name"
    );
}
