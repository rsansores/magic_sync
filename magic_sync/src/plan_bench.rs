//! What the engine costs on the operations that move many rows at once, on the
//! schema the engine's tests use. Ignored by default; `just bench-mirror` runs
//! them and prints the timings. Each measures the database work of planning
//! and recording — what the relay does besides encoding and publishing.

use super::plan_tests::{tables, REPLICAS, SCHEMA};
use super::*;
use std::time::{Duration, Instant};

/// Notes the relay drains per plan (`BATCH` in the server's relay).
const BATCH: i64 = 500;

struct Drained {
    notes: usize,
    upserts: usize,
    deletes: usize,
    plan: Duration,
    record: Duration,
}

impl std::fmt::Display for Drained {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} notes → {} upserts, {} deletes; plan {:?}, record {:?}, total {:?}",
            self.notes,
            self.upserts,
            self.deletes,
            self.plan,
            self.record,
            self.plan + self.record
        )
    }
}

/// Drains the outbox as the relay would, timing the plans and the records.
async fn drain(conn: &mut Conn, engine: &Engine) -> Drained {
    let mut out = Drained {
        notes: 0,
        upserts: 0,
        deletes: 0,
        plan: Duration::ZERO,
        record: Duration::ZERO,
    };
    loop {
        let noted = pending(conn, BATCH).await.expect("pending");
        if noted.is_empty() {
            return out;
        }
        let (ids, changes): (Vec<i64>, Vec<Change>) =
            noted.into_iter().map(|n| (n.id, n.change)).unzip();
        out.notes += ids.len();
        let started = Instant::now();
        let delta = engine.plan(conn, &changes).await.expect("plan");
        out.plan += started.elapsed();
        let count = |m: &BTreeMap<Uuid, BTreeMap<usize, BTreeSet<String>>>| -> usize {
            m.values().flat_map(|t| t.values()).map(BTreeSet::len).sum()
        };
        out.upserts += count(&delta.upserts);
        out.deletes += count(&delta.deletes);
        let started = Instant::now();
        engine.record(conn, &delta).await.expect("record");
        mark_done(conn, &ids).await.expect("mark");
        out.record += started.elapsed();
    }
}

/// A partition with `count` live replicas, and the engine.
async fn fleet(
    pool: &crate::DbPool,
    count: usize,
) -> (
    sqlx::pool::PoolConnection<crate::Db>,
    Engine,
    Uuid,
    Vec<Uuid>,
) {
    let mut conn = pool.acquire().await.expect("a connection");
    super::tests::install(&mut conn).await;
    sqlx::raw_sql(SCHEMA)
        .execute(&mut *conn)
        .await
        .expect("schema");
    let partition = Uuid::now_v7();
    let mut replicas = Vec::with_capacity(count);
    for _ in 0..count {
        let id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO fixture.t_replicas (id, partition_id, live) VALUES ($1, $2, true)",
        )
        .bind(id)
        .bind(partition)
        .execute(&mut *conn)
        .await
        .expect("a replica");
        replicas.push(id);
    }
    let tables = tables();
    let refs: Vec<&Table> = tables.iter().collect();
    let engine = Engine::new(&mut conn, &refs, REPLICAS)
        .await
        .expect("engine");
    (conn, engine, partition, replicas)
}

async fn exec(conn: &mut Conn, sql: &str, binds: &[Uuid]) {
    let mut query = sqlx::query(sql);
    for bind in binds {
        query = query.bind(*bind);
    }
    query.execute(&mut *conn).await.expect(sql);
}

/// A list of `size` new customers, each with an email.
async fn list_of(conn: &mut Conn, partition: Uuid, size: i64) -> Uuid {
    let list = Uuid::now_v7();
    exec(
        conn,
        "INSERT INTO fixture.t_lists (id, partition_id) VALUES ($1, $2)",
        &[list, partition],
    )
    .await;
    sqlx::query(
        "WITH made AS (
             INSERT INTO fixture.t_customers (id, partition_id, name)
             SELECT uuidv7(), $1, 'c' || n FROM generate_series(1, $3) n
             RETURNING id)
         , mailed AS (
             INSERT INTO fixture.t_emails (id, partition_id, customer_id, address)
             SELECT uuidv7(), $1, id, 'x@y' FROM made)
         INSERT INTO fixture.t_list_members (list_id, customer_id, partition_id)
         SELECT $2, id, $1 FROM made",
    )
    .bind(partition)
    .bind(list)
    .bind(size)
    .execute(&mut *conn)
    .await
    .expect("a list");
    list
}

async fn assign(conn: &mut Conn, partition: Uuid, list: Uuid, replica: Uuid) {
    exec(
        conn,
        "INSERT INTO fixture.t_replica_lists (list_id, replica_id, partition_id) VALUES ($1, $2, $3)",
        &[list, replica, partition],
    )
    .await;
}

#[sqlx::test(migrations = false)]
#[ignore = "a benchmark: just bench-mirror"]
async fn bench_a_list_of_20k_given_to_a_unit(pool: crate::DbPool) {
    let (mut conn, engine, partition, replicas) = fleet(&pool, 1).await;
    let list = list_of(&mut conn, partition, 20_000).await;
    drain(&mut conn, &engine).await;

    assign(&mut conn, partition, list, replicas[0]).await;
    let drained = drain(&mut conn, &engine).await;
    println!("20k-customer list given to a replica: {drained}");
    assert_eq!(drained.upserts, 40_000, "each customer and its email");
}

#[sqlx::test(migrations = false)]
#[ignore = "a benchmark: just bench-mirror"]
async fn bench_a_list_of_10k_swapped_for_one_of_8k(pool: crate::DbPool) {
    let (mut conn, engine, partition, replicas) = fleet(&pool, 1).await;
    let old = list_of(&mut conn, partition, 10_000).await;
    let new = list_of(&mut conn, partition, 8_000).await;
    assign(&mut conn, partition, old, replicas[0]).await;
    drain(&mut conn, &engine).await;

    exec(
        &mut conn,
        "DELETE FROM fixture.t_replica_lists WHERE list_id = $1",
        &[old],
    )
    .await;
    assign(&mut conn, partition, new, replicas[0]).await;
    let drained = drain(&mut conn, &engine).await;
    println!("10k-customer list swapped for an 8k one: {drained}");
    assert_eq!((drained.upserts, drained.deletes), (16_000, 20_000));
}

#[sqlx::test(migrations = false)]
#[ignore = "a benchmark: just bench-mirror"]
async fn bench_10k_products_imported_for_100_units(pool: crate::DbPool) {
    let (mut conn, engine, partition, _) = fleet(&pool, 100).await;
    sqlx::query(
        "INSERT INTO fixture.t_products (id, partition_id, name)
         SELECT uuidv7(), $1, 'p' || n FROM generate_series(1, 10000) n",
    )
    .bind(partition)
    .execute(&mut *conn)
    .await
    .expect("an import");
    let drained = drain(&mut conn, &engine).await;
    println!("10k products imported, 100 replicas: {drained}");
    assert_eq!(drained.upserts, 1_000_000);
}

#[sqlx::test(migrations = false)]
#[ignore = "a benchmark: just bench-mirror"]
async fn bench_a_replica_provisioned_with_30k_rows(pool: crate::DbPool) {
    let (mut conn, engine, partition, replicas) = fleet(&pool, 1).await;
    let list = list_of(&mut conn, partition, 10_000).await;
    assign(&mut conn, partition, list, replicas[0]).await;
    sqlx::query(
        "INSERT INTO fixture.t_products (id, partition_id, name)
         SELECT uuidv7(), $1, 'p' || n FROM generate_series(1, 10000) n",
    )
    .bind(partition)
    .execute(&mut *conn)
    .await
    .expect("a catalog");
    drain(&mut conn, &engine).await;

    note_resync(&mut conn, Some(partition), replicas[0])
        .await
        .expect("resync");
    let drained = drain(&mut conn, &engine).await;
    println!("a replica provisioned from scratch: {drained}");
    assert_eq!(drained.upserts, 30_000);
}

#[sqlx::test(migrations = false)]
#[ignore = "a benchmark: just bench-mirror"]
async fn bench_10k_customers_imported_into_a_list_10_units_hold(pool: crate::DbPool) {
    let (mut conn, engine, partition, replicas) = fleet(&pool, 10).await;
    let list = Uuid::now_v7();
    exec(
        &mut conn,
        "INSERT INTO fixture.t_lists (id, partition_id) VALUES ($1, $2)",
        &[list, partition],
    )
    .await;
    for replica in &replicas {
        assign(&mut conn, partition, list, *replica).await;
    }
    drain(&mut conn, &engine).await;

    sqlx::query(
        "WITH made AS (
             INSERT INTO fixture.t_customers (id, partition_id, name)
             SELECT uuidv7(), $1, 'c' || n FROM generate_series(1, 10000) n
             RETURNING id)
         INSERT INTO fixture.t_list_members (list_id, customer_id, partition_id)
         SELECT $2, id, $1 FROM made",
    )
    .bind(partition)
    .bind(list)
    .execute(&mut *conn)
    .await
    .expect("an import");
    let drained = drain(&mut conn, &engine).await;
    println!("10k customers imported into a list 10 replicas hold: {drained}");
    assert_eq!(drained.upserts, 100_000);
}

#[sqlx::test(migrations = false)]
#[ignore = "a benchmark: just bench-mirror"]
async fn bench_1k_products_imported_for_4000_units(pool: crate::DbPool) {
    let (mut conn, engine, partition, _) = fleet(&pool, 4000).await;
    sqlx::query(
        "INSERT INTO fixture.t_products (id, partition_id, name)
         SELECT uuidv7(), $1, 'p' || n FROM generate_series(1, 1000) n",
    )
    .bind(partition)
    .execute(&mut *conn)
    .await
    .expect("an import");
    let drained = drain(&mut conn, &engine).await;
    println!("1k products imported, 4000 replicas: {drained}");
    assert_eq!(drained.upserts, 4_000_000);
}
