//! What the engine costs on the operations that move many rows at once, on the
//! schema the engine's tests use. Ignored by default; the command in
//! CONTRIBUTING.md runs them and prints the timings. Most measure the database
//! work of planning and recording — what the relay does besides encoding and
//! publishing — and one what a dashboard's reads cost.

use super::plan_tests::{codecs, REPLICAS, SCHEMA};
use super::*;
use sqlx::Connection as _;
use std::time::{Duration, Instant};

/// Notes the relay drains per plan (`BATCH` in the server's relay).
const BATCH: i64 = 500;

struct Drained {
    notes: usize,
    upserts: usize,
    deletes: usize,
    took: Duration,
}

impl std::fmt::Display for Drained {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} notes → {} upserts, {} deletes; planned, loaded and recorded in {:?}",
            self.notes, self.upserts, self.deletes, self.took
        )
    }
}

/// Drains the notes as the relay would — planned, loaded, recorded and
/// written to the outboxes, never sent — timing it.
async fn drain(pool: &crate::DbPool, engine: &Engine) -> Drained {
    let codecs = codecs();
    let relay = crate::relay::Relay {
        engine,
        codecs: vec![&codecs],
        limits: crate::envelope::Limits {
            rows: 500,
            bytes: 512 * 1024,
        },
        batch: BATCH,
        timeout: Duration::from_secs(30),
        backoff: crate::run::Backoff::new(Duration::from_secs(1), Duration::from_secs(60)),
        bound: crate::relay::Bound {
            messages: i64::MAX,
            bytes: i64::MAX,
        },
        concurrency: 4,
        buffered: crate::relay::BUFFERED,
    };
    let mut out = Drained {
        notes: 0,
        upserts: 0,
        deletes: 0,
        took: Duration::ZERO,
    };
    loop {
        let started = Instant::now();
        let mut tx = pool.begin().await.expect("a transaction");
        engine.observe(&mut tx).await.expect("observed");
        tx.commit().await.expect("commit");
        let mut tx = pool
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ")
            .await
            .expect("a transaction");
        let outcome = relay.record(&mut tx).await.expect("recorded");
        tx.commit().await.expect("commit");
        out.took += started.elapsed();
        if outcome.done == 0 {
            return out;
        }
        out.notes += outcome.done;
        out.upserts += outcome.upserts;
        out.deletes += outcome.deletes;
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
    let codecs = codecs();
    let refs: Vec<&Table> = codecs.iter().map(|c| &c.table).collect();
    let mut tx = conn.begin().await.expect("a transaction");
    let engine = Engine::install_one(&mut tx, &refs, REPLICAS)
        .await
        .expect("engine");
    tx.commit().await.expect("installed");
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
#[ignore = "a benchmark: see CONTRIBUTING.md"]
async fn bench_a_list_of_20k_given_to_a_unit(pool: crate::DbPool) {
    let (mut conn, engine, partition, replicas) = fleet(&pool, 1).await;
    let list = list_of(&mut conn, partition, 20_000).await;
    drain(&pool, &engine).await;

    assign(&mut conn, partition, list, replicas[0]).await;
    let drained = drain(&pool, &engine).await;
    println!("20k-customer list given to a replica: {drained}");
    assert_eq!(drained.upserts, 40_000, "each customer and its email");
}

#[sqlx::test(migrations = false)]
#[ignore = "a benchmark: see CONTRIBUTING.md"]
async fn bench_a_list_of_10k_swapped_for_one_of_8k(pool: crate::DbPool) {
    let (mut conn, engine, partition, replicas) = fleet(&pool, 1).await;
    let old = list_of(&mut conn, partition, 10_000).await;
    let new = list_of(&mut conn, partition, 8_000).await;
    assign(&mut conn, partition, old, replicas[0]).await;
    drain(&pool, &engine).await;

    exec(
        &mut conn,
        "DELETE FROM fixture.t_replica_lists WHERE list_id = $1",
        &[old],
    )
    .await;
    assign(&mut conn, partition, new, replicas[0]).await;
    let drained = drain(&pool, &engine).await;
    println!("10k-customer list swapped for an 8k one: {drained}");
    assert_eq!((drained.upserts, drained.deletes), (16_000, 20_000));
}

#[sqlx::test(migrations = false)]
#[ignore = "a benchmark: see CONTRIBUTING.md"]
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
    let drained = drain(&pool, &engine).await;
    println!("10k products imported, 100 replicas: {drained}");
    assert_eq!(drained.upserts, 1_000_000);
}

/// A kind 10k products pointed at, through 20k changes of theirs not yet
/// pruned: what a replica holds at a version older than the row is read off
/// that history, and the kind's own change asks it.
#[sqlx::test(migrations = false)]
#[ignore = "a benchmark: see CONTRIBUTING.md"]
async fn bench_a_kind_changed_under_a_long_history_of_pointers(pool: crate::DbPool) {
    let (mut conn, engine, partition, _) = fleet(&pool, 10).await;
    let kind = Uuid::now_v7();
    exec(
        &mut conn,
        "INSERT INTO fixture.t_kinds (id, partition_id, name) VALUES ($1, $2, 'k')",
        &[kind, partition],
    )
    .await;
    sqlx::query(
        "INSERT INTO fixture.t_products (id, partition_id, kind_id, name)
         SELECT uuidv7(), $1, $2, 'p' || n FROM generate_series(1, 10000) n",
    )
    .bind(partition)
    .bind(kind)
    .execute(&mut *conn)
    .await
    .expect("an import");
    drain(&pool, &engine).await;
    for _ in 0..2 {
        exec(
            &mut conn,
            "UPDATE fixture.t_products SET name = name || '.' WHERE kind_id = $1",
            &[kind],
        )
        .await;
        drain(&pool, &engine).await;
    }

    exec(
        &mut conn,
        "UPDATE fixture.t_kinds SET name = 'k2' WHERE id = $1",
        &[kind],
    )
    .await;
    let drained = drain(&pool, &engine).await;
    println!("a kind changed under 20k changes that pointed at it: {drained}");
    assert_eq!(drained.upserts, 10);
}

#[sqlx::test(migrations = false)]
#[ignore = "a benchmark: see CONTRIBUTING.md"]
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
    drain(&pool, &engine).await;

    note_resync(&mut conn, replicas[0], Some(partition))
        .await
        .expect("resync");
    let drained = drain(&pool, &engine).await;
    println!("a replica provisioned from scratch: {drained}");
    assert_eq!(drained.upserts, 30_000);
}

#[sqlx::test(migrations = false)]
#[ignore = "a benchmark: see CONTRIBUTING.md"]
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
    drain(&pool, &engine).await;

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
    let drained = drain(&pool, &engine).await;
    println!("10k customers imported into a list 10 replicas hold: {drained}");
    assert_eq!(drained.upserts, 100_000);
}

/// A partition of 100 replicas, each at a site of its own, holding the 1k
/// notices a rule grants every site: 100k pairs. One rule revoked re-routes
/// the partition — the view asked again for every replica, against all it
/// holds — and takes back one notice from one replica.
#[sqlx::test(migrations = false)]
#[ignore = "a benchmark: see CONTRIBUTING.md"]
async fn bench_a_partition_of_100_units_holding_1k_notices_rerouted(pool: crate::DbPool) {
    let (mut conn, engine, partition, replicas) = fleet(&pool, 100).await;
    sqlx::raw_sql(
        "INSERT INTO fixture.t_sites (id, partition_id) SELECT id, partition_id FROM fixture.t_replicas;
         INSERT INTO fixture.t_site_replicas (site_id, replica_id, partition_id)
              SELECT id, id, partition_id FROM fixture.t_replicas;",
    )
    .execute(&mut *conn)
    .await
    .expect("sites");
    sqlx::query(
        "WITH made AS (
             INSERT INTO fixture.t_notices (id, partition_id, name)
             SELECT uuidv7(), $1, 'n' || n FROM generate_series(1, 1000) n
             RETURNING id)
         INSERT INTO fixture.t_rules (notice_id, site_id, partition_id, granted)
         SELECT m.id, s.id, $1, true FROM made m CROSS JOIN fixture.t_sites s",
    )
    .bind(partition)
    .execute(&mut *conn)
    .await
    .expect("notices, granted every site");
    let drained = drain(&pool, &engine).await;
    assert_eq!(drained.upserts, 100_000);

    exec(
        &mut conn,
        "UPDATE fixture.t_rules SET granted = false \
          WHERE site_id = $1 AND notice_id = (SELECT id FROM fixture.t_notices ORDER BY id LIMIT 1)",
        &[replicas[0]],
    )
    .await;
    let drained = drain(&pool, &engine).await;
    println!("a rule revoked, a partition of 100 replicas holding 1k notices re-routed: {drained}");
    assert_eq!((drained.upserts, drained.deletes), (0, 1));
}

#[sqlx::test(migrations = false)]
#[ignore = "a benchmark: see CONTRIBUTING.md"]
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
    let drained = drain(&pool, &engine).await;
    println!("1k products imported, 4000 replicas: {drained}");
    assert_eq!(drained.upserts, 4_000_000);
}

/// What a dashboard costs over a partition of 200 replicas holding 3M rows
/// between them: the overview, against counting what each should hold.
#[sqlx::test(migrations = false)]
#[ignore = "a benchmark: see CONTRIBUTING.md"]
async fn bench_the_dashboard_over_200_units_holding_3m_rows(pool: crate::DbPool) {
    use crate::status::Scope;
    let (mut conn, engine, partition, replicas) = fleet(&pool, 200).await;
    sqlx::query(
        "INSERT INTO fixture.t_products (id, partition_id, name)
         SELECT uuidv7(), $1, 'p' || n FROM generate_series(1, 5000) n",
    )
    .bind(partition)
    .execute(&mut *conn)
    .await
    .expect("a catalog");
    let list = list_of(&mut conn, partition, 5_000).await;
    for replica in &replicas {
        assign(&mut conn, partition, list, *replica).await;
    }
    let drained = drain(&pool, &engine).await;
    println!("set up: {drained}");
    // Every replica confirmed all of it.
    sqlx::query("UPDATE sync.held SET sequence = 1")
        .execute(&mut *conn)
        .await
        .expect("sent");
    sqlx::query(
        "INSERT INTO sync.replicas AS x (replica_id, last_sent_at, applied_epoch, applied_sequence, applied_at)
         SELECT r, now(), (SELECT epoch FROM sync.epoch), 1, now() FROM unnest($1::uuid[]) r
         ON CONFLICT (replica_id) DO UPDATE SET last_sent_at = now(), applied_epoch = EXCLUDED.applied_epoch,
           applied_sequence = 1, applied_at = now()",
    )
    .bind(&replicas)
    .execute(&mut *conn)
    .await
    .expect("applied");
    sqlx::query("UPDATE sync.chains SET last_sequence = 1")
        .execute(&mut *conn)
        .await
        .expect("sent");
    sqlx::query("ANALYZE")
        .execute(&mut *conn)
        .await
        .expect("analyze");

    let time = |what: &str, started: Instant| println!("{what}: {:?}", started.elapsed());
    for round in ["quiet", "importing", "offline"] {
        if round == "offline" {
            // Every replica went quiet before saying it applied any of it:
            // every row it holds is in flight.
            sqlx::query("UPDATE sync.replicas SET applied_sequence = 0")
                .execute(&mut *conn)
                .await
                .expect("offline");
        }
        let started = Instant::now();
        let overview = engine
            .overview(&mut conn, Scope::Partition(partition), 20)
            .await
            .expect("overview");
        time(&format!("{round}: overview of 200"), started);
        println!("  states: {:?}", overview.states);
        let started = Instant::now();
        engine
            .health(&mut conn, Scope::Partition(partition), 20)
            .await
            .expect("health");
        time(&format!("{round}: health"), started);
        let started = Instant::now();
        engine
            .progress(&mut conn, Scope::Replicas(&replicas[..1]))
            .await
            .expect("progress");
        time(&format!("{round}: progress of one"), started);
        let started = Instant::now();
        engine
            .progress(&mut conn, Scope::Partition(partition))
            .await
            .expect("progress");
        time(&format!("{round}: progress of 200"), started);
        // An import on its way: 2k renames noted, not yet planned.
        sqlx::query(
            "UPDATE fixture.t_products SET name = name || '!' WHERE id IN
               (SELECT id FROM fixture.t_products LIMIT 2000)",
        )
        .execute(&mut *conn)
        .await
        .expect("renames");
    }
}
