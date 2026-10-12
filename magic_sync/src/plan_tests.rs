//! The engine against a schema that has every strategy, keyed every way a key
//! can be — a uuid, an integer, several columns — driven by a seeded random
//! walk of the writes an office makes and of what happens to its replicas.
//!
//! Each replica is modelled as what it was sent: every row it holds, as the
//! bytes that carried it. Passes run as the relay runs them — the notes read
//! and planned in one transaction, the rows loaded in it, with another
//! connection writing in between, and now and then a writer whose
//! transaction spans passes — and what each replica is sent is applied to
//! its model. The walk writes every kind of change: rows and keys updated,
//! members and assignments moved, groups deleted, rows moved between
//! partitions, rows of a table that points at followers and of a chain of
//! tables that travel only when pointed at, columns the replicas own, rows
//! this build cannot encode or that fit no message — routed, following and
//! mid-chain — and rows the replicas refuse; and it plans in batches that
//! split what was written together.
//!
//! What each replica should hold is worked out here, from the rows, by the
//! definition — what its rules send it, every row one of those points at,
//! every row that follows one of those, none that cannot go or points at one
//! that cannot — independently of the engine. Then:
//!
//! - after every delivery, a replica holds no row whose parent it lacks;
//! - no message carries a row its replica holds already at that version
//!   (every write gives a row a version of its own), but a snapshot;
//! - a pass that planned every pending note sends a replica only rows it
//!   should hold, and takes back only rows it should not;
//! - whenever the system settles, each live replica holds exactly what it
//!   should, every row as it is now, and the engine records exactly that.

use super::*;
use crate::model::{Faults, Model, Received, Row, Rules, Wire};
use crate::testkit::Walk;
use chrono::{DateTime, Utc};
use sqlx::Connection as _;
use std::time::Duration;

pub(super) const SCHEMA: &str = "
CREATE SCHEMA fixture;
CREATE SEQUENCE fixture.numbers;
-- Every write gives a row a version of its own: a message that carries a
-- version its replica holds carries nothing new.
CREATE SEQUENCE fixture.revs;
CREATE FUNCTION fixture.bump() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN NEW.rev := nextval('fixture.revs'); RETURN NEW; END $$;
CREATE TABLE fixture.t_replicas (
    id UUID PRIMARY KEY, partition_id UUID NOT NULL, live BOOLEAN NOT NULL,
    retired_at TIMESTAMPTZ);
CREATE TABLE fixture.t_families (
    id UUID PRIMARY KEY, partition_id UUID NOT NULL, name TEXT NOT NULL,
    rev BIGINT NOT NULL DEFAULT nextval('fixture.revs'), retired_at TIMESTAMPTZ);
CREATE TABLE fixture.t_kinds (
    id UUID PRIMARY KEY, partition_id UUID NOT NULL, name TEXT NOT NULL,
    family_id UUID REFERENCES fixture.t_families (id),
    rev BIGINT NOT NULL DEFAULT nextval('fixture.revs'), retired_at TIMESTAMPTZ);
CREATE TABLE fixture.t_products (
    id UUID PRIMARY KEY, partition_id UUID NOT NULL, kind_id UUID REFERENCES fixture.t_kinds (id),
    name TEXT NOT NULL, rev BIGINT NOT NULL DEFAULT nextval('fixture.revs'), retired_at TIMESTAMPTZ);
CREATE TABLE fixture.t_customers (
    id UUID PRIMARY KEY, partition_id UUID NOT NULL, name TEXT NOT NULL,
    rev BIGINT NOT NULL DEFAULT nextval('fixture.revs'), retired_at TIMESTAMPTZ);
CREATE TABLE fixture.t_lists (id UUID PRIMARY KEY, partition_id UUID NOT NULL);
CREATE TABLE fixture.t_list_members (
    list_id UUID NOT NULL REFERENCES fixture.t_lists (id) ON DELETE CASCADE,
    customer_id UUID NOT NULL REFERENCES fixture.t_customers (id),
    partition_id UUID NOT NULL, PRIMARY KEY (list_id, customer_id));
CREATE TABLE fixture.t_replica_lists (
    list_id UUID NOT NULL REFERENCES fixture.t_lists (id) ON DELETE CASCADE,
    replica_id UUID NOT NULL REFERENCES fixture.t_replicas (id),
    partition_id UUID NOT NULL, PRIMARY KEY (list_id, replica_id));
CREATE TABLE fixture.t_emails (
    id UUID PRIMARY KEY, partition_id UUID NOT NULL,
    customer_id UUID NOT NULL REFERENCES fixture.t_customers (id), address TEXT NOT NULL,
    rev BIGINT NOT NULL DEFAULT nextval('fixture.revs'), retired_at TIMESTAMPTZ);
CREATE TABLE fixture.t_phones (
    customer_id UUID NOT NULL REFERENCES fixture.t_customers (id), number INTEGER NOT NULL,
    partition_id UUID NOT NULL, kind_id UUID REFERENCES fixture.t_kinds (id),
    rev BIGINT NOT NULL DEFAULT nextval('fixture.revs'), retired_at TIMESTAMPTZ,
    PRIMARY KEY (customer_id, number));
-- Rows of their own that point at a follower.
CREATE TABLE fixture.t_calls (
    id UUID PRIMARY KEY, partition_id UUID NOT NULL,
    replica_id UUID REFERENCES fixture.t_replicas (id),
    customer_id UUID NOT NULL, number INTEGER NOT NULL, name TEXT NOT NULL,
    rev BIGINT NOT NULL DEFAULT nextval('fixture.revs'), retired_at TIMESTAMPTZ,
    FOREIGN KEY (customer_id, number) REFERENCES fixture.t_phones (customer_id, number)
        ON UPDATE CASCADE);
CREATE TABLE fixture.t_orders (
    id UUID PRIMARY KEY, partition_id UUID NOT NULL,
    replica_id UUID REFERENCES fixture.t_replicas (id),
    customer_id UUID REFERENCES fixture.t_customers (id), name TEXT NOT NULL DEFAULT 'o',
    rev BIGINT NOT NULL DEFAULT nextval('fixture.revs'), retired_at TIMESTAMPTZ);
CREATE TABLE fixture.t_sizes (
    code INTEGER PRIMARY KEY, partition_id UUID NOT NULL, name TEXT NOT NULL,
    rev BIGINT NOT NULL DEFAULT nextval('fixture.revs'), retired_at TIMESTAMPTZ);
CREATE TABLE fixture.t_bins (
    store INTEGER NOT NULL, slot INTEGER NOT NULL, partition_id UUID NOT NULL,
    size_code INTEGER REFERENCES fixture.t_sizes (code) ON UPDATE CASCADE, name TEXT NOT NULL,
    rev BIGINT NOT NULL DEFAULT nextval('fixture.revs'), retired_at TIMESTAMPTZ,
    PRIMARY KEY (store, slot));
-- Keyed (region, number) by its row, and by its primary key the other way
-- round: the row's order is the one keys are written in.
CREATE TABLE fixture.t_accounts (
    region INTEGER NOT NULL, number BIGINT NOT NULL, partition_id UUID NOT NULL,
    size_code INTEGER REFERENCES fixture.t_sizes (code) ON UPDATE CASCADE, name TEXT NOT NULL,
    rev BIGINT NOT NULL DEFAULT nextval('fixture.revs'), retired_at TIMESTAMPTZ,
    PRIMARY KEY (number, region));
CREATE TABLE fixture.t_account_members (
    list_id UUID NOT NULL REFERENCES fixture.t_lists (id) ON DELETE CASCADE,
    region INTEGER NOT NULL, number BIGINT NOT NULL, partition_id UUID NOT NULL,
    FOREIGN KEY (number, region) REFERENCES fixture.t_accounts (number, region),
    PRIMARY KEY (list_id, region, number));
CREATE TABLE fixture.t_tasks (
    id BIGINT PRIMARY KEY, partition_id UUID NOT NULL,
    replica_id UUID REFERENCES fixture.t_replicas (id),
    store INTEGER, slot INTEGER, region INTEGER, number BIGINT,
    name TEXT NOT NULL, rev BIGINT NOT NULL DEFAULT nextval('fixture.revs'), retired_at TIMESTAMPTZ,
    FOREIGN KEY (store, slot) REFERENCES fixture.t_bins (store, slot) ON UPDATE CASCADE,
    FOREIGN KEY (number, region) REFERENCES fixture.t_accounts (number, region));
CREATE TABLE fixture.t_task_lines (
    task_id BIGINT NOT NULL REFERENCES fixture.t_tasks (id) ON UPDATE CASCADE, line INTEGER NOT NULL,
    partition_id UUID NOT NULL, size_code INTEGER REFERENCES fixture.t_sizes (code) ON UPDATE CASCADE,
    name TEXT NOT NULL, rev BIGINT NOT NULL DEFAULT nextval('fixture.revs'), retired_at TIMESTAMPTZ,
    PRIMARY KEY (task_id, line));
-- Written on both sides: the replicas own `seen`.
CREATE TABLE fixture.t_tallies (
    id UUID PRIMARY KEY, partition_id UUID NOT NULL, kind_id UUID REFERENCES fixture.t_kinds (id),
    name TEXT NOT NULL, seen INTEGER NOT NULL DEFAULT 0);
CREATE VIEW t_replicas AS SELECT * FROM fixture.t_replicas WHERE retired_at IS NULL;
CREATE VIEW t_families AS SELECT * FROM fixture.t_families WHERE retired_at IS NULL;
CREATE VIEW t_kinds AS SELECT * FROM fixture.t_kinds WHERE retired_at IS NULL;
CREATE VIEW t_products AS SELECT * FROM fixture.t_products WHERE retired_at IS NULL;
CREATE VIEW t_customers AS SELECT * FROM fixture.t_customers WHERE retired_at IS NULL;
CREATE VIEW t_emails AS SELECT * FROM fixture.t_emails WHERE retired_at IS NULL;
CREATE VIEW t_calls AS SELECT * FROM fixture.t_calls WHERE retired_at IS NULL;
CREATE VIEW t_orders AS SELECT * FROM fixture.t_orders WHERE retired_at IS NULL;
CREATE VIEW t_phones AS SELECT * FROM fixture.t_phones WHERE retired_at IS NULL;
CREATE VIEW t_sizes AS SELECT * FROM fixture.t_sizes WHERE retired_at IS NULL;
CREATE VIEW t_bins AS SELECT * FROM fixture.t_bins WHERE retired_at IS NULL;
CREATE VIEW t_accounts AS SELECT * FROM fixture.t_accounts WHERE retired_at IS NULL;
CREATE VIEW t_tasks AS SELECT * FROM fixture.t_tasks WHERE retired_at IS NULL;
CREATE VIEW t_task_lines AS SELECT * FROM fixture.t_task_lines WHERE retired_at IS NULL;
DO $$ DECLARE t TEXT; BEGIN
  FOREACH t IN ARRAY ARRAY['t_families', 't_kinds', 't_products', 't_customers', 't_emails',
      't_phones', 't_calls', 't_orders', 't_sizes', 't_bins', 't_accounts', 't_tasks', 't_task_lines'] LOOP
    EXECUTE format('CREATE TRIGGER bump BEFORE UPDATE ON fixture.%I FOR EACH ROW EXECUTE FUNCTION fixture.bump()', t);
  END LOOP;
END $$;
";

/// A name this build cannot encode: the bug a deploy fixes.
const POISON: &str = "poison";

/// Longer than a name, or an address, may be for any message to carry it.
const OVERSIZE: usize = 20_000;

fn poisoned<S: serde::Serializer>(name: &str, s: S) -> Result<S::Ok, S::Error> {
    if name == POISON {
        return Err(serde::ser::Error::custom(
            "a value this build cannot encode",
        ));
    }
    s.serialize_str(name)
}

#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(table = "t_families", schema = "fixture", mirror = "catalog")]
#[row(active = "t_families", retired = "retired_at")]
struct Family {
    id: Uuid,
    partition_id: Uuid,
    #[serde(serialize_with = "poisoned")]
    name: String,
    rev: i64,
    retired_at: Option<DateTime<Utc>>,
}

#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(table = "t_kinds", schema = "fixture", mirror = "catalog")]
#[row(active = "t_kinds", retired = "retired_at")]
struct Kind {
    id: Uuid,
    partition_id: Uuid,
    #[serde(serialize_with = "poisoned")]
    name: String,
    family_id: Option<Uuid>,
    rev: i64,
    retired_at: Option<DateTime<Utc>>,
}

#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(
    table = "t_products",
    schema = "fixture",
    mirror = "catalog",
    sync = "broadcast"
)]
#[row(active = "t_products", retired = "retired_at")]
struct Product {
    id: Uuid,
    partition_id: Uuid,
    kind_id: Option<Uuid>,
    #[serde(serialize_with = "poisoned")]
    name: String,
    rev: i64,
    retired_at: Option<DateTime<Utc>>,
}

#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(table = "t_customers", schema = "fixture", mirror = "catalog")]
#[row(sync = "grouped(members = t_list_members, assigned = t_replica_lists)")]
#[row(active = "t_customers", retired = "retired_at")]
struct Customer {
    id: Uuid,
    partition_id: Uuid,
    #[serde(serialize_with = "poisoned")]
    name: String,
    rev: i64,
    retired_at: Option<DateTime<Utc>>,
}

#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(
    table = "t_emails",
    schema = "fixture",
    mirror = "catalog",
    sync = "follows(customer_id)"
)]
#[row(active = "t_emails", retired = "retired_at")]
struct Email {
    id: Uuid,
    partition_id: Uuid,
    customer_id: Uuid,
    #[serde(serialize_with = "poisoned")]
    address: String,
    rev: i64,
    retired_at: Option<DateTime<Utc>>,
}

/// A follower keyed by its parent and a number, pointing at a kind too.
#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(
    table = "t_phones",
    schema = "fixture",
    mirror = "catalog",
    sync = "follows(customer_id)"
)]
#[row(
    key = "customer_id, number",
    active = "t_phones",
    retired = "retired_at"
)]
struct Phone {
    customer_id: Uuid,
    number: i32,
    partition_id: Uuid,
    kind_id: Option<Uuid>,
    rev: i64,
    retired_at: Option<DateTime<Utc>>,
}

/// A row of a replica's own that points at a follower.
#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(
    table = "t_calls",
    schema = "fixture",
    mirror = "catalog",
    sync = "assigned(replica_id)"
)]
#[row(active = "t_calls", retired = "retired_at")]
struct Call {
    id: Uuid,
    partition_id: Uuid,
    replica_id: Option<Uuid>,
    customer_id: Uuid,
    number: i32,
    name: String,
    rev: i64,
    retired_at: Option<DateTime<Utc>>,
}

#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(
    table = "t_orders",
    schema = "fixture",
    mirror = "catalog",
    sync = "assigned(replica_id)"
)]
#[row(active = "t_orders", retired = "retired_at")]
struct Order {
    id: Uuid,
    partition_id: Uuid,
    replica_id: Option<Uuid>,
    customer_id: Option<Uuid>,
    name: String,
    rev: i64,
    retired_at: Option<DateTime<Utc>>,
}

#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(
    table = "t_sizes",
    schema = "fixture",
    mirror = "catalog",
    key = "code"
)]
#[row(active = "t_sizes", retired = "retired_at")]
struct Size {
    code: i32,
    partition_id: Uuid,
    #[serde(serialize_with = "poisoned")]
    name: String,
    rev: i64,
    retired_at: Option<DateTime<Utc>>,
}

#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(
    table = "t_bins",
    schema = "fixture",
    mirror = "catalog",
    sync = "broadcast"
)]
#[row(key = "store, slot", active = "t_bins", retired = "retired_at")]
struct Bin {
    store: i32,
    slot: i32,
    partition_id: Uuid,
    size_code: Option<i32>,
    #[serde(serialize_with = "poisoned")]
    name: String,
    rev: i64,
    retired_at: Option<DateTime<Utc>>,
}

#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(table = "t_accounts", schema = "fixture", mirror = "catalog")]
#[row(sync = "grouped(members = t_account_members, assigned = t_replica_lists)")]
#[row(key = "region, number", active = "t_accounts", retired = "retired_at")]
struct Account {
    region: i32,
    number: i64,
    partition_id: Uuid,
    size_code: Option<i32>,
    name: String,
    rev: i64,
    retired_at: Option<DateTime<Utc>>,
}

#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(
    table = "t_tasks",
    schema = "fixture",
    mirror = "catalog",
    sync = "assigned(replica_id)"
)]
#[row(active = "t_tasks", retired = "retired_at")]
struct Task {
    id: i64,
    partition_id: Uuid,
    replica_id: Option<Uuid>,
    store: Option<i32>,
    slot: Option<i32>,
    region: Option<i32>,
    number: Option<i64>,
    #[serde(serialize_with = "poisoned")]
    name: String,
    rev: i64,
    retired_at: Option<DateTime<Utc>>,
}

#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(
    table = "t_task_lines",
    schema = "fixture",
    mirror = "catalog",
    sync = "follows(task_id)"
)]
#[row(key = "task_id, line", active = "t_task_lines", retired = "retired_at")]
struct TaskLine {
    task_id: i64,
    line: i32,
    partition_id: Uuid,
    size_code: Option<i32>,
    name: String,
    rev: i64,
    retired_at: Option<DateTime<Utc>>,
}

/// Written on both sides: the replicas own how often each was seen.
#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(
    table = "t_tallies",
    schema = "fixture",
    mirror = "both",
    sync = "broadcast"
)]
struct Tally {
    id: Uuid,
    partition_id: Uuid,
    kind_id: Option<Uuid>,
    name: String,
    #[row(replica_owned)]
    seen: i32,
}

pub(super) fn codecs() -> Vec<Codec<()>> {
    vec![
        Codec::json::<Phone>(None),
        Codec::json::<Call>(None),
        Codec::json::<Order>(None),
        Codec::json::<Email>(None),
        Codec::json::<Customer>(None),
        Codec::json::<Product>(None),
        Codec::json::<Tally>(None),
        Codec::json::<Kind>(None),
        Codec::json::<Family>(None),
        Codec::json::<TaskLine>(None),
        Codec::json::<Task>(None),
        Codec::json::<Account>(None),
        Codec::json::<Bin>(None),
        Codec::json::<Size>(None),
    ]
}

pub(super) const REPLICAS: Replicas = Replicas {
    relation: "t_replicas",
    table: "t_replicas",
    live: "live",
    partition: Some("partition_id"),
};

/// Every foreign key between the mirrored tables: the child, its columns in
/// the parent's key order, the parent.
const REFERENCES: &[(&str, &[&str], &str)] = &[
    ("t_kinds", &["family_id"], "t_families"),
    ("t_products", &["kind_id"], "t_kinds"),
    ("t_tallies", &["kind_id"], "t_kinds"),
    ("t_phones", &["customer_id"], "t_customers"),
    ("t_phones", &["kind_id"], "t_kinds"),
    ("t_calls", &["customer_id", "number"], "t_phones"),
    ("t_emails", &["customer_id"], "t_customers"),
    ("t_orders", &["customer_id"], "t_customers"),
    ("t_bins", &["size_code"], "t_sizes"),
    ("t_accounts", &["size_code"], "t_sizes"),
    ("t_tasks", &["store", "slot"], "t_bins"),
    ("t_tasks", &["region", "number"], "t_accounts"),
    ("t_task_lines", &["task_id"], "t_tasks"),
    ("t_task_lines", &["size_code"], "t_sizes"),
];

/// The tables that follow another: the follower, its column, the parent.
const FOLLOWS: &[(&str, &str, &str)] = &[
    ("t_emails", "customer_id", "t_customers"),
    ("t_phones", "customer_id", "t_customers"),
    ("t_task_lines", "task_id", "t_tasks"),
];

/// Every mirrored table, by its key's columns in the row's order.
const KEYS: &[(&str, &[&str])] = &[
    ("t_families", &["id"]),
    ("t_kinds", &["id"]),
    ("t_products", &["id"]),
    ("t_tallies", &["id"]),
    ("t_customers", &["id"]),
    ("t_emails", &["id"]),
    ("t_phones", &["customer_id", "number"]),
    ("t_calls", &["id"]),
    ("t_orders", &["id"]),
    ("t_sizes", &["code"]),
    ("t_bins", &["store", "slot"]),
    ("t_accounts", &["region", "number"]),
    ("t_tasks", &["id"]),
    ("t_task_lines", &["task_id", "line"]),
];

/// The columns a table's codec cannot encode `POISON` in.
const POISONED: &[(&str, &str)] = &[
    ("t_families", "name"),
    ("t_kinds", "name"),
    ("t_products", "name"),
    ("t_customers", "name"),
    ("t_emails", "address"),
    ("t_sizes", "name"),
    ("t_bins", "name"),
    ("t_tasks", "name"),
];

/// A key's text from the columns of a row as JSON; `None` when one is NULL.
fn key_of(row: &serde_json::Value, columns: &[&str]) -> Option<String> {
    let parts: Option<Vec<String>> = columns
        .iter()
        .map(|c| match row.get(*c)? {
            serde_json::Value::String(v) => Some(v.clone()),
            serde_json::Value::Number(v) => Some(v.to_string()),
            _ => None,
        })
        .collect();
    parts.map(|p| p.join("|"))
}

/// What a step did.
enum Step {
    Wrote(usize),
    Skipped,
    /// The bug is fixed: what was set aside goes again.
    Cured,
}

/// The rows a row of `table` points at, read off it as JSON.
fn parents(table: &str, bytes: &[u8]) -> Vec<Row> {
    let row: serde_json::Value = serde_json::from_slice(bytes).expect("a row as JSON");
    REFERENCES
        .iter()
        .filter(|(child, _, _)| *child == table)
        .filter_map(|(_, columns, parent)| key_of(&row, columns).map(|k| ((*parent).to_owned(), k)))
        .collect()
}

/// A name the replicas' build cannot take: the bug a deploy on their side,
/// or a fix of the data, cures.
const REFUSED: &str = "refused";

/// The replicas of the walk: their foreign keys, and the rows they refuse.
const RULES: Rules = Rules {
    refuses: |_, bytes| {
        serde_json::from_slice::<serde_json::Value>(bytes)
            .is_ok_and(|row| row.get("name").and_then(|n| n.as_str()) == Some(REFUSED))
    },
    parents,
};

/// The server's relay over the walk's tables, as a host's runs it, planning
/// `batch` notes a pass.
fn relay<'a>(
    engine: &'a Engine,
    codecs: &'a [Codec<()>],
    batch: i64,
) -> crate::relay::Relay<'a, ()> {
    crate::relay::Relay {
        engine,
        codecs,
        limits: crate::envelope::Limits {
            rows: 7,
            bytes: 16 << 10,
        },
        batch,
        timeout: std::time::Duration::from_secs(5),
        backoff: crate::run::Backoff::new(
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(60),
        ),
        bound: crate::relay::Bound {
            messages: 50,
            bytes: 1 << 30,
        },
        concurrency: 4,
    }
}

/// The server, its relay, and its replicas as the protocol says they behave,
/// on a transport that keeps each one's messages in order.
struct World {
    pool: crate::DbPool,
    conn: sqlx::pool::PoolConnection<crate::Db>,
    engine: Engine,
    codecs: Vec<Codec<()>>,
    partitions: Vec<Uuid>,
    wire: Wire,
    replicas: BTreeMap<Uuid, Model>,
    history: Vec<String>,
    /// Messages the transport lost.
    lost: i64,
    /// Notes a pass plans.
    batch: i64,
    /// A writer whose transaction spans passes, and how many more it spans.
    writer: Option<(sqlx::Transaction<'static, crate::Db>, usize)>,
}

/// Two partitions: three replicas for the first — the last not live — and
/// two for the second.
async fn world(pool: crate::DbPool) -> World {
    let mut conn = pool.acquire().await.expect("a connection");
    super::tests::install(&mut conn).await;
    sqlx::raw_sql(SCHEMA)
        .execute(&mut *conn)
        .await
        .expect("schema");
    // A write that waits on the long writer gives up, rather than wait on
    // a transaction this same thread is to commit.
    sqlx::raw_sql("SET lock_timeout = '100ms'")
        .execute(&mut *conn)
        .await
        .expect("a lock timeout");
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
    let codecs = codecs();
    let tables: Vec<&Table> = codecs.iter().map(|c| &c.table).collect();
    let mut tx = conn.begin().await.expect("a transaction");
    let engine = Engine::install(&mut tx, &tables, REPLICAS)
        .await
        .expect("engine");
    engine.observe(&mut tx).await.expect("observed");
    tx.commit().await.expect("installed");
    World {
        pool,
        conn,
        engine,
        codecs,
        partitions,
        wire: Wire::new(0x5EED),
        replicas: BTreeMap::new(),
        history: Vec::new(),
        lost: 0,
        batch: 100,
        writer: None,
    }
}

/// The rows a message carries to hold, with their bytes, when it is not
/// part of a snapshot.
fn carried(bytes: &[u8]) -> Vec<(Row, Vec<u8>)> {
    use prost::Message as _;
    let Some(crate::envelope::Body::Down(down)) = crate::envelope::ToReplica::decode(bytes)
        .ok()
        .and_then(|m| m.body)
    else {
        return Vec::new();
    };
    if !down.snapshot_id.is_empty() {
        return Vec::new();
    }
    down.tables
        .into_iter()
        .flat_map(|t| {
            let table = t.table.clone();
            t.upserts
                .into_iter()
                .zip(t.keys)
                .map(move |(bytes, key)| ((table.clone(), key.parts.join("|")), bytes))
        })
        .collect()
}

impl World {
    /// One round of the system: the relay's pass — observed, recorded,
    /// published — then each replica takes what reached it and says what it
    /// applied and refused. With `storm`, anything may go wrong on the way:
    /// the relay dies between its record and its sends, or loses its lock
    /// while recording; a replica takes only some of what waits for it, dies
    /// before acknowledging one, or says nothing, or its report is lost.
    /// Answers whether anything moved.
    async fn round(&mut self, mut storm: Option<&mut Walk>) -> bool {
        let mut chance = |n: usize| storm.as_deref_mut().is_some_and(|w| w.below(n) == 0);
        if !chance(1) || chance(2) {
            // The backoffs ran out: time passes between rounds, and out of
            // a storm nothing waits for a clock.
            sqlx::query("UPDATE sync.replicas SET retry_after = now()")
                .execute(&mut *self.conn)
                .await
                .expect("waits over");
        }
        let mut tx = self.conn.begin().await.expect("a transaction");
        self.engine.observe(&mut tx).await.expect("observed");
        tx.commit().await.expect("commit");
        let live: BTreeSet<Uuid> = sqlx::query_scalar("SELECT id FROM t_replicas WHERE live")
            .fetch_all(&mut *self.conn)
            .await
            .expect("live")
            .into_iter()
            .collect();
        let relay = relay(&self.engine, &self.codecs, self.batch);
        let mut tx = self
            .conn
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ")
            .await
            .expect("a transaction");
        let drawn: i64 = sqlx::query_scalar(
            "SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM sync.messages",
        )
        .fetch_one(&mut *tx)
        .await
        .expect("drawn");
        let held: BTreeSet<(Uuid, String, String)> =
            sqlx::query_as("SELECT replica_id, relation, row_key FROM sync.held")
                .fetch_all(&mut *tx)
                .await
                .expect("held")
                .into_iter()
                .collect();
        let outcome = relay.record(&mut tx).await.expect("recorded");
        assert_minimal(&mut tx, drawn, &held, &self.history).await;
        if chance(12) {
            self.history
                .push("the relay lost its lock while recording".to_owned());
            drop(tx);
        } else {
            tx.commit().await.expect("commit");
        }
        let before: BTreeMap<Uuid, usize> = self
            .replicas
            .keys()
            .chain(live.iter())
            .map(|r| (*r, self.wire.ids(*r).len()))
            .collect();
        let sent = if chance(12) {
            self.history
                .push("the relay died before it sent".to_owned());
            0
        } else {
            relay
                .publish(&mut self.conn, &self.wire)
                .await
                .expect("published")
                .sent
        };
        for (replica, count) in before {
            assert!(
                live.contains(&replica) || self.wire.ids(replica).len() == count,
                "{replica} is not live and was sent something; last steps: {:#?}",
                self.last_steps()
            );
        }
        let mut moved = outcome.done + sent > 0;
        let targets: Vec<Uuid> = self.wire.queues.lock().unwrap().keys().copied().collect();
        for replica in targets {
            let take = storm.as_deref_mut().map_or(usize::MAX, |w| w.below(4));
            let model = self.replicas.entry(replica).or_default();
            for _ in 0..take {
                let Some(bytes) = self.wire.next(replica) else {
                    break;
                };
                if storm.as_deref_mut().is_some_and(|w| w.below(60) == 0) {
                    // Lost on the way, against the transport's contract: the
                    // replica sees a gap, and says so.
                    self.wire.acknowledge(replica);
                    self.lost += 1;
                    continue;
                }
                let carried = carried(&bytes);
                let held: Vec<Option<Vec<u8>>> = carried
                    .iter()
                    .map(|(row, _)| model.rows.get(row).cloned())
                    .collect();
                let rejected = model.rejections.clone();
                if model.receive(&bytes, &RULES) == Received::Applied {
                    for ((row, bytes), held) in carried.iter().zip(held) {
                        assert!(
                            held.as_ref() != Some(bytes),
                            "{replica} was sent {row:?} again at the version it holds; last steps: {:#?}",
                            &self.history[self.history.len().saturating_sub(8)..]
                        );
                    }
                }
                moved = true;
                // A row it could not let go of because a row it holds
                // points at it is the replica covering for the server —
                // unless it refused that row's version that points
                // elsewhere, which the server cannot know it kept.
                for (row, rejection) in &model.rejections {
                    if rejection.reason != "pointed at"
                        || rejected
                            .get(row)
                            .is_some_and(|r| r.sequence == rejection.sequence)
                        || model.pinned.contains(row)
                    {
                        continue;
                    }
                    let pointing: Vec<&Row> = model
                        .rows
                        .iter()
                        .filter(|((t, _), bytes)| parents(t, bytes).contains(row))
                        .map(|(r, _)| r)
                        .filter(|r| !model.rejections.contains_key(*r))
                        .collect();
                    assert!(
                        pointing.is_empty(),
                        "{replica} was told to let go of {row:?}, which {pointing:?} it holds points at; last steps: {:#?}",
                        &self.history[self.history.len().saturating_sub(8)..]
                    );
                }
                let orphans = model.orphans(&RULES);
                assert!(
                    orphans.is_empty(),
                    "{replica} holds rows without their parents: {orphans:?}; last steps: {:#?}",
                    &self.history[self.history.len().saturating_sub(8)..]
                );
                if storm.as_deref_mut().is_some_and(|w| w.below(15) == 0) {
                    // It died after its commit, before acknowledging.
                    break;
                }
                self.wire.acknowledge(replica);
            }
        }
        let replicas: Vec<Uuid> = self.replicas.keys().copied().collect();
        for replica in replicas {
            if storm.as_deref_mut().is_some_and(|w| w.below(3) == 0) {
                continue;
            }
            let Some(up) = self.replicas[&replica].up() else {
                continue;
            };
            let mut tx = self.conn.begin().await.expect("a transaction");
            crate::filing::file(&mut tx, &self.codecs, &up, replica, &(), i64::MAX)
                .await
                .expect("filed");
            tx.commit().await.expect("filed");
            moved = true;
            if storm.as_deref_mut().is_some_and(|w| w.below(10) == 0) {
                // Filed, and the uplink's answer lost: it says it again.
                continue;
            }
            self.replicas
                .get_mut(&replica)
                .expect("modelled")
                .delivered(&up);
        }
        moved
    }

    /// The long writer commits, if one is open.
    async fn commit_writer(&mut self) {
        if let Some((writer, _)) = self.writer.take() {
            writer.commit().await.expect("the long writer commits");
            self.history.push("the long writer committed".to_owned());
        }
    }

    /// Rounds without a fault until nothing moves: every note planned, every
    /// outbox sent, every message applied and confirmed. Then each replica
    /// holds what the engine records it holding.
    async fn settle(&mut self) {
        self.commit_writer().await;
        let faults = std::mem::take(&mut *self.wire.faults.lock().unwrap());
        for _ in 0..400 {
            if self.round(None).await {
                continue;
            }
            // A message lost on the way, against the transport's contract,
            // leaves a replica waiting; the engine's net for it is to
            // provision a replica silent on what it was sent, after a while.
            let healed = crate::relay::heal(&mut self.conn, &self.engine, Duration::ZERO)
                .await
                .expect("healed");
            if healed == 0 {
                *self.wire.faults.lock().unwrap() = faults;
                let replicas: Vec<Uuid> = self.replicas.keys().copied().collect();
                for replica in replicas {
                    self.assert_recorded(replica).await;
                }
                return;
            }
        }
        panic!(
            "the system did not settle; last steps: {:#?}",
            self.last_steps()
        );
    }

    /// The engine records `replica` holding exactly what it holds — what it
    /// took as taken, what it refused as refused.
    async fn assert_recorded(&mut self, replica: Uuid) {
        let recorded: Vec<(String, String, bool)> = sqlx::query_as(
            "SELECT relation, row_key, refused IS NOT NULL FROM sync.held WHERE replica_id = $1",
        )
        .bind(replica)
        .fetch_all(&mut *self.conn)
        .await
        .expect("held");
        let live: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM t_replicas WHERE id = $1 AND live)")
                .bind(replica)
                .fetch_one(&mut *self.conn)
                .await
                .expect("live");
        if !live {
            assert!(
                recorded.is_empty(),
                "{replica} is not live and is recorded holding {recorded:?}"
            );
            return;
        }
        let model = self.replicas.get(&replica).cloned().unwrap_or_default();
        let held: BTreeSet<Row> = model
            .rows
            .keys()
            .filter(|row| !model.rejections.contains_key(*row))
            .cloned()
            .collect();
        let refused: BTreeSet<Row> = model.rejections.keys().cloned().collect();
        let as_recorded = |refused: bool| -> BTreeSet<Row> {
            recorded
                .iter()
                .filter(|r| r.2 == refused)
                .map(|(t, k, _)| (t.clone(), k.clone()))
                .collect()
        };
        for (what, recorded, holds) in [
            ("taken", as_recorded(false), &held),
            ("refused", as_recorded(true), &refused),
        ] {
            assert!(
                &recorded == holds,
                "{replica}: recorded {what} other than it is:\n  recorded, not so {:?}\n  so, not recorded {:?}\n  last steps: {:#?}",
                recorded.difference(holds).collect::<Vec<_>>(),
                holds.difference(&recorded).collect::<Vec<_>>(),
                self.last_steps(),
            );
        }
    }

    fn last_steps(&self) -> &[String] {
        &self.history[self.history.len().saturating_sub(8)..]
    }

    /// Each live replica holds exactly what it should, every row as it is
    /// now — but a row it refused to take or to let go of, which it holds as
    /// it had it, or not at all, and the columns it owns, which are its own.
    async fn assert_holdings(&mut self, seed: u64, step: usize) {
        let oracle = Oracle::read(&mut self.conn).await;
        let replicas: Vec<Uuid> =
            sqlx::query_scalar("SELECT id FROM t_replicas WHERE live ORDER BY id")
                .fetch_all(&mut *self.conn)
                .await
                .expect("replicas");
        for replica in replicas {
            let model = self.replicas.get(&replica).cloned().unwrap_or_default();
            let should = oracle.should(replica);
            let holds: BTreeSet<Row> = model
                .rows
                .keys()
                .filter(|row| !model.rejections.contains_key(*row))
                .cloned()
                .collect();
            let missing: Vec<&Row> = should
                .iter()
                .filter(|row| {
                    !model.rows.contains_key(*row) && !model.rejections.contains_key(*row)
                })
                .collect();
            let extra: Vec<(&Row, Vec<&Row>)> = model
                .rows
                .keys()
                .filter(|row| !should.contains(*row) && !model.rejections.contains_key(*row))
                .map(|row| {
                    let pointing = model
                        .rows
                        .iter()
                        .filter(|((t, _), bytes)| parents(t, bytes).contains(row))
                        .map(|(r, _)| r)
                        .collect();
                    (row, pointing)
                })
                .collect();
            assert!(
                missing.is_empty() && extra.is_empty(),
                "seed {seed:#x}, after step {step}, {replica}:\n  holds   {}\n  missing {missing:?}\n  extra, and what points at it there {extra:?}\n  last steps: {:#?}",
                holds.len(),
                self.last_steps(),
            );
            for row in &holds {
                let bytes = &model.rows[row];
                let mut held: serde_json::Value = serde_json::from_slice(bytes).expect("JSON");
                let mut now = self.current(&row.0, &row.1).await;
                for value in std::iter::once(&mut held).chain(now.as_mut()) {
                    if let Some(object) = value.as_object_mut() {
                        object.remove("seen");
                    }
                }
                assert!(
                    Some(&held) == now.as_ref(),
                    "seed {seed:#x}, after step {step}, {replica} holds {row:?} as\n  {held}\n  and it is now\n  {now:?}\n  last steps: {:#?}",
                    self.last_steps(),
                );
            }
        }
    }

    /// A row as it is now, as its codec encodes it; `None` once it is gone.
    async fn current(&mut self, table: &str, key: &str) -> Option<serde_json::Value> {
        let codec = self
            .codecs
            .iter()
            .find(|c| c.table.name == table)
            .expect("a codec");
        let parts: Vec<String> = key.split('|').map(str::to_owned).collect();
        let key = codec.key(&parts).expect("a key");
        let encoded = codec.load(&mut self.conn, &[key]).await.expect("load");
        let (_, bytes) = encoded.rows.first()?;
        Some(serde_json::from_slice(bytes).expect("JSON"))
    }

    /// What a relay does once a deploy fixed what refused them: every row
    /// set aside goes again.
    async fn retry_quarantine(&mut self) {
        crate::relay::retry_quarantine(&mut self.conn)
            .await
            .expect("noted again");
    }
}

/// What every replica should hold, worked out from the rows themselves by
/// the definition, independently of the engine.
struct Oracle {
    /// Every row of every mirrored table, as JSON.
    rows: BTreeMap<Row, serde_json::Value>,
    /// The live replicas, with their partitions.
    replicas: BTreeMap<Uuid, Uuid>,
    /// Who each group is given to, and its members, as rows.
    given: Vec<(Uuid, Uuid)>,
    members: Vec<(Uuid, Row)>,
    /// The rows that cannot go: that do not encode or fit no message, and
    /// every row that points at one.
    dead: BTreeSet<Row>,
}

impl Oracle {
    async fn read(conn: &mut Conn) -> Self {
        let mut rows = BTreeMap::new();
        for (table, key) in KEYS {
            let all: Vec<serde_json::Value> =
                sqlx::query_scalar(&format!("SELECT to_jsonb(t) FROM fixture.{table} t"))
                    .fetch_all(&mut *conn)
                    .await
                    .expect(table);
            for row in all {
                let key = key_of(&row, key).expect("a key");
                rows.insert(((*table).to_owned(), key), row);
            }
        }
        let replicas: Vec<(Uuid, Uuid)> =
            sqlx::query_as("SELECT id, partition_id FROM t_replicas WHERE live")
                .fetch_all(&mut *conn)
                .await
                .expect("replicas");
        let given: Vec<(Uuid, Uuid)> =
            sqlx::query_as("SELECT list_id, replica_id FROM fixture.t_replica_lists")
                .fetch_all(&mut *conn)
                .await
                .expect("given");
        let mut members: Vec<(Uuid, Row)> = Vec::new();
        for (sql, table) in [
            (
                "SELECT list_id, customer_id::text FROM fixture.t_list_members",
                "t_customers",
            ),
            (
                "SELECT list_id, region || '|' || number FROM fixture.t_account_members",
                "t_accounts",
            ),
        ] {
            let found: Vec<(Uuid, String)> = sqlx::query_as(sql)
                .fetch_all(&mut *conn)
                .await
                .expect("members");
            members.extend(found.into_iter().map(|(l, k)| (l, (table.to_owned(), k))));
        }
        let cannot = |(table, _): &Row, row: &serde_json::Value| {
            POISONED.iter().any(|(t, column)| {
                t == table && row.get(*column).and_then(|v| v.as_str()) == Some(POISON)
            }) || row.to_string().len() > OVERSIZE
        };
        let mut dead: BTreeSet<Row> = rows
            .iter()
            .filter(|(r, row)| cannot(r, row))
            .map(|(r, _)| r.clone())
            .collect();
        loop {
            let more: Vec<Row> = rows
                .iter()
                .filter(|(r, row)| {
                    !dead.contains(*r)
                        && REFERENCES.iter().any(|(child, columns, parent)| {
                            *child == r.0
                                && key_of(row, columns)
                                    .is_some_and(|k| dead.contains(&((*parent).to_owned(), k)))
                        })
                })
                .map(|(r, _)| r.clone())
                .collect();
            if more.is_empty() {
                break;
            }
            dead.extend(more);
        }
        Oracle {
            rows,
            replicas: replicas.into_iter().collect(),
            given,
            members,
            dead,
        }
    }

    fn active(row: &serde_json::Value) -> bool {
        row.get("retired_at").is_none_or(serde_json::Value::is_null)
    }

    /// What `replica` should hold: what its rules send it, every row one of
    /// those points at, every row that follows one of those — none that
    /// cannot go.
    fn should(&self, replica: Uuid) -> BTreeSet<Row> {
        let Some(partition) = self.replicas.get(&replica) else {
            return BTreeSet::new();
        };
        let uuid = |v: Option<&serde_json::Value>, u: &Uuid| {
            v.and_then(|v| v.as_str()) == Some(u.to_string().as_str())
        };
        let lists: BTreeSet<Uuid> = self
            .given
            .iter()
            .filter(|(_, r)| *r == replica)
            .map(|(l, _)| *l)
            .collect();
        let grouped: BTreeSet<&Row> = self
            .members
            .iter()
            .filter(|(l, _)| lists.contains(l))
            .map(|(_, r)| r)
            .collect();
        let mut out: BTreeSet<Row> = BTreeSet::new();
        let mut queue: Vec<Row> = self
            .rows
            .iter()
            .filter(|(r, row)| {
                Self::active(row)
                    && match r.0.as_str() {
                        "t_products" | "t_bins" | "t_tallies" => {
                            uuid(row.get("partition_id"), partition)
                        }
                        "t_orders" | "t_tasks" | "t_calls" => uuid(row.get("replica_id"), &replica),
                        "t_customers" | "t_accounts" => grouped.contains(r),
                        _ => false,
                    }
            })
            .map(|(r, _)| r.clone())
            .collect();
        while let Some(r) = queue.pop() {
            if self.dead.contains(&r) || !out.insert(r.clone()) {
                continue;
            }
            let row = &self.rows[&r];
            for (child, columns, parent) in REFERENCES {
                if *child == r.0 {
                    if let Some(k) = key_of(row, columns) {
                        let p = ((*parent).to_owned(), k);
                        if self.rows.contains_key(&p) {
                            queue.push(p);
                        }
                    }
                }
            }
            for (follower, column, parent) in FOLLOWS {
                if *parent == r.0 {
                    queue.extend(
                        self.rows
                            .iter()
                            .filter(|(f, frow)| {
                                f.0 == *follower
                                    && Self::active(frow)
                                    && key_of(frow, &[column]).as_deref() == Some(r.1.as_str())
                            })
                            .map(|(f, _)| f.clone()),
                    );
                }
            }
        }
        out
    }
}

/// A pass sends each replica only rows it should hold, or held already —
/// a change not planned yet may still point at it there — and takes back
/// only rows it should not hold: what `tx` — the pass, recorded and not
/// committed — wrote to the outboxes past the message `drawn`, against what
/// should be held as the pass read the rows, and `held`, what the replicas
/// held before it.
async fn assert_minimal(
    tx: &mut Conn,
    drawn: i64,
    held: &BTreeSet<(Uuid, String, String)>,
    history: &[String],
) {
    use prost::Message as _;
    let written: Vec<(Uuid, Vec<u8>)> =
        sqlx::query_as("SELECT replica_id, message FROM sync.outbox WHERE sequence > $1")
            .bind(drawn)
            .fetch_all(&mut *tx)
            .await
            .expect("written");
    if written.is_empty() {
        return;
    }
    let oracle = Oracle::read(&mut *tx).await;
    let mut should: BTreeMap<Uuid, BTreeSet<Row>> = BTreeMap::new();
    for (replica, message) in written {
        let Some(crate::envelope::Body::Down(down)) =
            crate::envelope::ToReplica::decode(message.as_slice())
                .ok()
                .and_then(|m| m.body)
        else {
            continue;
        };
        let should = should
            .entry(replica)
            .or_insert_with(|| oracle.should(replica));
        for table in down.tables {
            for key in &table.keys {
                let row = (table.table.clone(), key.parts.join("|"));
                assert!(
                    should.contains(&row) || held.contains(&(replica, row.0.clone(), row.1.clone())),
                    "{replica} was sent {row:?}, which it should not hold, nor held; last steps: {:#?}",
                    &history[history.len().saturating_sub(8)..]
                );
            }
            for key in &table.deletes {
                let row = (table.table.clone(), key.parts.join("|"));
                assert!(
                    !should.contains(&row),
                    "{replica} was told to let go of {row:?}, which it should hold; last steps: {:#?}",
                    &history[history.len().saturating_sub(8)..]
                );
            }
        }
    }
}

async fn uuids(conn: &mut Conn, table: &str, partition: Uuid) -> Vec<Uuid> {
    sqlx::query_scalar(&format!(
        "SELECT id FROM fixture.{table} WHERE partition_id = $1 ORDER BY id"
    ))
    .bind(partition)
    .fetch_all(&mut *conn)
    .await
    .expect(table)
}

async fn keys<T>(conn: &mut Conn, sql: &str, partition: Uuid) -> Vec<T>
where
    T: for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> + Send + Unpin,
{
    sqlx::query_as(sql)
        .bind(partition)
        .fetch_all(&mut *conn)
        .await
        .expect(sql)
}

/// Runs a write the schema may refuse — a row picked from none, a key taken,
/// a row something still points at, a row the long writer holds: answers
/// whether it went in. The write is its own transaction's, or in one of its
/// own within the long writer's, so a refusal leaves the long writer as it
/// was.
async fn wrote(
    conn: &mut Conn,
    query: sqlx::query::Query<'_, crate::Db, sqlx::postgres::PgArguments>,
) -> bool {
    let mut tx = conn.begin().await.expect("a transaction");
    match query.execute(&mut *tx).await {
        Ok(done) => {
            tx.commit().await.expect("committed");
            done.rows_affected() > 0
        }
        Err(sqlx::Error::Database(e))
            if matches!(
                e.code().as_deref(),
                Some("23502" | "23503" | "23505" | "55P03")
            ) =>
        {
            false
        }
        Err(e) => panic!("a step: {e}"),
    }
}

macro_rules! write {
    ($conn:expr, $sql:expr $(, $bind:expr)* $(,)?) => {
        wrote($conn, sqlx::query($sql)$(.bind($bind))*).await
    };
}

const RETIRE: &str = "retired_at = CASE WHEN retired_at IS NULL THEN now() END";

/// The fix, deployed: what this build could not encode or no message could
/// carry, and what the replicas' could not take, is data both take now.
async fn cure(conn: &mut Conn) {
    for (table, column) in POISONED.iter().copied().chain([
        ("t_customers", "name"),
        ("t_products", "name"),
        ("t_tasks", "name"),
    ]) {
        sqlx::query(&format!(
            "UPDATE fixture.{table} SET {column} = 'cured' \
              WHERE {column} IN ($1, $2) OR length({column}) > 1000"
        ))
        .bind(POISON)
        .bind(REFUSED)
        .execute(&mut *conn)
        .await
        .expect("the fix");
    }
}

/// One write of an office, or one thing that happens to a replica, picked at
/// random among those the schema allows.
async fn step(conn: &mut Conn, walk: &mut Walk, partitions: &[Uuid]) -> Step {
    let partition = partitions[walk.below(partitions.len())];
    let replicas: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM fixture.t_replicas ORDER BY id")
        .fetch_all(&mut *conn)
        .await
        .expect("replicas");
    let families = uuids(conn, "t_families", partition).await;
    let kinds = uuids(conn, "t_kinds", partition).await;
    let products = uuids(conn, "t_products", partition).await;
    let customers = uuids(conn, "t_customers", partition).await;
    let lists = uuids(conn, "t_lists", partition).await;
    let emails = uuids(conn, "t_emails", partition).await;
    let orders = uuids(conn, "t_orders", partition).await;
    let calls = uuids(conn, "t_calls", partition).await;
    let tallies = uuids(conn, "t_tallies", partition).await;
    let phones: Vec<(Uuid, i32)> = keys(
        conn,
        "SELECT customer_id, number FROM fixture.t_phones WHERE partition_id = $1 ORDER BY 1, 2",
        partition,
    )
    .await;
    let sizes: Vec<i32> =
        sqlx::query_scalar("SELECT code FROM fixture.t_sizes WHERE partition_id = $1 ORDER BY 1")
            .bind(partition)
            .fetch_all(&mut *conn)
            .await
            .expect("sizes");
    let bins: Vec<(i32, i32)> = keys(
        conn,
        "SELECT store, slot FROM fixture.t_bins WHERE partition_id = $1 ORDER BY 1, 2",
        partition,
    )
    .await;
    let accounts: Vec<(i32, i64)> = keys(
        conn,
        "SELECT region, number FROM fixture.t_accounts WHERE partition_id = $1 ORDER BY 1, 2",
        partition,
    )
    .await;
    let tasks: Vec<i64> =
        sqlx::query_scalar("SELECT id FROM fixture.t_tasks WHERE partition_id = $1 ORDER BY 1")
            .bind(partition)
            .fetch_all(&mut *conn)
            .await
            .expect("tasks");
    let lines: Vec<(i64, i32)> = keys(
        conn,
        "SELECT task_id, line FROM fixture.t_task_lines WHERE partition_id = $1 ORDER BY 1, 2",
        partition,
    )
    .await;
    let number: i64 = sqlx::query_scalar("SELECT nextval('fixture.numbers')")
        .fetch_one(&mut *conn)
        .await
        .expect("a number");
    let new = walk.uuid();
    let choice = walk.below(103);
    let (family, kind, product, customer, list, email, order) = (
        walk.pick(&families),
        walk.pick(&kinds),
        walk.pick(&products),
        walk.pick(&customers),
        walk.pick(&lists),
        walk.pick(&emails),
        walk.pick(&orders),
    );
    let (replica, phone, size, bin, account, task, line) = (
        walk.pick(&replicas),
        walk.pick(&phones),
        walk.pick(&sizes),
        walk.pick(&bins),
        walk.pick(&accounts),
        walk.pick(&tasks),
        walk.pick(&lines),
    );
    let (call, tally, other_list, other_customer) = (
        walk.pick(&calls),
        walk.pick(&tallies),
        walk.pick(&lists),
        walk.pick(&customers),
    );
    let small = i32::try_from(walk.below(4)).expect("small");
    let other = partitions[walk.below(partitions.len())];
    let big = "o".repeat(OVERSIZE + 1);
    let done = match choice {
        0 => write!(conn, "INSERT INTO fixture.t_kinds (id, partition_id, name, family_id) VALUES ($1, $2, 'k', $3)", new, partition, family),
        1 => write!(conn, "INSERT INTO fixture.t_products (id, partition_id, kind_id, name) VALUES ($1, $2, $3, 'p')", new, partition, kind),
        2 => write!(conn, "UPDATE fixture.t_products SET kind_id = $2 WHERE id = $1", product, kind),
        3 => write!(conn, "UPDATE fixture.t_products SET kind_id = NULL WHERE id = $1", product),
        4 => write!(conn, &format!("UPDATE fixture.t_products SET {RETIRE} WHERE id = $1"), product),
        5 => write!(conn, "UPDATE fixture.t_products SET name = name || '.' WHERE id = $1", product),
        6 => write!(conn, "DELETE FROM fixture.t_products WHERE id = $1", product),
        7 => write!(conn, "UPDATE fixture.t_kinds SET name = name || '.' WHERE id = $1 AND name <> $2", kind, POISON),
        8 => write!(conn, &format!("UPDATE fixture.t_kinds SET {RETIRE} WHERE id = $1"), kind),
        9 => write!(conn, "DELETE FROM fixture.t_kinds WHERE id = $1", kind),
        10 => write!(conn, "INSERT INTO fixture.t_customers (id, partition_id, name) VALUES ($1, $2, 'c')", new, partition),
        11 => write!(conn, "UPDATE fixture.t_customers SET name = name || '.' WHERE id = $1", customer),
        12 => write!(conn, &format!("UPDATE fixture.t_customers SET {RETIRE} WHERE id = $1"), customer),
        13 => write!(conn, "INSERT INTO fixture.t_lists (id, partition_id) VALUES ($1, $2)", new, partition),
        14 | 15 => write!(conn, "INSERT INTO fixture.t_list_members (list_id, customer_id, partition_id) VALUES ($1, $2, $3)", list, customer, partition),
        16 => write!(conn, "DELETE FROM fixture.t_list_members WHERE list_id = $1 AND customer_id = $2", list, customer),
        17 => write!(conn, "INSERT INTO fixture.t_replica_lists (list_id, replica_id, partition_id) VALUES ($1, $2, $3)", list, replica, partition),
        18 => write!(conn, "DELETE FROM fixture.t_replica_lists WHERE list_id = $1 AND replica_id = $2", list, replica),
        19 => write!(conn, "INSERT INTO fixture.t_emails (id, partition_id, customer_id, address) VALUES ($1, $2, $3, 'e')", new, partition, customer),
        20 => write!(conn, "UPDATE fixture.t_emails SET customer_id = $2 WHERE id = $1", email, customer),
        21 => write!(conn, &format!("UPDATE fixture.t_emails SET {RETIRE} WHERE id = $1"), email),
        22 => write!(conn, "DELETE FROM fixture.t_emails WHERE id = $1", email),
        23 => write!(conn, "INSERT INTO fixture.t_phones (customer_id, number, partition_id, kind_id) VALUES ($1, $2, $3, $4)", customer, small, partition, kind),
        24 => write!(conn, &format!("UPDATE fixture.t_phones SET {RETIRE} WHERE customer_id = $1 AND number = $2"), phone.map(|p| p.0), phone.map(|p| p.1)),
        25 => write!(conn, "UPDATE fixture.t_phones SET kind_id = $3 WHERE customer_id = $1 AND number = $2", phone.map(|p| p.0), phone.map(|p| p.1), kind),
        26 => write!(conn, "DELETE FROM fixture.t_phones WHERE customer_id = $1 AND number = $2", phone.map(|p| p.0), phone.map(|p| p.1)),
        27 => write!(conn, "INSERT INTO fixture.t_orders (id, partition_id, replica_id, customer_id) VALUES ($1, $2, $3, $4)", new, partition, replica, customer),
        28 => write!(conn, "UPDATE fixture.t_orders SET replica_id = $2 WHERE id = $1", order, replica),
        29 => write!(conn, &format!("UPDATE fixture.t_orders SET {RETIRE} WHERE id = $1"), order),
        30 => write!(conn, "DELETE FROM fixture.t_orders WHERE id = $1", order),
        31 => write!(conn, "UPDATE fixture.t_orders SET name = name || '.', customer_id = coalesce($2, customer_id) WHERE id = $1", order, customer),
        32 => write!(conn, "INSERT INTO fixture.t_sizes (code, partition_id, name) VALUES ($1, $2, 's')", i32::try_from(number).expect("a code"), partition),
        33 => write!(conn, "UPDATE fixture.t_sizes SET name = name || '.' WHERE code = $1 AND name <> $2", size, POISON),
        34 => write!(conn, &format!("UPDATE fixture.t_sizes SET {RETIRE} WHERE code = $1"), size),
        35 => write!(conn, "DELETE FROM fixture.t_sizes WHERE code = $1", size),
        36 => write!(conn, "INSERT INTO fixture.t_bins (store, slot, partition_id, size_code, name) VALUES ($1, $2, $3, $4, 'b')", small, i32::try_from(walk.below(4)).expect("small"), partition, size),
        37 => write!(conn, "UPDATE fixture.t_bins SET size_code = $3 WHERE store = $1 AND slot = $2", bin.map(|b| b.0), bin.map(|b| b.1), size),
        38 => write!(conn, "UPDATE fixture.t_bins SET name = name || '.' WHERE store = $1 AND slot = $2 AND name <> $3", bin.map(|b| b.0), bin.map(|b| b.1), POISON),
        39 => write!(conn, &format!("UPDATE fixture.t_bins SET {RETIRE} WHERE store = $1 AND slot = $2"), bin.map(|b| b.0), bin.map(|b| b.1)),
        40 => write!(conn, "DELETE FROM fixture.t_bins WHERE store = $1 AND slot = $2", bin.map(|b| b.0), bin.map(|b| b.1)),
        // Beyond 2^53 every other time: a number JSON would round.
        41 => write!(conn, "INSERT INTO fixture.t_accounts (region, number, partition_id, size_code, name) VALUES ($1, $2, $3, $4, 'a')", small, number + if number % 2 == 0 { 1 << 60 } else { 0 }, partition, size),
        42 => write!(conn, "UPDATE fixture.t_accounts SET name = name || '.' WHERE region = $1 AND number = $2", account.map(|a| a.0), account.map(|a| a.1)),
        43 => write!(conn, &format!("UPDATE fixture.t_accounts SET {RETIRE} WHERE region = $1 AND number = $2"), account.map(|a| a.0), account.map(|a| a.1)),
        44 | 45 => write!(conn, "INSERT INTO fixture.t_account_members (list_id, region, number, partition_id) VALUES ($1, $2, $3, $4)", list, account.map(|a| a.0), account.map(|a| a.1), partition),
        46 => write!(conn, "DELETE FROM fixture.t_account_members WHERE list_id = $1 AND region = $2 AND number = $3", list, account.map(|a| a.0), account.map(|a| a.1)),
        47 => write!(conn, "INSERT INTO fixture.t_tasks (id, partition_id, replica_id, store, slot, region, number, name) VALUES ($1, $2, $3, $4, $5, $6, $7, 't')", (1_i64 << 60) + number, partition, replica, bin.map(|b| b.0), bin.map(|b| b.1), account.map(|a| a.0), account.map(|a| a.1)),
        48 => write!(conn, "UPDATE fixture.t_tasks SET replica_id = $2 WHERE id = $1", task, replica),
        49 => write!(conn, "UPDATE fixture.t_tasks SET store = $2, slot = $3 WHERE id = $1", task, bin.map(|b| b.0), bin.map(|b| b.1)),
        50 => write!(conn, "UPDATE fixture.t_tasks SET region = $2, number = $3 WHERE id = $1", task, account.map(|a| a.0), account.map(|a| a.1)),
        51 => write!(conn, "UPDATE fixture.t_tasks SET name = name || '.' WHERE id = $1 AND name <> $2", task, POISON),
        52 => write!(conn, &format!("UPDATE fixture.t_tasks SET {RETIRE} WHERE id = $1"), task),
        53 => write!(conn, "DELETE FROM fixture.t_tasks WHERE id = $1", task),
        54 => write!(conn, "INSERT INTO fixture.t_task_lines (task_id, line, partition_id, size_code, name) VALUES ($1, $2, $3, $4, 'l')", task, small, partition, size),
        55 => write!(conn, "UPDATE fixture.t_task_lines SET name = name || '.', size_code = $3 WHERE task_id = $1 AND line = $2", line.map(|l| l.0), line.map(|l| l.1), size),
        56 => write!(conn, &format!("UPDATE fixture.t_task_lines SET {RETIRE} WHERE task_id = $1 AND line = $2"), line.map(|l| l.0), line.map(|l| l.1)),
        57 => write!(conn, "DELETE FROM fixture.t_task_lines WHERE task_id = $1 AND line = $2", line.map(|l| l.0), line.map(|l| l.1)),
        58 => write!(conn, "UPDATE fixture.t_replicas SET live = NOT live WHERE id = $1", replica),
        59 => write!(conn, "UPDATE fixture.t_replicas SET partition_id = $2 WHERE id = $1", replica, other),
        // Gone from the relation the replicas are read from, and back.
        60 => write!(conn, &format!("UPDATE fixture.t_replicas SET {RETIRE} WHERE id = $1"), replica),
        61 => {
            let few: i64 = sqlx::query_scalar("SELECT count(*) FROM fixture.t_replicas")
                .fetch_one(&mut *conn)
                .await
                .expect("replicas");
            few < 9 && write!(conn, "INSERT INTO fixture.t_replicas (id, partition_id, live) VALUES ($1, $2, true)", new, partition)
        }
        62 => write!(conn, "INSERT INTO sync.notes (partition_id, kind, replica_id) SELECT partition_id, 'resync', id FROM fixture.t_replicas WHERE id = $1", replica),
        // Provisioning in the same batch as a change to a row that travels
        // only when pointed at, and to the rows that pointed at it.
        63 => {
            write!(conn, "INSERT INTO sync.notes (partition_id, kind, replica_id) SELECT partition_id, 'resync', id FROM fixture.t_replicas WHERE id = $1", replica);
            write!(conn, "UPDATE fixture.t_kinds SET name = name || '.' WHERE id = $1 AND name <> $2", kind, POISON);
            write!(conn, "UPDATE fixture.t_products SET kind_id = NULL WHERE kind_id = $1", kind);
            write!(conn, "UPDATE fixture.t_phones SET kind_id = NULL WHERE kind_id = $1", kind)
        }
        64 => write!(conn, "UPDATE fixture.t_kinds SET name = $2 WHERE id = $1", kind, POISON),
        65 => write!(conn, "UPDATE fixture.t_sizes SET name = $2 WHERE code = $1", size, POISON),
        66 => write!(conn, "INSERT INTO fixture.t_kinds (id, partition_id, name) VALUES ($1, $2, $3)", new, partition, POISON),
        // Rows the replicas' build refuses: a parent, and rows that follow it.
        67 => write!(conn, "UPDATE fixture.t_customers SET name = $2 WHERE id = $1", customer, REFUSED),
        68 => write!(conn, "UPDATE fixture.t_products SET name = $2 WHERE id = $1", product, REFUSED),
        // A chain of tables that travel only when pointed at.
        69 => write!(conn, "INSERT INTO fixture.t_families (id, partition_id, name) VALUES ($1, $2, 'f')", new, partition),
        70 => write!(conn, "UPDATE fixture.t_kinds SET family_id = $2 WHERE id = $1", kind, family),
        71 => write!(conn, "UPDATE fixture.t_families SET name = name || '.' WHERE id = $1 AND name <> $2", family, POISON),
        72 => write!(conn, &format!("UPDATE fixture.t_families SET {RETIRE} WHERE id = $1"), family),
        73 => write!(conn, "DELETE FROM fixture.t_families WHERE id = $1", family),
        // Rows that point at a follower.
        74 => write!(conn, "INSERT INTO fixture.t_calls (id, partition_id, replica_id, customer_id, number, name) VALUES ($1, $2, $3, $4, $5, 'c')", new, partition, replica, phone.map(|p| p.0), phone.map(|p| p.1)),
        75 => write!(conn, "UPDATE fixture.t_calls SET replica_id = $2 WHERE id = $1", call, replica),
        76 => write!(conn, &format!("UPDATE fixture.t_calls SET {RETIRE} WHERE id = $1"), call),
        77 => write!(conn, "DELETE FROM fixture.t_calls WHERE id = $1", call),
        // Keys that change: a follower's, a parent's of many, an assigned
        // row's, a broadcast row's — what points at them along.
        78 => write!(conn, "UPDATE fixture.t_phones SET number = number + 4 WHERE customer_id = $1 AND number = $2", phone.map(|p| p.0), phone.map(|p| p.1)),
        79 => write!(conn, "UPDATE fixture.t_sizes SET code = code + 1000000 WHERE code = $1", size),
        80 => write!(conn, "UPDATE fixture.t_tasks SET id = id + 1 WHERE id = $1", task),
        81 => write!(conn, "UPDATE fixture.t_bins SET slot = slot + 4 WHERE store = $1 AND slot = $2", bin.map(|b| b.0), bin.map(|b| b.1)),
        // Members and assignments that move.
        82 => write!(conn, "UPDATE fixture.t_list_members SET customer_id = $3 WHERE list_id = $1 AND customer_id = $2", list, customer, other_customer),
        83 => write!(conn, "UPDATE fixture.t_list_members SET list_id = $3 WHERE list_id = $1 AND customer_id = $2", list, customer, other_list),
        84 => write!(conn, "UPDATE fixture.t_replica_lists SET replica_id = $3 WHERE list_id = $1 AND replica_id = $2", list, replica, walk.pick(&replicas)),
        85 => write!(conn, "UPDATE fixture.t_replica_lists SET list_id = $3 WHERE list_id = $1 AND replica_id = $2", list, replica, other_list),
        // A group, its members and its assignments gone at once.
        86 => write!(conn, "DELETE FROM fixture.t_lists WHERE id = $1", list),
        // Rows moved to another partition.
        87 => write!(conn, "UPDATE fixture.t_products SET partition_id = $2 WHERE id = $1", product, other),
        88 => write!(conn, "UPDATE fixture.t_bins SET partition_id = $3 WHERE store = $1 AND slot = $2", bin.map(|b| b.0), bin.map(|b| b.1), other),
        89 => write!(conn, "UPDATE fixture.t_customers SET partition_id = $2 WHERE id = $1", customer, other),
        // A table written on both sides: a write of the replicas' column
        // alone is no change here.
        90 => write!(conn, "INSERT INTO fixture.t_tallies (id, partition_id, kind_id, name) VALUES ($1, $2, $3, 't')", new, partition, kind),
        91 => write!(conn, "UPDATE fixture.t_tallies SET seen = seen + 1 WHERE id = $1", tally),
        92 => write!(conn, "UPDATE fixture.t_tallies SET name = name || '.' WHERE id = $1", tally),
        93 => write!(conn, "DELETE FROM fixture.t_tallies WHERE id = $1", tally),
        // What this build cannot encode, routed, following, mid-chain.
        94 => write!(conn, "UPDATE fixture.t_products SET name = $2 WHERE id = $1", product, POISON),
        95 => write!(conn, "UPDATE fixture.t_emails SET address = $2 WHERE id = $1", email, POISON),
        96 => write!(conn, "UPDATE fixture.t_bins SET name = $3 WHERE store = $1 AND slot = $2", bin.map(|b| b.0), bin.map(|b| b.1), POISON),
        97 => write!(conn, "UPDATE fixture.t_tasks SET name = $2 WHERE id = $1", task, POISON),
        98 => write!(conn, "UPDATE fixture.t_families SET name = $2 WHERE id = $1", family, POISON),
        99 => write!(conn, "UPDATE fixture.t_customers SET name = $2 WHERE id = $1", customer, POISON),
        // Rows no message can carry.
        100 => write!(conn, "UPDATE fixture.t_products SET name = $2 WHERE id = $1", product, &big),
        101 => write!(conn, "UPDATE fixture.t_tasks SET name = $2 WHERE id = $1", task, &big),
        _ => return Step::Cured,
    };
    if done {
        Step::Wrote(choice)
    } else {
        Step::Skipped
    }
}

#[sqlx::test(migrations = false)]
async fn every_replica_holds_exactly_what_its_strategies_say(pool: crate::DbPool) {
    let mut world = world(pool).await;
    for (n, seed) in SEEDS.iter().enumerate() {
        // Every other walk plans in batches that split what was written
        // together.
        world.batch = [100, 3, 1][n % 3];
        walk_from(&mut world, *seed, None).await;
    }
}

/// Whatever goes wrong on the way — sends that fail or are refused for good,
/// a relay that dies between its record and its sends or loses its lock
/// while recording, replicas that take part of what waits for them, die
/// before acknowledging, say nothing or are not heard — every replica holds
/// what the engine records it holding each time the system settles, never
/// a row without its parent on the way, and once the faults stop and the
/// fix is deployed, exactly what it should: with nobody touching the data,
/// and without a provisioning to cover for what went wrong.
#[sqlx::test(migrations = false)]
async fn whatever_fails_on_the_way_every_replica_ends_holding_what_it_should(pool: crate::DbPool) {
    let mut world = world(pool).await;
    for (n, seed) in SEEDS[..3].iter().enumerate() {
        world.batch = [100, 5, 2][n];
        let faults = Faults {
            fail: 20,
            refuse_one_in: 4,
        };
        walk_from(&mut world, *seed, Some(faults)).await;
        let (failed, refused) = *world.wire.failed.lock().unwrap();
        assert!(
            failed > 0 && refused > 0 && world.lost > 0,
            "seed {seed:#x}: the storm was none: {failed} failed, {refused} refused, {} lost",
            world.lost
        );
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

/// Provisionings the walk asked for itself, or that a replica's coming and
/// going asked for: any more were the engine covering for something.
async fn resyncs(conn: &mut Conn) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM sync.notes WHERE kind = 'resync'")
        .fetch_one(conn)
        .await
        .expect("resyncs")
}

async fn walk_from(world: &mut World, seed: u64, faults: Option<Faults>) {
    // What the previous walk left behind: every replica starts from a snapshot.
    sqlx::query(
        "INSERT INTO sync.notes (partition_id, kind, replica_id) \
         SELECT partition_id, 'resync', id FROM fixture.t_replicas",
    )
    .execute(&mut *world.conn)
    .await
    .expect("resync");
    world.settle().await;
    *world.wire.faults.lock().unwrap() = faults.unwrap_or_default();
    let mut walk = Walk(seed);
    let mut storm = Walk(seed ^ 0xFFFF);
    world.history.clear();
    let before = resyncs(&mut world.conn).await;
    let lost = world.lost;
    let mut asked = 0;
    for n in 0..400 {
        let last = n == 399;
        // Now and then a write begins that commits only some passes later.
        if world.writer.is_none() && walk.below(20) == 0 {
            let mut writer = world.pool.begin().await.expect("a long writer");
            sqlx::raw_sql("SET LOCAL lock_timeout = '100ms'")
                .execute(&mut *writer)
                .await
                .expect("a lock timeout");
            world.history.push("a long writer began".to_owned());
            world.writer = Some((writer, 1 + walk.below(4)));
        }
        let wrote = match &mut world.writer {
            Some((writer, _)) if walk.below(2) == 0 => {
                step(&mut *writer, &mut walk, &world.partitions).await
            }
            _ => step(&mut world.conn, &mut walk, &world.partitions).await,
        };
        match wrote {
            Step::Wrote(what) => {
                if (58..=63).contains(&what) {
                    asked += 1;
                }
                world.history.push(what.to_string());
            }
            Step::Skipped => {}
            Step::Cured => {
                world.commit_writer().await;
                cure(&mut world.conn).await;
                world.history.push("the fix".to_owned());
                world.retry_quarantine().await;
            }
        }
        if let Some((_, passes)) = &mut world.writer {
            *passes = passes.saturating_sub(1);
            if *passes == 0 {
                world.commit_writer().await;
            }
        }
        if faults.is_some() {
            world.round(Some(&mut storm)).await;
        } else if walk.below(2) == 0 {
            world.round(None).await;
        }
        // Settled now and then — rarely in a storm, so faults pile up.
        let settles = if faults.is_some() {
            n % 25 == 24
        } else {
            walk.below(3) == 0
        };
        if settles || last {
            if last {
                *world.wire.faults.lock().unwrap() = Faults::default();
                world.commit_writer().await;
                cure(&mut world.conn).await;
                world.settle().await;
                world.retry_quarantine().await;
            }
            world.settle().await;
            world.assert_holdings(seed, n).await;
        }
    }
    let quarantined: i64 = sqlx::query_scalar("SELECT count(*) FROM sync.quarantine")
        .fetch_one(&mut *world.conn)
        .await
        .expect("quarantine");
    assert_eq!(
        quarantined, 0,
        "seed {seed:#x}: still set aside after the fix"
    );
    let resynced = resyncs(&mut world.conn).await - before;
    let lost = world.lost - lost;
    // A replica that missed a message, that the transport refuses again and
    // again, or whose outbox outgrows its bound, is provisioned again: rare,
    // never the rule.
    assert!(
        resynced <= asked * 2 + lost + 3,
        "seed {seed:#x}: {resynced} provisionings for {asked} asked for and {lost} messages lost"
    );
}

#[sqlx::test(migrations = false)]
async fn a_changed_row_goes_again_to_every_replica_holding_it(pool: crate::DbPool) {
    let mut world = world(pool).await;
    let product = Uuid::now_v7();
    sqlx::query("INSERT INTO fixture.t_products (id, partition_id, name) VALUES ($1, $2, 'p')")
        .bind(product)
        .bind(world.partitions[1])
        .execute(&mut *world.conn)
        .await
        .expect("a product");
    world.settle().await;
    sqlx::query("UPDATE fixture.t_products SET name = 'q' WHERE id = $1")
        .bind(product)
        .execute(&mut *world.conn)
        .await
        .expect("a rename");
    world.settle().await;
    let renamed = world
        .replicas
        .values()
        .filter(|m| {
            m.rows
                .get(&("t_products".to_owned(), product.to_string()))
                .is_some_and(|bytes| {
                    serde_json::from_slice::<serde_json::Value>(bytes).expect("JSON")["name"] == "q"
                })
        })
        .count();
    assert_eq!(
        renamed, 2,
        "both live holdings of the partition get the new name"
    );
}

/// The fixture's tables and the engine's schema, and no trigger yet.
async fn bare(pool: &crate::DbPool) -> sqlx::pool::PoolConnection<crate::Db> {
    let mut conn = pool.acquire().await.expect("a connection");
    super::tests::install(&mut conn).await;
    sqlx::raw_sql(SCHEMA)
        .execute(&mut *conn)
        .await
        .expect("schema");
    conn
}

fn refusal(result: Result<Engine, Error>) -> String {
    match result {
        Err(Error::Misuse(why)) => why,
        Err(e) => panic!("refused for another reason: {e}"),
        Ok(_) => panic!("not refused"),
    }
}

#[sqlx::test(migrations = false)]
async fn the_engine_starts_only_on_the_triggers_it_prints(pool: crate::DbPool) {
    let mut conn = bare(&pool).await;
    let codecs = codecs();
    let tables: Vec<&Table> = codecs.iter().map(|c| &c.table).collect();
    let why = refusal(Engine::new(&mut conn, &tables, REPLICAS).await);
    assert!(why.contains("is missing"), "{why}");

    let triggers = Engine::triggers(&mut conn, &codecs, REPLICAS)
        .await
        .expect("the triggers");
    sqlx::raw_sql(&triggers)
        .execute(&mut *conn)
        .await
        .expect("a host's migration");
    Engine::new(&mut conn, &tables, REPLICAS)
        .await
        .expect("the triggers it printed are the ones it needs");

    // Keyed as the primary key orders it, not as the row declares it: every
    // key it noted would be read the other way round.
    sqlx::raw_sql(
        "DROP TRIGGER sync_note_row ON fixture.t_accounts;
         CREATE TRIGGER sync_note_row AFTER INSERT OR UPDATE OR DELETE ON fixture.t_accounts
         FOR EACH ROW EXECUTE FUNCTION sync.note_row('t_accounts', 'partition_id', '2', 'number', 'region', '1', 'size_code');",
    )
    .execute(&mut *conn)
    .await
    .expect("a hand-written trigger");
    let why = refusal(Engine::new(&mut conn, &tables, REPLICAS).await);
    assert!(
        why.contains("differs") && why.contains("t_accounts"),
        "{why}"
    );

    let mut tx = conn.begin().await.expect("a transaction");
    Engine::install(&mut tx, &tables, REPLICAS)
        .await
        .expect("installed");
    tx.commit().await.expect("installed");
    Engine::new(&mut conn, &tables, REPLICAS)
        .await
        .expect("install wrote it as it should be");

    sqlx::raw_sql("ALTER TABLE fixture.t_kinds DISABLE TRIGGER sync_note_row")
        .execute(&mut *conn)
        .await
        .expect("disabled");
    let why = refusal(Engine::new(&mut conn, &tables, REPLICAS).await);
    assert!(
        why.contains("t_kinds"),
        "a disabled trigger notes nothing: {why}"
    );
    sqlx::raw_sql("ALTER TABLE fixture.t_kinds ENABLE REPLICA TRIGGER sync_note_row")
        .execute(&mut *conn)
        .await
        .expect("enabled for replication sessions");
    let why = refusal(Engine::new(&mut conn, &tables, REPLICAS).await);
    assert!(
        why.contains("t_kinds"),
        "a trigger that fires for replication sessions only notes no other write: {why}"
    );
    sqlx::raw_sql("ALTER TABLE fixture.t_kinds ENABLE ALWAYS TRIGGER sync_note_row")
        .execute(&mut *conn)
        .await
        .expect("enabled always");
    Engine::new(&mut conn, &tables, REPLICAS)
        .await
        .expect("a trigger enabled always notes every write");

    let fewer: Vec<&Table> = tables
        .iter()
        .copied()
        .filter(|t| t.name != "t_task_lines")
        .collect();
    let why = refusal(Engine::new(&mut conn, &fewer, REPLICAS).await);
    assert!(
        why.contains("t_task_lines") && why.contains("drop it"),
        "a table noted that no row declares: {why}"
    );
}

/// The triggers the book shows a host's migrations writing for the shop
/// (`book/src/guide/shop-triggers.sql`) are the ones the engine prints for
/// the shop's tables. `MAGIC_SYNC_BLESS=1` writes them anew.
#[sqlx::test(migrations = false)]
async fn the_book_shows_the_triggers_the_engine_prints_for_the_shop(pool: crate::DbPool) {
    /// The shop's product, as `examples/shop/rows` declares it.
    #[derive(sqlx::FromRow, Clone, serde::Serialize, serde::Deserialize, crate::Row)]
    #[row(
        table = "products",
        schema = "shop",
        mirror = "catalog",
        sync = "broadcast",
        retired = "retired_at"
    )]
    struct Product {
        id: Uuid,
        name: String,
        price_cents: i64,
        #[row(version)]
        updated_at: DateTime<Utc>,
    }

    let mut conn = pool.acquire().await.expect("a connection");
    super::tests::install(&mut conn).await;
    let root = env!("CARGO_MANIFEST_DIR");
    let shop = std::fs::read_to_string(format!("{root}/../examples/shop/server/schema.sql"))
        .expect("the shop's schema");
    sqlx::raw_sql(&shop)
        .execute(&mut *conn)
        .await
        .expect("the shop's tables");
    let printed = Engine::triggers(
        &mut conn,
        &[Codec::<()>::json::<Product>(None)],
        Replicas::relation("shop.registers")
            .table("registers")
            .live("active"),
    )
    .await
    .expect("the triggers");
    let path = format!("{root}/../book/src/guide/shop-triggers.sql");
    if std::env::var_os("MAGIC_SYNC_BLESS").is_some() {
        std::fs::write(&path, format!("{printed}\n")).expect("write the book's triggers");
    }
    let shown = std::fs::read_to_string(&path).unwrap_or_default();
    assert!(
        shown == format!("{printed}\n"),
        "book/src/guide/shop-triggers.sql is not what the engine prints for the shop: \
         run this test with MAGIC_SYNC_BLESS=1 and commit the result"
    );
}

#[sqlx::test(migrations = false)]
async fn installs_at_once_run_one_after_the_other(pool: crate::DbPool) {
    let mut conn = bare(&pool).await;
    sqlx::raw_sql("DROP SCHEMA sync CASCADE")
        .execute(&mut *conn)
        .await
        .expect("a database without the engine");
    let install = || async {
        let codecs = codecs();
        let tables: Vec<&Table> = codecs.iter().map(|c| &c.table).collect();
        let mut tx = pool.begin().await.expect("a transaction");
        let engine = Engine::install(&mut tx, &tables, REPLICAS).await;
        tx.commit().await.expect("commit");
        engine.map(|_| ())
    };
    let (a, b) = tokio::join!(install(), install());
    a.expect("one");
    b.expect("the other, after it");
    let notes: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_trigger WHERE tgname = 'sync_note_row' AND tgrelid = 'fixture.t_kinds'::regclass",
    )
    .fetch_one(&mut *conn)
    .await
    .expect("triggers");
    assert_eq!(notes, 1);
}

#[sqlx::test(migrations = false)]
async fn a_schema_from_a_later_build_is_left_alone(pool: crate::DbPool) {
    let mut conn = bare(&pool).await;
    sqlx::raw_sql("UPDATE sync.version SET version = version + 1")
        .execute(&mut *conn)
        .await
        .expect("a later build's schema");
    let codecs = codecs();
    let tables: Vec<&Table> = codecs.iter().map(|c| &c.table).collect();
    let mut tx = conn.begin().await.expect("a transaction");
    let why = refusal(Engine::install(&mut tx, &tables, REPLICAS).await);
    assert!(why.contains("later build"), "{why}");
    drop(tx);
    let why = refusal(Engine::new(&mut conn, &tables, REPLICAS).await);
    assert!(
        why.contains("later build") && !why.contains("install"),
        "not installed over, nor told to be: {why}"
    );
}

#[sqlx::test(migrations = false)]
async fn a_database_without_the_engine_names_both_ways_to_install_it(pool: crate::DbPool) {
    let mut conn = pool.acquire().await.expect("a connection");
    sqlx::raw_sql(SCHEMA)
        .execute(&mut *conn)
        .await
        .expect("schema");
    let codecs = codecs();
    let tables: Vec<&Table> = codecs.iter().map(|c| &c.table).collect();
    let why = refusal(Engine::new(&mut conn, &tables, REPLICAS).await);
    assert!(
        why.contains("not installed")
            && why.contains("Server::install")
            && why.contains("migration"),
        "{why}"
    );
}

/// A host's own schema named `sync` is not installed into, nor read as the
/// engine's: either way the host is told the name is taken.
#[sqlx::test(migrations = false)]
async fn a_schema_of_the_hosts_named_sync_is_named_as_the_trouble(pool: crate::DbPool) {
    let mut conn = pool.acquire().await.expect("a connection");
    sqlx::raw_sql(&format!(
        "DROP SCHEMA IF EXISTS sync CASCADE; CREATE SCHEMA sync; \
         CREATE TABLE sync.jobs (id INTEGER PRIMARY KEY); {SCHEMA}"
    ))
    .execute(&mut *conn)
    .await
    .expect("a host's schema");
    let codecs = codecs();
    let tables: Vec<&Table> = codecs.iter().map(|c| &c.table).collect();
    let mut tx = conn.begin().await.expect("a transaction");
    let why = refusal(Engine::install(&mut tx, &tables, REPLICAS).await);
    assert!(why.contains("schema named `sync`"), "{why}");
    drop(tx);
    let why = refusal(Engine::new(&mut conn, &tables, REPLICAS).await);
    assert!(why.contains("schema named `sync`"), "{why}");
}

#[sqlx::test(migrations = false)]
async fn notes_are_planned_in_one_snapshot_or_not_at_all(pool: crate::DbPool) {
    let mut world = world(pool).await;
    match pending(&mut world.conn, 10).await {
        Err(Error::Misuse(why)) => assert!(why.contains("REPEATABLE READ"), "{why}"),
        other => panic!("read outside a snapshot: {other:?}"),
    }
}

#[sqlx::test(migrations = false)]
async fn notes_this_build_cannot_read_hold_back_none_behind_them(pool: crate::DbPool) {
    let mut world = world(pool).await;
    world.settle().await;
    for _ in 0..5 {
        sqlx::query("INSERT INTO sync.notes (kind) VALUES ('row')")
            .execute(&mut *world.conn)
            .await
            .expect("a note of a later build");
    }
    sqlx::query("INSERT INTO fixture.t_kinds (id, partition_id, name) VALUES ($1, $2, 'k')")
        .bind(Uuid::now_v7())
        .bind(world.partitions[0])
        .execute(&mut *world.conn)
        .await
        .expect("a kind");
    let mut tx = world
        .conn
        .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("a transaction");
    let notes = pending(&mut tx, 3).await.expect("pending");
    assert!(
        matches!(notes.as_slice(), [Note { change: Change::Row { table, .. }, .. }] if table == "t_kinds"),
        "{notes:?}"
    );
}

#[sqlx::test(migrations = false)]
async fn a_note_for_a_table_no_row_declares_is_refused_not_dropped(pool: crate::DbPool) {
    let mut world = world(pool).await;
    let change = Change::Row {
        table: "t_gone".to_owned(),
        key: Uuid::now_v7().to_string(),
        retry: false,
    };
    let mut tx = world
        .conn
        .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("a transaction");
    assert!(matches!(
        world.engine.plan(&mut tx, &[Note { id: 1, change }]).await,
        Err(Error::Misuse(_))
    ));
}

#[sqlx::test(migrations = false)]
async fn a_row_in_two_groups_of_one_replica_is_owed_once(pool: crate::DbPool) {
    let mut world = world(pool).await;
    let partition = world.partitions[0];
    let replica: Uuid = sqlx::query_scalar(
        "SELECT id FROM fixture.t_replicas WHERE partition_id = $1 AND live LIMIT 1",
    )
    .bind(partition)
    .fetch_one(&mut *world.conn)
    .await
    .expect("a replica");
    let customer = Uuid::now_v7();
    sqlx::query("INSERT INTO fixture.t_customers (id, partition_id, name) VALUES ($1, $2, 'c')")
        .bind(customer)
        .bind(partition)
        .execute(&mut *world.conn)
        .await
        .expect("a customer");
    for _ in 0..2 {
        let list = Uuid::now_v7();
        for sql in [
            "INSERT INTO fixture.t_lists (id, partition_id) VALUES ($1, $3)",
            "INSERT INTO fixture.t_list_members (list_id, customer_id, partition_id) VALUES ($1, $2, $3)",
        ] {
            sqlx::query(sql)
                .bind(list)
                .bind(customer)
                .bind(partition)
                .execute(&mut *world.conn)
                .await
                .expect(sql);
        }
        sqlx::query(
            "INSERT INTO fixture.t_replica_lists (list_id, replica_id, partition_id) VALUES ($1, $2, $3)",
        )
        .bind(list)
        .bind(replica)
        .bind(partition)
        .execute(&mut *world.conn)
        .await
        .expect("given");
    }
    let i = world.engine.index("t_customers").expect("customers");
    let wanted = world.engine.wanted_sql(i, "true").expect("a rule");
    let owed: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM ({wanted}) w WHERE w.replica_id = $1"
    ))
    .bind(replica)
    .fetch_one(&mut *world.conn)
    .await
    .expect("owed");
    assert_eq!(owed, 1, "what progress counts a replica owed");
}

/// A row written on both sides, the replicas owning one column.
#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(
    table = "t_counters",
    schema = "fixture",
    mirror = "both",
    sync = "broadcast"
)]
struct Counter {
    id: Uuid,
    partition_id: Uuid,
    kind_id: Option<Uuid>,
    name: String,
    #[row(replica_owned)]
    seen: i32,
}

/// A table partitioned by its rows' partition, as a large one is.
#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(
    table = "t_logs",
    schema = "fixture",
    mirror = "catalog",
    sync = "broadcast"
)]
#[row(key = "id, partition_id")]
struct Log {
    id: Uuid,
    partition_id: Uuid,
    line: String,
}

#[sqlx::test(migrations = false)]
async fn a_table_notes_what_changed_by_the_name_key_and_partition_the_engine_knows(
    pool: crate::DbPool,
) {
    let mut conn = bare(&pool).await;
    let [a, b] = [Uuid::now_v7(), Uuid::now_v7()];
    sqlx::raw_sql(&format!(
        "CREATE TABLE fixture.t_counters (
             id UUID PRIMARY KEY, partition_id UUID NOT NULL,
             kind_id UUID REFERENCES fixture.t_kinds (id), name TEXT NOT NULL, seen INTEGER NOT NULL);
         CREATE TABLE fixture.t_logs (id UUID NOT NULL, partition_id UUID NOT NULL, line TEXT NOT NULL,
             PRIMARY KEY (id, partition_id)) PARTITION BY LIST (partition_id);
         CREATE TABLE fixture.t_logs_a PARTITION OF fixture.t_logs FOR VALUES IN ('{a}');
         CREATE TABLE fixture.t_logs_b PARTITION OF fixture.t_logs FOR VALUES IN ('{b}');"
    ))
    .execute(&mut *conn)
    .await
    .expect("tables");
    let mut codecs = codecs();
    codecs.push(Codec::json::<Counter>(None));
    codecs.push(Codec::json::<Log>(None));
    let tables: Vec<&Table> = codecs.iter().map(|c| &c.table).collect();
    let mut tx = conn.begin().await.expect("a transaction");
    Engine::install(&mut tx, &tables, REPLICAS)
        .await
        .expect("installed");
    tx.commit().await.expect("installed");
    Engine::new(&mut conn, &tables, REPLICAS)
        .await
        .expect("a partition's triggers are its table's");

    let (kind, counter, log) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    sqlx::raw_sql(&format!(
        "INSERT INTO fixture.t_kinds (id, partition_id, name) VALUES ('{kind}', '{a}', 'k');
         INSERT INTO fixture.t_counters VALUES ('{counter}', '{a}', '{kind}', 'c', 0);
         INSERT INTO fixture.t_logs VALUES ('{log}', '{a}', 'l');
         DELETE FROM sync.notes;
         UPDATE fixture.t_counters SET seen = 1;
         UPDATE fixture.t_counters SET name = 'd';
         UPDATE fixture.t_logs SET line = 'm';
         UPDATE fixture.t_counters SET id = '{log}', partition_id = '{b}', kind_id = NULL;"
    ))
    .execute(&mut *conn)
    .await
    .expect("writes");
    let notes: Vec<(String, String, Option<Uuid>, Option<String>)> = sqlx::query_as(
        "SELECT n.relation, n.row_key, n.partition_id, p.relation || ' ' || p.row_key \
           FROM sync.notes n LEFT JOIN sync.pointed p ON p.note = n.id ORDER BY n.id",
    )
    .fetch_all(&mut *conn)
    .await
    .expect("notes");
    let pointed = Some(format!("t_kinds {kind}"));
    assert_eq!(
        notes,
        vec![
            // A write of only what the replicas own is not one here.
            (
                "t_counters".into(),
                counter.to_string(),
                Some(a),
                pointed.clone()
            ),
            // Named as the engine knows it, not by the partition it landed in.
            ("t_logs".into(), format!("{log}|{a}"), Some(a), None),
            // A key that changed: the new one where it is now, the old one
            // where it was, with what the row as it was pointed at.
            ("t_counters".into(), log.to_string(), Some(b), None),
            ("t_counters".into(), counter.to_string(), Some(a), pointed),
        ]
    );
}

/// The reads that ask whether a row, or a replica, has something pending
/// find it by index however many other notes are pending, or done and
/// waiting to be pruned: what a retry notes once, where a row stands, and a
/// replica's provisioning noted once.
#[sqlx::test(migrations = false)]
async fn what_is_pending_is_found_by_index_among_many_notes(pool: crate::DbPool) {
    let mut world = world(pool).await;
    sqlx::raw_sql(
        "INSERT INTO sync.notes (kind, relation, row_key, replica_id, done_at) \
         SELECT CASE WHEN n % 2 = 0 THEN 'row' ELSE 'resync' END, 't_kinds', n::text, \
                gen_random_uuid(), CASE WHEN n % 3 = 0 THEN now() END \
           FROM generate_series(1, 50000) AS n; \
         ANALYZE sync.notes;",
    )
    .execute(&mut *world.conn)
    .await
    .expect("done notes");
    for (sql, index) in [
        (
            "SELECT 1 FROM sync.notes n WHERE n.done_at IS NULL AND n.kind = 'row' \
              AND n.relation = 't_kinds' AND n.row_key = '7'",
            "notes_rows",
        ),
        (
            "SELECT 1 FROM sync.notes p WHERE p.kind = 'resync' \
              AND p.replica_id = gen_random_uuid() AND p.done_at IS NULL",
            "notes_resync",
        ),
    ] {
        let plan: Vec<String> = sqlx::query_scalar(&format!("EXPLAIN {sql}"))
            .fetch_all(&mut *world.conn)
            .await
            .expect("explained");
        assert!(
            plan.iter().any(|line| line.contains(index)),
            "{sql}\n{}",
            plan.join("\n")
        );
    }
}

/// A deploy that changes what places the rows — here, a table mirrored no
/// more — has every live replica provisioned again, once, and what the
/// engine kept of that table let go of; the same declarations again change
/// nothing.
#[sqlx::test(migrations = false)]
async fn a_changed_declaration_provisions_every_replica_again(pool: crate::DbPool) {
    let mut world = world(pool).await;
    let partition = world.partitions[0];
    let task: i64 = 1 << 40;
    let replica: Uuid = sqlx::query_scalar(
        "SELECT id FROM fixture.t_replicas WHERE live AND partition_id = $1 LIMIT 1",
    )
    .bind(partition)
    .fetch_one(&mut *world.conn)
    .await
    .expect("a replica");
    sqlx::raw_sql(&format!(
        "INSERT INTO fixture.t_tasks (id, partition_id, replica_id, name) \
         VALUES ({task}, '{partition}', '{replica}', 't'); \
         INSERT INTO fixture.t_task_lines (task_id, line, partition_id, name) \
         VALUES ({task}, 1, '{partition}', 'l');"
    ))
    .execute(&mut *world.conn)
    .await
    .expect("a task and its line");
    world.settle().await;
    let held_lines = || "SELECT count(*) FROM sync.held WHERE relation = 't_task_lines'";
    async fn resyncs(conn: &mut Conn) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar(
            "SELECT count(*) FROM sync.notes WHERE kind = 'resync' AND done_at IS NULL",
        )
        .fetch_one(conn)
        .await
    }
    let held: i64 = sqlx::query_scalar(held_lines())
        .fetch_one(&mut *world.conn)
        .await
        .expect("held");
    assert_eq!(held, 1);

    let mut tx = world.conn.begin().await.expect("a transaction");
    world.engine.observe(&mut tx).await.expect("observed");
    assert_eq!(
        resyncs(&mut tx).await.expect("resyncs"),
        0,
        "nothing changed"
    );
    tx.commit().await.expect("commit");

    let codecs = codecs();
    let fewer: Vec<&Table> = codecs
        .iter()
        .map(|c| &c.table)
        .filter(|t| t.name != "t_task_lines")
        .collect();
    let mut tx = world.conn.begin().await.expect("a transaction");
    let engine = Engine::install(&mut tx, &fewer, REPLICAS)
        .await
        .expect("installed");
    engine.observe(&mut tx).await.expect("observed");
    let live: i64 = sqlx::query_scalar("SELECT count(*) FROM t_replicas WHERE live")
        .fetch_one(&mut *tx)
        .await
        .expect("live");
    assert_eq!(
        resyncs(&mut tx).await.expect("resyncs"),
        live,
        "every live replica"
    );
    let held: i64 = sqlx::query_scalar(held_lines())
        .fetch_one(&mut *tx)
        .await
        .expect("held");
    assert_eq!(held, 0, "what it kept of a table no longer mirrored");
    engine.observe(&mut tx).await.expect("observed again");
    assert_eq!(resyncs(&mut tx).await.expect("resyncs"), live, "once");
}

/// A declaration of the replicas, or of a table, that the database does not
/// hold as it says is refused when the engine reads it, naming what is
/// wrong — never at the first write, or the first pass.
#[sqlx::test(migrations = false)]
async fn a_declaration_the_database_does_not_hold_is_refused_by_name(pool: crate::DbPool) {
    let mut conn = bare(&pool).await;
    let codecs = codecs();
    let refused = |result: Result<String, Error>| match result {
        Err(Error::Misuse(why)) => why,
        other => panic!("not refused: {other:?}"),
    };
    for (replicas, named) in [
        (
            Replicas::relation("fixture.nowhere").table("t_replicas"),
            "no table or view",
        ),
        (
            Replicas::relation("fixture.t_replicas")
                .table("t_replicas")
                .live("alive"),
            "does not read",
        ),
        (
            Replicas::relation("fixture.t_replicas")
                .table("t_replicas")
                .partitioned_by("retired_at"),
            "no uuid column `retired_at`",
        ),
    ] {
        let why = refused(Engine::triggers(&mut conn, &codecs, replicas).await);
        assert!(why.contains(named), "{why}");
    }
    // A table whose partition column is no uuid would fail every write its
    // trigger notes.
    sqlx::raw_sql(
        "ALTER TABLE fixture.t_list_members ALTER COLUMN partition_id TYPE text; \
         DROP VIEW t_kinds;",
    )
    .execute(&mut *conn)
    .await
    .expect("altered");
    let why = refused(Engine::triggers(&mut conn, &codecs, REPLICAS).await);
    assert!(
        why.contains("t_list_members") && why.contains("not a uuid"),
        "{why}"
    );
    sqlx::raw_sql("ALTER TABLE fixture.t_list_members ALTER COLUMN partition_id TYPE uuid USING partition_id::uuid")
        .execute(&mut *conn)
        .await
        .expect("altered back");
    let why = refused(Engine::triggers(&mut conn, &codecs, REPLICAS).await);
    assert!(
        why.contains("t_kinds") && why.contains("does not read"),
        "an active view that is not there: {why}"
    );
}

/// An update that writes a number at another scale is a change: it reads
/// otherwise on a replica, though PostgreSQL holds the two equal.
#[sqlx::test(migrations = false)]
async fn a_number_written_at_another_scale_is_noted(pool: crate::DbPool) {
    let mut conn = bare(&pool).await;
    sqlx::raw_sql(
        "CREATE TABLE fixture.prices (id INTEGER PRIMARY KEY, amount NUMERIC NOT NULL); \
         CREATE TRIGGER sync_note_row AFTER INSERT OR UPDATE OR DELETE ON fixture.prices \
         FOR EACH ROW EXECUTE FUNCTION sync.note_row('prices', '', '1', 'id', '0'); \
         INSERT INTO fixture.prices VALUES (1, 1.0); \
         DELETE FROM sync.notes; \
         UPDATE fixture.prices SET amount = 1.00; \
         UPDATE fixture.prices SET amount = 1.00;",
    )
    .execute(&mut *conn)
    .await
    .expect("written");
    let noted: i64 = sqlx::query_scalar("SELECT count(*) FROM sync.notes")
        .fetch_one(&mut *conn)
        .await
        .expect("notes");
    assert_eq!(noted, 1, "the new scale, and not the same value again");
}

/// A follower a row of the replica's points at stays with it when its
/// parent leaves the replica's rules, and so does that parent, with its
/// other followers: what points at a row keeps it, followers included, and
/// a follower kept keeps its parent. Once nothing points at them, they go.
#[sqlx::test(migrations = false)]
async fn a_follower_something_points_at_keeps_its_parent(pool: crate::DbPool) {
    let mut world = world(pool).await;
    let partition = world.partitions[0];
    let replica: Uuid = sqlx::query_scalar(
        "SELECT id FROM fixture.t_replicas WHERE partition_id = $1 AND live ORDER BY id LIMIT 1",
    )
    .bind(partition)
    .fetch_one(&mut *world.conn)
    .await
    .expect("a replica");
    let (customer, list, call, email) = (
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
    );
    sqlx::raw_sql(&format!(
        "INSERT INTO fixture.t_customers (id, partition_id, name) VALUES ('{customer}', '{partition}', 'c');
         INSERT INTO fixture.t_emails (id, partition_id, customer_id, address) VALUES ('{email}', '{partition}', '{customer}', 'e');
         INSERT INTO fixture.t_phones (customer_id, number, partition_id) VALUES ('{customer}', 1, '{partition}');
         INSERT INTO fixture.t_lists (id, partition_id) VALUES ('{list}', '{partition}');
         INSERT INTO fixture.t_list_members VALUES ('{list}', '{customer}', '{partition}');
         INSERT INTO fixture.t_replica_lists VALUES ('{list}', '{replica}', '{partition}');
         INSERT INTO fixture.t_calls (id, partition_id, replica_id, customer_id, number, name)
              VALUES ('{call}', '{partition}', '{replica}', '{customer}', 1, 'c');"
    ))
    .execute(&mut *world.conn)
    .await
    .expect("a customer, its email and phone, given to the replica; a call of its to the phone");
    world.settle().await;
    let holds = |world: &World| -> BTreeSet<String> {
        world.replicas[&replica]
            .rows
            .keys()
            .map(|(t, _)| t.clone())
            .collect()
    };
    let all: BTreeSet<String> = ["t_customers", "t_emails", "t_phones", "t_calls"]
        .map(str::to_owned)
        .into();
    assert_eq!(holds(&world), all);

    sqlx::query("DELETE FROM fixture.t_list_members WHERE list_id = $1")
        .bind(list)
        .execute(&mut *world.conn)
        .await
        .expect("the customer leaves the list");
    world.settle().await;
    assert_eq!(
        holds(&world),
        all,
        "the call keeps its phone, the phone its customer"
    );
    world.assert_holdings(0, 0).await;

    sqlx::query("DELETE FROM fixture.t_calls WHERE id = $1")
        .bind(call)
        .execute(&mut *world.conn)
        .await
        .expect("the call goes");
    world.settle().await;
    assert!(holds(&world).is_empty(), "{:?}", holds(&world));
}

/// Each declaration the schema cannot honour is refused when the engine
/// reads it, naming the table and what is wrong.
#[sqlx::test(migrations = false)]
async fn a_declaration_the_schema_cannot_honour_is_refused_naming_it(pool: crate::DbPool) {
    let mut conn = bare(&pool).await;
    let codecs = codecs();
    for (alteration, named) in [
        (
            "ALTER TABLE fixture.t_sizes DROP CONSTRAINT t_sizes_pkey CASCADE; \
             ALTER TABLE fixture.t_sizes ADD PRIMARY KEY (code, partition_id);",
            "`fixture.t_sizes` is mirrored keyed by [\"code\"], which is not its primary key",
        ),
        (
            "ALTER TABLE fixture.t_kinds ADD COLUMN parent_id UUID REFERENCES fixture.t_kinds (id);",
            "`t_kinds` points at itself",
        ),
        (
            "ALTER TABLE fixture.t_families ADD COLUMN kind_id UUID REFERENCES fixture.t_kinds (id);",
            "point at each other in a cycle",
        ),
        (
            "ALTER TABLE fixture.t_emails DROP CONSTRAINT t_emails_customer_id_fkey;",
            "`t_emails` follows `customer_id`, which is not a key into a mirrored table",
        ),
        (
            "ALTER TABLE fixture.t_customers ADD UNIQUE (name); \
             ALTER TABLE fixture.t_orders ADD COLUMN customer_name TEXT \
                 REFERENCES fixture.t_customers (name);",
            "`t_orders` points at `t_customers` by [\"name\"], which is not its key",
        ),
        (
            "ALTER TABLE fixture.t_calls DROP COLUMN replica_id CASCADE; \
             CREATE VIEW t_calls AS SELECT * FROM fixture.t_calls WHERE retired_at IS NULL;",
            "`t_calls` is sent by `replica_id`, which it does not have",
        ),
        (
            "ALTER TABLE fixture.t_products DROP COLUMN partition_id CASCADE; \
             CREATE VIEW t_products AS SELECT * FROM fixture.t_products WHERE retired_at IS NULL;",
            "`t_products` is sent by `partition_id`, which it does not have",
        ),
        (
            "ALTER TABLE fixture.t_list_members DROP CONSTRAINT t_list_members_customer_id_fkey;",
            "`t_customers` is grouped by `t_list_members` and `t_replica_lists`, but no member key points at it",
        ),
        (
            "ALTER TABLE fixture.t_replica_lists DROP CONSTRAINT t_replica_lists_list_id_fkey;",
            "the two do not point at one table of groups",
        ),
    ] {
        sqlx::raw_sql(&format!("DROP SCHEMA fixture CASCADE; {SCHEMA} {alteration}"))
            .execute(&mut *conn)
            .await
            .expect(alteration);
        match Engine::triggers(&mut conn, &codecs, REPLICAS).await {
            Err(Error::Misuse(why)) => assert!(why.contains(named), "{alteration}\n{why}"),
            other => panic!("{alteration}: not refused: {other:?}"),
        }
    }
}

/// A table mirrored for the first time that already has rows reached no
/// replica: installing its trigger provisions every live replica, once.
#[sqlx::test(migrations = false)]
async fn a_table_mirrored_for_the_first_time_with_rows_provisions_every_replica(
    pool: crate::DbPool,
) {
    let mut world = world(pool).await;
    world.settle().await;
    sqlx::raw_sql(
        "DROP TRIGGER sync_note_row ON fixture.t_sizes; \
         INSERT INTO fixture.t_sizes (code, partition_id, name) VALUES (7, gen_random_uuid(), 's');",
    )
    .execute(&mut *world.conn)
    .await
    .expect("a size written before its table was noted");
    let codecs = codecs();
    let tables: Vec<&Table> = codecs.iter().map(|c| &c.table).collect();
    let pending = || "SELECT count(*) FROM sync.notes WHERE kind = 'resync' AND done_at IS NULL";
    for _ in 0..2 {
        let mut tx = world.conn.begin().await.expect("a transaction");
        Engine::install(&mut tx, &tables, REPLICAS)
            .await
            .expect("installed");
        tx.commit().await.expect("committed");
    }
    let resyncs: i64 = sqlx::query_scalar(pending())
        .fetch_one(&mut *world.conn)
        .await
        .expect("resyncs");
    let live: i64 = sqlx::query_scalar("SELECT count(*) FROM t_replicas WHERE live")
        .fetch_one(&mut *world.conn)
        .await
        .expect("live");
    assert_eq!(resyncs, live, "every live replica, once");
}

/// A trigger disabled for a while let writes by unnoted: installing it again
/// provisions every replica again, which gets them.
#[sqlx::test(migrations = false)]
async fn a_trigger_enabled_again_by_install_provisions_every_replica(pool: crate::DbPool) {
    let mut world = world(pool).await;
    world.settle().await;
    sqlx::raw_sql(
        "ALTER TABLE fixture.t_sizes DISABLE TRIGGER sync_note_row; \
         INSERT INTO fixture.t_sizes (code, partition_id, name) VALUES (7, gen_random_uuid(), 's');",
    )
    .execute(&mut *world.conn)
    .await
    .expect("a size written while its table was not noted");
    let codecs = codecs();
    let tables: Vec<&Table> = codecs.iter().map(|c| &c.table).collect();
    let mut tx = world.conn.begin().await.expect("a transaction");
    Engine::install(&mut tx, &tables, REPLICAS)
        .await
        .expect("installed");
    tx.commit().await.expect("committed");
    let resyncs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sync.notes WHERE kind = 'resync' AND done_at IS NULL",
    )
    .fetch_one(&mut *world.conn)
    .await
    .expect("resyncs");
    let live: i64 = sqlx::query_scalar("SELECT count(*) FROM t_replicas WHERE live")
        .fetch_one(&mut *world.conn)
        .await
        .expect("live");
    assert_eq!(resyncs, live, "every live replica");
}

/// A replica asked to be provisioned again — by the host, by the relay —
/// is noted once while one is pending, in its partition, and again once
/// that one is done.
#[sqlx::test(migrations = false)]
async fn a_replica_is_provisioned_once_however_often_it_is_asked(pool: crate::DbPool) {
    let mut world = world(pool).await;
    world.settle().await;
    let (replica, partition): (Uuid, Uuid) =
        sqlx::query_as("SELECT id, partition_id FROM fixture.t_replicas WHERE live LIMIT 1")
            .fetch_one(&mut *world.conn)
            .await
            .expect("a replica");
    async fn noted(conn: &mut Conn, replica: Uuid) -> Result<(i64, Option<Uuid>), sqlx::Error> {
        sqlx::query_as(
            "SELECT count(*), min(partition_id::text)::uuid FROM sync.notes \
              WHERE kind = 'resync' AND replica_id = $1 AND done_at IS NULL",
        )
        .bind(replica)
        .fetch_one(conn)
        .await
    }
    note_resync(&mut world.conn, Some(partition), replica)
        .await
        .expect("noted");
    note_resync(&mut world.conn, Some(partition), replica)
        .await
        .expect("noted again");
    assert!(
        !crate::relay::resync(&mut world.conn, replica)
            .await
            .expect("asked by the relay"),
        "one is pending"
    );
    assert_eq!(
        noted(&mut world.conn, replica).await.expect("notes"),
        (1, Some(partition))
    );
    world.settle().await;
    assert!(crate::relay::resync(&mut world.conn, replica)
        .await
        .expect("asked again"));
    assert_eq!(
        noted(&mut world.conn, replica).await.expect("notes"),
        (1, Some(partition)),
        "in its partition"
    );
}

/// Done notes older than the age kept are pruned; newer ones, and every
/// pending one, are kept.
#[sqlx::test(migrations = false)]
async fn done_notes_are_pruned_once_older_than_kept(pool: crate::DbPool) {
    let mut world = world(pool).await;
    sqlx::raw_sql(
        "DELETE FROM sync.notes; \
         INSERT INTO sync.notes (kind, replica_id, done_at) VALUES \
           ('resync', gen_random_uuid(), now() - interval '2 hours'), \
           ('resync', gen_random_uuid(), now() - interval '2 hours'), \
           ('resync', gen_random_uuid(), now() - interval '30 minutes'), \
           ('resync', gen_random_uuid(), NULL);",
    )
    .execute(&mut *world.conn)
    .await
    .expect("notes");
    for left in [1, 1, 0] {
        let pruned = prune(&mut world.conn, Duration::from_secs(3600), 1)
            .await
            .expect("pruned");
        assert_eq!(pruned, left, "one at a time, the oldest first");
    }
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM sync.notes")
        .fetch_one(&mut *world.conn)
        .await
        .expect("left");
    assert_eq!(left, 2, "the newer done one, and the pending one");
}

/// A table that only travels with others is owed what the engine decided
/// to send: progress counts what each replica holds of it, and what is set
/// aside for it there besides.
#[sqlx::test(migrations = false)]
async fn progress_counts_a_table_with_no_rule_by_what_was_decided(pool: crate::DbPool) {
    let mut world = world(pool).await;
    let partition = world.partitions[0];
    let (kind, other) = (Uuid::now_v7(), Uuid::now_v7());
    sqlx::raw_sql(&format!(
        "INSERT INTO fixture.t_kinds (id, partition_id, name) VALUES ('{kind}', '{partition}', 'k'), \
                ('{other}', '{partition}', 'k'); \
         INSERT INTO fixture.t_products (id, partition_id, kind_id, name) \
         VALUES (gen_random_uuid(), '{partition}', '{kind}', 'p'), (gen_random_uuid(), '{partition}', '{other}', 'p');"
    ))
    .execute(&mut *world.conn)
    .await
    .expect("two kinds, each a product's");
    world.settle().await;
    let replica: Uuid = sqlx::query_scalar(
        "SELECT id FROM fixture.t_replicas WHERE partition_id = $1 AND live ORDER BY id LIMIT 1",
    )
    .bind(partition)
    .fetch_one(&mut *world.conn)
    .await
    .expect("a replica");
    let kinds = |progress: crate::status::Progress| {
        progress.replicas[0]
            .tables
            .iter()
            .find(|t| t.table == "t_kinds")
            .map(|t| (t.should, t.holds, t.quarantined))
            .expect("kinds")
    };
    let scope = [replica];
    let progress = world
        .engine
        .progress(&mut world.conn, crate::status::Scope::Replicas(&scope))
        .await
        .expect("progress");
    assert_eq!(kinds(progress), (2, 2, 0));
    sqlx::query("UPDATE fixture.t_kinds SET name = $2 WHERE id = $1")
        .bind(other)
        .bind(POISON)
        .execute(&mut *world.conn)
        .await
        .expect("a kind this build cannot encode");
    world.settle().await;
    let progress = world
        .engine
        .progress(&mut world.conn, crate::status::Scope::Replicas(&scope))
        .await
        .expect("progress");
    assert_eq!(kinds(progress), (2, 1, 1), "one held, one set aside");
}

/// The relay plans in temporary tables: a role that may not create them is
/// refused at start, rather than at every pass.
#[sqlx::test(migrations = false)]
async fn a_role_that_may_not_create_temporary_tables_is_refused_at_start(pool: crate::DbPool) {
    let mut world = world(pool).await;
    let role = format!("sync_no_temp_{}", Uuid::now_v7().simple());
    sqlx::raw_sql(&format!(
        "CREATE ROLE {role} NOLOGIN; \
         GRANT USAGE ON SCHEMA fixture, sync TO {role}; \
         GRANT SELECT ON ALL TABLES IN SCHEMA fixture, sync, public TO {role}; \
         DO $$ BEGIN EXECUTE format('REVOKE TEMPORARY ON DATABASE %I FROM PUBLIC', current_database()); END $$; \
         SET ROLE {role};"
    ))
    .execute(&mut *world.conn)
    .await
    .expect("a role that may not create temporary tables");
    let codecs = codecs();
    let tables: Vec<&Table> = codecs.iter().map(|c| &c.table).collect();
    let why = refusal(Engine::new(&mut world.conn, &tables, REPLICAS).await);
    sqlx::raw_sql(&format!(
        "RESET ROLE; DROP OWNED BY {role}; DROP ROLE {role};"
    ))
    .execute(&mut *world.conn)
    .await
    .expect("the role dropped");
    assert!(why.contains("TEMPORARY"), "{why}");
}

/// The live replicas of `partition`.
async fn live_in(conn: &mut Conn, partition: Uuid) -> Vec<Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM fixture.t_replicas WHERE live AND partition_id = $1 ORDER BY id",
    )
    .bind(partition)
    .fetch_all(conn)
    .await
    .expect("replicas")
}

/// Two writers of one row commit in another order than they took their
/// transaction ids, and a pass in between sends the row as the first commit
/// left it: the second commit goes too.
#[sqlx::test(migrations = false)]
async fn a_row_written_last_by_the_older_transaction_goes_again(pool: crate::DbPool) {
    let mut world = world(pool.clone()).await;
    let partition = world.partitions[1];
    let product = Uuid::now_v7();
    sqlx::query("INSERT INTO fixture.t_products (id, partition_id, name) VALUES ($1, $2, 'p')")
        .bind(product)
        .bind(partition)
        .execute(&mut *world.conn)
        .await
        .expect("a product");
    world.settle().await;
    let mut older = pool.begin().await.expect("a transaction");
    sqlx::query("SELECT pg_current_xact_id()")
        .execute(&mut *older)
        .await
        .expect("the older transaction's id");
    let replica = live_in(&mut world.conn, partition).await[0];
    note_resync(&mut world.conn, Some(partition), replica)
        .await
        .expect("a provisioning");
    sqlx::query("UPDATE fixture.t_products SET name = 'newer' WHERE id = $1")
        .bind(product)
        .execute(&mut *world.conn)
        .await
        .expect("the newer transaction's write");
    // A pass that plans the provisioning alone sends the row as it is now.
    world.batch = 1;
    world.round(None).await;
    world.batch = 100;
    sqlx::query("UPDATE fixture.t_products SET name = 'older' WHERE id = $1")
        .bind(product)
        .execute(&mut *older)
        .await
        .expect("the older transaction's write");
    older.commit().await.expect("committed");
    world.settle().await;
    world.assert_holdings(0, 0).await;
}

/// `url` naming the database `database` instead.
fn on_database(url: &str, database: &str) -> String {
    let (base, query) = url
        .split_once('?')
        .map_or((url, None), |(b, q)| (b, Some(q)));
    let (server, _) = base.rsplit_once('/').expect("a URL with a database");
    match query {
        Some(query) => format!("{server}/{database}?{query}"),
        None => format!("{server}/{database}"),
    }
}

/// The server's database moved to another cluster between two passes —
/// `pg_dump` into `pg_restore`, as a migration or a restore from a logical
/// backup moves it — keeps sending what changes, whatever the transaction
/// ids of the two clusters. `scripts/moved-database.sh` runs it between two
/// clusters, the first one's transaction ids far ahead of the second's;
/// `MAGIC_SYNC_MOVED_URL` names the second.
#[sqlx::test(migrations = false)]
#[ignore = "needs a second cluster: scripts/moved-database.sh"]
async fn a_database_moved_to_another_cluster_keeps_sending_what_changes(pool: crate::DbPool) {
    let moved = std::env::var("MAGIC_SYNC_MOVED_URL").expect("MAGIC_SYNC_MOVED_URL");
    let from = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let mut world = world(pool).await;
    let product = Uuid::now_v7();
    sqlx::query("INSERT INTO fixture.t_products (id, partition_id, name) VALUES ($1, $2, 'p')")
        .bind(product)
        .bind(world.partitions[1])
        .execute(&mut *world.conn)
        .await
        .expect("a product");
    world.settle().await;
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut *world.conn)
        .await
        .expect("its name");
    let target = format!("moved_{}", Uuid::now_v7().simple());
    let mut admin = sqlx::PgConnection::connect(&moved)
        .await
        .expect("the second cluster");
    sqlx::raw_sql(&format!("CREATE DATABASE {target}"))
        .execute(&mut admin)
        .await
        .expect("a database to move to");
    let mut dump = std::process::Command::new("pg_dump")
        .args([
            "--format=custom",
            "--dbname",
            &on_database(&from, &database),
        ])
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("pg_dump");
    let restored = std::process::Command::new("pg_restore")
        .args([
            "--no-owner",
            "--exit-on-error",
            "--dbname",
            &on_database(&moved, &target),
        ])
        .stdin(dump.stdout.take().expect("the dump"))
        .status()
        .expect("pg_restore");
    let dumped = dump.wait().expect("pg_dump");
    assert!(
        dumped.success() && restored.success(),
        "pg_dump | pg_restore failed"
    );
    world.pool = crate::DbPool::connect(&on_database(&moved, &target))
        .await
        .expect("the moved database");
    world.conn = world.pool.acquire().await.expect("a connection");
    sqlx::raw_sql("SET lock_timeout = '100ms'")
        .execute(&mut *world.conn)
        .await
        .expect("a lock timeout");
    let tables: Vec<&Table> = world.codecs.iter().map(|c| &c.table).collect();
    world.engine = Engine::new(&mut world.conn, &tables, REPLICAS)
        .await
        .expect("the engine, moved");
    sqlx::query("UPDATE fixture.t_products SET name = 'moved' WHERE id = $1")
        .bind(product)
        .execute(&mut *world.conn)
        .await
        .expect("a rename");
    world.settle().await;
    world.assert_holdings(0, 0).await;
    drop(world);
    sqlx::raw_sql(&format!("DROP DATABASE {target} WITH (FORCE)"))
        .execute(&mut admin)
        .await
        .expect("the moved database dropped");
}

/// A replica that stops being live after the relay looked at the replicas,
/// and is live again before it looks next, misses nothing it was sent in
/// between: a pass sends to the replicas as the relay last saw them.
#[sqlx::test(migrations = false)]
async fn a_replica_live_again_before_it_was_seen_gone_misses_nothing(pool: crate::DbPool) {
    let mut world = world(pool).await;
    let partition = world.partitions[1];
    let product = Uuid::now_v7();
    sqlx::query("INSERT INTO fixture.t_products (id, partition_id, name) VALUES ($1, $2, 'p')")
        .bind(product)
        .bind(partition)
        .execute(&mut *world.conn)
        .await
        .expect("a product");
    world.settle().await;
    let replica = live_in(&mut world.conn, partition).await[0];
    let mut tx = world.conn.begin().await.expect("a transaction");
    world.engine.observe(&mut tx).await.expect("observed");
    tx.commit().await.expect("commit");
    for sql in [
        "UPDATE fixture.t_replicas SET live = false WHERE id = $1",
        "UPDATE fixture.t_products SET name = 'q' WHERE partition_id = (SELECT partition_id FROM fixture.t_replicas WHERE id = $1)",
    ] {
        sqlx::query(sql)
            .bind(replica)
            .execute(&mut *world.conn)
            .await
            .expect("a write");
    }
    let relay = relay(&world.engine, &world.codecs, world.batch);
    let mut tx = world
        .conn
        .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("a transaction");
    relay.record(&mut tx).await.expect("recorded");
    tx.commit().await.expect("commit");
    relay
        .publish(&mut world.conn, &world.wire)
        .await
        .expect("published");
    sqlx::query("UPDATE fixture.t_replicas SET live = true WHERE id = $1")
        .bind(replica)
        .execute(&mut *world.conn)
        .await
        .expect("live again");
    world.settle().await;
    world.assert_holdings(0, 0).await;
}

/// What progress reads of each table is joined as sets — what the rules
/// send, what is held, what is pending — never asked row by row: a read
/// whose cost grows with the rows held times anything is a read that does
/// not finish over a large fleet.
#[sqlx::test(migrations = false)]
async fn progress_asks_nothing_row_by_row(pool: crate::DbPool) {
    let mut world = world(pool).await;
    sqlx::query("INSERT INTO fixture.t_products (id, partition_id, name) VALUES ($1, $2, 'p')")
        .bind(Uuid::now_v7())
        .bind(world.partitions[0])
        .execute(&mut *world.conn)
        .await
        .expect("a product");
    world.settle().await;
    sqlx::query("UPDATE fixture.t_products SET name = 'q'")
        .execute(&mut *world.conn)
        .await
        .expect("a change pending");
    let replicas: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM fixture.t_replicas")
        .fetch_all(&mut *world.conn)
        .await
        .expect("replicas");
    for i in 0..world.engine.plans.len() {
        let (sql, ruled) = world.engine.progress_sql(i);
        let plan: Vec<String> = sqlx::query_scalar(&format!("EXPLAIN {sql}"))
            .bind(world.engine.table(i))
            .bind(&replicas)
            .bind(ruled)
            .fetch_all(&mut *world.conn)
            .await
            .expect("a plan");
        let plan = plan.join("\n");
        assert!(
            !plan.contains("SubPlan"),
            "progress of {} asks row by row:\n{plan}",
            world.engine.table(i)
        );
    }
}

/// A pass loads and encodes as many rows at once as fit a byte budget, so
/// what it holds stays bounded however large the rows: never none.
#[sqlx::test(migrations = false)]
async fn rows_are_loaded_as_many_at_once_as_fit_the_budget(pool: crate::DbPool) {
    let mut world = world(pool).await;
    let i = world.engine.index("t_products").expect("products");
    let mut keys = Vec::new();
    for _ in 0..3 {
        let id = Uuid::now_v7();
        // Random, so it does not compress: a megabyte stored.
        sqlx::query(
            "INSERT INTO fixture.t_products (id, partition_id, name) \
             SELECT $1, $2, string_agg(md5(random()::text), '') FROM generate_series(1, 32768)",
        )
        .bind(id)
        .bind(world.partitions[0])
        .execute(&mut *world.conn)
        .await
        .expect("a large product");
        keys.push(id.to_string());
    }
    for (budget, fit) in [(2_500_000, 2), (10, 1)] {
        let fitting = world.engine.fitting(&mut world.conn, i, &keys, budget);
        assert_eq!(
            fitting.await.expect("fits"),
            fit,
            "{budget} bytes: never none"
        );
    }
}

/// What the server keeps of a replica's uploads, unable to file them, is
/// bounded by its bytes as well as by its parts: a replica sending a few
/// large ones is held back, as one sending many small ones is.
#[sqlx::test(migrations = false)]
async fn a_replicas_inbox_is_bounded_by_its_bytes(pool: crate::DbPool) {
    let mut world = world(pool).await;
    let replica = live_in(&mut world.conn, world.partitions[0]).await[0];
    sqlx::query(
        "INSERT INTO sync.inbox (replica_id, upload, reason) \
         VALUES ($1, convert_to(repeat('x', 64 << 20), 'UTF8'), 'a bug')",
    )
    .bind(replica)
    .execute(&mut *world.conn)
    .await
    .expect("a large part kept");
    let mut tx = world.conn.begin().await.expect("a transaction");
    let kept = crate::filing::keep_whole(&mut tx, replica, b"more", "a bug", i64::MAX).await;
    assert!(matches!(&kept, Err(e) if e.is_transient()), "{kept:?}");
}

/// What a replica says — a reason, a table's name — is kept at most 4 KiB
/// of it, cut between characters, however long it says it.
#[test]
fn text_a_replica_says_is_kept_bounded() {
    let said = format!("{}é{}", "x".repeat(4095), "y".repeat(1 << 20));
    let kept = crate::relay::stored(&said);
    assert_eq!(kept.len(), 4095, "cut before the character that crosses");
    assert_eq!(crate::relay::stored("a\0b"), "a\u{FFFD}b");
}

/// How a codec writes a row is part of what decides what a replica holds:
/// a deploy that writes a table's rows otherwise declares otherwise, and
/// every replica is provisioned again.
#[sqlx::test(migrations = false)]
async fn a_codec_that_writes_rows_otherwise_declares_otherwise(pool: crate::DbPool) {
    let mut world = world(pool).await;
    let codecs = codecs();
    let mut tables: Vec<&Table> = codecs.iter().map(|c| &c.table).collect();
    let before = Engine::new(&mut world.conn, &tables, REPLICAS)
        .await
        .expect("an engine")
        .declared();
    let families = Table {
        encoding: "fixture.Family",
        ..Table::of::<Family>()
    };
    for table in &mut tables {
        if table.name == families.name {
            *table = &families;
        }
    }
    let after = Engine::new(&mut world.conn, &tables, REPLICAS)
        .await
        .expect("an engine")
        .declared();
    assert_ne!(before, after);
}
