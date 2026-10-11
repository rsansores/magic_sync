//! The engine against a schema that has every strategy, keyed every way a key
//! can be — a uuid, an integer, several columns — driven by a seeded random
//! walk of the writes an office makes and of what happens to its replicas.
//!
//! Each replica is modelled as what it was sent: every row it holds, as the
//! bytes that carried it. Passes run as the relay runs them — the notes read
//! and planned in one transaction, the rows loaded in it, with another
//! connection writing in between — and what each replica is sent is applied
//! to its model. After every delivery a replica never holds a row whose parent
//! it lacks; after the outbox drains, each live replica holds exactly what its
//! strategies say, every row as it is now, and the engine records exactly
//! that. What each replica should hold is computed here from the rows,
//! independently of the engine.

use super::*;
use crate::testkit::Walk;
use chrono::{DateTime, Utc};
use sqlx::Connection as _;

pub(super) const SCHEMA: &str = "
CREATE SCHEMA fixture;
CREATE SEQUENCE fixture.numbers;
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
    customer_id UUID REFERENCES fixture.t_customers (id), name TEXT NOT NULL DEFAULT 'o',
    retired_at TIMESTAMPTZ);
CREATE TABLE fixture.t_sizes (
    code INTEGER PRIMARY KEY, partition_id UUID NOT NULL, name TEXT NOT NULL,
    retired_at TIMESTAMPTZ);
CREATE TABLE fixture.t_bins (
    store INTEGER NOT NULL, slot INTEGER NOT NULL, partition_id UUID NOT NULL,
    size_code INTEGER REFERENCES fixture.t_sizes (code), name TEXT NOT NULL,
    retired_at TIMESTAMPTZ, PRIMARY KEY (store, slot));
-- Keyed (region, number) by its row, and by its primary key the other way
-- round: the row's order is the one keys are written in.
CREATE TABLE fixture.t_accounts (
    region INTEGER NOT NULL, number BIGINT NOT NULL, partition_id UUID NOT NULL,
    size_code INTEGER REFERENCES fixture.t_sizes (code), name TEXT NOT NULL,
    retired_at TIMESTAMPTZ, PRIMARY KEY (number, region));
CREATE TABLE fixture.t_account_members (
    list_id UUID NOT NULL REFERENCES fixture.t_lists (id),
    region INTEGER NOT NULL, number BIGINT NOT NULL, partition_id UUID NOT NULL,
    FOREIGN KEY (number, region) REFERENCES fixture.t_accounts (number, region),
    PRIMARY KEY (list_id, region, number));
CREATE TABLE fixture.t_tasks (
    id BIGINT PRIMARY KEY, partition_id UUID NOT NULL,
    replica_id UUID REFERENCES fixture.t_replicas (id),
    store INTEGER, slot INTEGER, region INTEGER, number BIGINT,
    name TEXT NOT NULL, retired_at TIMESTAMPTZ,
    FOREIGN KEY (store, slot) REFERENCES fixture.t_bins (store, slot),
    FOREIGN KEY (number, region) REFERENCES fixture.t_accounts (number, region));
CREATE TABLE fixture.t_task_lines (
    task_id BIGINT NOT NULL REFERENCES fixture.t_tasks (id), line INTEGER NOT NULL,
    partition_id UUID NOT NULL, size_code INTEGER REFERENCES fixture.t_sizes (code),
    name TEXT NOT NULL, retired_at TIMESTAMPTZ, PRIMARY KEY (task_id, line));
CREATE VIEW t_replicas AS SELECT * FROM fixture.t_replicas WHERE retired_at IS NULL;
CREATE VIEW t_kinds AS SELECT * FROM fixture.t_kinds WHERE retired_at IS NULL;
CREATE VIEW t_products AS SELECT * FROM fixture.t_products WHERE retired_at IS NULL;
CREATE VIEW t_customers AS SELECT * FROM fixture.t_customers WHERE retired_at IS NULL;
CREATE VIEW t_emails AS SELECT * FROM fixture.t_emails WHERE retired_at IS NULL;
CREATE VIEW t_orders AS SELECT * FROM fixture.t_orders WHERE retired_at IS NULL;
CREATE VIEW t_phones AS SELECT * FROM fixture.t_phones WHERE retired_at IS NULL;
CREATE VIEW t_sizes AS SELECT * FROM fixture.t_sizes WHERE retired_at IS NULL;
CREATE VIEW t_bins AS SELECT * FROM fixture.t_bins WHERE retired_at IS NULL;
CREATE VIEW t_accounts AS SELECT * FROM fixture.t_accounts WHERE retired_at IS NULL;
CREATE VIEW t_tasks AS SELECT * FROM fixture.t_tasks WHERE retired_at IS NULL;
CREATE VIEW t_task_lines AS SELECT * FROM fixture.t_task_lines WHERE retired_at IS NULL;
";

/// A name this build cannot encode: the bug a deploy fixes.
const POISON: &str = "poison";

fn poisoned<S: serde::Serializer>(name: &str, s: S) -> Result<S::Ok, S::Error> {
    if name == POISON {
        return Err(serde::ser::Error::custom(
            "a value this build cannot encode",
        ));
    }
    s.serialize_str(name)
}

#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(table = "t_kinds", schema = "fixture", mirror = "catalog")]
#[row(active = "t_kinds", retired = "retired_at")]
struct Kind {
    id: Uuid,
    partition_id: Uuid,
    #[serde(serialize_with = "poisoned")]
    name: String,
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
    name: String,
    retired_at: Option<DateTime<Utc>>,
}

#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(table = "t_customers", schema = "fixture", mirror = "catalog")]
#[row(sync = "grouped(members = t_list_members, assigned = t_replica_lists)")]
#[row(active = "t_customers", retired = "retired_at")]
struct Customer {
    id: Uuid,
    partition_id: Uuid,
    name: String,
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
    address: String,
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
    name: String,
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
    name: String,
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
    retired_at: Option<DateTime<Utc>>,
}

pub(super) fn codecs() -> Vec<Codec<()>> {
    vec![
        Codec::json::<Phone>(None),
        Codec::json::<Order>(None),
        Codec::json::<Email>(None),
        Codec::json::<Customer>(None),
        Codec::json::<Product>(None),
        Codec::json::<Kind>(None),
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
    ("t_products", &["kind_id"], "t_kinds"),
    ("t_phones", &["customer_id"], "t_customers"),
    ("t_phones", &["kind_id"], "t_kinds"),
    ("t_emails", &["customer_id"], "t_customers"),
    ("t_orders", &["customer_id"], "t_customers"),
    ("t_bins", &["size_code"], "t_sizes"),
    ("t_accounts", &["size_code"], "t_sizes"),
    ("t_tasks", &["store", "slot"], "t_bins"),
    ("t_tasks", &["region", "number"], "t_accounts"),
    ("t_task_lines", &["task_id"], "t_tasks"),
    ("t_task_lines", &["size_code"], "t_sizes"),
];

/// A replica as the engine told it: each row it holds, as it was sent.
type Held = BTreeMap<(String, String), serde_json::Value>;

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
    Wrote(String),
    Skipped,
    /// The bug is fixed: what was set aside goes again.
    Cured,
}

struct World {
    pool: crate::DbPool,
    conn: sqlx::pool::PoolConnection<crate::Db>,
    engine: Engine,
    codecs: Vec<Codec<()>>,
    partitions: Vec<Uuid>,
    held: BTreeMap<Uuid, Held>,
    /// Rows the engine could not send a replica, until the fix.
    quarantined: BTreeSet<(Uuid, String, String)>,
    history: Vec<String>,
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
    tx.commit().await.expect("installed");
    World {
        pool,
        conn,
        engine,
        codecs,
        partitions,
        held: BTreeMap::new(),
        quarantined: BTreeSet::new(),
        history: Vec::new(),
    }
}

impl World {
    /// Drains the outbox as the relay does, a batch at a time — what became of
    /// the replicas looked at first — into the
    /// modelled replicas; when `walk` is given, another connection writes
    /// between the plan and the load of some batches. Answers the deltas.
    async fn sync(&mut self, mut walk: Option<&mut Walk>) -> Vec<Delta> {
        let mut deltas = Vec::new();
        loop {
            let mut tx = self.conn.begin().await.expect("a transaction");
            self.engine.observe(&mut tx).await.expect("observed");
            tx.commit().await.expect("commit");
            let mut tx = self
                .conn
                .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ")
                .await
                .expect("a transaction");
            let notes = pending(&mut tx, 1000).await.expect("pending");
            if notes.is_empty() {
                return deltas;
            }
            let live: BTreeSet<Uuid> = sqlx::query_scalar("SELECT id FROM t_replicas WHERE live")
                .fetch_all(&mut *tx)
                .await
                .expect("live")
                .into_iter()
                .collect();
            let mut by_scope: BTreeMap<Option<Uuid>, Vec<Note>> = BTreeMap::new();
            for note in notes {
                by_scope.entry(note.scope).or_default().push(note);
            }
            let mut planned = Vec::new();
            for (scope, notes) in by_scope {
                let changes: Vec<Change> = notes.iter().map(|n| n.change.clone()).collect();
                let mut delta = self.engine.plan(&mut tx, &changes).await.expect("plan");
                if let Some(replica) = scope {
                    delta.keep_only(replica);
                }
                let dead: Vec<Uuid> = Engine::replicas(&delta)
                    .difference(&live)
                    .copied()
                    .collect();
                assert!(
                    dead.is_empty(),
                    "{dead:?} are not live and are sent something; last steps: {:#?}",
                    &self.history[self.history.len().saturating_sub(8)..]
                );
                planned.push((notes, delta));
            }
            if let Some(walk) = walk.as_deref_mut() {
                if walk.below(2) == 0 {
                    let mut other = self.pool.acquire().await.expect("a second connection");
                    if let Step::Wrote(what) = step(&mut other, walk, &self.partitions).await {
                        self.history.push(format!("between plan and load: {what}"));
                    }
                }
            }
            let mut loaded = Vec::new();
            for (_, delta) in &mut planned {
                loaded.push(
                    self.engine
                        .load(&mut tx, &self.codecs, delta, usize::MAX)
                        .await
                        .expect("load"),
                );
            }
            tx.commit().await.expect("commit");
            for ((_, delta), loaded) in planned.iter().zip(&loaded) {
                self.deliver(delta, loaded);
            }
            let mut tx = self.conn.begin().await.expect("a transaction");
            for (notes, delta) in &planned {
                self.engine.record(&mut tx, delta).await.expect("record");
                let ids: Vec<i64> = notes.iter().map(|n| n.id).collect();
                mark_done(&mut tx, &ids).await.expect("mark");
            }
            tx.commit().await.expect("commit");
            for delta in planned.iter().map(|(_, delta)| delta) {
                for replica in Engine::replicas(delta) {
                    self.assert_recorded(replica).await;
                }
            }
            deltas.extend(planned.into_iter().map(|(_, delta)| delta));
        }
    }

    /// What one plan sends, applied to each replica's model as the replica
    /// applies its messages; a row that could not be sent waits for the fix.
    fn deliver(&mut self, delta: &Delta, loaded: &Loaded) {
        for unsent in &loaded.failed {
            assert!(
                unsent.reason.contains("a value this build cannot encode"),
                "{unsent:?} could not go, and nothing but the bug should stop a row"
            );
            self.quarantined
                .insert((unsent.replica, unsent.table.to_owned(), unsent.key.clone()));
        }
        for replica in Engine::replicas(delta) {
            let held = self.held.entry(replica).or_default();
            // A snapshot's end sweeps every row of the tables it names that
            // the snapshot did not send.
            if delta.snapshots.contains(&replica) {
                let swept = self.engine.snapshot_tables();
                held.retain(|(table, _), _| !swept.contains(&table.as_str()));
            }
            for table in self.engine.rows(delta, loaded, replica) {
                for (bytes, key) in table.upserts.iter().zip(&table.keys) {
                    let row = serde_json::from_slice(bytes).expect("a row as JSON");
                    held.insert((table.table.clone(), key.parts.join("|")), row);
                }
                for key in &table.deletes {
                    held.remove(&(table.table.clone(), key.parts.join("|")));
                }
            }
            for ((table, key), row) in held.iter() {
                for (child, columns, parent) in REFERENCES {
                    if child != table {
                        continue;
                    }
                    if let Some(parent_key) = key_of(row, columns) {
                        assert!(
                            held.contains_key(&((*parent).to_owned(), parent_key.clone())),
                            "{replica} was sent {table} {key} without its {parent} {parent_key}; \
                             last steps: {:#?}",
                            &self.history[self.history.len().saturating_sub(8)..],
                        );
                    }
                }
            }
        }
    }

    /// The engine records `replica` holding exactly what it was sent.
    async fn assert_recorded(&mut self, replica: Uuid) {
        let recorded: BTreeSet<(String, String)> =
            sqlx::query_as("SELECT relation, row_key FROM sync.held WHERE replica_id = $1")
                .bind(replica)
                .fetch_all(&mut *self.conn)
                .await
                .expect("held")
                .into_iter()
                .collect();
        let holds: BTreeSet<(String, String)> = self
            .held
            .get(&replica)
            .map(|held| held.keys().cloned().collect())
            .unwrap_or_default();
        assert!(
            recorded == holds,
            "{replica}: recorded other than sent:\n  recorded, not sent {:?}\n  sent, not recorded {:?}\n  last steps: {:#?}",
            recorded.difference(&holds).collect::<Vec<_>>(),
            holds.difference(&recorded).collect::<Vec<_>>(),
            self.last_steps(),
        );
    }

    fn last_steps(&self) -> &[String] {
        &self.history[self.history.len().saturating_sub(8)..]
    }

    /// Each live replica holds exactly what its strategies say, every row as
    /// it is now, and the engine records exactly what it holds; one not live
    /// is recorded as holding nothing. A replica with rows set aside for it is
    /// left out until the fix lets them through.
    async fn assert_holdings(&mut self, seed: u64, step: usize) {
        let replicas: Vec<(Uuid, bool)> = sqlx::query_as(
            "SELECT r.id, coalesce(v.live, false) FROM fixture.t_replicas r \
               LEFT JOIN t_replicas v ON v.id = r.id ORDER BY r.id",
        )
        .fetch_all(&mut *self.conn)
        .await
        .expect("replicas");
        for (replica, live) in replicas {
            let recorded: BTreeSet<(String, String)> =
                sqlx::query_as("SELECT relation, row_key FROM sync.held WHERE replica_id = $1")
                    .bind(replica)
                    .fetch_all(&mut *self.conn)
                    .await
                    .expect("held")
                    .into_iter()
                    .collect();
            if !live {
                assert!(
                    recorded.is_empty(),
                    "seed {seed:#x}, after step {step}: {replica} is not live and is recorded holding {recorded:?}"
                );
                continue;
            }
            self.assert_recorded(replica).await;
            let held = self.held.get(&replica).cloned().unwrap_or_default();
            let holds: BTreeSet<(String, String)> = held.keys().cloned().collect();
            if self.quarantined.iter().any(|(r, _, _)| *r == replica) {
                continue;
            }
            let should = expected(&mut self.conn, replica).await;
            assert!(
                holds == should,
                "seed {seed:#x}, after step {step}, {replica}:\n  holds   {}\n  missing {:?}\n  extra   {:?}\n  last steps: {:#?}",
                holds.len(),
                should.difference(&holds).collect::<Vec<_>>(),
                holds.difference(&should).collect::<Vec<_>>(),
                self.last_steps(),
            );
            for ((table, key), row) in &held {
                let now = self.current(table, key).await;
                assert!(
                    Some(row) == now.as_ref(),
                    "seed {seed:#x}, after step {step}, {replica} holds {table} {key} as\n  {row}\n  and it is now\n  {now:?}\n  last steps: {:#?}",
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

    /// Notes again, for its replica alone, every row set aside: what the
    /// relay does once a deploy fixed what refused them.
    async fn retry_quarantine(&mut self) {
        for (replica, table, key) in std::mem::take(&mut self.quarantined) {
            sqlx::query("INSERT INTO sync.notes (kind, relation, row_key, scope) VALUES ('row', $1, $2, $3)")
                .bind(table)
                .bind(key)
                .bind(replica)
                .execute(&mut *self.conn)
                .await
                .expect("noted again");
        }
    }
}

/// What `replica` should hold, read off the rows: what its strategies send
/// it, what those point at, and the followers of whatever it holds.
async fn expected(conn: &mut Conn, replica: Uuid) -> BTreeSet<(String, String)> {
    sqlx::query_as(
        "WITH r AS (SELECT id, partition_id FROM t_replicas WHERE id = $1 AND live),
         lists AS (SELECT a.list_id FROM fixture.t_replica_lists a JOIN r ON a.replica_id = r.id),
         products AS (SELECT p.* FROM fixture.t_products p JOIN r USING (partition_id) WHERE p.retired_at IS NULL),
         orders AS (SELECT o.* FROM fixture.t_orders o JOIN r ON o.replica_id = r.id WHERE o.retired_at IS NULL),
         tasks AS (SELECT t.* FROM fixture.t_tasks t JOIN r ON t.replica_id = r.id WHERE t.retired_at IS NULL),
         customers AS (
             SELECT c.id FROM fixture.t_customers c JOIN fixture.t_list_members m ON m.customer_id = c.id
               JOIN lists USING (list_id) WHERE c.retired_at IS NULL
             UNION SELECT customer_id FROM orders WHERE customer_id IS NOT NULL),
         accounts AS (
             SELECT c.region, c.number FROM fixture.t_accounts c
               JOIN fixture.t_account_members m USING (region, number)
               JOIN lists USING (list_id) WHERE c.retired_at IS NULL
             UNION SELECT region, number FROM tasks WHERE region IS NOT NULL AND number IS NOT NULL),
         bins AS (
             SELECT b.store, b.slot FROM fixture.t_bins b JOIN r USING (partition_id) WHERE b.retired_at IS NULL
             UNION SELECT store, slot FROM tasks WHERE store IS NOT NULL AND slot IS NOT NULL),
         emails AS (SELECT e.id FROM fixture.t_emails e JOIN customers c ON c.id = e.customer_id
                     WHERE e.retired_at IS NULL),
         phones AS (SELECT p.* FROM fixture.t_phones p JOIN customers c ON c.id = p.customer_id
                     WHERE p.retired_at IS NULL),
         lines AS (SELECT l.* FROM fixture.t_task_lines l JOIN tasks t ON t.id = l.task_id
                    WHERE l.retired_at IS NULL),
         kinds AS (SELECT kind_id FROM products WHERE kind_id IS NOT NULL
                   UNION SELECT kind_id FROM phones WHERE kind_id IS NOT NULL),
         sizes AS (SELECT b.size_code FROM fixture.t_bins b JOIN bins USING (store, slot)
                    WHERE b.size_code IS NOT NULL
                   UNION SELECT a.size_code FROM fixture.t_accounts a JOIN accounts USING (region, number)
                    WHERE a.size_code IS NOT NULL
                   UNION SELECT size_code FROM lines WHERE size_code IS NOT NULL)
         SELECT 't_products', id::text FROM products
         UNION ALL SELECT 't_orders', id::text FROM orders
         UNION ALL SELECT 't_tasks', id::text FROM tasks
         UNION ALL SELECT 't_customers', id::text FROM customers
         UNION ALL SELECT 't_accounts', region || '|' || number FROM accounts
         UNION ALL SELECT 't_bins', store || '|' || slot FROM bins
         UNION ALL SELECT 't_emails', id::text FROM emails
         UNION ALL SELECT 't_phones', customer_id || '|' || number FROM phones
         UNION ALL SELECT 't_task_lines', task_id || '|' || line FROM lines
         UNION ALL SELECT 't_kinds', kind_id::text FROM kinds
         UNION ALL SELECT 't_sizes', size_code::text FROM sizes",
    )
    .bind(replica)
    .fetch_all(&mut *conn)
    .await
    .expect("expected")
    .into_iter()
    .collect()
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
/// a row something still points at: answers whether it went in.
async fn wrote(
    conn: &mut Conn,
    query: sqlx::query::Query<'_, crate::Db, sqlx::postgres::PgArguments>,
) -> bool {
    match query.execute(&mut *conn).await {
        Ok(done) => done.rows_affected() > 0,
        Err(sqlx::Error::Database(e))
            if matches!(e.code().as_deref(), Some("23502" | "23503" | "23505")) =>
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

/// One write of an office, or one thing that happens to a replica, picked at
/// random among those the schema allows.
async fn step(conn: &mut Conn, walk: &mut Walk, partitions: &[Uuid]) -> Step {
    let partition = partitions[walk.below(partitions.len())];
    let replicas: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM fixture.t_replicas ORDER BY id")
        .fetch_all(&mut *conn)
        .await
        .expect("replicas");
    let kinds = uuids(conn, "t_kinds", partition).await;
    let products = uuids(conn, "t_products", partition).await;
    let customers = uuids(conn, "t_customers", partition).await;
    let lists = uuids(conn, "t_lists", partition).await;
    let emails = uuids(conn, "t_emails", partition).await;
    let orders = uuids(conn, "t_orders", partition).await;
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
    let choice = walk.below(70);
    let (kind, product, customer, list, email, order) = (
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
    let small = i32::try_from(walk.below(4)).expect("small");
    let other = partitions[walk.below(partitions.len())];
    let done = match choice {
        0 => write!(conn, "INSERT INTO fixture.t_kinds (id, partition_id, name) VALUES ($1, $2, 'k')", new, partition),
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
        38 => write!(conn, "UPDATE fixture.t_bins SET name = name || '.' WHERE store = $1 AND slot = $2", bin.map(|b| b.0), bin.map(|b| b.1)),
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
        51 => write!(conn, "UPDATE fixture.t_tasks SET name = name || '.' WHERE id = $1", task),
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
        _ => {
            for table in ["t_kinds", "t_sizes"] {
                sqlx::query(&format!("UPDATE fixture.{table} SET name = 'cured' WHERE name = $1"))
                    .bind(POISON)
                    .execute(&mut *conn)
                    .await
                    .expect("the fix");
            }
            return Step::Cured;
        }
    };
    if done {
        Step::Wrote(format!("{choice}"))
    } else {
        Step::Skipped
    }
}

#[sqlx::test(migrations = false)]
async fn every_replica_holds_exactly_what_its_strategies_say(pool: crate::DbPool) {
    let mut world = world(pool).await;
    for seed in SEEDS {
        walk_from(&mut world, seed).await;
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

async fn walk_from(world: &mut World, seed: u64) {
    // What the previous walk left behind: every replica starts from a snapshot.
    sqlx::query(
        "INSERT INTO sync.notes (partition_id, kind, replica_id) \
         SELECT partition_id, 'resync', id FROM fixture.t_replicas",
    )
    .execute(&mut *world.conn)
    .await
    .expect("resync");
    world.sync(None).await;
    let mut walk = Walk(seed);
    world.history.clear();
    for n in 0..400 {
        let last = n == 399;
        match step(&mut world.conn, &mut walk, &world.partitions).await {
            Step::Wrote(what) => world.history.push(what),
            Step::Skipped => {}
            Step::Cured => {
                world.history.push("the fix".to_owned());
                world.retry_quarantine().await;
            }
        }
        if walk.below(3) == 0 || last {
            if last {
                for table in ["t_kinds", "t_sizes"] {
                    sqlx::query(&format!(
                        "UPDATE fixture.{table} SET name = 'cured' WHERE name = $1"
                    ))
                    .bind(POISON)
                    .execute(&mut *world.conn)
                    .await
                    .expect("the fix");
                }
                world.sync(None).await;
                world.retry_quarantine().await;
            }
            world.sync(Some(&mut walk)).await;
            if last {
                world.sync(None).await;
            }
            world.assert_holdings(seed, n).await;
        }
    }
    assert!(
        world.quarantined.is_empty(),
        "seed {seed:#x}: still set aside after the fix: {:?}",
        world.quarantined
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
    world.sync(None).await;
    sqlx::query("UPDATE fixture.t_products SET name = 'q' WHERE id = $1")
        .bind(product)
        .execute(&mut *world.conn)
        .await
        .expect("a rename");
    let deltas = world.sync(None).await;
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
        Replicas::new("shop.registers", "registers", "active"),
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
    world.sync(None).await;
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
        previous: None,
        xid: 1,
    };
    let mut tx = world
        .conn
        .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("a transaction");
    assert!(matches!(
        world.engine.plan(&mut tx, &[change]).await,
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
    let notes: Vec<(String, String, Option<Uuid>, Option<serde_json::Value>)> = sqlx::query_as(
        "SELECT relation, row_key, partition_id, previous FROM sync.notes ORDER BY id",
    )
    .fetch_all(&mut *conn)
    .await
    .expect("notes");
    let pointed = serde_json::json!({ "kind_id": kind.to_string() });
    assert_eq!(
        notes,
        vec![
            // A write of only what the replicas own is not one here.
            (
                "t_counters".into(),
                counter.to_string(),
                Some(a),
                Some(pointed.clone())
            ),
            // Named as the engine knows it, not by the partition it landed in.
            (
                "t_logs".into(),
                format!("{log}|{a}"),
                Some(a),
                Some(serde_json::json!({}))
            ),
            // A key that changed: the new one where it is now, the old one
            // where it was; of the row as it was, only what it pointed at.
            (
                "t_counters".into(),
                log.to_string(),
                Some(b),
                Some(pointed.clone())
            ),
            (
                "t_counters".into(),
                counter.to_string(),
                Some(a),
                Some(pointed)
            ),
        ]
    );
}
