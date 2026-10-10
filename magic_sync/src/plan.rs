//! The planner: which rows each replica holds, and what to send it so it holds
//! exactly those.
//!
//! Every table the server sends down declares how its rows find replicas
//! ([`Sync`]); nothing here names a table. From the declarations and the
//! foreign keys between the mirrored tables the engine builds, once, the query
//! that answers "who should hold which rows" of each table. Everything it does
//! is then one operation — compare what should be held with `sync.held`, what
//! is — narrowed to what a change can have moved:
//!
//! - a row changed: that row, sent to whoever should hold it, taken back from
//!   whoever holds it and no longer should; the rows that follow it, re-routed.
//! - a group changed (a member joined or left, a group was given or taken): the
//!   replicas it reaches, every row of the grouped table.
//! - a replica is (re)provisioned: that replica, every table, from scratch.
//!
//! A row travels with the rows it points at: a parent a replica lacks goes
//! with it, and one is taken back only once nothing the replica still holds
//! points at it — until then it is kept, and sent again when it changes. So a
//! replica never holds a row whose parent it lacks.
//!
//! The notes are read, planned and loaded in one `REPEATABLE READ`
//! transaction: the plan, the parents it closes over and the rows it encodes
//! are one snapshot of the database.

use crate::envelope;
use crate::row::{key_text, Column, Key, KeyPart, Mirror, Sync, Table};
use crate::wire::Codec;
use crate::Error;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use uuid::Uuid;

/// The rows that are replicas, and which of them are live (have somewhere to
/// send to). The engine compares them with what it last saw on every pass: a
/// replica that became live, or moved to another partition, is provisioned
/// again from scratch; one that stopped being live, or is gone, is forgotten
/// and sent nothing.
pub struct Replicas {
    /// The relation listing them, with an `id` column (and the partition's).
    pub relation: &'static str,
    /// The table behind it, as a foreign key names it.
    pub table: &'static str,
    /// Which of them receive: a condition on the relation's columns.
    pub live: &'static str,
    /// The column that splits rows and replicas into partitions no row
    /// crosses: a broadcast row goes to every live replica of its partition.
    /// `None`: one partition, every replica.
    pub partition: Option<&'static str>,
}

/// A foreign key from one mirrored table to another, as the child's columns
/// that hold the parent's key.
#[derive(Clone, Debug)]
struct Reference {
    parent: usize,
    /// The child's columns, in the parent's key order.
    columns: Vec<String>,
}

/// A table of the schema, by its schema and name.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Relation {
    schema: String,
    name: String,
}

impl Relation {
    fn sql(&self) -> String {
        format!("{}.{}", ident(&self.schema), ident(&self.name))
    }
}

/// How a grouped table reaches replicas.
#[derive(Clone, Debug)]
struct Group {
    members: Relation,
    /// The members' columns naming the grouped row, in its key order.
    member_row: Vec<String>,
    /// The columns naming a group, the members' and the assigned table's,
    /// pair by pair.
    group: Vec<(String, String)>,
    assigned: Relation,
    /// The assigned table's column naming the replica.
    assigned_replica: String,
}

pub(crate) struct Plan {
    pub(crate) table: &'static str,
    relation: Relation,
    /// What the rows replicas should hold are read from.
    active: String,
    pub(crate) key: Vec<Column>,
    sync: Sync,
    references: Vec<Reference>,
    group: Option<Group>,
    /// The columns the replicas own: a write of only these is no change here.
    owned: Vec<&'static str>,
}

/// The rows to send to, and take back from, each replica.
#[derive(Default, Debug)]
pub struct Delta {
    pub upserts: BTreeMap<Uuid, BTreeMap<usize, BTreeSet<String>>>,
    pub deletes: BTreeMap<Uuid, BTreeMap<usize, BTreeSet<String>>>,
    /// Replicas being provisioned: what they held is forgotten and what they
    /// should hold sent whole, then everything else swept on their side.
    pub snapshots: BTreeSet<Uuid>,
    /// The xmin of the snapshot the plan read: every transaction before it had
    /// finished, so what it noted is in the rows sent.
    pub horizon: i64,
    /// What was planned, for [`Engine::load`] to plan it again around a row
    /// that cannot go.
    changes: Vec<Change>,
    /// Rows that cannot go to a replica, and why: never sent it, and taken
    /// back where it holds them, with whatever points at them.
    blocked: BTreeMap<(Uuid, usize, String), String>,
    /// [`Delta::keep_only`] and [`Delta::forget`], kept to apply again.
    only: Option<Uuid>,
    left_out: BTreeSet<Uuid>,
}

impl Delta {
    fn upsert(&mut self, replica: Uuid, table: usize, key: String) {
        if self.blocked.contains_key(&(replica, table, key.clone())) {
            return;
        }
        if let Some(keys) = self
            .deletes
            .get_mut(&replica)
            .and_then(|t| t.get_mut(&table))
        {
            keys.remove(&key);
        }
        self.upserts
            .entry(replica)
            .or_default()
            .entry(table)
            .or_default()
            .insert(key);
    }

    /// The keys of `table` being sent, per replica.
    fn sent(&self, table: usize) -> Vec<(Uuid, Vec<String>)> {
        self.upserts
            .iter()
            .filter_map(|(e, t)| t.get(&table).map(|k| (*e, k.iter().cloned().collect())))
            .collect()
    }

    /// The keys of `table` being taken back, per replica.
    fn taken(&self, table: usize) -> Vec<(Uuid, Vec<String>)> {
        self.deletes
            .iter()
            .filter_map(|(e, t)| t.get(&table).map(|k| (*e, k.iter().cloned().collect())))
            .collect()
    }

    fn unsend(&mut self, replica: Uuid, table: usize, key: &str) {
        if let Some(keys) = self
            .upserts
            .get_mut(&replica)
            .and_then(|t| t.get_mut(&table))
        {
            keys.remove(key);
        }
    }

    fn delete(&mut self, replica: Uuid, table: usize, key: String) {
        self.deletes
            .entry(replica)
            .or_default()
            .entry(table)
            .or_default()
            .insert(key);
    }

    fn has_upsert(&self, replica: Uuid, table: usize, key: &str) -> bool {
        self.upserts
            .get(&replica)
            .and_then(|t| t.get(&table))
            .is_some_and(|k| k.contains(key))
    }

    fn has_delete(&self, replica: Uuid, table: usize, key: &str) -> bool {
        self.deletes
            .get(&replica)
            .and_then(|t| t.get(&table))
            .is_some_and(|k| k.contains(key))
    }

    /// Only what `replica` is owed: what a note planned for that replica alone
    /// asks.
    pub fn keep_only(&mut self, replica: Uuid) {
        self.only = Some(replica);
        self.restrict();
    }

    /// Everything `replica` is owed, left out: it was not sent, so it is not
    /// recorded.
    pub fn forget(&mut self, replica: Uuid) {
        self.left_out.insert(replica);
        self.restrict();
    }

    fn restrict(&mut self) {
        let (only, left_out) = (self.only, self.left_out.clone());
        let keep = |r: &Uuid| only.is_none_or(|o| o == *r) && !left_out.contains(r);
        self.upserts.retain(|r, _| keep(r));
        self.deletes.retain(|r, _| keep(r));
        self.snapshots.retain(keep);
        self.blocked.retain(|(r, _, _), _| keep(r));
        for rows in [&mut self.upserts, &mut self.deletes] {
            for tables in rows.values_mut() {
                tables.retain(|_, keys| !keys.is_empty());
            }
            rows.retain(|_, tables| !tables.is_empty());
        }
    }

    /// Every row this delta sends `replica`, as its table and key.
    pub fn upserted(&self, replica: Uuid) -> Vec<(usize, String)> {
        self.upserts
            .get(&replica)
            .into_iter()
            .flat_map(|tables| {
                tables
                    .iter()
                    .flat_map(|(i, keys)| keys.iter().map(move |k| (*i, k.clone())))
            })
            .collect()
    }

    /// What is held, less what the snapshot replicas held: they start from
    /// nothing, so it is not there to count on.
    fn fresh<T>(
        &self,
        held: impl IntoIterator<Item = ((Uuid, String), T)>,
    ) -> BTreeMap<(Uuid, String), T> {
        held.into_iter()
            .filter(|((e, _), _)| !self.snapshots.contains(e))
            .collect()
    }
}

/// A pending note: what changed, and the one replica it is still owed to
/// when a send to that replica failed.
#[derive(Clone, Debug)]
pub struct Note {
    pub id: i64,
    pub change: Change,
    pub scope: Option<Uuid>,
}

/// What changed, as the outbox noted it.
#[derive(Clone, Debug)]
pub enum Change {
    /// A row of a mirrored table, by its key, and the columns it pointed at
    /// other mirrored rows with, as they were before an update or a delete.
    Row {
        table: String,
        key: String,
        previous: Option<serde_json::Value>,
        /// The transaction that noted it.
        xid: i64,
    },
    /// A member joined or left a group of a grouped table's members table: the
    /// member row, as it was or is.
    Members {
        table: String,
        member: serde_json::Value,
    },
    /// A group was given to or taken from a replica.
    Assigned { table: String, replica: Uuid },
    /// A replica is provisioned again, from scratch.
    Resync { replica: Uuid },
}

/// Every row a plan sends, encoded once: by table index and key text.
#[derive(Default)]
pub struct Loaded {
    rows: HashMap<(usize, String), Vec<u8>>,
    /// Rows that could not go, per replica they were to go to — that do not
    /// encode, or point at a row that could not go where the replica lacks
    /// it: left out of the delta, neither sent nor recorded as held.
    pub failed: Vec<Unsent>,
}

/// A row the engine could not send a replica, and why.
#[derive(Debug)]
pub struct Unsent {
    pub replica: Uuid,
    pub table: &'static str,
    pub key: String,
    pub reason: String,
}

/// A foreign key as the catalogue reads it: the child, the parent, then the
/// child's and the parent's columns, in order.
type ForeignKey = (Relation, Relation, Vec<String>, Vec<String>);

/// A [`ForeignKey`] as the catalogue query answers it, each relation as its
/// schema and name.
type CatalogForeignKey = (String, String, String, String, Vec<String>, Vec<String>);

/// Rows `record` writes per statement.
const RECORD_CHUNK: usize = 50_000;

/// The transaction-scoped advisory lock installs take, so two processes
/// installing at once run one after the other ("SYNCINST").
const INSTALL_LOCK: i64 = 0x5359_4e43_494e_5354;

/// The engine's schema, one script per version: a database at version `n`
/// runs the scripts after the `n`th, in one transaction. Each script ends by
/// writing its version.
const MIGRATIONS: [&str; crate::SCHEMA_VERSION as usize] = [crate::POSTGRES_SCHEMA];

/// The engine, built once from the codecs and the schema.
pub struct Engine {
    pub(crate) plans: Vec<Plan>,
    by_name: HashMap<&'static str, usize>,
    /// Parents before children.
    pub(crate) order: Vec<usize>,
    pub(crate) replicas: Replicas,
}

type Conn = crate::row::Conn;

/// A name as SQL quotes it, whatever it is spelled with.
fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A string as an SQL literal.
fn literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// The key text of the row aliased `alias`, its `columns` joined by `|`.
fn key_sql(alias: &str, columns: &[impl AsRef<str>]) -> String {
    let parts: Vec<String> = columns
        .iter()
        .map(|c| format!("{alias}.{}::text", ident(c.as_ref())))
        .collect();
    format!("concat_ws('|', {})", parts.join(", "))
}

/// `columns` (named as in the table aliased `alias`, typed as `types`) among
/// the key texts of the array parameter `param`. Each part of the text is cast
/// back to its column's type and compared as a row value, so the table's key
/// index serves a composite key as well as a single one.
fn key_in(alias: &str, columns: &[impl AsRef<str>], types: &[Column], param: &str) -> String {
    if let ([column], [typed]) = (columns, types) {
        return format!(
            "{alias}.{} IN (SELECT unnest({param}::text[])::{})",
            ident(column.as_ref()),
            KeyPart::sql_type(typed)
        );
    }
    let row: Vec<String> = columns
        .iter()
        .map(|c| format!("{alias}.{}", ident(c.as_ref())))
        .collect();
    let parts: Vec<String> = types
        .iter()
        .enumerate()
        .map(|(i, typed)| {
            format!(
                "split_part(k, '|', {})::{}",
                i + 1,
                KeyPart::sql_type(typed)
            )
        })
        .collect();
    format!(
        "({}) IN (SELECT {} FROM unnest({param}::text[]) AS k)",
        row.join(", "),
        parts.join(", ")
    )
}

/// Parses a key back into its parts, as the table's key columns type them.
pub(crate) fn parse_key(columns: &[Column], text: &str) -> Result<Key, Error> {
    let parts: Vec<&str> = text.split('|').collect();
    if parts.len() != columns.len() {
        return Err(Error::Malformed {
            table: String::new(),
            reason: format!(
                "`{text}` has {} parts, and the key {}",
                parts.len(),
                columns.len()
            ),
        });
    }
    columns
        .iter()
        .zip(parts)
        .map(|(column, part)| KeyPart::parse(column, part))
        .collect()
}

/// The key text `columns` hold in a row as JSON: `None` when one is NULL.
/// Numbers are read as JSON writes them, so an integer key from `to_jsonb`
/// is the same text as its column's.
fn key_of(row: &serde_json::Value, columns: &[String]) -> Option<String> {
    let parts: Option<Vec<String>> = columns
        .iter()
        .map(|c| match row.get(c)? {
            serde_json::Value::String(v) => Some(v.clone()),
            serde_json::Value::Number(v) => Some(v.to_string()),
            _ => None,
        })
        .collect();
    parts.map(|p| p.join("|"))
}

/// A trigger the engine needs, as `CREATE TRIGGER` names it and `pg_trigger`
/// stores it.
#[derive(Debug, PartialEq, Eq)]
struct Trigger {
    on: Relation,
    name: &'static str,
    /// `pg_trigger.tgtype`: row or statement, before or after, the events.
    kind: i16,
    function: &'static str,
    args: Vec<String>,
}

/// `tgtype` bits.
const ROW: i16 = 1;
const BEFORE: i16 = 2;
const INSERT: i16 = 4;
const DELETE: i16 = 8;
const UPDATE: i16 = 16;
const TRUNCATE: i16 = 32;

impl Trigger {
    fn ddl(&self) -> String {
        let timing = if self.kind & BEFORE != 0 {
            "BEFORE"
        } else {
            "AFTER"
        };
        let events: Vec<&str> = [
            (INSERT, "INSERT"),
            (UPDATE, "UPDATE"),
            (DELETE, "DELETE"),
            (TRUNCATE, "TRUNCATE"),
        ]
        .into_iter()
        .filter(|(bit, _)| self.kind & bit != 0)
        .map(|(_, event)| event)
        .collect();
        let each = if self.kind & ROW != 0 {
            "ROW"
        } else {
            "STATEMENT"
        };
        let args: Vec<String> = self.args.iter().map(|a| literal(a)).collect();
        format!(
            "CREATE TRIGGER {} {timing} {} ON {} FOR EACH {each} EXECUTE FUNCTION sync.{}({});",
            self.name,
            events.join(" OR "),
            self.on.sql(),
            self.function,
            args.join(", ")
        )
    }
}

/// A trigger of the database whose function is the engine's.
#[derive(sqlx::FromRow)]
struct LiveTrigger {
    schema: String,
    table: String,
    name: String,
    kind: i16,
    function: String,
    nargs: i16,
    args: Vec<u8>,
    /// Enabled, with no `WHEN` and no column list: it fires on every row.
    plain: bool,
}

impl LiveTrigger {
    fn on(&self) -> Relation {
        Relation {
            schema: self.schema.clone(),
            name: self.table.clone(),
        }
    }

    fn matches(&self, expected: &Trigger) -> bool {
        let args: Vec<String> = self
            .args
            .split(|b| *b == 0)
            .take(usize::try_from(self.nargs).unwrap_or_default())
            .map(|a| String::from_utf8_lossy(a).into_owned())
            .collect();
        self.plain
            && self.kind == expected.kind
            && self.function == expected.function
            && args == expected.args
    }
}

/// Every trigger whose function is in the engine's schema, but those a
/// partition inherits from its partitioned table.
async fn live_triggers(conn: &mut Conn) -> Result<Vec<LiveTrigger>, Error> {
    Ok(sqlx::query_as(
        "SELECT n.nspname::text AS schema, c.relname::text AS table, t.tgname::text AS name, \
                t.tgtype AS kind, p.proname::text AS function, t.tgnargs AS nargs, t.tgargs AS args, \
                t.tgenabled <> 'D' AND t.tgqual IS NULL AND t.tgattr::text = '' AS plain \
           FROM pg_trigger t \
           JOIN pg_class c ON c.oid = t.tgrelid \
           JOIN pg_namespace n ON n.oid = c.relnamespace \
           JOIN pg_proc p ON p.oid = t.tgfoid \
           JOIN pg_namespace pn ON pn.oid = p.pronamespace \
          WHERE pn.nspname = 'sync' AND NOT t.tgisinternal AND t.tgparentid = 0",
    )
    .fetch_all(&mut *conn)
    .await?)
}

/// Brings the engine's schema to this build's version, in the caller's
/// transaction: one process at a time, every missing step or none.
async fn migrate(conn: &mut Conn) -> Result<(), Error> {
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(INSTALL_LOCK)
        .execute(&mut *conn)
        .await?;
    let installed = version(conn).await?.unwrap_or(0);
    let from = usize::try_from(installed).unwrap_or(usize::MAX);
    if from > MIGRATIONS.len() {
        return Err(Error::Misuse(format!(
            "the engine's schema is version {installed}, from a later build than this one ({})",
            crate::SCHEMA_VERSION
        )));
    }
    for script in &MIGRATIONS[from..] {
        sqlx::raw_sql(script).execute(&mut *conn).await?;
    }
    Ok(())
}

/// The version of the engine's schema the database has, if any.
async fn version(conn: &mut Conn) -> Result<Option<i32>, Error> {
    let versioned: bool = sqlx::query_scalar("SELECT to_regclass('sync.version') IS NOT NULL")
        .fetch_one(&mut *conn)
        .await?;
    if !versioned {
        return Ok(None);
    }
    Ok(sqlx::query_scalar("SELECT version FROM sync.version")
        .fetch_optional(&mut *conn)
        .await?)
}

impl Engine {
    /// Reads the foreign keys between the mirrored tables and each grouped
    /// table's group tables from the catalog, and checks that every trigger
    /// the engine needs is there as [`Engine::triggers`] writes it. Fails on
    /// a declaration the schema cannot honour — a `follows` column that is not
    /// a key, a group table that does not have the columns a group needs, a
    /// cycle — and on a trigger missing or different.
    pub async fn new(
        conn: &mut Conn,
        tables: &[&Table],
        replicas: Replicas,
    ) -> Result<Self, Error> {
        let engine = Self::read(conn, tables, replicas).await?;
        let live = live_triggers(conn).await?;
        let mut wrong = Vec::new();
        for expected in engine.triggers_needed() {
            match live
                .iter()
                .find(|t| t.on() == expected.on && t.name == expected.name)
            {
                Some(t) if t.matches(&expected) => {}
                Some(_) => wrong.push(format!("differs, and is: {}", expected.ddl())),
                None => wrong.push(format!("is missing: {}", expected.ddl())),
            }
        }
        for stray in engine.strays(&live) {
            wrong.push(format!(
                "`{}` on {} notes for no table this engine declares: drop it",
                stray.name,
                stray.on().sql()
            ));
        }
        if !wrong.is_empty() {
            return Err(Error::Misuse(format!(
                "the engine's triggers are not as it needs them — write them as Engine::triggers \
                 does, in a migration:\n{}",
                wrong.join("\n")
            )));
        }
        Ok(engine)
    }

    /// [`Engine::new`], having first brought the database to what it needs,
    /// in the caller's transaction and one process at a time: the engine's
    /// schema installed or upgraded, and every trigger missing or different
    /// written as [`Engine::triggers`] does (and any of the engine's on a
    /// table no longer declared dropped). A table mirrored for the first time
    /// that already has rows is sent whole: every live replica is provisioned
    /// again. Idempotent.
    ///
    /// For a host that has no migrations of its own. Run it before anything
    /// writes a mirrored table — a write before its trigger is never noted —
    /// on a connection that may create triggers. A host with migrations runs
    /// [`POSTGRES_SCHEMA`](crate::POSTGRES_SCHEMA) in one and writes each
    /// table's triggers in the migration that creates it, as it does its
    /// indexes; [`Engine::new`] refuses to start while one is missing.
    pub async fn install(
        conn: &mut Conn,
        tables: &[&Table],
        replicas: Replicas,
    ) -> Result<Self, Error> {
        migrate(conn).await?;
        let engine = Self::read(conn, tables, replicas).await?;
        let live = live_triggers(conn).await?;
        for stray in engine.strays(&live) {
            sqlx::raw_sql(&format!(
                "DROP TRIGGER {} ON {}",
                ident(&stray.name),
                stray.on().sql()
            ))
            .execute(&mut *conn)
            .await?;
        }
        let mut first_noted = false;
        for expected in engine.triggers_needed() {
            let existing = live
                .iter()
                .find(|t| t.on() == expected.on && t.name == expected.name);
            if existing.is_some_and(|t| t.matches(&expected)) {
                continue;
            }
            if existing.is_some() {
                sqlx::raw_sql(&format!(
                    "DROP TRIGGER {} ON {}",
                    expected.name,
                    expected.on.sql()
                ))
                .execute(&mut *conn)
                .await?;
            } else if expected.function == "note_row" {
                let has_rows: bool = sqlx::query_scalar(&format!(
                    "SELECT EXISTS (SELECT 1 FROM {})",
                    expected.on.sql()
                ))
                .fetch_one(&mut *conn)
                .await?;
                first_noted |= has_rows;
            }
            sqlx::raw_sql(&expected.ddl()).execute(&mut *conn).await?;
        }
        if first_noted {
            // Rows written before their table was noted reached no replica:
            // every replica is provisioned again, and gets them.
            sqlx::query(&format!(
                "INSERT INTO sync.notes (kind, replica_id, partition_id) \
                 SELECT 'resync', s.id, s.partition FROM {} WHERE {}",
                engine.replicas_sql(),
                RESYNC_PENDING.replace("$1", "s.id")
            ))
            .execute(&mut *conn)
            .await?;
        }
        Ok(engine)
    }

    /// The statements that create every trigger the engine needs, one per
    /// line: what a host with migrations writes in the migration that creates
    /// the tables, and what [`Engine::install`] runs. A mirrored table notes
    /// every write that changes a column it does not leave to the replicas,
    /// and refuses a truncate; a group table notes every write.
    pub async fn triggers(
        conn: &mut Conn,
        tables: &[&Table],
        replicas: Replicas,
    ) -> Result<String, Error> {
        let engine = Self::read(conn, tables, replicas).await?;
        Ok(engine
            .triggers_needed()
            .iter()
            .map(Trigger::ddl)
            .collect::<Vec<_>>()
            .join("\n"))
    }

    fn triggers_needed(&self) -> Vec<Trigger> {
        let partition = self.replicas.partition.unwrap_or_default().to_owned();
        let mut out = Vec::new();
        let mut groups: BTreeMap<Relation, (String, String, &str)> = BTreeMap::new();
        for plan in &self.plans {
            let mut kept: Vec<String> = Vec::new();
            for column in plan.references.iter().flat_map(|r| &r.columns) {
                if !kept.contains(column) {
                    kept.push(column.clone());
                }
            }
            let mut args = vec![
                plan.table.to_owned(),
                partition.clone(),
                plan.key.len().to_string(),
            ];
            args.extend(plan.key.iter().map(|c| c.name.to_owned()));
            args.push(kept.len().to_string());
            args.extend(kept);
            args.extend(plan.owned.iter().map(|c| (*c).to_owned()));
            out.push(Trigger {
                on: plan.relation.clone(),
                name: "sync_note_row",
                kind: ROW | INSERT | UPDATE | DELETE,
                function: "note_row",
                args,
            });
            out.push(Trigger {
                on: plan.relation.clone(),
                name: "sync_refuse_truncate",
                kind: BEFORE | TRUNCATE,
                function: "refuse_truncate",
                args: Vec::new(),
            });
            if let (Some(g), Sync::Grouped { members, assigned }) = (&plan.group, plan.sync) {
                groups.insert(
                    g.members.clone(),
                    (members.to_owned(), g.group[0].0.clone(), "members"),
                );
                groups.insert(
                    g.assigned.clone(),
                    (assigned.to_owned(), g.assigned_replica.clone(), "assigned"),
                );
            }
        }
        for (on, (name, column, role)) in groups {
            out.push(Trigger {
                on,
                name: "sync_note_group",
                kind: ROW | INSERT | UPDATE | DELETE,
                function: "note_group",
                args: vec![name, column, role.to_owned(), partition.clone()],
            });
        }
        out
    }

    /// The engine's triggers on tables it does not need them on.
    fn strays<'a>(&self, live: &'a [LiveTrigger]) -> Vec<&'a LiveTrigger> {
        let needed = self.triggers_needed();
        live.iter()
            .filter(|t| !needed.iter().any(|n| n.on == t.on() && n.name == t.name))
            .collect()
    }

    async fn read(conn: &mut Conn, tables: &[&Table], replicas: Replicas) -> Result<Self, Error> {
        let version = version(conn).await?;
        if version != Some(crate::SCHEMA_VERSION) {
            return Err(Error::Misuse(format!(
                "the engine's schema is version {}, and this build needs {}: install it",
                version.map_or("none".to_owned(), |v| v.to_string()),
                crate::SCHEMA_VERSION
            )));
        }
        let tables: Vec<&Table> = tables
            .iter()
            .copied()
            .filter(|t| matches!(t.mirror, Mirror::Catalog | Mirror::Both))
            .collect();
        let mut by_name: HashMap<&'static str, usize> = HashMap::new();
        for (i, t) in tables.iter().enumerate() {
            // A replica knows a table by its name alone.
            if by_name.insert(t.name, i).is_some() {
                return Err(Error::Misuse(format!(
                    "two mirrored tables are named `{}`: a replica tells tables apart by name",
                    t.name
                )));
            }
        }

        let foreign_keys: Vec<CatalogForeignKey> =
            sqlx::query_as(
                "SELECT child_ns.nspname::text, child.relname::text,
                        parent_ns.nspname::text, parent.relname::text,
                        ARRAY(SELECT a.attname::text FROM unnest(c.conkey) WITH ORDINALITY AS k(n, o)
                              JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.n
                              ORDER BY k.o),
                        ARRAY(SELECT a.attname::text FROM unnest(c.confkey) WITH ORDINALITY AS k(n, o)
                              JOIN pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = k.n
                              ORDER BY k.o)
                   FROM pg_constraint c
                   JOIN pg_class child ON child.oid = c.conrelid
                   JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
                   JOIN pg_class parent ON parent.oid = c.confrelid
                   JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
                  WHERE c.contype = 'f' AND c.conparentid = 0",
            )
            .fetch_all(&mut *conn)
            .await?;
        let foreign_keys: Vec<ForeignKey> = foreign_keys
            .into_iter()
            .map(|(cs, c, ps, p, cc, pc)| {
                (
                    Relation {
                        schema: cs,
                        name: c,
                    },
                    Relation {
                        schema: ps,
                        name: p,
                    },
                    cc,
                    pc,
                )
            })
            .collect();
        let primary_keys: Vec<(String, String, Vec<String>)> = sqlx::query_as(
            "SELECT n.nspname::text, c.relname::text,
                    ARRAY(SELECT a.attname::text FROM unnest(i.indkey) AS k(n)
                          JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.n)
               FROM pg_index i
               JOIN pg_class c ON c.oid = i.indrelid
               JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE i.indisprimary",
        )
        .fetch_all(&mut *conn)
        .await?;

        let mut plans: Vec<Plan> = Vec::with_capacity(tables.len());
        for t in &tables {
            let relation = Relation {
                schema: t.schema.to_owned(),
                name: t.name.to_owned(),
            };
            let key: Vec<Column> = t
                .key
                .iter()
                .map(|k| {
                    t.columns
                        .iter()
                        .find(|c| c.name == *k)
                        .copied()
                        .ok_or_else(|| {
                            Error::Misuse(format!(
                                "`{}` is keyed by `{k}`, which it does not declare",
                                t.name
                            ))
                        })
                })
                .collect::<Result<_, _>>()?;
            // The key is the declaration's, in its order: notes write keys in
            // it, so it has to be the primary key's columns.
            let declared: BTreeSet<&str> = t.key.iter().copied().collect();
            let primary = primary_keys
                .iter()
                .find(|(s, n, _)| *s == relation.schema && *n == relation.name);
            if primary.is_none_or(|(_, _, cols)| {
                cols.iter().map(String::as_str).collect::<BTreeSet<_>>() != declared
            }) {
                return Err(Error::Misuse(format!(
                    "`{}.{}` is mirrored keyed by {:?}, which is not its primary key",
                    t.schema, t.name, t.key
                )));
            }
            plans.push(Plan {
                table: t.name,
                active: t.active.map_or_else(|| relation.sql(), str::to_owned),
                relation,
                key,
                sync: t.sync,
                references: Vec::new(),
                group: None,
                owned: t
                    .columns
                    .iter()
                    .filter(|c| c.replica_owned)
                    .map(|c| c.name)
                    .collect(),
            });
        }

        let mirrored = |relation: &Relation| {
            by_name
                .get(relation.name.as_str())
                .copied()
                .filter(|&i| plans[i].relation == *relation)
        };
        let mut references: Vec<(usize, Reference)> = Vec::new();
        for (child, parent, child_columns, parent_columns) in &foreign_keys {
            let (Some(c), Some(p)) = (mirrored(child), mirrored(parent)) else {
                continue;
            };
            if c == p {
                return Err(Error::Misuse(format!(
                    "`{}` points at itself: the engine cannot order a table's rows among themselves",
                    child.name
                )));
            }
            // The child's columns that hold the parent's key, in its order.
            let columns: Option<Vec<String>> = plans[p]
                .key
                .iter()
                .map(|k| {
                    parent_columns
                        .iter()
                        .position(|pc| pc == k.name)
                        .map(|i| child_columns[i].clone())
                })
                .collect();
            let Some(columns) = columns.filter(|_| parent_columns.len() == plans[p].key.len())
            else {
                return Err(Error::Misuse(format!(
                    "`{}` points at `{}` by {parent_columns:?}, which is not its key: the engine \
                     sends a parent by its key",
                    child.name, parent.name
                )));
            };
            references.push((c, Reference { parent: p, columns }));
        }
        for (c, reference) in references {
            plans[c].references.push(reference);
        }

        for plan in &mut plans {
            match plan.sync {
                Sync::Follows(column) => {
                    if !plan
                        .references
                        .iter()
                        .any(|r| r.columns == [column.to_owned()])
                    {
                        return Err(Error::Misuse(format!(
                            "`{}` follows `{column}`, which is not a key into a mirrored table",
                            plan.table
                        )));
                    }
                }
                Sync::Grouped { members, assigned } => {
                    plan.group = Some(group_of(
                        &foreign_keys,
                        plan,
                        members,
                        assigned,
                        replicas.table,
                    )?);
                }
                _ => {}
            }
        }

        let columns: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT table_schema::text, table_name::text, column_name::text \
               FROM information_schema.columns",
        )
        .fetch_all(&mut *conn)
        .await?;
        for plan in &plans {
            let has = |column: &str| {
                columns.iter().any(|(s, t, c)| {
                    *s == plan.relation.schema && *t == plan.relation.name && c == column
                })
            };
            let missing = match plan.sync {
                Sync::Broadcast => replicas.partition.filter(|p| !has(p)),
                Sync::Assigned(column) if !has(column) => Some(column),
                _ => None,
            };
            if let Some(column) = missing {
                return Err(Error::Misuse(format!(
                    "`{}` is sent by `{column}`, which it does not have",
                    plan.table
                )));
            }
        }
        let order = topological(&plans)?;
        Ok(Self {
            plans,
            by_name,
            order,
            replicas,
        })
    }

    pub(crate) fn index(&self, table: &str) -> Option<usize> {
        self.by_name.get(table).copied()
    }

    pub fn table(&self, index: usize) -> &'static str {
        self.plans[index].table
    }

    /// The live replicas, as a subquery `s(id, partition)`.
    pub(crate) fn replicas_sql(&self) -> String {
        let partition = self
            .replicas
            .partition
            .map_or("NULL::uuid".to_owned(), ident);
        format!(
            "(SELECT id, {partition} AS partition FROM {} WHERE {}) AS s",
            self.replicas.relation, self.replicas.live
        )
    }

    /// Compares the live replicas with what the engine saw of them last time,
    /// in the caller's transaction: a replica that became live, or moved to
    /// another partition, is noted for provisioning from scratch; one that is
    /// no longer live, or gone, is forgotten — nothing is recorded as held by
    /// it, and nothing is sent it until it is live again. The relay runs it
    /// before every pass.
    pub async fn observe(&self, conn: &mut Conn) -> Result<(), Error> {
        sqlx::query(&format!(
            "WITH now AS (SELECT s.id, s.partition FROM {}), \
             seen AS (SELECT replica_id, partition_id FROM sync.replicas WHERE live), \
             arrived AS (SELECT n.id, n.partition FROM now n \
                          WHERE NOT EXISTS (SELECT 1 FROM seen s WHERE s.replica_id = n.id \
                                              AND s.partition_id IS NOT DISTINCT FROM n.partition)), \
             gone AS (SELECT s.replica_id FROM seen s \
                       WHERE NOT EXISTS (SELECT 1 FROM now n WHERE n.id = s.replica_id)), \
             provisioned AS (INSERT INTO sync.notes (kind, replica_id, partition_id) \
                             SELECT 'resync', a.id, a.partition FROM arrived a WHERE {}), \
             forgotten AS (DELETE FROM sync.held h USING gone g WHERE h.replica_id = g.replica_id), \
             came AS (INSERT INTO sync.replicas AS r (replica_id, live, partition_id) \
                      SELECT id, true, partition FROM arrived \
                      ON CONFLICT (replica_id) DO UPDATE SET live = true, partition_id = EXCLUDED.partition_id) \
             UPDATE sync.replicas r SET live = false, partition_id = NULL \
               FROM gone g WHERE r.replica_id = g.replica_id",
            self.replicas_sql(),
            RESYNC_PENDING.replace("$1", "a.id")
        ))
        .execute(&mut *conn)
        .await?;
        Ok(())
    }

    /// How a broadcast row meets the replicas of its partition.
    fn same_partition(&self) -> String {
        match self.replicas.partition {
            Some(column) => format!("s.partition = t.{}", ident(column)),
            None => "true".to_owned(),
        }
    }

    /// `SELECT replica_id, key` for every row of a table its strategy sends
    /// to a live replica, each once, narrowed by `filter` on the table aliased
    /// `t` and the replicas aliased `s`; `None` for a table that travels only
    /// when pointed at, or with its parent.
    pub(crate) fn wanted_sql(&self, i: usize, filter: &str) -> Option<String> {
        let plan = &self.plans[i];
        let key = key_sql("t", &plan.key.iter().map(|c| c.name).collect::<Vec<_>>());
        let replicas = self.replicas_sql();
        let same_partition = self.same_partition();
        let view = &plan.active;
        Some(match plan.sync {
            Sync::Referenced | Sync::Follows(_) => return None,
            Sync::Broadcast => format!(
                "SELECT s.id AS replica_id, {key} AS key FROM {view} t \
                 JOIN {replicas} ON {same_partition} WHERE {filter}"
            ),
            Sync::Assigned(column) => format!(
                "SELECT s.id AS replica_id, {key} AS key FROM {view} t \
                 JOIN {replicas} ON s.id = t.{} WHERE {filter}",
                ident(column)
            ),
            Sync::Grouped { .. } => {
                let g = plan.group.as_ref()?;
                let member_row: Vec<String> = g
                    .member_row
                    .iter()
                    .zip(&plan.key)
                    .map(|(m, k)| format!("m.{} = t.{}", ident(m), ident(k.name)))
                    .collect();
                let group: Vec<String> = g
                    .group
                    .iter()
                    .map(|(m, a)| format!("a.{} = m.{}", ident(a), ident(m)))
                    .collect();
                // A row in two groups given to one replica is held once.
                format!(
                    "SELECT DISTINCT s.id AS replica_id, {key} AS key FROM {view} t \
                     JOIN {} m ON {} JOIN {} a ON {} \
                     JOIN {replicas} ON s.id = a.{} WHERE {filter}",
                    g.members.sql(),
                    member_row.join(" AND "),
                    g.assigned.sql(),
                    group.join(" AND "),
                    ident(&g.assigned_replica),
                )
            }
        })
    }

    /// Who should hold which of `keys` (`None`: every key) and, where
    /// `replicas` is given, only among those.
    pub(crate) async fn wanted(
        &self,
        conn: &mut Conn,
        i: usize,
        keys: Option<&[String]>,
        replicas: Option<&[Uuid]>,
    ) -> Result<BTreeSet<(Uuid, String)>, Error> {
        let plan = &self.plans[i];
        let names: Vec<&str> = plan.key.iter().map(|c| c.name).collect();
        // Only the filters asked for, so each query plans for what it reads.
        let mut filter = vec!["true".to_owned()];
        if keys.is_some() {
            filter.push(key_in("t", &names, &plan.key, "$1"));
        }
        if replicas.is_some() {
            filter.push(format!("s.id = ANY(${})", 1 + usize::from(keys.is_some())));
        }
        let Some(sql) = self.wanted_sql(i, &filter.join(" AND ")) else {
            return Ok(BTreeSet::new());
        };
        let mut query = sqlx::query_as::<_, (Uuid, String)>(&sql);
        if let Some(keys) = keys {
            query = query.bind(keys);
        }
        if let Some(replicas) = replicas {
            query = query.bind(replicas);
        }
        Ok(query.fetch_all(&mut *conn).await?.into_iter().collect())
    }

    /// Who holds which of `keys` (`None`: every key), among `replicas`
    /// when given, and the horizon each was last sent from.
    async fn held(
        &self,
        conn: &mut Conn,
        i: usize,
        keys: Option<&[String]>,
        replicas: Option<&[Uuid]>,
    ) -> Result<BTreeMap<(Uuid, String), i64>, Error> {
        let mut sql = "SELECT replica_id, row_key, horizon::text::bigint FROM sync.held \
                       WHERE relation = $1"
            .to_owned();
        if keys.is_some() {
            sql.push_str(" AND row_key = ANY($2)");
        }
        if replicas.is_some() {
            sql.push_str(&format!(
                " AND replica_id = ANY(${})",
                2 + usize::from(keys.is_some())
            ));
        }
        let mut query = sqlx::query_as::<_, (Uuid, String, i64)>(&sql).bind(self.plans[i].table);
        if let Some(keys) = keys {
            query = query.bind(keys);
        }
        if let Some(replicas) = replicas {
            query = query.bind(replicas);
        }
        Ok(query
            .fetch_all(&mut *conn)
            .await?
            .into_iter()
            .map(|(e, k, h)| ((e, k), h))
            .collect())
    }

    /// A change to rows of `i`: sent where its strategy sends it and the held
    /// version predates the change; held where it should no longer be, taken
    /// back — unless something the replica keeps points at it, which the pass
    /// over references decides.
    async fn reconcile_keys(
        &self,
        conn: &mut Conn,
        delta: &mut Delta,
        i: usize,
        keys: &[String],
        notes: Option<&BTreeMap<String, i64>>,
    ) -> Result<(), Error> {
        if keys.is_empty() {
            return Ok(());
        }
        let horizons = delta.fresh(self.held(conn, i, Some(keys), None).await?);
        let wanted = self.wanted(conn, i, Some(keys), None).await?;
        for holder in &wanted {
            if Self::owed(&horizons, notes, holder) {
                delta.upsert(holder.0, i, holder.1.clone());
            }
        }
        for (replica, key) in horizons.keys() {
            if !wanted.contains(&(*replica, key.clone())) {
                delta.delete(*replica, i, key.clone());
            }
        }
        Ok(())
    }

    /// Whether `holder` must be sent its row: it lacks it, or holds a version
    /// sent from before the latest change `notes` has for it — a change whose
    /// transaction the send's horizon is past is already in what it holds.
    fn owed(
        horizons: &BTreeMap<(Uuid, String), i64>,
        notes: Option<&BTreeMap<String, i64>>,
        holder: &(Uuid, String),
    ) -> bool {
        match horizons.get(holder) {
            None => true,
            Some(horizon) => notes
                .and_then(|n| n.get(&holder.1))
                .is_some_and(|xid| xid >= horizon),
        }
    }

    /// Every row of `i`, for `replicas`: what only who-holds-what can have moved.
    async fn reconcile_replicas(
        &self,
        conn: &mut Conn,
        delta: &mut Delta,
        i: usize,
        replicas: &[Uuid],
    ) -> Result<(), Error> {
        if replicas.is_empty() {
            return Ok(());
        }
        let held = delta.fresh(self.held(conn, i, None, Some(replicas)).await?);
        let wanted = self.wanted(conn, i, None, Some(replicas)).await?;
        for holder in &wanted {
            if !held.contains_key(holder) {
                delta.upsert(holder.0, i, holder.1.clone());
            }
        }
        for (replica, key) in held.keys() {
            if !wanted.contains(&(*replica, key.clone())) {
                delta.delete(*replica, i, key.clone());
            }
        }
        Ok(())
    }

    /// What a batch of changes asks of every replica, read in the caller's
    /// `REPEATABLE READ` transaction.
    pub async fn plan(&self, conn: &mut Conn, changes: &[Change]) -> Result<Delta, Error> {
        let delta = Delta {
            changes: changes.to_vec(),
            ..Delta::default()
        };
        self.plan_around(conn, delta).await
    }

    /// [`Engine::plan`] of `delta`'s changes, around the rows it says cannot
    /// go: the plan of every replica not narrowed away by
    /// [`Delta::keep_only`] and [`Delta::forget`].
    async fn plan_around(&self, conn: &mut Conn, mut delta: Delta) -> Result<Delta, Error> {
        let horizon: i64 =
            sqlx::query_scalar("SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint")
                .fetch_one(&mut *conn)
                .await?;
        let live: BTreeSet<Uuid> =
            sqlx::query_scalar(&format!("SELECT s.id FROM {}", self.replicas_sql()))
                .fetch_all(&mut *conn)
                .await?
                .into_iter()
                .collect();
        delta.horizon = horizon;
        let changes = std::mem::take(&mut delta.changes);
        // Per table, each changed key and the latest transaction that noted it.
        let mut rows: BTreeMap<usize, BTreeMap<String, i64>> = BTreeMap::new();
        let mut released = Released::default();
        let mut regrouped: BTreeMap<usize, BTreeSet<String>> = BTreeMap::new();
        let mut reassigned: BTreeMap<usize, BTreeSet<Uuid>> = BTreeMap::new();
        for change in &changes {
            match change {
                Change::Row {
                    table,
                    key,
                    previous,
                    xid,
                } => {
                    let i = self.index(table).ok_or_else(|| {
                        Error::Misuse(format!(
                            "a change to `{table}`, which no row declares: its trigger outlived its declaration"
                        ))
                    })?;
                    // A key its table cannot have fails here, the same way
                    // every time, rather than wherever a query casts it; one it
                    // can is read in the form every query writes it.
                    let key =
                        key_text(&parse_key(&self.plans[i].key, key).map_err(|e| match e {
                            Error::Malformed { reason, .. } => Error::Malformed {
                                table: table.clone(),
                                reason,
                            },
                            other => other,
                        })?);
                    let latest = rows.entry(i).or_default().entry(key.clone()).or_default();
                    *latest = (*latest).max(*xid);
                    if let Some(previous) = previous {
                        released
                            .previous
                            .entry((i, key))
                            .or_default()
                            .push(previous.clone());
                    }
                }
                Change::Members { table, member } => {
                    for (i, g) in self.groups_through(table, |g| &g.members) {
                        if let Some(key) = key_of(member, &g.member_row) {
                            regrouped.entry(i).or_default().insert(key);
                        }
                    }
                }
                Change::Assigned { table, replica } => {
                    for (i, _) in self.groups_through(table, |g| &g.assigned) {
                        reassigned.entry(i).or_default().insert(*replica);
                    }
                }
                Change::Resync { replica } => {
                    delta.snapshots.insert(*replica);
                }
            }
        }
        // A member that moved is routed again by its key; a group given or
        // taken, once per table and batch for its replicas.
        for (i, keys) in regrouped {
            let keys: Vec<String> = keys.into_iter().collect();
            self.reconcile_keys(conn, &mut delta, i, &keys, None)
                .await?;
        }
        for (i, replicas) in reassigned {
            let replicas: Vec<Uuid> = replicas.into_iter().collect();
            self.reconcile_replicas(conn, &mut delta, i, &replicas)
                .await?;
        }
        let mut touched: BTreeMap<usize, BTreeMap<String, i64>> = BTreeMap::new();
        for (i, notes) in &rows {
            if matches!(self.plans[*i].sync, Sync::Follows(_)) {
                touched.insert(*i, notes.clone());
                continue;
            }
            let keys: Vec<String> = notes.keys().cloned().collect();
            self.reconcile_keys(conn, &mut delta, *i, &keys, Some(notes))
                .await?;
        }
        let snapshots: Vec<Uuid> = delta.snapshots.iter().copied().collect();
        if !snapshots.is_empty() {
            for &i in &self.order {
                let wanted = self.wanted(conn, i, None, Some(&snapshots)).await?;
                for (replica, key) in wanted {
                    delta.upsert(replica, i, key);
                }
            }
        }
        // Each pass can give the others work — a parent a follower needs, a
        // parent a deleted follower released — so they run until none does.
        for _ in 0..16 {
            let before = (delta.upserts.clone(), delta.deletes.clone());
            self.close_references(conn, &mut delta).await?;
            self.block(conn, &mut delta).await?;
            self.release_references(conn, &mut delta, &released, &rows)
                .await?;
            self.follow_parents(conn, &mut delta, &touched).await?;
            if before == (delta.upserts.clone(), delta.deletes.clone()) {
                // One not live is sent nothing, a snapshot least of all.
                let dead: Vec<Uuid> = Engine::replicas(&delta)
                    .difference(&live)
                    .copied()
                    .collect();
                for replica in dead {
                    delta.forget(replica);
                }
                delta.changes = changes;
                delta.restrict();
                return Ok(delta);
            }
        }
        Err(Error::Misuse(
            "the sync plan did not settle in 16 passes".to_owned(),
        ))
    }

    fn groups_through(
        &self,
        table: &str,
        pick: impl Fn(&Group) -> &Relation,
    ) -> Vec<(usize, Group)> {
        self.plans
            .iter()
            .enumerate()
            .filter_map(|(i, p)| {
                let declared = match p.sync {
                    Sync::Grouped { members, assigned } => [members, assigned],
                    _ => return None,
                };
                p.group
                    .as_ref()
                    .filter(|g| {
                        // The note names the group table as the row declares it.
                        let name = &pick(g).name;
                        name == table && declared.contains(&name.as_str())
                    })
                    .map(|g| (i, g.clone()))
            })
            .collect()
    }

    /// The parents a row points at, per row: `(child key, parent key)` for one
    /// reference of `i`.
    async fn parents_of(
        &self,
        conn: &mut Conn,
        i: usize,
        reference: &Reference,
        keys: &[String],
        active: bool,
    ) -> Result<Vec<(String, String)>, Error> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let plan = &self.plans[i];
        let from = if active {
            plan.active.clone()
        } else {
            plan.relation.sql()
        };
        let names: Vec<&str> = plan.key.iter().map(|c| c.name).collect();
        let key = key_sql("t", &names);
        let parent_key = key_sql("t", &reference.columns);
        let not_null: Vec<String> = reference
            .columns
            .iter()
            .map(|c| format!("t.{} IS NOT NULL", ident(c)))
            .collect();
        Ok(sqlx::query_as(&format!(
            "SELECT {key}, {parent_key} FROM {from} t WHERE {} AND {}",
            key_in("t", &names, &plan.key, "$1"),
            not_null.join(" AND ")
        ))
        .bind(keys)
        .fetch_all(&mut *conn)
        .await?)
    }

    /// Sends along every parent a sent row points at that its replica lacks,
    /// children first so a parent's own parents follow.
    async fn close_references(&self, conn: &mut Conn, delta: &mut Delta) -> Result<(), Error> {
        for &i in self.order.iter().rev() {
            let sent = delta.sent(i);
            if sent.is_empty() {
                continue;
            }
            let keys: Vec<String> = sent
                .iter()
                .flat_map(|(_, keys)| keys.iter().cloned())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            for reference in self.plans[i].references.clone() {
                let parent_of: BTreeMap<String, String> = self
                    .parents_of(conn, i, &reference, &keys, false)
                    .await?
                    .into_iter()
                    .collect();
                let parent_keys: Vec<String> = parent_of
                    .values()
                    .cloned()
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                let replicas: Vec<Uuid> = sent.iter().map(|(e, _)| *e).collect();
                let held = if parent_keys.is_empty() {
                    BTreeMap::new()
                } else {
                    delta.fresh(
                        self.held(conn, reference.parent, Some(&parent_keys), Some(&replicas))
                            .await?,
                    )
                };
                for (replica, keys) in &sent {
                    for parent in keys.iter().filter_map(|k| parent_of.get(k)) {
                        if !held.contains_key(&(*replica, parent.clone()))
                            && !delta.has_upsert(*replica, reference.parent, parent)
                        {
                            delta.upsert(*replica, reference.parent, parent.clone());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Takes back what nothing a replica keeps points at any more, and keeps
    /// what something still does — sending it again where it changed since the
    /// replica's version.
    ///
    /// The candidates are the deletes already decided — a row a replica
    /// should no longer hold — and the parents a change released: the ones a
    /// changed row pointed at before, for whoever holds it. Tables are visited
    /// children first, so a delete that stands releases its own parents in
    /// turn. A candidate stays when a row its replica keeps or is being sent
    /// points at it, or when its own strategy still sends it there.
    async fn release_references(
        &self,
        conn: &mut Conn,
        delta: &mut Delta,
        released: &Released,
        changed: &BTreeMap<usize, BTreeMap<String, i64>>,
    ) -> Result<(), Error> {
        let mut candidates: BTreeMap<usize, BTreeSet<(Uuid, String)>> = BTreeMap::new();
        let mut by_table: BTreeMap<usize, Vec<String>> = BTreeMap::new();
        for (i, key) in released.previous.keys() {
            by_table.entry(*i).or_default().push(key.clone());
        }
        let mut holders_of: BTreeMap<(usize, String), Vec<Uuid>> = BTreeMap::new();
        for (i, keys) in &by_table {
            for (replica, key) in self.held(conn, *i, Some(keys), None).await?.into_keys() {
                holders_of.entry((*i, key)).or_default().push(replica);
            }
        }
        for ((i, key), versions) in &released.previous {
            let holders = holders_of
                .get(&(*i, key.clone()))
                .cloned()
                .unwrap_or_default();
            for reference in &self.plans[*i].references {
                for previous in versions {
                    let Some(parent) = key_of(previous, &reference.columns) else {
                        continue;
                    };
                    for replica in &holders {
                        candidates
                            .entry(reference.parent)
                            .or_default()
                            .insert((*replica, parent.clone()));
                    }
                }
            }
        }

        for &p in self.order.iter().rev() {
            let mut examined: BTreeSet<(Uuid, String)> = delta
                .deletes
                .iter()
                .flat_map(|(e, t)| {
                    t.get(&p)
                        .into_iter()
                        .flatten()
                        .map(move |k| (*e, k.clone()))
                })
                .collect();
            if let Some(released) = candidates.remove(&p) {
                // Only what is held and not on its way back can be taken back.
                let keys: Vec<String> = released.iter().map(|(_, k)| k.clone()).collect();
                let replicas: Vec<Uuid> = released.iter().map(|(e, _)| *e).collect();
                let held = delta.fresh(self.held(conn, p, Some(&keys), Some(&replicas)).await?);
                let wanted = self.wanted(conn, p, Some(&keys), Some(&replicas)).await?;
                // A row on its way again only because it changed is still
                // examined: sending it does not keep it.
                for candidate in released {
                    if held.contains_key(&candidate) && !wanted.contains(&candidate) {
                        examined.insert(candidate);
                    }
                }
            }
            if examined.is_empty() {
                continue;
            }
            let mut kept = self.pointed_at(conn, delta, p, &examined).await?;
            kept.retain(|(r, k)| !delta.blocked.contains_key(&(*r, p, k.clone())));
            // Kept for what points at it: it goes again where it changed.
            if let Some(notes) = changed.get(&p) {
                let keys: Vec<String> = kept
                    .iter()
                    .filter(|(_, k)| notes.contains_key(k))
                    .map(|(_, k)| k.clone())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                if !keys.is_empty() {
                    let horizons = self.held(conn, p, Some(&keys), None).await?;
                    for holder in &kept {
                        if notes.contains_key(&holder.1)
                            && Self::owed(&horizons, Some(notes), holder)
                        {
                            delta.upsert(holder.0, p, holder.1.clone());
                        }
                    }
                }
            }
            for (replica, key) in &examined {
                if kept.contains(&(*replica, key.clone())) {
                    if let Some(keys) = delta.deletes.get_mut(replica).and_then(|t| t.get_mut(&p)) {
                        keys.remove(key);
                    }
                } else {
                    delta.unsend(*replica, p, key);
                    delta.delete(*replica, p, key.clone());
                }
            }
            // A delete that stands releases what the row points at.
            let standing: Vec<(Uuid, String)> = examined.difference(&kept).cloned().collect();
            let keys: Vec<String> = standing
                .iter()
                .map(|(_, k)| k.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            for reference in &self.plans[p].references {
                let mut parents: BTreeMap<String, String> = self
                    .parents_of(conn, p, reference, &keys, false)
                    .await?
                    .into_iter()
                    .collect();
                for key in &keys {
                    if parents.contains_key(key) {
                        continue;
                    }
                    if let Some(parent) = released
                        .previous
                        .get(&(p, key.clone()))
                        .and_then(|versions| versions.last())
                        .and_then(|row| key_of(row, &reference.columns))
                    {
                        parents.insert(key.clone(), parent);
                    }
                }
                for (replica, key) in &standing {
                    if let Some(parent) = parents.get(key) {
                        candidates
                            .entry(reference.parent)
                            .or_default()
                            .insert((*replica, parent.clone()));
                    }
                }
            }
        }
        for tables in delta.deletes.values_mut() {
            tables.retain(|_, keys| !keys.is_empty());
        }
        delta.deletes.retain(|_, tables| !tables.is_empty());
        Ok(())
    }

    /// The rows of `c` that `replicas` hold and that point, through
    /// `reference`, at one of `keys`: `(replica, row, parent)`. From the few
    /// pointing rows to whether a replica holds them, by index.
    async fn held_pointing(
        &self,
        conn: &mut Conn,
        c: usize,
        reference: &Reference,
        replicas: &[Uuid],
        keys: &[String],
    ) -> Result<Vec<(Uuid, String, String)>, Error> {
        let plan = &self.plans[c];
        Ok(sqlx::query_as(&format!(
            "SELECT h.replica_id, h.row_key, {} FROM {} t \
               JOIN sync.held h ON h.relation = $1 AND h.replica_id = ANY($2) \
                               AND h.row_key = {} \
              WHERE {}",
            key_sql("t", &reference.columns),
            plan.relation.sql(),
            key_sql("t", &plan.key.iter().map(|k| k.name).collect::<Vec<_>>()),
            key_in(
                "t",
                &reference.columns,
                &self.plans[reference.parent].key,
                "$3"
            ),
        ))
        .bind(plan.table)
        .bind(replicas)
        .bind(keys)
        .fetch_all(&mut *conn)
        .await?)
    }

    /// A row that cannot go to a replica takes with it, parents first, what
    /// points at it there — held or being sent. None of them is sent it, and
    /// one it holds is taken back.
    async fn block(&self, conn: &mut Conn, delta: &mut Delta) -> Result<(), Error> {
        if delta.blocked.is_empty() {
            return Ok(());
        }
        for &c in &self.order {
            for reference in &self.plans[c].references {
                let p = reference.parent;
                let parents: BTreeMap<(Uuid, String), String> = delta
                    .blocked
                    .iter()
                    .filter(|((_, t, _), _)| *t == p)
                    .map(|((r, _, k), why)| ((*r, k.clone()), why.clone()))
                    .collect();
                if parents.is_empty() {
                    continue;
                }
                let replicas: Vec<Uuid> = parents
                    .keys()
                    .map(|(r, _)| *r)
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                let keys: Vec<String> = parents
                    .keys()
                    .map(|(_, k)| k.clone())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                let mut pointing: Vec<(Uuid, String, String)> = self
                    .held_pointing(conn, c, reference, &replicas, &keys)
                    .await?
                    .into_iter()
                    .filter(|(r, _, _)| !delta.snapshots.contains(r))
                    .collect();
                let sent: Vec<(Uuid, Vec<String>)> = delta
                    .sent(c)
                    .into_iter()
                    .filter(|(r, _)| replicas.contains(r))
                    .collect();
                let child_keys: Vec<String> = sent
                    .iter()
                    .flat_map(|(_, keys)| keys.iter().cloned())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                let parent_of: BTreeMap<String, String> = self
                    .parents_of(conn, c, reference, &child_keys, false)
                    .await?
                    .into_iter()
                    .collect();
                for (replica, keys) in sent {
                    for key in keys {
                        if let Some(parent) = parent_of.get(&key) {
                            pointing.push((replica, key, parent.clone()));
                        }
                    }
                }
                for (replica, row, parent) in pointing {
                    if let Some(why) = parents.get(&(replica, parent.clone())) {
                        delta.blocked.entry((replica, c, row)).or_insert_with(|| {
                            format!(
                                "it points at `{}` {parent}, which cannot go: {why}",
                                self.plans[p].table
                            )
                        });
                    }
                }
            }
        }
        let mut by_table: BTreeMap<usize, Vec<String>> = BTreeMap::new();
        for (_, i, key) in delta.blocked.keys() {
            by_table.entry(*i).or_default().push(key.clone());
        }
        for (i, keys) in by_table {
            let held = delta.fresh(self.held(conn, i, Some(&keys), None).await?);
            let blocked: Vec<(Uuid, String)> = delta
                .blocked
                .keys()
                .filter(|(_, t, _)| *t == i)
                .map(|(r, _, k)| (*r, k.clone()))
                .collect();
            for (replica, key) in blocked {
                delta.unsend(replica, i, &key);
                if held.contains_key(&(replica, key.clone())) {
                    delta.delete(replica, i, key);
                }
            }
        }
        Ok(())
    }

    /// Which of `candidates` (rows of `p`, per replica) a row the replica
    /// keeps — held and not being taken back, or being sent — points at.
    async fn pointed_at(
        &self,
        conn: &mut Conn,
        delta: &Delta,
        p: usize,
        candidates: &BTreeSet<(Uuid, String)>,
    ) -> Result<BTreeSet<(Uuid, String)>, Error> {
        let keys: Vec<String> = candidates
            .iter()
            .map(|(_, k)| k.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let replicas: Vec<Uuid> = candidates
            .iter()
            .map(|(e, _)| *e)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let mut kept = BTreeSet::new();
        for (c, plan) in self.plans.iter().enumerate() {
            // A follower goes where its parent goes: it never keeps it.
            let follows = match plan.sync {
                Sync::Follows(column) => Some(column),
                _ => None,
            };
            for reference in plan
                .references
                .iter()
                .filter(|r| r.parent == p && follows.is_none_or(|f| r.columns != [f.to_owned()]))
            {
                let pointing = self
                    .held_pointing(conn, c, reference, &replicas, &keys)
                    .await?;
                for (replica, child, parent) in pointing {
                    if !delta.has_delete(replica, c, &child) {
                        kept.insert((replica, parent));
                    }
                }
                let sent: Vec<(Uuid, Vec<String>)> = delta
                    .sent(c)
                    .into_iter()
                    .filter(|(e, _)| replicas.contains(e))
                    .collect();
                let child_keys: Vec<String> = sent
                    .iter()
                    .flat_map(|(_, keys)| keys.iter().cloned())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                let parent_of: BTreeMap<String, String> = self
                    .parents_of(conn, c, reference, &child_keys, false)
                    .await?
                    .into_iter()
                    .collect();
                for (replica, keys) in &sent {
                    for parent in keys.iter().filter_map(|k| parent_of.get(k)) {
                        kept.insert((*replica, parent.clone()));
                    }
                }
            }
        }
        Ok(kept.intersection(candidates).cloned().collect())
    }

    /// Followers go wherever their parent goes, parents first: a parent newly
    /// sent to a replica brings its followers, one taken back takes them
    /// back, and a follower that changed goes to whoever will hold its parent.
    async fn follow_parents(
        &self,
        conn: &mut Conn,
        delta: &mut Delta,
        touched: &BTreeMap<usize, BTreeMap<String, i64>>,
    ) -> Result<(), Error> {
        for &c in &self.order {
            let Sync::Follows(column) = self.plans[c].sync else {
                continue;
            };
            let reference = self.plans[c]
                .references
                .iter()
                .find(|r| r.columns == [column.to_owned()])
                .expect("checked in new()")
                .clone();
            let parent = reference.parent;
            let child_key = key_sql(
                "t",
                &self.plans[c].key.iter().map(|k| k.name).collect::<Vec<_>>(),
            );

            // Followers that changed: to whoever will hold their parent.
            if let Some(notes) = touched.get(&c) {
                let keys: Vec<String> = notes.keys().cloned().collect();
                let keys = &keys;
                // Read from the active rows: a retired follower goes to no
                // one, and is taken back from whoever holds it.
                let parents: BTreeMap<String, String> = self
                    .parents_of(conn, c, &reference, keys, true)
                    .await?
                    .into_iter()
                    .collect();
                let parent_keys: Vec<String> = parents
                    .values()
                    .cloned()
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                let parent_held = if parent_keys.is_empty() {
                    BTreeMap::new()
                } else {
                    delta.fresh(self.held(conn, parent, Some(&parent_keys), None).await?)
                };
                let horizons = delta.fresh(self.held(conn, c, Some(keys), None).await?);
                let mut targets: BTreeSet<(Uuid, String)> = BTreeSet::new();
                for (key, pk) in &parents {
                    for (replica, _) in parent_held.keys().filter(|(_, k)| k == pk) {
                        if !delta.has_delete(*replica, parent, pk) {
                            targets.insert((*replica, key.clone()));
                        }
                    }
                    for (replica, tables) in &delta.upserts {
                        if tables.get(&parent).is_some_and(|k| k.contains(pk)) {
                            targets.insert((*replica, key.clone()));
                        }
                    }
                }
                for target in &targets {
                    if delta.snapshots.contains(&target.0)
                        || Self::owed(&horizons, Some(notes), target)
                    {
                        delta.upsert(target.0, c, target.1.clone());
                    }
                }
                for holder in horizons.keys() {
                    if !targets.contains(holder) {
                        delta.delete(holder.0, c, holder.1.clone());
                    }
                }
            }

            let column_in = |param: &str| key_in("t", &[column], &self.plans[parent].key, param);

            // Parents newly sent bring their followers.
            let sent = delta.sent(parent);
            let parent_keys: Vec<String> = sent
                .iter()
                .flat_map(|(_, keys)| keys.iter().cloned())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            if !parent_keys.is_empty() {
                let replicas: Vec<Uuid> = sent.iter().map(|(e, _)| *e).collect();
                let held = delta.fresh(
                    self.held(conn, parent, Some(&parent_keys), Some(&replicas))
                        .await?,
                );
                let followers: Vec<(String, String)> = sqlx::query_as(&format!(
                    "SELECT {child_key}, t.{}::text FROM {} t WHERE {}",
                    ident(column),
                    self.plans[c].active,
                    column_in("$1"),
                ))
                .bind(&parent_keys)
                .fetch_all(&mut *conn)
                .await?;
                let mut of_parent: BTreeMap<String, Vec<String>> = BTreeMap::new();
                for (follower, parent_key) in followers {
                    of_parent.entry(parent_key).or_default().push(follower);
                }
                for (replica, keys) in &sent {
                    for key in keys {
                        if held.contains_key(&(*replica, key.clone())) {
                            continue;
                        }
                        for follower in of_parent.get(key).into_iter().flatten() {
                            delta.upsert(*replica, c, follower.clone());
                        }
                    }
                }
            }

            // Parents taken back take their followers first.
            let taken = delta.taken(parent);
            let parent_keys: Vec<String> = taken
                .iter()
                .flat_map(|(_, keys)| keys.iter().cloned())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            if !parent_keys.is_empty() {
                let replicas: Vec<Uuid> = taken.iter().map(|(e, _)| *e).collect();
                let held: Vec<(Uuid, String, String)> = sqlx::query_as(&format!(
                    "SELECT h.replica_id, h.row_key, t.{}::text FROM {} t \
                       JOIN sync.held h ON h.relation = $1 AND h.replica_id = ANY($2) \
                                       AND h.row_key = {child_key} \
                      WHERE {}",
                    ident(column),
                    self.plans[c].relation.sql(),
                    column_in("$3"),
                ))
                .bind(self.plans[c].table)
                .bind(&replicas)
                .bind(&parent_keys)
                .fetch_all(&mut *conn)
                .await?;
                for (replica, key, parent_key) in held {
                    let taken_here = delta.has_delete(replica, parent, &parent_key);
                    if taken_here && !delta.has_upsert(replica, c, &key) {
                        delta.delete(replica, c, key);
                    }
                }
            }
        }
        Ok(())
    }

    /// Loads and encodes every row the delta sends, once however many
    /// replicas it goes to, in the plan's transaction. A row that does not
    /// encode, or is larger than `largest_row` bytes, which no message could
    /// carry, cannot go: the delta is planned again around it, and it is
    /// answered in [`Loaded::failed`] for each replica it was to go to — with
    /// every row that points at it there, which the replica could not apply.
    /// A replica holding an older version of one has it taken back, with
    /// what points at it, rather than keep a version the engine no longer
    /// knows the contents of; each comes back once a fix lets it through.
    pub async fn load<P: std::marker::Sync + 'static>(
        &self,
        conn: &mut Conn,
        codecs: &[Codec<P>],
        delta: &mut Delta,
        largest_row: usize,
    ) -> Result<Loaded, Error> {
        let mut loaded = Loaded::default();
        let mut refused: BTreeMap<(usize, String), String> = BTreeMap::new();
        loop {
            let mut wanted: BTreeMap<usize, BTreeSet<String>> = BTreeMap::new();
            for tables in delta.upserts.values() {
                for (i, keys) in tables {
                    wanted.entry(*i).or_default().extend(
                        keys.iter()
                            .filter(|k| !loaded.rows.contains_key(&(*i, (*k).clone())))
                            .cloned(),
                    );
                }
            }
            for (i, keys) in wanted {
                if keys.is_empty() {
                    continue;
                }
                let plan = &self.plans[i];
                let codec = codecs
                    .iter()
                    .find(|c| c.table.name == plan.table)
                    .ok_or_else(|| {
                        Error::Misuse(format!("no codec for mirrored table `{}`", plan.table))
                    })?;
                let parsed: Vec<Key> = keys
                    .iter()
                    .map(|k| parse_key(&plan.key, k))
                    .collect::<Result<_, _>>()?;
                let encoded = codec.load(conn, &parsed).await?;
                for (key, bytes) in encoded.rows {
                    if bytes.len() > largest_row {
                        let reason = format!(
                            "a row of {} bytes, larger than any message may be ({largest_row})",
                            bytes.len()
                        );
                        refused.insert((i, key_text(&key)), reason);
                        continue;
                    }
                    loaded.rows.insert((i, key_text(&key)), bytes);
                }
                for (key, e) in encoded.failed {
                    refused.insert((i, key_text(&key)), e.to_string());
                }
                // The plan read it in this snapshot: a row not there is a
                // bug, set aside like one.
                for key in keys {
                    if !loaded.rows.contains_key(&(i, key.clone())) {
                        refused
                            .entry((i, key))
                            .or_insert_with(|| "the plan's snapshot has no such row".to_owned());
                    }
                }
            }
            let mut blocked = delta.blocked.clone();
            for (replica, tables) in &delta.upserts {
                for (i, keys) in tables {
                    for key in keys {
                        if let Some(reason) = refused.get(&(*i, key.clone())) {
                            blocked.insert((*replica, *i, key.clone()), reason.clone());
                        }
                    }
                }
            }
            if blocked.len() == delta.blocked.len() {
                break;
            }
            let again = Delta {
                changes: delta.changes.clone(),
                blocked,
                only: delta.only,
                left_out: delta.left_out.clone(),
                ..Delta::default()
            };
            *delta = self.plan_around(conn, again).await?;
        }
        loaded.failed = delta
            .blocked
            .iter()
            .map(|((replica, i, key), reason)| Unsent {
                replica: *replica,
                table: self.plans[*i].table,
                key: key.clone(),
                reason: reason.clone(),
            })
            .collect();
        Ok(loaded)
    }

    /// One replica's rows from what [`Engine::load`] read: its upserts
    /// parents first, then its deletes children first.
    pub fn rows(&self, delta: &Delta, loaded: &Loaded, replica: Uuid) -> Vec<envelope::Table> {
        let mut out = Vec::new();
        if let Some(tables) = delta.upserts.get(&replica) {
            for &i in &self.order {
                let Some(keys) = tables.get(&i) else { continue };
                let sent: Vec<(&String, &Vec<u8>)> = keys
                    .iter()
                    .filter_map(|k| loaded.rows.get(&(i, k.clone())).map(|bytes| (k, bytes)))
                    .collect();
                out.push(envelope::Table {
                    table: self.plans[i].table.to_owned(),
                    upserts: sent.iter().map(|(_, bytes)| (*bytes).clone()).collect(),
                    deletes: Vec::new(),
                    keys: sent
                        .iter()
                        .map(|(k, _)| envelope::Key {
                            parts: k.split('|').map(str::to_owned).collect(),
                        })
                        .collect(),
                });
            }
        }
        if let Some(tables) = delta.deletes.get(&replica) {
            for &i in self.order.iter().rev() {
                let Some(keys) = tables.get(&i) else { continue };
                out.push(envelope::Table {
                    table: self.plans[i].table.to_owned(),
                    upserts: Vec::new(),
                    keys: Vec::new(),
                    deletes: keys
                        .iter()
                        .map(|k| envelope::Key {
                            parts: k.split('|').map(str::to_owned).collect(),
                        })
                        .collect(),
                });
            }
        }
        out
    }

    /// Records what the replicas hold once their messages are out: what was
    /// sent, less what was taken back. A snapshot replaces everything its
    /// replica held.
    pub async fn record(&self, conn: &mut Conn, delta: &Delta) -> Result<(), Error> {
        let snapshots: Vec<Uuid> = delta.snapshots.iter().copied().collect();
        if !snapshots.is_empty() {
            sqlx::query("DELETE FROM sync.held WHERE replica_id = ANY($1)")
                .bind(&snapshots)
                .execute(&mut *conn)
                .await?;
        }
        let (mut e, mut t, mut k) = (Vec::new(), Vec::new(), Vec::new());
        for (replica, tables) in &delta.upserts {
            for (i, keys) in tables {
                for key in keys {
                    e.push(*replica);
                    t.push(self.plans[*i].table);
                    k.push(key.as_str());
                }
            }
        }
        for start in (0..e.len()).step_by(RECORD_CHUNK) {
            let range = start..(start + RECORD_CHUNK).min(e.len());
            sqlx::query(
                "INSERT INTO sync.held (replica_id, relation, row_key, horizon) \
                 SELECT e, t, k, $4::text::xid8 \
                   FROM unnest($1::uuid[], $2::text[], $3::text[]) AS u(e, t, k) \
                 ON CONFLICT (replica_id, relation, row_key) \
                 DO UPDATE SET sent_at = now(), horizon = EXCLUDED.horizon",
            )
            .bind(&e[range.clone()])
            .bind(&t[range.clone()])
            .bind(&k[range])
            .bind(delta.horizon.to_string())
            .execute(&mut *conn)
            .await?;
        }

        let (mut e, mut t, mut k) = (Vec::new(), Vec::new(), Vec::new());
        for (replica, tables) in &delta.deletes {
            for (i, keys) in tables {
                for key in keys {
                    e.push(*replica);
                    t.push(self.plans[*i].table);
                    k.push(key.as_str());
                }
            }
        }
        for start in (0..e.len()).step_by(RECORD_CHUNK) {
            let range = start..(start + RECORD_CHUNK).min(e.len());
            sqlx::query(
                "DELETE FROM sync.held h \
                  USING unnest($1::uuid[], $2::text[], $3::text[]) AS d(e, t, k) \
                  WHERE h.replica_id = d.e AND h.relation = d.t AND h.row_key = d.k",
            )
            .bind(&e[range.clone()])
            .bind(&t[range.clone()])
            .bind(&k[range])
            .execute(&mut *conn)
            .await?;
        }
        Ok(())
    }

    /// The tables every snapshot names, parents first.
    pub fn snapshot_tables(&self) -> Vec<&'static str> {
        self.order.iter().map(|&i| self.plans[i].table).collect()
    }

    /// Every replica the delta touches.
    pub fn replicas(delta: &Delta) -> BTreeSet<Uuid> {
        delta
            .upserts
            .keys()
            .chain(delta.deletes.keys())
            .chain(delta.snapshots.iter())
            .copied()
            .collect()
    }
}

/// A note as the engine reads it.
#[derive(sqlx::FromRow)]
struct Noted {
    id: i64,
    kind: String,
    relation: Option<String>,
    row_key: Option<String>,
    replica_id: Option<Uuid>,
    previous: Option<serde_json::Value>,
    scope: Option<Uuid>,
    xid: i64,
}

impl Noted {
    /// The change a note records; `None` for one this build cannot read,
    /// which [`READABLE`] keeps from being read.
    fn change(self) -> Option<Change> {
        Some(match self.kind.as_str() {
            "resync" => Change::Resync {
                replica: self.replica_id?,
            },
            "row" => Change::Row {
                table: self.relation?,
                key: self.row_key?,
                previous: self.previous,
                xid: self.xid,
            },
            "members" => Change::Members {
                table: self.relation?,
                member: self.previous?,
            },
            "assigned" => Change::Assigned {
                table: self.relation?,
                replica: self.replica_id?,
            },
            _ => return None,
        })
    }
}

/// The notes this build reads: one it cannot, from a later build, is left
/// where a later build finds it, and holds back none behind it.
const READABLE: &str = "((kind = 'row' AND relation IS NOT NULL AND row_key IS NOT NULL) \
     OR (kind = 'members' AND relation IS NOT NULL AND previous IS NOT NULL) \
     OR (kind = 'assigned' AND relation IS NOT NULL AND replica_id IS NOT NULL) \
     OR (kind = 'resync' AND replica_id IS NOT NULL))";

/// The pending notes, oldest first: not parked, not owed to a replica that is
/// waiting out a failure, and of a kind this build reads. `conn` is in the
/// `REPEATABLE READ` transaction the notes are planned and loaded in, so the
/// rows they are planned against do not move under the plan.
pub async fn pending(conn: &mut Conn, limit: i64) -> Result<Vec<Note>, Error> {
    let isolation: String = sqlx::query_scalar("SELECT current_setting('transaction_isolation')")
        .fetch_one(&mut *conn)
        .await?;
    if !matches!(isolation.as_str(), "repeatable read" | "serializable") {
        return Err(Error::Misuse(format!(
            "the notes are read in a REPEATABLE READ transaction, and this one is {isolation}: \
             a plan read across snapshots can send a row without its parent"
        )));
    }
    let rows: Vec<Noted> = sqlx::query_as(&format!(
        "SELECT id, kind, relation, row_key, replica_id, previous, scope, xid::text::bigint AS xid \
           FROM sync.notes n \
          WHERE done_at IS NULL AND parked_at IS NULL AND {READABLE} \
            AND NOT EXISTS (SELECT 1 FROM sync.replicas r \
                             WHERE r.replica_id = n.scope AND r.retry_after > now()) \
          ORDER BY id LIMIT $1"
    ))
    .bind(limit)
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(|noted| {
            let (id, scope) = (noted.id, noted.scope);
            noted.change().map(|change| Note { id, change, scope })
        })
        .collect())
}

/// No provisioning of the replica `$1` is pending: one is noted once, however
/// many reasons ask for it. It is planned with the changes of its pass, so the
/// snapshot carries them rather than a second message.
const RESYNC_PENDING: &str = "NOT EXISTS (SELECT 1 FROM sync.notes p WHERE p.kind = 'resync' \
                              AND p.replica_id = $1 AND p.done_at IS NULL)";

/// Queues `replica`'s provisioning from scratch, in the caller's transaction,
/// unless one is pending: it is sent everything it should hold and sweeps the
/// rest.
pub async fn note_resync(
    conn: &mut Conn,
    partition: Option<Uuid>,
    replica: Uuid,
) -> Result<(), Error> {
    sqlx::query(&format!(
        "INSERT INTO sync.notes (kind, replica_id, partition_id) \
         SELECT 'resync', $1, $2 WHERE {RESYNC_PENDING}"
    ))
    .bind(replica)
    .bind(partition)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Marks notes the engine has sent and recorded.
pub async fn mark_done(conn: &mut Conn, ids: &[i64]) -> Result<(), Error> {
    sqlx::query("UPDATE sync.notes SET done_at = now() WHERE id = ANY($1)")
        .bind(ids)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Deletes the notes done longer ago than `age`; answers how many.
pub async fn prune(conn: &mut Conn, age: std::time::Duration) -> Result<u64, Error> {
    let done =
        sqlx::query("DELETE FROM sync.notes WHERE done_at < now() - make_interval(secs => $1)")
            .bind(age.as_secs_f64())
            .execute(&mut *conn)
            .await?;
    Ok(done.rows_affected())
}

/// What a batch of changes may have released: each changed row as it was.
#[derive(Default)]
struct Released {
    previous: BTreeMap<(usize, String), Vec<serde_json::Value>>,
}

/// A grouped table's members and assigned tables, read off their foreign
/// keys: the members table is the one of that name pointing at the grouped
/// table, by its key; the assigned one, of that name, points at the replicas;
/// and both point at one more table, the groups, whose columns pair them.
fn group_of(
    foreign_keys: &[ForeignKey],
    plan: &Plan,
    members: &str,
    assigned: &str,
    replicas: &str,
) -> Result<Group, Error> {
    let bad = |why: &str| {
        Error::Misuse(format!(
            "`{}` is grouped by `{members}` and `{assigned}`, but {why}",
            plan.table
        ))
    };
    fn one<'f>(
        found: Vec<&'f ForeignKey>,
        what: &str,
        bad: impl Fn(&str) -> Error,
    ) -> Result<&'f ForeignKey, Error> {
        match found.as_slice() {
            [fk] => Ok(*fk),
            [] => Err(bad(&format!("no {what}"))),
            _ => Err(bad(&format!("more than one {what}"))),
        }
    }
    let (member_table, _, member_columns, parent_columns) = one(
        foreign_keys
            .iter()
            .filter(|(c, p, _, _)| c.name == members && *p == plan.relation)
            .collect(),
        "member key points at it",
        bad,
    )?;
    let member_row: Option<Vec<String>> = plan
        .key
        .iter()
        .map(|k| {
            parent_columns
                .iter()
                .position(|c| c == k.name)
                .map(|i| member_columns[i].clone())
        })
        .collect();
    let member_row = member_row
        .filter(|_| parent_columns.len() == plan.key.len())
        .ok_or_else(|| bad("its members do not point at it by its key"))?;
    let (assigned_table, _, replica_columns, _) = one(
        foreign_keys
            .iter()
            .filter(|(c, p, _, _)| c.name == assigned && p.name == replicas)
            .collect(),
        "assigned key points at a replica",
        bad,
    )?;
    let [assigned_replica] = replica_columns.as_slice() else {
        return Err(bad(
            "the assigned table points at a replica by more than its id",
        ));
    };
    let into_groups = |table: &Relation| -> Vec<&ForeignKey> {
        foreign_keys
            .iter()
            .filter(|(c, p, _, _)| c == table && *p != plan.relation && p.name != replicas)
            .collect()
    };
    let group = into_groups(member_table)
        .into_iter()
        .find_map(|(_, groups, members_cols, members_parent)| {
            let (_, _, assigned_cols, assigned_parent) = into_groups(assigned_table)
                .into_iter()
                .find(|(_, p, _, _)| p == groups)?;
            members_parent
                .iter()
                .zip(members_cols)
                .map(|(parent, m)| {
                    assigned_parent
                        .iter()
                        .position(|a| a == parent)
                        .map(|i| (m.clone(), assigned_cols[i].clone()))
                })
                .collect::<Option<Vec<_>>>()
        })
        .ok_or_else(|| bad("the two do not point at one table of groups"))?;
    Ok(Group {
        members: member_table.clone(),
        member_row,
        group,
        assigned: assigned_table.clone(),
        assigned_replica: assigned_replica.clone(),
    })
}

/// Parents before children; a cycle between mirrored tables is refused.
fn topological(plans: &[Plan]) -> Result<Vec<usize>, Error> {
    let mut order = Vec::with_capacity(plans.len());
    let mut state = vec![0u8; plans.len()];
    fn visit(
        i: usize,
        plans: &[Plan],
        state: &mut [u8],
        order: &mut Vec<usize>,
    ) -> Result<(), Error> {
        match state[i] {
            2 => return Ok(()),
            1 => {
                return Err(Error::Misuse(format!(
                    "mirrored tables point at each other in a cycle through `{}`",
                    plans[i].table
                )))
            }
            _ => {}
        }
        state[i] = 1;
        for reference in &plans[i].references {
            visit(reference.parent, plans, state, order)?;
        }
        state[i] = 2;
        order.push(i);
        Ok(())
    }
    for i in 0..plans.len() {
        visit(i, plans, &mut state, &mut order)?;
    }
    Ok(order)
}

#[cfg(test)]
#[path = "plan_tests.rs"]
mod plan_tests;

#[cfg(test)]
#[path = "plan_bench.rs"]
mod plan_bench;

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// The engine's schema as it ships, whatever the cluster's template
    /// already holds: a host may have primed it with its own copy.
    pub(crate) async fn install(conn: &mut Conn) {
        sqlx::raw_sql("DROP SCHEMA IF EXISTS sync CASCADE")
            .execute(&mut *conn)
            .await
            .expect("drop the schema");
        sqlx::raw_sql(crate::POSTGRES_SCHEMA)
            .execute(&mut *conn)
            .await
            .expect("the schema");
    }

    #[test]
    fn a_change_is_sent_again_only_where_the_held_version_predates_it() {
        let replica = Uuid::now_v7();
        let held = |horizon: i64| BTreeMap::from([((replica, "k".to_owned()), horizon)]);
        let notes = BTreeMap::from([("k".to_owned(), 100_i64)]);
        let holder = (replica, "k".to_owned());

        assert!(Engine::owed(&BTreeMap::new(), None, &holder), "not held");
        assert!(
            Engine::owed(&held(90), Some(&notes), &holder),
            "sent before the change"
        );
        assert!(
            Engine::owed(&held(100), Some(&notes), &holder),
            "the change still running then"
        );
        assert!(
            !Engine::owed(&held(101), Some(&notes), &holder),
            "the send carried it"
        );
        assert!(
            !Engine::owed(&held(90), None, &holder),
            "only who holds it moved"
        );
    }
}
