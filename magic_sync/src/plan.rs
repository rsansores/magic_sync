//! The planner: which rows each replica holds, and what to send it so it holds
//! exactly those.
//!
//! Every table the server sends down declares how its rows find replicas
//! (its `sync = "…"`); nothing here names a table. From the declarations and the
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
//!
//! A host meets three things here: [`Replicas`], where the server finds its
//! replicas; [`Engine`], which prints the triggers the server needs and
//! answers about its progress; and [`note_resync`], which provisions a
//! replica again on demand.

use crate::dialect::{ident, literal};
use crate::row::{key_text, Column, Key, KeyPart, Mirror, Route, Table};
use crate::wire::Codec;
use crate::Error;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use uuid::Uuid;

/// Where the server finds its replicas: the rows that are replicas, and
/// which of them are live (have somewhere to send to). The engine compares
/// them with what it last saw on every pass: a replica that became live, or
/// moved to another partition, is provisioned again from scratch; one that
/// stopped being live, or is gone, is forgotten and sent nothing.
///
/// ```
/// use magic_sync::plan::Replicas;
///
/// pub const REGISTERS: Replicas = Replicas::relation("shop.registers")
///     .table("registers")
///     .live("active");
/// pub const DEVICES: Replicas = Replicas::relation("fleet.devices")
///     .table("devices")
///     .live("enrolled_at IS NOT NULL")
///     .partitioned_by("company_id");
/// ```
#[derive(Clone, Copy, Debug)]
pub struct Replicas {
    pub(crate) relation: &'static str,
    pub(crate) table: &'static str,
    pub(crate) live: &'static str,
    pub(crate) partition: Option<&'static str>,
}

impl Replicas {
    /// The replicas are the rows of `relation`, as SQL names it (a table or a
    /// view, with an `id` uuid column) — every one live, in one partition,
    /// until said otherwise; name the [table](Replicas::table) behind it.
    #[must_use]
    pub const fn relation(relation: &'static str) -> Self {
        Replicas {
            relation,
            table: "",
            live: "true",
            partition: None,
        }
    }

    /// The table behind the relation, as a foreign key names it: required.
    #[must_use]
    pub const fn table(self, table: &'static str) -> Self {
        Replicas { table, ..self }
    }

    /// Which replicas receive anything: an SQL condition on the relation's
    /// columns.
    #[must_use]
    pub const fn live(self, condition: &'static str) -> Self {
        Replicas {
            live: condition,
            ..self
        }
    }

    /// The replicas, split into partitions no row crosses by the uuid
    /// `column` — the relation's, and every partitioned table's: a broadcast
    /// row goes to every live replica of its partition.
    #[must_use]
    pub const fn partitioned_by(self, column: &'static str) -> Self {
        Replicas {
            partition: Some(column),
            ..self
        }
    }
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
    route: Route,
    references: Vec<Reference>,
    group: Option<Group>,
    /// The columns the replicas own: a write of only these is no change here.
    owned: Vec<&'static str>,
    /// Every column a row of it carries.
    columns: Vec<&'static str>,
}

/// A plan: what a batch of changes asks of every replica. The rows it sends
/// and takes back are not here but in the temporary tables of the
/// connection that planned it (see [`Engine::plan`]), read a chunk at a time
/// by what records them, in the same transaction: a plan is never in memory
/// whole, however large a replica it provisions.
#[derive(Default, Debug)]
pub(crate) struct Delta {
    /// Replicas being provisioned: what they held is forgotten and what they
    /// should hold sent whole, then everything else swept on their side.
    pub(crate) snapshots: BTreeSet<Uuid>,
    /// The snapshot the plan read, as PostgreSQL writes one: a row sent from
    /// it holds the change of every transaction it sees.
    pub(crate) snapshot: String,
    /// What was planned, for [`Engine::replan`] to plan it again around a
    /// row that cannot go.
    pub(crate) notes: Vec<Note>,
    /// Rows that cannot go to a replica, and why: never sent it, and taken
    /// back where it holds them, with whatever points at them.
    pub(crate) blocked: BTreeMap<(Uuid, usize, String), String>,
}

/// What a batch asks about: the tables with rows changed, members moved and
/// groups given or taken; whether a changed row released a parent, and
/// whether a replica is provisioned.
#[derive(Default)]
struct Asked {
    changed: BTreeSet<usize>,
    regrouped: BTreeSet<usize>,
    reassigned: BTreeSet<usize>,
    released: bool,
    fresh: bool,
}

/// A pending note: what changed.
#[derive(Clone, Debug)]
pub(crate) struct Note {
    pub(crate) id: i64,
    pub(crate) change: Change,
}

/// What changed, as the outbox noted it.
#[derive(Clone, Debug)]
pub(crate) enum Change {
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

/// Rows of one table, encoded: by key text, and the ones that cannot go,
/// with why.
#[derive(Default)]
pub(crate) struct Encoded {
    pub(crate) rows: BTreeMap<String, Vec<u8>>,
    pub(crate) failed: BTreeMap<String, String>,
}

/// A row the engine could not send a replica, and why.
#[derive(Debug)]
pub(crate) struct Unsent {
    pub(crate) replica: Uuid,
    pub(crate) table: &'static str,
    pub(crate) key: String,
    pub(crate) reason: String,
}

/// A foreign key as the catalogue reads it: the child, the parent, then the
/// child's and the parent's columns, in order.
type ForeignKey = (Relation, Relation, Vec<String>, Vec<String>);

/// A [`ForeignKey`] as the catalogue query answers it, each relation as its
/// schema and name.
type CatalogForeignKey = (String, String, String, String, Vec<String>, Vec<String>);

/// Rows a pass loads, encodes and records at a time: what it holds in
/// memory however much it sends.
pub(crate) const RECORD_CHUNK: usize = 2_000;

/// The transaction-scoped advisory lock installs take, so two processes
/// installing at once run one after the other ("SYNCINST").
const INSTALL_LOCK: i64 = 0x5359_4e43_494e_5354;

/// The engine's schema, one script per version: a database at version `n`
/// runs the scripts after the `n`th, in one transaction. Each script ends by
/// writing its version.
const MIGRATIONS: [&str; crate::SCHEMA_VERSION as usize] = [crate::POSTGRES_SCHEMA];

/// The server's engine, built once from the codecs and the schema by the
/// [`Server`](crate::Server), which hands it out for what a host asks of it:
/// the triggers it needs ([`Engine::triggers`]), and how far along every
/// replica is ([`status`](crate::status)).
pub struct Engine {
    pub(crate) plans: Vec<Plan>,
    by_name: HashMap<&'static str, usize>,
    /// Parents before children.
    pub(crate) order: Vec<usize>,
    pub(crate) replicas: Replicas,
}

use crate::Conn;

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
            KeyPart::sql_type(typed.key)
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
                KeyPart::sql_type(typed.key)
            )
        })
        .collect();
    format!(
        "({}) IN (SELECT {} FROM unnest({param}::text[]) AS k)",
        row.join(", "),
        parts.join(", ")
    )
}

/// `columns` of the row aliased `alias` equal, part by part, to the key text
/// `text` (an SQL expression), each part cast back to its type in `types`:
/// what lets an index on those columns serve.
fn key_eq(alias: &str, columns: &[impl AsRef<str>], types: &[Column], text: &str) -> String {
    let row: Vec<String> = columns
        .iter()
        .map(|c| format!("{alias}.{}", ident(c.as_ref())))
        .collect();
    let parts: Vec<String> = types
        .iter()
        .enumerate()
        .map(|(i, typed)| {
            let part = if types.len() == 1 {
                text.to_owned()
            } else {
                format!("split_part({text}, '|', {})", i + 1)
            };
            format!("({part})::{}", KeyPart::sql_type(typed.key))
        })
        .collect();
    format!("({}) = ({})", row.join(", "), parts.join(", "))
}

/// Has PostgreSQL count what the plan's temporary `tables` hold, which it
/// cannot do by itself: so it plans what reads them for what they hold.
async fn analyze(conn: &mut Conn, tables: &str) -> Result<(), Error> {
    sqlx::Executor::execute(&mut *conn, format!("ANALYZE {tables}").as_str()).await?;
    Ok(())
}

/// A set of rows a plan is worked out in.
#[derive(Clone, Copy)]
enum Set {
    Wanted,
    Suspect,
    Blocked,
    /// What replicas keep of a table.
    Kept(usize),
}

impl Set {
    fn table(self) -> &'static str {
        match self {
            Set::Wanted => "sync_wanted",
            Set::Suspect => "sync_suspect",
            Set::Blocked => "sync_blocked",
            Set::Kept(_) => "sync_kept",
        }
    }
}

/// The rows of `f` — `replica_id` and `row_key`, a row of `table`, worked
/// out apart — that their replica holds, as a join: looked up by the whole
/// key, however many rows of the table it holds.
fn held_by(f: &str, table: &str) -> String {
    format!(
        "JOIN sync_holds h ON h.replica_id = {f}.replica_id AND h.relation = {} \
                          AND h.row_key = {f}.row_key",
        literal(table)
    )
}

/// Every one of `columns` of the row aliased `alias` holds a value.
fn not_null(alias: &str, columns: &[String]) -> String {
    columns
        .iter()
        .map(|c| format!("{alias}.{} IS NOT NULL", ident(c)))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// The temporary tables a plan is worked out in, on the connection that
/// plans, emptied at every commit: the live replicas, those provisioned,
/// the batch's notes and what they changed, the rows that cannot go, those
/// a replica's rules send it, those whose place is in question and those it
/// keeps; and what each replica holds as the plan counts it — nothing, for
/// one provisioned, and refused while the last version sent it is the one
/// it refused. They have no index: what reads them reads them whole,
/// and a large plan fills them faster without, and joins them by hash.
const PLAN_TABLES: &str = "
CREATE TEMP TABLE IF NOT EXISTS sync_live (replica_id UUID) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS sync_fresh (replica_id UUID) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS sync_batch (id BIGINT) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS sync_changed (
    rel INTEGER, relation TEXT, row_key TEXT, xid XID8) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS sync_regrouped (rel INTEGER, relation TEXT, row_key TEXT)
    ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS sync_reassigned (rel INTEGER, relation TEXT, replica_id UUID)
    ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS sync_released (
    relation TEXT, row_key TEXT, parent INTEGER, parent_relation TEXT, parent_key TEXT)
    ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS sync_blocked (
    replica_id UUID, rel INTEGER, row_key TEXT, why TEXT, round INTEGER) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS sync_wanted (replica_id UUID, rel INTEGER, row_key TEXT)
    ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS sync_suspect (
    replica_id UUID, rel INTEGER, row_key TEXT, round INTEGER) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS sync_staged (
    replica_id UUID, rel INTEGER, row_key TEXT, why TEXT, round INTEGER) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS sync_kept (
    replica_id UUID, rel INTEGER, row_key TEXT, round INTEGER,
    sends BOOLEAN, up BOOLEAN, down BOOLEAN) ON COMMIT DELETE ROWS;
CREATE OR REPLACE TEMP VIEW sync_holds AS
    SELECT h.replica_id, h.relation, h.row_key, h.snapshot, h.refused >= h.sequence AS refused
      FROM sync.held h
     WHERE NOT EXISTS (SELECT 1 FROM sync_fresh f WHERE f.replica_id = h.replica_id);
";

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
        .map(|(column, part)| {
            KeyPart::parse(column.key, part).ok_or_else(|| Error::Malformed {
                table: String::new(),
                reason: format!("`{part}` is no value of the key column `{}`", column.name),
            })
        })
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
    /// Fires on every row of every write: enabled for every session but a
    /// replication one, or for every session — not disabled, not for
    /// replication sessions only — with no `WHEN` and no column list.
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
                t.tgenabled IN ('O', 'A') AND t.tgqual IS NULL AND t.tgattr::text = '' AS plain \
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
    let installed = match version(conn).await? {
        Some(installed) => installed,
        None => {
            refuse_foreign_schema(conn).await?;
            0
        }
    };
    let from = usize::try_from(installed).unwrap_or(usize::MAX);
    if from > MIGRATIONS.len() {
        return Err(Error::Misuse(format!(
            "the engine's schema is version {installed}, from a later build than this one ({})",
            crate::SCHEMA_VERSION
        )));
    }
    for script in &MIGRATIONS[from..] {
        // Not `raw_sql`: its future is not `Send` for every lifetime a
        // host's spawn asks of it.
        sqlx::Executor::execute(&mut *conn, *script).await?;
    }
    Ok(())
}

/// Refuses a database whose schema `sync` is not the engine's — a host's
/// own, of that name — rather than install into it, or fail half-way.
async fn refuse_foreign_schema(conn: &mut Conn) -> Result<(), Error> {
    let taken: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'sync')")
            .fetch_one(&mut *conn)
            .await?;
    if taken {
        return Err(Error::Misuse(
            "the database has a schema named `sync` that is not the engine's: the engine \
             keeps its tables in `sync`, so that one needs another name"
                .to_owned(),
        ));
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
    /// table's group tables from the catalog, and checks that every table is
    /// its row column for column and that every trigger the engine needs is
    /// there as [`Engine::triggers`] writes it. Fails on a declaration the
    /// schema cannot honour — a table that differs from its row, a `follows`
    /// column that is not a key, a group table that does not have the columns
    /// a group needs, a cycle — and on a trigger missing or different.
    pub(crate) async fn new(
        conn: &mut Conn,
        tables: &[&Table],
        replicas: Replicas,
    ) -> Result<Self, Error> {
        crate::row::parity(conn, tables).await?;
        let engine = Self::read(conn, tables, replicas).await?;
        let temporary: bool =
            sqlx::query_scalar("SELECT has_database_privilege(current_database(), 'TEMPORARY')")
                .fetch_one(&mut *conn)
                .await?;
        if !temporary {
            return Err(Error::Misuse(
                "the engine's role may not create temporary tables, which the relay plans in: \
                 grant it TEMPORARY on the database"
                    .to_owned(),
            ));
        }
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
    pub(crate) async fn install(
        conn: &mut Conn,
        tables: &[&Table],
        replicas: Replicas,
    ) -> Result<Self, Error> {
        migrate(conn).await?;
        crate::row::parity(conn, tables).await?;
        let engine = Self::read(conn, tables, replicas).await?;
        let live = live_triggers(conn).await?;
        for stray in engine.strays(&live) {
            let drop = format!(
                "DROP TRIGGER {} ON {}",
                ident(&stray.name),
                stray.on().sql()
            );
            sqlx::Executor::execute(&mut *conn, drop.as_str()).await?;
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
                let drop = format!("DROP TRIGGER {} ON {}", expected.name, expected.on.sql());
                sqlx::Executor::execute(&mut *conn, drop.as_str()).await?;
            } else if expected.function == "note_row" {
                let has_rows: bool = sqlx::query_scalar(&format!(
                    "SELECT EXISTS (SELECT 1 FROM {})",
                    expected.on.sql()
                ))
                .fetch_one(&mut *conn)
                .await?;
                first_noted |= has_rows;
            }
            sqlx::Executor::execute(&mut *conn, expected.ddl().as_str()).await?;
        }
        if first_noted {
            // Rows written before their table was noted reached no replica:
            // every replica is provisioned again, and gets them.
            sqlx::query(&resync_sql(&format!(
                "SELECT s.id, s.partition FROM {}",
                engine.replicas_sql()
            )))
            .execute(&mut *conn)
            .await?;
        }
        Ok(engine)
    }

    /// The statements that create every trigger the server needs for
    /// `codecs`, one per line, read off the database `conn` is on: what a
    /// host with migrations writes in the migration that creates the tables —
    /// after [`POSTGRES_SCHEMA`](crate::POSTGRES_SCHEMA) — and what
    /// [`Server::install`](crate::Server::install) runs. A mirrored table
    /// notes every write that changes a column it does not leave to the
    /// replicas, and refuses a truncate; a group table notes every write.
    /// [`Server::new`](crate::Server::new) refuses to start while one is
    /// missing or differs.
    ///
    /// A trigger as printed fires in every session but one whose
    /// `session_replication_role` is `replica` — a logical replication
    /// subscription applying changes, a restore told to disable triggers —
    /// and what such a session writes is never noted. Where one writes
    /// mirrored tables, make their triggers `ENABLE ALWAYS`: the engine takes
    /// a trigger enabled always as well as one enabled as printed, and
    /// refuses one disabled, or enabled for replication sessions only.
    ///
    /// ```no_run
    /// # async fn print(pool: sqlx::PgPool, codecs: Vec<magic_sync::wire::Codec<()>>) -> Result<(), magic_sync::Error> {
    /// use magic_sync::plan::{Engine, Replicas};
    ///
    /// const REGISTERS: Replicas = Replicas::relation("shop.registers").table("registers").live("active");
    /// let mut conn = pool.acquire().await?;
    /// println!("{}", Engine::triggers(&mut conn, &codecs, REGISTERS).await?);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`Error::Database`] when the database fails, and [`Error::Misuse`] when
    /// the engine's schema is missing or at another version, or a declaration
    /// is one the schema cannot honour.
    pub async fn triggers<P: std::marker::Sync + 'static>(
        conn: &mut Conn,
        codecs: &[Codec<P>],
        replicas: Replicas,
    ) -> Result<String, Error> {
        let tables: Vec<&Table> = codecs.iter().map(|c| &c.table).collect();
        let engine = Self::read(conn, &tables, replicas).await?;
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
            if let (Some(g), Route::Grouped { members, assigned }) = (&plan.group, plan.route) {
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
        if replicas.table.is_empty() {
            return Err(Error::Misuse(format!(
                "the replicas of `{}` name no table: `Replicas::table` says which is behind it",
                replicas.relation
            )));
        }
        let needed = crate::SCHEMA_VERSION;
        match version(conn).await? {
            Some(v) if v == needed => {}
            Some(v) if v > needed => {
                return Err(Error::Misuse(format!(
                    "the engine's schema is version {v}, from a later build than this one \
                     ({needed}): run that build, or a later one"
                )))
            }
            v => {
                if v.is_none() {
                    refuse_foreign_schema(conn).await?;
                }
                return Err(Error::Misuse(format!(
                    "the engine's schema is {}, and this build needs version {needed}: \
                     `Server::install` brings it there, or a migration of yours runs the \
                     engine's steps (the book's \"Upgrading\")",
                    v.map_or("not installed".to_owned(), |v| format!("version {v}"))
                )));
            }
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
            let primary = crate::dialect::primary_key(conn, t.schema, t.name).await?;
            if primary.iter().map(String::as_str).collect::<BTreeSet<_>>() != declared {
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
                route: t.route,
                references: Vec::new(),
                group: None,
                owned: t
                    .columns
                    .iter()
                    .filter(|c| c.replica_owned)
                    .map(|c| c.name)
                    .collect(),
                columns: t.columns.iter().map(|c| c.name).collect(),
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
            match plan.route {
                Route::Follows(column) => {
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
                Route::Grouped { members, assigned } => {
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

        let partition = replicas.partition.unwrap_or_default();
        let replicas_typed = typed_columns(conn, replicas.relation).await?;
        let type_of = |columns: &[(String, String)], column: &str| {
            columns
                .iter()
                .find(|(c, _)| c == column)
                .map(|(_, t)| t.clone())
        };
        if replicas_typed.is_empty() {
            return Err(Error::Misuse(format!(
                "the replicas' relation `{}` is no table or view of the database",
                replicas.relation
            )));
        }
        for column in ["id", partition].into_iter().filter(|c| !c.is_empty()) {
            if type_of(&replicas_typed, column).as_deref() != Some("uuid") {
                return Err(Error::Misuse(format!(
                    "the replicas' relation `{}` has no uuid column `{column}`",
                    replicas.relation
                )));
            }
        }
        readable(
            conn,
            &format!("SELECT s.id FROM {} LIMIT 0", Self::replicas_of(replicas)),
            &format!(
                "the replicas, `{}` where `{}`",
                replicas.relation, replicas.live
            ),
        )
        .await?;
        let mut noted: Vec<Relation> = plans.iter().map(|p| p.relation.clone()).collect();
        noted.extend(
            plans
                .iter()
                .filter_map(|p| p.group.as_ref())
                .flat_map(|g| [g.members.clone(), g.assigned.clone()]),
        );
        for relation in &noted {
            let columns = typed_columns(conn, &relation.sql()).await?;
            // The triggers note the partition of every row that has one.
            if type_of(&columns, partition).is_some_and(|t| t != "uuid") {
                return Err(Error::Misuse(format!(
                    "{} has a column `{partition}` that is not a uuid: the replicas are \
                     partitioned by the uuid of that name",
                    relation.sql()
                )));
            }
        }
        for plan in &plans {
            readable(
                conn,
                &format!("SELECT 1 FROM {} LIMIT 0", plan.active),
                &format!("the active rows of `{}`, `{}`", plan.table, plan.active),
            )
            .await?;
            let columns = typed_columns(conn, &plan.relation.sql()).await?;
            let has = |column: &str| type_of(&columns, column).is_some();
            let missing = match plan.route {
                Route::Broadcast => replicas.partition.filter(|p| !has(p)),
                Route::Assigned(column) if !has(column) => Some(column),
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

    /// What decides which rows each replica holds and what a row carries,
    /// as one text: the replicas, and per table, parents first, its key,
    /// the rule that routes it, what it points at, its groups and its
    /// columns. Two builds that write the same text place every row alike.
    fn declared(&self) -> String {
        use std::fmt::Write as _;
        let r = &self.replicas;
        let mut out = format!(
            "replicas {} ({}) live {} partition {:?}\n",
            r.relation, r.table, r.live, r.partition
        );
        for &i in &self.order {
            let p = &self.plans[i];
            let key: Vec<String> = p
                .key
                .iter()
                .map(|c| format!("{} {}", c.name, KeyPart::sql_type(c.key)))
                .collect();
            let references: Vec<String> = p
                .references
                .iter()
                .map(|r| format!("{} {:?}", self.plans[r.parent].table, r.columns))
                .collect();
            let _ = writeln!(
                out,
                "{} {} active {} key {key:?} {:?} references {references:?} group {:?} columns {:?}",
                p.table,
                p.relation.sql(),
                p.active,
                p.route,
                p.group,
                p.columns,
            );
        }
        out
    }

    pub(crate) fn index(&self, table: &str) -> Option<usize> {
        self.by_name.get(table).copied()
    }

    pub(crate) fn table(&self, index: usize) -> &'static str {
        self.plans[index].table
    }

    /// The live replicas, as a subquery `s(id, partition)`: each once, in
    /// the first of its partitions should the relation repeat it.
    pub(crate) fn replicas_sql(&self) -> String {
        Self::replicas_of(self.replicas)
    }

    fn replicas_of(replicas: Replicas) -> String {
        let partition = replicas.partition.map_or("NULL::uuid".to_owned(), ident);
        format!(
            "(SELECT DISTINCT ON (id) id, {partition} AS partition FROM {} WHERE {} \
               ORDER BY id, {partition}) AS s",
            replicas.relation, replicas.live
        )
    }

    /// Whether `replica` is a row of the replicas' relation, live or not.
    pub(crate) async fn is_replica(&self, conn: &mut Conn, replica: Uuid) -> Result<bool, Error> {
        Ok(sqlx::query_scalar(&format!(
            "SELECT EXISTS (SELECT 1 FROM {} WHERE id = $1)",
            self.replicas.relation
        ))
        .bind(replica)
        .fetch_one(&mut *conn)
        .await?)
    }

    /// Compares the live replicas with what the engine saw of them last time,
    /// in the caller's transaction: a replica that became live, or moved to
    /// another partition, is noted for provisioning from scratch; one that is
    /// no longer live, or gone, is forgotten — nothing is recorded as held by
    /// it, what waited in its outbox is dropped, and nothing is sent it until
    /// it is live again. When what places the rows changed since the engine
    /// last ran, every live replica is provisioned again. The relay runs it
    /// before every pass.
    pub(crate) async fn observe(&self, conn: &mut Conn) -> Result<(), Error> {
        sqlx::query(&format!(
            "WITH now AS (SELECT s.id, s.partition FROM {}), \
             seen AS (SELECT replica_id, partition_id FROM sync.replicas WHERE live), \
             arrived AS (SELECT n.id, n.partition FROM now n \
                          WHERE NOT EXISTS (SELECT 1 FROM seen s WHERE s.replica_id = n.id \
                                              AND s.partition_id IS NOT DISTINCT FROM n.partition)), \
             gone AS (SELECT s.replica_id FROM seen s \
                       WHERE NOT EXISTS (SELECT 1 FROM now n WHERE n.id = s.replica_id)), \
             provisioned AS ({}), \
             forgotten AS (DELETE FROM sync.held h USING gone g WHERE h.replica_id = g.replica_id), \
             untaken AS (DELETE FROM sync.gone t USING gone g WHERE t.replica_id = g.replica_id), \
             dropped AS (DELETE FROM sync.outbox o USING gone g WHERE o.replica_id = g.replica_id), \
             unchained AS (DELETE FROM sync.chains c USING gone g WHERE c.replica_id = g.replica_id), \
             came AS (INSERT INTO sync.replicas AS r (replica_id, live, partition_id) \
                      SELECT id, true, partition FROM arrived \
                      ON CONFLICT (replica_id) DO UPDATE SET live = true, partition_id = EXCLUDED.partition_id) \
             UPDATE sync.replicas r SET live = false, partition_id = NULL \
               FROM gone g WHERE r.replica_id = g.replica_id",
            self.replicas_sql(),
            resync_sql("SELECT id, partition FROM arrived")
        ))
        .execute(&mut *conn)
        .await?;
        self.redeclared(conn).await
    }

    /// Compares what decides which rows each replica holds and what a row
    /// carries with what the engine last ran with: when it changed — a
    /// deploy that routes a table otherwise, mirrors one more or one less,
    /// sends another column — every live replica is provisioned again, and
    /// what the engine kept of a table it no longer mirrors is let go of.
    async fn redeclared(&self, conn: &mut Conn) -> Result<(), Error> {
        let declared = self.declared();
        let before: Option<Option<String>> = sqlx::query_scalar(
            "UPDATE sync.version v SET declared = md5($1) FROM sync.version was \
              WHERE v.declared IS DISTINCT FROM md5($1) RETURNING was.declared",
        )
        .bind(&declared)
        .fetch_optional(&mut *conn)
        .await?;
        let Some(Some(_)) = before else {
            return Ok(());
        };
        tracing::warn!("sync: what places the rows changed since the engine last ran — every replica is provisioned again");
        let tables: Vec<&str> = self.plans.iter().map(|p| p.table).collect();
        for sql in [
            "DELETE FROM sync.held WHERE relation <> ALL($1)",
            "DELETE FROM sync.gone WHERE relation <> ALL($1)",
            "DELETE FROM sync.quarantine WHERE relation <> ALL($1)",
            "UPDATE sync.notes SET done_at = now() \
              WHERE done_at IS NULL AND kind = 'row' AND relation <> ALL($1)",
        ] {
            sqlx::query(sql).bind(&tables).execute(&mut *conn).await?;
        }
        sqlx::query(&resync_sql(&format!(
            "SELECT s.id, s.partition FROM {}",
            self.replicas_sql()
        )))
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
        Some(match plan.route {
            Route::Referenced | Route::Follows(_) => return None,
            Route::Broadcast => format!(
                "SELECT s.id AS replica_id, {key} AS key FROM {view} t \
                 JOIN {replicas} ON {same_partition} WHERE {filter}"
            ),
            Route::Assigned(column) => format!(
                "SELECT s.id AS replica_id, {key} AS key FROM {view} t \
                 JOIN {replicas} ON s.id = t.{} WHERE {filter}",
                ident(column)
            ),
            Route::Grouped { .. } => {
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

    /// What a batch of notes asks of every replica, worked out in the
    /// caller's `REPEATABLE READ` transaction, in temporary tables of its
    /// connection: one plan, so no part of it undoes another.
    ///
    /// What each replica should hold is the least set closed under three
    /// rules: the rows its tables' rules send it, every row one of those
    /// points at, and every row that follows one of those. The plan works
    /// that out only around what changed. The rows whose place is in question
    /// for a replica — those it holds that a change may have taken out of
    /// its set: the changed rows it holds and should not, the parents a
    /// changed row pointed at before, rows that cannot go — are closed over
    /// what they point at and what follows them among the rows it holds.
    /// Every row it holds outside them stays, as recorded. From the rows its
    /// rules send it, the rows in question that something outside them
    /// points at, and the followers of a parent it keeps outside them, what
    /// it keeps spreads to the parents and followers it needs, a round at a
    /// time from the rows the last round added. What it lacks of that is
    /// sent, with the rows that changed since its version; the rows in
    /// question it does not keep are taken back. A row it holds that a
    /// change not yet planned pointed at, before that change, stays too: the
    /// replica still holds that change's older version.
    ///
    /// A row that cannot go, and every row that points at it — held, or
    /// that the plan would send — is kept by no replica: it is taken back
    /// where held. When the plan would send a row that points at one, that
    /// row joins them and the plan runs again.
    pub(crate) async fn plan(&self, conn: &mut Conn, notes: &[Note]) -> Result<Delta, Error> {
        let delta = Delta {
            notes: notes.to_vec(),
            ..Delta::default()
        };
        self.plan_around(conn, delta).await
    }

    async fn plan_around(&self, conn: &mut Conn, mut delta: Delta) -> Result<Delta, Error> {
        let made: bool = sqlx::query_scalar("SELECT to_regclass('pg_temp.sync_holds') IS NOT NULL")
            .fetch_one(&mut *conn)
            .await?;
        if !made {
            sqlx::Executor::execute(&mut *conn, PLAN_TABLES).await?;
        }
        sqlx::Executor::execute(
            &mut *conn,
            "TRUNCATE sync_live, sync_fresh, sync_batch, sync_changed, sync_regrouped, \
             sync_reassigned, sync_released, sync_blocked, sync_wanted, sync_suspect, sync_kept",
        )
        .await?;
        delta.snapshot = sqlx::query_scalar("SELECT pg_current_snapshot()::text")
            .fetch_one(&mut *conn)
            .await?;
        sqlx::query(&format!(
            "INSERT INTO sync_live SELECT s.id FROM {}",
            self.replicas_sql()
        ))
        .execute(&mut *conn)
        .await?;
        let mut asked = self.note(conn, &delta.notes).await?;
        delta.snapshots = sqlx::query_scalar(
            "INSERT INTO sync_fresh SELECT DISTINCT r FROM unnest($1::uuid[]) AS r \
              WHERE r IN (SELECT replica_id FROM sync_live) RETURNING replica_id",
        )
        .bind(
            delta
                .notes
                .iter()
                .filter_map(|n| match n.change {
                    Change::Resync { replica } => Some(replica),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .collect();
        asked.fresh = !delta.snapshots.is_empty();
        let mut rows = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for ((replica, i, key), why) in &delta.blocked {
            rows.0.push(*replica);
            rows.1.push(i32::try_from(*i).unwrap_or(i32::MAX));
            rows.2.push(key.as_str());
            rows.3.push(why.as_str());
        }
        sqlx::query(
            "INSERT INTO sync_blocked (replica_id, rel, row_key, why, round) \
             SELECT *, 0 FROM unnest($1::uuid[], $2::int4[], $3::text[], $4::text[])",
        )
        .bind(&rows.0)
        .bind(&rows.1)
        .bind(&rows.2)
        .bind(&rows.3)
        .execute(&mut *conn)
        .await?;
        analyze(
            conn,
            "sync_live, sync_fresh, sync_batch, sync_changed, sync_regrouped, sync_reassigned, \
             sync_released, sync_blocked",
        )
        .await?;
        let mut blocked_from = 0;
        loop {
            blocked_from = self.spread_blocked(conn, blocked_from).await?;
            self.question(conn, &asked).await?;
            self.keep_all(conn, &asked).await?;
            if !self.conflicts(conn, blocked_from).await? {
                break;
            }
            sqlx::Executor::execute(&mut *conn, "TRUNCATE sync_wanted, sync_suspect, sync_kept")
                .await?;
        }
        let blocked: Vec<(Uuid, i32, String, String)> = sqlx::query_as(
            "SELECT b.replica_id, b.rel, b.row_key, b.why FROM sync_blocked b \
               JOIN sync_live USING (replica_id)",
        )
        .fetch_all(&mut *conn)
        .await?;
        delta.blocked = blocked
            .into_iter()
            .map(|(r, i, k, why)| ((r, usize::try_from(i).unwrap_or(usize::MAX), k), why))
            .collect();
        Ok(delta)
    }

    /// The notes of the plan, as what changed: the rows, by their key in the
    /// one form keys are compared in, with the latest transaction that
    /// changed each; the parents each pointed at before; the members that
    /// moved; the groups given or taken.
    async fn note(&self, conn: &mut Conn, notes: &[Note]) -> Result<Asked, Error> {
        let mut changed: BTreeMap<(usize, String), i64> = BTreeMap::new();
        let mut released: BTreeSet<(usize, String, usize, String)> = BTreeSet::new();
        let mut regrouped: BTreeSet<(usize, String)> = BTreeSet::new();
        let mut reassigned: BTreeSet<(usize, Uuid)> = BTreeSet::new();
        for note in notes {
            match &note.change {
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
                    // every time, rather than wherever a query casts it; one
                    // it can is read in the form every query writes it.
                    let of_table = |e| match e {
                        Error::Malformed { reason, .. } => Error::Malformed {
                            table: table.clone(),
                            reason,
                        },
                        other => other,
                    };
                    let key = key_text(&parse_key(&self.plans[i].key, key).map_err(of_table)?);
                    for reference in &self.plans[i].references {
                        let Some(parent) = previous
                            .as_ref()
                            .and_then(|p| key_of(p, &reference.columns))
                        else {
                            continue;
                        };
                        let parent = key_text(
                            &parse_key(&self.plans[reference.parent].key, &parent)
                                .map_err(of_table)?,
                        );
                        released.insert((i, key.clone(), reference.parent, parent));
                    }
                    let latest = changed.entry((i, key)).or_default();
                    *latest = (*latest).max(*xid);
                }
                Change::Members { table, member } => {
                    for (i, g) in self.groups_through(table, |g| &g.members) {
                        if let Some(key) = key_of(member, &g.member_row) {
                            let key = key_text(&parse_key(&self.plans[i].key, &key)?);
                            regrouped.insert((i, key));
                        }
                    }
                }
                Change::Assigned { table, replica } => {
                    for (i, _) in self.groups_through(table, |g| &g.assigned) {
                        reassigned.insert((i, *replica));
                    }
                }
                Change::Resync { .. } => {}
            }
        }
        let rel = |i: usize| i32::try_from(i).unwrap_or(i32::MAX);
        let name = |i: usize| self.plans[i].table;
        let ids: Vec<i64> = notes.iter().map(|n| n.id).collect();
        sqlx::query("INSERT INTO sync_batch SELECT DISTINCT unnest($1::int8[])")
            .bind(&ids)
            .execute(&mut *conn)
            .await?;
        let (keys, xids): (Vec<_>, Vec<i64>) = changed.into_iter().unzip();
        sqlx::query(
            "INSERT INTO sync_changed \
             SELECT r, t, k, x::text::xid8 FROM unnest($1::int4[], $2::text[], $3::text[], $4::int8[]) \
                 AS u(r, t, k, x)",
        )
        .bind(keys.iter().map(|(i, _)| rel(*i)).collect::<Vec<_>>())
        .bind(keys.iter().map(|(i, _)| name(*i)).collect::<Vec<_>>())
        .bind(keys.iter().map(|(_, k)| k.as_str()).collect::<Vec<_>>())
        .bind(&xids)
        .execute(&mut *conn)
        .await?;
        sqlx::query(
            "INSERT INTO sync_released \
             SELECT * FROM unnest($1::text[], $2::text[], $3::int4[], $4::text[], $5::text[])",
        )
        .bind(released.iter().map(|r| name(r.0)).collect::<Vec<_>>())
        .bind(released.iter().map(|r| r.1.as_str()).collect::<Vec<_>>())
        .bind(released.iter().map(|r| rel(r.2)).collect::<Vec<_>>())
        .bind(released.iter().map(|r| name(r.2)).collect::<Vec<_>>())
        .bind(released.iter().map(|r| r.3.as_str()).collect::<Vec<_>>())
        .execute(&mut *conn)
        .await?;
        sqlx::query(
            "INSERT INTO sync_regrouped SELECT * FROM unnest($1::int4[], $2::text[], $3::text[])",
        )
        .bind(regrouped.iter().map(|(i, _)| rel(*i)).collect::<Vec<_>>())
        .bind(regrouped.iter().map(|(i, _)| name(*i)).collect::<Vec<_>>())
        .bind(
            regrouped
                .iter()
                .map(|(_, k)| k.as_str())
                .collect::<Vec<_>>(),
        )
        .execute(&mut *conn)
        .await?;
        sqlx::query(
            "INSERT INTO sync_reassigned SELECT * FROM unnest($1::int4[], $2::text[], $3::uuid[])",
        )
        .bind(reassigned.iter().map(|(i, _)| rel(*i)).collect::<Vec<_>>())
        .bind(reassigned.iter().map(|(i, _)| name(*i)).collect::<Vec<_>>())
        .bind(reassigned.iter().map(|(_, r)| *r).collect::<Vec<_>>())
        .execute(&mut *conn)
        .await?;
        Ok(Asked {
            changed: keys.iter().map(|(i, _)| *i).collect(),
            regrouped: regrouped.iter().map(|(i, _)| *i).collect(),
            reassigned: reassigned.iter().map(|(i, _)| *i).collect(),
            released: !released.is_empty(),
            fresh: false,
        })
    }

    /// Closes the rows that cannot go over what points at them that their
    /// replica holds, from the round `from` on; answers the round after the
    /// last.
    async fn spread_blocked(&self, conn: &mut Conn, from: i32) -> Result<i32, Error> {
        let mut round = from;
        loop {
            let parents = self.frontier(conn, "sync_blocked", round).await?;
            if parents.is_empty() {
                return Ok(round);
            }
            for (c, reference) in self
                .references()
                .filter(|(_, r)| parents.contains(&r.parent))
            {
                let child = &self.plans[c];
                let select = format!(
                    "WITH f AS MATERIALIZED ( \
                         SELECT b.replica_id, {} AS row_key, \
                                format('it points at `%s` %s, which cannot go: %s', {}, b.row_key, b.why) AS why \
                           FROM sync_blocked b JOIN {} t ON {} \
                          WHERE b.rel = {} AND b.round = $1 - 1) \
                     SELECT f.replica_id, {c}, f.row_key, f.why FROM f {}",
                    self.key_of_row(c),
                    literal(self.plans[reference.parent].table),
                    child.relation.sql(),
                    self.points_at(reference, "b.row_key"),
                    reference.parent,
                    held_by("f", child.table),
                );
                self.add(conn, Set::Blocked, &select, round + 1).await?;
            }
            round += 1;
        }
    }

    /// The rows each replica should hold that the batch asks about —
    /// `sync_wanted` — and the rows it holds whose place is in question —
    /// `sync_suspect` — closed over what they point at and what follows
    /// them among the rows it holds; the rows in question its tables' rules
    /// send it are wanted too.
    async fn question(&self, conn: &mut Conn, asked: &Asked) -> Result<(), Error> {
        for i in 0..self.plans.len() {
            let mut filters = Vec::new();
            if asked.changed.contains(&i) {
                filters.push(self.key_among(
                    i,
                    &format!("SELECT row_key FROM sync_changed WHERE rel = {i}"),
                    false,
                ));
            }
            if asked.regrouped.contains(&i) {
                filters.push(self.key_among(
                    i,
                    &format!("SELECT row_key FROM sync_regrouped WHERE rel = {i}"),
                    false,
                ));
            }
            if asked.reassigned.contains(&i) {
                filters.push(format!(
                    "s.id IN (SELECT replica_id FROM sync_reassigned WHERE rel = {i})"
                ));
            }
            for filter in filters {
                self.want(conn, i, &filter).await?;
            }
        }
        analyze(conn, "sync_wanted").await?;
        let unwanted = "NOT EXISTS (SELECT 1 FROM sync_wanted w WHERE w.replica_id = h.replica_id \
                                    AND w.rel = x.rel AND w.row_key = h.row_key)";
        let seeds = [
            (
                !asked.changed.is_empty(),
                format!(
                    "SELECT h.replica_id, x.rel, h.row_key FROM sync_changed x \
                       JOIN sync_holds h ON h.relation = x.relation AND h.row_key = x.row_key \
                      WHERE {unwanted}"
                ),
            ),
            (
                !asked.regrouped.is_empty(),
                format!(
                    "SELECT h.replica_id, x.rel, h.row_key FROM sync_regrouped x \
                       JOIN sync_holds h ON h.relation = x.relation AND h.row_key = x.row_key \
                      WHERE {unwanted}"
                ),
            ),
            (
                !asked.reassigned.is_empty(),
                format!(
                    "SELECT h.replica_id, x.rel, h.row_key FROM sync_reassigned x \
                       JOIN sync_holds h ON h.replica_id = x.replica_id AND h.relation = x.relation \
                      WHERE {unwanted}"
                ),
            ),
            // The parents a changed row pointed at, for whoever holds it.
            (
                asked.released,
                "SELECT h.replica_id, x.parent, x.parent_key FROM sync_released x \
                   JOIN sync_holds c ON c.relation = x.relation AND c.row_key = x.row_key \
                   JOIN sync_holds h ON h.replica_id = c.replica_id AND h.relation = x.parent_relation \
                                    AND h.row_key = x.parent_key"
                    .to_owned(),
            ),
        ];
        for (_, sql) in seeds.into_iter().filter(|(asked, _)| *asked) {
            let select = format!("SELECT q.*, NULL FROM ({sql}) AS q");
            self.add(conn, Set::Suspect, &select, 0).await?;
        }
        for i in self.present(conn, "sync_blocked").await? {
            let select = format!(
                "SELECT b.replica_id, b.rel, b.row_key, NULL FROM sync_blocked b \
                   JOIN sync_holds h ON h.replica_id = b.replica_id AND h.relation = {} \
                                    AND h.row_key = b.row_key \
                  WHERE b.rel = {i}",
                literal(self.plans[i].table)
            );
            self.add(conn, Set::Suspect, &select, 0).await?;
        }
        let mut round = 0;
        loop {
            analyze(conn, "sync_suspect").await?;
            let from = self.frontier(conn, "sync_suspect", round).await?;
            if from.is_empty() {
                break;
            }
            for (c, reference) in self.references().filter(|(c, _)| from.contains(c)) {
                let select = format!(
                    "WITH f AS MATERIALIZED ( \
                         SELECT d.replica_id, {} AS row_key FROM sync_suspect d JOIN {} t ON {} \
                          WHERE d.rel = {c} AND d.round = $1 - 1 AND {}) \
                     SELECT f.replica_id, {}, f.row_key, NULL FROM f {}",
                    key_sql("t", &reference.columns),
                    self.plans[c].relation.sql(),
                    self.row_is(c, "d.row_key"),
                    not_null("t", &reference.columns),
                    reference.parent,
                    held_by("f", self.plans[reference.parent].table),
                );
                self.add(conn, Set::Suspect, &select, round + 1).await?;
            }
            for (f, column, p) in self.followers().filter(|(_, _, p)| from.contains(p)) {
                let select = format!(
                    "WITH f AS MATERIALIZED ( \
                         SELECT d.replica_id, {} AS row_key FROM sync_suspect d JOIN {} t ON {} \
                          WHERE d.rel = {p} AND d.round = $1 - 1) \
                     SELECT f.replica_id, {f}, f.row_key, NULL FROM f {}",
                    self.key_of_row(f),
                    self.plans[f].relation.sql(),
                    key_eq("t", &[column], &self.plans[p].key, "d.row_key"),
                    held_by("f", self.plans[f].table),
                );
                self.add(conn, Set::Suspect, &select, round + 1).await?;
            }
            round += 1;
        }
        for i in self.present(conn, "sync_suspect").await? {
            let filter = self.key_among(
                i,
                &format!("SELECT replica_id, row_key FROM sync_suspect WHERE rel = {i}"),
                true,
            );
            self.want(conn, i, &filter).await?;
        }
        analyze(conn, "sync_wanted").await
    }

    /// Adds to `sync_wanted` the rows of `i` its rule sends to a live
    /// replica, narrowed by `filter`.
    async fn want(&self, conn: &mut Conn, i: usize, filter: &str) -> Result<(), Error> {
        let Some(wanted) = self.wanted_sql(i, filter) else {
            return Ok(());
        };
        let select = format!("SELECT w.replica_id, {i}, w.key, NULL FROM ({wanted}) w");
        self.add(conn, Set::Wanted, &select, 0).await?;
        Ok(())
    }

    /// What every replica keeps, in `sync_kept`: its wanted rows, the rows
    /// in question something outside them points at, and the followers of
    /// a parent outside them that are in question or changed; then, a
    /// round at a time, what those need — until a round adds nothing and no
    /// change waiting to be planned keeps anything more.
    async fn keep_all(&self, conn: &mut Conn, asked: &Asked) -> Result<(), Error> {
        // What a replica provisioned holds, from scratch: it holds nothing
        // in question, so its rules' rows are kept with no question asked.
        for i in (0..self.plans.len()).filter(|_| asked.fresh) {
            let fresh = self.wanted_sql(i, "s.id IN (SELECT replica_id FROM sync_fresh)");
            if let Some(wanted) = fresh {
                self.keep(
                    conn,
                    i,
                    0,
                    &format!("SELECT w.replica_id, w.key FROM ({wanted}) w"),
                )
                .await?;
            }
        }
        for i in self.present(conn, "sync_wanted").await? {
            self.keep(
                conn,
                i,
                0,
                &format!("SELECT replica_id, row_key FROM sync_wanted WHERE rel = {i}"),
            )
            .await?;
        }
        let doubted = self.present(conn, "sync_suspect").await?;
        for (c, reference) in self
            .references()
            .filter(|(_, r)| doubted.contains(&r.parent))
        {
            let source = format!(
                "WITH f AS MATERIALIZED ( \
                     SELECT d.replica_id, {} AS row_key, d.row_key AS parent FROM sync_suspect d \
                       JOIN {} t ON {} WHERE d.rel = {}) \
                 SELECT f.replica_id, f.parent FROM f {} \
                  WHERE NOT EXISTS (SELECT 1 FROM sync_suspect x WHERE x.replica_id = f.replica_id \
                                       AND x.rel = {c} AND x.row_key = f.row_key)",
                self.key_of_row(c),
                self.plans[c].relation.sql(),
                self.points_at(reference, "d.row_key"),
                reference.parent,
                held_by("f", self.plans[c].table),
            );
            self.keep(conn, reference.parent, 0, &source).await?;
        }
        for (f, column, p) in self.followers() {
            let parent = literal(self.plans[p].table);
            let outside = format!(
                "NOT EXISTS (SELECT 1 FROM sync_suspect x WHERE x.replica_id = hp.replica_id \
                                AND x.rel = {p} AND x.row_key = hp.row_key)"
            );
            let mut sources = Vec::new();
            if doubted.contains(&f) {
                sources.push(format!(
                    "SELECT d.replica_id, d.row_key FROM sync_suspect d JOIN {} t ON {} \
                       JOIN sync_holds hp ON hp.replica_id = d.replica_id AND hp.relation = {parent} \
                                         AND hp.row_key = t.{}::text \
                      WHERE d.rel = {f} AND {outside}",
                    self.plans[f].active,
                    self.row_is(f, "d.row_key"),
                    ident(column),
                ));
            }
            if asked.changed.contains(&f) {
                sources.push(format!(
                    "SELECT hp.replica_id, {} FROM {} t \
                       JOIN sync_holds hp ON hp.relation = {parent} AND hp.row_key = t.{}::text \
                      WHERE {} AND {outside}",
                    self.key_of_row(f),
                    self.plans[f].active,
                    ident(column),
                    self.key_among(
                        f,
                        &format!("SELECT row_key FROM sync_changed WHERE rel = {f}"),
                        false,
                    ),
                ));
            }
            for source in sources {
                self.keep(conn, f, 0, &source).await?;
            }
        }
        let mut round = 0;
        loop {
            analyze(conn, "sync_kept").await?;
            let from = self.frontier(conn, "sync_kept", round).await?;
            if from.is_empty() {
                round += 1;
                if doubted.is_empty() || !self.keep_older(conn, round).await? {
                    return Ok(());
                }
                continue;
            }
            for (c, reference) in self.references().filter(|(c, _)| from.contains(c)) {
                let source = format!(
                    "SELECT k.replica_id, {} FROM sync_kept k JOIN {} t ON {} \
                      WHERE k.rel = {c} AND k.round = $1 - 1 AND k.up AND {}",
                    key_sql("t", &reference.columns),
                    self.plans[c].relation.sql(),
                    self.row_is(c, "k.row_key"),
                    not_null("t", &reference.columns),
                );
                self.keep(conn, reference.parent, round + 1, &source)
                    .await?;
            }
            for (f, column, p) in self.followers().filter(|(_, _, p)| from.contains(p)) {
                let source = format!(
                    "SELECT k.replica_id, {} FROM sync_kept k JOIN {} t ON {} \
                      WHERE k.rel = {p} AND k.round = $1 - 1 AND k.down",
                    self.key_of_row(f),
                    self.plans[f].active,
                    key_eq("t", &[column], &self.plans[p].key, "k.row_key"),
                );
                self.keep(conn, f, round + 1, &source).await?;
            }
            round += 1;
        }
    }

    /// Keeps, at `round`, the rows in question that a row its replica holds
    /// still points at in the version it holds: one a change not yet planned
    /// — beyond the batch, or parked — pointed at before. Answers whether it
    /// kept any.
    async fn keep_older(&self, conn: &mut Conn, round: i32) -> Result<bool, Error> {
        let waiting: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM sync.notes n WHERE n.done_at IS NULL AND n.kind = 'row' \
                              AND n.previous IS NOT NULL \
                              AND NOT EXISTS (SELECT 1 FROM sync_batch b WHERE b.id = n.id))",
        )
        .fetch_one(&mut *conn)
        .await?;
        if !waiting {
            return Ok(false);
        }
        let mut kept = 0;
        for (c, reference) in self.references() {
            let pointed: Vec<String> = reference
                .columns
                .iter()
                .map(|column| format!("n.previous ->> {}", literal(column)))
                .collect();
            let source = format!(
                "SELECT d.replica_id, d.row_key FROM sync_suspect d \
                   JOIN sync.notes n ON n.done_at IS NULL AND n.kind = 'row' AND n.relation = {child} \
                                    AND concat_ws('|', {}) = d.row_key \
                   JOIN sync_holds h ON h.replica_id = d.replica_id AND h.relation = {child} \
                                    AND h.row_key = n.row_key \
                  WHERE d.rel = {} AND {} \
                    AND NOT EXISTS (SELECT 1 FROM sync_batch b WHERE b.id = n.id) \
                    AND (NOT EXISTS (SELECT 1 FROM sync_suspect x WHERE x.replica_id = h.replica_id \
                                        AND x.rel = {c} AND x.row_key = h.row_key) \
                         OR EXISTS (SELECT 1 FROM sync_kept x WHERE x.replica_id = h.replica_id \
                                       AND x.rel = {c} AND x.row_key = h.row_key AND NOT x.sends))",
                pointed.join(", "),
                reference.parent,
                pointed
                    .iter()
                    .map(|p| format!("{p} IS NOT NULL"))
                    .collect::<Vec<_>>()
                    .join(" AND "),
                child = literal(self.plans[c].table),
            );
            kept += self.keep(conn, reference.parent, round, &source).await?;
        }
        Ok(kept > 0)
    }

    /// Keeps for `i`, at `round` — `$1` to `source` — the rows `source`
    /// names, `(replica_id, row_key)`, but those that cannot go and those
    /// kept already. Each is sent where its replica lacks it, refused it, or
    /// holds a version from before a change of the batch; it spreads to its
    /// parents when sent or in question, and to its followers when its
    /// replica lacks it or it is in question. Answers how many it kept.
    async fn keep(
        &self,
        conn: &mut Conn,
        i: usize,
        round: i32,
        source: &str,
    ) -> Result<u64, Error> {
        let select = format!(
            "SELECT x.replica_id, {i}, x.row_key, NULL FROM ({source}) AS x(replica_id, row_key)"
        );
        self.add(conn, Set::Kept(i), &select, round).await
    }

    /// Adds to the plan's `set` the rows `select` answers —
    /// `(replica_id, rel, row_key, why)` — at `round`, which `select` reads
    /// as `$1`: each once, but those the set has, or that cannot go, for the
    /// rows a replica keeps. They are staged and counted first, so what
    /// joins them to the plan's other sets is planned for how many they
    /// are, whatever the statistics of the tables they were read from say.
    async fn add(&self, conn: &mut Conn, set: Set, select: &str, round: i32) -> Result<u64, Error> {
        sqlx::Executor::execute(&mut *conn, "TRUNCATE sync_staged").await?;
        // Each row once; a row that cannot go, with one of its reasons.
        let once = match set {
            Set::Blocked => "DISTINCT ON (q.replica_id, q.rel, q.row_key)",
            _ => "DISTINCT",
        };
        let staged = sqlx::query(&format!(
            "INSERT INTO sync_staged \
             SELECT {once} q.replica_id, q.rel, q.row_key, q.why, $1::int4 \
               FROM ({select}) AS q(replica_id, rel, row_key, why)"
        ))
        .bind(round)
        .execute(&mut *conn)
        .await?
        .rows_affected();
        if staged == 0 {
            return Ok(0);
        }
        let table = set.table();
        // Never read while written: an insert that looked into the set it
        // fills would read every row it added so far, for every row.
        analyze(conn, &format!("sync_staged, {table}")).await?;
        sqlx::Executor::execute(
            &mut *conn,
            format!(
                "DELETE FROM sync_staged s USING {table} x \
                  WHERE x.replica_id = s.replica_id AND x.rel = s.rel AND x.row_key = s.row_key"
            )
            .as_str(),
        )
        .await?;
        let sql = match set {
            Set::Wanted => {
                "INSERT INTO sync_wanted SELECT s.replica_id, s.rel, s.row_key FROM sync_staged s"
                    .to_owned()
            }
            Set::Suspect => "INSERT INTO sync_suspect SELECT s.replica_id, s.rel, s.row_key, s.round \
                               FROM sync_staged s"
                .to_owned(),
            Set::Blocked => "INSERT INTO sync_blocked \
                             SELECT s.replica_id, s.rel, s.row_key, s.why, s.round FROM sync_staged s"
                .to_owned(),
            Set::Kept(i) => format!(
                "INSERT INTO sync_kept \
                 SELECT s.replica_id, s.rel, s.row_key, s.round, lacks OR stale, lacks OR stale OR doubt, \
                        lacks OR doubt \
                   FROM (SELECT s.*, h.replica_id IS NULL OR h.refused AS lacks, \
                                coalesce(NOT pg_visible_in_snapshot(c.xid, h.snapshot), false) AS stale, \
                                d.replica_id IS NOT NULL AS doubt \
                           FROM sync_staged s \
                           LEFT JOIN sync_holds h ON h.replica_id = s.replica_id AND h.relation = {} \
                                                 AND h.row_key = s.row_key \
                           LEFT JOIN sync_changed c ON c.rel = s.rel AND c.row_key = s.row_key \
                           LEFT JOIN sync_suspect d ON d.replica_id = s.replica_id AND d.rel = s.rel \
                                                   AND d.row_key = s.row_key \
                          WHERE NOT EXISTS (SELECT 1 FROM sync_blocked b WHERE b.replica_id = s.replica_id \
                                               AND b.rel = s.rel AND b.row_key = s.row_key)) AS s",
                literal(self.plans[i].table)
            ),
        };
        Ok(sqlx::query(&sql).execute(&mut *conn).await?.rows_affected())
    }

    /// Rows kept that point at a row that cannot go, there: each joins the
    /// rows that cannot go, at round `round`. Answers whether there was one.
    async fn conflicts(&self, conn: &mut Conn, round: i32) -> Result<bool, Error> {
        let blocked = self.present(conn, "sync_blocked").await?;
        let mut found = 0;
        for (c, reference) in self
            .references()
            .filter(|(_, r)| blocked.contains(&r.parent))
        {
            let select = format!(
                "SELECT k.replica_id, {c}, k.row_key, \
                        format('it points at `%s` %s, which cannot go: %s', {}, b.row_key, b.why) \
                   FROM sync_kept k JOIN {} t ON {} \
                   JOIN sync_blocked b ON b.replica_id = k.replica_id AND b.rel = {} \
                                      AND b.row_key = {} \
                  WHERE k.rel = {c}",
                literal(self.plans[reference.parent].table),
                self.plans[c].relation.sql(),
                self.row_is(c, "k.row_key"),
                reference.parent,
                key_sql("t", &reference.columns),
            );
            found += self.add(conn, Set::Blocked, &select, round).await?;
        }
        Ok(found > 0)
    }

    /// The tables `set` has rows of at `round`.
    async fn frontier(
        &self,
        conn: &mut Conn,
        set: &str,
        round: i32,
    ) -> Result<BTreeSet<usize>, Error> {
        let rels: Vec<i32> =
            sqlx::query_scalar(&format!("SELECT DISTINCT rel FROM {set} WHERE round = $1"))
                .bind(round)
                .fetch_all(&mut *conn)
                .await?;
        Ok(rels
            .into_iter()
            .filter_map(|r| usize::try_from(r).ok())
            .collect())
    }

    /// The tables `set` has rows of.
    async fn present(&self, conn: &mut Conn, set: &str) -> Result<BTreeSet<usize>, Error> {
        let rels: Vec<i32> = sqlx::query_scalar(&format!("SELECT DISTINCT rel FROM {set}"))
            .fetch_all(&mut *conn)
            .await?;
        Ok(rels
            .into_iter()
            .filter_map(|r| usize::try_from(r).ok())
            .collect())
    }

    /// Every reference between the mirrored tables, by the child.
    fn references(&self) -> impl Iterator<Item = (usize, &Reference)> {
        self.plans
            .iter()
            .enumerate()
            .flat_map(|(c, p)| p.references.iter().map(move |r| (c, r)))
    }

    /// Every table that follows another: itself, its column, its parent.
    fn followers(&self) -> impl Iterator<Item = (usize, &'static str, usize)> + '_ {
        self.plans.iter().enumerate().filter_map(|(f, plan)| {
            let Route::Follows(column) = plan.route else {
                return None;
            };
            let reference = plan.references.iter().find(|r| r.columns == [column])?;
            Some((f, column, reference.parent))
        })
    }

    /// The key text of the row of `i` aliased `t`.
    fn key_of_row(&self, i: usize) -> String {
        key_sql(
            "t",
            &self.plans[i].key.iter().map(|c| c.name).collect::<Vec<_>>(),
        )
    }

    /// The row of `i` aliased `t` is the one whose key is the text `text`.
    fn row_is(&self, i: usize, text: &str) -> String {
        let plan = &self.plans[i];
        key_eq(
            "t",
            &plan.key.iter().map(|c| c.name).collect::<Vec<_>>(),
            &plan.key,
            text,
        )
    }

    /// The row aliased `t` points through `reference` at the row whose key
    /// is the text `text`.
    fn points_at(&self, reference: &Reference, text: &str) -> String {
        key_eq(
            "t",
            &reference.columns,
            &self.plans[reference.parent].key,
            text,
        )
    }

    /// The row of `i` aliased `t` is one of the keys `keys` — a query of a
    /// text column, after a replica's uuid `by_replica`, which the replica
    /// aliased `s` is then — answers: what lets the table's key index serve.
    fn key_among(&self, i: usize, keys: &str, by_replica: bool) -> String {
        let plan = &self.plans[i];
        let mut row: Vec<String> = plan
            .key
            .iter()
            .map(|c| format!("t.{}", ident(c.name)))
            .collect();
        let mut typed: Vec<String> = plan
            .key
            .iter()
            .enumerate()
            .map(|(n, c)| {
                let part = if plan.key.len() == 1 {
                    "k.row_key".to_owned()
                } else {
                    format!("split_part(k.row_key, '|', {})", n + 1)
                };
                format!("({part})::{}", KeyPart::sql_type(c.key))
            })
            .collect();
        let columns = if by_replica {
            row.insert(0, "s.id".to_owned());
            typed.insert(0, "k.replica_id".to_owned());
            "replica_id, row_key"
        } else {
            "row_key"
        };
        format!(
            "({}) IN (SELECT {} FROM ({keys}) AS k({columns}))",
            row.join(", "),
            typed.join(", ")
        )
    }

    /// What the plan takes back: the rows in question each live replica
    /// does not keep.
    const DROPPED: &'static str = "SELECT d.replica_id, d.rel, d.row_key FROM sync_suspect d \
          JOIN sync_live USING (replica_id) \
         WHERE NOT EXISTS (SELECT 1 FROM sync_kept k WHERE k.replica_id = d.replica_id \
                              AND k.rel = d.rel AND k.row_key = d.row_key)";

    /// Every replica the plan sends to, takes from or provisions.
    pub(crate) async fn touched(&self, conn: &mut Conn) -> Result<BTreeSet<Uuid>, Error> {
        Ok(sqlx::query_scalar(&format!(
            "SELECT replica_id FROM sync_kept JOIN sync_live USING (replica_id) WHERE sends \
             UNION SELECT replica_id FROM ({}) AS d UNION SELECT replica_id FROM sync_fresh",
            Self::DROPPED
        ))
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .collect())
    }

    /// Opens, as the cursor `sync_sent`, every row the plan sends a live
    /// replica — `(table, key, replica)` — tables parents first, each by
    /// key: read once, a page at a time, by [`Engine::next`].
    pub(crate) async fn open_sent(&self, conn: &mut Conn) -> Result<(), Error> {
        sqlx::Executor::execute(
            &mut *conn,
            format!(
                "DECLARE sync_sent NO SCROLL CURSOR FOR \
                 SELECT k.rel, k.row_key, k.replica_id FROM sync_kept k JOIN sync_live USING (replica_id) \
                  WHERE k.sends ORDER BY array_position({}, k.rel), k.row_key, k.replica_id",
                self.order_array(false)
            )
            .as_str(),
        )
        .await?;
        Ok(())
    }

    /// Opens, as the cursor `sync_dropped`, every row the plan takes back
    /// from a live replica — `(table, key, replica)` — by replica, children
    /// first.
    pub(crate) async fn open_dropped(&self, conn: &mut Conn) -> Result<(), Error> {
        sqlx::Executor::execute(
            &mut *conn,
            format!(
                "DECLARE sync_dropped NO SCROLL CURSOR FOR \
                 SELECT d.rel, d.row_key, d.replica_id FROM ({}) AS d \
                  ORDER BY d.replica_id, array_position({}, d.rel), d.row_key",
                Self::DROPPED,
                self.order_array(true)
            )
            .as_str(),
        )
        .await?;
        Ok(())
    }

    /// The next `limit` rows of the cursor `cursor`, closed once there are
    /// none.
    pub(crate) async fn next(
        &self,
        conn: &mut Conn,
        cursor: &str,
        limit: usize,
    ) -> Result<Vec<(usize, String, Uuid)>, Error> {
        let rows: Vec<(i32, String, Uuid)> =
            sqlx::query_as(&format!("FETCH FORWARD {limit} FROM {cursor}"))
                .fetch_all(&mut *conn)
                .await?;
        if rows.is_empty() {
            sqlx::Executor::execute(&mut *conn, format!("CLOSE {cursor}").as_str()).await?;
        }
        Ok(rows
            .into_iter()
            .map(|(i, k, r)| (usize::try_from(i).unwrap_or(usize::MAX), k, r))
            .collect())
    }

    /// The tables, parents first — or children first — as an SQL array.
    fn order_array(&self, children_first: bool) -> String {
        let mut order: Vec<String> = self.order.iter().map(ToString::to_string).collect();
        if children_first {
            order.reverse();
        }
        format!("ARRAY[{}]::int4[]", order.join(", "))
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
                let declared = match p.route {
                    Route::Grouped { members, assigned } => [members, assigned],
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

    /// The rows of `keys` of table `i`, encoded in the plan's snapshot, by
    /// key text: the ones that cannot go — that do not encode, are larger
    /// than `largest_row` bytes, which no message could carry, or are not in
    /// the snapshot the plan read — answered apart, with why.
    pub(crate) async fn encode<P: std::marker::Sync + 'static>(
        &self,
        conn: &mut Conn,
        codecs: &[Codec<P>],
        i: usize,
        keys: &[String],
        largest_row: usize,
    ) -> Result<Encoded, Error> {
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
        let loaded = codec.load(conn, &parsed).await?;
        let mut out = Encoded::default();
        for (key, bytes) in loaded.rows {
            let key = key_text(&key);
            if bytes.len() > largest_row {
                let why = format!(
                    "a row of {} bytes, larger than any message may be ({largest_row})",
                    bytes.len()
                );
                out.failed.insert(key, why);
            } else {
                out.rows.insert(key, bytes);
            }
        }
        for (key, e) in loaded.failed {
            out.failed.insert(key_text(&key), e.to_string());
        }
        // The plan read it in this snapshot: a row not there is a bug, set
        // aside like one.
        for key in keys {
            if !out.rows.contains_key(key) {
                out.failed
                    .entry(key.clone())
                    .or_insert_with(|| "the plan's snapshot has no such row".to_owned());
            }
        }
        Ok(out)
    }

    /// `delta` planned again, in the same snapshot, around rows that cannot
    /// go to a replica as well as the ones it already went around: each is
    /// never sent there, a replica holding an older version of one has it
    /// taken back, and so does every row that points at one there — a
    /// replica never keeps content the engine no longer knows the contents
    /// of. Each comes back once a fix lets it through.
    pub(crate) async fn replan(
        &self,
        conn: &mut Conn,
        mut delta: Delta,
        blocked: BTreeMap<(Uuid, usize, String), String>,
    ) -> Result<Delta, Error> {
        for (row, why) in blocked {
            delta.blocked.entry(row).or_insert(why);
        }
        self.plan_around(conn, delta).await
    }

    /// `delta` planned again, in the same snapshot, with `replicas`
    /// provisioned from scratch besides.
    pub(crate) async fn provision(
        &self,
        conn: &mut Conn,
        mut delta: Delta,
        replicas: &[Uuid],
    ) -> Result<Delta, Error> {
        delta.notes.extend(replicas.iter().map(|r| Note {
            id: 0,
            change: Change::Resync { replica: *r },
        }));
        self.plan_around(conn, delta).await
    }

    /// The rows the delta could not send, each per replica it was to go to.
    pub(crate) fn unsent(&self, delta: &Delta) -> Vec<Unsent> {
        delta
            .blocked
            .iter()
            .map(|((replica, i, key), reason)| Unsent {
                replica: *replica,
                table: self.plans[*i].table,
                key: key.clone(),
                reason: reason.clone(),
            })
            .collect()
    }

    /// The tables every snapshot names, parents first.
    pub(crate) fn snapshot_tables(&self) -> Vec<&'static str> {
        self.order.iter().map(|&i| self.plans[i].table).collect()
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

/// The pending notes, oldest first: not parked, and of a kind this build
/// reads. `conn` is in the
/// `REPEATABLE READ` transaction the notes are planned and loaded in, so the
/// rows they are planned against do not move under the plan.
pub(crate) async fn pending(conn: &mut Conn, limit: i64) -> Result<Vec<Note>, Error> {
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
        "SELECT id, kind, relation, row_key, replica_id, previous, xid::text::bigint AS xid \
           FROM sync.notes WHERE done_at IS NULL AND parked_at IS NULL AND {READABLE} \
          ORDER BY id LIMIT $1"
    ))
    .bind(limit)
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(|noted| {
            let id = noted.id;
            noted.change().map(|change| Note { id, change })
        })
        .collect())
}

/// The statement that notes the provisioning from scratch of each replica
/// `source` names — `(id, partition)` — unless one is pending for it: a
/// replica is provisioned once, however many reasons ask for it, and its
/// snapshot, planned with the changes of its pass, carries them rather than
/// a second message. Every provisioning is noted through it.
pub(crate) fn resync_sql(source: &str) -> String {
    format!(
        "INSERT INTO sync.notes (kind, replica_id, partition_id) \
         SELECT DISTINCT ON (r.id) 'resync', r.id, r.partition FROM ({source}) AS r(id, partition) \
          WHERE NOT EXISTS (SELECT 1 FROM sync.notes p WHERE p.kind = 'resync' \
                               AND p.replica_id = r.id AND p.done_at IS NULL)"
    )
}

/// Queues `replica`'s provisioning from scratch, in the caller's transaction,
/// unless one is pending: it is sent everything it should hold and sweeps the
/// rest.
///
/// # Errors
///
/// [`Error::Database`] when the database fails.
pub async fn note_resync(
    conn: &mut Conn,
    partition: Option<Uuid>,
    replica: Uuid,
) -> Result<(), Error> {
    sqlx::query(&resync_sql("SELECT $1::uuid, $2::uuid"))
        .bind(replica)
        .bind(partition)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Marks notes the engine has sent and recorded.
pub(crate) async fn mark_done(conn: &mut Conn, ids: &[i64]) -> Result<(), Error> {
    sqlx::query("UPDATE sync.notes SET done_at = now() WHERE id = ANY($1)")
        .bind(ids)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Deletes the notes done longer ago than `age`; answers how many.
pub(crate) async fn prune(conn: &mut Conn, age: std::time::Duration) -> Result<u64, Error> {
    let done =
        sqlx::query("DELETE FROM sync.notes WHERE done_at < now() - make_interval(secs => $1)")
            .bind(age.as_secs_f64())
            .execute(&mut *conn)
            .await?;
    Ok(done.rows_affected())
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

/// The columns of `relation` — a table or a view, as SQL names it — with
/// their types, as the catalog has them whatever the role may read of it;
/// none when there is no such relation.
async fn typed_columns(conn: &mut Conn, relation: &str) -> Result<Vec<(String, String)>, Error> {
    Ok(sqlx::query_as(
        "SELECT a.attname::text, format_type(a.atttypid, NULL) FROM pg_attribute a \
          WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped",
    )
    .bind(relation)
    .fetch_all(&mut *conn)
    .await?)
}

/// Refuses a declaration whose SQL — `what`, read as `sql` — the database
/// does not run.
async fn readable(conn: &mut Conn, sql: &str, what: &str) -> Result<(), Error> {
    let mut tx = sqlx::Connection::begin(&mut *conn).await?;
    let read = sqlx::query(sql).execute(&mut *tx).await;
    tx.rollback().await?;
    match read {
        Err(sqlx::Error::Database(e)) => Err(Error::Misuse(format!(
            "{what} does not read: {}",
            e.message()
        ))),
        other => other.map(|_| ()).map_err(Error::from),
    }
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
}
