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
use crate::row::{key_text, Column, Key, KeyPart, Route, Table};
use crate::wire::Codec;
use crate::Error;
use routing::Routing;
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
/// pub const REGISTERS: Replicas = Replicas::new("shop.registers", "registers").live("active");
/// pub const DEVICES: Replicas = Replicas::new("fleet.devices", "devices")
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
    /// until said otherwise. `table` is the table behind the relation, as a
    /// foreign key names it.
    #[must_use]
    pub const fn new(relation: &'static str, table: &'static str) -> Self {
        Replicas {
            relation,
            table,
            live: "true",
            partition: None,
        }
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
    ///
    /// In a foreign key, the columns that are `column` on both sides say the
    /// two rows share a partition, not which parent it is: `(company_id,
    /// region_id)` into `regions (company_id, id)` points at a region by
    /// `region_id`, and a key of `company_id` alone points at no parent —
    /// unless the parent's primary key holds `column`, when the key points
    /// at it by every column, as at any other.
    #[must_use]
    pub const fn partitioned_by(self, column: &'static str) -> Self {
        Replicas {
            partition: Some(column),
            ..self
        }
    }
}

/// One kind of replica: where the server finds them ([`Replicas`]) and the
/// tables they hold, each with its codec ([`Codec`]), parents first. A server
/// serves one audience or several, from one note stream: a table is planned
/// for every audience that declares it, against that audience's replicas,
/// and travels as that audience's codec writes it — two audiences may hold
/// the same table in other shapes, or one a part of what the other holds.
///
/// ```
/// use magic_sync::plan::{Audience, Replicas};
/// # fn audiences(registers: Vec<magic_sync::wire::Codec<()>>, ledgers: Vec<magic_sync::wire::Codec<()>>) {
/// const REGISTERS: Replicas = Replicas::new("shop.registers", "registers").live("active");
/// const LEDGERS: Replicas = Replicas::new("accounting.ledgers", "ledgers");
///
/// let audiences = vec![
///     Audience::new("registers", REGISTERS, registers),
///     Audience::new("ledgers", LEDGERS, ledgers),
/// ];
/// # }
/// ```
pub struct Audience<P> {
    pub(crate) name: &'static str,
    pub(crate) replicas: Replicas,
    pub(crate) codecs: Vec<Codec<P>>,
}

impl<P> std::fmt::Debug for Audience<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Audience")
            .field("name", &self.name)
            .field("replicas", &self.replicas)
            .field("codecs", &self.codecs)
            .finish()
    }
}

impl<P> Audience<P> {
    /// The audience `name` — what the transport and the status call it, and
    /// what the engine records each of its replicas under — of the replicas
    /// `replicas` finds, holding the tables of `codecs`, parents first. A
    /// replica two audiences count live is the first's, unless it is live in
    /// the other already, which keeps it until it lets it go.
    #[must_use]
    pub fn new(name: &'static str, replicas: Replicas, codecs: Vec<Codec<P>>) -> Self {
        Audience {
            name,
            replicas,
            codecs,
        }
    }

    /// Its name.
    #[must_use]
    pub fn name(&self) -> &'static str {
        self.name
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

/// A parent whose columns a table's rows copy: a change of it is a change
/// of every row pointing at it.
#[derive(Clone, Debug)]
struct Dependency {
    parent: Relation,
    /// The child's columns that hold the parent's key, in its order.
    columns: Vec<String>,
    /// The parent's key, in key order: what its notes name it by.
    key: Vec<String>,
    /// The parent has the replicas' partition column, which its notes say.
    partitioned: bool,
    /// The SQL types of the child's columns: each part of a parent's key,
    /// as text, is cast to its column's, so the child's rows are found by
    /// its index.
    casts: Vec<String>,
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
    /// What a row's columns are read from: the table, or the relation the
    /// row is read `from`.
    read: String,
    pub(crate) key: Vec<Column>,
    route: Route,
    references: Vec<Reference>,
    /// The parents whose columns its rows copy.
    depends: Vec<Dependency>,
    /// The table has the replicas' partition column, which its notes say.
    has_partition: bool,
    group: Option<Group>,
    /// How a routed table reaches replicas.
    routing: Option<Routing>,
    /// The columns the replicas own: a write of only these is no change here.
    owned: Vec<&'static str>,
    /// Every column a row of it carries.
    columns: Vec<&'static str>,
    /// How large a row aliased `t` is, as SQL: every column it carries, as
    /// long as it is rather than as the database stores it.
    size: String,
    /// How its codec writes a row.
    encoding: &'static str,
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
    /// What was planned, for [`Engine::replan`] to plan it again around a
    /// row that cannot go.
    pub(crate) notes: Vec<Note>,
    /// Rows that cannot go to a replica, and why: never sent it, and taken
    /// back where it holds them, with whatever points at them.
    pub(crate) blocked: BTreeMap<(Uuid, usize, String), String>,
}

/// What a batch asks about: the tables with rows changed, members moved and
/// groups given or taken — or routed again, every replica of a partition
/// asked about as one given a group; whether a changed row released a
/// parent, and whether a replica is provisioned.
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
    /// A row of a mirrored table, by its key; what it pointed at before
    /// an update or a delete is in `sync.pointed`, by the note.
    Row {
        table: String,
        key: String,
        /// A row set aside, tried again where it was set aside: no change
        /// of it, so a replica holding a version of it holds this one.
        retry: bool,
    },
    /// A member joined or left a group of a grouped table's members table: the
    /// member row, as it was or is.
    Members {
        table: String,
        member: serde_json::Value,
    },
    /// A group was given to or taken from a replica.
    Assigned { table: String, replica: Uuid },
    /// What a routed table's view reads of `table` — as SQL names it —
    /// changed in `partition`, or in every partition (`None`): the view is
    /// asked again for every replica of it.
    Rerouted {
        table: String,
        partition: Option<Uuid>,
    },
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
/// schema and name, then the columns of the parent's primary key.
type CatalogForeignKey = (
    String,
    String,
    String,
    String,
    Vec<String>,
    Vec<String>,
    Vec<String>,
);

/// Rows a pass loads, encodes and records at a time: what it holds in
/// memory however much it sends.
pub(crate) const RECORD_CHUNK: usize = 2_000;

/// The transaction-scoped advisory lock installs take, so two processes
/// installing at once run one after the other ("SYNCINST").
const INSTALL_LOCK: i64 = 0x5359_4e43_494e_5354;

/// The engine's steps, in order, each as released and never edited: the
/// `n`th (from 1) brings the engine's tables from version `n - 1` to `n`.
const MIGRATIONS: [&str; crate::SERVER_SCHEMA_VERSION as usize] = [
    include_str!("../sql/postgres/1.sql"),
    include_str!("../sql/postgres/2.sql"),
    include_str!("../sql/postgres/3.sql"),
    include_str!("../sql/postgres/4.sql"),
];

/// The engine's step to `version` of its schema on the server, as one SQL
/// script that also records the version it brings it to — `None` for a
/// version no build of the engine has had yet. [`Server::install`] runs every
/// step a database lacks, in order; a host with migrations runs each, in one
/// of its own: every step up to [`SERVER_SCHEMA_VERSION`], in order, before
/// the migration that writes [`Engine::triggers`]' script, and each later one
/// when a release brings it (its changelog says). A step is never edited once
/// released.
///
/// [`Server::install`]: crate::run::server::Server::install
/// [`SERVER_SCHEMA_VERSION`]: crate::SERVER_SCHEMA_VERSION
#[must_use]
pub fn migration(version: i32) -> Option<String> {
    let step = MIGRATIONS.get(usize::try_from(version.checked_sub(1)?).ok()?)?;
    Some(format!(
        "{step}\nINSERT INTO sync.version (id, version) VALUES (1, {version}) \
         ON CONFLICT (id) DO UPDATE SET version = excluded.version;\n"
    ))
}

/// The server's engine, built once from the audiences and the schema by the
/// [`Server`](crate::run::server::Server), which hands it out for what a host asks of it:
/// the triggers and indexes it needs ([`Engine::triggers`],
/// [`Engine::indexes`]), and how far along every
/// replica is ([`status`](crate::status)).
pub struct Engine {
    /// One planner per audience, in the order the host declared them.
    pub(crate) audiences: Vec<Planner>,
    /// Every trigger the audiences need, one per table.
    triggers: Vec<Trigger>,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("audiences", &self.audiences)
            .finish_non_exhaustive()
    }
}

/// What one audience's replicas hold: its tables planned against its
/// replicas.
pub(crate) struct Planner {
    /// The audience's name.
    pub(crate) name: &'static str,
    pub(crate) plans: Vec<Plan>,
    by_name: HashMap<&'static str, usize>,
    /// Parents before children.
    pub(crate) order: Vec<usize>,
    pub(crate) replicas: Replicas,
}

impl std::fmt::Debug for Planner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tables: Vec<_> = self.order.iter().map(|&i| self.plans[i].table).collect();
        f.debug_struct("Audience")
            .field("name", &self.name)
            .field("tables", &tables)
            .field("replicas", &self.replicas)
            .finish_non_exhaustive()
    }
}

/// An audience as the engine is built from it: its name, its replicas and
/// its tables.
#[derive(Clone)]
pub(crate) struct Declared<'a> {
    pub(crate) name: &'static str,
    pub(crate) replicas: Replicas,
    pub(crate) tables: Vec<&'a Table>,
}

impl<'a> Declared<'a> {
    /// What `audiences` declare.
    pub(crate) fn of<P>(audiences: &'a [Audience<P>]) -> Vec<Self> {
        audiences
            .iter()
            .map(|a| Declared {
                name: a.name,
                replicas: a.replicas,
                tables: a.codecs.iter().map(|c| &c.table).collect(),
            })
            .collect()
    }
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
    Suspect,
    Blocked,
    /// What replicas keep of a table beyond what its rules send them; for
    /// a version of a row they hold that points there when `older`, which
    /// holds up that row and keeps of its followers only those they hold:
    /// what that version needs is the row, not what follows it since.
    Kept(usize, bool),
}

/// Whether the replica a row of `sync.held` as `h` is of refused the row: the
/// last version sent it is the one it refused. One sent again since is on its
/// way. What plans, what the status shows and what is compared ask this one
/// question.
pub(crate) const REFUSED: &str = "coalesce(h.refused >= h.sequence, false)";

/// What the replica `replica` holds of the row of the relation `relation`
/// (SQL text both) whose key is the text `key`, as `alias(replica_id,
/// row_key, noted, refused)` — nothing, for one provisioned; refused while
/// the last version sent it is the one it refused — `join`ed (`JOIN`, `LEFT
/// JOIN`) to the rows a plan asks about: looked up by the row, never a join
/// over all of `sync.held`, whatever its statistics say.
fn holds(join: &str, alias: &str, replica: &str, relation: &str, key: &str) -> String {
    format!(
        "{join} LATERAL (SELECT h.replica_id, h.row_key, h.noted, \
                                {REFUSED} AS refused \
                           FROM sync.held h \
                          WHERE h.replica_id = {replica} AND h.relation = {relation} AND h.row_key = {key} \
                            AND NOT EXISTS (SELECT 1 FROM sync_live provisioned \
                                             WHERE provisioned.replica_id = {replica} AND provisioned.fresh) \
                         OFFSET 0) {alias} ON true"
    )
}

/// Joins `c`, every live replica that holds the row of the relation
/// `relation` whose key is `key` (SQL text both) — but those provisioned —
/// looked up by the row.
fn holders(relation: &str, key: &str) -> String {
    format!(
        "CROSS JOIN LATERAL (SELECT h.replica_id FROM sync.held h \
                              WHERE h.relation = {relation} AND h.row_key = {key} \
                                AND h.replica_id IN (SELECT replica_id FROM sync_live WHERE NOT fresh) \
                             OFFSET 0) c"
    )
}

/// What replicas hold of the rows of the relation `relation` (SQL text)
/// whose keys `keys` answers, as `SELECT replica_id, row_key, noted,
/// refused` (see [`holds`]), looked up key by key.
fn held_by_key(relation: &str, keys: &str) -> String {
    format!(
        "SELECT h.replica_id, h.row_key, h.noted, {REFUSED} AS refused \
           FROM ({keys}) AS k(row_key) \
          CROSS JOIN LATERAL (SELECT * FROM sync.held h \
                               WHERE h.relation = {relation} AND h.row_key = k.row_key OFFSET 0) h"
    )
}

/// What the replicas `replicas` answers hold of the relation `relation`,
/// as [`held_by_key`] does, looked up replica by replica.
fn held_by_replica(relation: &str, replicas: &str) -> String {
    format!(
        "SELECT h.replica_id, h.row_key, h.noted, {REFUSED} AS refused \
           FROM ({replicas}) AS r(replica_id) \
          CROSS JOIN LATERAL (SELECT * FROM sync.held h \
                               WHERE h.replica_id = r.replica_id AND h.relation = {relation} OFFSET 0) h"
    )
}

/// The rows of `f` — `replica_id` and `row_key`, a row of `table`, worked
/// out apart — that their replica holds, as a join.
fn held_by(f: &str, table: &str) -> String {
    holds(
        "JOIN",
        "h",
        &format!("{f}.replica_id"),
        &literal(table),
        &format!("{f}.row_key"),
    )
}

/// Whether the version of a row that the held row aliased `h` was sent at
/// is older than the row: `newest`, the newest note of a change of it among
/// those in question, is past the newest that version saw. The planner asks
/// it of the notes it plans, the status of every pending one: one predicate,
/// so the two agree. A note's number, unlike its transaction's, rises in
/// the order a row's changes commit, and survives a dump and a restore.
pub(crate) fn stale(newest: &str) -> String {
    format!("coalesce({newest} > h.noted, false)")
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
/// plans, emptied at every commit: the live replicas, those provisioned
/// marked; the batch's notes and what they changed; the rows that cannot
/// go, those whose place is in question, and those a replica keeps beyond
/// what its tables' rules send it — the parents and followers those need,
/// the rows in question it keeps; what the rules send of the rows the batch
/// asks about; the replicas the plan touches, and the rows set aside it
/// owes no replica any more. The sets a round adds to are keyed, so a row
/// found again is found once.
///
/// What the rules hold a replica to is never kept whole — every row of a
/// table changed for every replica of a partition: of the rows the batch
/// asks about only what goes is kept, no more than what records it; and a
/// replica provisioned is sent every row of its rules as they are read.
const PLAN_TABLES: &str = "
CREATE TEMP TABLE IF NOT EXISTS sync_live (replica_id UUID PRIMARY KEY, fresh BOOLEAN NOT NULL)
    ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS sync_batch (id BIGINT PRIMARY KEY) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS sync_changed (
    rel INTEGER, relation TEXT NOT NULL, row_key TEXT, noted BIGINT NOT NULL,
    PRIMARY KEY (rel, row_key)) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS sync_regrouped (
    rel INTEGER, relation TEXT NOT NULL, row_key TEXT, PRIMARY KEY (rel, row_key))
    ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS sync_reassigned (
    rel INTEGER, relation TEXT NOT NULL, replica_id UUID, PRIMARY KEY (rel, replica_id))
    ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS sync_released (
    relation TEXT, row_key TEXT, parent INTEGER, parent_relation TEXT, parent_key TEXT)
    ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS sync_blocked (
    replica_id UUID, rel INTEGER, row_key TEXT, why TEXT NOT NULL, round INTEGER NOT NULL,
    PRIMARY KEY (replica_id, rel, row_key)) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS sync_suspect (
    replica_id UUID, rel INTEGER, row_key TEXT, round INTEGER NOT NULL,
    PRIMARY KEY (replica_id, rel, row_key)) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS sync_kept (
    replica_id UUID, rel INTEGER, row_key TEXT, round INTEGER NOT NULL,
    sends BOOLEAN NOT NULL, up BOOLEAN NOT NULL, down BOOLEAN NOT NULL, older BOOLEAN NOT NULL,
    PRIMARY KEY (replica_id, rel, row_key)) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS sync_sends (
    rel INTEGER, row_key TEXT, replica_id UUID, lacks BOOLEAN) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS sync_touched (replica_id UUID PRIMARY KEY) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS sync_settled (replica_id UUID, relation TEXT, row_key TEXT)
    ON COMMIT DELETE ROWS;
";

/// The sets of a plan, emptied for the next: a plan run again in the same
/// transaction, around rows that cannot go, starts from nothing. Its hashes
/// and sorts are the size of what it sends, which the default `work_mem`
/// spills to disk.
const PLAN_RESET: &str = "
SET LOCAL work_mem = '64MB';
DELETE FROM sync_live; DELETE FROM sync_batch; DELETE FROM sync_changed;
DELETE FROM sync_regrouped; DELETE FROM sync_reassigned; DELETE FROM sync_released;
DELETE FROM sync_blocked; DELETE FROM sync_suspect; DELETE FROM sync_kept;
DELETE FROM sync_sends; DELETE FROM sync_touched; DELETE FROM sync_settled;
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
#[derive(Clone, Debug, PartialEq, Eq)]
struct Trigger {
    on: Relation,
    name: &'static str,
    /// `pg_trigger.tgtype`: row or statement, before or after, the events.
    kind: i16,
    function: &'static str,
    args: Vec<String>,
    /// The statement's rows before and after it, as the transition tables
    /// `sync_old` and `sync_new`, for a trigger of each statement.
    old_table: bool,
    new_table: bool,
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
        let tables: Vec<&str> = [
            (self.old_table, " OLD TABLE AS sync_old"),
            (self.new_table, " NEW TABLE AS sync_new"),
        ]
        .into_iter()
        .filter(|(named, _)| *named)
        .map(|(_, table)| table)
        .collect();
        let referencing = if tables.is_empty() {
            String::new()
        } else {
            format!(" REFERENCING{}", tables.concat())
        };
        format!(
            "CREATE TRIGGER {} {timing} {} ON {}{referencing} FOR EACH {each} EXECUTE FUNCTION sync.{}({});",
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
    /// The names of its transition tables, if any.
    old_table: Option<String>,
    new_table: Option<String>,
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
        let named = |name: &str, expected: bool| expected.then(|| name.to_owned());
        self.plain
            && self.kind == expected.kind
            && self.function == expected.function
            && args == expected.args
            && self.old_table == named("sync_old", expected.old_table)
            && self.new_table == named("sync_new", expected.new_table)
    }
}

/// Every trigger whose function is in the engine's schema, but those a
/// partition inherits from its partitioned table.
async fn live_triggers(conn: &mut Conn) -> Result<Vec<LiveTrigger>, Error> {
    Ok(sqlx::query_as(
        "SELECT n.nspname::text AS schema, c.relname::text AS table, t.tgname::text AS name, \
                t.tgtype AS kind, p.proname::text AS function, t.tgnargs AS nargs, t.tgargs AS args, \
                t.tgenabled IN ('O', 'A') AND t.tgqual IS NULL AND t.tgattr::text = '' AS plain, \
                t.tgoldtable::text AS old_table, t.tgnewtable::text AS new_table \
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

/// An index the planner's lookups need: on `on`, led by `columns`, in
/// any order.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Index {
    on: Relation,
    columns: Vec<String>,
}

impl Index {
    fn ddl(&self) -> String {
        let columns: Vec<String> = self.columns.iter().map(|c| ident(c)).collect();
        format!(
            "CREATE INDEX {} ON {} ({});",
            ident(&self.name()),
            self.on.sql(),
            columns.join(", ")
        )
    }

    /// Its name: its table's and its columns', as far as a name holds them,
    /// and a digest of them apart — the same names joined otherwise, a
    /// table `a_b` on `c` and a table `a` on `b, c`, are other indexes.
    fn name(&self) -> String {
        let name = format!("{}_{}", self.on.name, self.columns.join("_"));
        let apart = format!("{}\0{}", self.on.name, self.columns.join("\0"));
        let digest = apart.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
        });
        let mut cut = name.len().min(41);
        while !name.is_char_boundary(cut) {
            cut -= 1;
        }
        format!("{}_sync_{digest:016x}", &name[..cut])
    }

    /// Whether one of `live` serves it: a valid b-tree index, of every row,
    /// whose first key columns are its columns.
    fn served(&self, live: &[LiveIndex]) -> bool {
        let mut needed: Vec<&str> = self.columns.iter().map(String::as_str).collect();
        needed.sort_unstable();
        live.iter().any(|index| {
            let mut leading: Vec<&str> = index
                .columns
                .iter()
                .take(needed.len())
                .map(|c| c.as_deref().unwrap_or_default())
                .collect();
            leading.sort_unstable();
            index.on() == self.on && leading == needed
        })
    }
}

/// An index of the database: its table, and its key columns in order, an
/// expression as `None`.
#[derive(sqlx::FromRow)]
struct LiveIndex {
    schema: String,
    table: String,
    columns: Vec<Option<String>>,
}

impl LiveIndex {
    fn on(&self) -> Relation {
        Relation {
            schema: self.schema.clone(),
            name: self.table.clone(),
        }
    }
}

/// Every valid b-tree index of every row of a table of `relations`.
async fn live_indexes(
    conn: &mut Conn,
    relations: &BTreeSet<&Relation>,
) -> Result<Vec<LiveIndex>, Error> {
    let schemas: Vec<&str> = relations.iter().map(|r| r.schema.as_str()).collect();
    let names: Vec<&str> = relations.iter().map(|r| r.name.as_str()).collect();
    Ok(sqlx::query_as(
        "SELECT n.nspname::text AS schema, c.relname::text AS table, \
                array_agg(a.attname::text ORDER BY k.n) AS columns \
           FROM unnest($1::text[], $2::text[]) AS r(schema, name) \
           JOIN pg_namespace n ON n.nspname = r.schema \
           JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = r.name \
           JOIN pg_index i ON i.indrelid = c.oid \
           JOIN pg_class ic ON ic.oid = i.indexrelid \
           JOIN pg_am am ON am.oid = ic.relam \
           CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, n) \
           LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum \
          WHERE i.indisvalid AND i.indpred IS NULL AND am.amname = 'btree' \
            AND k.n <= i.indnkeyatts \
          GROUP BY i.indexrelid, n.nspname, c.relname",
    )
    .bind(&schemas)
    .bind(&names)
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
            crate::SERVER_SCHEMA_VERSION
        )));
    }
    for step in (installed + 1)..=crate::SERVER_SCHEMA_VERSION {
        let script = migration(step).expect("a step of this build");
        // Not `raw_sql`: its future is not `Send` for every lifetime a
        // host's spawn asks of it.
        sqlx::Executor::execute(&mut *conn, script.as_str()).await?;
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
    /// its row column for column, that every trigger the engine needs is
    /// there as [`Engine::triggers`] writes it, and that an index serves each
    /// of the planner's lookups ([`Engine::indexes`]). Fails on a declaration
    /// the schema cannot honour — a table that differs from its row, a
    /// `follows` column that is not a key, a group table that does not have
    /// the columns a group needs, a cycle — on a trigger missing or
    /// different, and on an index missing.
    pub(crate) async fn new(conn: &mut Conn, audiences: &[Declared<'_>]) -> Result<Self, Error> {
        crate::row::parity(conn, &tables_of(audiences)).await?;
        let engine = Self::read(conn, audiences).await?;
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
        let missing = engine.indexes_missing(conn).await?;
        if !missing.is_empty() {
            let ddl: Vec<String> = missing.iter().map(Index::ddl).collect();
            return Err(Error::Misuse(format!(
                "the planner looks rows up by columns no index leads with — create the indexes \
                 Engine::indexes prints, in a migration:\n{}",
                ddl.join("\n")
            )));
        }
        Ok(engine)
    }

    /// [`Engine::new`], having first brought the database to what it needs,
    /// in the caller's transaction and one process at a time: the engine's
    /// schema installed or upgraded, every trigger missing or different
    /// written as [`Engine::triggers`] does (and any of the engine's on a
    /// table no longer declared dropped), and every index missing created as
    /// [`Engine::indexes`] prints it. A table mirrored for the first time
    /// that already has rows is sent whole: every live replica is provisioned
    /// again. Idempotent.
    ///
    /// For a host that has no migrations of its own. Run it before anything
    /// writes a mirrored table — a write before its trigger is never noted —
    /// on a connection that may create triggers. A host with migrations runs
    /// every [`migration`] step in one and writes each table's triggers in
    /// the migration that creates it, as it does its indexes; [`Engine::new`] refuses to start while one is missing.
    pub(crate) async fn install(
        conn: &mut Conn,
        audiences: &[Declared<'_>],
    ) -> Result<Self, Error> {
        migrate(conn).await?;
        crate::row::parity(conn, &tables_of(audiences)).await?;
        let engine = Self::read(conn, audiences).await?;
        let live = live_triggers(conn).await?;
        for stray in engine.strays(&live) {
            let drop = format!(
                "DROP TRIGGER {} ON {}",
                ident(&stray.name),
                stray.on().sql()
            );
            sqlx::Executor::execute(&mut *conn, drop.as_str()).await?;
        }
        let mut unnoted = false;
        for expected in engine.triggers_needed() {
            let existing = live
                .iter()
                .find(|t| t.on() == expected.on && t.name == expected.name);
            if existing.is_some_and(|t| t.matches(&expected)) {
                continue;
            }
            if let Some(existing) = existing {
                let drop = format!("DROP TRIGGER {} ON {}", expected.name, expected.on.sql());
                sqlx::Executor::execute(&mut *conn, drop.as_str()).await?;
                // Disabled, or firing for some writes only: what it let by
                // was never noted.
                unnoted |= !existing.plain;
            } else if expected.function == "note_row" {
                let has_rows: bool = sqlx::query_scalar(&format!(
                    "SELECT EXISTS (SELECT 1 FROM {})",
                    expected.on.sql()
                ))
                .fetch_one(&mut *conn)
                .await?;
                unnoted |= has_rows;
            }
            sqlx::Executor::execute(&mut *conn, expected.ddl().as_str()).await?;
        }
        for index in engine.indexes_missing(conn).await? {
            sqlx::Executor::execute(&mut *conn, index.ddl().as_str()).await?;
        }
        if unnoted {
            // Rows written while their table was not noted reached no
            // replica: every replica is provisioned again, and gets them.
            for planner in &engine.audiences {
                sqlx::query(&resync_sql(&format!(
                    "SELECT s.id, s.partition FROM {}",
                    Planner::replicas_of(planner.replicas)
                )))
                .execute(&mut *conn)
                .await?;
            }
        }
        Ok(engine)
    }

    /// The statements that create every trigger the server needs for
    /// `audiences`, one per line, read off the database `conn` is on: what a
    /// host with migrations writes in the migration that creates the tables —
    /// after every step of the engine's schema ([`migration`]) — and what
    /// [`Server::install`](crate::run::server::Server::install) runs. A mirrored table
    /// notes every write that changes a column it does not leave to the
    /// replicas, and refuses a truncate; a group table notes every write.
    /// [`Server::new`](crate::run::server::Server::new) refuses to start while one is
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
    /// use magic_sync::plan::{Audience, Engine, Replicas};
    ///
    /// const REGISTERS: Replicas = Replicas::new("shop.registers", "registers").live("active");
    /// let audiences = [Audience::new("registers", REGISTERS, codecs)];
    /// let mut conn = pool.acquire().await?;
    /// println!("{}", Engine::triggers(&mut conn, &audiences).await?);
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
        audiences: &[Audience<P>],
    ) -> Result<String, Error> {
        let wishes = audiences
            .iter()
            .map(|a| crate::wish::engines(&a.codecs))
            .collect::<Result<Vec<_>, _>>()?;
        let mut declared = Declared::of(audiences);
        for (audience, wishes) in declared.iter_mut().zip(&wishes) {
            audience.tables.extend(wishes.iter().map(|c| &c.table));
        }
        let engine = Self::read(conn, &declared).await?;
        Ok(engine
            .triggers_needed()
            .iter()
            .map(Trigger::ddl)
            .collect::<Vec<_>>()
            .join("\n"))
    }

    /// The statements that create every index the planner's lookups need
    /// for `audiences` and the database `conn` is on does not have, one per
    /// line: what a host with migrations writes beside the triggers
    /// ([`Engine::triggers`]), and what
    /// [`Server::install`](crate::run::server::Server::install) runs. The
    /// planner finds rows by the columns that point at a mirrored table's
    /// rows, a row's replica or partition, and a group's members and
    /// assignments — on a large table, a lookup no index serves reads the
    /// whole table on every pass. An index that leads with those columns,
    /// in any order, serves; [`Server::new`](crate::run::server::Server::new)
    /// refuses to start while one is missing.
    ///
    /// # Errors
    ///
    /// As [`Engine::triggers`].
    pub async fn indexes<P: std::marker::Sync + 'static>(
        conn: &mut Conn,
        audiences: &[Audience<P>],
    ) -> Result<String, Error> {
        let wishes = audiences
            .iter()
            .map(|a| crate::wish::engines(&a.codecs))
            .collect::<Result<Vec<_>, _>>()?;
        let mut declared = Declared::of(audiences);
        for (audience, wishes) in declared.iter_mut().zip(&wishes) {
            audience.tables.extend(wishes.iter().map(|c| &c.table));
        }
        let engine = Self::read(conn, &declared).await?;
        Ok(engine
            .indexes_missing(conn)
            .await?
            .iter()
            .map(Index::ddl)
            .collect::<Vec<_>>()
            .join("\n"))
    }

    /// The indexes the planner's lookups need: the columns of a mirrored
    /// table that point at another, its route's column — its replica, the
    /// row it follows, its partition, the one it is sent by — and each
    /// group's members by their row and by their group, and its
    /// assignments by their group and by their replica.
    fn indexes_needed(&self) -> BTreeSet<Index> {
        let mut out = BTreeSet::new();
        for planner in &self.audiences {
            planner.indexes_needed(&mut out);
        }
        out
    }

    /// The indexes the planner needs that no index of the database serves.
    async fn indexes_missing(&self, conn: &mut Conn) -> Result<Vec<Index>, Error> {
        let needed = self.indexes_needed();
        let relations: BTreeSet<&Relation> = needed.iter().map(|i| &i.on).collect();
        let live = live_indexes(conn, &relations).await?;
        Ok(needed.into_iter().filter(|i| !i.served(&live)).collect())
    }

    /// The engine of `audiences`, read off the schema: the engine's tables at
    /// this build's version, each audience planned, and what they declare
    /// of one table together the one trigger can note.
    async fn read(conn: &mut Conn, audiences: &[Declared<'_>]) -> Result<Self, Error> {
        let needed = crate::SERVER_SCHEMA_VERSION;
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
        if audiences.is_empty() {
            return Err(Error::Misuse(
                "no audience: a server serves one kind of replica at least".to_owned(),
            ));
        }
        let mut names = BTreeSet::new();
        let mut planners = Vec::with_capacity(audiences.len());
        for audience in audiences {
            if audience.name.is_empty() || !names.insert(audience.name) {
                return Err(Error::Misuse(format!(
                    "two audiences are named `{}`, or one is named nothing: the engine records \
                     each replica under its audience's name",
                    audience.name
                )));
            }
            planners.push(
                Planner::read(conn, audience.name, &audience.tables, audience.replicas).await?,
            );
        }
        let mut engine = Engine {
            audiences: planners,
            triggers: Vec::new(),
        };
        engine.triggers = engine.noted_rows()?;
        Ok(engine)
    }

    /// The triggers every table the audiences note needs, one per table
    /// however many audiences declare it: what it points at is every
    /// mirrored parent of any of them, and an update of the columns every
    /// one of them leaves to its replicas is no change. Fails when two
    /// audiences note one table otherwise than one trigger can: by another
    /// partition column, or a group table in another role.
    fn noted_rows(&self) -> Result<Vec<Trigger>, Error> {
        struct Noted {
            table: String,
            /// The partition column its trigger is given, and the one it
            /// notes: none, when the table has no such column.
            partition: &'static str,
            effective: &'static str,
            key: Vec<String>,
            references: Vec<(&'static str, Vec<String>)>,
            owned: Vec<&'static str>,
        }
        let mut rows: Vec<(Relation, Noted)> = Vec::new();
        let mut groups: BTreeMap<Relation, (String, String, &str, &str)> = BTreeMap::new();
        for planner in &self.audiences {
            let partition = planner.replicas.partition.unwrap_or_default();
            for plan in &planner.plans {
                let references: Vec<(&'static str, Vec<String>)> = plan
                    .references
                    .iter()
                    .map(|r| (planner.plans[r.parent].table, r.columns.clone()))
                    .collect();
                let effective = if plan.has_partition { partition } else { "" };
                let key: Vec<String> = plan.key.iter().map(|c| c.name.to_owned()).collect();
                let Some((_, noted)) = rows.iter_mut().find(|(on, _)| *on == plan.relation) else {
                    rows.push((
                        plan.relation.clone(),
                        Noted {
                            table: plan.table.to_owned(),
                            partition,
                            effective,
                            key,
                            references,
                            owned: plan.owned.clone(),
                        },
                    ));
                    continue;
                };
                if noted.effective != effective {
                    return Err(Error::Misuse(format!(
                        "`{}` is held by audiences partitioned by {:?} and by {:?}: one trigger \
                         notes a row's partition by one column",
                        plan.table, noted.effective, effective
                    )));
                }
                // Partitioned otherwise, the audiences agree the table has no
                // partition column: the trigger says none, whatever their order.
                if noted.partition != partition {
                    noted.partition = "";
                }
                // The trigger writes the key in one order, which every
                // audience reads it in.
                if noted.key != key {
                    return Err(Error::Misuse(format!(
                        "`{}` is keyed by {:?} in one audience and by {key:?} in another: \
                         declare its key in one order",
                        plan.table, noted.key
                    )));
                }
                for reference in references {
                    if !noted.references.contains(&reference) {
                        noted.references.push(reference);
                    }
                }
                noted.owned.retain(|c| plan.owned.contains(c));
            }
            // A parent only copied from is noted all the same, by its key.
            for dependency in planner.plans.iter().flat_map(|p| &p.depends) {
                let partition = if dependency.partitioned {
                    partition
                } else {
                    ""
                };
                match rows.iter_mut().find(|(on, _)| *on == dependency.parent) {
                    Some((_, noted)) if noted.effective != partition => {
                        return Err(Error::Misuse(format!(
                            "{} is noted by audiences partitioned by {:?} and by {:?}: one \
                             trigger notes a row's partition by one column",
                            dependency.parent.sql(),
                            noted.effective,
                            partition
                        )));
                    }
                    // Whatever columns of it a row copies, a change of any
                    // is a change: none is left to the replicas.
                    Some((_, noted)) => {
                        if noted.partition != partition {
                            noted.partition = "";
                        }
                        noted.owned.clear();
                    }
                    None => rows.push((
                        dependency.parent.clone(),
                        Noted {
                            table: dependency.parent.name.clone(),
                            partition,
                            effective: partition,
                            key: dependency.key.clone(),
                            references: Vec::new(),
                            owned: Vec::new(),
                        },
                    )),
                }
            }
            for plan in &planner.plans {
                let (Some(g), Route::Grouped { members, assigned }) = (&plan.group, plan.route)
                else {
                    continue;
                };
                for (on, args) in [
                    (
                        &g.members,
                        (
                            members.to_owned(),
                            g.group[0].0.clone(),
                            "members",
                            partition,
                        ),
                    ),
                    (
                        &g.assigned,
                        (
                            assigned.to_owned(),
                            g.assigned_replica.clone(),
                            "assigned",
                            partition,
                        ),
                    ),
                ] {
                    match groups.get(on) {
                        Some(known) if *known != args => {
                            return Err(Error::Misuse(format!(
                                "{} is a group table of two audiences, noted otherwise by each: \
                                 one trigger notes it",
                                on.sql()
                            )));
                        }
                        Some(_) => {}
                        None => {
                            groups.insert(on.clone(), args);
                        }
                    }
                }
            }
        }
        // A replica, and the notes, know a table by its name alone.
        for (i, (on, noted)) in rows.iter().enumerate() {
            if let Some((other, _)) = rows[..i].iter().find(|(_, n)| n.table == noted.table) {
                return Err(Error::Misuse(format!(
                    "{} and {} are both noted as `{}`: the engine tells tables apart by name",
                    other.sql(),
                    on.sql(),
                    noted.table
                )));
            }
        }
        let mut out = Vec::new();
        for (on, noted) in rows {
            let mut args = vec![
                noted.table.clone(),
                noted.partition.to_owned(),
                noted.key.len().to_string(),
            ];
            args.extend(noted.key.iter().cloned());
            args.push(noted.references.len().to_string());
            for (parent, columns) in &noted.references {
                args.push((*parent).to_owned());
                args.push(columns.len().to_string());
                args.extend(columns.iter().cloned());
            }
            args.extend(noted.owned.iter().map(|c| (*c).to_owned()));
            out.push(Trigger {
                on: on.clone(),
                name: "sync_note_row",
                kind: ROW | INSERT | UPDATE | DELETE,
                function: "note_row",
                args,
                old_table: false,
                new_table: false,
            });
            out.push(Trigger {
                on,
                name: "sync_refuse_truncate",
                kind: BEFORE | TRUNCATE,
                function: "refuse_truncate",
                args: Vec::new(),
                old_table: false,
                new_table: false,
            });
        }
        for (on, (name, column, role, partition)) in groups {
            out.push(Trigger {
                on,
                name: "sync_note_group",
                kind: ROW | INSERT | UPDATE | DELETE,
                function: "note_group",
                args: vec![name, column, role.to_owned(), partition.to_owned()],
                old_table: false,
                new_table: false,
            });
        }
        out.extend(self.reroutes());
        Ok(out)
    }

    /// The triggers of every table a routed table's view reads, by the
    /// table: one per statement that can change what a view reads of it,
    /// each noting the partitions the statement touched — once each,
    /// however many rows. PostgreSQL gives a trigger its statement's rows
    /// only for one event, and with no column list: an update is told
    /// apart by the columns the views read of the table, which its trigger
    /// compares. A truncate takes every row, of every partition. A trigger
    /// of statements fires for the table a statement names, and is not
    /// cloned to its partitions or its children: a view that reads a table
    /// of a partitioned or an inheritance tree is refused when it is read
    /// (`routing`). Tables in one order, columns in one order: printed
    /// twice, they are the same text.
    fn reroutes(&self) -> Vec<Trigger> {
        let mut reads: BTreeMap<&Relation, (&str, BTreeSet<&str>)> = BTreeMap::new();
        for planner in &self.audiences {
            let partition = planner.replicas.partition.unwrap_or_default();
            for routing in planner.plans.iter().filter_map(|p| p.routing.as_ref()) {
                for (on, columns) in &routing.reads {
                    let partition = if routing.partitioned.contains(on) {
                        partition
                    } else {
                        ""
                    };
                    let (noted, read) = reads.entry(on).or_insert((partition, BTreeSet::new()));
                    // Partitioned otherwise by two audiences, a write notes
                    // every partition.
                    if *noted != partition {
                        *noted = "";
                    }
                    read.extend(columns.iter().map(String::as_str));
                }
            }
        }
        let mut out = Vec::new();
        for (on, (partition, read)) in reads {
            let args = vec![on.sql(), partition.to_owned()];
            let mut update = args.clone();
            update.extend(read.iter().map(|c| (*c).to_owned()));
            for (name, kind, args, old_table, new_table) in [
                ("sync_reroute_insert", INSERT, args.clone(), false, true),
                ("sync_reroute_update", UPDATE, update, true, true),
                ("sync_reroute_delete", DELETE, args.clone(), true, false),
                (
                    "sync_reroute_truncate",
                    TRUNCATE,
                    args.clone(),
                    false,
                    false,
                ),
            ] {
                out.push(Trigger {
                    on: on.clone(),
                    name,
                    kind,
                    function: "note_reroute",
                    args,
                    old_table,
                    new_table,
                });
            }
        }
        out
    }

    fn triggers_needed(&self) -> Vec<Trigger> {
        self.triggers.clone()
    }

    /// The engine's triggers on tables it does not need them on.
    fn strays<'a>(&self, live: &'a [LiveTrigger]) -> Vec<&'a LiveTrigger> {
        let needed = self.triggers_needed();
        live.iter()
            .filter(|t| !needed.iter().any(|n| n.on == t.on() && n.name == t.name))
            .collect()
    }

    /// The audience `replica` is of, by its place among the audiences: the
    /// one the engine records it live in, which plans what it holds; for one
    /// it has not seen live, the first whose replicas count it live now, then
    /// the first whose relation names it at all. `None` for a replica none
    /// names.
    pub(crate) async fn audience_of(
        &self,
        conn: &mut Conn,
        replica: Uuid,
    ) -> Result<Option<usize>, Error> {
        let recorded: Option<String> =
            sqlx::query_scalar("SELECT audience FROM sync.replicas WHERE replica_id = $1 AND live")
                .bind(replica)
                .fetch_optional(&mut *conn)
                .await?
                .flatten();
        if let Some(a) = recorded.as_deref().and_then(|name| self.audience(name)) {
            return Ok(Some(a));
        }
        for (i, planner) in self.audiences.iter().enumerate() {
            if planner.is_live(conn, replica).await? {
                return Ok(Some(i));
            }
        }
        for (i, planner) in self.audiences.iter().enumerate() {
            if planner.is_replica(conn, replica).await? {
                return Ok(Some(i));
            }
        }
        Ok(None)
    }

    /// Looks at every audience's replicas, as [`Planner::observe`] does;
    /// the pending changes of a table no audience notes any more are done.
    pub(crate) async fn observe(&self, conn: &mut Conn) -> Result<(), Error> {
        let names: Vec<&str> = self.audiences.iter().map(|a| a.name).collect();
        for planner in &self.audiences {
            planner.observe(conn, &names).await?;
        }
        // A replica no audience took — of an audience no longer declared,
        // or seen before there were audiences and live in none now — is
        // forgotten, as one no longer live is; and so is what the engine
        // kept of an audience no longer declared.
        sqlx::query(
            "WITH gone AS (SELECT replica_id FROM sync.replicas \
                            WHERE live AND (audience IS NULL OR audience <> ALL($1))), \
                  forgotten AS (DELETE FROM sync.held h USING gone g WHERE h.replica_id = g.replica_id), \
                  untaken AS (DELETE FROM sync.gone t USING gone g WHERE t.replica_id = g.replica_id), \
                  dropped AS (DELETE FROM sync.outbox o USING gone g WHERE o.replica_id = g.replica_id), \
                  unchained AS (DELETE FROM sync.chains c USING gone g WHERE c.replica_id = g.replica_id), \
                  undeclared AS (DELETE FROM sync.audiences WHERE name <> ALL($1)), \
                  legacy AS (UPDATE sync.version SET declared = NULL WHERE declared IS NOT NULL) \
             UPDATE sync.replicas r SET live = false, partition_id = NULL \
               FROM gone g WHERE r.replica_id = g.replica_id",
        )
        .bind(&names)
        .execute(&mut *conn)
        .await?;
        // A change of a table no audience notes any more — no longer
        // declared, or of an audience gone — is done: nothing plans it.
        sqlx::query(
            "UPDATE sync.notes SET done_at = now() \
              WHERE done_at IS NULL AND kind IN ('row', 'retry') AND relation <> ALL($1)",
        )
        .bind(self.noted_tables())
        .execute(&mut *conn)
        .await?;
        // And so is a change of a table no view routing a table reads any
        // more.
        sqlx::query(
            "UPDATE sync.notes SET done_at = now() \
              WHERE done_at IS NULL AND kind = 'rerouted' AND relation <> ALL($1)",
        )
        .bind(self.routed_tables())
        .execute(&mut *conn)
        .await?;
        Ok(())
    }

    /// Whether `planner`'s audience plans `change`: a change to one of its
    /// tables, a group of one of them, what a view routing one of them
    /// reads, a replica to provision — and a change no audience knows,
    /// which fails where it is planned.
    pub(crate) fn concerns(&self, planner: &Planner, change: &Change) -> bool {
        let knows = |p: &Planner| match change {
            Change::Row { table, .. } => p.index(table).is_some() || p.depends_on(table),
            Change::Members { table, .. } => !p.groups_through(table, |g| &g.members).is_empty(),
            Change::Assigned { table, .. } => !p.groups_through(table, |g| &g.assigned).is_empty(),
            Change::Rerouted { table, .. } => p.routes_through(table),
            Change::Resync { .. } => true,
        };
        knows(planner) || !self.audiences.iter().any(knows)
    }

    /// Every table a view routing a table of some audience reads, as SQL
    /// names it.
    fn routed_tables(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .audiences
            .iter()
            .flat_map(|a| &a.plans)
            .filter_map(|p| p.routing.as_ref())
            .flat_map(|r| r.reads.keys().map(Relation::sql))
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }

    /// The name of every table some audience notes.
    pub(crate) fn noted_tables(&self) -> Vec<&str> {
        let mut out: Vec<&str> = self
            .audiences
            .iter()
            .flat_map(|a| a.plans.iter().map(|p| p.table))
            .chain(
                self.audiences
                    .iter()
                    .flat_map(|a| &a.plans)
                    .flat_map(|p| &p.depends)
                    .map(|d| d.parent.name.as_str()),
            )
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }

    /// The live replicas of every audience as the engine last saw them, as
    /// a subquery `s(id, partition)`.
    pub(crate) fn replicas_sql(&self) -> String {
        "(SELECT replica_id AS id, partition_id AS partition FROM sync.replicas WHERE live) AS s"
            .to_owned()
    }

    /// The audience `name`, by its place.
    pub(crate) fn audience(&self, name: &str) -> Option<usize> {
        self.audiences.iter().position(|a| a.name == name)
    }
}

/// Every table `audiences` declare.
fn tables_of<'a>(audiences: &[Declared<'a>]) -> Vec<&'a Table> {
    audiences
        .iter()
        .flat_map(|a| a.tables.iter().copied())
        .collect()
}

impl Planner {
    /// The indexes this audience's lookups need, added to `out`.
    fn indexes_needed(&self, out: &mut BTreeSet<Index>) {
        let mut need = |on: &Relation, columns: Vec<String>| {
            out.insert(Index {
                on: on.clone(),
                columns,
            });
        };
        for plan in &self.plans {
            for reference in &plan.references {
                need(&plan.relation, reference.columns.clone());
            }
            for dependency in &plan.depends {
                need(&plan.relation, dependency.columns.clone());
            }
            match plan.route {
                Route::Broadcast => {
                    if let Some(partition) = self.replicas.partition {
                        need(&plan.relation, vec![partition.to_owned()]);
                    }
                }
                Route::Assigned(column) | Route::Follows(column) => {
                    need(&plan.relation, vec![column.to_owned()]);
                }
                // A view has no index of its own: what it reads is the
                // host's to index for its own questions.
                Route::Referenced | Route::Grouped { .. } | Route::Routed { .. } => {}
            }
            if let Some(g) = &plan.group {
                need(&g.members, g.member_row.clone());
                need(&g.members, g.group.iter().map(|(m, _)| m.clone()).collect());
                need(
                    &g.assigned,
                    g.group.iter().map(|(_, a)| a.clone()).collect(),
                );
                need(&g.assigned, vec![g.assigned_replica.clone()]);
            }
        }
    }

    /// The audience `name`'s plans: the foreign keys between its tables and
    /// each grouped table's group tables read from the catalog, every
    /// declaration checked against the schema.
    async fn read(
        conn: &mut Conn,
        name: &'static str,
        tables: &[&Table],
        replicas: Replicas,
    ) -> Result<Self, Error> {
        let tables: Vec<&Table> = tables
            .iter()
            .copied()
            .filter(|t| t.mirror.sent_down())
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
        let foreign_keys: Vec<CatalogForeignKey> =
            sqlx::query_as(
                "SELECT child_ns.nspname::text, child.relname::text,
                        parent_ns.nspname::text, parent.relname::text,
                        ARRAY(SELECT a.attname::text FROM unnest(c.conkey) WITH ORDINALITY AS k(n, o)
                              JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.n
                              ORDER BY k.o),
                        ARRAY(SELECT a.attname::text FROM unnest(c.confkey) WITH ORDINALITY AS k(n, o)
                              JOIN pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = k.n
                              ORDER BY k.o),
                        ARRAY(SELECT a.attname::text FROM pg_index i
                              JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
                              WHERE i.indrelid = c.confrelid AND i.indisprimary
                              ORDER BY a.attname)
                   FROM pg_constraint c
                   JOIN pg_class child ON child.oid = c.conrelid
                   JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
                   JOIN pg_class parent ON parent.oid = c.confrelid
                   JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
                  WHERE c.contype = 'f' AND c.conparentid = 0
                  ORDER BY 1, 2, 3, 4, 5, 6",
            )
            .fetch_all(&mut *conn)
            .await?;
        let foreign_keys: Vec<(ForeignKey, Vec<String>)> = foreign_keys
            .into_iter()
            .map(|(cs, c, ps, p, cc, pc, parent_key)| {
                (
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
                    ),
                    parent_key,
                )
            })
            .collect();
        let foreign_keys = within_partition(foreign_keys, replicas.partition);
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
                active: t
                    .active
                    .or(t.from)
                    .map_or_else(|| relation.sql(), str::to_owned),
                read: t.from.map_or_else(|| relation.sql(), str::to_owned),
                relation,
                key,
                route: t.route,
                references: Vec::new(),
                depends: Vec::new(),
                has_partition: false,
                group: None,
                routing: None,
                owned: t
                    .columns
                    .iter()
                    .filter(|c| c.replica_owned)
                    .map(|c| c.name)
                    .collect(),
                columns: t.columns.iter().map(|c| c.name).collect(),
                size: String::new(),
                encoding: t.encoding,
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
        for (plan, t) in plans.iter_mut().zip(&tables) {
            for &column in t.depends {
                // The foreign key of the column — beside the partition
                // column, when the parent's key holds that too.
                let Some((_, parent, child_columns, parent_columns)) =
                    foreign_keys.iter().find(|(child, _, c, _)| {
                        *child == plan.relation
                            && c.iter().any(|c| c == column)
                            && c.iter().all(|c| c == column || c == partition)
                    })
                else {
                    return Err(Error::Misuse(format!(
                        "`{}` depends on `{column}`, which is no foreign key of its table — \
                         alone, or beside the partition column",
                        plan.table
                    )));
                };
                let key = crate::dialect::primary_key(conn, &parent.schema, &parent.name).await?;
                let in_key_order: Option<Vec<String>> = key
                    .iter()
                    .map(|k| {
                        parent_columns
                            .iter()
                            .position(|p| p == k)
                            .map(|i| child_columns[i].clone())
                    })
                    .collect();
                let Some(columns) = in_key_order.filter(|_| parent_columns.len() == key.len())
                else {
                    return Err(Error::Misuse(format!(
                        "`{}` depends on `{column}`, which points at {} by {parent_columns:?}, \
                         not its key",
                        plan.table,
                        parent.sql()
                    )));
                };
                let parent_typed = typed_columns(conn, &parent.sql()).await?;
                let partitioned = match type_of(&parent_typed, partition) {
                    None => false,
                    Some(t) if t == "uuid" => true,
                    Some(_) => {
                        return Err(Error::Misuse(format!(
                            "{} has a column `{partition}` that is not a uuid: the replicas \
                             are partitioned by the uuid of that name",
                            parent.sql()
                        )))
                    }
                };
                let typed = typed_columns(conn, &plan.relation.sql()).await?;
                let casts = columns
                    .iter()
                    .map(|c| type_of(&typed, c).expect("a column of a foreign key is a column"))
                    .collect();
                plan.depends.push(Dependency {
                    parent: parent.clone(),
                    columns,
                    key,
                    partitioned,
                    casts,
                });
            }
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
                Route::Routed { view, row, replica } => {
                    let typed = typed_columns(conn, &plan.relation.sql()).await?;
                    let key_types: Vec<(&str, String)> = plan
                        .key
                        .iter()
                        .map(|k| (k.name, type_of(&typed, k.name).unwrap_or_default()))
                        .collect();
                    let key: Vec<(&str, &str)> =
                        key_types.iter().map(|(k, t)| (*k, t.as_str())).collect();
                    plan.routing = Some(
                        Routing::read(
                            conn,
                            &plan.relation,
                            &key,
                            &plan.owned,
                            (view, row, replica),
                            replicas.partition,
                        )
                        .await?,
                    );
                }
                _ => {}
            }
        }

        let mut noted: Vec<Relation> = plans.iter().map(|p| p.relation.clone()).collect();
        noted.extend(
            plans
                .iter()
                .filter_map(|p| p.group.as_ref())
                .flat_map(|g| [g.members.clone(), g.assigned.clone()]),
        );
        let mut partitioned = BTreeSet::new();
        for relation in &noted {
            let columns = typed_columns(conn, &relation.sql()).await?;
            if type_of(&columns, partition).is_some() {
                partitioned.insert(relation.clone());
            }
            // The triggers note the partition of every row that has one.
            if type_of(&columns, partition).is_some_and(|t| t != "uuid") {
                return Err(Error::Misuse(format!(
                    "{} has a column `{partition}` that is not a uuid: the replicas are \
                     partitioned by the uuid of that name",
                    relation.sql()
                )));
            }
        }
        for plan in &mut plans {
            plan.has_partition = partitioned.contains(&plan.relation);
        }
        for plan in &mut plans {
            readable(
                conn,
                &format!("SELECT 1 FROM {} LIMIT 0", plan.active),
                &format!("the active rows of `{}`, `{}`", plan.table, plan.active),
            )
            .await?;
            let columns = typed_columns(conn, &plan.read).await?;
            // The length of text and bytes is read off the stored value
            // without decompressing it; anything else, as its text.
            plan.size = plan
                .columns
                .iter()
                .map(|c| match type_of(&columns, c).as_deref() {
                    Some("text" | "bytea" | "character varying" | "character") => {
                        format!("coalesce(octet_length(t.{}), 0)", ident(c))
                    }
                    _ => format!("coalesce(octet_length(t.{}::text), 0)", ident(c)),
                })
                .chain(["0".to_owned()])
                .collect::<Vec<_>>()
                .join(" + ");
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
            name,
            plans,
            by_name,
            order,
            replicas,
        })
    }

    /// What decides which rows each replica holds and what a row carries,
    /// as one text: the replicas, and per table, parents first, its key,
    /// the rule that routes it, what it points at, its groups, its columns
    /// and how its codec writes a row. Two builds that write the same text place every row alike.
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
            // Read from the table, it says nothing: what an engine of an
            // earlier build declared is declared alike.
            let mut read = if p.read == p.relation.sql() {
                String::new()
            } else {
                format!(" from {}", p.read)
            };
            for d in &p.depends {
                let _ = write!(read, " depends {} {:?}", d.parent.sql(), d.columns);
            }
            // What the view reads and how it reads it: a view written
            // otherwise routes rows otherwise.
            if let Some(routing) = &p.routing {
                read.push_str(&routing.declared());
            }
            let _ = writeln!(
                out,
                "{} {} active {} key {key:?} {:?} references {references:?} group {:?} columns {:?} \
                 as {}{read}",
                p.table,
                p.relation.sql(),
                p.active,
                p.route,
                p.group,
                p.columns,
                p.encoding,
            );
        }
        out
    }

    /// Whether a row of the audience copies columns of `table`'s rows.
    pub(crate) fn depends_on(&self, table: &str) -> bool {
        self.plans
            .iter()
            .flat_map(|p| &p.depends)
            .any(|d| d.parent.name == table)
    }

    /// The tables of the audience routed by a view that reads `table`, as
    /// SQL names it.
    fn routed_through(&self, table: &str) -> Vec<usize> {
        self.plans
            .iter()
            .enumerate()
            .filter(|(_, p)| {
                p.routing
                    .as_ref()
                    .is_some_and(|r| r.reads.keys().any(|on| on.sql() == table))
            })
            .map(|(i, _)| i)
            .collect()
    }

    /// Whether a view routing a table of the audience reads `table`, as SQL
    /// names it.
    pub(crate) fn routes_through(&self, table: &str) -> bool {
        !self.routed_through(table).is_empty()
    }

    /// Whether a change of `table` is the audience's to plan: one of its
    /// tables, or a group table of one.
    pub(crate) fn knows(&self, table: &str) -> bool {
        self.index(table).is_some()
            || !self.groups_through(table, |g| &g.members).is_empty()
            || !self.groups_through(table, |g| &g.assigned).is_empty()
    }

    pub(crate) fn index(&self, table: &str) -> Option<usize> {
        self.by_name.get(table).copied()
    }

    pub(crate) fn table(&self, index: usize) -> &'static str {
        self.plans[index].table
    }

    /// The audience's live replicas as the engine last saw them
    /// ([`Planner::observe`]), as a subquery `s(id, partition)`: what a pass
    /// sends to and what the status counts. A replica the host stops
    /// counting as live after the relay looked is still sent what that look
    /// owed it, and is let go of by the next.
    pub(crate) fn replicas_sql(&self) -> String {
        format!(
            "(SELECT replica_id AS id, partition_id AS partition FROM sync.replicas \
               WHERE live AND audience = {}) AS s",
            literal(self.name)
        )
    }

    /// The replicas the host counts as live now, as a subquery `s(id,
    /// partition)`: each once, in the first of its partitions should the
    /// relation repeat it.
    fn replicas_of(replicas: Replicas) -> String {
        let partition = replicas.partition.map_or("NULL::uuid".to_owned(), ident);
        format!(
            "(SELECT DISTINCT ON (id) id, {partition} AS partition FROM {} WHERE {} \
               ORDER BY id, {partition}) AS s",
            replicas.relation, replicas.live
        )
    }

    /// Whether `replica` is a row of the replicas' relation, live or not.
    /// Whether `replica` is one the audience's replicas count live now.
    async fn is_live(&self, conn: &mut Conn, replica: Uuid) -> Result<bool, Error> {
        Ok(sqlx::query_scalar(&format!(
            "SELECT EXISTS (SELECT 1 FROM {} WHERE s.id = $1)",
            Self::replicas_of(self.replicas)
        ))
        .bind(replica)
        .fetch_one(&mut *conn)
        .await?)
    }

    pub(crate) async fn is_replica(&self, conn: &mut Conn, replica: Uuid) -> Result<bool, Error> {
        Ok(sqlx::query_scalar(&format!(
            "SELECT EXISTS (SELECT 1 FROM {} WHERE id = $1)",
            self.replicas.relation
        ))
        .bind(replica)
        .fetch_one(&mut *conn)
        .await?)
    }

    /// Compares the audience's live replicas with what the engine saw of
    /// them last time, in the caller's transaction: a replica that became
    /// live, or moved to another partition, is noted for provisioning from
    /// scratch; one that is no longer live, or gone, is forgotten — nothing
    /// is recorded as held by it, what waited in its outbox is dropped, and
    /// nothing is sent it until it is live again. A replica live in another
    /// of the audiences `declared` is that one's until it lets it go; one
    /// recorded live in an audience no longer declared is taken as one that
    /// arrived. When what places the audience's rows changed since the
    /// engine last ran, every one of its live replicas is provisioned again.
    /// The relay runs it before every pass.
    pub(crate) async fn observe(&self, conn: &mut Conn, declared: &[&str]) -> Result<(), Error> {
        let name = literal(self.name);
        let claimed: i64 = sqlx::query_scalar(&format!(
            "WITH now AS (SELECT s.id, s.partition FROM {} \
                           WHERE NOT EXISTS (SELECT 1 FROM sync.replicas o \
                                              WHERE o.replica_id = s.id AND o.live \
                                                AND o.audience <> {name} AND o.audience = ANY($1))), \
             seen AS (SELECT replica_id, partition_id FROM sync.replicas \
                       WHERE live AND (audience = {name} \
                                       OR audience IS NULL AND replica_id IN (SELECT id FROM now))), \
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
             claimed AS (UPDATE sync.replicas r SET audience = {name} FROM seen s \
                          WHERE r.replica_id = s.replica_id AND r.audience IS NULL \
                            AND s.replica_id NOT IN (SELECT id FROM arrived) \
                          RETURNING r.replica_id), \
             came AS (INSERT INTO sync.replicas AS r (replica_id, live, partition_id, audience) \
                      SELECT id, true, partition, {name} FROM arrived \
                      ON CONFLICT (replica_id) DO UPDATE SET live = true, \
                        partition_id = EXCLUDED.partition_id, audience = EXCLUDED.audience), \
             released AS (UPDATE sync.replicas r SET live = false, partition_id = NULL \
                           FROM gone g WHERE r.replica_id = g.replica_id) \
             SELECT count(*) FROM claimed",
            Self::replicas_of(self.replicas),
            resync_sql("SELECT id, partition FROM arrived")
        ))
        .bind(declared)
        .fetch_one(&mut *conn)
        .await?;
        self.redeclared(conn, claimed > 0).await
    }

    /// Compares what decides which rows the audience's replicas hold and
    /// what a row carries with what the engine last ran with: when it
    /// changed — a deploy that routes a table otherwise, mirrors one more or
    /// one less, sends another column — every live replica of the audience
    /// is provisioned again, and what the engine kept for them of a table
    /// the audience no longer holds is let go of. On the first run after an engine of one audience learned of
    /// audiences, an audience that took over that engine's replicas
    /// (`legacy`), or declares what it ran with, compares with what it ran
    /// with.
    async fn redeclared(&self, conn: &mut Conn, legacy: bool) -> Result<(), Error> {
        let declared = self.declared();
        let now: String = sqlx::query_scalar("SELECT encode(sync.digest($1), 'hex')")
            .bind(&declared)
            .fetch_one(&mut *conn)
            .await?;
        let own: Option<String> =
            sqlx::query_scalar("SELECT declared FROM sync.audiences WHERE name = $1")
                .bind(self.name)
                .fetch_optional(&mut *conn)
                .await?;
        if own.as_deref() == Some(now.as_str()) {
            return Ok(());
        }
        let before = match own {
            Some(own) => Some(own),
            None => sqlx::query_scalar::<_, Option<String>>("SELECT declared FROM sync.version")
                .fetch_one(&mut *conn)
                .await?
                .filter(|ran| legacy || *ran == now),
        };
        sqlx::query(
            "INSERT INTO sync.audiences (name, declared) VALUES ($1, $2) \
             ON CONFLICT (name) DO UPDATE SET declared = EXCLUDED.declared",
        )
        .bind(self.name)
        .bind(&now)
        .execute(&mut *conn)
        .await?;
        if before.is_none_or(|before| before == now) {
            return Ok(());
        }
        tracing::warn!(audience = self.name, "sync: what places the rows changed since the engine last ran — every replica of the audience is provisioned again");
        let tables: Vec<&str> = self.plans.iter().map(|p| p.table).collect();
        for sql in [
            "DELETE FROM sync.held WHERE relation <> ALL($1) \
               AND replica_id IN (SELECT replica_id FROM sync.replicas WHERE audience = $2)",
            "DELETE FROM sync.gone WHERE relation <> ALL($1) \
               AND replica_id IN (SELECT replica_id FROM sync.replicas WHERE audience = $2)",
            "DELETE FROM sync.quarantine WHERE relation <> ALL($1) \
               AND replica_id IN (SELECT replica_id FROM sync.replicas WHERE audience = $2)",
        ] {
            sqlx::query(sql)
                .bind(&tables)
                .bind(self.name)
                .execute(&mut *conn)
                .await?;
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
            // The view decides the pairs, and may answer one twice; the
            // active rows decide what travels at all, and a row's partition
            // which replicas may hold it.
            Route::Routed { .. } => {
                let names: Vec<&str> = plan.key.iter().map(|c| c.name).collect();
                format!(
                    "SELECT DISTINCT s.id AS replica_id, {key} AS key FROM {view} t {} \
                     WHERE {filter}",
                    plan.routing
                        .as_ref()?
                        .join(&names, &replicas, plan.has_partition),
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
    /// question it does not keep are taken back. A row it holds in a version
    /// older than the row — kept while a change of it waits, is parked, or
    /// went where the replica could not take it — holds up what that version
    /// pointed at, which the first change after it recorded: whatever that
    /// change's state, one rule for every question of what a held row points
    /// at.
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
        let made: bool = sqlx::query_scalar("SELECT to_regclass('pg_temp.sync_kept') IS NOT NULL")
            .fetch_one(&mut *conn)
            .await?;
        if !made {
            // The sets a large plan keeps outgrow the default temp_buffers,
            // and then every row a round adds is read back from disk. The
            // setting takes only before a session's first temporary table:
            // on a connection that had one, the default stays.
            sqlx::Executor::execute(&mut *conn, "SAVEPOINT sync_temp_buffers").await?;
            let end = match sqlx::Executor::execute(&mut *conn, "SET temp_buffers = '128MB'").await
            {
                Ok(_) => "RELEASE SAVEPOINT sync_temp_buffers",
                Err(_) => "ROLLBACK TO SAVEPOINT sync_temp_buffers",
            };
            sqlx::Executor::execute(&mut *conn, end).await?;
            sqlx::Executor::execute(&mut *conn, PLAN_TABLES).await?;
        }
        sqlx::Executor::execute(&mut *conn, PLAN_RESET).await?;
        let provisioned: Vec<Uuid> = delta
            .notes
            .iter()
            .filter_map(|n| match n.change {
                Change::Resync { replica } => Some(replica),
                _ => None,
            })
            .collect();
        delta.snapshots = sqlx::query_scalar(&format!(
            "WITH live AS (INSERT INTO sync_live SELECT s.id, s.id = ANY($1) FROM {} \
                           RETURNING replica_id, fresh) \
             SELECT replica_id FROM live WHERE fresh",
            self.replicas_sql()
        ))
        .bind(&provisioned)
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .collect();
        let mut asked = self.note(conn, &delta.notes).await?;
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
            "sync_live, sync_batch, sync_changed, sync_regrouped, sync_reassigned, \
             sync_released, sync_blocked",
        )
        .await?;
        let mut blocked_from = 0;
        loop {
            blocked_from = self.spread_blocked(conn, blocked_from).await?;
            self.keep_sends(conn, &asked).await?;
            self.question(conn, &asked).await?;
            self.keep_all(conn, &asked).await?;
            if !self.conflicts(conn, &asked, blocked_from).await? {
                break;
            }
            sqlx::Executor::execute(
                &mut *conn,
                "DELETE FROM sync_suspect; DELETE FROM sync_kept;",
            )
            .await?;
        }
        self.touch(conn).await?;
        self.settled(conn, &asked).await?;
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
    /// one form keys are compared in, with the newest note of a change of
    /// each (0: tried again only); the parents each pointed at before; the members that
    /// moved; the groups given or taken.
    async fn note(&self, conn: &mut Conn, notes: &[Note]) -> Result<Asked, Error> {
        let ids: Vec<i64> = notes.iter().map(|n| n.id).collect();
        let mut pointed: HashMap<i64, Vec<(String, String)>> = HashMap::new();
        let rows: Vec<(i64, String, String)> =
            sqlx::query_as("SELECT note, relation, row_key FROM sync.pointed WHERE note = ANY($1)")
                .bind(&ids)
                .fetch_all(&mut *conn)
                .await?;
        for (note, table, key) in rows {
            pointed.entry(note).or_default().push((table, key));
        }
        let mut changed: BTreeMap<(usize, String), i64> = BTreeMap::new();
        let mut released: BTreeSet<(usize, String, usize, String)> = BTreeSet::new();
        let mut regrouped: BTreeSet<(usize, String)> = BTreeSet::new();
        let mut reassigned: BTreeSet<(usize, Uuid)> = BTreeSet::new();
        // Parents changed whose columns rows of the audience copy, by key,
        // with their note.
        let mut copied: Vec<(&str, &str, i64)> = Vec::new();
        // Routed tables whose views read what changed, with the partitions
        // it changed in: `None`, every one.
        let mut rerouted: BTreeMap<usize, Option<BTreeSet<Uuid>>> = BTreeMap::new();
        for note in notes {
            match &note.change {
                Change::Row { table, key, retry } => {
                    if !retry && self.depends_on(table) {
                        copied.push((table.as_str(), key.as_str(), note.id));
                    }
                    let Some(i) = self.index(table) else {
                        if self.depends_on(table) {
                            continue;
                        }
                        return Err(Error::Misuse(format!(
                            "a change to `{table}`, which no row declares: its trigger outlived its declaration"
                        )));
                    };
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
                    for (parent, at) in pointed.get(&note.id).into_iter().flatten() {
                        let Some(p) = self.index(parent) else {
                            continue;
                        };
                        let at = key_text(&parse_key(&self.plans[p].key, at).map_err(of_table)?);
                        released.insert((i, key.clone(), p, at));
                    }
                    let newest = changed.entry((i, key)).or_default();
                    if !retry {
                        *newest = (*newest).max(note.id);
                    }
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
                Change::Rerouted { table, partition } => {
                    let routed = self.routed_through(table);
                    if routed.is_empty() {
                        return Err(Error::Misuse(format!(
                            "a change to {table}, which no routed table's view reads: its \
                             trigger outlived its declaration"
                        )));
                    }
                    for i in routed {
                        let partitions = rerouted.entry(i).or_insert_with(|| Some(BTreeSet::new()));
                        match (partitions.as_mut(), partition) {
                            (Some(partitions), Some(partition)) => {
                                partitions.insert(*partition);
                            }
                            _ => *partitions = None,
                        }
                    }
                }
                Change::Resync { .. } => {}
            }
        }
        // A view asked again for a partition is asked for every replica
        // of it, as if each were given a group: what the view answers for
        // it is compared with all it holds of the table.
        for (i, partitions) in rerouted {
            let every = partitions.is_none();
            let partitions: Vec<Uuid> = partitions.into_iter().flatten().collect();
            let replicas: Vec<Uuid> = sqlx::query_scalar(&format!(
                "SELECT s.id FROM {} WHERE $1 OR s.partition = ANY($2)",
                self.replicas_sql()
            ))
            .bind(every)
            .bind(&partitions)
            .fetch_all(&mut *conn)
            .await?;
            reassigned.extend(replicas.into_iter().map(|replica| (i, replica)));
        }
        // A row that copies a changed parent's columns changed with it, as
        // of the parent's note.
        for (i, plan) in self.plans.iter().enumerate() {
            for dependency in &plan.depends {
                let (keys, ids): (Vec<&str>, Vec<i64>) = copied
                    .iter()
                    .filter(|(table, _, _)| *table == dependency.parent.name)
                    .map(|(_, key, id)| (*key, *id))
                    .unzip();
                if keys.is_empty() {
                    continue;
                }
                let parts: Vec<String> = dependency
                    .columns
                    .iter()
                    .zip(&dependency.casts)
                    .enumerate()
                    .map(|(n, (c, cast))| {
                        format!(
                            "t.{} = split_part(p.row_key, '|', {})::{cast}",
                            ident(c),
                            n + 1
                        )
                    })
                    .collect();
                let rows: Vec<(String, i64)> = sqlx::query_as(&format!(
                    "SELECT {}, max(p.id) FROM unnest($1::text[], $2::int8[]) AS p(row_key, id) \
                       JOIN {} t ON {} GROUP BY 1",
                    self.key_of_row(i),
                    plan.relation.sql(),
                    parts.join(" AND "),
                ))
                .bind(&keys)
                .bind(&ids)
                .fetch_all(&mut *conn)
                .await?;
                for (key, id) in rows {
                    let newest = changed.entry((i, key)).or_default();
                    *newest = (*newest).max(id);
                }
            }
        }
        let rel = |i: usize| i32::try_from(i).unwrap_or(i32::MAX);
        let name = |i: usize| self.plans[i].table;
        sqlx::query("INSERT INTO sync_batch SELECT DISTINCT unnest($1::int8[])")
            .bind(&ids)
            .execute(&mut *conn)
            .await?;
        let (keys, noted): (Vec<_>, Vec<i64>) = changed.into_iter().unzip();
        sqlx::query(
            "INSERT INTO sync_changed \
             SELECT * FROM unnest($1::int4[], $2::text[], $3::text[], $4::int8[])",
        )
        .bind(keys.iter().map(|(i, _)| rel(*i)).collect::<Vec<_>>())
        .bind(keys.iter().map(|(i, _)| name(*i)).collect::<Vec<_>>())
        .bind(keys.iter().map(|(_, k)| k.as_str()).collect::<Vec<_>>())
        .bind(&noted)
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
    /// replica holds — as they are, or in the version it holds — from the
    /// round `from` on; answers the round after the last.
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
            // A version it holds that pointed there, changed since, points
            // there still.
            for (c, parent) in self
                .pointing()
                .into_iter()
                .filter(|(_, p)| parents.contains(p))
            {
                let select = format!(
                    "SELECT b.replica_id, {c}, h.row_key, \
                            format('it points at `%s` %s, which cannot go: %s', {}, b.row_key, b.why) \
                       FROM sync_blocked b {} \
                      WHERE b.rel = {parent} AND b.round = $1 - 1",
                    literal(self.plans[parent].table),
                    self.held_older(c, parent, "b.replica_id", "b.row_key"),
                );
                self.add(conn, Set::Blocked, &select, round + 1).await?;
            }
            round += 1;
        }
    }

    /// The rows each replica holds whose place is in question —
    /// `sync_suspect` — closed over what they point at and what follows
    /// them among the rows it holds: of the rows that changed, moved
    /// between groups or belong to a group given or taken, those it holds
    /// that its tables' rules no longer send it; the parents a changed row
    /// pointed at before, for whoever holds it; the rows it holds that
    /// cannot go.
    async fn question(&self, conn: &mut Conn, asked: &Asked) -> Result<(), Error> {
        // The rows a table with a rule sends were put in question with
        // what it sends (see `keep_sends`); those of a table with none, it
        // holds only as long as something needs them.
        for i in (0..self.plans.len()).filter(|&i| self.wanted_sql(i, "true").is_none()) {
            if let Some(held) = self.held_asked(i, asked) {
                let select =
                    format!("SELECT h.replica_id, {i}, h.row_key, NULL FROM ({held}) AS h");
                self.add(conn, Set::Suspect, &select, 0).await?;
            }
        }
        if asked.released {
            // The parents a changed row pointed at, for whoever holds it.
            self.add(
                conn,
                Set::Suspect,
                &format!(
                    "SELECT h.replica_id, x.parent, x.parent_key, NULL FROM sync_released x {} {}",
                    holders("x.relation", "x.row_key"),
                    holds(
                        "JOIN",
                        "h",
                        "c.replica_id",
                        "x.parent_relation",
                        "x.parent_key"
                    )
                ),
                0,
            )
            .await?;
        }
        for i in self.present(conn, "sync_blocked").await? {
            let select = format!(
                "SELECT b.replica_id, b.rel, b.row_key, NULL FROM sync_blocked b {} WHERE b.rel = {i}",
                held_by("b", self.plans[i].table)
            );
            self.add(conn, Set::Suspect, &select, 0).await?;
        }
        // The parent of a follower the batch changed, held by a replica
        // that may hold it only for a version of a row that points there
        // and changed since: whether it keeps it for more than that, and
        // the new follower with it, is the plan's question.
        for (f, column, p) in self
            .followers()
            .filter(|(f, _, _)| asked.changed.contains(f))
        {
            for c in self
                .pointing()
                .into_iter()
                .filter(|(_, parent)| *parent == p)
                .map(|(c, _)| c)
            {
                let select = format!(
                    "SELECT DISTINCT h.replica_id, {p}, pp.row_key, NULL \
                       FROM sync_changed x JOIN {} t ON {} \
                       JOIN sync.pointed pp ON pp.relation = {} AND pp.row_key = t.{}::text \
                       JOIN sync.notes n ON n.id = pp.note AND n.relation = {} \
                                        AND n.id NOT IN (SELECT id FROM sync_batch) \
                       JOIN sync.held h ON h.relation = n.relation AND h.row_key = n.row_key \
                                       AND h.noted < n.id \
                       JOIN sync_live l ON l.replica_id = h.replica_id AND NOT l.fresh \
                      WHERE x.rel = {f}",
                    self.plans[f].relation.sql(),
                    self.row_is(f, "x.row_key"),
                    literal(self.plans[p].table),
                    ident(column),
                    literal(self.plans[c].table),
                );
                self.add(conn, Set::Suspect, &select, 0).await?;
            }
        }
        let mut round = 0;
        loop {
            let from = self.frontier(conn, "sync_suspect", round).await?;
            if from.is_empty() {
                break;
            }
            for (c, reference) in self.references().filter(|(c, _)| from.contains(c)) {
                let p = reference.parent;
                // A parent its own rules send the replica stays whatever
                // points at it: its place is in no question, nor what it
                // holds up.
                let ruled = self
                    .wanted_sql(
                        p,
                        &self.key_among(p, "SELECT replica_id, row_key FROM g", true),
                    )
                    .map_or(String::new(), |w| {
                        format!(" EXCEPT SELECT w.replica_id, w.key FROM ({w}) w")
                    });
                let select = format!(
                    "WITH f AS MATERIALIZED ( \
                         SELECT d.replica_id, {} AS row_key FROM sync_suspect d JOIN {} t ON {} \
                          WHERE d.rel = {c} AND d.round = $1 - 1 AND {}), \
                          g AS MATERIALIZED (SELECT DISTINCT f.replica_id, f.row_key FROM f {}) \
                     SELECT q.replica_id, {p}, q.row_key, NULL FROM (TABLE g{ruled}) AS q",
                    key_sql("t", &reference.columns),
                    self.plans[c].relation.sql(),
                    self.row_is(c, "d.row_key"),
                    not_null("t", &reference.columns),
                    held_by("f", self.plans[p].table),
                );
                self.add(conn, Set::Suspect, &select, round + 1).await?;
            }
            // What the version a replica holds of a row changed since
            // pointed at is in question with it, whatever the row points
            // at now, or whether it is still there.
            for (c, parent) in self
                .pointing()
                .into_iter()
                .filter(|(c, _)| from.contains(c))
            {
                let select = format!(
                    "SELECT d.replica_id, {parent}, p.row_key, NULL FROM sync_suspect d {} {} \
                      WHERE d.rel = {c} AND d.round = $1 - 1",
                    self.pointed_by_held(c, parent, "d"),
                    holds(
                        "JOIN",
                        "h",
                        "d.replica_id",
                        &literal(self.plans[parent].table),
                        "p.row_key"
                    ),
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
        analyze(conn, "sync_suspect").await
    }

    /// What every replica keeps beyond the rows its rules send it, in
    /// `sync_kept`: the rows in question its rules send it, the rows in
    /// question something outside them points at, and the followers of a
    /// parent outside them that are in question or changed; the parents of
    /// the rows its rules send it that it lacks or holds older, and the
    /// followers of those it lacks; then, a round at a time, what those
    /// need — until a round adds nothing and no change waiting to be
    /// planned keeps anything more.
    async fn keep_all(&self, conn: &mut Conn, asked: &Asked) -> Result<(), Error> {
        let doubted = self.present(conn, "sync_suspect").await?;
        for &i in &doubted {
            let filter = self.key_among(
                i,
                &format!("SELECT replica_id, row_key FROM sync_suspect WHERE rel = {i}"),
                true,
            );
            if let Some(wanted) = self.wanted_sql(i, &filter) {
                let source = format!("SELECT w.replica_id, w.key FROM ({wanted}) w");
                self.keep(conn, i, 0, &source).await?;
            }
        }
        for (c, reference) in self.references() {
            if let Some(sends) = self.asked_sends(c, asked, &not_null("t", &reference.columns)) {
                let source = format!(
                    "SELECT a.replica_id, {} FROM ({sends}) a JOIN {} t ON {} WHERE {}",
                    key_sql("t", &reference.columns),
                    self.plans[c].relation.sql(),
                    self.row_is(c, "a.row_key"),
                    not_null("t", &reference.columns),
                );
                self.keep(conn, reference.parent, 0, &source).await?;
            }
        }
        for (f, column, p) in self.followers() {
            if let Some(sends) = self.asked_sends(p, asked, "true") {
                let source = format!(
                    "SELECT a.replica_id, {} FROM ({sends}) a JOIN {} t ON {} WHERE a.lacks",
                    self.key_of_row(f),
                    self.plans[f].active,
                    key_eq("t", &[column], &self.plans[p].key, "a.row_key"),
                );
                self.keep(conn, f, 0, &source).await?;
            }
        }
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
                    "SELECT d.replica_id, d.row_key FROM sync_suspect d JOIN {} t ON {} {} \
                      WHERE d.rel = {f} AND {outside}",
                    self.plans[f].active,
                    self.row_is(f, "d.row_key"),
                    holds(
                        "JOIN",
                        "hp",
                        "d.replica_id",
                        &parent,
                        &format!("t.{}::text", ident(column))
                    ),
                ));
            }
            if asked.changed.contains(&f) {
                sources.push(format!(
                    "SELECT c.replica_id, {} FROM {} t {} \
                      WHERE {} AND {}",
                    self.key_of_row(f),
                    self.plans[f].active,
                    holders(&parent, &format!("t.{}::text", ident(column))),
                    self.key_among(
                        f,
                        &format!("SELECT row_key FROM sync_changed WHERE rel = {f}"),
                        false,
                    ),
                    outside
                        .replace("hp.replica_id", "c.replica_id")
                        .replace("hp.row_key", &format!("t.{}::text", ident(column))),
                ));
            }
            for source in sources {
                self.keep(conn, f, 0, &source).await?;
            }
        }
        let mut round = 0;
        loop {
            let from = self.frontier(conn, "sync_kept", round).await?;
            if from.is_empty() {
                round += 1;
                if doubted.is_empty() || !self.keep_older(conn, round).await? {
                    return analyze(conn, "sync_kept").await;
                }
                continue;
            }
            // A row sent holds up what it points at; one kept as its
            // replica holds it, what that version points at: the row as it
            // is, unless it changed since — then the first change after
            // that version says (`pointed_by_held`), whether planned, in
            // the batch, waiting or parked.
            for (c, reference) in self.references().filter(|(c, _)| from.contains(c)) {
                let source = format!(
                    "SELECT k.replica_id, {} FROM sync_kept k JOIN {} t ON {} \
                      WHERE k.rel = {c} AND k.round = $1 - 1 AND k.up AND {} \
                        AND (k.sends OR NOT EXISTS ( \
                              SELECT 1 FROM sync.held h JOIN sync.notes n ON n.kind = 'row' \
                                 AND n.relation = h.relation AND n.row_key = h.row_key AND n.id > h.noted \
                               WHERE h.replica_id = k.replica_id AND h.relation = {} \
                                 AND h.row_key = k.row_key))",
                    key_sql("t", &reference.columns),
                    self.plans[c].relation.sql(),
                    self.row_is(c, "k.row_key"),
                    not_null("t", &reference.columns),
                    literal(self.plans[c].table),
                );
                self.keep(conn, reference.parent, round + 1, &source)
                    .await?;
            }
            for (c, parent) in self
                .pointing()
                .into_iter()
                .filter(|(c, _)| from.contains(c))
            {
                let source = format!(
                    "SELECT k.replica_id, p.row_key FROM sync_kept k {} \
                      WHERE k.rel = {c} AND k.round = $1 - 1 AND k.up AND NOT k.sends",
                    self.pointed_by_held(c, parent, "k"),
                );
                self.keep_for_older(conn, parent, round + 1, &source)
                    .await?;
            }
            for (f, column, p) in self.followers().filter(|(_, _, p)| from.contains(p)) {
                let source = format!(
                    "SELECT k.replica_id, {key} FROM sync_kept k JOIN {} t ON {} \
                      WHERE k.rel = {p} AND k.round = $1 - 1 AND k.down \
                        AND (NOT k.older OR EXISTS (SELECT 1 FROM sync.held h \
                                                     WHERE h.replica_id = k.replica_id \
                                                       AND h.relation = {} AND h.row_key = {key}))",
                    self.plans[f].active,
                    key_eq("t", &[column], &self.plans[p].key, "k.row_key"),
                    literal(self.plans[f].table),
                    key = self.key_of_row(f),
                );
                self.keep(conn, f, round + 1, &source).await?;
            }
            round += 1;
        }
    }

    /// Keeps, at `round`, the rows in question that a row its replica holds
    /// still points at in the version it holds: one the first change of it
    /// after that version — beyond the batch, waiting, parked or planned
    /// long ago — pointed at before. Answers whether it kept any.
    async fn keep_older(&self, conn: &mut Conn, round: i32) -> Result<bool, Error> {
        let mut kept = 0;
        for (c, parent) in self.pointing() {
            let source = format!(
                "SELECT d.replica_id, d.row_key FROM sync_suspect d {} \
                  WHERE d.rel = {parent} \
                    AND (NOT EXISTS (SELECT 1 FROM sync_suspect x WHERE x.replica_id = h.replica_id \
                                        AND x.rel = {c} AND x.row_key = h.row_key) \
                         OR EXISTS (SELECT 1 FROM sync_kept x WHERE x.replica_id = h.replica_id \
                                       AND x.rel = {c} AND x.row_key = h.row_key AND NOT x.sends))",
                self.held_older(c, parent, "d.replica_id", "d.row_key"),
            );
            kept += self.keep_for_older(conn, parent, round, &source).await?;
        }
        Ok(kept > 0)
    }

    /// Joins `p`, what the version of the row of `c` aliased `alias`
    /// (`replica_id`, `row_key`) that its replica holds pointed at in
    /// `parent`, when a note changed the row since: the pointers the next
    /// note after that version kept.
    fn pointed_by_held(&self, c: usize, parent: usize, alias: &str) -> String {
        let child = literal(self.plans[c].table);
        format!(
            "{} CROSS JOIN LATERAL (SELECT min(n.id) AS id FROM sync.notes n \
                                     WHERE n.kind = 'row' AND n.relation = {child} \
                                       AND n.row_key = {alias}.row_key AND n.id > held.noted) after \
             JOIN sync.pointed p ON p.note = after.id AND p.relation = {}",
            holds(
                "JOIN",
                "held",
                &format!("{alias}.replica_id"),
                &child,
                &format!("{alias}.row_key")
            ),
            literal(self.plans[parent].table),
        )
    }

    /// Every pair of tables one of whose rows can point at a row of the
    /// other: the child, the parent.
    fn pointing(&self) -> BTreeSet<(usize, usize)> {
        self.references().map(|(c, r)| (c, r.parent)).collect()
    }

    /// Joins `h`, the rows of `c` the replica `replica` holds in a version
    /// that pointed at the row of `parent` whose key is the text `text`:
    /// what the first change of the row after that version — beyond the
    /// batch, whether waiting, parked or planned long ago — pointed at
    /// before, which `sync.pointed` keeps by the row pointed at. A change in
    /// the batch is the plan's own question ([`Engine::pointed_by_held`]).
    fn held_older(&self, c: usize, parent: usize, replica: &str, text: &str) -> String {
        let child = literal(self.plans[c].table);
        format!(
            "JOIN sync.pointed p ON p.relation = {} AND p.row_key = {text} \
             JOIN sync.notes n ON n.id = p.note AND n.relation = {child} \
                              AND n.id NOT IN (SELECT id FROM sync_batch) \
             {} \
             JOIN LATERAL (SELECT min(m.id) AS id FROM sync.notes m \
                            WHERE m.kind = 'row' AND m.relation = {child} AND m.row_key = n.row_key \
                              AND m.id > h.noted) first ON first.id = n.id",
            literal(self.plans[parent].table),
            holds("JOIN", "h", replica, &child, "n.row_key"),
        )
    }

    /// Keeps for `i`, at `round` — `$1` to `source` — the rows `source`
    /// names, `(replica_id, row_key)`, but those that cannot go, those kept
    /// already, those its rules send a replica provisioned, and those its
    /// replica holds as they are and that are in no question: those stay
    /// as recorded, and what they need with them. Each is sent where its replica lacks it, refused it, or
    /// holds a version from before a change of the batch — while the row
    /// is there to send: one a change not planned yet keeps may be gone. It
    /// spreads to its parents when sent or in question, and to its
    /// followers when its replica lacks it or it is in question. Answers how
    /// many it kept.
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
        self.add(conn, Set::Kept(i, false), &select, round).await
    }

    /// Keeps for `i`, at `round`, what `source` names as [`Engine::keep`]
    /// does, for a version of a row its replica holds that points there:
    /// the row is held up, and no follower it lacks goes with it.
    async fn keep_for_older(
        &self,
        conn: &mut Conn,
        i: usize,
        round: i32,
        source: &str,
    ) -> Result<u64, Error> {
        let select = format!(
            "SELECT x.replica_id, {i}, x.row_key, NULL FROM ({source}) AS x(replica_id, row_key)"
        );
        self.add(conn, Set::Kept(i, true), &select, round).await
    }

    /// Adds to the plan's `set` the rows `select` answers —
    /// `(replica_id, rel, row_key, why)` — at `round`, which `select` reads
    /// as `$1`: those the set lacks, each once; for the rows a replica
    /// keeps, but those that cannot go. Answers how many it added.
    async fn add(&self, conn: &mut Conn, set: Set, select: &str, round: i32) -> Result<u64, Error> {
        let sql = match set {
            Set::Suspect => format!(
                "INSERT INTO sync_suspect (replica_id, rel, row_key, round) \
                 SELECT q.replica_id, q.rel, q.row_key, $1 FROM ({select}) AS q(replica_id, rel, row_key, why) \
                 ON CONFLICT DO NOTHING"
            ),
            Set::Blocked => format!(
                "INSERT INTO sync_blocked (replica_id, rel, row_key, why, round) \
                 SELECT q.replica_id, q.rel, q.row_key, q.why, $1 FROM ({select}) AS q(replica_id, rel, row_key, why) \
                 ON CONFLICT DO NOTHING"
            ),
            Set::Kept(i, older) => {
                let plan = &self.plans[i];
                format!(
                    "INSERT INTO sync_kept (replica_id, rel, row_key, round, sends, up, down, older) \
                     SELECT DISTINCT ON (s.replica_id, s.rel, s.row_key) \
                            s.replica_id, s.rel, s.row_key, $1, s.sends, s.sends OR s.doubt, \
                            s.lacks OR s.doubt, {older} \
                       FROM (SELECT q.replica_id, q.rel, q.row_key, \
                                    h.replica_id IS NULL OR h.refused AS lacks, \
                                    (h.replica_id IS NULL OR h.refused OR {}) AND t.{} IS NOT NULL AS sends, \
                                    d.replica_id IS NOT NULL AS doubt \
                               FROM ({select}) AS q(replica_id, rel, row_key, why) \
                               {} \
                               LEFT JOIN sync_changed c ON c.rel = q.rel AND c.row_key = q.row_key \
                               LEFT JOIN sync_suspect d ON d.replica_id = q.replica_id AND d.rel = q.rel \
                                                       AND d.row_key = q.row_key \
                               LEFT JOIN {} t ON {} \
                              WHERE NOT EXISTS (SELECT 1 FROM sync_blocked b WHERE b.replica_id = q.replica_id \
                                                   AND b.rel = q.rel AND b.row_key = q.row_key) {}) AS s \
                      WHERE s.lacks OR s.sends OR s.doubt \
                     ON CONFLICT (replica_id, rel, row_key) DO UPDATE SET older = false, round = $1 \
                      WHERE sync_kept.older AND NOT EXCLUDED.older",
                    stale("c.noted"),
                    ident(plan.key[0].name),
                    holds("LEFT JOIN", "h", "q.replica_id", &literal(plan.table), "q.row_key"),
                    plan.relation.sql(),
                    self.row_is(i, "q.row_key"),
                    // What its rules send a replica provisioned goes with
                    // everything it needs already.
                    self.wanted_sql(
                        i,
                        &format!("s.id = q.replica_id AND {}", self.row_is(i, "q.row_key"))
                    )
                    .map_or(String::new(), |whole| format!(
                        "AND NOT (q.replica_id IN (SELECT replica_id FROM sync_live WHERE fresh) \
                                  AND EXISTS ({whole}))"
                    )),
                )
            }
        };
        // A row kept for an older version, and then for itself, spreads
        // again, to every follower, from where it was found the second time.
        Ok(sqlx::query(&sql)
            .bind(round)
            .execute(&mut *conn)
            .await?
            .rows_affected())
    }

    /// Rows kept, or the rules send, that point at a row that cannot go,
    /// there: each joins the rows that cannot go, at round `round`. Answers
    /// whether there was one.
    async fn conflicts(&self, conn: &mut Conn, asked: &Asked, round: i32) -> Result<bool, Error> {
        let blocked = self.present(conn, "sync_blocked").await?;
        let mut found = 0;
        for (c, reference) in self
            .references()
            .filter(|(_, r)| blocked.contains(&r.parent))
        {
            let mut sources = vec![format!(
                "SELECT k.replica_id, k.row_key FROM sync_kept k WHERE k.rel = {c}"
            )];
            sources.extend(
                self.asked_sends(c, asked, "true")
                    .map(|sends| format!("SELECT a.replica_id, a.row_key FROM ({sends}) a")),
            );
            let select = format!(
                "SELECT k.replica_id, {c}, k.row_key, \
                        format('it points at `%s` %s, which cannot go: %s', {}, b.row_key, b.why) \
                   FROM ({}) AS k(replica_id, row_key) JOIN {} t ON {} \
                   JOIN sync_blocked b ON b.replica_id = k.replica_id AND b.rel = {} \
                                      AND b.row_key = {}",
                literal(self.plans[reference.parent].table),
                sources.join(" UNION ALL "),
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

    /// The rows of `i` its rule sends a live replica that the batch asks
    /// about — every one, for a replica provisioned: read as they are sent,
    /// never kept, a snapshot of every replica of a partition being as large
    /// as it is; and of a key changed or moved between groups, of a group
    /// given or taken, what `sync_sends` kept of them — that the replica
    /// lacks, refused, or holds a version of from before a change of the
    /// batch, but those that cannot go: `SELECT replica_id, row_key, lacks`;
    /// those read as they are sent narrowed by `filter` on the table aliased
    /// `t` and the replicas aliased `s`. `None` when the batch asks about
    /// none.
    fn asked_sends(&self, i: usize, asked: &Asked, filter: &str) -> Option<String> {
        let mut parts: Vec<String> = self.fresh_sends(i, asked, filter).into_iter().collect();
        if [&asked.changed, &asked.regrouped, &asked.reassigned]
            .iter()
            .any(|asks| asks.contains(&i))
        {
            parts.push(format!(
                "SELECT replica_id, row_key, lacks FROM sync_sends WHERE rel = {i}"
            ));
        }
        (!parts.is_empty()).then(|| parts.join(" UNION ALL "))
    }

    /// Keeps in `sync_sends` the rows the rules send that the batch asks
    /// about — of a key changed or moved between groups, of a group given
    /// or taken; not those of a replica provisioned — that the replica
    /// lacks, refused, or holds a version of from before a change of the
    /// batch, but those that cannot go: no more than what records it holds
    /// them. And of those rows, the ones a replica holds that its rules no
    /// longer send it, in `sync_suspect`: their place is in question.
    ///
    /// What the rules send and what the replicas hold of the rows asked
    /// about are read once each, and compared as sets, whatever the
    /// statistics of the tables they are read from say.
    async fn keep_sends(&self, conn: &mut Conn, asked: &Asked) -> Result<(), Error> {
        sqlx::Executor::execute(&mut *conn, "DELETE FROM sync_sends").await?;
        let held = "IN (SELECT replica_id FROM sync_live WHERE NOT fresh)";
        for i in 0..self.plans.len() {
            let table = literal(self.plans[i].table);
            let reassigned = format!("IN (SELECT replica_id FROM sync_reassigned WHERE rel = {i})");
            let keys: Vec<String> = [
                (&asked.changed, "sync_changed"),
                (&asked.regrouped, "sync_regrouped"),
            ]
            .into_iter()
            .filter(|(asks, _)| asks.contains(&i))
            .map(|(_, of)| format!("SELECT row_key FROM {of} WHERE rel = {i}"))
            .collect();
            let mut sets = Vec::new();
            if !keys.is_empty() {
                let keys = keys.join(" UNION ");
                let mut those = format!("{held} AND {}", self.key_among(i, &keys, false));
                let mut holders = format!("h.replica_id {held}");
                if asked.reassigned.contains(&i) {
                    those = format!("{those} AND s.id NOT {reassigned}");
                    holders = format!("{holders} AND h.replica_id NOT {reassigned}");
                }
                sets.push((
                    self.wanted_sql(i, &format!("s.id {those}")),
                    format!(
                        "SELECT * FROM ({}) h WHERE {holders}",
                        held_by_key(&table, &keys)
                    ),
                ));
            }
            if asked.reassigned.contains(&i) {
                let replicas = format!(
                    "SELECT x.replica_id FROM sync_reassigned x JOIN sync_live l USING (replica_id) \
                      WHERE x.rel = {i} AND NOT l.fresh"
                );
                sets.push((
                    self.wanted_sql(i, &format!("s.id {held} AND s.id {reassigned}")),
                    held_by_replica(&table, &replicas),
                ));
            }
            // A table with no rule of its own: its rows in question are
            // read with the rest (see `question`).
            for (wanted, holds) in sets {
                let Some(wanted) = wanted else { continue };
                sqlx::query(&format!(
                    "WITH w AS MATERIALIZED (SELECT w.replica_id, w.key FROM ({wanted}) w), \
                          h AS MATERIALIZED (SELECT h.replica_id, h.row_key, h.refused, {} AS stale \
                                               FROM ({holds}) h \
                                               LEFT JOIN sync_changed c ON c.rel = {i} AND c.row_key = h.row_key), \
                          doubted AS (INSERT INTO sync_suspect (replica_id, rel, row_key, round) \
                                      SELECT q.replica_id, {i}, q.row_key, 0 \
                                        FROM (SELECT replica_id, row_key FROM h EXCEPT TABLE w) q \
                                      ON CONFLICT DO NOTHING) \
                     INSERT INTO sync_sends \
                     SELECT {i}, q.row_key, q.replica_id, q.lacks \
                       FROM (SELECT q.*, true AS lacks \
                               FROM (TABLE w EXCEPT SELECT replica_id, row_key FROM h WHERE NOT refused) q \
                             UNION ALL \
                             SELECT q.*, false \
                               FROM (SELECT replica_id, row_key FROM h WHERE NOT refused AND stale \
                                     INTERSECT TABLE w) q) AS q(replica_id, row_key, lacks) \
                      WHERE NOT EXISTS (SELECT 1 FROM sync_blocked b WHERE b.replica_id = q.replica_id \
                                           AND b.rel = {i} AND b.row_key = q.row_key)",
                    stale("c.noted")
                ))
                .execute(&mut *conn)
                .await?;
            }
        }
        analyze(conn, "sync_sends").await
    }

    /// The rows of `i` its rule sends the replicas provisioned, every one,
    /// but those that cannot go, narrowed by `filter` on the table aliased
    /// `t` and the replicas aliased `s`: `SELECT replica_id, row_key, lacks`.
    fn fresh_sends(&self, i: usize, asked: &Asked, filter: &str) -> Option<String> {
        if !asked.fresh {
            return None;
        }
        let wanted = self.wanted_sql(
            i,
            &format!("{filter} AND s.id IN (SELECT replica_id FROM sync_live WHERE fresh)"),
        )?;
        Some(format!(
            "SELECT w.replica_id, w.key AS row_key, true AS lacks FROM ({wanted}) w \
              WHERE NOT EXISTS (SELECT 1 FROM sync_blocked b WHERE b.replica_id = w.replica_id \
                                   AND b.rel = {i} AND b.row_key = w.key)"
        ))
    }

    /// What the replicas hold of the rows of `i` the batch asks about — its
    /// keys changed or moved between groups, every row of a replica given
    /// or taken a group of it — as `SELECT replica_id, row_key`, looked up
    /// by the row and the replica: `None` when it asks about none.
    fn held_asked(&self, i: usize, asked: &Asked) -> Option<String> {
        let table = literal(self.plans[i].table);
        let mut parts = Vec::new();
        for (asks, of) in [
            (&asked.changed, "sync_changed"),
            (&asked.regrouped, "sync_regrouped"),
        ] {
            if asks.contains(&i) {
                let keys = format!("SELECT row_key FROM {of} WHERE rel = {i}");
                parts.push(held_by_key(&table, &keys));
            }
        }
        if asked.reassigned.contains(&i) {
            let replicas = format!("SELECT replica_id FROM sync_reassigned WHERE rel = {i}");
            parts.push(held_by_replica(&table, &replicas));
        }
        (!parts.is_empty()).then(|| {
            format!(
                "SELECT h.replica_id, h.row_key FROM ({}) AS h \
                  WHERE h.replica_id IN (SELECT replica_id FROM sync_live WHERE NOT fresh)",
                parts.join(" UNION ")
            )
        })
    }

    /// What the batch the plan in this transaction planned asks about, as
    /// its sets record it.
    async fn asked(&self, conn: &mut Conn) -> Result<Asked, Error> {
        let rows: Vec<(String, i32)> = sqlx::query_as(
            "SELECT DISTINCT 'changed', rel FROM sync_changed \
             UNION ALL SELECT DISTINCT 'regrouped', rel FROM sync_regrouped \
             UNION ALL SELECT DISTINCT 'reassigned', rel FROM sync_reassigned \
             UNION ALL SELECT 'fresh', 0 WHERE EXISTS (SELECT 1 FROM sync_live WHERE fresh)",
        )
        .fetch_all(&mut *conn)
        .await?;
        let mut asked = Asked::default();
        for (what, rel) in rows {
            let Ok(i) = usize::try_from(rel) else {
                continue;
            };
            match what.as_str() {
                "changed" => asked.changed.insert(i),
                "regrouped" => asked.regrouped.insert(i),
                "reassigned" => asked.reassigned.insert(i),
                _ => {
                    asked.fresh = true;
                    true
                }
            };
        }
        Ok(asked)
    }

    /// Records in `sync_touched` every replica the plan sends to, takes
    /// from or provisions.
    async fn touch(&self, conn: &mut Conn) -> Result<(), Error> {
        let mut sources = vec![
            "SELECT replica_id FROM sync_live WHERE fresh".to_owned(),
            "SELECT replica_id FROM sync_kept WHERE sends".to_owned(),
            format!("SELECT replica_id FROM ({}) AS d", Self::DROPPED),
        ];
        sources.push("SELECT DISTINCT replica_id FROM sync_sends".to_owned());
        for source in sources {
            sqlx::query(&format!(
                "INSERT INTO sync_touched {source} ON CONFLICT DO NOTHING"
            ))
            .execute(&mut *conn)
            .await?;
        }
        Ok(())
    }

    /// What the plan takes back: the rows in question each live replica
    /// does not keep.
    const DROPPED: &'static str = "SELECT d.replica_id, d.rel, d.row_key FROM sync_suspect d \
          JOIN sync_live USING (replica_id) \
         WHERE NOT EXISTS (SELECT 1 FROM sync_kept k WHERE k.replica_id = d.replica_id \
                              AND k.rel = d.rel AND k.row_key = d.row_key)";

    /// Per replica, the tables the plan takes rows of back from it.
    pub(crate) async fn dropping(
        &self,
        conn: &mut Conn,
    ) -> Result<BTreeMap<Uuid, BTreeSet<String>>, Error> {
        let rows: Vec<(Uuid, i32)> = sqlx::query_as(&format!(
            "SELECT DISTINCT d.replica_id, d.rel FROM ({}) AS d",
            Self::DROPPED
        ))
        .fetch_all(&mut *conn)
        .await?;
        let mut dropping: BTreeMap<Uuid, BTreeSet<String>> = BTreeMap::new();
        for (replica, i) in rows {
            let table = usize::try_from(i).map_or("", |i| self.table(i));
            dropping
                .entry(replica)
                .or_default()
                .insert(table.to_owned());
        }
        Ok(dropping)
    }

    /// Every replica the plan sends to, takes from or provisions.
    pub(crate) async fn touched(&self, conn: &mut Conn) -> Result<BTreeSet<Uuid>, Error> {
        Ok(sqlx::query_scalar("SELECT replica_id FROM sync_touched")
            .fetch_all(&mut *conn)
            .await?
            .into_iter()
            .collect())
    }

    /// Opens, as the cursor `sync_sent`, every row the plan sends a live
    /// replica — `(table, key, replica)` — tables parents first, and of a
    /// table what its rules send a replica provisioned, the rest they send,
    /// and what the replicas keep besides, each by key: read once, a page at
    /// a time, by [`Engine::next`]. A snapshot is read in the order of the
    /// table's key, which its index gives, rather than sorted whole.
    pub(crate) async fn open_sent(&self, conn: &mut Conn) -> Result<(), Error> {
        let asked = self.asked(conn).await?;
        let mut parts = Vec::new();
        for &i in &self.order {
            let plan = &self.plans[i];
            let key = self.key_of_row(i);
            let unkept = |replica: &str, key: &str| {
                format!(
                    "NOT EXISTS (SELECT 1 FROM sync_kept k WHERE k.replica_id = {replica} \
                                    AND k.rel = {i} AND k.row_key = {key})"
                )
            };
            let fresh = format!(
                "s.id IN (SELECT replica_id FROM sync_live WHERE fresh) \
                 AND NOT EXISTS (SELECT 1 FROM sync_blocked b WHERE b.replica_id = s.id \
                                    AND b.rel = {i} AND b.row_key = {key}) AND {}",
                unkept("s.id", &key)
            );
            if asked.fresh && matches!(plan.route, Route::Broadcast) {
                // Every row for every replica of its partition: the rows
                // of the partitions provisioned are put in order, each then
                // joined to its replicas, so what goes is never sorted
                // whole.
                let provisioned = match self.replicas.partition {
                    Some(column) => format!(
                        "WHERE t.{} IN (SELECT s.partition FROM {} \
                                         WHERE s.id IN (SELECT replica_id FROM sync_live WHERE fresh))",
                        ident(column),
                        self.replicas_sql()
                    ),
                    None => String::new(),
                };
                parts.push(format!(
                    "(SELECT {i}, t.key, s.id FROM (SELECT {key} AS key, t.* FROM {} t {provisioned} \
                                                   ORDER BY 1 OFFSET 0) t \
                       CROSS JOIN LATERAL (SELECT s.id FROM {} WHERE {} AND {fresh} OFFSET 0) s)",
                    plan.active,
                    self.replicas_sql(),
                    self.same_partition(),
                ));
            } else if let Some(wanted) = self.wanted_sql(i, &fresh).filter(|_| asked.fresh) {
                let by = match plan.route {
                    Route::Grouped { .. } | Route::Routed { .. } => "2".to_owned(),
                    _ => plan
                        .key
                        .iter()
                        .map(|c| format!("t.{}", ident(c.name)))
                        .collect::<Vec<_>>()
                        .join(", "),
                };
                parts.push(format!(
                    "(SELECT {i}, x.key, x.replica_id FROM ({wanted} ORDER BY {by}) AS x)"
                ));
            }
            parts.push(format!(
                "(SELECT {i}, s.row_key, s.replica_id FROM sync_sends s \
                   WHERE s.rel = {i} AND {} ORDER BY s.row_key)",
                unkept("s.replica_id", "s.row_key")
            ));
            parts.push(format!(
                "(SELECT k.rel, k.row_key, k.replica_id FROM sync_kept k \
                   WHERE k.rel = {i} AND k.sends ORDER BY k.row_key)"
            ));
        }
        sqlx::Executor::execute(
            &mut *conn,
            format!(
                "DECLARE sync_sent NO SCROLL CURSOR FOR {}",
                parts.join(" UNION ALL ")
            )
            .as_str(),
        )
        .await?;
        Ok(())
    }

    /// Records in `sync_settled` the rows tried again that the plan no
    /// longer sends a replica that set them aside — deleted, retired, no
    /// longer its: owed to it by nobody — unless they failed again.
    async fn settled(&self, conn: &mut Conn, asked: &Asked) -> Result<(), Error> {
        for &i in &asked.changed {
            let table = literal(self.plans[i].table);
            let quarantined =
                format!("SELECT replica_id, row_key FROM sync.quarantine WHERE relation = {table}");
            let mut sent = vec![format!(
                "SELECT replica_id, row_key FROM sync_kept WHERE rel = {i} AND sends"
            )];
            sent.extend(
                self.fresh_sends(i, asked, &self.key_among(i, &quarantined, true))
                    .map(|sends| format!("SELECT replica_id, row_key FROM ({sends}) AS a")),
            );
            sent.push(format!(
                "SELECT replica_id, row_key FROM sync_sends WHERE rel = {i}"
            ));
            sqlx::query(&format!(
                "INSERT INTO sync_settled \
                 SELECT q.replica_id, q.relation, q.row_key FROM sync.quarantine q \
                   JOIN sync_changed c ON q.relation = c.relation AND q.row_key = c.row_key \
                  WHERE c.rel = {i} AND (q.replica_id, q.row_key) NOT IN ({}) \
                    AND NOT EXISTS (SELECT 1 FROM sync_blocked b WHERE b.replica_id = q.replica_id \
                                       AND b.rel = c.rel AND b.row_key = c.row_key) \
                    AND NOT EXISTS (SELECT 1 FROM sync.replicas r WHERE r.replica_id = q.replica_id \
                                       AND r.audience <> {})",
                sent.join(" UNION ALL "),
                literal(self.name)
            ))
            .execute(&mut *conn)
            .await?;
        }
        Ok(())
    }

    /// Lets go of the rows set aside the plan owes no replica any more; and
    /// notes again, to be tried where they were set aside, those the server
    /// set aside that point at a row the batch changed: what kept them from
    /// going — that row could not go — may be gone with the change, which
    /// the rows themselves did not see.
    pub(crate) async fn settle(&self, conn: &mut Conn) -> Result<(), Error> {
        sqlx::query(
            "DELETE FROM sync.quarantine q USING sync_settled s \
              WHERE q.replica_id = s.replica_id AND q.relation = s.relation AND q.row_key = s.row_key",
        )
        .execute(&mut *conn)
        .await?;
        for (c, reference) in self.references() {
            let child = literal(self.plans[c].table);
            sqlx::query(&format!(
                "INSERT INTO sync.notes (kind, relation, row_key) \
                 SELECT DISTINCT 'retry', q.relation, q.row_key FROM sync_changed x \
                   JOIN {} t ON {} \
                   JOIN sync.quarantine q ON q.relation = {child} AND q.row_key = {} \
                                         AND q.cause = 'server' \
                  WHERE x.rel = {} \
                    AND NOT EXISTS (SELECT 1 FROM sync.notes n \
                                     WHERE n.done_at IS NULL AND n.kind = 'retry' \
                                       AND n.relation = q.relation AND n.row_key = q.row_key)",
                self.plans[c].relation.sql(),
                self.points_at(reference, "x.row_key"),
                self.key_of_row(c),
                reference.parent,
            ))
            .execute(&mut *conn)
            .await?;
        }
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

    /// How many of `keys` of table `i`, from the first, hold no more than
    /// `budget` bytes between them, each as long as it is, not as the
    /// database stores it — one at least: what a pass loads and encodes at
    /// once stays bounded however large the rows.
    pub(crate) async fn fitting(
        &self,
        conn: &mut Conn,
        i: usize,
        keys: &[String],
        budget: usize,
    ) -> Result<usize, Error> {
        let plan = &self.plans[i];
        let sizes: Vec<i64> = sqlx::query_scalar(&format!(
            "SELECT ({})::int8 \
               FROM unnest($1::text[]) WITH ORDINALITY AS k(row_key, n) \
               LEFT JOIN {} t ON {} ORDER BY k.n",
            plan.size,
            plan.read,
            self.row_is(i, "k.row_key"),
        ))
        .bind(keys)
        .fetch_all(&mut *conn)
        .await?;
        let mut total = 0;
        let fit = sizes
            .iter()
            .take_while(|size| {
                total += usize::try_from(**size).unwrap_or(usize::MAX);
                total <= budget
            })
            .count();
        Ok(fit.max(1))
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
    partition_id: Option<Uuid>,
    previous: Option<serde_json::Value>,
}

impl Noted {
    /// The change a note records; `None` for one this build cannot read,
    /// which [`READABLE`] keeps from being read.
    fn change(self) -> Option<Change> {
        Some(match self.kind.as_str() {
            "resync" => Change::Resync {
                replica: self.replica_id?,
            },
            "row" | "retry" => Change::Row {
                retry: self.kind == "retry",
                table: self.relation?,
                key: self.row_key?,
            },
            "members" => Change::Members {
                table: self.relation?,
                member: self.previous?,
            },
            "assigned" => Change::Assigned {
                table: self.relation?,
                replica: self.replica_id?,
            },
            "rerouted" => Change::Rerouted {
                table: self.relation?,
                partition: self.partition_id,
            },
            _ => return None,
        })
    }
}

/// The notes this build reads: one it cannot, from a later build, is left
/// where a later build finds it, and holds back none behind it.
pub(crate) const READABLE: &str =
    "((kind IN ('row', 'retry') AND relation IS NOT NULL AND row_key IS NOT NULL) \
     OR (kind = 'members' AND relation IS NOT NULL AND previous IS NOT NULL) \
     OR (kind = 'assigned' AND relation IS NOT NULL AND replica_id IS NOT NULL) \
     OR (kind = 'rerouted' AND relation IS NOT NULL) \
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
        "SELECT id, kind, relation, row_key, replica_id, partition_id, previous \
           FROM sync.notes WHERE done_at IS NULL AND parked_at IS NULL AND {READABLE} \
            AND noted_at <= now() \
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
/// `source` names — `(id, partition)` — unless one is pending for it, and
/// due: a replica is provisioned once, however many reasons ask for it (one
/// noted for later is carried out with it), and its
/// snapshot, planned with the changes of its pass, carries them rather than
/// a second message. Every provisioning is noted through it.
pub(crate) fn resync_sql(source: &str) -> String {
    format!(
        "INSERT INTO sync.notes (kind, replica_id, partition_id) \
         SELECT DISTINCT ON (r.id) 'resync', r.id, r.partition FROM ({source}) AS r(id, partition) \
          WHERE NOT EXISTS (SELECT 1 FROM sync.notes p WHERE p.kind = 'resync' \
                               AND p.replica_id = r.id AND p.done_at IS NULL \
                               AND p.noted_at <= now())"
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
    replica: Uuid,
    partition: Option<Uuid>,
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

/// Deletes the notes done longer ago than `age`, the oldest first, at most
/// `limit` of them, but those of a row a replica holds a version of from
/// before them; answers how many: a backlog of them is pruned a statement
/// — and a transaction — at a time, never in one.
pub(crate) async fn prune(
    conn: &mut Conn,
    age: std::time::Duration,
    limit: i64,
) -> Result<u64, Error> {
    // A note of a row a replica holds a version from before it says what
    // that version pointed at: it stays while the replica holds it — or may
    // keep it, refusing the row's take-back.
    let done = sqlx::query(
        "DELETE FROM sync.notes WHERE id IN (SELECT n.id FROM sync.notes n \
           WHERE n.done_at < now() - make_interval(secs => $1) \
             AND NOT EXISTS (SELECT 1 FROM sync.held h WHERE n.kind = 'row' \
                                AND h.relation = n.relation AND h.row_key = n.row_key \
                                AND h.noted < n.id) \
             AND NOT EXISTS (SELECT 1 FROM sync.gone g WHERE n.kind = 'row' \
                                AND g.relation = n.relation AND g.row_key = n.row_key \
                                AND g.noted < n.id) \
           ORDER BY n.done_at LIMIT $2)",
    )
    .bind(age.as_secs_f64())
    .bind(limit)
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

/// Each foreign key as the engine reads it, beside its parent's primary
/// key: the columns that are the partition on both sides say the two rows
/// share a partition, which no row crosses anyway, not which parent it is —
/// unless the parent is keyed by the partition too. A key such as
/// `(company_id, region_id)` into `regions (company_id, id)` — how
/// PostgreSQL keeps a row from pointing into another company — points at a
/// region by `region_id`; one made only of the partition column points at no
/// parent; and one into a table keyed by `(id, company_id)`, as a table
/// partitioned by `company_id` is, points at it by both.
fn within_partition(
    foreign_keys: Vec<(ForeignKey, Vec<String>)>,
    partition: Option<&str>,
) -> Vec<ForeignKey> {
    foreign_keys
        .into_iter()
        .filter_map(
            |((child, parent, child_columns, parent_columns), parent_key)| {
                let Some(partition) = partition.filter(|p| !parent_key.iter().any(|k| k == p))
                else {
                    return Some((child, parent, child_columns, parent_columns));
                };
                let (child_columns, parent_columns): (Vec<String>, Vec<String>) = child_columns
                    .into_iter()
                    .zip(parent_columns)
                    .filter(|(c, p)| !(c == partition && p == partition))
                    .unzip();
                (!child_columns.is_empty()).then_some((
                    child,
                    parent,
                    child_columns,
                    parent_columns,
                ))
            },
        )
        .collect()
}

#[path = "routing.rs"]
mod routing;

#[cfg(test)]
#[path = "plan_tests.rs"]
mod plan_tests;

#[cfg(test)]
#[path = "routing_tests.rs"]
mod routing_tests;

#[cfg(test)]
#[path = "plan_bench.rs"]
mod plan_bench;

#[cfg(test)]
impl Engine {
    /// [`Engine::install`] of one audience, `test`.
    pub(crate) async fn install_one(
        conn: &mut Conn,
        tables: &[&Table],
        replicas: Replicas,
    ) -> Result<Self, Error> {
        Self::install(conn, &[Self::one(tables, replicas)]).await
    }

    /// [`Engine::new`] of one audience, `test`.
    pub(crate) async fn new_one(
        conn: &mut Conn,
        tables: &[&Table],
        replicas: Replicas,
    ) -> Result<Self, Error> {
        Self::new(conn, &[Self::one(tables, replicas)]).await
    }

    fn one<'a>(tables: &[&'a Table], replicas: Replicas) -> Declared<'a> {
        Declared {
            name: "test",
            replicas,
            tables: tables.to_vec(),
        }
    }

    /// The planner of an engine of one audience.
    pub(crate) fn only(&self) -> &Planner {
        assert_eq!(self.audiences.len(), 1, "an engine of one audience");
        &self.audiences[0]
    }
}

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
        for step in 1..=crate::SERVER_SCHEMA_VERSION {
            sqlx::raw_sql(&migration(step).expect("a step of this build"))
                .execute(&mut *conn)
                .await
                .expect("the schema");
        }
    }
}
