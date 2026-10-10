//! A table declared once, as the struct that is its row.
//!
//! [`Row`] is implemented on a model whose fields are exactly its table's
//! columns, and says where the table lives:
//!
//! - [`Mirror::Catalog`] — written on the server, held by its replicas.
//! - [`Mirror::Record`] — written on a replica, held by the server.
//! - [`Mirror::Both`] — written on both, split by column: the columns the
//!   replicas own ([`Column::replica_owned`]) travel up, the rest down.
//! - [`Mirror::ServerOnly`] — only the server has it.
//!
//! A table the server sends down says which replicas hold which of its rows
//! with a [`Sync`] strategy, and sync routes by it without a line of code of
//! its own.
//!
//! A mirrored row gets two tests from the derive, so a table cannot drift
//! without a red build: `parity_<table>` holds the struct against the schema
//! of whichever backend the test binary runs on — names, nullability, types,
//! generated columns and the primary key — so the postgres run and the sqlite
//! run together keep both schemas the same; and `registered_<table>` fails if
//! the row is missing from its crate's [`mirrored!`](crate::mirrored) list.
//!
//! What a mirrored table needs on either backend is then generic over its
//! row, and names no column, so a column added to the struct is written and
//! read without touching them:
//!
//! - [`upsert`] — every column, newest wins when the row has a version
//!   ([`Row::VERSION`]).
//! - [`overwrite_except`] / [`update_only`] — a table written on both sides,
//!   each side's columns kept apart.
//! - [`insert_ignore`] — history arriving: first delivery wins.
//! - [`get`] / [`get_many`] — active rows by key, every column.
//!
//! None rewrites [`Row::CREATED`]: a row's creation is its first write.

use crate::{Db, DbPool, Error};
use sqlx::query_builder::Separated;

/// A connection of this backend: `&mut *tx` inside a transaction.
pub type Conn = <Db as sqlx::Database>::Connection;
use uuid::Uuid;

/// Where a table lives, which decides who writes it and who holds a copy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mirror {
    /// Written on the server, mirrored on its replicas.
    Catalog,
    /// Written on a replica, mirrored on the server.
    Record,
    /// Written on both, column by column: `#[row(replica_owned)]` columns travel
    /// up, the others down.
    Both,
    /// Only the server has it.
    ServerOnly,
}

/// Which replicas hold which rows of a table the server sends down
/// (`sync = "…"` on the row).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sync {
    /// No rule of its own: a row travels while a row that travels points at it.
    Referenced,
    /// Every replica of the row's partition.
    Broadcast,
    /// The replica the column names.
    Assigned(&'static str),
    /// Every replica a group holding the row is assigned to. `members` is
    /// `(<group>_id, <row>_id)`, `assigned` is `(<group>_id, <replica>_id)`.
    Grouped {
        members: &'static str,
        assigned: &'static str,
    },
    /// Wherever the row the column points at goes.
    Follows(&'static str),
}

/// How a column travels: a proto scalar for numbers and booleans, text for
/// everything else.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Wire {
    String,
    /// A `smallint`: an `int32` on the wire, narrowed back on arrival.
    Int16,
    Int32,
    Int64,
    Bool,
    Float,
    Double,
    /// A JSON document, as its text.
    Json,
}

/// One column a struct declares.
#[derive(Clone, Copy)]
pub struct Column {
    pub name: &'static str,
    /// The struct field, which is also its name on the wire.
    pub field: &'static str,
    /// Whether the field is an `Option` — read off the field's type as
    /// written, so an alias to `Option` counts as NOT NULL.
    pub nullable: bool,
    /// Computed by the database (`#[row(generated)]`): read, never written.
    pub generated: bool,
    /// Written only by a replica, in a table that syncs both ways.
    pub replica_owned: bool,
    pub wire: Wire,
    /// Whether the field's Rust type decodes the column's database type.
    pub compatible: fn(&<Db as sqlx::Database>::TypeInfo) -> bool,
}

/// A struct's columns and their binds, in one order. Derived with
/// `#[derive(Row)]`, or with `#[derive(Columns)]` for a part a row flattens in.
pub trait Columns {
    /// The column marked `version`, here or in a part flattened in.
    const VERSION_COLUMN: Option<&'static str> = None;
    /// The column marked `created`, here or in a part flattened in.
    const CREATED_COLUMN: Option<&'static str> = None;

    fn columns(out: &mut Vec<Column>);

    fn push_binds<'args>(self, sep: &mut Separated<'_, 'args, Db, &'static str>);
}

pub trait Row:
    Columns + for<'r> sqlx::FromRow<'r, <Db as sqlx::Database>::Row> + Send + Unpin + 'static
{
    const TABLE: &'static str;
    /// The primary key, checked by the parity test: `id`, the parent's for a
    /// table that extends another one to one, or several for a link table.
    const KEY: &'static [&'static str];
    /// The postgres schema; sqlite has none.
    const SCHEMA: &'static str;
    /// The table as this backend names it: `raw.regions` or `regions`.
    const QUALIFIED: &'static str;
    const MIRROR: Mirror;
    const SYNC: Sync;
    /// The relation the rows replicas hold are read through on the server: a
    /// view that leaves out the rows no replica should hold.
    /// `None`: the table itself, every row of it.
    const ACTIVE: Option<&'static str> = None;
    /// The column a replica sets on a row it lets go of but cannot delete,
    /// because a row of its own still points at it. `None`: such a row is a
    /// refusal.
    const RETIRED: Option<&'static str> = None;
    /// Columns the table has on one side or the other that the row leaves out.
    const UNMIRRORED: &'static [&'static str] = &[];
    /// The key column naming the replica a row of a record belongs to: a
    /// replica deletes only its own rows.
    const ORIGIN: Option<&'static str> = None;
    /// The column whose value only moves forward, when the row has it: a row
    /// older than the stored one is not applied.
    const VERSION: Option<&'static str> = None;
    /// The column written once, when the row is created, when the row has it.
    const CREATED: Option<&'static str> = None;

    /// The row's key values, as one comparable string.
    fn key_repr(&self) -> String;

    /// The row's key values, in `KEY` order.
    fn key(&self) -> Key;
}

/// One value of a key: every key column is a uuid or an integer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum KeyPart {
    Uuid(Uuid),
    Int(i64),
}

/// A type a key column can have.
pub trait KeyValue {
    fn key_part(self) -> KeyPart;
}

impl KeyValue for Uuid {
    fn key_part(self) -> KeyPart {
        KeyPart::Uuid(self)
    }
}

impl KeyValue for i64 {
    fn key_part(self) -> KeyPart {
        KeyPart::Int(self)
    }
}

impl KeyValue for i32 {
    fn key_part(self) -> KeyPart {
        KeyPart::Int(self.into())
    }
}

impl KeyPart {
    /// Whether a key column is one of the integer kinds; any other is a UUID.
    pub(crate) fn is_int(column: &Column) -> bool {
        matches!(column.wire, Wire::Int16 | Wire::Int32 | Wire::Int64)
    }

    /// Reads a part back from its text form, as `column` types it: any form
    /// a uuid or an integer is written in, which [`Display`](std::fmt::Display)
    /// writes back in the one form keys are compared in. `None`: no value of
    /// the column.
    pub fn parse(column: &Column, text: &str) -> Option<Self> {
        if Self::is_int(column) {
            text.parse().map(Self::Int).ok()
        } else {
            Uuid::parse_str(text).map(Self::Uuid).ok()
        }
    }

    /// The SQL type a part's text casts back to, as `column` types it.
    pub fn sql_type(column: &Column) -> &'static str {
        if Self::is_int(column) {
            "bigint"
        } else {
            "uuid"
        }
    }

    fn bind<'q>(self, sep: &mut Separated<'_, 'q, Db, &'static str>) {
        match self {
            Self::Uuid(v) => sep.push_bind(v),
            Self::Int(v) => sep.push_bind(v),
        };
    }
}

impl std::fmt::Display for KeyPart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Uuid(v) => v.fmt(f),
            Self::Int(v) => v.fmt(f),
        }
    }
}

/// A row's key, one part per `KEY` column.
pub type Key = Vec<KeyPart>;

/// A key as sync writes it: its parts joined by `|`, each in its one form —
/// a uuid lowercase and hyphenated, an integer decimal.
pub fn key_text(key: &Key) -> String {
    key.iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("|")
}

fn declared<R: Columns>() -> Vec<Column> {
    let mut out = Vec::new();
    R::columns(&mut out);
    out
}

/// The columns a write names, in bind order: all but the generated ones.
fn written_names<R: Columns>() -> Vec<&'static str> {
    declared::<R>()
        .into_iter()
        .filter(|c| !c.generated)
        .map(|c| c.name)
        .collect()
}

/// Inserts `rows` — the last of any two with one key, since a statement may
/// not touch a row twice — in statements that stay under the backend's bind
/// limit, each ending in `conflict`. Answers the keys of the rows it wrote:
/// inserted, or updated where the conflict clause let it.
async fn write_keys<R>(exec: &mut Conn, rows: Vec<R>, conflict: &str) -> Result<Vec<Key>, Error>
where
    R: Row,
{
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let mut seen = std::collections::HashSet::new();
    let mut unique: Vec<R> = rows
        .into_iter()
        .rev()
        .filter(|row| seen.insert(row.key_repr()))
        .collect();
    unique.reverse();

    let names = written_names::<R>();
    let per_statement = (crate::dialect::MAX_BINDS / names.len().max(1)).max(1);
    let kinds = key_kinds::<R>();
    let mut written = Vec::new();
    let mut rest = unique.into_iter().peekable();
    while rest.peek().is_some() {
        let chunk: Vec<R> = rest.by_ref().take(per_statement).collect();
        let mut qb = sqlx::QueryBuilder::new(format!(
            "INSERT INTO {} ({}) ",
            R::QUALIFIED,
            names.join(", ")
        ));
        qb.push_values(chunk, |mut sep, row| row.push_binds(&mut sep));
        qb.push(conflict);
        qb.push(format!(" RETURNING {}", R::KEY.join(", ")));
        let rows = qb.build().fetch_all(&mut *exec).await?;
        for row in &rows {
            written.push(key_of_row(row, &kinds)?);
        }
    }
    Ok(written)
}

/// How each key column decodes.
fn key_kinds<R: Row>() -> Vec<Wire> {
    let columns = declared::<R>();
    R::KEY
        .iter()
        .map(|k| {
            columns
                .iter()
                .find(|c| c.name == *k)
                .map_or(Wire::String, |c| c.wire)
        })
        .collect()
}

/// A key read off a result row whose leading columns are the key's.
fn key_of_row(row: &<Db as sqlx::Database>::Row, kinds: &[Wire]) -> Result<Key, Error> {
    use sqlx::Row as _;
    kinds
        .iter()
        .enumerate()
        .map(|(i, wire)| {
            Ok(match wire {
                Wire::Int16 | Wire::Int32 | Wire::Int64 => KeyPart::Int(row.try_get::<i64, _>(i)?),
                _ => KeyPart::Uuid(row.try_get::<Uuid, _>(i)?),
            })
        })
        .collect()
}

/// `ON CONFLICT (key) DO UPDATE SET` every written column `take` keeps but
/// the key and [`Row::CREATED`]; nothing to set is `DO NOTHING`.
fn update_clause_where<R: Row>(take: impl Fn(&str) -> bool) -> String {
    let set: Vec<String> = written_names::<R>()
        .into_iter()
        .filter(|c| !R::KEY.contains(c) && Some(*c) != R::CREATED && take(c))
        .map(|c| format!("{c} = EXCLUDED.{c}"))
        .collect();
    if set.is_empty() {
        return format!(" ON CONFLICT ({}) DO NOTHING", R::KEY.join(", "));
    }
    format!(
        " ON CONFLICT ({}) DO UPDATE SET {}",
        R::KEY.join(", "),
        set.join(", ")
    )
}

/// Active rows by key, every column: on the server through the relation its
/// [`Row::ACTIVE`] names, on a replica less the rows it retired.
pub async fn get_many<R: Row>(exec: &mut Conn, keys: &[Key]) -> Result<Vec<R>, Error> {
    let names: Vec<&str> = declared::<R>().into_iter().map(|c| c.name).collect();
    let relation = crate::dialect::active::<R>();
    let mut out = Vec::with_capacity(keys.len());
    for chunk in keys.chunks(keys_per_statement::<R>()) {
        let mut qb =
            sqlx::QueryBuilder::new(format!("SELECT {} FROM {relation}", names.join(", ")));
        key_filter::<R>(&mut qb, chunk);
        out.extend(qb.build_query_as::<R>().fetch_all(&mut *exec).await?);
    }
    Ok(out)
}

/// The active row of `key`, as [`get_many`] reads it.
pub async fn get<R: Row>(exec: &mut Conn, key: &Key) -> Result<Option<R>, Error> {
    Ok(get_many::<R>(exec, std::slice::from_ref(key)).await?.pop())
}

/// `WHERE (k1, k2) IN ((…), …)` for one chunk of keys.
fn key_filter<'q, R: Row>(qb: &mut sqlx::QueryBuilder<'q, Db>, keys: &[Key]) {
    qb.push(format!(" WHERE ({}) IN (", R::KEY.join(", ")));
    for (i, key) in keys.iter().enumerate() {
        if i > 0 {
            qb.push(", ");
        }
        qb.push("(");
        let mut sep = qb.separated(", ");
        for part in key {
            part.bind(&mut sep);
        }
        qb.push(")");
    }
    qb.push(")");
}

/// How many keys fit a statement under the backend's bind limit.
fn keys_per_statement<R: Row>() -> usize {
    (crate::dialect::MAX_BINDS / R::KEY.len().max(1)).max(1)
}

/// Every row of `keys`, retired or not — what sync sends, read from the
/// table itself rather than its active rows.
pub async fn get_by_keys<R: Row>(exec: &mut Conn, keys: &[Key]) -> Result<Vec<R>, Error> {
    let names: Vec<&str> = declared::<R>().into_iter().map(|c| c.name).collect();
    let mut out = Vec::with_capacity(keys.len());
    for chunk in keys.chunks(keys_per_statement::<R>()) {
        let mut qb =
            sqlx::QueryBuilder::new(format!("SELECT {} FROM {}", names.join(", "), R::QUALIFIED));
        key_filter::<R>(&mut qb, chunk);
        out.extend(qb.build_query_as::<R>().fetch_all(&mut *exec).await?);
    }
    Ok(out)
}

/// Deletes the rows of `keys`.
pub async fn delete_by_keys<R: Row>(exec: &mut Conn, keys: &[Key]) -> Result<u64, Error> {
    let mut affected = 0;
    for chunk in keys.chunks(keys_per_statement::<R>()) {
        let mut qb = sqlx::QueryBuilder::new(format!("DELETE FROM {}", R::QUALIFIED));
        key_filter::<R>(&mut qb, chunk);
        affected += qb.build().execute(&mut *exec).await?.rows_affected();
    }
    Ok(affected)
}

/// What [`retire_by_keys`] did with the rows it was asked to let go of.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Retired {
    pub deleted: u64,
    /// Kept, because a row here still points at them, and retired.
    pub kept: u64,
    /// Still held as they were: the database refused to let them go (a
    /// trigger), or a row here points at them and the table has no
    /// [`Row::RETIRED`] column. With why.
    pub refused: Vec<(Key, String)>,
}

/// Lets go of the rows of `keys` on a replica, one by one as far as the
/// database is concerned: a row nothing here points at is deleted; one a row
/// of the replica's own still points at — through a foreign key that would
/// refuse the delete, checked now whether it is deferred or not — is kept
/// with its [`Row::RETIRED`] column set; and one the database refuses to let
/// go is answered apart, so it cannot fail the rest. A transient failure is
/// the error.
#[cfg(all(feature = "sqlite", not(feature = "postgres")))]
pub async fn retire_by_keys<R: Row>(exec: &mut Conn, keys: &[Key]) -> Result<Retired, Error> {
    let pointed: Vec<String> = crate::dialect::restricting(exec, R::TABLE, R::KEY)
        .await?
        .into_iter()
        .map(|(child, from, to)| {
            let on: Vec<String> = from
                .iter()
                .zip(&to)
                .map(|(f, t)| format!("c.\"{f}\" = {}.\"{t}\"", R::QUALIFIED))
                .collect();
            format!(
                "EXISTS (SELECT 1 FROM \"{child}\" c WHERE {})",
                on.join(" AND ")
            )
        })
        .collect();
    let unpointed = if pointed.is_empty() {
        String::new()
    } else {
        format!(" AND NOT ({})", pointed.join(" OR "))
    };
    let mut retired = Retired::default();
    for chunk in keys.chunks(keys_per_statement::<R>()) {
        let delete = |keys: &[Key]| {
            let mut qb = sqlx::QueryBuilder::new(format!("DELETE FROM {}", R::QUALIFIED));
            key_filter::<R>(&mut qb, keys);
            qb.push(&unpointed);
            qb
        };
        let refused = one_by_one(exec, chunk, delete, &mut retired.deleted).await?;
        let left: Vec<Key> = chunk
            .iter()
            .filter(|key| !refused.iter().any(|(r, _)| r == *key))
            .cloned()
            .collect();
        retired.refused.extend(refused);
        let Some(column) = R::RETIRED else {
            let held = key_select::<R>(exec, &left).await?;
            retired.refused.extend(held.into_iter().map(|key| {
                let why = format!(
                    "a row here points at it, and `{}` has no retired column to keep it by",
                    R::TABLE
                );
                (key, why)
            }));
            continue;
        };
        let retire = |keys: &[Key]| {
            let mut qb = sqlx::QueryBuilder::new(format!(
                "UPDATE {} SET {column} = {}",
                R::QUALIFIED,
                crate::dialect::NOW
            ));
            key_filter::<R>(&mut qb, keys);
            qb.push(format!(" AND {column} IS NULL"));
            qb
        };
        let refused = one_by_one(exec, &left, retire, &mut retired.kept).await?;
        retired.refused.extend(refused);
    }
    Ok(retired)
}

/// Runs the statement `build` makes for `keys` in a savepoint, adding the
/// rows it touched to `affected`; when the database refuses it, runs it key
/// by key, and answers the keys refused, with why.
#[cfg(all(feature = "sqlite", not(feature = "postgres")))]
async fn one_by_one<'k>(
    exec: &mut Conn,
    keys: &'k [Key],
    build: impl Fn(&'k [Key]) -> sqlx::QueryBuilder<'k, Db>,
    affected: &mut u64,
) -> Result<Vec<(Key, String)>, Error> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    savepoint(exec).await?;
    let all = build(keys).build().execute(&mut *exec).await;
    match released(exec, all.map_err(Error::from)).await? {
        Ok(done) => {
            *affected += done.rows_affected();
            return Ok(Vec::new());
        }
        Err(e) => tracing::warn!("rows refused together, tried one by one: {e}"),
    }
    let mut refused = Vec::new();
    for key in keys {
        savepoint(exec).await?;
        let one = build(std::slice::from_ref(key))
            .build()
            .execute(&mut *exec)
            .await;
        match released(exec, one.map_err(Error::from)).await? {
            Ok(done) => *affected += done.rows_affected(),
            Err(e) => refused.push((key.clone(), e.to_string())),
        }
    }
    Ok(refused)
}

/// The keys of `keys` the table holds.
#[cfg(all(feature = "sqlite", not(feature = "postgres")))]
async fn key_select<R: Row>(exec: &mut Conn, keys: &[Key]) -> Result<Vec<Key>, Error> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let kinds = key_kinds::<R>();
    let mut qb = sqlx::QueryBuilder::new(format!(
        "SELECT {} FROM {}",
        R::KEY.join(", "),
        R::QUALIFIED
    ));
    key_filter::<R>(&mut qb, keys);
    let rows = qb.build().fetch_all(&mut *exec).await?;
    rows.iter().map(|row| key_of_row(row, &kinds)).collect()
}

/// Opens a savepoint, for [`released`] to close.
pub(crate) async fn savepoint(exec: &mut Conn) -> Result<(), Error> {
    sqlx::query("SAVEPOINT sync_rows")
        .execute(&mut *exec)
        .await?;
    Ok(())
}

/// Closes the savepoint [`savepoint`] opened around the work that answered
/// `result`: a refusal — any error but a transient one, which retrying would
/// meet again — comes back as the inner error, rolled back with the
/// transaction still usable; a transient failure is the outer one. When the
/// database already rolled the whole transaction back (a trigger's
/// `RAISE(ROLLBACK)`, a full disk), the refusal is the outer error too: there
/// is nothing left to go on with.
pub(crate) async fn released<T>(
    exec: &mut Conn,
    result: Result<T, Error>,
) -> Result<Result<T, Error>, Error> {
    let refusal = match result {
        Ok(value) => {
            sqlx::query("RELEASE SAVEPOINT sync_rows")
                .execute(&mut *exec)
                .await?;
            return Ok(Ok(value));
        }
        Err(e) if e.is_transient() => return Err(e),
        Err(e) => e,
    };
    let rolled_back = sqlx::query("ROLLBACK TO SAVEPOINT sync_rows")
        .execute(&mut *exec)
        .await;
    if rolled_back.is_err() {
        return Err(refusal);
    }
    sqlx::query("RELEASE SAVEPOINT sync_rows")
        .execute(&mut *exec)
        .await?;
    Ok(Err(refusal))
}

/// Clears the [`Row::RETIRED`] column of the rows of `keys`: the other side
/// offers them again.
pub async fn unretire_by_keys<R: Row>(exec: &mut Conn, keys: &[Key]) -> Result<(), Error> {
    let Some(retired) = R::RETIRED else {
        return Ok(());
    };
    for chunk in keys.chunks(keys_per_statement::<R>()) {
        let mut qb =
            sqlx::QueryBuilder::new(format!("UPDATE {} SET {retired} = NULL", R::QUALIFIED));
        key_filter::<R>(&mut qb, chunk);
        qb.push(format!(" AND {retired} IS NOT NULL"));
        qb.build().execute(&mut *exec).await?;
    }
    Ok(())
}

/// Every key the table holds that is not retired.
pub async fn live_keys<R: Row>(exec: &mut Conn) -> Result<Vec<Key>, Error> {
    let live = R::RETIRED
        .map(|retired| format!(" WHERE {retired} IS NULL"))
        .unwrap_or_default();
    keys_where::<R>(exec, &live).await
}

/// Every key the table holds.
pub async fn keys<R: Row>(exec: &mut Conn) -> Result<Vec<Key>, Error> {
    keys_where::<R>(exec, "").await
}

/// The keys of the rows `filter` admits. A key stored in a form the engine
/// does not read — a uuid as text, written by something other than the
/// engine — is logged and left out: that row is not one the engine holds.
async fn keys_where<R: Row>(exec: &mut Conn, filter: &str) -> Result<Vec<Key>, Error> {
    let kinds = key_kinds::<R>();
    let rows = sqlx::query(&format!(
        "SELECT {} FROM {}{filter}",
        R::KEY.join(", "),
        R::QUALIFIED
    ))
    .fetch_all(&mut *exec)
    .await?;
    Ok(rows
        .iter()
        .filter_map(|row| {
            key_of_row(row, &kinds)
                .inspect_err(|e| {
                    tracing::error!(
                        table = R::TABLE,
                        "a key stored in a form the engine does not read — left out: {e}"
                    );
                })
                .ok()
        })
        .collect())
}

/// Every row, the incoming value winning on a known key for every column but
/// `keep` — the columns the other side owns — unless the stored version is
/// newer: a redelivered older version never rolls the row back. Answers the
/// keys it wrote.
pub async fn overwrite_except<R: Row>(
    exec: &mut Conn,
    rows: Vec<R>,
    keep: &[&str],
) -> Result<Vec<Key>, Error> {
    let conflict = newest::<R>(update_clause_where::<R>(|c| !keep.contains(&c)));
    write_keys(exec, rows, &conflict).await
}

/// Every row, a known key taking only `columns` — the ones this side owns.
/// Answers the keys it wrote.
pub async fn update_only<R: Row>(
    exec: &mut Conn,
    rows: Vec<R>,
    columns: &[&str],
) -> Result<Vec<Key>, Error> {
    write_keys(
        exec,
        rows,
        &update_clause_where::<R>(|c| columns.contains(&c)),
    )
    .await
}

/// Every column of every row, the incoming value winning on a known key —
/// unless the stored version is newer, so a late redelivery never rolls the
/// row back. Answers the keys it wrote.
pub async fn upsert<R: Row>(exec: &mut Conn, rows: Vec<R>) -> Result<Vec<Key>, Error> {
    write_keys(exec, rows, &newest::<R>(update_clause_where::<R>(|_| true))).await
}

/// Every row whose key is new; a redelivered one is left as it was. Answers
/// the keys it inserted.
pub async fn insert_ignore<R: Row>(exec: &mut Conn, rows: Vec<R>) -> Result<Vec<Key>, Error> {
    let conflict = format!(" ON CONFLICT ({}) DO NOTHING", R::KEY.join(", "));
    write_keys(exec, rows, &conflict).await
}

/// The row's version column, when it has one.
pub fn version<R: Row>() -> Option<&'static str> {
    R::VERSION.filter(|v| written_names::<R>().contains(v))
}

/// A conflict update that applies only when the incoming version is not older
/// than the stored one, for a row that has one.
fn newest<R: Row>(mut conflict: String) -> String {
    if let Some(version) = version::<R>().filter(|_| conflict.contains("DO UPDATE")) {
        let kind = declared::<R>()
            .iter()
            .find(|c| c.name == version)
            .map_or(Wire::String, |c| c.wire);
        conflict.push_str(" WHERE ");
        conflict.push_str(&crate::dialect::not_older(
            &format!("EXCLUDED.{version}"),
            &format!("{}.{version}", R::QUALIFIED),
            kind,
        ));
    }
    conflict
}

/// Fails unless the table has exactly the struct's columns — each as
/// nullable as its field, of a type the field decodes, generated where the
/// field says so — and the primary key `KEY` names, less the columns the row
/// leaves out ([`Row::RETIRED`], [`Row::UNMIRRORED`]).
pub async fn assert_parity<R: Row>(pool: &DbPool) {
    let table = R::QUALIFIED;
    let declared = declared::<R>();

    let mut actual: Vec<(String, bool, bool)> = crate::dialect::columns(pool, R::SCHEMA, R::TABLE)
        .await
        .expect("read the table's columns")
        .into_iter()
        .filter(|(c, _, _)| Some(c.as_str()) != R::RETIRED && !R::UNMIRRORED.contains(&c.as_str()))
        .collect();
    assert!(
        !actual.is_empty(),
        "table `{table}` not found on {}",
        crate::dialect::NAME
    );

    let mut expected: Vec<(String, bool, bool)> = declared
        .iter()
        .map(|c| (c.name.to_owned(), c.nullable, c.generated))
        .collect();
    expected.sort();
    actual.sort();
    assert_eq!(
        actual,
        expected,
        "`{table}` on {} differs from its row struct (column, nullable, generated)",
        crate::dialect::NAME,
    );

    let primary_key = crate::dialect::primary_key(pool, R::SCHEMA, R::TABLE)
        .await
        .expect("read the table's primary key");
    assert_eq!(
        primary_key,
        R::KEY,
        "`{table}` on {}: the row's `key` is not the table's primary key",
        crate::dialect::NAME,
    );

    let described = sqlx::Executor::describe(pool, &format!("SELECT * FROM {table}"))
        .await
        .expect("describe the table");
    let mismatched: Vec<String> = declared
        .iter()
        .filter_map(|c| {
            let column = described
                .columns()
                .iter()
                .find(|d| sqlx::Column::name(*d) == c.name)?;
            let info = sqlx::Column::type_info(column);
            (!(c.compatible)(info)).then(|| format!("{} ({info})", c.name))
        })
        .collect();
    assert!(
        mismatched.is_empty(),
        "`{table}` on {}: these columns' types don't decode into their fields: {}",
        crate::dialect::NAME,
        mismatched.join(", "),
    );
}

/// A mirrored table as sync reads it: where it lives, who holds its rows and
/// the columns that travel.
pub struct Table {
    pub name: &'static str,
    pub schema: &'static str,
    /// The row struct's name, which is its message on the wire.
    pub message: &'static str,
    /// The row struct's full path, for the generated mappers.
    pub rust_path: &'static str,
    pub key: &'static [&'static str],
    pub mirror: Mirror,
    pub sync: Sync,
    /// [`Row::ACTIVE`].
    pub active: Option<&'static str>,
    pub columns: Vec<Column>,
    /// [`Row::RETIRED`].
    pub retired: Option<&'static str>,
    /// [`Row::ORIGIN`].
    pub origin: Option<&'static str>,
    /// [`Row::VERSION`], when the row has it.
    pub version: Option<&'static str>,
}

impl Table {
    pub fn of<R: Row>() -> Self {
        let path = std::any::type_name::<R>();
        Self {
            name: R::TABLE,
            schema: R::SCHEMA,
            message: path.rsplit("::").next().unwrap_or(path),
            rust_path: path,
            key: R::KEY,
            mirror: R::MIRROR,
            sync: R::SYNC,
            active: R::ACTIVE,
            columns: declared::<R>(),
            retired: R::RETIRED,
            origin: R::ORIGIN,
            version: version::<R>(),
        }
    }
}

/// A crate's mirrored tables, listed once — the list sync dispatches on, in
/// the order it sends them: a table after the tables it points at. Defines
/// `TABLES` and `tables()`; a server-only row in it does not compile, and
/// `Engine::new` refuses a noted table no row
/// declares.
///
/// Led by `migrator = "path::TO_MIGRATOR";`, it also defines a test holding
/// every listed row against the table that migrator builds
/// ([`assert_parity`]): a column added on one side only fails it.
#[macro_export]
macro_rules! mirrored {
    (migrator = $migrator:literal; $($row:path),* $(,)?) => {
        $crate::mirrored!($($row),*);

        #[cfg(test)]
        #[sqlx::test(migrator = $migrator)]
        async fn every_mirrored_row_is_its_table_column_for_column(pool: $crate::DbPool) {
            $($crate::row::assert_parity::<$row>(&pool).await;)*
        }
    };
    ($($row:path),* $(,)?) => {
        $(const _: () = assert!(
            !matches!(<$row as $crate::row::Row>::MIRROR, $crate::row::Mirror::ServerOnly),
            "a server-only row is not mirrored",
        );)*
        pub const TABLES: &[&str] = &[$(<$row as $crate::row::Row>::TABLE),*];

        pub fn tables() -> Vec<$crate::row::Table> {
            vec![$($crate::row::Table::of::<$row>()),*]
        }
    };
}
