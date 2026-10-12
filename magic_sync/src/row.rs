//! A table declared once, as the struct that is its row.
//!
//! A struct derives [`Row`] with fields that are exactly its
//! table's columns, and says where the table lives ([`Mirror`]) and which
//! replicas hold which of its rows; the derive's documentation lists every
//! attribute. The engine checks the declaration against the database when it
//! starts, on either side: a column the table has and the struct does not, or
//! the other way round, one as nullable on one side only, of a type its field
//! does not decode, or a primary key other than the row's `key`, is refused
//! with every difference named.
//!
//! What a host does with its rows directly is then generic over the row and
//! names no column, so a column added to the struct is written and read
//! without touching them:
//!
//! - [`upsert`] — every column, newest wins when the row has a version.
//! - [`insert_ignore`] — history: the first delivery wins.
//! - [`get`] / [`get_many`] — rows as the replicas see them, by key.
//! - [`get_by_keys`] — rows by key, every one the table holds.
//!
//! None rewrites a `#[row(created)]` column: a row's creation is its first
//! write.

use crate::dialect::ident;
use crate::{Conn, Db, Error};
use sqlx::query_builder::Separated;
use uuid::Uuid;

/// Where a table lives, which decides who writes it and who holds a copy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Mirror {
    /// Written on the server, held by its replicas (`mirror = "catalog"`).
    Catalog,
    /// Written on a replica, held by the server (`mirror = "record"`).
    Record,
    /// Written on both, column by column (`mirror = "both"`): the columns the
    /// replicas own travel up, the others down.
    Both,
    /// Only the server has it (`mirror = "server"`): a row the host writes
    /// with this module's functions, and that never travels.
    ServerOnly,
}

/// Which replicas hold which rows of a table the server sends down: the
/// row's `sync = "…"`.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
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

/// One column a struct declares, as the derive writes it.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct Column {
    pub name: &'static str,
    /// The struct field, which is the column's name in the field's serde.
    pub field: &'static str,
    /// Whether the field is an `Option`, as its type is written.
    pub nullable: bool,
    /// Computed by the database (`#[row(generated)]`): read, never written.
    pub generated: bool,
    /// Written only by a replica, in a table that syncs both ways.
    pub replica_owned: bool,
    /// A key column's kind, read off its type.
    pub key: Option<KeyKind>,
    /// The version column's kind, read off its type.
    pub version: Option<VersionKind>,
    /// Whether the field is an `i64` (or an `Option` of one), which JSON
    /// writes as its text.
    pub int64: bool,
    /// Whether the field's Rust type decodes the column's database type.
    pub compatible: fn(&<Db as sqlx::Database>::TypeInfo) -> bool,
}

/// A struct's columns, and their binds in one order: what
/// [`#[derive(Columns)]`](crate::Columns) writes for a part a row flattens in
/// (`#[sqlx(flatten)]`), and [`#[derive(Row)]`](crate::Row) for the row.
///
/// Its items are the derive's; it is sealed, and implemented only through the
/// derive:
///
/// ```compile_fail,E0277
/// struct Mine;
/// impl magic_sync::Columns for Mine {}
/// ```
pub trait Columns: crate::__private::Sealed {
    /// The column marked `version`, here or in a part flattened in.
    #[doc(hidden)]
    const VERSION: Option<&'static str>;
    /// The column marked `created`, here or in a part flattened in.
    #[doc(hidden)]
    const CREATED: Option<&'static str>;
    /// Whether a column here, or in a part flattened in, is replica-owned.
    #[doc(hidden)]
    const REPLICA_OWNED: bool;

    /// Every column, in the struct's order.
    #[doc(hidden)]
    fn columns(out: &mut Vec<Column>);

    /// Binds every column a write names, in [`Columns::columns`]' order.
    #[doc(hidden)]
    fn push_binds<'args>(self, sep: &mut Separated<'_, 'args, Db, &'static str>);
}

/// A struct that is a table's row, column for column: implemented by
/// [`#[derive(Row)]`](crate::Row), and only by it.
///
/// ```
/// use chrono::{DateTime, Utc};
/// use uuid::Uuid;
///
/// /// A product: written at the head office, held by every register.
/// #[derive(sqlx::FromRow, Clone, magic_sync::Row)]
/// #[row(table = "products", schema = "shop", mirror = "catalog", sync = "broadcast")]
/// pub struct Product {
///     pub id: Uuid,
///     pub name: String,
///     #[row(version)]
///     pub updated_at: DateTime<Utc>,
/// }
///
/// use magic_sync::row::{Mirror, Row};
/// assert_eq!(Product::TABLE, "products");
/// assert_eq!(Product::KEY, ["id"]);
/// assert_eq!(Product::MIRROR, Mirror::Catalog);
/// ```
pub trait Row:
    Columns + for<'r> sqlx::FromRow<'r, <Db as sqlx::Database>::Row> + Send + Unpin + 'static
{
    /// The table's name: also its name on the wire.
    const TABLE: &'static str;
    /// Its PostgreSQL schema; a replica's SQLite has none.
    const SCHEMA: &'static str;
    /// The primary key's columns, in key order: `id`, unless the row says
    /// otherwise.
    const KEY: &'static [&'static str];
    /// Where the table lives.
    const MIRROR: Mirror;

    /// The table as this backend names it in SQL, quoted:
    /// `"shop"."products"` on postgres, `"products"` on sqlite.
    #[doc(hidden)]
    const QUALIFIED: &'static str;
    #[doc(hidden)]
    const ROUTE: Route;
    /// The relation, as SQL, the rows replicas hold are read through on the
    /// server: a view that leaves out the rows no replica should hold.
    /// `None`: the table itself, every row of it.
    #[doc(hidden)]
    const ACTIVE: Option<&'static str>;
    /// The column a replica sets on a row it lets go of but cannot delete,
    /// because a row of its own still points at it. `None`: such a row is a
    /// refusal.
    #[doc(hidden)]
    const RETIRED: Option<&'static str>;
    /// Columns the table has on one side or the other that the row leaves out.
    #[doc(hidden)]
    const UNMIRRORED: &'static [&'static str];
    /// The key column naming the replica a row of a record belongs to: a
    /// replica deletes only its own rows.
    #[doc(hidden)]
    const ORIGIN: Option<&'static str>;

    /// The row's key values, in [`Row::KEY`] order.
    fn key(&self) -> Key;
}

/// One value of a key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[non_exhaustive]
pub enum KeyPart {
    /// A uuid column's value.
    Uuid(Uuid),
    /// An integer column's value, whatever its width.
    Int(i64),
}

/// What a key column holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum KeyKind {
    /// A uuid.
    Uuid,
    /// An integer: `smallint`, `integer` or `bigint`.
    Integer,
}

/// A type a key column can have: a uuid or an integer. A host's newtype
/// around one implements it the way its inner type does.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be a key column",
    label = "a key column is a `Uuid`, an `i64`, an `i32` or an `i16`",
    note = "a newtype around one of them implements `magic_sync::row::KeyValue` as its inner type does"
)]
pub trait KeyValue {
    /// What the column holds.
    const KIND: KeyKind;

    /// The value, as one part of a key.
    fn key_part(&self) -> KeyPart;
}

impl KeyValue for Uuid {
    const KIND: KeyKind = KeyKind::Uuid;

    fn key_part(&self) -> KeyPart {
        KeyPart::Uuid(*self)
    }
}

impl KeyValue for i64 {
    const KIND: KeyKind = KeyKind::Integer;

    fn key_part(&self) -> KeyPart {
        KeyPart::Int(*self)
    }
}

impl KeyValue for i32 {
    const KIND: KeyKind = KeyKind::Integer;

    fn key_part(&self) -> KeyPart {
        KeyPart::Int((*self).into())
    }
}

impl KeyValue for i16 {
    const KIND: KeyKind = KeyKind::Integer;

    fn key_part(&self) -> KeyPart {
        KeyPart::Int((*self).into())
    }
}

/// What a `#[row(version)]` column holds, which decides how two versions
/// compare where the database stores them alike.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum VersionKind {
    /// A number that only grows.
    Integer,
    /// An instant: SQLite stores it as text, compared as the instant it
    /// writes.
    Instant,
}

/// A type a `#[row(version)]` column can have: an integer or an instant. A
/// host's newtype around one implements it the way its inner type does.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be a #[row(version)] column",
    label = "a version is an integer or a `chrono` instant",
    note = "a newtype around one of them implements `magic_sync::row::Version` as its inner type does"
)]
pub trait Version {
    /// What the column holds.
    const KIND: VersionKind;
}

impl Version for i16 {
    const KIND: VersionKind = VersionKind::Integer;
}

impl Version for i32 {
    const KIND: VersionKind = VersionKind::Integer;
}

impl Version for i64 {
    const KIND: VersionKind = VersionKind::Integer;
}

impl<Tz: chrono::TimeZone> Version for chrono::DateTime<Tz> {
    const KIND: VersionKind = VersionKind::Instant;
}

impl Version for chrono::NaiveDateTime {
    const KIND: VersionKind = VersionKind::Instant;
}

impl KeyPart {
    /// Reads a part back from its text form, as a column of `kind`: any form
    /// a uuid or an integer is written in, which [`Display`](std::fmt::Display)
    /// writes back in the one form keys are compared in. `None`: no value of
    /// such a column.
    pub(crate) fn parse(kind: Option<KeyKind>, text: &str) -> Option<Self> {
        match kind {
            Some(KeyKind::Integer) => text.parse().map(Self::Int).ok(),
            _ => Uuid::parse_str(text).map(Self::Uuid).ok(),
        }
    }

    /// The SQL type a part's text casts back to, as a column of `kind`.
    #[cfg(feature = "postgres")]
    pub(crate) fn sql_type(kind: Option<KeyKind>) -> &'static str {
        match kind {
            Some(KeyKind::Integer) => "bigint",
            _ => "uuid",
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
    /// The one form keys are compared in: a uuid lowercase and hyphenated, an
    /// integer decimal.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Uuid(v) => v.fmt(f),
            Self::Int(v) => v.fmt(f),
        }
    }
}

/// A row's key, one part per key column.
pub type Key = Vec<KeyPart>;

/// A key as sync writes it: its parts joined by `|`, each in its one form —
/// a uuid lowercase and hyphenated, an integer decimal.
pub(crate) fn key_text(key: &Key) -> String {
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

/// `names` quoted, as a list SQL reads.
fn listed(names: &[&str]) -> String {
    names
        .iter()
        .map(|n| ident(n))
        .collect::<Vec<_>>()
        .join(", ")
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
        .filter(|row| seen.insert(row.key()))
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
            listed(&names)
        ));
        qb.push_values(chunk, |mut sep, row| row.push_binds(&mut sep));
        qb.push(conflict);
        qb.push(format!(" RETURNING {}", listed(R::KEY)));
        let rows = qb.build().fetch_all(&mut *exec).await?;
        for row in &rows {
            written.push(key_of_row(row, &kinds)?);
        }
    }
    Ok(written)
}

/// What each key column holds, in key order.
fn key_kinds<R: Row>() -> Vec<Option<KeyKind>> {
    let columns = declared::<R>();
    R::KEY
        .iter()
        .map(|k| columns.iter().find(|c| c.name == *k).and_then(|c| c.key))
        .collect()
}

/// A key read off a result row whose leading columns are the key's.
fn key_of_row(row: &<Db as sqlx::Database>::Row, kinds: &[Option<KeyKind>]) -> Result<Key, Error> {
    use sqlx::Row as _;
    kinds
        .iter()
        .enumerate()
        .map(|(i, kind)| {
            Ok(match kind {
                Some(KeyKind::Integer) => KeyPart::Int(row.try_get::<i64, _>(i)?),
                _ => KeyPart::Uuid(row.try_get::<Uuid, _>(i)?),
            })
        })
        .collect()
}

/// `ON CONFLICT (key) DO UPDATE SET` every written column `take` keeps but
/// the key and the row's `created` column; nothing to set is `DO NOTHING`.
fn update_clause_where<R: Row>(take: impl Fn(&str) -> bool) -> String {
    let set: Vec<String> = written_names::<R>()
        .into_iter()
        .filter(|c| !R::KEY.contains(c) && Some(*c) != R::CREATED && take(c))
        .map(|c| format!("{0} = EXCLUDED.{0}", ident(c)))
        .collect();
    if set.is_empty() {
        return format!(" ON CONFLICT ({}) DO NOTHING", listed(R::KEY));
    }
    format!(
        " ON CONFLICT ({}) DO UPDATE SET {}",
        listed(R::KEY),
        set.join(", ")
    )
}

/// The rows of `keys` replicas see, every column: on the server through the
/// relation the row's `active` names, on a replica less the rows it retired.
/// A key with no such row is left out.
///
/// # Errors
///
/// [`Error::Database`] when the database fails, or a row does not decode as
/// `R`.
pub async fn get_many<R: Row>(exec: &mut Conn, keys: &[Key]) -> Result<Vec<R>, Error> {
    let names: Vec<&str> = declared::<R>().into_iter().map(|c| c.name).collect();
    let relation = crate::dialect::active::<R>();
    let mut out = Vec::with_capacity(keys.len());
    for chunk in keys.chunks(keys_per_statement::<R>()) {
        let mut qb = sqlx::QueryBuilder::new(format!("SELECT {} FROM {relation}", listed(&names)));
        key_filter::<R>(&mut qb, chunk);
        out.extend(qb.build_query_as::<R>().fetch_all(&mut *exec).await?);
    }
    Ok(out)
}

/// The row of `key` replicas see, as [`get_many`] reads it.
///
/// # Errors
///
/// [`Error::Database`] when the database fails, or a row does not decode as
/// `R`.
pub async fn get<R: Row>(exec: &mut Conn, key: &Key) -> Result<Option<R>, Error> {
    Ok(get_many::<R>(exec, std::slice::from_ref(key)).await?.pop())
}

/// `WHERE (k1, k2) IN ((…), …)` for one chunk of keys.
fn key_filter<R: Row>(qb: &mut sqlx::QueryBuilder<'_, Db>, keys: &[Key]) {
    qb.push(format!(" WHERE ({}) IN (", listed(R::KEY)));
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

/// Every row of `keys` the table holds, retired or not — what sync sends,
/// read from the table itself rather than as the replicas see it. A key with
/// no row is left out.
///
/// # Errors
///
/// [`Error::Database`] when the database fails, or a row does not decode as
/// `R`.
pub async fn get_by_keys<R: Row>(exec: &mut Conn, keys: &[Key]) -> Result<Vec<R>, Error> {
    let names: Vec<&str> = declared::<R>().into_iter().map(|c| c.name).collect();
    let mut out = Vec::with_capacity(keys.len());
    for chunk in keys.chunks(keys_per_statement::<R>()) {
        let mut qb =
            sqlx::QueryBuilder::new(format!("SELECT {} FROM {}", listed(&names), R::QUALIFIED));
        key_filter::<R>(&mut qb, chunk);
        out.extend(qb.build_query_as::<R>().fetch_all(&mut *exec).await?);
    }
    Ok(out)
}

/// Deletes the rows of `keys`.
#[cfg(feature = "postgres")]
pub(crate) async fn delete_by_keys<R: Row>(exec: &mut Conn, keys: &[Key]) -> Result<u64, Error> {
    let mut affected = 0;
    for chunk in keys.chunks(keys_per_statement::<R>()) {
        let mut qb = sqlx::QueryBuilder::new(format!("DELETE FROM {}", R::QUALIFIED));
        key_filter::<R>(&mut qb, chunk);
        affected += qb.build().execute(&mut *exec).await?.rows_affected();
    }
    Ok(affected)
}

/// What [`retire_by_keys`] did with the rows it was asked to let go of.
#[cfg(feature = "sqlite")]
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Retired {
    pub(crate) deleted: u64,
    /// Kept, because a row here still points at them, and retired.
    pub(crate) kept: u64,
    /// Still held as they were: the database refused to let them go (a
    /// trigger), or a row here points at them and the table has no retired
    /// column. With why.
    pub(crate) refused: Vec<(Key, String)>,
}

/// Lets go of the rows of `keys` on a replica, one by one as far as the
/// database is concerned: a row nothing here points at is deleted; one a row
/// of the replica's own still points at — through any foreign key, checked
/// now whether it is deferred or not, and whatever it does on delete: a
/// cascade or a set-null would change the replica's own row — is kept with
/// its retired column set; and one the database refuses to let go is
/// answered apart, so it cannot fail the rest. The rows of `held` are kept
/// retired whatever points at them, for what the replica wrote of them has
/// not reached the server yet. A transient failure is the error.
#[cfg(feature = "sqlite")]
pub(crate) async fn retire_by_keys<R: Row>(
    exec: &mut Conn,
    keys: &[Key],
    held: &[Key],
) -> Result<Retired, Error> {
    let mut retired = Retired::default();
    if keys.is_empty() && held.is_empty() {
        return Ok(retired);
    }
    let pointed: Vec<String> = crate::dialect::pointing(exec, R::TABLE, R::KEY)
        .await?
        .into_iter()
        .map(|(child, from, to)| {
            let on: Vec<String> = from
                .iter()
                .zip(&to)
                .map(|(f, t)| format!("c.{} = {}.{}", ident(f), R::QUALIFIED, ident(t)))
                .collect();
            format!(
                "EXISTS (SELECT 1 FROM {} c WHERE {})",
                ident(&child),
                on.join(" AND ")
            )
        })
        .collect();
    let unpointed = if pointed.is_empty() {
        String::new()
    } else {
        format!(" AND NOT ({})", pointed.join(" OR "))
    };
    for (keys, hold) in [(keys, false), (held, true)] {
        for chunk in keys.chunks(keys_per_statement::<R>()) {
            let delete = |keys: &[Key]| {
                let mut qb = sqlx::QueryBuilder::new(format!("DELETE FROM {}", R::QUALIFIED));
                key_filter::<R>(&mut qb, keys);
                qb.push(&unpointed);
                qb
            };
            let refused = if hold {
                Vec::new()
            } else {
                one_by_one(exec, chunk, delete, &mut retired.deleted).await?
            };
            let left: Vec<Key> = chunk
                .iter()
                .filter(|key| !refused.iter().any(|(r, _)| r == *key))
                .cloned()
                .collect();
            retired.refused.extend(refused);
            let Some(column) = R::RETIRED else {
                let kept = key_select::<R>(exec, &left).await?;
                retired.refused.extend(kept.into_iter().map(|key| {
                    let why = if hold {
                        format!(
                            "what this replica wrote of it has not reached the server, and `{}` \
                             has no retired column to keep it by",
                            R::TABLE
                        )
                    } else {
                        format!(
                            "a row here points at it, and `{}` has no retired column to keep it by",
                            R::TABLE
                        )
                    };
                    (key, why)
                }));
                continue;
            };
            let retire = |keys: &[Key]| {
                let mut qb = sqlx::QueryBuilder::new(format!(
                    "UPDATE {} SET {} = {}",
                    R::QUALIFIED,
                    ident(column),
                    crate::dialect::NOW
                ));
                key_filter::<R>(&mut qb, keys);
                qb.push(format!(" AND {} IS NULL", ident(column)));
                qb
            };
            let refused = one_by_one(exec, &left, retire, &mut retired.kept).await?;
            retired.refused.extend(refused);
        }
    }
    Ok(retired)
}

/// The keys of `keys` whose row points, through a foreign key, at a row the
/// database does not hold: what a deferred key refuses at commit, found
/// before it does — none when the connection enforces no foreign key. A key
/// with a null column points at nothing.
#[cfg(feature = "sqlite")]
pub(crate) async fn unparented<R: Row>(exec: &mut Conn, keys: &[Key]) -> Result<Vec<Key>, Error> {
    let enforced: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(&mut *exec)
        .await?;
    if enforced == 0 {
        return Ok(Vec::new());
    }
    let missing: Vec<String> = crate::dialect::pointing_from(exec, R::TABLE)
        .await?
        .into_iter()
        .map(|(parent, from, to)| {
            let set = |c: &String| format!("{}.{}", R::QUALIFIED, ident(c));
            let known: Vec<String> = from
                .iter()
                .zip(&to)
                .map(|(f, t)| format!("p.{} = {}", ident(t), set(f)))
                .collect();
            let present: Vec<String> = from
                .iter()
                .map(|f| format!("{} IS NOT NULL", set(f)))
                .collect();
            format!(
                "({} AND NOT EXISTS (SELECT 1 FROM {} p WHERE {}))",
                present.join(" AND "),
                ident(&parent),
                known.join(" AND ")
            )
        })
        .collect();
    if missing.is_empty() {
        return Ok(Vec::new());
    }
    let kinds = key_kinds::<R>();
    let mut orphans = Vec::new();
    for chunk in keys.chunks(keys_per_statement::<R>()) {
        let mut qb =
            sqlx::QueryBuilder::new(format!("SELECT {} FROM {}", listed(R::KEY), R::QUALIFIED));
        key_filter::<R>(&mut qb, chunk);
        qb.push(format!(" AND ({})", missing.join(" OR ")));
        for row in qb.build().fetch_all(&mut *exec).await? {
            orphans.push(key_of_row(&row, &kinds)?);
        }
    }
    Ok(orphans)
}

/// Runs the statement `build` makes for `keys` in a savepoint, adding the
/// rows it touched to `affected`; when the database refuses it, runs it key
/// by key, and answers the keys refused, with why.
#[cfg(feature = "sqlite")]
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
#[cfg(feature = "sqlite")]
async fn key_select<R: Row>(exec: &mut Conn, keys: &[Key]) -> Result<Vec<Key>, Error> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let kinds = key_kinds::<R>();
    let mut qb =
        sqlx::QueryBuilder::new(format!("SELECT {} FROM {}", listed(R::KEY), R::QUALIFIED));
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
/// `result`: a refusal of the rows ([`Error::is_refusal`]) comes back as the
/// inner error, rolled back with the transaction still usable; any other
/// failure is the outer one. When the database already rolled the whole
/// transaction back (a trigger's `RAISE(ROLLBACK)`), the refusal is the
/// outer error too: there is nothing left to go on with.
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
        Err(e) if !e.is_refusal() => return Err(e),
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

/// Clears the retired column of the rows of `keys`: the other side offers
/// them again.
#[cfg(feature = "sqlite")]
pub(crate) async fn unretire_by_keys<R: Row>(exec: &mut Conn, keys: &[Key]) -> Result<(), Error> {
    let Some(retired) = R::RETIRED else {
        return Ok(());
    };
    for chunk in keys.chunks(keys_per_statement::<R>()) {
        let mut qb = sqlx::QueryBuilder::new(format!(
            "UPDATE {} SET {} = NULL",
            R::QUALIFIED,
            ident(retired)
        ));
        key_filter::<R>(&mut qb, chunk);
        qb.push(format!(" AND {} IS NOT NULL", ident(retired)));
        qb.build().execute(&mut *exec).await?;
    }
    Ok(())
}

/// Every key the table holds that is not retired.
#[cfg(feature = "sqlite")]
pub(crate) async fn live_keys<R: Row>(exec: &mut Conn) -> Result<Vec<Key>, Error> {
    let live = R::RETIRED
        .map(|retired| format!(" WHERE {} IS NULL", ident(retired)))
        .unwrap_or_default();
    keys_where::<R>(exec, &live).await
}

/// Every key the table holds.
#[cfg(feature = "sqlite")]
pub(crate) async fn keys<R: Row>(exec: &mut Conn) -> Result<Vec<Key>, Error> {
    keys_where::<R>(exec, "").await
}

/// The keys of the rows `filter` admits. A key stored in a form the engine
/// does not read — a uuid as text, written by something other than the
/// engine — is logged and left out: that row is not one the engine holds.
#[cfg(feature = "sqlite")]
async fn keys_where<R: Row>(exec: &mut Conn, filter: &str) -> Result<Vec<Key>, Error> {
    let kinds = key_kinds::<R>();
    let rows = sqlx::query(&format!(
        "SELECT {} FROM {}{filter}",
        listed(R::KEY),
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
/// `keep` — the columns the other side owns — whatever its version: what the
/// server sends is ordered by the message that carries it, not by the row's
/// version. Answers the keys it wrote.
#[cfg(feature = "sqlite")]
pub(crate) async fn overwrite_except<R: Row>(
    exec: &mut Conn,
    rows: Vec<R>,
    keep: &[&str],
) -> Result<Vec<Key>, Error> {
    let conflict = update_clause_where::<R>(|c| !keep.contains(&c));
    write_keys(exec, rows, &conflict).await
}

/// Every row, a known key taking only `columns` — the ones this side owns.
/// Answers the keys it wrote.
#[cfg(feature = "postgres")]
pub(crate) async fn update_only<R: Row>(
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

/// Writes every column of every row, the incoming value winning on a key the
/// table holds — unless the row has a version and the stored one is newer,
/// so a late write never rolls the row back. Answers the keys it wrote. On
/// a replica, of a table it writes whole: a record.
///
/// # Errors
///
/// [`Error::Database`] when the database fails or refuses a row;
/// [`Error::Misuse`] on a replica, for a table the server writes — a
/// catalog, or one written on both sides, of which a replica edits only
/// the columns it owns, with an `UPDATE` of those.
pub async fn upsert<R: Row>(exec: &mut Conn, rows: Vec<R>) -> Result<Vec<Key>, Error> {
    writes_whole::<R>()?;
    write_keys(exec, rows, &newest::<R>(update_clause_where::<R>(|_| true))).await
}

/// Refuses a host's write of whole rows of a table the server writes, on a
/// replica: its triggers would refuse the rows one by one.
fn writes_whole<R: Row>() -> Result<(), Error> {
    let how = match R::MIRROR {
        _ if crate::dialect::WRITES_ANY_TABLE => return Ok(()),
        Mirror::Record => return Ok(()),
        Mirror::Both => {
            "a row of it begins on the server: a replica edits the columns it owns, with an UPDATE of those"
        }
        Mirror::Catalog | Mirror::ServerOnly => "it is the server's: a replica does not write it",
    };
    Err(Error::Misuse(format!(
        "`{}` is not a replica's to write whole: {how}",
        R::TABLE
    )))
}

/// Inserts every row whose key is new; one the table holds is left as it
/// was. Answers the keys it inserted. On a replica, of a record only, as
/// [`upsert`].
///
/// # Errors
///
/// [`Error::Database`] when the database fails or refuses a row;
/// [`Error::Misuse`] on a replica, for a table the server writes.
pub async fn insert_ignore<R: Row>(exec: &mut Conn, rows: Vec<R>) -> Result<Vec<Key>, Error> {
    writes_whole::<R>()?;
    let conflict = format!(" ON CONFLICT ({}) DO NOTHING", listed(R::KEY));
    write_keys(exec, rows, &conflict).await
}

/// A conflict update that applies only when the incoming version is not older
/// than the stored one, for a row that has one.
fn newest<R: Row>(mut conflict: String) -> String {
    let Some(version) = R::VERSION.filter(|_| conflict.contains("DO UPDATE")) else {
        return conflict;
    };
    let kind = declared::<R>()
        .iter()
        .find(|c| c.name == version)
        .and_then(|c| c.version)
        .unwrap_or(VersionKind::Integer);
    conflict.push_str(" WHERE ");
    conflict.push_str(&crate::dialect::not_older(
        &format!("EXCLUDED.{}", ident(version)),
        &format!("{}.{}", R::QUALIFIED, ident(version)),
        kind,
    ));
    conflict
}

/// A mirrored table as sync reads it: where it lives, who holds its rows and
/// the columns that travel.
pub(crate) struct Table {
    pub(crate) name: &'static str,
    pub(crate) schema: &'static str,
    /// The table as SQL names it on this backend, quoted.
    pub(crate) qualified: &'static str,
    pub(crate) key: &'static [&'static str],
    pub(crate) mirror: Mirror,
    #[cfg(feature = "postgres")]
    pub(crate) route: Route,
    /// [`Row::ACTIVE`].
    #[cfg(feature = "postgres")]
    pub(crate) active: Option<&'static str>,
    pub(crate) columns: Vec<Column>,
    /// [`Row::RETIRED`].
    pub(crate) retired: Option<&'static str>,
    /// [`Row::UNMIRRORED`].
    pub(crate) unmirrored: &'static [&'static str],
    /// [`Row::ORIGIN`].
    pub(crate) origin: Option<&'static str>,
    /// How its codec writes a row: `json`, or the message's type. Empty
    /// until a codec says.
    #[cfg(feature = "postgres")]
    pub(crate) encoding: &'static str,
}

impl Table {
    pub(crate) fn of<R: Row>() -> Self {
        Self {
            name: R::TABLE,
            schema: R::SCHEMA,
            qualified: R::QUALIFIED,
            key: R::KEY,
            mirror: R::MIRROR,
            #[cfg(feature = "postgres")]
            route: R::ROUTE,
            #[cfg(feature = "postgres")]
            active: R::ACTIVE,
            columns: declared::<R>(),
            retired: R::RETIRED,
            unmirrored: R::UNMIRRORED,
            origin: R::ORIGIN,
            #[cfg(feature = "postgres")]
            encoding: "",
        }
    }

    /// The columns replicas write in a table written on both sides.
    pub(crate) fn replica_owned(&self) -> Vec<&'static str> {
        self.columns
            .iter()
            .filter(|c| c.replica_owned)
            .map(|c| c.name)
            .collect()
    }

    /// Where the row contradicts itself in a way only the types of its
    /// flattened parts show, which the derive cannot read: a column twice,
    /// or one the row leaves `unmirrored` that is also a field.
    fn contradictions(&self) -> Vec<String> {
        let mut out = Vec::new();
        for (i, column) in self.columns.iter().enumerate() {
            if self.columns[..i].iter().any(|c| c.name == column.name) {
                out.push(format!(
                    "the column `{}` is a field of the row twice, once in a part flattened in",
                    column.name
                ));
            }
        }
        for name in self.unmirrored {
            if self.columns.iter().any(|c| c.name == *name) {
                out.push(format!(
                    "`{name}` is a field of the row, in a part flattened in, and is also `unmirrored`"
                ));
            }
        }
        out
    }

    /// How the table differs from its row on this database: each column the
    /// table has and the row does not, or the other way round, one nullable
    /// on one side only, generated on one side only, or of a type its field
    /// does not decode, and a primary key of other columns than the row's. The columns
    /// the row leaves out — its retired column, its unmirrored ones — are
    /// not compared. Empty when they agree.
    pub(crate) async fn differences(&self, conn: &mut Conn) -> Result<Vec<String>, Error> {
        let mut out = self.contradictions();
        let actual = crate::dialect::columns(conn, self.schema, self.name).await?;
        if actual.is_empty() {
            out.push(format!("the table is not on {}", crate::dialect::NAME));
            return Ok(out);
        }
        let left_out = |name: &str| Some(name) == self.retired || self.unmirrored.contains(&name);
        for (name, nullable, generated) in &actual {
            if left_out(name) {
                continue;
            }
            let Some(declared) = self.columns.iter().find(|c| c.name == name) else {
                out.push(format!("the column `{name}` is not a field of the row"));
                continue;
            };
            if declared.nullable != *nullable {
                out.push(format!(
                    "`{name}` is {} in the table and {} in the row",
                    if *nullable { "nullable" } else { "NOT NULL" },
                    if declared.nullable {
                        "an Option"
                    } else {
                        "no Option"
                    },
                ));
            }
            if declared.generated != *generated {
                out.push(format!(
                    "`{name}` is {}generated in the table and {}#[row(generated)] in the row",
                    if *generated { "" } else { "not " },
                    if declared.generated { "" } else { "not " },
                ));
            }
        }
        for column in &self.columns {
            if !actual.iter().any(|(name, _, _)| name == column.name) {
                out.push(format!(
                    "the field `{}` is no column of the table",
                    column.name
                ));
            }
        }
        let mut primary_key = crate::dialect::primary_key(conn, self.schema, self.name).await?;
        let mut key: Vec<&str> = self.key.to_vec();
        // In any order: a key is compared, written and bound column by
        // column, by name.
        primary_key.sort_unstable();
        key.sort_unstable();
        if primary_key != key {
            out.push(format!(
                "the primary key is {primary_key:?}, and the row's key {key:?}"
            ));
        }
        let described =
            sqlx::Executor::describe(&mut *conn, &format!("SELECT * FROM {}", self.qualified))
                .await?;
        for column in described.columns() {
            let name = sqlx::Column::name(column);
            let Some(declared) = self.columns.iter().find(|c| c.name == name) else {
                continue;
            };
            let info = sqlx::Column::type_info(column);
            if !(declared.compatible)(info) {
                out.push(format!(
                    "`{name}` is {info} in the table, which its field does not decode"
                ));
            }
        }
        Ok(out)
    }
}

/// Refuses tables that are not their rows, column for column, naming every
/// difference of every table ([`Table::differences`]): the engine never runs
/// on a declaration the database does not hold.
pub(crate) async fn parity(conn: &mut Conn, tables: &[&Table]) -> Result<(), Error> {
    let mut wrong = Vec::new();
    for table in tables {
        for difference in table.differences(conn).await? {
            wrong.push(format!("`{}.{}`: {difference}", table.schema, table.name));
        }
    }
    if wrong.is_empty() {
        return Ok(());
    }
    Err(Error::Misuse(format!(
        "tables that are not their rows — change the struct or the table:\n{}",
        wrong.join("\n")
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    type Cents = i64;

    /// Every name a database quotes: a reserved word, a space, a capital.
    #[derive(sqlx::FromRow, Clone, Debug, PartialEq, crate::Row)]
    #[row(
        table = "order",
        schema = "Shop",
        mirror = "catalog",
        key = "group, line",
        active = "Shop.live orders",
        retired = "retired at"
    )]
    struct Order {
        group: Uuid,
        line: i32,
        r#type: String,
        #[sqlx(rename = "Total Due")]
        total: Cents,
        note: Option<Cents>,
        #[row(version)]
        at: chrono::DateTime<chrono::Utc>,
    }

    #[derive(sqlx::FromRow, Clone, Debug, PartialEq, crate::Columns)]
    struct Label {
        label: String,
    }

    /// Two parts that bring in the same column, and one that brings in a
    /// column the row says it leaves out: nothing the derive can see.
    #[derive(sqlx::FromRow, Clone, Debug, PartialEq, crate::Row)]
    #[row(table = "tangled", mirror = "catalog", unmirrored = "label")]
    struct Tangled {
        id: Uuid,
        #[sqlx(flatten)]
        first: Label,
        #[sqlx(flatten)]
        second: Label,
    }

    #[test]
    fn a_row_whose_flattened_parts_contradict_it_says_so() {
        assert_eq!(
            Table::of::<Tangled>().contradictions(),
            [
                "the column `label` is a field of the row twice, once in a part flattened in",
                "`label` is a field of the row, in a part flattened in, and is also `unmirrored`",
            ]
        );
        assert!(Table::of::<Order>().contradictions().is_empty());
    }

    /// Nullable is read off the standard `Option`, by any of its paths.
    #[derive(sqlx::FromRow, Clone, Debug, PartialEq, crate::Row)]
    #[row(table = "paths", mirror = "catalog")]
    struct Paths {
        id: Uuid,
        bare: Option<String>,
        by_std: std::option::Option<String>,
        by_core: ::core::option::Option<String>,
        plain: String,
    }

    #[test]
    fn an_option_is_nullable_by_whichever_path_it_is_written() {
        let nullable: Vec<_> = Table::of::<Paths>()
            .columns
            .iter()
            .map(|c| (c.name, c.nullable))
            .collect();
        assert_eq!(
            nullable,
            [
                ("id", false),
                ("bare", true),
                ("by_std", true),
                ("by_core", true),
                ("plain", false)
            ]
        );
    }

    #[test]
    fn a_row_declares_its_table_as_the_database_names_it() {
        let table = Table::of::<Order>();
        assert_eq!(table.name, "order");
        assert_eq!(table.schema, "Shop");
        #[cfg(feature = "postgres")]
        {
            assert_eq!(table.qualified, r#""Shop"."order""#);
            assert_eq!(table.active, Some(r#""Shop"."live orders""#));
        }
        #[cfg(feature = "sqlite")]
        assert_eq!(table.qualified, r#""order""#);
        assert_eq!(table.key, ["group", "line"]);
        let column = |name: &str| *table.columns.iter().find(|c| c.name == name).unwrap();
        assert_eq!(column("group").key, Some(KeyKind::Uuid));
        assert_eq!(column("line").key, Some(KeyKind::Integer));
        assert_eq!(column("type").field, "type");
        assert_eq!(column("Total Due").field, "total");
        assert!(column("Total Due").int64, "an alias of i64 is an i64");
        assert!(column("note").int64 && column("note").nullable);
        assert!(!column("line").int64);
        assert_eq!(column("at").version, Some(VersionKind::Instant));
        assert_eq!(Order::VERSION, Some("at"));
        assert_eq!(table.retired, Some("retired at"));
    }

    /// The engine writes and reads a table whatever its names, quoting each.
    #[cfg(feature = "sqlite")]
    #[sqlx::test(migrations = false)]
    async fn a_table_named_as_sql_would_refuse_unquoted_is_written_and_read(
        pool: sqlx::SqlitePool,
    ) {
        sqlx::raw_sql(
            r#"CREATE TABLE "order" ("group" BLOB NOT NULL, line INTEGER NOT NULL,
                 "type" TEXT NOT NULL, "Total Due" INTEGER NOT NULL, note INTEGER,
                 at TEXT NOT NULL, "retired at" TEXT, PRIMARY KEY ("group", line))"#,
        )
        .execute(&pool)
        .await
        .unwrap();
        let mut conn = pool.acquire().await.unwrap();
        let order = Order {
            group: Uuid::now_v7(),
            line: 1,
            r#type: "retail".into(),
            total: 1 << 40,
            note: None,
            at: chrono::Utc::now(),
        };
        let written = overwrite_except(&mut conn, vec![order.clone()], &[])
            .await
            .unwrap();
        assert_eq!(written, [order.key()]);
        let newer = Order {
            total: 7,
            at: order.at + chrono::Duration::seconds(1),
            ..order.clone()
        };
        overwrite_except(&mut conn, vec![newer.clone()], &[])
            .await
            .unwrap();
        overwrite_except(&mut conn, vec![order.clone()], &[])
            .await
            .unwrap();
        assert_eq!(
            get::<Order>(&mut conn, &order.key()).await.unwrap(),
            Some(order.clone()),
            "an older version, taken"
        );
        let table = Table::of::<Order>();
        assert_eq!(
            parity(&mut conn, &[&table])
                .await
                .map_err(|e| e.to_string()),
            Ok(())
        );
        let retired = retire_by_keys::<Order>(&mut conn, &[order.key()], &[])
            .await
            .unwrap();
        assert_eq!(retired.deleted, 1);
    }
}
