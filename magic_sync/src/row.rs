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
//! The engine's own reads and writes of a row are generic over it and name no
//! column, so a column added to the struct is written and read without
//! touching them; none rewrites a `#[row(created)]` column: a row's creation
//! is its first write.

use crate::dialect::ident;
use crate::{Conn, Db, Error};
use sqlx::query_builder::Separated;
#[cfg(feature = "sqlite")]
use std::collections::HashSet;
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
    /// A value a replica holds and acts on, written there and held by the
    /// server (`mirror = "setting"`), which may wish its desired columns
    /// other values ([`Setting`]).
    Setting,
}

impl Mirror {
    /// Whether a replica writes the table's rows and they go up.
    pub(crate) fn goes_up(self) -> bool {
        matches!(self, Mirror::Record | Mirror::Both | Mirror::Setting)
    }

    /// Whether the server writes the table's rows and sends them down.
    pub(crate) fn sent_down(self) -> bool {
        matches!(self, Mirror::Catalog | Mirror::Both)
    }
}

/// Which wish a replica carries out: a setting's `apply = "…"`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Apply {
    /// Every wish, whatever the replica did to the setting before it
    /// arrived (`apply = "always"`): the last wish carried out wins over a
    /// change made on the replica before it, and one made after it wins
    /// until the next wish.
    Always,
    /// Only while the replica still holds the version the wish is based on
    /// (`apply = "if_unchanged"`): one based on a version it no longer holds
    /// is superseded, and the replica's value stands. The version is compared
    /// as the handler is handed the wish: a write the replica makes of the row
    /// while the handler runs is overwritten by the wished columns.
    IfUnchanged,
}

/// A table that is a setting (`mirror = "setting"`): a row the replica owns,
/// as a record, whose `#[row(desired)]` columns the server may wish other
/// values. Implemented by [`#[derive(Row)]`](crate::Row) for such a row,
/// and only by it.
pub trait Setting: Row + Clone + Sync {
    /// Which wish a replica carries out.
    #[doc(hidden)]
    const APPLY: Apply;

    /// Takes the desired columns of `from`, leaving the rest.
    #[doc(hidden)]
    fn take_desired(&mut self, from: Self);

    /// Moves the version past what it is ([`Version::raised`]).
    #[doc(hidden)]
    fn raise_version(&mut self);
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
    /// Every replica a relation of the host's pairs the row with. `view` is
    /// the relation as SQL names it, quoted; `row` its columns that hold the
    /// row's key, in key order; `replica` its column naming the replica.
    Routed {
        view: &'static str,
        row: &'static [&'static str],
        replica: &'static str,
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
    /// A column a wish of a setting sets.
    pub desired: bool,
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
    /// The columns the server's `ON CONFLICT` names when it files a `record`,
    /// and its table's `PRIMARY KEY`: the key, unless the table's unique
    /// indexes must hold more — a partitioned table's (or a TimescaleDB
    /// hypertable's) hold the partition column. The host never changes the
    /// columns beyond the key once the row is written: the engine does not
    /// check, and a report with another value is filed as another row while
    /// the first stays as it was. A replica's table is keyed by the key alone.
    const CONFLICT: &'static [&'static str] = Self::KEY;
    /// Where the table lives.
    const MIRROR: Mirror;

    /// The table as this backend names it in SQL, quoted:
    /// `"shop"."products"` on postgres, `"products"` on sqlite.
    #[doc(hidden)]
    const QUALIFIED: &'static str;
    #[doc(hidden)]
    const ROUTE: Route;
    /// The relation, as SQL, the rows the table's rule sends are read
    /// through on the server: a view that leaves out the rows no replica is
    /// sent for their own sake. A row it leaves out still travels while a
    /// row a replica holds points at it. `None`: the table itself, every row
    /// of it.
    #[doc(hidden)]
    const ACTIVE: Option<&'static str>;
    /// The relation, as SQL, a row is read from on the server when not the
    /// table: a view over the table, keyed as it is, that may join columns
    /// of other tables in and leave rows out — the rows it leaves out are
    /// not sent, as with [`Row::ACTIVE`]. The table is still what the row is
    /// noted, keyed and placed by. `None`: the table.
    #[doc(hidden)]
    const FROM: Option<&'static str>;
    /// The columns of the table, each the whole of a foreign key, whose
    /// parent's columns the row copies (through [`Row::FROM`]): a change of
    /// the parent is a change of every row pointing at it so.
    #[doc(hidden)]
    const DEPENDS: &'static [&'static str];
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

    /// Its `#[row(version)]` field's value, as [`Version::text`] writes it:
    /// `None` for a row with no version field of its own.
    #[doc(hidden)]
    fn version_text(&self) -> Option<String>;

    /// The row's key as the engine writes it where a key is text — the `key`
    /// of `plan::Engine::row_progress`, a key in the engine's tables and in a
    /// log: its parts joined by `|`, a uuid lowercase and hyphenated, an
    /// integer decimal.
    fn key_text(&self) -> String {
        key_text(&self.key())
    }
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

/// A type a `#[row(version)]` column can have: an integer or an instant — a
/// `NaiveDateTime` is read as UTC. A host's newtype around one implements it
/// the way its inner type does.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be a #[row(version)] column",
    label = "a version is an integer or a `chrono` instant",
    note = "a newtype around one of them implements `magic_sync::row::Version` as its inner type does"
)]
pub trait Version: Sized {
    /// What the column holds.
    const KIND: VersionKind;

    /// The version as text, the one form two versions of a setting are
    /// compared in: an integer decimal; an instant in UTC, RFC 3339, to the
    /// microsecond — what PostgreSQL keeps, so a version that went to the
    /// server and came back compares equal to the one it was.
    fn text(&self) -> String;

    /// The version after this one, for a write the engine makes: an integer
    /// one more, or itself at its type's maximum — a version that cannot
    /// move, so the replica refuses a wish then; an instant now, or a
    /// microsecond past this one when the clock is behind it.
    #[must_use]
    fn raised(&self) -> Self;
}

impl Version for i16 {
    const KIND: VersionKind = VersionKind::Integer;

    fn text(&self) -> String {
        self.to_string()
    }

    fn raised(&self) -> Self {
        self.saturating_add(1)
    }
}

impl Version for i32 {
    const KIND: VersionKind = VersionKind::Integer;

    fn text(&self) -> String {
        self.to_string()
    }

    fn raised(&self) -> Self {
        self.saturating_add(1)
    }
}

impl Version for i64 {
    const KIND: VersionKind = VersionKind::Integer;

    fn text(&self) -> String {
        self.to_string()
    }

    fn raised(&self) -> Self {
        self.saturating_add(1)
    }
}

/// `at` to the microsecond, and the instant after it a version takes.
fn raised_utc(at: chrono::DateTime<chrono::Utc>) -> chrono::DateTime<chrono::Utc> {
    use chrono::SubsecRound;
    let past = at.trunc_subsecs(6) + chrono::TimeDelta::microseconds(1);
    chrono::Utc::now().trunc_subsecs(6).max(past)
}

fn instant_text(at: chrono::DateTime<chrono::Utc>) -> String {
    use chrono::SubsecRound;
    at.trunc_subsecs(6)
        .to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
}

impl<Tz: chrono::TimeZone> Version for chrono::DateTime<Tz> {
    const KIND: VersionKind = VersionKind::Instant;

    fn text(&self) -> String {
        instant_text(self.with_timezone(&chrono::Utc))
    }

    fn raised(&self) -> Self {
        raised_utc(self.with_timezone(&chrono::Utc)).with_timezone(&self.timezone())
    }
}

impl Version for chrono::NaiveDateTime {
    const KIND: VersionKind = VersionKind::Instant;

    fn text(&self) -> String {
        instant_text(self.and_utc())
    }

    fn raised(&self) -> Self {
        raised_utc(self.and_utc()).naive_utc()
    }
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
            written.push(key_of_row(row, &kinds, 0)?);
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

/// A key read off a result row's columns from `first` on.
fn key_of_row(
    row: &<Db as sqlx::Database>::Row,
    kinds: &[Option<KeyKind>],
    first: usize,
) -> Result<Key, Error> {
    use sqlx::Row as _;
    kinds
        .iter()
        .enumerate()
        .map(|(i, kind)| {
            Ok(match kind {
                Some(KeyKind::Integer) => KeyPart::Int(row.try_get::<i64, _>(first + i)?),
                _ => KeyPart::Uuid(row.try_get::<Uuid, _>(first + i)?),
            })
        })
        .collect()
}

/// The columns this side's primary key is: [`Row::CONFLICT`] on the server,
/// the key on a replica.
fn conflict<R: Row>() -> &'static [&'static str] {
    if cfg!(feature = "postgres") {
        R::CONFLICT
    } else {
        R::KEY
    }
}

/// `ON CONFLICT (conflict) DO UPDATE SET` every written column `take` keeps but
/// the conflict columns and the row's `created` column; nothing to set is `DO NOTHING`.
fn update_clause_where<R: Row>(take: impl Fn(&str) -> bool) -> String {
    let conflict = conflict::<R>();
    let set: Vec<String> = written_names::<R>()
        .into_iter()
        .filter(|c| !conflict.contains(c) && Some(*c) != R::CREATED && take(c))
        .map(|c| format!("{0} = EXCLUDED.{0}", ident(c)))
        .collect();
    if set.is_empty() {
        return format!(" ON CONFLICT ({}) DO NOTHING", listed(conflict));
    }
    format!(
        " ON CONFLICT ({}) DO UPDATE SET {}",
        listed(conflict),
        set.join(", ")
    )
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

/// Where `R`'s rows are read from on this side: its `from` relation on the
/// server, when it has one, else its table.
fn read_from<R: Row>() -> &'static str {
    #[cfg(feature = "postgres")]
    if let Some(from) = R::FROM {
        return from;
    }
    R::QUALIFIED
}

/// Every row of `keys` the table holds, retired or not — what sync sends,
/// read from the table, or the relation the row is read `from`, rather than
/// as the replicas see it. A key with no row is left out.
///
/// # Errors
///
/// [`Error::Database`] when the database fails, or a row does not decode as
/// `R`.
pub(crate) async fn get_by_keys<R: Row>(exec: &mut Conn, keys: &[Key]) -> Result<Vec<R>, Error> {
    let names: Vec<&str> = declared::<R>().into_iter().map(|c| c.name).collect();
    let mut out = Vec::with_capacity(keys.len());
    for chunk in keys.chunks(keys_per_statement::<R>()) {
        let mut qb = sqlx::QueryBuilder::new(format!(
            "SELECT {} FROM {}",
            listed(&names),
            read_from::<R>()
        ));
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
    /// Kept, because a row that stays points at them, and retired.
    pub(crate) kept: u64,
    /// Still held as they were: the database refused to let them go (a
    /// trigger, a row of the server's that stays and points at them), or a
    /// row that stays points at them and the table has no retired column.
    /// With why.
    pub(crate) refused: Vec<(Key, String)>,
}

/// Lets go of the rows of `keys` on a replica, together. A row a row that
/// stays for the replica points at — one of its own, through any foreign
/// key, checked now whether it is deferred or not, and whatever it does on
/// delete: a cascade or a set-null would change the replica's own row; or a
/// row it retired, of `server`'s tables (each with its retired column) — is
/// kept with its retired column set, and then so are the rows that one
/// points at among them. The rest are deleted together, so rows that point
/// only at one another go; one the database refuses to let go — a trigger,
/// a row of the server's that stays and points at it — is answered apart,
/// so it cannot fail the rest, once those it waited for have gone. The rows
/// of `held` are kept retired whatever points at them, for what the replica
/// wrote of them has not reached the server yet. A transient failure is the
/// error.
#[cfg(feature = "sqlite")]
pub(crate) async fn retire_by_keys<R: Row>(
    exec: &mut Conn,
    keys: &[Key],
    held: &[Key],
    server: &[(&str, Option<&str>)],
) -> Result<Retired, Error> {
    let mut retired = Retired::default();
    if keys.is_empty() && held.is_empty() {
        return Ok(retired);
    }
    // A row of the server's keeps one only while it is retired: a live one
    // is let go of before it, or refuses the delete.
    let staying = |child: &str| match server.iter().find(|(t, _)| t.eq_ignore_ascii_case(child)) {
        None => Some(String::new()),
        Some((_, Some(column))) => Some(format!(" AND c.{} IS NOT NULL", ident(column))),
        Some((_, None)) => None,
    };
    keep::<R>(exec, held, true, &mut retired).await?;
    let (mut pinned, pointed) = pointed_at::<R>(exec, keys, staying).await?;
    // What stays keeps what it points at: a row asked to stay by another
    // table, or retired before and not let go of now, or kept so in turn.
    let asked: HashSet<&Key> = keys.iter().collect();
    pin(&mut pinned, &pointed, |(child, retired)| {
        retired && !asked.contains(child)
    });
    let kept: Vec<Key> = keys
        .iter()
        .filter(|key| pinned.contains(*key))
        .cloned()
        .collect();
    keep::<R>(exec, &kept, false, &mut retired).await?;
    let going: Vec<Key> = keys
        .iter()
        .filter(|key| !pinned.contains(*key))
        .cloned()
        .collect();
    delete_together::<R>(exec, going, "", &mut retired).await?;
    Ok(retired)
}

/// Lets go of the retired rows of `keys` that nothing which stays points
/// at — no row of any table, live or retired, but those of `keys`
/// themselves, which go with them — together, as [`retire_by_keys`]
/// deletes; the rest stay as they are, and so does a row of `keys` that is
/// live. Answers how many went.
#[cfg(feature = "sqlite")]
pub(crate) async fn collect_by_keys<R: Row>(exec: &mut Conn, keys: &[Key]) -> Result<u64, Error> {
    let (mut pinned, pointed) = pointed_at::<R>(exec, keys, |_| Some(String::new())).await?;
    let asked: HashSet<&Key> = keys.iter().collect();
    pin(&mut pinned, &pointed, |(child, _)| !asked.contains(child));
    let going: Vec<Key> = keys
        .iter()
        .filter(|key| !pinned.contains(*key))
        .cloned()
        .collect();
    let retired = R::RETIRED.map_or(" AND false".to_owned(), |column| {
        format!(" AND {} IS NOT NULL", ident(column))
    });
    let mut gone = Retired::default();
    delete_together::<R>(exec, going, &retired, &mut gone).await?;
    Ok(gone.deleted)
}

/// A row of the table pointing at one of it: its key, whether it is
/// retired, and the key of the row it points at.
#[cfg(feature = "sqlite")]
type Pointed = (Key, bool, Key);

/// Of `keys`, the rows a row of another table points at — of a table
/// `staying` names the filter of rows that keep one by, `None` for a table
/// whose rows keep none — and every row of the table itself that points at
/// one of them ([`Pointed`]).
#[cfg(feature = "sqlite")]
async fn pointed_at<R: Row>(
    exec: &mut Conn,
    keys: &[Key],
    staying: impl Fn(&str) -> Option<String>,
) -> Result<(HashSet<Key>, Vec<Pointed>), Error> {
    let mut pointers = Vec::new();
    // The table's own keys into itself, whose rows let go of together keep
    // one another only while one of them stays.
    let mut inward = Vec::new();
    for crate::dialect::Pointer {
        child, from, to, ..
    } in crate::dialect::pointing(exec, R::TABLE, R::KEY).await?
    {
        if child.eq_ignore_ascii_case(R::TABLE) {
            inward.push((from, to));
            continue;
        }
        let Some(stays) = staying(&child) else {
            continue;
        };
        // The parent's column first: compared by its collation, as the key
        // compares, and through the child's index.
        let on: Vec<String> = from
            .iter()
            .zip(&to)
            .map(|(f, t)| format!("{}.{} = c.{}", R::QUALIFIED, ident(t), ident(f)))
            .collect();
        pointers.push(format!(
            "EXISTS (SELECT 1 FROM {} c WHERE {}{stays})",
            ident(&child),
            on.join(" AND ")
        ));
    }
    let kinds = key_kinds::<R>();
    let mut pinned = HashSet::new();
    let mut pointed = Vec::new();
    for chunk in keys.chunks(keys_per_statement::<R>()) {
        if !pointers.is_empty() {
            let mut qb =
                sqlx::QueryBuilder::new(format!("SELECT {} FROM {}", listed(R::KEY), R::QUALIFIED));
            key_filter::<R>(&mut qb, chunk);
            qb.push(format!(" AND ({})", pointers.join(" OR ")));
            for row in qb.build().fetch_all(&mut *exec).await? {
                pinned.insert(key_of_row(&row, &kinds, 0)?);
            }
        }
        for (from, to) in &inward {
            let column = |alias: &str, names: &[&str]| {
                names
                    .iter()
                    .map(|n| format!("{alias}.{}", ident(n)))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            let retired_column =
                R::RETIRED.map_or("NULL".to_owned(), |r| format!("c.{}", ident(r)));
            let on: Vec<String> = from
                .iter()
                .zip(to)
                .map(|(f, t)| format!("p.{} = c.{}", ident(t), ident(f)))
                .collect();
            let mut qb = sqlx::QueryBuilder::new(format!(
                "SELECT {}, {retired_column} IS NOT NULL, {} FROM {} c JOIN (SELECT * FROM {}",
                column("c", R::KEY),
                column("p", R::KEY),
                R::QUALIFIED,
                R::QUALIFIED
            ));
            key_filter::<R>(&mut qb, chunk);
            qb.push(format!(") p ON {}", on.join(" AND ")));
            for row in qb.build().fetch_all(&mut *exec).await? {
                use sqlx::Row as _;
                let retired_child: bool = row.try_get(R::KEY.len())?;
                pointed.push((
                    key_of_row(&row, &kinds, 0)?,
                    retired_child,
                    key_of_row(&row, &kinds, R::KEY.len() + 1)?,
                ));
            }
        }
    }
    Ok((pinned, pointed))
}

/// Pins, in turn, what a pinned row points at, and what a row `stays` (its
/// key, whether it is retired) points at.
#[cfg(feature = "sqlite")]
fn pin(pinned: &mut HashSet<Key>, pointed: &[Pointed], stays: impl Fn((&Key, bool)) -> bool) {
    loop {
        let more: Vec<Key> = pointed
            .iter()
            .filter(|(child, retired, parent)| {
                !pinned.contains(parent) && (pinned.contains(child) || stays((child, *retired)))
            })
            .map(|(_, _, parent)| parent.clone())
            .collect();
        if more.is_empty() {
            return;
        }
        pinned.extend(more);
    }
}

/// Deletes the rows of `going` the SQL condition `only` admits (empty:
/// every one) together, so rows that point only at one another go; one the
/// database refuses is tried again once those it waited for have gone, and
/// answered in `retired` when that changes nothing.
#[cfg(feature = "sqlite")]
async fn delete_together<R: Row>(
    exec: &mut Conn,
    mut going: Vec<Key>,
    only: &str,
    retired: &mut Retired,
) -> Result<(), Error> {
    loop {
        let before = retired.deleted;
        let mut refused = Vec::new();
        for chunk in going.chunks(keys_per_statement::<R>()) {
            let delete = |keys: &[Key]| {
                let mut qb = sqlx::QueryBuilder::new(format!("DELETE FROM {}", R::QUALIFIED));
                key_filter::<R>(&mut qb, keys);
                qb.push(only);
                qb
            };
            refused.extend(one_by_one(exec, chunk, delete, &mut retired.deleted).await?);
        }
        if refused.is_empty() || retired.deleted == before {
            retired.refused.extend(refused);
            return Ok(());
        }
        going = refused.into_iter().map(|(key, _)| key).collect();
    }
}

/// Keeps the rows of `keys` retired, or — where the table has no retired
/// column — refuses to let them go, saying why: what the replica wrote of
/// them has not reached the server (`unfiled`), or a row that stays points
/// at them. Answers how many it retired that were not already.
#[cfg(feature = "sqlite")]
async fn keep<R: Row>(
    exec: &mut Conn,
    keys: &[Key],
    unfiled: bool,
    retired: &mut Retired,
) -> Result<u64, Error> {
    let Some(column) = R::RETIRED else {
        let why = if unfiled {
            format!(
                "what this replica wrote of it has not reached the server, and `{}` has no \
                 retired column to keep it by",
                R::TABLE
            )
        } else {
            format!(
                "a row here points at it, and `{}` has no retired column to keep it by",
                R::TABLE
            )
        };
        let kept = key_select::<R>(exec, keys).await?;
        retired
            .refused
            .extend(kept.into_iter().map(|key| (key, why.clone())));
        return Ok(0);
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
    let before = retired.kept;
    for chunk in keys.chunks(keys_per_statement::<R>()) {
        let refused = one_by_one(exec, chunk, retire, &mut retired.kept).await?;
        retired.refused.extend(refused);
    }
    Ok(retired.kept - before)
}

/// Which row of a table each row pointing at it through one foreign key
/// points at — by the key's own comparison — as of when it was recorded,
/// kept in a temporary table for [`broken`] to read once a message changed
/// the table. A table without rowids is not recorded: what its rows point at
/// is not found.
#[cfg(feature = "sqlite")]
pub(crate) struct Pointing {
    temp: String,
    child: String,
    id: i64,
    from: Vec<String>,
}

/// What every foreign key into the table points at ([`Pointing`]), in
/// temporary tables named from `name` on — none when the connection
/// enforces no foreign key. [`forget`] drops them.
#[cfg(feature = "sqlite")]
pub(crate) async fn pointing<R: Row>(exec: &mut Conn, name: usize) -> Result<Vec<Pointing>, Error> {
    let enforced: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(&mut *exec)
        .await?;
    if enforced == 0 {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for pointer in crate::dialect::pointing(exec, R::TABLE, R::KEY).await? {
        if !crate::dialect::has_rowid(exec, &pointer.child).await? {
            continue;
        }
        let temp = format!("sync_pointing_{}", name + out.len());
        let from: Vec<String> = pointer
            .from
            .iter()
            .enumerate()
            .map(|(n, f)| format!("c.{} AS sync_from_{n}", ident(f)))
            .collect();
        let keys: Vec<String> = R::KEY
            .iter()
            .enumerate()
            .map(|(n, k)| format!("p.{} AS sync_key_{n}", ident(k)))
            .collect();
        sqlx::query(&format!(
            "CREATE TEMP TABLE {temp} AS SELECT c.rowid AS sync_child, {}, {} FROM {} c \
               JOIN {} p ON {}",
            from.join(", "),
            keys.join(", "),
            ident(&pointer.child),
            R::QUALIFIED,
            crate::dialect::points("p", &pointer.to, "c", &pointer.from)
        ))
        .execute(&mut *exec)
        .await?;
        out.push(Pointing {
            temp,
            child: pointer.child,
            id: pointer.id,
            from: pointer.from,
        });
    }
    Ok(out)
}

/// The keys of the rows `pointing` recorded that rows still point at — by
/// the same values — through a key the database finds broken: what a key
/// checked at commit refuses, the table having changed or let go of them.
#[cfg(feature = "sqlite")]
pub(crate) async fn broken<R: Row>(
    exec: &mut Conn,
    pointing: &[Pointing],
) -> Result<Vec<Key>, Error> {
    let kinds = key_kinds::<R>();
    let keys: Vec<String> = (0..R::KEY.len())
        .map(|n| format!("w.sync_key_{n}"))
        .collect();
    let mut out = Vec::new();
    for p in pointing {
        let same: Vec<String> = p
            .from
            .iter()
            .enumerate()
            .map(|(n, f)| format!("w.sync_from_{n} IS c.{}", ident(f)))
            .collect();
        let rows = sqlx::query(&format!(
            "SELECT DISTINCT {} FROM {} w JOIN {} c ON c.rowid = w.sync_child \
              WHERE {} AND w.sync_child IN \
                (SELECT rowid FROM pragma_foreign_key_check($1) WHERE fkid = $2)",
            keys.join(", "),
            p.temp,
            ident(&p.child),
            same.join(" AND ")
        ))
        .bind(&p.child)
        .bind(p.id)
        .fetch_all(&mut *exec)
        .await?;
        for row in &rows {
            out.push(key_of_row(row, &kinds, 0)?);
        }
    }
    Ok(out)
}

/// Drops the temporary tables of `pointing`.
#[cfg(feature = "sqlite")]
pub(crate) async fn forget(exec: &mut Conn, pointing: &[Pointing]) -> Result<(), Error> {
    for p in pointing {
        sqlx::query(&format!("DROP TABLE temp.{}", p.temp))
            .execute(&mut *exec)
            .await?;
    }
    Ok(())
}

/// The keys of `keys` whose row the database finds pointing, through a
/// foreign key, at a row it does not hold — none of a table without rowids.
#[cfg(feature = "sqlite")]
pub(crate) async fn orphaned<R: Row>(exec: &mut Conn, keys: &[Key]) -> Result<Vec<Key>, Error> {
    if !crate::dialect::has_rowid(exec, R::TABLE).await? {
        return Ok(Vec::new());
    }
    let kinds = key_kinds::<R>();
    let mut out = Vec::new();
    for chunk in keys.chunks(keys_per_statement::<R>()) {
        let mut qb =
            sqlx::QueryBuilder::new(format!("SELECT {} FROM {}", listed(R::KEY), R::QUALIFIED));
        key_filter::<R>(&mut qb, chunk);
        qb.push(format!(
            " AND rowid IN (SELECT rowid FROM pragma_foreign_key_check({}))",
            crate::dialect::literal(R::TABLE)
        ));
        for row in qb.build().fetch_all(&mut *exec).await? {
            out.push(key_of_row(&row, &kinds, 0)?);
        }
    }
    Ok(out)
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
            let present: Vec<String> = from
                .iter()
                .map(|f| format!("{}.{} IS NOT NULL", R::QUALIFIED, ident(f)))
                .collect();
            format!(
                "({} AND NOT EXISTS (SELECT 1 FROM {} p WHERE {}))",
                present.join(" AND "),
                ident(&parent),
                crate::dialect::points("p", &to, R::QUALIFIED, &from)
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
            orphans.push(key_of_row(&row, &kinds, 0)?);
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

/// The keys of `keys` the table holds, retired or not.
#[cfg(feature = "sqlite")]
pub(crate) async fn key_select<R: Row>(exec: &mut Conn, keys: &[Key]) -> Result<Vec<Key>, Error> {
    let kinds = key_kinds::<R>();
    let mut out = Vec::new();
    for chunk in keys.chunks(keys_per_statement::<R>()) {
        let mut qb =
            sqlx::QueryBuilder::new(format!("SELECT {} FROM {}", listed(R::KEY), R::QUALIFIED));
        key_filter::<R>(&mut qb, chunk);
        for row in qb.build().fetch_all(&mut *exec).await? {
            out.push(key_of_row(&row, &kinds, 0)?);
        }
    }
    Ok(out)
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

/// Every key the table holds retired: none where it has nothing to retire
/// rows by.
#[cfg(feature = "sqlite")]
pub(crate) async fn retired_keys<R: Row>(exec: &mut Conn) -> Result<Vec<Key>, Error> {
    let Some(retired) = R::RETIRED else {
        return Ok(Vec::new());
    };
    keys_where::<R>(exec, &format!(" WHERE {} IS NOT NULL", ident(retired))).await
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
            key_of_row(row, &kinds, 0)
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

/// `columns` — the ones replicas own — of every row the table has and
/// `sender` is recorded holding, taken from `rows`; nothing else. A row
/// written on both sides begins on the server: an edit of one it does not
/// have is never a row, and one `sender` is not recorded holding is no edit
/// of its own yet. Answers the keys it wrote.
#[cfg(feature = "postgres")]
pub(crate) async fn update_held<R: Row>(
    exec: &mut Conn,
    rows: Vec<R>,
    columns: &[&str],
    sender: Uuid,
) -> Result<Vec<Key>, Error> {
    let names = written_names::<R>();
    let set: Vec<String> = names
        .iter()
        .filter(|c| columns.contains(c) && !R::KEY.contains(c))
        .map(|c| format!("{0} = v.{0}", ident(c)))
        .collect();
    if rows.is_empty() || set.is_empty() {
        return Ok(Vec::new());
    }
    let mut seen = std::collections::HashSet::new();
    let mut unique: Vec<R> = rows
        .into_iter()
        .rev()
        .filter(|row| seen.insert(row.key()))
        .collect();
    unique.reverse();
    let matched: Vec<String> = R::KEY
        .iter()
        .map(|k| format!("t.{0} = v.{0}", ident(k)))
        .collect();
    let key_text: Vec<String> = R::KEY
        .iter()
        .map(|k| format!("t.{}::text", ident(k)))
        .collect();
    let returned: Vec<String> = R::KEY.iter().map(|k| format!("t.{}", ident(k))).collect();
    // Every row's columns, and the sender.
    let per_statement = ((crate::dialect::MAX_BINDS - 1) / names.len().max(1)).max(1);
    let kinds = key_kinds::<R>();
    let mut written = Vec::new();
    let mut rest = unique.into_iter().peekable();
    while rest.peek().is_some() {
        let chunk: Vec<R> = rest.by_ref().take(per_statement).collect();
        let mut qb = sqlx::QueryBuilder::new(format!(
            "UPDATE {} AS t SET {} FROM (",
            R::QUALIFIED,
            set.join(", ")
        ));
        qb.push_values(chunk, |mut sep, row| row.push_binds(&mut sep));
        qb.push(format!(
            ") AS v ({}) WHERE {} AND EXISTS (SELECT 1 FROM sync.held h WHERE h.replica_id = ",
            listed(&names),
            matched.join(" AND ")
        ));
        qb.push_bind(sender);
        qb.push(format!(
            " AND h.relation = {} AND h.row_key = concat_ws('|', {})) RETURNING {}",
            crate::dialect::literal(R::TABLE),
            key_text.join(", "),
            returned.join(", ")
        ));
        for row in &qb.build().fetch_all(&mut *exec).await? {
            written.push(key_of_row(row, &kinds, 0)?);
        }
    }
    Ok(written)
}

/// Writes every column of every row, the incoming value winning on a key the
/// table holds — unless the row has a version and the stored one is newer,
/// so a late write never rolls the row back. Answers the keys it wrote.
///
/// # Errors
///
/// [`Error::Database`] when the database fails or refuses a row.
#[cfg(feature = "postgres")]
pub(crate) async fn upsert<R: Row>(exec: &mut Conn, rows: Vec<R>) -> Result<Vec<Key>, Error> {
    write_keys(exec, rows, &newest::<R>(update_clause_where::<R>(|_| true))).await
}

/// `row` as PostgreSQL writes it as JSON (`to_jsonb`): its written columns
/// by name, each value as its column's type writes it — the row in the
/// table's own shape, which [`wanted`] reads back.
///
/// # Errors
///
/// [`Error::Database`] when the database fails or refuses a value.
#[cfg(feature = "postgres")]
pub(crate) async fn as_json<R: Row>(exec: &mut Conn, row: R) -> Result<serde_json::Value, Error> {
    let names = written_names::<R>();
    let mut qb = sqlx::QueryBuilder::new("SELECT to_jsonb(v) FROM (VALUES (");
    let mut sep = qb.separated(", ");
    row.push_binds(&mut sep);
    qb.push(format!(")) AS v({})", listed(&names)));
    Ok(qb.build_query_scalar().fetch_one(&mut *exec).await?)
}

/// The row a wish of `key` (as its text) wants: the row as the table holds
/// it now — or, when it holds none, as `whole` (from [`as_json`]) is — with
/// `whole`'s desired, key and version columns laid over it. Whatever
/// columns the table has gained since `whole` was written come from its own
/// row.
///
/// # Errors
///
/// [`Error::Malformed`] for a key that is not the table's;
/// [`Error::Database`] when the database fails, or the row does not decode
/// as `R` — one the table does not hold, and a column it gained since
/// `whole` was written is NULL in.
#[cfg(feature = "postgres")]
pub(crate) async fn wanted<R: Row>(
    exec: &mut Conn,
    key: &str,
    whole: &serde_json::Value,
) -> Result<R, Error> {
    let table = Table::of::<R>();
    let key_columns: Vec<Column> = table
        .key
        .iter()
        .filter_map(|k| table.columns.iter().find(|c| c.name == *k).copied())
        .collect();
    let key = crate::plan::parse_key(&key_columns, key)?;
    let laid: serde_json::Map<String, serde_json::Value> = whole
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(name, _)| {
            table.columns.iter().any(|c| {
                c.name == name.as_str() && (c.desired || c.key.is_some() || c.version.is_some())
            })
        })
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();
    let names: Vec<&str> = declared::<R>().into_iter().map(|c| c.name).collect();
    let mut qb = sqlx::QueryBuilder::new(format!(
        "SELECT {} FROM (SELECT (jsonb_populate_record(coalesce(\
           (SELECT t FROM {} t",
        listed(&names),
        R::QUALIFIED
    ));
    key_filter::<R>(&mut qb, std::slice::from_ref(&key));
    qb.push(format!("), jsonb_populate_record(NULL::{}, ", R::QUALIFIED));
    qb.push_bind(whole.clone());
    qb.push(")), ");
    qb.push_bind(serde_json::Value::Object(laid));
    qb.push(")).*) w");
    Ok(qb.build_query_as::<R>().fetch_one(&mut *exec).await?)
}

/// Every row of the table — of the replica `of`, by the table's origin,
/// when given — as `key \t version` lines: what a digest of a setting
/// fingerprints.
///
/// # Errors
///
/// [`Error::Database`] when the database fails, or a row does not decode.
pub(crate) async fn versions<R: Row>(
    exec: &mut Conn,
    of: Option<Uuid>,
) -> Result<Vec<String>, Error> {
    let names: Vec<&str> = declared::<R>().into_iter().map(|c| c.name).collect();
    let mut qb =
        sqlx::QueryBuilder::new(format!("SELECT {} FROM {}", listed(&names), R::QUALIFIED));
    if let (Some(replica), Some(origin)) = (of, R::ORIGIN) {
        qb.push(format!(" WHERE {} = ", ident(origin)));
        qb.push_bind(replica);
    }
    Ok(qb
        .build_query_as::<R>()
        .fetch_all(&mut *exec)
        .await?
        .iter()
        .map(|row| {
            format!(
                "{}\t{}",
                row.key_text(),
                row.version_text().unwrap_or_default()
            )
        })
        .collect())
}

/// Inserts every row, or overwrites the one the table holds, whatever its
/// version. Answers the keys it wrote.
///
/// # Errors
///
/// [`Error::Database`] when the database fails or refuses a row.
#[cfg(feature = "postgres")]
pub(crate) async fn overwrite<R: Row>(exec: &mut Conn, rows: Vec<R>) -> Result<Vec<Key>, Error> {
    write_keys(exec, rows, &update_clause_where::<R>(|_| true)).await
}

/// Inserts every row whose key is new; one the table holds is left as it
/// was. Answers the keys it inserted.
///
/// # Errors
///
/// [`Error::Database`] when the database fails or refuses a row.
#[cfg(feature = "postgres")]
pub(crate) async fn insert_ignore<R: Row>(
    exec: &mut Conn,
    rows: Vec<R>,
) -> Result<Vec<Key>, Error> {
    let conflict = format!(" ON CONFLICT ({}) DO NOTHING", listed(conflict::<R>()));
    write_keys(exec, rows, &conflict).await
}

/// A conflict update that applies only when the incoming version is not older
/// than the stored one, for a row that has one.
#[cfg(feature = "postgres")]
fn newest<R: Row>(mut conflict: String) -> String {
    let Some(version) = R::VERSION.filter(|_| conflict.contains("DO UPDATE")) else {
        return conflict;
    };
    conflict.push_str(" WHERE ");
    conflict.push_str(&crate::dialect::not_older(
        &format!("EXCLUDED.{}", ident(version)),
        &format!("{}.{}", R::QUALIFIED, ident(version)),
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
    /// [`Row::CONFLICT`]: what the server's table's primary key must be.
    pub(crate) conflict: &'static [&'static str],
    pub(crate) mirror: Mirror,
    #[cfg(feature = "postgres")]
    pub(crate) route: Route,
    /// [`Row::ACTIVE`].
    #[cfg(feature = "postgres")]
    pub(crate) active: Option<&'static str>,
    /// [`Row::FROM`].
    #[cfg(feature = "postgres")]
    pub(crate) from: Option<&'static str>,
    /// [`Row::DEPENDS`].
    #[cfg(feature = "postgres")]
    pub(crate) depends: &'static [&'static str],
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
            conflict: R::CONFLICT,
            mirror: R::MIRROR,
            #[cfg(feature = "postgres")]
            route: R::ROUTE,
            #[cfg(feature = "postgres")]
            active: R::ACTIVE,
            #[cfg(feature = "postgres")]
            from: R::FROM,
            #[cfg(feature = "postgres")]
            depends: R::DEPENDS,
            columns: declared::<R>(),
            retired: R::RETIRED,
            unmirrored: R::UNMIRRORED,
            origin: R::ORIGIN,
            #[cfg(feature = "postgres")]
            encoding: "",
        }
    }

    /// The relation its rows are read from on this side when not the table:
    /// its `from`, on the server.
    fn read_from(&self) -> Option<&'static str> {
        #[cfg(feature = "postgres")]
        return self.from;
        #[cfg(not(feature = "postgres"))]
        None
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
    /// does not decode, and a primary key of other columns than the row's
    /// — its conflict columns on the server, its key on a replica. The columns
    /// the row leaves out — its retired column, its unmirrored ones — are
    /// not compared. Empty when they agree.
    pub(crate) async fn differences(&self, conn: &mut Conn) -> Result<Vec<String>, Error> {
        let mut out = self.contradictions();
        // A row read from a relation of its own is that relation's, column
        // for column — a view's columns all read as nullable, and none is
        // written, so only which columns there are and their types are
        // compared — and the table's key.
        let from = self.read_from();
        let actual = match from {
            #[cfg(feature = "postgres")]
            Some(from) => crate::dialect::relation_columns(conn, from).await?,
            _ => crate::dialect::columns(conn, self.schema, self.name).await?,
        };
        if actual.is_empty() {
            out.push(match from {
                Some(from) => format!(
                    "the relation it is read from, {from}, is not on {}",
                    crate::dialect::NAME
                ),
                None => format!("the table is not on {}", crate::dialect::NAME),
            });
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
            if from.is_some() {
                continue;
            }
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
                    "the field `{}` is no column of {}",
                    column.name,
                    from.map_or("the table", |_| "the relation it is read from")
                ));
            }
        }
        let mut primary_key = crate::dialect::primary_key(conn, self.schema, self.name).await?;
        let by_conflict = cfg!(feature = "postgres") && self.conflict != self.key;
        let mut key: Vec<&str> = if by_conflict {
            self.conflict.to_vec()
        } else {
            self.key.to_vec()
        };
        // In any order: a key is compared, written and bound column by
        // column, by name.
        primary_key.sort_unstable();
        key.sort_unstable();
        if primary_key != key {
            let what = if by_conflict {
                "conflict columns"
            } else {
                "key"
            };
            out.push(format!(
                "the primary key is {primary_key:?}, and the row's {what} {key:?}"
            ));
        }
        let described = sqlx::Executor::describe(
            &mut *conn,
            &format!("SELECT * FROM {}", from.unwrap_or(self.qualified)),
        )
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
        at: chrono::DateTime<chrono::Utc>,
    }

    #[derive(sqlx::FromRow, Clone, Debug, PartialEq, crate::Columns)]
    struct Label {
        label: String,
    }

    /// A record whose reports are ordered by an instant.
    #[derive(sqlx::FromRow, Clone, Debug, PartialEq, crate::Row)]
    #[row(table = "reports", mirror = "record")]
    struct Report {
        id: Uuid,
        #[row(version)]
        at: chrono::DateTime<chrono::Utc>,
    }

    #[test]
    fn a_rows_key_text_joins_its_parts_in_their_one_form() {
        let group = Uuid::parse_str("0192F6A0-0000-7000-8000-00000000000A").unwrap();
        let order = Order {
            group,
            line: -3,
            r#type: String::new(),
            total: 0,
            note: None,
            at: chrono::Utc::now(),
        };
        assert_eq!(order.key_text(), "0192f6a0-0000-7000-8000-00000000000a|-3");
    }

    #[test]
    fn a_records_version_column_is_read_off_its_type() {
        let table = Table::of::<Report>();
        let column = |name: &str| *table.columns.iter().find(|c| c.name == name).unwrap();
        assert_eq!(column("at").version, Some(VersionKind::Instant));
        assert_eq!(column("id").version, None);
        assert_eq!(Report::VERSION, Some("at"));
    }

    #[cfg(feature = "postgres")]
    #[sqlx::test(migrations = false)]
    async fn a_record_is_upserted_newest_wins_and_inserted_first_wins(pool: sqlx::PgPool) {
        sqlx::raw_sql("CREATE TABLE reports (id uuid PRIMARY KEY, at timestamptz NOT NULL)")
            .execute(&pool)
            .await
            .unwrap();
        let mut conn = pool.acquire().await.unwrap();
        let first = Report {
            id: Uuid::now_v7(),
            at: chrono::DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap(),
        };
        let older = Report {
            at: first.at - chrono::Duration::seconds(1),
            ..first.clone()
        };
        let newer = Report {
            at: first.at + chrono::Duration::seconds(1),
            ..first.clone()
        };
        async fn by_key(conn: &mut Conn, report: &Report) -> Vec<Report> {
            get_by_keys::<Report>(conn, &[report.key()]).await.unwrap()
        }
        upsert(&mut conn, vec![first.clone()]).await.unwrap();
        upsert(&mut conn, vec![older.clone()]).await.unwrap();
        assert_eq!(
            by_key(&mut conn, &first).await,
            vec![first.clone()],
            "a late report"
        );
        upsert(&mut conn, vec![newer.clone()]).await.unwrap();
        assert_eq!(by_key(&mut conn, &first).await, vec![newer.clone()]);
        let other = Report {
            id: Uuid::now_v7(),
            ..first.clone()
        };
        let inserted = insert_ignore(&mut conn, vec![older, other.clone()])
            .await
            .unwrap();
        assert_eq!(inserted, [other.key()], "the one it held left as it was");
        assert_eq!(by_key(&mut conn, &first).await, [newer]);
    }

    /// A record filed into a table partitioned by when it was taken, whose
    /// unique index holds that column beside the key.
    #[derive(sqlx::FromRow, Clone, Debug, PartialEq, crate::Row)]
    #[row(table = "readings", mirror = "record", conflict = "id, at")]
    struct Reading {
        id: Uuid,
        at: chrono::DateTime<chrono::Utc>,
        value: i64,
    }

    #[cfg(feature = "postgres")]
    #[sqlx::test(migrations = false)]
    async fn a_record_is_filed_into_a_partitioned_table_by_its_conflict_columns(
        pool: sqlx::PgPool,
    ) {
        sqlx::raw_sql(
            "CREATE TABLE readings (id uuid NOT NULL, at timestamptz NOT NULL, value bigint NOT NULL, \
                                    PRIMARY KEY (id, at)) PARTITION BY RANGE (at); \
             CREATE TABLE readings_all PARTITION OF readings DEFAULT",
        )
        .execute(&pool)
        .await
        .unwrap();
        let mut conn = pool.acquire().await.unwrap();
        parity(&mut conn, &[&Table::of::<Reading>()])
            .await
            .expect("its primary key is its conflict columns");
        let first = Reading {
            id: Uuid::now_v7(),
            at: chrono::DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap(),
            value: 1,
        };
        let again = Reading {
            value: 2,
            ..first.clone()
        };
        let other = Reading {
            id: Uuid::now_v7(),
            ..first.clone()
        };
        let inserted = insert_ignore(&mut conn, vec![first.clone(), other.clone()])
            .await
            .unwrap();
        assert_eq!(inserted, [first.key(), other.key()]);
        let inserted = insert_ignore(&mut conn, vec![again.clone()]).await.unwrap();
        assert!(inserted.is_empty(), "the one it held left as it was");
        assert_eq!(
            get_by_keys::<Reading>(&mut conn, &[first.key()])
                .await
                .unwrap(),
            [first]
        );
        overwrite(&mut conn, vec![again.clone()]).await.unwrap();
        assert_eq!(
            get_by_keys::<Reading>(&mut conn, &[again.key()])
                .await
                .unwrap(),
            [again]
        );
    }

    #[cfg(feature = "postgres")]
    #[sqlx::test(migrations = false)]
    async fn a_server_table_keyed_by_the_key_alone_is_refused(pool: sqlx::PgPool) {
        sqlx::raw_sql(
            "CREATE TABLE readings (id uuid PRIMARY KEY, at timestamptz NOT NULL, value bigint NOT NULL)",
        )
        .execute(&pool)
        .await
        .unwrap();
        let mut conn = pool.acquire().await.unwrap();
        let refused = parity(&mut conn, &[&Table::of::<Reading>()])
            .await
            .expect_err("its primary key is not its conflict columns");
        assert!(
            refused.to_string().contains(
                r#"the primary key is ["id"], and the row's conflict columns ["at", "id"]"#
            ),
            "{refused}"
        );
    }

    /// The engine trusts the host to never change a conflict column: a report
    /// with another value is another row, and the first stays.
    #[cfg(feature = "postgres")]
    #[sqlx::test(migrations = false)]
    async fn a_record_reported_with_another_conflict_value_is_another_row(pool: sqlx::PgPool) {
        sqlx::raw_sql(
            "CREATE TABLE readings (id uuid NOT NULL, at timestamptz NOT NULL, value bigint NOT NULL, \
                                    PRIMARY KEY (id, at))",
        )
        .execute(&pool)
        .await
        .unwrap();
        let mut conn = pool.acquire().await.unwrap();
        let first = Reading {
            id: Uuid::now_v7(),
            at: chrono::DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap(),
            value: 1,
        };
        let moved = Reading {
            at: first.at + chrono::Duration::seconds(1),
            ..first.clone()
        };
        insert_ignore(&mut conn, vec![first.clone()]).await.unwrap();
        assert_eq!(
            insert_ignore(&mut conn, vec![moved]).await.unwrap(),
            [first.key()]
        );
        assert_eq!(
            get_by_keys::<Reading>(&mut conn, &[first.key()])
                .await
                .unwrap()
                .len(),
            2
        );
    }

    /// A record of the same table whose later reports win.
    #[derive(sqlx::FromRow, Clone, Debug, PartialEq, crate::Row)]
    #[row(table = "readings", mirror = "record", conflict = "id, at")]
    struct Revised {
        id: Uuid,
        at: chrono::DateTime<chrono::Utc>,
        value: i64,
        #[row(version)]
        revision: i64,
    }

    #[cfg(feature = "postgres")]
    #[sqlx::test(migrations = false)]
    async fn a_versioned_record_is_upserted_by_its_conflict_columns(pool: sqlx::PgPool) {
        sqlx::raw_sql(
            "CREATE TABLE readings (id uuid NOT NULL, at timestamptz NOT NULL, value bigint NOT NULL, \
                                    revision bigint NOT NULL, PRIMARY KEY (id, at)) PARTITION BY RANGE (at); \
             CREATE TABLE readings_all PARTITION OF readings DEFAULT",
        )
        .execute(&pool)
        .await
        .unwrap();
        let mut conn = pool.acquire().await.unwrap();
        let first = Revised {
            id: Uuid::now_v7(),
            at: chrono::DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap(),
            value: 1,
            revision: 2,
        };
        let older = Revised {
            value: 0,
            revision: 1,
            ..first.clone()
        };
        let newer = Revised {
            value: 3,
            revision: 3,
            ..first.clone()
        };
        upsert(&mut conn, vec![first.clone()]).await.unwrap();
        upsert(&mut conn, vec![older]).await.unwrap();
        assert_eq!(
            get_by_keys::<Revised>(&mut conn, &[first.key()])
                .await
                .unwrap(),
            [first],
            "a late report"
        );
        upsert(&mut conn, vec![newer.clone()]).await.unwrap();
        assert_eq!(
            get_by_keys::<Revised>(&mut conn, &[newer.key()])
                .await
                .unwrap(),
            [newer]
        );
    }

    /// A replica's table is keyed by the key alone, whatever the server's
    /// primary key holds.
    #[cfg(feature = "sqlite")]
    #[sqlx::test(migrations = false)]
    async fn a_replica_keys_a_record_with_conflict_columns_by_its_key(pool: sqlx::SqlitePool) {
        sqlx::raw_sql(
            "CREATE TABLE readings (id BLOB NOT NULL PRIMARY KEY, at TEXT NOT NULL, value INTEGER NOT NULL)",
        )
        .execute(&pool)
        .await
        .unwrap();
        let mut conn = pool.acquire().await.unwrap();
        parity(&mut conn, &[&Table::of::<Reading>()])
            .await
            .expect("keyed by its key");
    }

    /// A setting: the replica's, whose level the server may wish.
    #[derive(sqlx::FromRow, Clone, Debug, PartialEq, crate::Row)]
    #[row(
        table = "screens",
        mirror = "setting",
        key = "replica_id, id",
        origin = "replica_id",
        apply = "if_unchanged"
    )]
    struct Screen {
        replica_id: Uuid,
        id: Uuid,
        name: String,
        #[row(desired)]
        level: i64,
        #[row(version)]
        at: chrono::DateTime<chrono::Utc>,
    }

    #[test]
    fn a_setting_takes_only_its_desired_columns_and_moves_its_version() {
        let at = chrono::DateTime::from_timestamp_micros(1_700_000_000_000_001).unwrap();
        let held = Screen {
            replica_id: Uuid::now_v7(),
            id: Uuid::now_v7(),
            name: "front".into(),
            level: 40,
            at,
        };
        let wanted = Screen {
            replica_id: Uuid::nil(),
            id: Uuid::nil(),
            name: "anything".into(),
            level: 70,
            at: at - chrono::Duration::days(1),
        };
        assert_eq!(
            Table::of::<Screen>()
                .columns
                .iter()
                .filter(|c| c.desired)
                .map(|c| c.name)
                .collect::<Vec<_>>(),
            ["level"]
        );
        assert_eq!(Screen::APPLY, Apply::IfUnchanged);
        let mut row = held.clone();
        row.take_desired(wanted);
        assert_eq!(
            row,
            Screen {
                level: 70,
                ..held.clone()
            }
        );
        assert_eq!(
            row.version_text().as_deref(),
            Some("2023-11-14T22:13:20.000001Z")
        );
        row.raise_version();
        assert!(row.at > at, "moved past what it was");
        assert_eq!(
            row.at.timestamp_subsec_nanos() % 1_000,
            0,
            "to the microsecond"
        );
    }

    #[test]
    fn a_version_is_compared_as_its_text_to_the_microsecond() {
        let nanos = chrono::DateTime::from_timestamp(1_700_000_000, 123_456_789).unwrap();
        let micros = chrono::DateTime::from_timestamp(1_700_000_000, 123_456_000).unwrap();
        assert_eq!(nanos.text(), micros.text(), "what PostgreSQL keeps of it");
        assert_eq!(micros.text(), "2023-11-14T22:13:20.123456Z");
        let east = micros.with_timezone(&chrono::FixedOffset::east_opt(3_600).unwrap());
        assert_eq!(east.text(), micros.text(), "the same instant, in UTC");
        assert_eq!(micros.naive_utc().text(), micros.text());
        assert_eq!(41_i64.text(), "41");
        assert_eq!(41_i16.raised(), 42);
        assert_eq!(i32::MAX.raised(), i32::MAX);
        let future = chrono::Utc::now() + chrono::Duration::days(1);
        assert_eq!(
            future.raised().text(),
            (future + chrono::Duration::microseconds(1)).text(),
            "a microsecond past a clock that is behind"
        );
        assert!(
            micros.raised() > chrono::Utc::now() - chrono::Duration::seconds(5),
            "now"
        );
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

    /// Routed by a view whose name a database quotes, keyed by two columns.
    #[derive(sqlx::FromRow, Clone, Debug, PartialEq, crate::Row)]
    #[row(table = "documents", mirror = "catalog", key = "shelf, number")]
    #[row(
        sync = "routed(view = Library.document readers, row = (shelf, document_number), replica = reader_id)"
    )]
    struct Document {
        shelf: i32,
        number: i64,
    }

    #[test]
    fn a_routed_row_names_its_view_quoted_and_its_key_in_key_order() {
        assert_eq!(
            Document::ROUTE,
            Route::Routed {
                view: r#""Library"."document readers""#,
                row: &["shelf", "document_number"],
                replica: "reader_id",
            }
        );
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
            get_by_keys::<Order>(&mut conn, &[order.key()])
                .await
                .unwrap(),
            vec![order.clone()],
            "an older version, taken"
        );
        let table = Table::of::<Order>();
        assert_eq!(
            parity(&mut conn, &[&table])
                .await
                .map_err(|e| e.to_string()),
            Ok(())
        );
        let retired = retire_by_keys::<Order>(&mut conn, &[order.key()], &[], &[])
            .await
            .unwrap();
        assert_eq!(retired.deleted, 1);
    }
}
