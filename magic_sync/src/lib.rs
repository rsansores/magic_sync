//! Keeps partial copies of PostgreSQL tables on SQLite replicas, and brings
//! back what the replicas write — offline-first, by comparing what each
//! replica holds with what it should hold.
//!
//! A table is a struct that derives [`Row`] and says how it travels, and a
//! [`Codec`](wire::Codec) says how its rows turn into bytes. On the server, a
//! `Server` (feature `postgres`) notes every change in the writing
//! transaction, plans per replica what it should hold against what it is
//! known to hold, and sends the difference through your transport. On a
//! replica, a `Replica` (feature `sqlite`) applies what arrives in one
//! transaction, and sends up what the replica wrote.
//!
//! **[The book](https://rsansores.github.io/magic_sync/)** builds a small
//! system step by step and explains the engine from the ground up;
//! [the replica protocol](https://rsansores.github.io/magic_sync/protocol.html)
//! is what a replica in any language must do.
//!
//! A build of this crate is one side: enable `postgres` for the server or
//! `sqlite` for a replica, each in a workspace of its own
//! ([Set up a project](https://rsansores.github.io/magic_sync/guide/setup.html)).
//! These docs show the side they were built for; the book publishes both,
//! [the server's](https://rsansores.github.io/magic_sync/api/server/magic_sync/)
//! and [a replica's](https://rsansores.github.io/magic_sync/api/replica/magic_sync/).
//!
//! # Public dependencies
//!
//! What a host passes to the engine or reads back is another crate's type,
//! and a new major version of any of them is a new major version of this
//! crate:
//!
//! - [`sqlx`] 0.8: the pools, connections and transactions a host hands the
//!   engine, and the database error ([`Error::Database`], [`sqlx::Error`]).
//! - [`prost`] 0.14: a table that travels as protobuf is a message of it.
//! - [`uuid`] 1: replicas, partitions and keys are [`uuid::Uuid`]s.
//! - [`chrono`] 0.4: the times a status carries are its `DateTime<Utc>`.
//! - [`serde`] 1: a table that travels as JSON ([`wire::Codec::json`]) is a
//!   row that is `Serialize` and `Deserialize`, and the dashboard's
//!   documents are serialized with it.

#![warn(missing_docs, clippy::missing_errors_doc)]

#[cfg(not(any(feature = "postgres", feature = "sqlite")))]
compile_error!(
    "magic_sync is built for one side: enable `postgres` (the server's) or `sqlite` (a replica's)"
);

#[cfg(all(feature = "postgres", feature = "sqlite"))]
compile_error!(
    "magic_sync is built for one side, and this build enables both `postgres` and `sqlite`: \
     a crate that needs the server's side and one that needs a replica's were built together \
     (one workspace, or one test crate) — build each side in a workspace of its own"
);

#[cfg(test)]
mod conformance;
mod dialect;
pub mod envelope;
mod error;
#[cfg(feature = "postgres")]
mod filing;
#[cfg(test)]
mod model;
#[cfg(feature = "postgres")]
pub mod plan;
#[cfg(feature = "postgres")]
mod relay;
#[cfg(feature = "postgres")]
mod replica;
#[cfg(feature = "sqlite")]
pub mod replica;
pub mod row;
pub mod run;
#[cfg(feature = "postgres")]
pub mod status;
#[cfg(test)]
mod testkit;
pub mod wire;
mod wish;

pub use error::Error;
pub use row::{Columns, Row};

/// One struct is one table, column for column, and says how it travels:
/// implements [`Row`](trait@Row). The same struct serves both sides.
///
/// ```
/// use chrono::{DateTime, Utc};
/// use uuid::Uuid;
///
/// /// Written at the head office, held by every register; a register keeps
/// /// one a receipt of its own still points at, retired.
/// #[derive(sqlx::FromRow, Clone, magic_sync::Row)]
/// #[row(table = "products", schema = "shop", mirror = "catalog", sync = "broadcast", retired = "retired_at")]
/// pub struct Product {
///     pub id: Uuid,
///     pub name: String,
///     pub price_cents: i64,
///     pub updated_at: DateTime<Utc>,
/// }
///
/// /// Written by a register, filed at the head office under the register
/// /// that sent it.
/// #[derive(sqlx::FromRow, Clone, magic_sync::Row)]
/// #[row(table = "receipts", schema = "shop", mirror = "record", key = "register_id, id", origin = "register_id")]
/// pub struct Receipt {
///     pub register_id: Uuid,
///     pub id: Uuid,
///     pub product_id: Uuid,
///     pub total_cents: i64,
///     #[row(created)]
///     pub created_at: DateTime<Utc>,
/// }
/// ```
///
/// On the struct, `#[row(…)]` takes:
///
/// | key | means |
/// |---|---|
/// | `table` | the table's name (required) |
/// | `schema` | its PostgreSQL schema; `public` when left out |
/// | `mirror` | where it lives: `catalog` (written on the server, held by replicas), `record` (written on a replica, held by the server), `both` (each side its own columns) or `setting` (a value a replica holds and acts on, held by the server, which may wish its `desired` columns other values) — required |
/// | `sync` | which replicas hold which rows, for `catalog` and `both`: `broadcast` (every replica of the row's partition), `assigned(col)` (the replica `col` names: a uuid, or an `Option` of one), `follows(col)` (wherever the row `col` points at goes: one column, the whole of a foreign key into a mirrored table), `grouped(members = t, assigned = t)` (every replica a group holding the row is given to) or `routed(view = v, row = col, replica = col)` (every replica a view of yours pairs the row with: `view` is `view` or `schema.view`, `row` its columns holding the row's key — `row = (a, b)` for a key of two, in key order — and `replica` its uuid column naming the replica; partitioned, it has the partition column too. The server reads what the view reads off the catalog, through other views and SQL-standard functions, and notes a change of any of it; it refuses to start on a view that calls a function whose body it cannot read, or one of PostgreSQL's or an extension's that is not immutable (`now()`), or that reads a materialized view, a foreign table, a partitioned or inherited table, or a `replica_owned` column of the routed table: [the book](https://rsansores.github.io/magic_sync/guide/routed.html)). Left out, a row travels only with a row that points at it |
/// | `key` | the primary key's columns, in any order, when it is not `id`: `key = "list_id, customer_id"`. Each is a uuid or an integer ([`row::KeyValue`]), and a field of the row itself — not of a part flattened in |
/// | `conflict` | for `record`, the server's `PRIMARY KEY` when it is more than the key — `conflict = "id, taken_at"` — and what the server files the record against. A partitioned table, or a TimescaleDB hypertable, holds the partition column in every unique index. The key's columns and more, each a field of the row itself, none an `Option` nor `generated`, `version`, `created`, `replica_owned` or `desired`. The host never changes the extra ones once the row is written: the engine does not check, and a report with another value is filed as another row while the first stays. A replica's table is keyed by the key alone, which still names the row everywhere else ([the book](https://rsansores.github.io/magic_sync/guide/recipes.html#records-kept-in-a-partitioned-table)) |
/// | `active` | for `catalog` and `both`, the relation the rows the table's rule sends are read through on the server — `view` or `schema.view` (a name has no dot), a view that leaves out the rows no replica is sent for their own sake; one it leaves out still travels while a row a replica holds points at it |
/// | `from` | for `catalog`, the relation a row is read from on the server when not the table — `view` or `schema.view` — a view over the table, keyed as it is, that may join columns of other tables in and leave rows out: the rows it leaves out are not sent, as with `active`, which it replaces. The table is still what the row is noted, keyed and placed by, so two audiences (the server's `plan::Audience`) may hold one table in two shapes. A row it leaves out that a row sent points at cannot go, and neither can that row |
/// | `depends` | for `catalog` and `both`, columns of the table — each a foreign key of its own, or beside the partition column when the parent's key holds that too — whose parent's columns the row copies through `from`: `depends = "list_id"`. A change of the parent is a change of every row pointing at it so, sent again to whoever holds it, in the audience whose row says so. The parent need not be mirrored: the engine notes it all the same. One hop: a grandparent's columns are the parent's to say |
/// | `retired` | for `catalog` and `both`, the column a replica sets on a row it lets go of but must keep, because a row of its own points at it. It is left out of the row as a rule; a row that has it as a field too gets it back as the server sends it, which is live |
/// | `origin` | for `record`, `both` and `setting` — required for a `setting`, where it is the replica a wish of the row goes to — the uuid key column naming the replica a row belongs to: a replica deletes only its own rows |
/// | `apply` | for a `setting`, which wish its replica carries out ([`row::Apply`]): `always` (the default) or `if_unchanged`, only while it still holds the version the wish is based on |
/// | `unmirrored` | columns one side's table has that the row leaves out: `unmirrored = "archived_at, notes"` |
/// | `crate` | the path to this crate, when a host reaches it through another: `crate = "platform::sync"` |
///
/// On a field:
///
/// | flag | means |
/// |---|---|
/// | `#[row(version)]` | the column whose value only moves forward: an older report of a replica's record never overwrites a newer one on the server. An integer or a `chrono` instant ([`row::Version`]), never NULL. Only a `record` or a `setting` has one — a setting must, a field of its own: what a wish is based on. The server files a setting as a record, the newer version winning, but one its replica re-states as a snapshot begins whatever its version: a replica restored from an older copy holds what it holds. A replica takes what the server sends whatever its version |
/// | `#[row(desired)]` | in a `setting`, a column the server may wish other values ([`row::Setting`]); at least one, a field of the row itself, never its key or version |
/// | `#[row(created)]` | written once, when the row is created; never by an update |
/// | `#[row(replica_owned)]` | in a `both` table, a column only replicas write: it goes up, the rest come down |
/// | `#[row(generated)]` | computed by the database: read, never written |
///
/// Fields follow `sqlx::FromRow`: `#[sqlx(rename = "…")]` names the column,
/// `#[sqlx(try_from = "…")]` is the type the column decodes into,
/// `#[sqlx(skip)]` is no column (and takes no `rename`), and `#[sqlx(flatten)]` brings in the columns
/// of a part that derives [`Columns`](macro@Columns). `#[sqlx(json)]` and a
/// struct-level `#[sqlx(rename_all)]` are refused rather than
/// half-understood. A nullable column is a field written as `Option<…>`, spelled
/// so (`Option<T>` or `std::option::Option<T>`): the
/// engine refuses the table of a field typed through an alias.
///
/// Every name is the database's own, as written — the engine quotes each —
/// and at most the 63 bytes PostgreSQL keeps of one. Whatever the derive
/// cannot honour is a compile error pointing at what says it; whatever the
/// database disagrees with — a column on one side only, a nullability, a
/// type its field does not decode, another primary key — is refused when the
/// server or a replica starts.
pub use magic_sync_derive::Row;

/// A part of a row, flattened into it with `#[sqlx(flatten)]`: its columns
/// are the row's. Its fields take the same flags as a row's; on the struct,
/// only `#[row(crate = "…")]`.
///
/// ```
/// use chrono::{DateTime, Utc};
///
/// /// When a row was created, for every table that keeps it.
/// #[derive(sqlx::FromRow, Clone, magic_sync::Columns)]
/// pub struct Stamps {
///     #[row(created)]
///     pub created_at: DateTime<Utc>,
/// }
///
/// #[derive(sqlx::FromRow, Clone, magic_sync::Row)]
/// #[row(table = "notes", mirror = "record")]
/// pub struct Note {
///     pub id: uuid::Uuid,
///     pub text: String,
///     #[sqlx(flatten)]
///     pub stamps: Stamps,
/// }
/// ```
pub use magic_sync_derive::Columns;

// The derive's generated code names this crate, and sqlx through it.
extern crate self as magic_sync;

/// A connection of this side's database: `&mut *tx` inside a transaction.
#[cfg(feature = "postgres")]
pub type Conn = sqlx::PgConnection;
/// A connection of this side's database: `&mut *tx` inside a transaction.
#[cfg(feature = "sqlite")]
pub type Conn = sqlx::SqliteConnection;

/// The version of the engine's tables on the server's side: the server
/// refuses to run against a database that installed another. It counts the
/// server's own steps, independent of the replicas' version.
#[cfg(feature = "postgres")]
pub const SERVER_SCHEMA_VERSION: i32 = 5;

/// The version of the engine's tables on a replica: the replica refuses to
/// run against a database that installed another. It counts the replica's own
/// steps (`replica::migration`), independent of the server's version.
#[cfg(feature = "sqlite")]
pub const REPLICA_SCHEMA_VERSION: i32 = 4;

/// How long a row a replica wrote may wait before it is behind, unless a
/// host says otherwise: the server's `Settings::behind_after`, and the
/// replica's own status judges by the same.
pub(crate) const BEHIND_AFTER: std::time::Duration = std::time::Duration::from_secs(2 * 60);

use __private::Db;

#[cfg(all(test, feature = "postgres"))]
type DbPool = sqlx::PgPool;

/// What the derive's generated code names, and nothing else may: none of it
/// is part of the crate's API.
#[doc(hidden)]
pub mod __private {
    pub use crate::row::{Column, Columns, Route, Row};
    pub use sqlx;

    /// Keeps [`Columns`] and [`Row`] to what the derives write.
    pub trait Sealed {}

    /// This side's database.
    #[cfg(feature = "postgres")]
    pub type Db = sqlx::Postgres;
    /// This side's database.
    #[cfg(feature = "sqlite")]
    pub type Db = sqlx::Sqlite;

    /// Whether `T` is an `i64`, or an `Option` of one: whatever it is spelled
    /// as, an alias included.
    pub fn is_int64<T: 'static>() -> bool {
        use std::any::TypeId;
        [TypeId::of::<i64>(), TypeId::of::<Option<i64>>()].contains(&TypeId::of::<T>())
    }

    /// How many of `found` are `Some`.
    pub const fn count(found: &[Option<&'static str>]) -> usize {
        let mut n = 0;
        let mut i = 0;
        while i < found.len() {
            if found[i].is_some() {
                n += 1;
            }
            i += 1;
        }
        n
    }

    /// The first of `found` that is `Some`.
    pub const fn first(found: &[Option<&'static str>]) -> Option<&'static str> {
        let mut i = 0;
        while i < found.len() {
            if found[i].is_some() {
                return found[i];
            }
            i += 1;
        }
        None
    }
}
