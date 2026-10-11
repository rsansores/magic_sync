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
//! [the replica protocol](https://github.com/rsansores/magic_sync/blob/main/magic_sync/PROTOCOL.md)
//! is what a replica in any language must do.
//!
//! A build of this crate is one side: enable `postgres` for the server or
//! `sqlite` for a replica. These docs show the side they were built for;
//! `cargo doc --features sqlite` shows a replica's.
//!
//! # Public dependencies
//!
//! The engine works over [`sqlx`] 0.8: the pools, connections and
//! transactions a host hands it are sqlx's, and a database error is
//! [`sqlx::Error`] ([`Error::Database`]). A table that travels as protobuf
//! is a [`prost`] 0.14 message. A new major version of either is a new major
//! version of this crate.

#![warn(missing_docs)]

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

pub use error::Error;
pub use magic_sync_derive::{Columns, Row};
pub use row::{Columns, Row};
#[cfg(feature = "sqlite")]
pub use run::replica::Replica;
#[cfg(feature = "postgres")]
pub use run::server::Server;

// The derive's generated code names this crate, and sqlx through it.
extern crate self as magic_sync;

/// A connection of this side's database — `&mut *tx` inside a transaction:
/// a `PgConnection` on the server, a `SqliteConnection` on a replica.
pub type Conn = <Db as sqlx::Database>::Connection;

/// The version of the engine's schema, on both sides: the engine refuses to
/// run against a database that installed another.
pub const SCHEMA_VERSION: i32 = 1;

/// The server's schema at [`SCHEMA_VERSION`], as one SQL script: a host with
/// migrations runs it in one, before the migration that writes the triggers
/// [`plan::Engine::triggers`] prints.
#[cfg(feature = "postgres")]
pub const POSTGRES_SCHEMA: &str = include_str!("../sql/postgres.sql");

/// A replica's schema at [`SCHEMA_VERSION`], as one SQL script: a host with
/// migrations runs it in one, before the triggers [`replica::triggers`]
/// prints.
#[cfg(feature = "sqlite")]
pub const SQLITE_SCHEMA: &str = include_str!("../sql/sqlite.sql");

use __private::Db;

#[cfg(all(test, feature = "postgres"))]
type DbPool = sqlx::PgPool;

/// What the derive's generated code names, and nothing else may: none of it
/// is part of the crate's API.
#[doc(hidden)]
pub mod __private {
    pub use crate::row::{Column, Columns, Route, Row};
    pub use sqlx;

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
