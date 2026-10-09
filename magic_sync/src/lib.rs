//! Keeps partial copies of PostgreSQL tables on SQLite replicas, and brings
//! back what the replicas write — offline-first, by comparing what each
//! replica holds with what it should hold.
//!
//! A table is a struct that derives [`Row`] and says how it travels. On the
//! server, a `Server` (feature `postgres`) notes every change in the writing
//! transaction, plans per replica what it should hold against what it is known
//! to hold, and sends the difference through your transport. On a replica, a
//! `Replica` (feature `sqlite`) applies what arrives in one transaction, and
//! sends up what the replica wrote.
//!
//! **[The book](https://rsansores.github.io/magic_sync/)** builds a small
//! system step by step and explains the engine from the ground up;
//! [the replica protocol](https://github.com/rsansores/magic_sync/blob/main/magic_sync/PROTOCOL.md)
//! is what a replica in any language must do.
//!
//! A build of this crate is one side: enable `postgres` for the server or
//! `sqlite` for a replica. These docs show the server's side; the replica's
//! is in the book, and in `cargo doc --features sqlite`.

#[cfg(not(any(feature = "postgres", feature = "sqlite")))]
compile_error!(
    "magic_sync is built for one side: enable `postgres` (the server's) or `sqlite` (a replica's)"
);

pub mod dialect;
pub mod envelope;
mod error;
#[cfg(all(feature = "postgres", not(feature = "sqlite")))]
pub mod plan;
#[cfg(all(feature = "postgres", not(feature = "sqlite")))]
pub mod relay;
pub mod replica;
pub mod row;
pub mod run;
#[cfg(test)]
mod testkit;
pub mod wire;

pub use error::Error;
pub use magic_sync_derive::{Columns, Row};

// The derive's generated code names this crate, and sqlx through it.
extern crate self as magic_sync;
#[cfg(all(feature = "sqlite", not(feature = "postgres")))]
pub use run::replica::Replica;
#[cfg(all(feature = "postgres", not(feature = "sqlite")))]
pub use run::server::Server;
#[doc(hidden)]
pub use sqlx as __sqlx;

/// The version of the engine's schema, on both sides: the engine refuses to
/// run against a database that installed another.
pub const SCHEMA_VERSION: i32 = 1;

/// The server's schema, as one SQL script: a host runs it in a migration
/// before any table calls `sync.mirror_table`.
#[cfg(all(feature = "postgres", not(feature = "sqlite")))]
pub const POSTGRES_SCHEMA: &str = include_str!("../sql/postgres.sql");

/// A replica's schema, as one SQL script a host runs in a migration.
#[cfg(all(feature = "sqlite", not(feature = "postgres")))]
pub const SQLITE_SCHEMA: &str = include_str!("../sql/sqlite.sql");

#[cfg(all(feature = "postgres", not(feature = "sqlite")))]
pub type Db = sqlx::Postgres;
#[cfg(all(feature = "sqlite", not(feature = "postgres")))]
pub type Db = sqlx::Sqlite;

#[cfg(all(feature = "postgres", not(feature = "sqlite")))]
pub type DbPool = sqlx::PgPool;
#[cfg(all(feature = "sqlite", not(feature = "postgres")))]
pub type DbPool = sqlx::SqlitePool;
