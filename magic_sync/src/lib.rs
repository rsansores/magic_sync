//! Replication of database tables to replicas by what each replica should
//! hold.
//!
//! A table declares which way it travels and which replicas hold which of its
//! rows ([`row`]). The server notes every change in the writing transaction,
//! plans per replica what it should hold against what it is known to hold
//! ([`plan`]), and sends the difference as rows encoded by their table's codec
//! ([`wire`]); a replica applies them by the rules the table declares.

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
#[cfg(all(feature = "sqlite", not(feature = "postgres")))]
pub use run::replica::Replica;
#[cfg(all(feature = "postgres", not(feature = "sqlite")))]
pub use run::server::Server;

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
