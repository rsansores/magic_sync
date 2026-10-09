//! What the two backends spell differently, and nothing else.
//!
//! Generic code over [`Row`](crate::row::Row) never carries a backend `cfg`:
//! where postgres and sqlite differ, the difference is an item here.

use crate::{DbPool, Error};

#[cfg(all(feature = "postgres", not(feature = "sqlite")))]
mod imp {
    use super::*;

    pub const NAME: &str = "postgres";

    /// This instant, as the backend writes a timestamp.
    pub const NOW: &str = "now()";

    /// Microsecond precision, as `timestamptz` stores it.
    pub fn not_older(incoming: &str, stored: &str) -> String {
        format!("{incoming} >= {stored}")
    }

    /// Binds one statement may carry.
    pub const MAX_BINDS: usize = 65_535;

    /// How a test table declares a uuid column.
    pub const UUID_TYPE: &str = "UUID";

    /// `(column, nullable, generated)` for every column of `schema.table`.
    /// Read from `pg_attribute`, which answers whatever the role may see.
    pub async fn columns(
        pool: &DbPool,
        schema: &str,
        table: &str,
    ) -> Result<Vec<(String, bool, bool)>, Error> {
        Ok(sqlx::query_as(
            "SELECT a.attname::text, NOT a.attnotnull, a.attgenerated <> '' \
               FROM pg_attribute a \
               JOIN pg_class c ON c.oid = a.attrelid \
               JOIN pg_namespace n ON n.oid = c.relnamespace \
              WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 AND NOT a.attisdropped",
        )
        .bind(schema)
        .bind(table)
        .fetch_all(pool)
        .await?)
    }

    /// The primary key's columns of `schema.table`, in key order.
    pub async fn primary_key(
        pool: &DbPool,
        schema: &str,
        table: &str,
    ) -> Result<Vec<String>, Error> {
        Ok(sqlx::query_scalar(
            "SELECT a.attname::text \
               FROM pg_index i \
               JOIN pg_class c ON c.oid = i.indrelid \
               JOIN pg_namespace n ON n.oid = c.relnamespace \
               JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY (i.indkey) \
              WHERE i.indisprimary AND n.nspname = $1 AND c.relname = $2 \
              ORDER BY array_position(i.indkey::int2[], a.attnum)",
        )
        .bind(schema)
        .bind(table)
        .fetch_all(pool)
        .await?)
    }
}

#[cfg(all(feature = "sqlite", not(feature = "postgres")))]
mod imp {
    use super::*;

    pub const NAME: &str = "sqlite";

    /// This instant, as the backend writes a timestamp.
    pub const NOW: &str = "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')";

    // Timestamps are text here, and not every writer formats them alike, so
    // compare them normalised — to the millisecond, `%f`'s precision.
    pub fn not_older(incoming: &str, stored: &str) -> String {
        format!(
            "strftime('%Y-%m-%dT%H:%M:%fZ', {incoming}) >= strftime('%Y-%m-%dT%H:%M:%fZ', {stored})"
        )
    }

    /// Binds one statement may carry (`SQLITE_MAX_VARIABLE_NUMBER`).
    pub const MAX_BINDS: usize = 32_766;

    /// How a test table declares a uuid column.
    pub const UUID_TYPE: &str = "BLOB";

    /// `(column, nullable, generated)` for every column of `table`. A primary
    /// key is NOT NULL only when declared so: sqlite lets a non-INTEGER one
    /// hold NULL.
    pub async fn columns(
        pool: &DbPool,
        _schema: &str,
        table: &str,
    ) -> Result<Vec<(String, bool, bool)>, Error> {
        Ok(sqlx::query_as(
            r#"SELECT name, "notnull" = 0, hidden IN (2, 3) FROM pragma_table_xinfo(?) WHERE hidden <> 1"#,
        )
        .bind(table)
        .fetch_all(pool)
        .await?)
    }

    /// The primary key's columns of `table`, in key order.
    pub async fn primary_key(
        pool: &DbPool,
        _schema: &str,
        table: &str,
    ) -> Result<Vec<String>, Error> {
        Ok(
            sqlx::query_scalar("SELECT name FROM pragma_table_info(?) WHERE pk > 0 ORDER BY pk")
                .bind(table)
                .fetch_all(pool)
                .await?,
        )
    }
}

pub use imp::*;

/// A table's name on this backend, as a `&'static str` a `const` can hold:
/// `qualified!("raw", "regions")` is `raw.regions` on postgres and `regions`
/// on sqlite.
#[cfg(all(feature = "postgres", not(feature = "sqlite")))]
#[macro_export]
macro_rules! qualified {
    ($schema:literal, $table:literal) => {
        concat!($schema, ".", $table)
    };
}

#[cfg(all(feature = "sqlite", not(feature = "postgres")))]
#[macro_export]
macro_rules! qualified {
    ($schema:literal, $table:literal) => {
        $table
    };
}
