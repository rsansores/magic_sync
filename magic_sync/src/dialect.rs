//! What the two backends spell differently, and nothing else.
//!
//! Generic code over [`Row`](crate::row::Row) never carries a backend `cfg`:
//! where postgres and sqlite differ, the difference is an item here.

use crate::row::{Row, Wire};
use crate::{DbPool, Error};

#[cfg(all(feature = "postgres", not(feature = "sqlite")))]
mod imp {
    use super::*;

    pub const NAME: &str = "postgres";

    /// This instant, as the backend writes a timestamp.
    pub const NOW: &str = "now()";

    /// Every version type compares natively here: a `timestamptz` to the
    /// microsecond.
    pub fn not_older(incoming: &str, stored: &str, _kind: Wire) -> String {
        format!("{incoming} >= {stored}")
    }

    /// The relation a row's active rows are read from: the view its `ACTIVE`
    /// names, or the table.
    pub fn active<R: Row>() -> String {
        R::ACTIVE.unwrap_or(R::QUALIFIED).to_owned()
    }

    /// Binds one statement may carry.
    pub const MAX_BINDS: usize = 65_535;

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

    /// Numbers compare natively. Anything else is text here, and a timestamp
    /// is text not every writer formats alike, so text compares normalised as
    /// an instant (to the millisecond, `%f`'s precision) — and as itself when
    /// it is no instant, rather than as a NULL no comparison passes.
    pub fn not_older(incoming: &str, stored: &str, kind: Wire) -> String {
        match kind {
            Wire::String | Wire::Json => {
                let instant =
                    |v: &str| format!("coalesce(strftime('%Y-%m-%dT%H:%M:%fZ', {v}), {v})");
                format!("{} >= {}", instant(incoming), instant(stored))
            }
            Wire::Int16 | Wire::Int32 | Wire::Int64 | Wire::Bool | Wire::Float | Wire::Double => {
                format!("{incoming} >= {stored}")
            }
        }
    }

    /// The relation a row's active rows are read from: on a replica, its rows
    /// less the ones it retired.
    pub fn active<R: Row>() -> String {
        match R::RETIRED {
            Some(retired) => format!("(SELECT * FROM {} WHERE {retired} IS NULL)", R::QUALIFIED),
            None => R::QUALIFIED.to_owned(),
        }
    }

    /// The foreign keys that refuse to let a row of `table` go while a row
    /// points at it — `NO ACTION` or `RESTRICT` on delete, deferred or not —
    /// as `(child, its columns, the parent's columns)`. A key naming no
    /// parent columns names the parent's primary key, `key`.
    pub async fn restricting(
        conn: &mut crate::row::Conn,
        table: &str,
        key: &[&str],
    ) -> Result<Vec<(String, Vec<String>, Vec<String>)>, Error> {
        let rows: Vec<(String, i64, String, Option<String>)> = sqlx::query_as(
            r#"SELECT m.name, f.id, f."from", f."to"
                 FROM sqlite_master m JOIN pragma_foreign_key_list(m.name) f
                WHERE m.type = 'table' AND f."table" = ?1 COLLATE NOCASE
                  AND upper(f.on_delete) IN ('NO ACTION', 'RESTRICT')
                ORDER BY m.name, f.id, f.seq"#,
        )
        .bind(table)
        .fetch_all(&mut *conn)
        .await?;
        let mut out: Vec<(String, Vec<String>, Vec<String>)> = Vec::new();
        let mut last = None;
        for (child, id, from, to) in rows {
            if last.as_ref() != Some(&(child.clone(), id)) {
                out.push((child.clone(), Vec::new(), Vec::new()));
                last = Some((child, id));
            }
            let (_, from_columns, to_columns) = out.last_mut().expect("just pushed");
            from_columns.push(from);
            to_columns.extend(to);
        }
        for (_, from, to) in &mut out {
            if to.len() != from.len() {
                *to = key.iter().map(|k| (*k).to_owned()).collect();
            }
        }
        Ok(out)
    }

    /// Binds one statement may carry (`SQLITE_MAX_VARIABLE_NUMBER`).
    pub const MAX_BINDS: usize = 32_766;

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
