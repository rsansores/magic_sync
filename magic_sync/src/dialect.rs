//! What the two backends spell differently, and nothing else.
//!
//! Generic code over [`Row`](crate::row::Row) never carries a backend `cfg`:
//! where postgres and sqlite differ, the difference is an item here.

use crate::row::{Row, VersionKind};
use crate::{Conn, Error};

/// A name as SQL quotes it, whatever it is spelled with.
pub(crate) fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A string as an SQL literal.
pub(crate) fn literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

#[cfg(feature = "postgres")]
mod imp {
    use super::*;

    pub(crate) const NAME: &str = "postgres";

    /// Every version type compares natively here: a `timestamptz` to the
    /// microsecond.
    pub(crate) fn not_older(incoming: &str, stored: &str, _kind: VersionKind) -> String {
        format!("{incoming} >= {stored}")
    }

    /// The relation a row's active rows are read from: the view its `ACTIVE`
    /// names, or the table.
    pub(crate) fn active<R: Row>() -> String {
        R::ACTIVE.unwrap_or(R::QUALIFIED).to_owned()
    }

    /// Binds one statement may carry.
    pub(crate) const MAX_BINDS: usize = 65_535;

    /// A host writes whole rows of any table here.
    pub(crate) const WRITES_ANY_TABLE: bool = true;

    /// `(column, nullable, generated)` for every column of `schema.table`.
    /// Read from `pg_attribute`, which answers whatever the role may see.
    pub(crate) async fn columns(
        conn: &mut Conn,
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
        .fetch_all(&mut *conn)
        .await?)
    }

    /// The primary key's columns of `schema.table`, in key order.
    pub(crate) async fn primary_key(
        conn: &mut Conn,
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
        .fetch_all(&mut *conn)
        .await?)
    }
}

#[cfg(feature = "sqlite")]
mod imp {
    use super::*;

    pub(crate) const NAME: &str = "sqlite";

    /// This instant, as the backend writes a timestamp.
    pub(crate) const NOW: &str = "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')";

    /// Numbers compare natively. Anything else is text here, and a timestamp
    /// is text not every writer formats alike, so text compares normalised as
    /// an instant (to the millisecond, `%f`'s precision) — and as itself when
    /// it is no instant, rather than as a NULL no comparison passes.
    pub(crate) fn not_older(incoming: &str, stored: &str, kind: VersionKind) -> String {
        match kind {
            VersionKind::Instant => {
                let instant =
                    |v: &str| format!("coalesce(strftime('%Y-%m-%dT%H:%M:%fZ', {v}), {v})");
                format!("{} >= {}", instant(incoming), instant(stored))
            }
            VersionKind::Integer => format!("{incoming} >= {stored}"),
        }
    }

    /// The relation a row's active rows are read from: on a replica, its rows
    /// less the ones it retired.
    pub(crate) fn active<R: Row>() -> String {
        match R::RETIRED {
            Some(retired) => format!(
                "(SELECT * FROM {} WHERE {} IS NULL)",
                R::QUALIFIED,
                ident(retired)
            ),
            None => R::QUALIFIED.to_owned(),
        }
    }

    /// The foreign keys that point at rows of `table`, whatever they do on
    /// delete — refuse it (`NO ACTION`, `RESTRICT`, deferred or not), or
    /// change the row that points (`CASCADE`, `SET NULL`, `SET DEFAULT`),
    /// which would change a row of the replica's own the server never sent —
    /// as `(child, its columns, the parent's columns)`. A key naming no
    /// parent columns names the parent's primary key, `key`.
    pub(crate) async fn pointing(
        conn: &mut Conn,
        table: &str,
        key: &[&str],
    ) -> Result<Vec<(String, Vec<String>, Vec<String>)>, Error> {
        let rows: Vec<(String, i64, String, Option<String>)> = sqlx::query_as(
            r#"SELECT m.name, f.id, f."from", f."to"
                 FROM sqlite_master m JOIN pragma_foreign_key_list(m.name) f
                WHERE m.type = 'table' AND f."table" = ?1 COLLATE NOCASE
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
    pub(crate) const MAX_BINDS: usize = 32_766;

    /// A host writes whole rows here only of a record: of a two-way row a
    /// replica writes the columns it owns, and the rest are the server's.
    pub(crate) const WRITES_ANY_TABLE: bool = false;

    /// `(column, nullable, generated)` for every column of `table`. A primary
    /// key is NOT NULL only when declared so: sqlite lets a non-INTEGER one
    /// hold NULL.
    pub(crate) async fn columns(
        conn: &mut Conn,
        _schema: &str,
        table: &str,
    ) -> Result<Vec<(String, bool, bool)>, Error> {
        Ok(sqlx::query_as(
            r#"SELECT name, "notnull" = 0, hidden IN (2, 3) FROM pragma_table_xinfo(?) WHERE hidden <> 1"#,
        )
        .bind(table)
        .fetch_all(&mut *conn)
        .await?)
    }

    /// The primary key's columns of `table`, in key order.
    pub(crate) async fn primary_key(
        conn: &mut Conn,
        _schema: &str,
        table: &str,
    ) -> Result<Vec<String>, Error> {
        Ok(
            sqlx::query_scalar("SELECT name FROM pragma_table_info(?) WHERE pk > 0 ORDER BY pk")
                .bind(table)
                .fetch_all(&mut *conn)
                .await?,
        )
    }
}

pub(crate) use imp::*;

/// A table's name on this backend, as a `&'static str` a `const` can hold:
/// the derive's `QUALIFIED`, `"raw"."regions"` on postgres and `"regions"`
/// on sqlite, from the two names it quotes.
#[cfg(feature = "postgres")]
#[doc(hidden)]
#[macro_export]
macro_rules! __qualified {
    ($schema:literal, $table:literal) => {
        concat!($schema, ".", $table)
    };
}

#[cfg(feature = "sqlite")]
#[doc(hidden)]
#[macro_export]
macro_rules! __qualified {
    ($schema:literal, $table:literal) => {
        $table
    };
}
