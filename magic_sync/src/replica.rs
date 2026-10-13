//! A replica's side: what the server sends, applied table by table through
//! the codecs, and what the replica wrote, gathered to go up — run by the
//! [`Replica`](crate::run::replica::Replica). What a host does here itself is set up the
//! database ([`install`], or [`triggers`] and [`check`] for one with
//! migrations), say how its writes go up ([`Lane`], [`Outbox`]), and ask how
//! far along they are ([`progress`]).

use crate::envelope;
use crate::wire::Codec;
#[cfg(feature = "sqlite")]
use {
    crate::dialect::{ident, literal},
    crate::envelope::Up,
    crate::envelope::{framed, Body, Down, EndSnapshot, Table, ToReplica},
    crate::row::{key_text, Key, KeyKind},
    crate::{Conn, Error},
    prost::Message as _,
    std::collections::{HashMap, HashSet},
    uuid::Uuid,
};

/// The codec of `table`, when this build mirrors it.
pub(crate) fn codec_of<'a, P>(codecs: &'a [Codec<P>], table: &str) -> Option<&'a Codec<P>> {
    codecs.iter().find(|c| c.table.name == table)
}

/// The keys of `wire` that parse as `codec`'s; one that does not is logged
/// and left out.
pub(crate) fn keys_of<P: Sync + 'static>(
    codec: &Codec<P>,
    wire: &[envelope::Key],
) -> Vec<crate::row::Key> {
    parsed(codec, wire).into_iter().flatten().collect()
}

/// Each key of `wire` as `codec`'s, in its place: `None`, logged, where one
/// does not parse.
pub(crate) fn parsed<P: Sync + 'static>(
    codec: &Codec<P>,
    wire: &[envelope::Key],
) -> Vec<Option<crate::row::Key>> {
    wire.iter()
        .map(|key| {
            codec
                .key(&key.parts)
                .inspect_err(|e| {
                    tracing::error!(
                        table = codec.table.name,
                        "sync: a key that does not parse — left out: {}",
                        capped(&e.to_string())
                    );
                })
                .ok()
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The engine's tables and triggers.

/// The engine's steps, in order, each as released and never edited: the
/// `n`th (from 1) brings the engine's tables from version `n - 1` to `n`.
#[cfg(feature = "sqlite")]
const MIGRATIONS: [&str; crate::REPLICA_SCHEMA_VERSION as usize] = [
    include_str!("../sql/sqlite/1.sql"),
    include_str!("../sql/sqlite/2.sql"),
];

/// The engine's step to `version` of its tables on a replica, as one SQL
/// script that also records the version it brings them to — `None` for a
/// version no build of the engine has had yet. [`install`] runs every step a
/// database lacks, in order; a host with migrations runs each, in one of its
/// own: every step up to [`REPLICA_SCHEMA_VERSION`], in order, in the
/// migration that creates its tables, before [`triggers`]' script, and each
/// later one when a release brings it (its changelog says). A step is never
/// edited once released.
///
/// [`REPLICA_SCHEMA_VERSION`]: crate::REPLICA_SCHEMA_VERSION
#[cfg(feature = "sqlite")]
#[must_use]
pub fn migration(version: i32) -> Option<String> {
    let step = MIGRATIONS.get(usize::try_from(version.checked_sub(1)?).ok()?)?;
    Some(format!(
        "{step}\nINSERT INTO sync_version (id, version) VALUES (1, {version}) \
         ON CONFLICT (id) DO UPDATE SET version = excluded.version;\n"
    ))
}

/// A trigger the engine needs on a replica database.
#[cfg(feature = "sqlite")]
struct Trigger {
    name: String,
    sql: String,
}

/// The triggers every mirrored table needs, as the engine writes them:
///
/// - the server's tables (catalog) refuse a write that is not the engine's:
///   a replica's own edit would be overwritten, and never go up;
/// - a table that goes up notes every write in `sync_notes`, one note per
///   row, counting its writes — the row's key,
///   in the one form keys are compared in, and on an update that changes it
///   the old key too, which is gone — and refuses a key stored in a form the
///   engine does not read back (a uuid is a 16-byte blob, an integer an
///   integer);
/// - a record without a version is history: an update of one is refused;
/// - a row of a table written on both sides begins on the server: an insert
///   is refused, so is an update of a column the replica does not own, and a
///   delete unless the table is keyed by the replica and the row by this
///   one ([`identified`]); only an update of the columns the replica owns,
///   and such a delete, go up.
///
/// A write the engine makes while it applies what the server sent is noted
/// by none and refused by none. Before the replica first runs knowing which
/// it is ([`identified`]), the host is setting it up: nothing is refused, and
/// a write to a table the server sends is not noted either — nothing has gone
/// up yet, and the server's provisioning sets those rows straight. A write
/// to the replica's own tables is noted then too, and goes up once it knows.
#[cfg(feature = "sqlite")]
fn generated<P: Sync + 'static>(codecs: &[&Codec<P>]) -> Vec<Trigger> {
    use crate::row::Mirror;
    // Until the replica knows which it is, the host sets it up: it writes
    // what it needs, and the server's provisioning sets its rows straight.
    const SETTING_UP: &str = " WHEN NOT EXISTS (SELECT 1 FROM sync_replica)";
    const IDENTIFIED: &str = " WHEN EXISTS (SELECT 1 FROM sync_replica)";
    // Neither the engine applying what the server sent, nor the host setting
    // the replica up.
    const GUARDED: &str = " WHEN NOT EXISTS (SELECT 1 FROM sync_applying) \
                           AND EXISTS (SELECT 1 FROM sync_replica)";
    // A row noted again is one note, written once more: a hot row goes up
    // once, as it is by then, and a write since clears what set it aside.
    const NOTED: &str =
        "ON CONFLICT (relation, row_key) DO UPDATE SET writes = writes + 1, set_aside = NULL";
    let mut out = Vec::new();
    for codec in codecs {
        let table = codec.table.name;
        let on = ident(table);
        let key_columns = key_columns(&codec.table);
        let key = |row: &str| key_sql(&codec.table, row);
        let readable = key_columns
            .iter()
            .map(|c| {
                let value = format!("NEW.{}", ident(c.name));
                if c.key == Some(KeyKind::Integer) {
                    format!("typeof({value}) = 'integer'")
                } else {
                    format!("(typeof({value}) = 'blob' AND length({value}) = 16)")
                }
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        let guard = format!(
            "SELECT RAISE(ABORT, {}) WHERE NOT ({readable}); ",
            literal(&format!(
                "`{table}`: a key stored in a form sync does not read — a uuid is a 16-byte blob, an integer an integer"
            ))
        );
        let note = |row: &str| {
            format!(
                "INSERT INTO sync_notes (relation, row_key) VALUES ({}, {}) {NOTED}; ",
                literal(table),
                key(row)
            )
        };
        let gone = format!(
            "INSERT INTO sync_notes (relation, row_key) SELECT {}, {} WHERE {} IS NOT {} {NOTED}; ",
            literal(table),
            key("OLD"),
            key("OLD"),
            key("NEW")
        );
        let refuse = |event: &str, when: &str, why: String| Trigger {
            name: format!("sync_refuse_{table}_{}", event.to_lowercase()),
            sql: format!(
                "CREATE TRIGGER {} BEFORE {event} ON {on}{when} BEGIN SELECT RAISE(ABORT, {}); END",
                ident(&format!("sync_refuse_{table}_{}", event.to_lowercase())),
                literal(&why)
            ),
        };
        let noting = |event: &str, of: &str, when: &str, body: String| Trigger {
            name: format!("sync_note_{table}_{}", event.to_lowercase()),
            sql: format!(
                "CREATE TRIGGER {} AFTER {event}{of} ON {on}{when} BEGIN {body}END",
                ident(&format!("sync_note_{table}_{}", event.to_lowercase()))
            ),
        };
        match codec.table.mirror {
            Mirror::Catalog => {
                for event in ["INSERT", "UPDATE", "DELETE"] {
                    let why = format!("`{table}` is the server's: a replica does not write it");
                    out.push(refuse(event, GUARDED, why));
                }
            }
            Mirror::Record | Mirror::Setting => {
                out.push(noting("INSERT", "", "", format!("{guard}{}", note("NEW"))));
                if codec.table.columns.iter().any(|c| c.version.is_some()) {
                    let body = format!("{guard}{}{gone}", note("NEW"));
                    out.push(noting("UPDATE", "", "", body));
                } else {
                    let why = format!(
                        "`{table}` is history: a row of it is written once (a version column makes it editable)"
                    );
                    out.push(refuse("UPDATE", IDENTIFIED, why));
                    // Nothing went up while it was set up: a key renamed then
                    // leaves no note of the old one behind.
                    let renamed = format!(
                        "DELETE FROM sync_notes WHERE relation = {} AND row_key = {} AND {} IS NOT {}; ",
                        literal(table),
                        key("OLD"),
                        key("OLD"),
                        key("NEW")
                    );
                    let body = format!("{guard}{renamed}{}", note("NEW"));
                    out.push(noting("UPDATE", "", SETTING_UP, body));
                }
                out.push(noting("DELETE", "", "", note("OLD")));
            }
            Mirror::Both => {
                let why =
                    format!("a row of `{table}` begins on the server: a replica only edits it");
                out.push(refuse("INSERT", GUARDED, why));
                let (owned, theirs): (Vec<&crate::row::Column>, Vec<&crate::row::Column>) =
                    codec.table.columns.iter().partition(|c| c.replica_owned);
                let changed = theirs
                    .iter()
                    .map(|c| format!("OLD.{0} IS NOT NEW.{0}", ident(c.name)))
                    .collect::<Vec<_>>()
                    .join(" OR ");
                let why = format!(
                    "`{table}` is the server's but for the columns a replica owns: a replica \
                     edits only those"
                );
                out.push(refuse("UPDATE", &format!("{GUARDED} AND ({changed})"), why));
                let of = if owned.is_empty() {
                    String::new()
                } else {
                    let owned: Vec<String> = owned.iter().map(|c| ident(c.name)).collect();
                    format!(" OF {}", owned.join(", "))
                };
                let body = format!("{guard}{}{gone}", note("NEW"));
                out.push(noting("UPDATE", &of, GUARDED, body));
                if let Some(origin) = codec.table.origin.filter(|_| codec.takes_deletes()) {
                    let why = format!(
                        "a row of `{table}` is let go of by the server: a replica deletes only \
                         the ones keyed by it, once it knows which it is"
                    );
                    let others = format!(
                        "{GUARDED} AND NOT EXISTS (SELECT 1 FROM sync_replica WHERE replica = OLD.{})",
                        ident(origin)
                    );
                    out.push(refuse("DELETE", &others, why));
                    out.push(noting("DELETE", "", GUARDED, note("OLD")));
                } else {
                    let why = format!(
                        "a row of `{table}` is let go of by the server: a replica deletes one \
                         only of a table keyed by it"
                    );
                    out.push(refuse("DELETE", GUARDED, why));
                }
            }
        }
    }
    out
}

/// The columns of `table`'s key, in key order.
#[cfg(feature = "sqlite")]
fn key_columns(table: &crate::row::Table) -> Vec<&crate::row::Column> {
    table
        .key
        .iter()
        .filter_map(|k| table.columns.iter().find(|c| c.name == *k))
        .collect()
}

/// The key of the row `row` (an alias, `NEW` or `OLD`) of `table` as SQL
/// writes it in the one form keys are compared in.
#[cfg(feature = "sqlite")]
fn key_sql(table: &crate::row::Table, row: &str) -> String {
    key_columns(table)
        .iter()
        .map(|c| {
            let value = format!("{row}.{}", ident(c.name));
            if c.key == Some(KeyKind::Integer) {
                format!("CAST({value} AS TEXT)")
            } else {
                let hex = format!("hex({value})");
                format!(
                    "lower(substr({hex}, 1, 8) || '-' || substr({hex}, 9, 4) || '-' || \
                     substr({hex}, 13, 4) || '-' || substr({hex}, 17, 4) || '-' || substr({hex}, 21))"
                )
            }
        })
        .collect::<Vec<_>>()
        .join(" || '|' || ")
}

/// Notes again every row of the replica's settings, and every answer it gave
/// to a wish: what the server records of them may be behind — a server or a
/// replica restored from an older copy — and a snapshot beginning is the
/// moment the server sets out to find what this replica holds. What it notes
/// so goes up re-stated, which the server takes whatever its version, until
/// the upload that carries it is filed (7).
#[cfg(feature = "sqlite")]
async fn restate<P: Sync + 'static>(conn: &mut Conn, codecs: &[Codec<P>]) -> Result<(), Error> {
    for codec in codecs {
        let table = &codec.table;
        let only = match table.mirror {
            // SQLite reads an upsert's ON after a SELECT with no WHERE as a
            // join's.
            crate::row::Mirror::Setting => " WHERE true",
            _ if table.name == crate::wish::TABLE => {
                " WHERE t.answered IS NOT NULL AND t.retired_at IS NULL"
            }
            _ => continue,
        };
        sqlx::query(&format!(
            "INSERT INTO sync_notes (relation, row_key, restated) SELECT {}, {}, 1 FROM {} t{only} \
             ON CONFLICT (relation, row_key) DO UPDATE SET writes = writes + 1, restated = 1, \
                    upload = NULL, sent = NULL, sent_at = NULL",
            literal(table.name),
            key_sql(table, "t"),
            ident(table.name)
        ))
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

/// Of `recorded`, the rows of a setting the server records this replica
/// holding, notes those it does not hold as its own deletes: the server
/// lets go of them as it files the deletes (4.2).
#[cfg(feature = "sqlite")]
async fn gone_here<P: Sync + 'static>(
    conn: &mut Conn,
    codec: &Codec<P>,
    recorded: &[Key],
) -> Result<(), Error> {
    let held = codec.present(conn, recorded).await?;
    for key in recorded.iter().filter(|k| !held.contains(k)) {
        sqlx::query(
            "INSERT INTO sync_notes (relation, row_key) VALUES ($1, $2) \
             ON CONFLICT (relation, row_key) DO UPDATE SET writes = writes + 1",
        )
        .bind(codec.table.name)
        .bind(key_text(key))
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

/// The triggers a replica database needs for `codecs`, as one SQL script: a
/// host with migrations puts it in the one that creates the tables, after
/// every step of the engine's schema up to [`REPLICA_SCHEMA_VERSION`]
/// ([`migration`]), and again whenever a table is added or changes how it
/// travels, or a release writes them otherwise: each trigger is dropped if it
/// exists before it is created, so the script runs again over what an
/// earlier run wrote. [`check`] refuses a database whose triggers are not
/// these.
///
/// # Errors
///
/// [`Error::Misuse`] for a table of `codecs` whose name the engine keeps
/// for its own (`sync_…`).
///
/// [`REPLICA_SCHEMA_VERSION`]: crate::REPLICA_SCHEMA_VERSION
#[cfg(feature = "sqlite")]
pub fn triggers<P: Sync + 'static>(codecs: &[Codec<P>]) -> Result<String, Error> {
    let wishes = crate::wish::engines(codecs)?;
    Ok(generated(&codecs.iter().chain(&wishes).collect::<Vec<_>>())
        .into_iter()
        .map(|t| format!("DROP TRIGGER IF EXISTS {};\n{};\n", ident(&t.name), t.sql))
        .collect())
}

/// Records which replica this is, for the triggers to know its rows by.
#[cfg(feature = "sqlite")]
pub(crate) async fn identified(conn: &mut Conn, replica: Uuid) -> Result<(), Error> {
    sqlx::query(
        "INSERT INTO sync_replica (id, replica) VALUES (1, $1) \
         ON CONFLICT (id) DO UPDATE SET replica = EXCLUDED.replica \
           WHERE replica IS NOT EXCLUDED.replica",
    )
    .bind(replica)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// The version of the engine's tables a database holds: 0 when none.
#[cfg(feature = "sqlite")]
async fn schema_version(conn: &mut Conn) -> Result<i32, Error> {
    let versioned: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'sync_version')",
    )
    .fetch_one(&mut *conn)
    .await?;
    if !versioned {
        return Ok(0);
    }
    Ok(sqlx::query_scalar("SELECT version FROM sync_version")
        .fetch_optional(&mut *conn)
        .await?
        .unwrap_or(0))
}

/// Refuses a replica database the engine cannot run against: another
/// version of its tables; a table that is not its row, column for column —
/// one missing or extra, nullable on one side only, of a type its field does
/// not decode, keyed otherwise — with every difference named; a unique index
/// of a table with a retired column that is not partial on `<retired> IS
/// NULL` (alone, or among conditions joined by `AND`), which would hold a retired row's values against the server's live
/// ones; triggers that
/// are not the ones [`triggers`] writes — one missing, one that differs, or
/// one left from a table that no longer travels, the engine's being those
/// named `sync_note_…` and `sync_refuse_…` — but the note triggers of a
/// table this build does not know, which a later build wrote; or a connection the engine
/// cannot keep its word on — `synchronous` below `FULL`, `journal_mode`
/// `OFF` or `MEMORY` (of a database with a file), `recursive_triggers` off — which the pool's options
/// set. [`Replica::new`] runs it; a host with migrations also runs it in a
/// test, over the database its migrations build.
///
/// [`Replica::new`]: crate::run::replica::Replica::new
///
/// # Errors
///
/// [`Error::Misuse`] naming whatever the database is not, as above;
/// [`Error::Database`] when the database fails.
#[cfg(feature = "sqlite")]
pub async fn check<P: Sync + 'static>(conn: &mut Conn, codecs: &[Codec<P>]) -> Result<(), Error> {
    let wishes = crate::wish::engines(codecs)?;
    checked(conn, &codecs.iter().chain(&wishes).collect::<Vec<_>>()).await
}

/// [`check`] of `codecs` as the engine holds them: the engine's own tables
/// among them.
#[cfg(feature = "sqlite")]
pub(crate) async fn checked<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[&Codec<P>],
) -> Result<(), Error> {
    let (version, needed) = (schema_version(conn).await?, crate::REPLICA_SCHEMA_VERSION);
    if version > needed {
        return Err(Error::Misuse(format!(
            "the engine's tables are version {version}, from a later build than this one \
             ({needed}): run that build, or a later one"
        )));
    }
    if version < needed {
        return Err(Error::Misuse(format!(
            "the engine's tables are version {version}, and this build needs {needed}: \
             `replica::install` brings them there, or a migration of yours runs the \
             engine's steps (the book's \"Upgrading\")"
        )));
    }
    let tables: Vec<&crate::row::Table> = codecs.iter().map(|c| &c.table).collect();
    crate::row::parity(conn, &tables).await?;
    for table in &tables {
        let Some(retired) = table.retired else {
            continue;
        };
        for (index, sql) in crate::dialect::unique_indexes(conn, table.name).await? {
            if !sql.is_some_and(|sql| leaves_out(&sql, retired)) {
                return Err(Error::Misuse(format!(
                    "`{}`: the unique index `{index}` holds its retired rows' values too, which \
                     would refuse every row the server sends with one of them while the retired \
                     row stays — make it partial: `WHERE {retired} IS NULL`",
                    table.name
                )));
            }
        }
    }
    let rows: Vec<(String, String, String)> =
        sqlx::query_as("SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger'")
            .fetch_all(&mut *conn)
            .await?;
    let live: HashMap<String, String> = rows
        .into_iter()
        .filter(|(name, table, _)| !unknown(codecs, name, table))
        .map(|(name, _, sql)| (name, sql))
        .collect();
    let normal = |sql: &str| sql.split_whitespace().collect::<Vec<_>>().join(" ");
    let wanted = generated(codecs);
    for trigger in &wanted {
        match live.get(&trigger.name) {
            None => {
                return Err(Error::Misuse(format!(
                    "the trigger `{}` is missing: install, or run replica::triggers' script in a migration",
                    trigger.name
                )))
            }
            Some(sql) if normal(sql) != normal(&trigger.sql) => {
                return Err(Error::Misuse(format!(
                    "the trigger `{}` is not the one replica::triggers writes: {sql}",
                    trigger.name
                )))
            }
            Some(_) => {}
        }
    }
    if let Some(name) = live
        .keys()
        .find(|name| engines(name) && !wanted.iter().any(|t| t.name == **name))
    {
        return Err(Error::Misuse(format!(
            "the trigger `{name}` is not one of the engine's: a table that no longer travels, or one written by hand — drop it"
        )));
    }
    settled(conn).await?;
    Ok(())
}

/// Whether the unique index `sql` creates leaves out every row `retired`
/// is set on: its filter says `<retired> IS NULL`, alone or as one of the
/// conditions it joins with `AND` — with no `OR` beside them — in whatever
/// case, quoting, spacing or parentheses.
#[cfg(feature = "sqlite")]
fn leaves_out(sql: &str, retired: &str) -> bool {
    let normal = |text: &str| {
        text.to_lowercase()
            .replace(['"', '`', '[', ']'], "")
            .replace('(', " ( ")
            .replace(')', " ) ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    };
    let live = normal(&format!("{retired} is null"));
    let sql = normal(sql);
    let Some((_, filter)) = sql.rsplit_once(" where ") else {
        return false;
    };
    let mut terms = vec![filter.to_owned()];
    while let Some(term) = terms.pop() {
        if term == live {
            return true;
        }
        // Split at each `and` outside parentheses; a term that is one pair
        // of them, opened. An `or` outside them binds looser than any
        // `and`: such a term admits rows its conditions do not, and says
        // nothing.
        let words: Vec<&str> = term.split(' ').collect();
        let mut depth = 0i32;
        let mut parts = vec![Vec::new()];
        let mut closed_at_end = true;
        let mut either = false;
        for (at, word) in words.iter().enumerate() {
            match *word {
                "(" => depth += 1,
                ")" => {
                    depth -= 1;
                    closed_at_end &= depth > 0 || at == words.len() - 1;
                }
                "and" if depth == 0 => {
                    parts.push(Vec::new());
                    continue;
                }
                "or" if depth == 0 => either = true,
                _ => {}
            }
            parts.last_mut().expect("one at least").push(*word);
        }
        if either {
            continue;
        }
        if parts.len() > 1 {
            terms.extend(parts.into_iter().map(|p| p.join(" ")));
        } else if words.first() == Some(&"(") && closed_at_end {
            terms.push(words[1..words.len() - 1].join(" "));
        }
    }
    false
}

/// Whether a trigger of this name is one the engine writes (and [`install`]
/// replaces): a host's own may be named anything else, and do anything —
/// read the engine's tables included.
#[cfg(feature = "sqlite")]
fn engines(name: &str) -> bool {
    name.starts_with("sync_note_") || name.starts_with("sync_refuse_")
}

/// Whether `name` is the engine's note trigger of a table this build does
/// not know: one a later build wrote, which an earlier one keeps — what
/// the host writes while it runs is noted, and set aside until the later
/// build runs again ([`Replica::new`]). A table that no longer travels for
/// good is the host's to drop the trigger of.
///
/// [`Replica::new`]: crate::run::replica::Replica::new
#[cfg(feature = "sqlite")]
fn unknown<P: Sync + 'static>(codecs: &[&Codec<P>], name: &str, table: &str) -> bool {
    name.starts_with("sync_note_")
        && !codecs
            .iter()
            .any(|c| c.table.name.eq_ignore_ascii_case(table))
}

/// Refuses a connection the engine cannot keep its word on, each setting of
/// which is the connection's own — so each one a message is applied through
/// is asked: commits that are not durable (`synchronous` below `FULL`), since
/// a message is acknowledged once its commit returns; a journal that cannot
/// roll a transaction back (`journal_mode` `OFF` or `MEMORY`), since a
/// message is applied whole or not at all (a database with no file, in memory
/// or private to the connection, has no crash to survive, and is always in
/// memory); and triggers that do not fire on
/// the rows a conflict replaces (`recursive_triggers` off), since a row an
/// `INSERT OR REPLACE` deletes would then never go up as gone.
#[cfg(feature = "sqlite")]
pub(crate) async fn settled(conn: &mut Conn) -> Result<(), Error> {
    let synchronous: i64 = sqlx::query_scalar("PRAGMA synchronous")
        .fetch_one(&mut *conn)
        .await?;
    let journal: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&mut *conn)
        .await?;
    let file: String =
        sqlx::query_scalar("SELECT file FROM pragma_database_list WHERE name = 'main'")
            .fetch_one(&mut *conn)
            .await?;
    let recursive: i64 = sqlx::query_scalar("PRAGMA recursive_triggers")
        .fetch_one(&mut *conn)
        .await?;
    let refused = if synchronous < 2 {
        "its commits are not durable (`synchronous` below FULL): a message acknowledged could be lost"
    } else if !file.is_empty() && ["off", "memory"].contains(&journal.to_lowercase().as_str()) {
        "it cannot roll a transaction back after a crash (`journal_mode` OFF or MEMORY): a message could be half applied"
    } else if recursive == 0 {
        "its triggers miss the rows a conflict replaces (`recursive_triggers` off): a row an \
         INSERT OR REPLACE deletes would never go up as gone — set it ON in the pool's options"
    } else {
        return Ok(());
    };
    Err(Error::Misuse(format!(
        "the database's connection: {refused}"
    )))
}

/// Brings a replica database to what this build runs against, in one
/// transaction (`BEGIN IMMEDIATE`, so no write slips in half-way): the
/// engine's tables migrated from whatever version they are at, and every
/// trigger [`triggers`] writes, in place of the engine's own it had — but
/// the note triggers of a table this build does not know, which it keeps:
/// a later build wrote them, and runs again. Then
/// [`check`]s it. For a host with no migrations of its own; run it before
/// anything writes, on a connection with no transaction open. Idempotent.
///
/// # Errors
///
/// [`Error::Database`] when the database fails (the transaction is rolled back,
/// and nothing is installed), and [`Error::Misuse`] when the engine's tables
/// are at a later version than this build's, or [`check`] refuses what results.
#[cfg(feature = "sqlite")]
pub async fn install<P: Sync + 'static>(conn: &mut Conn, codecs: &[Codec<P>]) -> Result<(), Error> {
    let wishes = crate::wish::engines(codecs)?;
    let all: &[&Codec<P>] = &codecs.iter().chain(&wishes).collect::<Vec<_>>();
    let mut tx = sqlx::Connection::begin_with(&mut *conn, "BEGIN IMMEDIATE").await?;
    let version = schema_version(&mut tx).await?;
    if version > crate::REPLICA_SCHEMA_VERSION {
        return Err(Error::Misuse(format!(
            "the engine's tables are version {version}, later than this build's {}",
            crate::REPLICA_SCHEMA_VERSION
        )));
    }
    for step in version + 1..=crate::REPLICA_SCHEMA_VERSION {
        let script = migration(step).expect("a step of this build");
        sqlx::Executor::execute(&mut *tx, script.as_str()).await?;
    }
    let live: Vec<(String, String)> =
        sqlx::query_as("SELECT name, tbl_name FROM sqlite_master WHERE type = 'trigger'")
            .fetch_all(&mut *tx)
            .await?;
    let ours = live
        .into_iter()
        .filter(|(name, table)| engines(name) && !unknown(all, name, table));
    for (name, _) in ours {
        sqlx::query(&format!("DROP TRIGGER {}", ident(&name)))
            .execute(&mut *tx)
            .await?;
    }
    for trigger in generated(all) {
        sqlx::query(&trigger.sql).execute(&mut *tx).await?;
    }
    checked(&mut tx, all).await?;
    tx.commit().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// What the server sends.

/// What a message left the replica holding, of one table.
#[cfg(feature = "sqlite")]
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Applied {
    /// The table.
    pub table: String,
    /// The keys of its rows the message carried that the replica now holds.
    pub keys: Vec<Key>,
}

/// What a message from the server did. Every one of these is acknowledged to
/// the transport once the transaction it was acted on in commits.
#[cfg(feature = "sqlite")]
#[derive(Debug)]
#[non_exhaustive]
pub enum Received {
    /// Rows applied: per table, the keys the replica now holds.
    Applied(Vec<Applied>),
    /// A snapshot ended, and what it did not name went.
    SnapshotEnded,
    /// Uploads the server filed: their notes are done.
    Filed,
    /// A message at or below the last one applied from the same server: a
    /// redelivery, one a relay that lost its lock sent late, or one a server
    /// restored from an older copy numbered again. Nothing done; the server
    /// is told again what was applied.
    AlreadyApplied,
    /// A message that follows one this replica never applied — it missed
    /// one — or any but the beginning of a snapshot once it has. Nothing
    /// done, now or until a snapshot begins; the server is told, and
    /// provisions it again.
    Gap,
    /// Bytes this build can do nothing with, and never will — no message, a
    /// message with nothing it knows in it, a sequence or an epoch no server
    /// sends — logged and dropped. The digests find what they carried, if
    /// it mattered.
    Malformed(String),
}

/// A message from the server, as the transport delivered it — one that
/// carries more rows, or more tables or uploads, than `limits` is malformed
/// — acted on in the caller's transaction — one begun with `BEGIN IMMEDIATE`, so that what it
/// reads cannot change before it writes. The caller commits, then
/// acknowledges the message to the transport — never before: a message
/// acknowledged and not committed would never come again.
///
/// An error means the message must come again: the replica's own storage
/// failed, or the message speaks a later protocol than this build
/// ([`Error::Protocol`]) and waits for it to be updated. Nothing a message
/// carries is an error: a row it cannot take is rejected, a message it
/// cannot read is [`Received::Malformed`] — and a row whose refusal rolled
/// the whole transaction back, or that a foreign key checked at commit
/// refuses (its parent is not held, or rows point at what it changed), stops
/// it with its [`Culprit`], for the caller to apply the message again,
/// `care`fully, with that row refused. A commit the database refuses anyway
/// is the caller's to answer by applying the message with more [`Care`]:
/// carefully, which finds the row, and then whole, every row refused.
#[cfg(feature = "sqlite")]
pub(crate) async fn receive<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    bytes: &[u8],
    limits: &envelope::Limits,
    care: &Care,
) -> Result<Received, Stop> {
    let message = match ToReplica::decode(bytes) {
        Ok(message) => message,
        Err(e) => return Ok(malformed(format!("bytes that are no message: {e}"))),
    };
    if message.protocol > envelope::PROTOCOL {
        return Err(Stop::Failed(Error::Protocol {
            theirs: message.protocol,
            ours: envelope::PROTOCOL,
        }));
    }
    if message.protocol == 0 {
        return Ok(malformed("a message that names no protocol".to_owned()));
    }
    let Some(body) = message.body else {
        return Ok(malformed(
            "a message with nothing this build knows in it".to_owned(),
        ));
    };
    if let Some(why) = envelope::oversized(&body, limits) {
        return Ok(malformed(why));
    }
    sqlx::query("INSERT OR IGNORE INTO sync_applying (id) VALUES (1)")
        .execute(&mut *conn)
        .await?;
    let received = if care.careful() {
        carefully(conn, codecs, body, care).await
    } else {
        act(conn, codecs, body, care).await
    };
    let quiet = sqlx::query("DELETE FROM sync_applying")
        .execute(&mut *conn)
        .await;
    let received = received?;
    quiet?;
    Ok(received)
}

/// Why applying a message stopped short: the replica failed, or rows'
/// refusal would take the whole transaction with it — one that did, or rows
/// a key checked at commit would refuse.
#[cfg(feature = "sqlite")]
#[derive(Debug)]
pub(crate) enum Stop {
    Failed(Error),
    Culprits(Vec<Culprit>),
}

#[cfg(feature = "sqlite")]
impl From<Error> for Stop {
    fn from(e: Error) -> Self {
        Stop::Failed(e)
    }
}

#[cfg(feature = "sqlite")]
impl From<sqlx::Error> for Stop {
    fn from(e: sqlx::Error) -> Self {
        Stop::Failed(e.into())
    }
}

/// A row whose refusal rolled back the whole transaction it was applied in
/// — a trigger's `RAISE(ROLLBACK)`, a constraint declared `ON CONFLICT
/// ROLLBACK` — so that the message can be applied again without it.
#[cfg(feature = "sqlite")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Culprit {
    table: String,
    row: Spot,
    why: String,
}

/// Where a row is in a message: the upsert at `at` of its `entry`th table,
/// or a key it lets go of (a delete, a sweep, a let-go once filed).
#[cfg(feature = "sqlite")]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Spot {
    Upsert { entry: usize, at: usize },
    LetGo(String),
}

/// How a message is applied: all at once; or — once a refusal rolled a
/// whole transaction back — `Careful`ly, row by row, each in a savepoint of
/// its own, and the rows that did so refused before they are tried; or —
/// once the database refuses it for what no row of it explains — `Whole`,
/// every row of it refused, so that what the message says of where the
/// replica stands is taken all the same.
#[cfg(feature = "sqlite")]
#[derive(Debug, Default)]
pub(crate) struct Care {
    level: Level,
    culprits: HashMap<(String, Spot), Culprit>,
}

#[cfg(feature = "sqlite")]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Level {
    #[default]
    Bold,
    Careful,
    Whole,
}

/// Why every row of a message is refused, once nothing else took.
#[cfg(feature = "sqlite")]
const WHOLE: &str =
    "the database refused the message it came in, for what no row of it alone explains";

#[cfg(feature = "sqlite")]
impl Care {
    fn careful(&self) -> bool {
        self.level >= Level::Careful
    }

    /// The database refused the message as applied: the next way to apply
    /// it, row by row, then whole. Answers whether there is one — once
    /// every row of it is refused, the database refuses something no
    /// message changes.
    pub(crate) fn heed(&mut self) -> bool {
        let next = match self.level {
            Level::Bold => Level::Careful,
            Level::Careful => Level::Whole,
            Level::Whole => return false,
        };
        self.level = next;
        true
    }

    /// Refuses `culprits` before they are tried, next time, all at once:
    /// answers whether any is news — one refused already is not what
    /// failed.
    pub(crate) fn refuse(&mut self, culprits: Vec<Culprit>) -> bool {
        self.level = self.level.max(Level::Careful);
        let mut news = false;
        for culprit in culprits {
            let place = (culprit.table.clone(), culprit.row.clone());
            if self.culprits.contains_key(&place) {
                continue;
            }
            tracing::error!(
                table = culprit.table,
                "sync: a row the database would refuse the message for — applied again without it: {}",
                culprit.why
            );
            self.culprits.insert(place, culprit);
            news = true;
        }
        news
    }

    /// Why the row at `row` of `table` is refused before it is tried, if
    /// it is.
    fn culprit(&self, table: &str, row: &Spot) -> Option<&str> {
        if self.level == Level::Whole {
            return Some(WHOLE);
        }
        self.culprits
            .get(&(table.to_owned(), row.clone()))
            .map(|c| c.why.as_str())
    }
}

/// The savepoint one row's work runs in, careful.
#[cfg(feature = "sqlite")]
const ROW: &str = "sync_row";

/// Closes the savepoint one row's careful work ran in, as `result` went: a
/// refusal comes back as the inner error, the transaction going on without
/// the row; a refusal that took the whole transaction with it stops the
/// message, naming the row (`table`, `row`).
#[cfg(feature = "sqlite")]
async fn row_done<T>(
    conn: &mut Conn,
    result: Result<T, Stop>,
    table: &str,
    row: Spot,
) -> Result<Result<T, Error>, Stop> {
    let refusal = match result {
        Ok(value) => {
            sqlx::query(&format!("RELEASE SAVEPOINT {ROW}"))
                .execute(&mut *conn)
                .await?;
            return Ok(Ok(value));
        }
        Err(Stop::Failed(e)) if e.is_refusal() => e,
        Err(stop) => return Err(stop),
    };
    if sqlx::query(&format!("ROLLBACK TO SAVEPOINT {ROW}"))
        .execute(&mut *conn)
        .await
        .is_err()
    {
        return Err(Stop::Culprits(vec![Culprit {
            table: table.to_owned(),
            row,
            why: refusal.to_string(),
        }]));
    }
    sqlx::query(&format!("RELEASE SAVEPOINT {ROW}"))
        .execute(&mut *conn)
        .await?;
    Ok(Err(refusal))
}

/// Opens the savepoint [`row_done`] closes.
#[cfg(feature = "sqlite")]
async fn row_begun(conn: &mut Conn) -> Result<(), Error> {
    sqlx::query(&format!("SAVEPOINT {ROW}"))
        .execute(&mut *conn)
        .await?;
    Ok(())
}

#[cfg(feature = "sqlite")]
pub(crate) fn malformed(why: String) -> Received {
    tracing::error!("sync: a message this build can do nothing with — dropped: {why}");
    Received::Malformed(why)
}

/// `body` acted on carefully, once a refusal took a whole transaction with
/// it — the commit's own included: and then, since a foreign key checked at
/// commit refuses what a row of the message changed, or let go of, under
/// rows that point at it, or a row of it that points at one the replica
/// does not hold, the rows that did so are found, to be refused — by the
/// database's own check of its keys. What each row pointing at the server's
/// tables points at is recorded before, and read again after.
#[cfg(feature = "sqlite")]
async fn carefully<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    body: Body,
    care: &Care,
) -> Result<Received, Stop> {
    let mut upserts: HashMap<(&str, Key), Spot> = HashMap::new();
    if let Body::Down(down) = &body {
        for (entry, table) in down.tables.iter().enumerate() {
            let Some(codec) = codec_of(codecs, &table.table).filter(|c| c.sent_down()) else {
                continue;
            };
            for (at, key) in parsed(codec, &table.keys).into_iter().enumerate() {
                if let Some(key) = key {
                    upserts.insert((codec.table.name, key), Spot::Upsert { entry, at });
                }
            }
        }
    }
    // Any of the server's tables may change: a message's rows, the end of a
    // snapshot, a let-go once filed.
    let mut before = Vec::new();
    let mut recorded = 0;
    for codec in codecs.iter().filter(|c| c.sent_down()) {
        let pointing = codec.pointing(conn, recorded).await?;
        recorded += pointing.len();
        before.push((codec, pointing));
    }
    let received = act(conn, codecs, body, care).await?;
    let mut culprits = Vec::new();
    let why = "rows here point at what it changes, through a key checked at commit";
    for (codec, pointing) in &before {
        let table = codec.table.name;
        let broken = codec.broken(conn, pointing).await?;
        // One still held was changed by its upsert; one gone, let go of.
        let held: HashSet<Key> = codec.present(conn, &broken).await?.into_iter().collect();
        for key in broken {
            let row = held
                .contains(&key)
                .then(|| upserts.remove(&(table, key.clone())))
                .flatten()
                .unwrap_or_else(|| Spot::LetGo(key_text(&key)));
            culprits.push(Culprit {
                table: table.to_owned(),
                row,
                why: why.to_owned(),
            });
        }
        crate::row::forget(conn, pointing).await?;
        // A row of the message pointing at one it let go of is the let-go's
        // to answer for, found first.
        if !culprits.is_empty() {
            continue;
        }
        let mine: Vec<Key> = upserts
            .keys()
            .filter(|(t, _)| *t == table)
            .map(|(_, key)| key.clone())
            .collect();
        for key in codec.orphaned(conn, &mine).await? {
            let row = upserts.remove(&(table, key)).expect("one of the message's");
            culprits.push(Culprit {
                table: table.to_owned(),
                row,
                why: "a row it points at, through a foreign key, is not held here".to_owned(),
            });
        }
    }
    if culprits.is_empty() {
        Ok(received)
    } else {
        Err(Stop::Culprits(culprits))
    }
}

#[cfg(feature = "sqlite")]
async fn act<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    body: Body,
    care: &Care,
) -> Result<Received, Stop> {
    match body {
        Body::Filed(message) => {
            filed(conn, codecs, &message, care).await?;
            Ok(Received::Filed)
        }
        Body::Down(down) => {
            if let Some(table) = down.tables.iter().find(|t| t.keys.len() != t.upserts.len()) {
                return Ok(malformed(format!(
                    "`{}`: {} rows and {} keys beside them",
                    capped(&table.table),
                    table.upserts.len(),
                    table.keys.len()
                )));
            }
            let place = Place {
                epoch: &down.epoch,
                sequence: down.sequence,
                nonce: down.nonce,
                mac: &down.mac,
                previous: (down.previous, down.previous_nonce),
                snapshot: &down.snapshot_id,
                ends: false,
            };
            match chained(conn, place).await? {
                Ok(by) => {
                    if down.previous == 0 {
                        restate(conn, codecs).await?;
                    }
                    Ok(Received::Applied(
                        apply(conn, codecs, &down, &by, care).await?,
                    ))
                }
                Err(skipped) => Ok(skipped),
            }
        }
        Body::EndSnapshot(end) => {
            let place = Place {
                epoch: &end.epoch,
                sequence: end.sequence,
                nonce: end.nonce,
                mac: &end.mac,
                previous: (end.previous, end.previous_nonce),
                snapshot: &end.snapshot_id,
                ends: true,
            };
            match chained(conn, place).await? {
                Ok(by) => {
                    if end.previous == 0 {
                        restate(conn, codecs).await?;
                    }
                    end_snapshot(conn, codecs, &end, &by, care).await?;
                    Ok(Received::SnapshotEnded)
                }
                Err(skipped) => Ok(skipped),
            }
        }
    }
}

/// Where a message stands in what the server sends this replica.
#[cfg(feature = "sqlite")]
struct Place<'m> {
    epoch: &'m str,
    sequence: u64,
    nonce: u64,
    mac: &'m [u8],
    /// The message it follows: its sequence and nonce.
    previous: (u64, u64),
    snapshot: &'m str,
    /// It ends the snapshot it names.
    ends: bool,
}

/// How long a server's seal is: the replica keeps it, and gives it back in
/// every confirmation, so one of any other length is no server's.
#[cfg(feature = "sqlite")]
const SEALED: usize = 16;

/// The message a row was refused by — the one that carried it, or let go of
/// it — which the server reads the rejection against.
#[cfg(feature = "sqlite")]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct By {
    epoch: String,
    sequence: i64,
}

/// The last message this replica applied, and where it stands.
#[cfg(feature = "sqlite")]
#[derive(sqlx::FromRow)]
struct Last {
    epoch: String,
    sequence: i64,
    nonce: i64,
    snapshot_id: Option<String>,
    gap: i64,
}

/// Whether the message at `place` is the next one to apply, recording it as
/// the last applied when it is: its sequence when it is, and otherwise what
/// it is instead. The next one begins a snapshot (`previous` 0), whatever
/// came before — of this epoch or another, which is a server installed
/// again — or follows the last applied: its sequence and its nonce. One
/// that follows anything else is a gap, and so is every one after it until
/// a snapshot begins: nothing is applied meanwhile. One at or below the last
/// applied of its epoch is a redelivery, or a server restored from an older
/// copy numbering again; either way the server is told again what was
/// applied, which tells a restored server what it no longer knows.
#[cfg(feature = "sqlite")]
async fn chained(conn: &mut Conn, place: Place<'_>) -> Result<Result<By, Received>, Error> {
    // 0 too is outside: nothing is before it.
    let Ok(sequence) = i64::try_from(place.sequence) else {
        return Ok(Err(malformed(format!(
            "sequence {}, outside 1 to 2^63 - 1",
            place.sequence
        ))));
    };
    let Ok(epoch) = Uuid::parse_str(place.epoch).map(|e| e.to_string()) else {
        return Ok(Err(malformed(format!(
            "epoch `{}` is not a uuid",
            capped(place.epoch)
        ))));
    };
    let snapshot = if place.snapshot.is_empty() {
        Ok(String::new())
    } else {
        Uuid::parse_str(place.snapshot).map(|id| id.to_string())
    };
    let Ok(snapshot) = snapshot else {
        return Ok(Err(malformed(format!(
            "snapshot `{}` is not a uuid",
            capped(place.snapshot)
        ))));
    };
    let Some(previous) = i64::try_from(place.previous.0)
        .ok()
        .filter(|p| *p < sequence)
    else {
        return Ok(Err(malformed(format!(
            "message {sequence} follows {}, which is not before it",
            place.previous.0
        ))));
    };
    let (Ok(nonce @ 1..), Ok(previous_nonce)) =
        (i64::try_from(place.nonce), i64::try_from(place.previous.1))
    else {
        return Ok(Err(malformed(format!(
            "message {sequence}: nonce {}, following nonce {}, outside 1 to 2^63 - 1",
            place.nonce, place.previous.1
        ))));
    };
    if (previous == 0) != (previous_nonce == 0) {
        return Ok(Err(malformed(format!(
            "message {sequence} follows {previous} of nonce {previous_nonce}"
        ))));
    }
    if previous == 0 && snapshot.is_empty() {
        return Ok(Err(malformed(
            "a message that follows nothing and begins no snapshot".to_owned(),
        )));
    }
    if place.mac.len() != SEALED {
        return Ok(Err(malformed(format!(
            "message {sequence} sealed with {} bytes, not {SEALED}",
            place.mac.len()
        ))));
    }
    let last: Option<Last> = sqlx::query_as(
        "SELECT epoch, sequence, nonce, snapshot_id, gap FROM sync_applied WHERE id = 1",
    )
    .fetch_optional(&mut *conn)
    .await?;
    if last
        .as_ref()
        .is_some_and(|l| l.epoch == epoch && sequence <= l.sequence)
    {
        tracing::info!(
            sequence,
            "sync: a message at or below the last applied — nothing to do"
        );
        sqlx::query("UPDATE sync_applied SET acknowledged = 0 WHERE id = 1")
            .execute(&mut *conn)
            .await?;
        return Ok(Err(Received::AlreadyApplied));
    }
    if previous == 0 {
        if last.as_ref().is_some_and(|l| l.epoch != epoch) {
            // A server installed again, which knows nothing it was told.
            sqlx::query("DELETE FROM sync_backlog")
                .execute(&mut *conn)
                .await?;
        }
        sqlx::query("DELETE FROM sync_snapshot_keys")
            .execute(&mut *conn)
            .await?;
        sqlx::query(
            "INSERT INTO sync_applied (id, epoch, sequence, nonce, mac, snapshot_id, snapshot_began) \
             VALUES (1, $1, $2, $3, $4, $5, $2) \
             ON CONFLICT (id) DO UPDATE SET epoch = EXCLUDED.epoch, sequence = EXCLUDED.sequence, \
               nonce = EXCLUDED.nonce, mac = EXCLUDED.mac, \
               snapshot_id = EXCLUDED.snapshot_id, snapshot_began = EXCLUDED.snapshot_began, \
               gap = 0, gap_told = 0, \
               acknowledged = CASE WHEN sync_applied.epoch = EXCLUDED.epoch \
                                   THEN sync_applied.acknowledged ELSE 0 END",
        )
        .bind(&epoch)
        .bind(sequence)
        .bind(nonce)
        .bind(place.mac)
        .bind(&snapshot)
        .execute(&mut *conn)
        .await?;
        return Ok(Ok(By { epoch, sequence }));
    }
    let follows = last.as_ref().is_some_and(|l| {
        l.gap == 0 && l.epoch == epoch && l.sequence == previous && l.nonce == previous_nonce
    });
    let Some(Last { snapshot_id, .. }) = last.filter(|_| follows) else {
        tracing::warn!(
            sequence,
            previous,
            "sync: a message that follows one this replica never applied — it missed one, \
             and waits to be provisioned again"
        );
        sqlx::query(
            "INSERT INTO sync_applied (id, epoch, sequence, gap) VALUES (1, '', 0, $1) \
             ON CONFLICT (id) DO UPDATE SET gap = max(gap, EXCLUDED.gap)",
        )
        .bind(sequence)
        .execute(&mut *conn)
        .await?;
        return Ok(Err(Received::Gap));
    };
    let named = (!snapshot.is_empty() || place.ends).then_some(snapshot.as_str());
    if named != snapshot_id.as_deref() {
        return Ok(Err(malformed(format!(
            "message {sequence} of snapshot `{}`, while `{}` is in progress",
            snapshot,
            snapshot_id.unwrap_or_default()
        ))));
    }
    sqlx::query("UPDATE sync_applied SET sequence = $1, nonce = $2, mac = $3 WHERE id = 1")
        .bind(sequence)
        .bind(nonce)
        .bind(place.mac)
        .execute(&mut *conn)
        .await?;
    Ok(Ok(By { epoch, sequence }))
}

/// `keys` as one JSON array, for a statement to read through `json_each`
/// with one bind however many there are.
#[cfg(feature = "sqlite")]
fn json<T: serde::Serialize>(keys: &[T]) -> String {
    serde_json::to_string(keys).expect("strings and numbers serialize")
}

/// Rows to hold and rows to let go, in the order the server put them.
///
/// A row the replica cannot take — of a table this build does not mirror or
/// that the server does not send, one that does not decode, one the database
/// refuses for its data — and a row it cannot let go of — one the database
/// refuses to delete, or that a row of the replica's own points at in a table
/// with nothing to retire it by — is left as it was and noted as rejected,
/// for [`outbox`] to report, rather than holding back the queue behind it; a
/// storage failure is the error. A row refused is tried again once the rest
/// of its part is in, and once the whole message is, while that takes one
/// more: what it points at, or the value it takes, may come later in the
/// message. A row the replica takes or lets go of is no longer one to
/// report. A row of its own it [`deleted`] is not taken until the server
/// filed the delete: it is reported held back.
#[cfg(feature = "sqlite")]
async fn apply<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    message: &Down,
    by: &By,
    care: &Care,
) -> Result<Vec<Applied>, Stop> {
    let naming = !message.snapshot_id.is_empty();
    let mut parts = Vec::new();
    let foreign = foreign_rows(codecs, &message.tables);
    for (entry, table) in message.tables.iter().enumerate() {
        if let Some(codec) = codec_of(codecs, &table.table)
            .filter(|c| naming && c.table.mirror == crate::row::Mirror::Setting)
        {
            gone_here(conn, codec, &keys_of(codec, &table.recorded)).await?;
        }
        let Some(codec) = codec_of(codecs, &table.table).filter(|c| c.sent_down()) else {
            continue;
        };
        let deletes = keys_of(codec, &table.deletes);
        let beside = parsed(codec, &table.keys);
        // A row refused earlier in the message and let go of now is the
        // server's to let go, and one sent again now is this part's: it is
        // not tried again.
        for part in parts
            .iter_mut()
            .filter(|p: &&mut Part<P>| p.codec.table.name == codec.table.name)
        {
            let refused = std::mem::take(&mut part.refused);
            let (gone, kept): (Vec<_>, Vec<_>) = refused
                .into_iter()
                .filter(|(at, _)| {
                    part.beside[*at]
                        .as_ref()
                        .is_none_or(|k| !beside.contains(&Some(k.clone())))
                })
                .partition(|(at, _)| {
                    part.beside[*at]
                        .as_ref()
                        .is_some_and(|k| deletes.contains(k))
                });
            part.refused = kept;
            if naming {
                let keys: Vec<Key> = gone
                    .iter()
                    .filter_map(|(at, _)| part.beside[*at].clone())
                    .collect();
                let held = codec.present(conn, &keys).await?;
                named(
                    conn,
                    codec.table.name,
                    &held.iter().map(key_text).collect::<Vec<_>>(),
                )
                .await?;
            }
        }
        let_go(conn, codecs, codec, &deletes, by, care).await?;
        // A key twice in a part: the last row beside it is the one.
        let mut last = HashMap::new();
        for (at, key) in beside.iter().enumerate() {
            if let Some(key) = key {
                last.insert(key, at);
            }
        }
        let deleted = deleted(conn, codec, &beside).await?;
        let held_back: Vec<String> = deleted.iter().map(key_text).collect();
        if !held_back.is_empty() {
            reject(conn, codec.table.name, &held_back, HELD_BACK, true, by).await?;
        }
        let mut pending = Vec::new();
        for at in (0..beside.len()).filter(|at| {
            beside[*at]
                .as_ref()
                .is_none_or(|k| last[k] == *at && !deleted.contains(k))
        }) {
            match care.culprit(&table.table, &Spot::Upsert { entry, at }) {
                Some(why) => {
                    let key = beside[at].as_ref();
                    refuse(conn, codec, key, why, naming, by).await?;
                }
                None => pending.push(at),
            }
        }
        let mut part = Part {
            entry,
            codec,
            table,
            beside,
            held: Vec::new(),
            refused: Vec::new(),
        };
        part.attempt(conn, &pending, naming, care).await?;
        while part.again(conn, naming, care).await? {}
        parts.push(part);
    }
    loop {
        let mut took = false;
        for part in &mut parts {
            took |= part.again(conn, naming, care).await?;
        }
        if !took {
            break;
        }
    }
    for (table, keys) in &foreign {
        let why = if codec_of(codecs, table).is_some() {
            "a table the server does not send"
        } else {
            "a table this replica does not mirror"
        };
        reject(conn, table, keys, why, false, by).await?;
    }
    let mut taken = Vec::new();
    let mut culprits = Vec::new();
    for part in parts {
        for (at, why) in &part.refused {
            let key = part.beside[*at].as_ref();
            refuse(conn, part.codec, key, why, naming, by).await?;
        }
        let keys: Vec<Key> = part.held.iter().map(|(_, key)| key.clone()).collect();
        let orphans: HashSet<Key> = part
            .codec
            .unparented(conn, &keys)
            .await?
            .into_iter()
            .collect();
        culprits.extend(
            part.held
                .iter()
                .filter(|(_, key)| orphans.contains(key))
                .map(|(at, _)| Culprit {
                    table: part.codec.table.name.to_owned(),
                    row: Spot::Upsert {
                        entry: part.entry,
                        at: *at,
                    },
                    why: "a row it points at, through a foreign key, is not held here".to_owned(),
                }),
        );
        taken.push(Applied {
            table: part.table.table.clone(),
            keys,
        });
    }
    if !culprits.is_empty() {
        return Err(Stop::Culprits(culprits));
    }
    Ok(taken)
}

/// Why a row of its own the replica deleted is not taken while the server
/// has not filed the delete.
#[cfg(feature = "sqlite")]
const HELD_BACK: &str = "deleted here, and the delete not filed yet";

/// The rows of `keys` the replica deleted of its own, in a table keyed by
/// it, whose deletes the server has not filed: one it sends meanwhile it
/// sent before it heard of the delete, and sends again once it has, should
/// it keep the row.
#[cfg(feature = "sqlite")]
async fn deleted<P: Sync + 'static>(
    conn: &mut Conn,
    codec: &Codec<P>,
    keys: &[Option<Key>],
) -> Result<HashSet<Key>, Error> {
    if !codec.takes_deletes() {
        return Ok(HashSet::new());
    }
    let keys: Vec<Key> = keys.iter().flatten().cloned().collect();
    let noted: HashSet<String> = sqlx::query_scalar(
        "SELECT row_key FROM sync_notes WHERE relation = $1 \
           AND row_key IN (SELECT value FROM json_each($2))",
    )
    .bind(codec.table.name)
    .bind(json(&keys.iter().map(key_text).collect::<Vec<_>>()))
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .collect();
    let noted: Vec<Key> = keys
        .into_iter()
        .filter(|key| noted.contains(&key_text(key)))
        .collect();
    let held: HashSet<Key> = codec.present(conn, &noted).await?.into_iter().collect();
    Ok(noted
        .into_iter()
        .filter(|key| !held.contains(key))
        .collect())
}

/// One table's part of a message, as far as it was taken: the rows held, by
/// their place in it with their keys, and the ones refused so far, with why.
#[cfg(feature = "sqlite")]
struct Part<'m, P: 'static> {
    entry: usize,
    codec: &'m Codec<P>,
    table: &'m Table,
    beside: Vec<Option<Key>>,
    held: Vec<(usize, Key)>,
    refused: Vec<(usize, String)>,
}

#[cfg(feature = "sqlite")]
impl<P: Sync + 'static> Part<'_, P> {
    /// The rows refused so far tried again: answers whether it took any.
    async fn again(&mut self, conn: &mut Conn, naming: bool, care: &Care) -> Result<bool, Stop> {
        let refused: Vec<usize> = std::mem::take(&mut self.refused)
            .into_iter()
            .map(|(at, _)| at)
            .collect();
        let held = self.held.len();
        self.attempt(conn, &refused, naming, care).await?;
        Ok(self.held.len() > held)
    }

    /// The rows at `positions` tried: all at once, or — careful — each in a
    /// savepoint of its own, so that one whose refusal takes the whole
    /// transaction with it stops the message, named.
    async fn attempt(
        &mut self,
        conn: &mut Conn,
        positions: &[usize],
        naming: bool,
        care: &Care,
    ) -> Result<(), Stop> {
        if positions.is_empty() {
            return Ok(());
        }
        if !care.careful() {
            let (held, refused) = take(
                conn,
                self.codec,
                self.table,
                &self.beside,
                positions,
                naming,
            )
            .await?;
            self.held.extend(held);
            self.refused.extend(refused);
            return Ok(());
        }
        for at in positions {
            row_begun(conn).await?;
            let one = std::slice::from_ref(at);
            let took = take(conn, self.codec, self.table, &self.beside, one, naming).await;
            let row = Spot::Upsert {
                entry: self.entry,
                at: *at,
            };
            match row_done(conn, took, &self.table.table, row).await? {
                Ok((held, refused)) => {
                    self.held.extend(held);
                    self.refused.extend(refused);
                }
                Err(e) => self.refused.push((*at, e.to_string())),
            }
        }
        Ok(())
    }
}

/// Text the replica keeps of what a server said, at most: a reason, the
/// name and a key of a table it does not know. A rejection always fits a
/// message up, so one never holds back the replica's confirmation; and what
/// either side logs of what the other sent.
const TEXT_CAP: usize = 512;

/// The rows of tables the replica does not take, keys per table however many
/// parts name it: rejected — and logged — once each.
#[cfg(feature = "sqlite")]
fn foreign_rows<'a, P: Sync + 'static>(
    codecs: &[Codec<P>],
    tables: &'a [Table],
) -> Vec<(&'a str, Vec<String>)> {
    let mut foreign: Vec<(&str, Vec<String>)> = Vec::new();
    for table in tables {
        if codec_of(codecs, &table.table).is_some_and(|c| c.sent_down()) {
            continue;
        }
        let keys = foreign_keys(table);
        match foreign.iter_mut().find(|(name, _)| *name == table.table) {
            Some((_, all)) => all.extend(keys),
            None => foreign.push((&table.table, keys)),
        }
    }
    foreign
}

/// The keys of rows of a table this build does not take, as the server
/// spelled them, to report: none when the name or a key is longer than any
/// table's — what no server sends is logged, not kept.
#[cfg(feature = "sqlite")]
fn foreign_keys(table: &Table) -> Vec<String> {
    let keys: Vec<String> = table.keys.iter().map(|k| k.parts.join("|")).collect();
    if table.table.len() > TEXT_CAP || keys.iter().any(|k| k.len() > TEXT_CAP) {
        tracing::error!(
            "sync: rows of a table named or keyed longer than {TEXT_CAP} bytes — not kept"
        );
        return Vec::new();
    }
    keys
}

/// `text`, cut to [`TEXT_CAP`] bytes at a character.
fn capped(text: &str) -> &str {
    let mut end = text.len().min(TEXT_CAP);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// The rows of `table` at `positions`, taken, each under the key it came
/// `beside`: the ones taken are no longer ones to report, nor to let go
/// once filed, and while a snapshot is `naming` them, it names each.
/// Answers the rows the replica now holds and the ones it could not take,
/// with why, each by its place in `table`.
#[cfg(feature = "sqlite")]
async fn take<P: Sync + 'static>(
    conn: &mut Conn,
    codec: &Codec<P>,
    table: &Table,
    beside: &[Option<Key>],
    positions: &[usize],
    naming: bool,
) -> Result<(Vec<(usize, Key)>, Vec<(usize, String)>), Stop> {
    let rows = positions
        .iter()
        .map(|at| (table.upserts[*at].clone(), beside[*at].clone()))
        .collect();
    let refused: Vec<(usize, String)> = codec
        .apply_down(conn, rows)
        .await?
        .into_iter()
        .map(|refused| (positions[refused.at], refused.reason))
        .collect();
    let held: Vec<(usize, Key)> = positions
        .iter()
        .filter(|at| !refused.iter().any(|(r, _)| r == *at))
        .filter_map(|at| Some((*at, beside[*at].clone()?)))
        .collect();
    let texts: Vec<String> = held.iter().map(|(_, key)| key_text(key)).collect();
    cleared(conn, &table.table, &texts).await?;
    sqlx::query(
        "DELETE FROM sync_leaving WHERE relation = $1 \
           AND row_key IN (SELECT value FROM json_each($2))",
    )
    .bind(&table.table)
    .bind(json(&texts))
    .execute(&mut *conn)
    .await?;
    if naming {
        named(conn, &table.table, &texts).await?;
    }
    Ok((held, refused))
}

/// A row of `codec`'s table the replica could not take (`why`): rejected,
/// and named by the snapshot in progress when the replica holds a copy of
/// it.
#[cfg(feature = "sqlite")]
async fn refuse<P: Sync + 'static>(
    conn: &mut Conn,
    codec: &Codec<P>,
    key: Option<&Key>,
    why: &str,
    naming: bool,
    by: &By,
) -> Result<(), Error> {
    let table = codec.table.name;
    reject(
        conn,
        table,
        &Vec::from_iter(key.map(key_text)),
        why,
        false,
        by,
    )
    .await?;
    if naming {
        let held = codec.present(conn, &Vec::from_iter(key.cloned())).await?;
        named(conn, table, &held.iter().map(key_text).collect::<Vec<_>>()).await?;
    }
    Ok(())
}

/// Rows of `table` the snapshot in progress names: a refused one keeps what
/// the replica had of it, and its rejection, and is not swept — so one it
/// does not hold is not named, and what is named is bounded by what it
/// holds.
#[cfg(feature = "sqlite")]
async fn named(conn: &mut Conn, table: &str, keys: &[String]) -> Result<(), Error> {
    sqlx::query(
        "INSERT OR IGNORE INTO sync_snapshot_keys (table_name, row_key) \
         SELECT $1, value FROM json_each($2)",
    )
    .bind(table)
    .bind(json(keys))
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Lets go of the rows of `keys` ([`let_go_together`]) — carefully, one by
/// one, once a refusal rolled a whole transaction back: one the database
/// refuses to let go of alone is tried again once the others have gone, and
/// refused when that changes nothing; one whose refusal took the whole
/// transaction with it is refused before it is tried.
#[cfg(feature = "sqlite")]
async fn let_go<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    codec: &Codec<P>,
    keys: &[Key],
    by: &By,
    care: &Care,
) -> Result<(), Stop> {
    if !care.careful() {
        let_go_together(conn, codecs, codec, keys, by).await?;
        return Ok(());
    }
    let table = codec.table.name;
    let mut pending = Vec::new();
    for key in keys {
        match care.culprit(table, &Spot::LetGo(key_text(key))) {
            Some(why) => reject(conn, table, &[key_text(key)], why, false, by).await?,
            None => pending.push(key.clone()),
        }
    }
    loop {
        let mut again = Vec::new();
        for key in &pending {
            row_begun(conn).await?;
            let one = std::slice::from_ref(key);
            let gone = let_go_together(conn, codecs, codec, one, by).await;
            match row_done(conn, gone, table, Spot::LetGo(key_text(key))).await? {
                Ok(refused) => again.extend(refused),
                Err(e) => reject(conn, table, &[key_text(key)], &e.to_string(), false, by).await?,
            }
        }
        if again.is_empty() || again.len() == pending.len() {
            return Ok(());
        }
        pending = again;
    }
}

/// Lets go of the rows of `keys`, together ([`Codec::retire`]), as the
/// message `by` says: the ones the database refuses to let go are rejected
/// — and answered — the rest are no longer ones to report. A row of a table
/// written on both sides whose edits have not reached the server is held
/// retired until they have ([`filed`]), so the edits are not lost; and the
/// message that let go of it is kept with it, to reject it by should the
/// database refuse to let it go then.
#[cfg(feature = "sqlite")]
async fn let_go_together<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    codec: &Codec<P>,
    keys: &[Key],
    by: &By,
) -> Result<Vec<Key>, Stop> {
    let noted: HashSet<String> = if matches!(codec.table.mirror, crate::row::Mirror::Both) {
        sqlx::query_scalar(
            "SELECT row_key FROM sync_notes WHERE relation = $1 \
               AND row_key IN (SELECT value FROM json_each($2))",
        )
        .bind(codec.table.name)
        .bind(json(&keys.iter().map(key_text).collect::<Vec<_>>()))
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .collect()
    } else {
        HashSet::new()
    };
    let (held, free): (Vec<Key>, Vec<Key>) = keys
        .iter()
        .cloned()
        .partition(|key| noted.contains(&key_text(key)));
    let server: Vec<(&str, Option<&str>)> = codecs
        .iter()
        .filter(|c| c.sent_down())
        .map(|c| (c.table.name, c.table.retired))
        .collect();
    let retired = codec.retire(conn, &free, &held, &server).await?;
    let refused: HashSet<&Key> = retired.refused.iter().map(|(key, _)| key).collect();
    let leaving: Vec<String> = held
        .iter()
        .filter(|key| !refused.contains(key))
        .map(key_text)
        .collect();
    sqlx::query(
        "INSERT INTO sync_leaving (relation, row_key, epoch, sequence) \
         SELECT $1, value, $3, $4 FROM json_each($2) WHERE true \
         ON CONFLICT (relation, row_key) DO UPDATE SET epoch = EXCLUDED.epoch, \
           sequence = EXCLUDED.sequence",
    )
    .bind(codec.table.name)
    .bind(json(&leaving))
    .bind(&by.epoch)
    .bind(by.sequence)
    .execute(&mut *conn)
    .await?;
    for (key, why) in &retired.refused {
        reject(conn, codec.table.name, &[key_text(key)], why, false, by).await?;
    }
    let gone: Vec<String> = keys
        .iter()
        .filter(|key| !refused.contains(key))
        .map(key_text)
        .collect();
    cleared(conn, codec.table.name, &gone).await?;
    tracing::debug!(
        table = codec.table.name,
        deleted = retired.deleted,
        kept = retired.kept,
        refused = retired.refused.len(),
        "sync: rows let go of — the kept ones retired, still pointed at here or with edits on their way"
    );
    Ok(retired.refused.into_iter().map(|(key, _)| key).collect())
}

/// Notes rows the replica could not take, or let go of, for the server to
/// hear of: refused by the message `by` — or `held_back`, rows of its own
/// it deleted, not taken while the deletes are on their way, which is no
/// fault to log.
#[cfg(feature = "sqlite")]
async fn reject(
    conn: &mut Conn,
    table: &str,
    keys: &[String],
    why: &str,
    held_back: bool,
    by: &By,
) -> Result<(), Error> {
    if !held_back {
        tracing::error!(
            table = capped(table),
            rows = keys.len(),
            first = capped(keys.first().map_or("", String::as_str)),
            sequence = by.sequence,
            "sync: rows this replica cannot take or let go of — rejected, for the server to hear of: {}",
            capped(why)
        );
    }
    sqlx::query(
        "INSERT INTO sync_rejections (table_name, row_key, reason, epoch, sequence, held_back) \
         SELECT $1, value, $3, $5, $4, $6 FROM json_each($2) WHERE true \
         ON CONFLICT (table_name, row_key) DO UPDATE SET reason = EXCLUDED.reason, \
           epoch = EXCLUDED.epoch, sequence = EXCLUDED.sequence, held_back = EXCLUDED.held_back, \
           rejected_at = EXCLUDED.rejected_at, reported_at = NULL",
    )
    .bind(table)
    .bind(json(keys))
    .bind(capped(why))
    .bind(by.sequence)
    .bind(&by.epoch)
    .bind(held_back)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Rows no longer ones to report: taken, or let go of.
#[cfg(feature = "sqlite")]
async fn cleared(conn: &mut Conn, table: &str, keys: &[String]) -> Result<(), Error> {
    sqlx::query(
        "DELETE FROM sync_rejections WHERE table_name = $1 \
           AND row_key IN (SELECT value FROM json_each($2))",
    )
    .bind(table)
    .bind(json(keys))
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Lets go of the rows of the server's tables the replica retired that
/// nothing keeps any more — no row that stays points at them, live or
/// retired, of any table; no edit of them is on its way — as a delete
/// would, once a message is applied: a retired row stays only for what
/// needs it. It runs after the message commits, in a transaction of its
/// own, children first — the server's tables in reverse — so it never
/// makes a message be applied again; and should the database refuse it,
/// row by row, each in a transaction of its own, while that lets one more
/// go. One the database refuses to let go stays retired, and nobody is
/// told: the server let go of it already. A failure is the replica's
/// storage, and the next message collects again.
#[cfg(feature = "sqlite")]
pub(crate) async fn collect<P: Sync + 'static>(
    pool: &sqlx::SqlitePool,
    codecs: &[Codec<P>],
) -> Result<(), Error> {
    let server: Vec<&Codec<P>> = codecs.iter().filter(|c| c.sent_down()).rev().collect();
    let mut conn = pool.acquire().await?;
    let refusal = match collected(&mut conn, &server, None).await {
        Ok(_) => return Ok(()),
        Err(e) if e.is_refusal() => e,
        Err(e) => return Err(e),
    };
    // The database may have rolled the transaction back under it: a
    // connection that no longer knows whether it is in one goes.
    conn.close_on_drop();
    drop(conn);
    tracing::warn!("sync: retired rows refused to go together, let go of one by one: {refusal}");
    loop {
        let mut went = 0;
        for codec in &server {
            let keys = collectable(&mut *pool.acquire().await?, codec).await?;
            for key in &keys {
                let mut conn = pool.acquire().await?;
                match collected(&mut conn, &[codec], Some(key)).await {
                    Ok(gone) => went += gone,
                    Err(e) if e.is_refusal() => conn.close_on_drop(),
                    Err(e) => return Err(e),
                }
            }
        }
        if went == 0 {
            return Ok(());
        }
    }
}

/// In a transaction of its own, lets go of the retired rows of `codecs`'
/// tables nothing keeps — of `one` alone, when given, as it stands now: a
/// message applied since it was listed may have sent it again — and
/// commits: answers how many went.
#[cfg(feature = "sqlite")]
async fn collected<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[&Codec<P>],
    one: Option<&Key>,
) -> Result<u64, Error> {
    let mut tx = sqlx::Connection::begin_with(&mut *conn, "BEGIN IMMEDIATE").await?;
    // The engine's writes, which no trigger notes or refuses.
    sqlx::query("INSERT INTO sync_applying (id) VALUES (1)")
        .execute(&mut *tx)
        .await?;
    let mut gone = 0;
    for codec in codecs {
        let mut keys = collectable(&mut tx, codec).await?;
        if let Some(one) = one {
            keys.retain(|key| key == one);
        }
        gone += codec.collect(&mut tx, &keys).await?;
    }
    sqlx::query("DELETE FROM sync_applying")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(gone)
}

/// The rows of `codec`'s table the replica retired with no edit of them on
/// its way: the ones [`collect`] may let go of.
#[cfg(feature = "sqlite")]
async fn collectable<P: Sync + 'static>(
    conn: &mut Conn,
    codec: &Codec<P>,
) -> Result<Vec<Key>, Error> {
    let noted: HashSet<String> =
        sqlx::query_scalar("SELECT row_key FROM sync_notes WHERE relation = $1")
            .bind(codec.table.name)
            .fetch_all(&mut *conn)
            .await?
            .into_iter()
            .collect();
    Ok(codec
        .retired_keys(conn)
        .await?
        .into_iter()
        .filter(|key| !noted.contains(&key_text(key)))
        .collect())
}

/// The snapshot in progress is complete: every row of its tables it did not
/// name goes, children first — the server lists them parents first. What
/// was rejected in those tables before it began is forgotten: it sent again
/// what the replica should hold.
#[cfg(feature = "sqlite")]
async fn end_snapshot<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    message: &EndSnapshot,
    by: &By,
    care: &Care,
) -> Result<(), Stop> {
    let began: i64 = sqlx::query_scalar("SELECT snapshot_began FROM sync_applied WHERE id = 1")
        .fetch_one(&mut *conn)
        .await?;
    let mut seen = HashSet::new();
    let tables: Vec<&String> = message.tables.iter().filter(|t| seen.insert(*t)).collect();
    for table in tables.into_iter().rev() {
        sqlx::query(
            "DELETE FROM sync_rejections WHERE table_name = $1 \
               AND (sequence < $2 OR epoch <> (SELECT epoch FROM sync_applied WHERE id = 1))",
        )
        .bind(table)
        .bind(began)
        .execute(&mut *conn)
        .await?;
        let Some(codec) = codec_of(codecs, table).filter(|c| c.sent_down()) else {
            tracing::error!(
                table = capped(table),
                "sync: a snapshot of a table this replica does not hold from the server — not swept"
            );
            continue;
        };
        let named: HashSet<String> =
            sqlx::query_scalar("SELECT row_key FROM sync_snapshot_keys WHERE table_name = $1")
                .bind(table)
                .fetch_all(&mut *conn)
                .await?
                .into_iter()
                .collect();
        let stale: Vec<Key> = codec
            .keys(conn)
            .await?
            .into_iter()
            .filter(|key| !named.contains(&key_text(key)))
            .collect();
        let_go(conn, codecs, codec, &stale, by, care).await?;
    }
    // A table the replica holds from no server is in no snapshot, so none
    // clears what a server since gone made it refuse of it, and nothing
    // reports that: it goes with the server.
    let held: Vec<&str> = codecs
        .iter()
        .filter(|c| c.sent_down())
        .map(|c| c.table.name)
        .collect();
    sqlx::query(
        "DELETE FROM sync_rejections WHERE table_name NOT IN (SELECT value FROM json_each($1)) \
           AND epoch <> (SELECT epoch FROM sync_applied WHERE id = 1)",
    )
    .bind(json(&held))
    .execute(&mut *conn)
    .await?;
    sqlx::query("DELETE FROM sync_snapshot_keys")
        .execute(&mut *conn)
        .await?;
    sqlx::query("UPDATE sync_applied SET snapshot_id = NULL, snapshot_began = NULL WHERE id = 1")
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// The server filed these uploads: the notes their latest send carried are
/// done — unless the row was written since, which goes up again. A number
/// no upload of this replica has, or that a note was sent under again since,
/// names nothing: whether it was meant for another replica, or for this one
/// before it was restored. A row the server let go of, held for its edits,
/// is let go of once they are filed.
#[cfg(feature = "sqlite")]
async fn filed<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    message: &envelope::Filed,
    care: &Care,
) -> Result<(), Stop> {
    let uploads: Vec<i64> = message
        .uploads
        .iter()
        .filter_map(|u| i64::try_from(*u).ok())
        .collect();
    // A row written again since goes again, re-stated no more: the server
    // took it as it was re-stated, and its later writes are newer (7).
    for sql in [
        "DELETE FROM sync_notes WHERE upload IN (SELECT value FROM json_each($1)) \
            AND writes = sent",
        "UPDATE sync_notes SET restated = 0 WHERE upload IN (SELECT value FROM json_each($1))",
    ] {
        sqlx::query(sql)
            .bind(json(&uploads))
            .execute(&mut *conn)
            .await?;
    }
    let both: Vec<&str> = codecs
        .iter()
        .filter(|c| matches!(c.table.mirror, crate::row::Mirror::Both))
        .map(|c| c.table.name)
        .collect();
    let done: Vec<(String, String, String, i64)> = sqlx::query_as(
        "DELETE FROM sync_leaving AS l WHERE relation IN (SELECT value FROM json_each($1)) \
           AND NOT EXISTS (SELECT 1 FROM sync_notes n \
                            WHERE n.relation = l.relation AND n.row_key = l.row_key) \
         RETURNING relation, row_key, epoch, sequence",
    )
    .bind(json(&both))
    .fetch_all(&mut *conn)
    .await?;
    let mut by: std::collections::BTreeMap<(&str, By), Vec<Key>> = Default::default();
    for (table, key, epoch, sequence) in &done {
        let Some(codec) = codec_of(codecs, table) else {
            continue;
        };
        let parts: Vec<String> = key.split('|').map(str::to_owned).collect();
        if let Ok(key) = codec.key(&parts) {
            let message = By {
                epoch: epoch.clone(),
                sequence: *sequence,
            };
            by.entry((codec.table.name, message)).or_default().push(key);
        }
    }
    for ((table, message), keys) in by {
        let codec = codec_of(codecs, table).expect("a table of this build's");
        let_go(conn, codecs, codec, &keys, &message, care).await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// What goes up.

/// How far this replica is from having sent the server what it wrote, per
/// table: rows noted and not sent yet, rows sent the server has not said it
/// filed, and rows that cannot go as they are.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct UpProgress {
    /// The table.
    pub table: String,
    /// Rows written and not sent yet.
    pub unsent: i64,
    /// Rows sent that the server has not said it filed.
    pub unfiled: i64,
    /// Rows that cannot go up as they are, until a build that can send them.
    #[serde(default)]
    pub set_aside: i64,
}

/// A table's backlog as the replica stores what it last told the server
/// — its own shape, apart from [`UpProgress`]'s, which a host reads and a
/// later release may change.
#[cfg(feature = "sqlite")]
#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Told {
    table: String,
    unsent: u64,
    unfiled: u64,
    set_aside: u64,
}

/// [`UpProgress`] for every table with something on its way.
///
/// # Errors
///
/// [`Error::Database`] when the database fails.
#[cfg(feature = "sqlite")]
pub async fn progress(conn: &mut Conn) -> Result<Vec<UpProgress>, Error> {
    let rows: Vec<(String, i64, i64, i64)> = sqlx::query_as(
        "SELECT relation, \
                sum(set_aside IS NULL AND (sent_at IS NULL OR writes > sent)), \
                sum(set_aside IS NULL AND sent_at IS NOT NULL AND writes = sent), \
                sum(set_aside IS NOT NULL) \
           FROM sync_notes GROUP BY relation ORDER BY relation",
    )
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(table, unsent, unfiled, set_aside)| UpProgress {
            table,
            unsent,
            unfiled,
            set_aside,
        })
        .collect())
}

/// The backlog to tell the server, `now` being what it is: when it changed
/// since it was last told — with a message going anyway, when it empties or
/// starts, or when the last time is `backlog_every` ago — and, while it is
/// not empty, every `backlog_every` even unchanged. A clock set back since
/// it was last told counts as that long ago.
#[cfg(feature = "sqlite")]
async fn backlog_due(
    conn: &mut Conn,
    now: Vec<UpProgress>,
    settings: &Outbox,
    sending: bool,
) -> Result<Option<envelope::Backlog>, Error> {
    let stored: Option<(String, i64)> =
        sqlx::query_as("SELECT backlog, reported_at FROM sync_backlog WHERE id = 1")
            .fetch_optional(&mut *conn)
            .await?;
    // What it cannot read of what it told — stored by another build — it
    // never told.
    let (before, at) = match stored {
        Some((backlog, at)) => match serde_json::from_str::<Vec<Told>>(&backlog) {
            Ok(before) => (Some(before), at),
            Err(_) => (None, 0),
        },
        None => (None, 0),
    };
    let now = backlog_of(now);
    let every = i64::try_from(settings.backlog_every.as_millis()).unwrap_or(i64::MAX);
    let clock = chrono::Utc::now().timestamp_millis();
    let stale = clock < at || clock - at >= every;
    let unchanged = before.as_ref() == Some(&now.tables.iter().map(told).collect());
    let due = if unchanged {
        // Unchanged: said again now and then while not empty, should the
        // server have lost it.
        !now.tables.is_empty() && stale
    } else {
        let edge = now.tables.is_empty() || before.as_ref().is_none_or(Vec::is_empty);
        sending || edge || stale
    };
    Ok(due.then_some(now))
}

#[cfg(feature = "sqlite")]
fn told(table: &envelope::TableBacklog) -> Told {
    Told {
        table: table.table.clone(),
        unsent: table.unsent,
        unfiled: table.unfiled,
        set_aside: table.set_aside,
    }
}

#[cfg(feature = "sqlite")]
fn backlog_of(progress: Vec<UpProgress>) -> envelope::Backlog {
    let count = |n: i64| u64::try_from(n).unwrap_or_default();
    envelope::Backlog {
        tables: progress
            .into_iter()
            .map(|t| envelope::TableBacklog {
                table: t.table,
                unsent: count(t.unsent),
                unfiled: count(t.unfiled),
                set_aside: count(t.set_aside),
            })
            .collect(),
    }
}

/// Which of a replica's writes one stream of uploads carries. A replica may
/// keep a table that writes often (positions, events) on a lane of its own,
/// so it never waits behind the rest, nor the rest behind it.
#[cfg(feature = "sqlite")]
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Lane {
    /// The uplink's name for it.
    pub name: &'static str,
    /// The tables it carries; every table no other lane names when empty.
    pub tables: &'static [&'static str],
}

#[cfg(feature = "sqlite")]
impl Lane {
    /// The lane `name`, carrying `tables` — or, when empty, every table no
    /// other lane names.
    #[must_use]
    pub const fn new(name: &'static str, tables: &'static [&'static str]) -> Self {
        Lane { name, tables }
    }
}

/// What a replica sends up in one message, and how much: a host changes
/// what it needs of an `Outbox::default()`.
#[cfg(feature = "sqlite")]
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Outbox {
    /// How large a message the uplink takes: rows per message — the rows
    /// it carries and the rejections together — and bytes.
    pub limits: envelope::Limits,
    /// How long an upload the server has not answered waits before it goes
    /// again.
    pub resend_after: std::time::Duration,
    /// How often, at most, the replica tells the server its backlog while it
    /// grows — and how often it says it again, unchanged, while it is not
    /// empty; it says at once when it empties, or starts.
    pub backlog_every: std::time::Duration,
}

#[cfg(feature = "sqlite")]
impl Default for Outbox {
    fn default() -> Self {
        Outbox {
            limits: envelope::Limits::default(),
            resend_after: std::time::Duration::from_secs(600),
            backlog_every: std::time::Duration::from_secs(60),
        }
    }
}

/// Refuses lanes a replica cannot send up with: no lane, two of one name, a
/// table on two lanes or on none, a lane naming a table that does not go up,
/// or two lanes for every other table.
#[cfg(feature = "sqlite")]
pub(crate) fn check_settings<P: Sync + 'static>(
    codecs: &[Codec<P>],
    lanes: &[Lane],
) -> Result<(), Error> {
    let misuse = |why: String| Err(Error::Misuse(why));
    if lanes.is_empty() {
        return misuse("a replica needs a lane to send up on".into());
    }
    if lanes.iter().filter(|l| l.tables.is_empty()).count() > 1 {
        return misuse("one lane at most carries every table no other lane names".into());
    }
    let mut names = HashSet::new();
    if let Some(lane) = lanes.iter().find(|l| !names.insert(l.name)) {
        return misuse(format!("two lanes are named `{}`", lane.name));
    }
    let mut named = HashSet::new();
    for table in lanes.iter().flat_map(|l| l.tables.iter()) {
        if !named.insert(*table) {
            return misuse(format!("`{table}` is on two lanes"));
        }
        if !codec_of(codecs, table).is_some_and(Codec::goes_up) {
            return misuse(format!("a lane carries `{table}`, which does not go up"));
        }
    }
    if lanes.iter().all(|l| !l.tables.is_empty()) {
        // The engine's own tables go on the first lane, unless one names them.
        if let Some(codec) = codecs.iter().find(|c| {
            c.goes_up() && !named.contains(c.table.name) && c.table.name != crate::wish::TABLE
        }) {
            return misuse(format!("`{}` goes up on no lane", codec.table.name));
        }
    }
    Ok(())
}

/// The last message applied, as the replica tells the server of it, and
/// what it told already.
#[cfg(feature = "sqlite")]
#[derive(sqlx::FromRow)]
struct Applying {
    epoch: String,
    sequence: i64,
    nonce: i64,
    mac: Vec<u8>,
    acknowledged: i64,
    gap: i64,
    gap_told: i64,
}

/// A message on its way up, and what to record once the transport took it.
#[cfg(feature = "sqlite")]
#[derive(Debug)]
pub(crate) struct Outgoing {
    pub(crate) lane: &'static str,
    pub(crate) up: Up,
    /// The notes its rows carry.
    notes: Vec<i64>,
    /// Its rejections, each as refused by the message numbered so, and
    /// whether the replica keeps it once reported — of a row it holds a copy
    /// of, which a snapshot clears — or forgets it then.
    rejected: Vec<(String, String, i64, bool)>,
}

#[cfg(feature = "sqlite")]
impl Outgoing {
    /// As the bytes a transport carries.
    pub(crate) fn bytes(&self) -> Vec<u8> {
        self.up.encode_to_vec()
    }

    /// How many of the replica's writes, and of its rejections, it carries:
    /// what a transport that refuses it for good may refuse it for.
    pub(crate) fn items(&self) -> usize {
        self.notes.len() + self.rejected.len()
    }
}

/// Which of its rows and rejections a message carries when the transport
/// refused a larger one for good: `take` of them at most, after the first
/// `skip` that would otherwise go.
#[cfg(feature = "sqlite")]
#[derive(Debug, Clone, Copy)]
pub(crate) struct Window {
    pub(crate) take: usize,
    pub(crate) skip: usize,
}

/// What this replica has to tell the server on `lane` of `lanes`, as one
/// message within the outbox's limits: `None` when it has nothing. Rows it
/// wrote (as they are now), on every lane; on the first lane — the main one
/// — also the rows it could not take, the last message it applied when the
/// server has not heard of it, the backlog when it is due, and the
/// `digests` of what it holds, when given ([`held`]) — but of a table
/// written since they were read. Run it in a transaction begun with
/// `BEGIN IMMEDIATE`, and commit before the message goes: it reads all of
/// that as of one moment, and draws the upload's number. Hand the message
/// to the transport, then to [`delivered`] — or, when the transport refuses
/// it for good, ask again with a `window` on the rows and rejections it
/// carries, halving it until the one it refuses is alone: then to
/// [`refused`] — or, to see whether the transport takes any other, with the
/// window moved past it. The digests go only with the last message applied they were
/// read with, which goes only once every refusal of it is told, and only
/// when they fit; otherwise they wait.
///
/// Every byte counts against the transport's limit: what the message is
/// built with exactly, and what it takes by the end — its upload's number,
/// the backlog — at most.
///
/// A row that cannot go as it is — of a table this build does not send up, a
/// key it cannot read, a row that does not encode or is larger than a
/// message with nothing else in it — is set aside, and counted in the
/// backlog, until [`retry_set_aside`].
#[cfg(feature = "sqlite")]
pub(crate) async fn outbox<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    lanes: &[Lane],
    lane: usize,
    settings: &Outbox,
    digests: Option<Held>,
    window: Option<Window>,
) -> Result<Option<Outgoing>, Error> {
    let Some(this) = lanes.get(lane) else {
        return Err(Error::Misuse(format!("no lane {lane} of {}", lanes.len())));
    };
    let main = lane == 0;
    let mut up = Up {
        protocol: envelope::PROTOCOL,
        ..Up::default()
    };
    let limit = settings.limits.bytes;
    // What the message takes beyond what it is built with: its upload's
    // number (63 bits, nine bytes), and on the main lane the backlog.
    let mut reserved = framed(9);
    let mut rejected = Vec::new();
    let mut untold_gap = false;
    // Whether the last applied is news to the server.
    let mut news = false;
    // Rows and rejections it may carry, together, at most, after those it
    // passes over.
    let mut items = settings
        .limits
        .rows
        .min(window.map_or(usize::MAX, |w| w.take));
    let mut skip = window.map_or(0, |w| w.skip);
    // Room the backlog takes, while it is to go.
    let mut backlog = None;
    if main {
        let applied: Option<Applying> = sqlx::query_as(
            "SELECT epoch, sequence, nonce, mac, acknowledged, gap, gap_told FROM sync_applied \
              WHERE id = 1",
        )
        .fetch_optional(&mut *conn)
        .await?;
        // The last applied, and a gap, whether or not they go.
        reserved += applied
            .as_ref()
            .map_or(0, |a| framed(a.epoch.len()) + framed(a.mac.len()) + 3 * 11);
        // The backlog, whatever it says by the end — per table, its name
        // and three numbers — unless it could not go even alone.
        let tables: usize = progress(conn)
            .await?
            .iter()
            .map(|t| framed(framed(t.table.len()) + 3 * 11))
            .sum();
        if up.encoded_len() + reserved + framed(tables) <= limit {
            reserved += framed(tables);
            backlog = Some(framed(tables));
        } else {
            tracing::error!(
                bytes = framed(tables),
                limit,
                "sync: a backlog larger than a message carries — not told"
            );
        }
        // One over the cap says whether this message reports them all: the
        // server must hear of a refusal before the message it refused is
        // confirmed, or it would take the refused row for applied.
        let mut pending = unreported(conn, skip.saturating_add(items).saturating_add(1)).await?;
        let passed = skip.min(pending.len());
        pending.drain(..passed);
        skip -= passed;
        let mut all_reported = passed == 0 && pending.len() <= items;
        for (row, sequence) in pending.into_iter().take(items) {
            let weight = framed(row.encoded_len());
            // The first always fits, the backlog waiting for the next
            // message when it leaves it no room.
            if up.encoded_len() + reserved + weight > limit && up.rejected.is_empty() {
                reserved -= backlog.take().unwrap_or_default();
            }
            if up.encoded_len() + reserved + weight > limit {
                all_reported = false;
                break;
            }
            let key = row
                .key
                .as_ref()
                .map(|k| k.parts.join("|"))
                .unwrap_or_default();
            // Kept once told only while the replica holds a copy, which its
            // digests leave out and a snapshot's sweep must not take.
            let mut kept = false;
            if let Some(codec) = codec_of(codecs, &row.table).filter(|c| c.sent_down()) {
                let parts = row
                    .key
                    .as_ref()
                    .map(|k| k.parts.clone())
                    .unwrap_or_default();
                if let Ok(key) = codec.key(&parts) {
                    kept = !codec.present(conn, &[key]).await?.is_empty();
                }
            }
            rejected.push((row.table.clone(), key, sequence, kept));
            up.rejected.push(row);
        }
        items -= up.rejected.len();
        if let Some(a) = applied {
            up.gap = u64::try_from(a.gap).unwrap_or_default();
            untold_gap = a.gap > a.gap_told;
            // The last applied goes once every refusal of it is told: with
            // digests always (they are compared against it), with a gap (the
            // server reads what it missed against it), and otherwise when
            // the server has not heard of it.
            if all_reported {
                let read = Some((a.epoch.clone(), a.sequence));
                if let Some(mut held) = digests.filter(|held| held.applied == read) {
                    // A table the host wrote since they were read goes
                    // without one: the write may be in this very message.
                    let noted: HashSet<String> =
                        sqlx::query_scalar("SELECT DISTINCT relation FROM sync_notes")
                            .fetch_all(&mut *conn)
                            .await?
                            .into_iter()
                            .collect();
                    held.digests.retain(|d| !noted.contains(&d.table));
                    let weight: usize = held.digests.iter().map(|d| framed(d.encoded_len())).sum();
                    if up.encoded_len() + reserved + weight <= limit {
                        up.digests = held.digests;
                    }
                }
                news = a.sequence > a.acknowledged;
                if news || !up.digests.is_empty() || a.gap > 0 {
                    up.epoch = a.epoch;
                    up.sequence = u64::try_from(a.sequence).unwrap_or_default();
                    up.nonce = u64::try_from(a.nonce).unwrap_or_default();
                    up.mac = a.mac;
                }
            }
        }
    }
    // A row larger than a message with nothing else in it can carry never
    // goes; one that does not fit this one waits for the next.
    let bare = Up {
        protocol: envelope::PROTOCOL,
        ..Up::default()
    };
    let largest = limit.saturating_sub(bare.encoded_len() + reserved);
    let budget = limit.saturating_sub(up.encoded_len() + reserved);
    let notes = unsent(conn, lanes, this, settings, items, skip).await?;
    let gathered = gather(conn, codecs, notes, largest, budget).await?;
    up.tables = gathered.tables;
    if !gathered.carried.is_empty() {
        // A random number, a fresh one on every send: no answer meant for
        // another replica, or for this one before a restore, names its notes.
        let number: i64 = sqlx::query_scalar(
            "SELECT CASE WHEN n = 0 THEN 1 ELSE n END \
               FROM (SELECT random() & 9223372036854775807 AS n)",
        )
        .fetch_one(&mut *conn)
        .await?;
        sqlx::query(
            "UPDATE sync_notes SET upload = $1, sent = writes, sent_at = NULL \
              WHERE id IN (SELECT value FROM json_each($2))",
        )
        .bind(number)
        .bind(json(&gathered.carried))
        .execute(&mut *conn)
        .await?;
        up.uploads = vec![u64::try_from(number).unwrap_or_default()];
    }
    let sending = !up.tables.is_empty()
        || !up.rejected.is_empty()
        || !up.digests.is_empty()
        || news
        || untold_gap;
    if backlog.is_some() {
        let now = progress(conn).await?;
        up.backlog = backlog_due(conn, now, settings, sending).await?;
    }
    Ok((sending || up.backlog.is_some()).then_some(Outgoing {
        lane: this.name,
        up,
        notes: gathered.carried,
        rejected,
    }))
}

/// A note of a row the replica wrote.
#[cfg(feature = "sqlite")]
#[derive(sqlx::FromRow)]
struct Note {
    id: i64,
    relation: String,
    row_key: String,
    /// Re-stated when a snapshot began, and not filed since (section 7).
    restated: bool,
}

/// Up to a message's rows of notes, and `most`, to send on `lane`, oldest
/// first: the ones
/// never sent, or whose send the transport did not take, or written since
/// they were sent, and the ones sent longer than `resend_after` ago — or, by
/// a clock set back since, in the future — that the server never said it
/// filed.
#[cfg(feature = "sqlite")]
async fn unsent(
    conn: &mut Conn,
    lanes: &[Lane],
    lane: &Lane,
    settings: &Outbox,
    most: usize,
    skip: usize,
) -> Result<Vec<Note>, Error> {
    let quoted = |tables: &mut dyn Iterator<Item = &&str>| {
        tables.map(|t| literal(t)).collect::<Vec<_>>().join(", ")
    };
    // The engine's table of wishes goes where a lane names it; else with
    // every table no lane names; else on the first lane.
    let wishes = (std::ptr::eq(lane, &lanes[0])
        && lanes.iter().all(|l| !l.tables.is_empty())
        && !lanes.iter().any(|l| l.tables.contains(&crate::wish::TABLE)))
    .then_some(&crate::wish::TABLE);
    let filter = if lane.tables.is_empty() {
        let others = quoted(&mut lanes.iter().flat_map(|l| l.tables.iter()));
        if others.is_empty() {
            String::new()
        } else {
            format!("AND relation NOT IN ({others})")
        }
    } else {
        format!(
            "AND relation IN ({})",
            quoted(&mut lane.tables.iter().chain(wishes))
        )
    };
    let now = crate::dialect::NOW;
    Ok(sqlx::query_as(&format!(
        "SELECT id, relation, row_key, restated FROM sync_notes \
          WHERE set_aside IS NULL \
            AND (sent_at IS NULL OR writes > sent \
                 OR sent_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-{:.3} seconds') \
                 OR sent_at > {now}) {filter} \
          ORDER BY id LIMIT $1 OFFSET $2",
        settings.resend_after.as_secs_f64()
    ))
    .bind(i64::try_from(settings.limits.rows.min(most)).unwrap_or(i64::MAX))
    .bind(i64::try_from(skip).unwrap_or(i64::MAX))
    .fetch_all(&mut *conn)
    .await?)
}

/// Keys of one table, each with its note.
#[cfg(feature = "sqlite")]
type Keyed = Vec<(Key, i64)>;

/// Notes made into a message's tables.
#[cfg(feature = "sqlite")]
struct Gathered {
    tables: Vec<Table>,
    /// The notes the tables carry.
    carried: Vec<i64>,
}

/// `notes` as tables of an upload within `budget` bytes, the rows as they
/// are now: upserts parents first, in the order the codecs list the tables,
/// then deletes children first — the order the server files them in. A row
/// that is gone goes as its key only for a table keyed by its replica:
/// history a replica let go of before it went up is not the server's to
/// delete, and its note is done here. What does not fit waits for the next
/// message; a note that cannot go as it is — a row more than `largest`
/// bytes among them — is set aside.
#[cfg(feature = "sqlite")]
async fn gather<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    notes: Vec<Note>,
    largest: usize,
    mut budget: usize,
) -> Result<Gathered, Error> {
    let mut aside: Vec<(i64, String)> = Vec::new();
    let mut done: Vec<i64> = Vec::new();
    // Per table, and whether re-stated, each key with its note, in the
    // order first noted.
    let mut keyed: HashMap<(&str, bool), Keyed> = HashMap::new();
    for note in &notes {
        let Some(codec) = codec_of(codecs, &note.relation).filter(|c| c.goes_up()) else {
            aside.push((note.id, "a table this build does not send up".to_owned()));
            continue;
        };
        let parts: Vec<String> = note.row_key.split('|').map(str::to_owned).collect();
        let key = match codec.key(&parts) {
            Ok(key) => key,
            Err(e) => {
                aside.push((note.id, e.to_string()));
                continue;
            }
        };
        keyed
            .entry((codec.table.name, note.restated))
            .or_default()
            .push((key, note.id));
    }
    let mut carried = Vec::new();
    let mut tables = Vec::new();
    let mut gone: Vec<(&Codec<P>, Keyed)> = Vec::new();
    let mut full = false;
    for (codec, restated) in codecs
        .iter()
        .flat_map(|codec| [(codec, true), (codec, false)])
    {
        let Some(keys) = keyed.remove(&(codec.table.name, restated)) else {
            continue;
        };
        let wanted: Vec<Key> = keys.iter().map(|(k, _)| k.clone()).collect();
        let encoded = codec.load(conn, &wanted).await?;
        let mut part = Table {
            table: codec.table.name.to_owned(),
            restated,
            ..Table::default()
        };
        let mut len = framed(part.table.len()) + usize::from(restated) * 2;
        let mut missing = Vec::new();
        for (key, id) in keys {
            if let Some((_, e)) = encoded.failed.iter().find(|(k, _)| *k == key) {
                aside.push((id, e.to_string()));
                continue;
            }
            let Some((_, bytes)) = encoded.rows.iter().find(|(k, _)| *k == key) else {
                missing.push((key, id));
                continue;
            };
            let wire = wire_of(&key);
            let added = framed(bytes.len()) + framed(wire.encoded_len());
            if framed(framed(part.table.len()) + usize::from(restated) * 2 + added) > largest {
                let why = format!(
                    "a row of {} bytes, more than a message carries",
                    bytes.len()
                );
                aside.push((id, why));
                continue;
            }
            let weight = growth(len, added, part.upserts.is_empty());
            if full || weight > budget {
                full = true;
                continue;
            }
            budget -= weight;
            len += added;
            part.upserts.push(bytes.clone());
            part.keys.push(wire);
            carried.push(id);
        }
        if !part.upserts.is_empty() {
            tables.push(part);
        }
        if codec.takes_deletes() {
            match gone.last_mut() {
                Some((last, keys)) if std::ptr::eq(*last, codec) => keys.extend(missing),
                _ => gone.push((codec, missing)),
            }
        } else {
            done.extend(missing.into_iter().map(|(_, id)| id));
        }
    }
    for (codec, keys) in gone.into_iter().rev() {
        let mut part = Table {
            table: codec.table.name.to_owned(),
            ..Table::default()
        };
        let mut len = framed(part.table.len());
        for (key, id) in keys {
            let wire = wire_of(&key);
            let added = framed(wire.encoded_len());
            let weight = growth(len, added, part.deletes.is_empty());
            if full || weight > budget {
                full = true;
                continue;
            }
            budget -= weight;
            len += added;
            part.deletes.push(wire);
            carried.push(id);
        }
        if !part.deletes.is_empty() {
            tables.push(part);
        }
    }
    for (id, why) in &aside {
        tracing::error!(
            note = id,
            "sync: a write that cannot go up as it is — set aside: {why}"
        );
        sqlx::query("UPDATE sync_notes SET set_aside = $2 WHERE id = $1")
            .bind(id)
            .bind(why)
            .execute(&mut *conn)
            .await?;
    }
    sqlx::query("DELETE FROM sync_notes WHERE id IN (SELECT value FROM json_each($1))")
        .bind(json(&done))
        .execute(&mut *conn)
        .await?;
    Ok(Gathered { tables, carried })
}

/// What a field of `added` bytes costs a message, added to a table's part
/// of `len` bytes: itself, and what the part's own length grows by — or,
/// for a part not in the message yet, the part whole.
#[cfg(feature = "sqlite")]
fn growth(len: usize, added: usize, new: bool) -> usize {
    framed(len + added) - if new { 0 } else { framed(len) }
}

#[cfg(feature = "sqlite")]
fn wire_of(key: &Key) -> envelope::Key {
    envelope::Key {
        parts: key.iter().map(ToString::to_string).collect(),
    }
}

/// The rejections of this epoch the server has not heard of, oldest first,
/// up to `limit`, each with the message that refused it: one of another
/// epoch is of a server that is gone, and its snapshot clears it.
#[cfg(feature = "sqlite")]
async fn unreported(
    conn: &mut Conn,
    limit: usize,
) -> Result<Vec<(envelope::Rejected, i64)>, Error> {
    let rows: Vec<(String, String, String, String, i64, bool)> = sqlx::query_as(
        "SELECT table_name, row_key, reason, epoch, sequence, held_back FROM sync_rejections \
          WHERE reported_at IS NULL AND epoch = (SELECT epoch FROM sync_applied WHERE id = 1) \
          ORDER BY rejected_at, table_name, row_key LIMIT $1",
    )
    .bind(i64::try_from(limit).unwrap_or(i64::MAX))
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(table, key, reason, epoch, sequence, held_back)| {
            let row = envelope::Rejected {
                table,
                key: Some(envelope::Key {
                    parts: key.split('|').map(str::to_owned).collect(),
                }),
                reason,
                epoch,
                sequence: u64::try_from(sequence).unwrap_or_default(),
                held_back,
            };
            (row, sequence)
        })
        .collect())
}

/// The transport took `outgoing`: its notes — the ones still under its
/// number, the server's answer may have come first — wait for the server's
/// [`Filed`](envelope::Filed), its rejections are reported (unless refused
/// again since; those of a row the replica holds no copy of forgotten), and
/// the server of its epoch knows
/// what was applied. Run it in one transaction.
#[cfg(feature = "sqlite")]
pub(crate) async fn delivered(conn: &mut Conn, outgoing: &Outgoing) -> Result<(), Error> {
    if let Some(number) = outgoing.up.uploads.first() {
        sqlx::query(&format!(
            "UPDATE sync_notes SET sent_at = {} \
              WHERE upload = $1 AND id IN (SELECT value FROM json_each($2))",
            crate::dialect::NOW
        ))
        .bind(i64::try_from(*number).unwrap_or(i64::MAX))
        .bind(json(&outgoing.notes))
        .execute(&mut *conn)
        .await?;
    }
    for (table, key, sequence, kept) in &outgoing.rejected {
        let told = if *kept {
            format!(
                "UPDATE sync_rejections SET reported_at = {}",
                crate::dialect::NOW
            )
        } else {
            "DELETE FROM sync_rejections".to_owned()
        };
        sqlx::query(&format!(
            "{told} WHERE table_name = $1 AND row_key = $2 AND sequence = $3"
        ))
        .bind(table)
        .bind(key)
        .bind(sequence)
        .execute(&mut *conn)
        .await?;
    }
    if let Some(backlog) = &outgoing.up.backlog {
        let tables: Vec<Told> = backlog.tables.iter().map(told).collect();
        sqlx::query(
            "INSERT INTO sync_backlog (id, backlog, reported_at) VALUES (1, $1, $2) \
             ON CONFLICT (id) DO UPDATE SET backlog = EXCLUDED.backlog, reported_at = EXCLUDED.reported_at",
        )
        .bind(sqlx::types::Json(tables))
        .bind(chrono::Utc::now().timestamp_millis())
        .execute(&mut *conn)
        .await?;
    }
    if outgoing.up.gap > 0 {
        sqlx::query(
            "UPDATE sync_applied SET gap_told = max(gap_told, $1) WHERE id = 1 AND gap > 0",
        )
        .bind(i64::try_from(outgoing.up.gap).unwrap_or(i64::MAX))
        .execute(&mut *conn)
        .await?;
    }
    // A message that says nothing of it names no epoch, and changes nothing.
    sqlx::query(
        "UPDATE sync_applied SET acknowledged = max(acknowledged, $2) WHERE id = 1 AND epoch = $1",
    )
    .bind(&outgoing.up.epoch)
    .bind(i64::try_from(outgoing.up.sequence).unwrap_or(i64::MAX))
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// The transport refused `outgoing` for good, and it carries one row or one
/// rejection: what the transport refuses. The row is set aside, with why,
/// until [`retry_set_aside`] — unless written since, which goes as it is
/// now. The rejection is taken as told, since no message can tell it: it
/// must not hold back the confirmation of what was applied.
#[cfg(feature = "sqlite")]
pub(crate) async fn refused(conn: &mut Conn, outgoing: &Outgoing, why: &str) -> Result<(), Error> {
    sqlx::query(
        "UPDATE sync_notes SET set_aside = $1 \
          WHERE upload = $3 AND writes = sent AND id IN (SELECT value FROM json_each($2))",
    )
    .bind(capped(why))
    .bind(json(&outgoing.notes))
    .bind(
        outgoing
            .up
            .uploads
            .first()
            .map(|n| i64::try_from(*n).unwrap_or_default()),
    )
    .execute(&mut *conn)
    .await?;
    for (table, key, sequence, _) in &outgoing.rejected {
        tracing::error!(
            table,
            key,
            "sync: a rejection the uplink refuses for good — taken as told: {why}"
        );
        sqlx::query(&format!(
            "UPDATE sync_rejections SET reported_at = {} \
              WHERE table_name = $1 AND row_key = $2 AND sequence = $3",
            crate::dialect::NOW
        ))
        .bind(table)
        .bind(key)
        .bind(sequence)
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

/// Offers every write set aside to go up again: what an earlier build could
/// not send — of a table it did not send up, a row it could not encode —
/// this one may; what it cannot either, it sets aside again.
#[cfg(feature = "sqlite")]
pub(crate) async fn retry_set_aside(conn: &mut Conn) -> Result<u64, Error> {
    Ok(
        sqlx::query("UPDATE sync_notes SET set_aside = NULL WHERE set_aside IS NOT NULL")
            .execute(&mut *conn)
            .await?
            .rows_affected(),
    )
}

/// Whether the digests are due, `now`: when they were last said, or — the
/// first time this database is asked — `first` after now; a clock set back
/// past it counts as due.
#[cfg(feature = "sqlite")]
pub(crate) async fn digests_due(
    conn: &mut Conn,
    first: std::time::Duration,
    every: std::time::Duration,
) -> Result<bool, Error> {
    let millis = |d: std::time::Duration| i64::try_from(d.as_millis()).unwrap_or(i64::MAX);
    let now = chrono::Utc::now().timestamp_millis();
    sqlx::query("INSERT OR IGNORE INTO sync_digests (id, due) VALUES (1, $1)")
        .bind(now.saturating_add(millis(first)))
        .execute(&mut *conn)
        .await?;
    let due: i64 = sqlx::query_scalar("SELECT due FROM sync_digests WHERE id = 1")
        .fetch_one(&mut *conn)
        .await?;
    Ok(now >= due || due > now.saturating_add(millis(first.max(every))))
}

/// The digests went: due again `every` from now.
#[cfg(feature = "sqlite")]
pub(crate) async fn digested(conn: &mut Conn, every: std::time::Duration) -> Result<(), Error> {
    let every = i64::try_from(every.as_millis()).unwrap_or(i64::MAX);
    sqlx::query("INSERT INTO sync_digests (id, due) VALUES (1, $1) ON CONFLICT (id) DO UPDATE SET due = EXCLUDED.due")
        .bind(chrono::Utc::now().timestamp_millis().saturating_add(every))
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// What this replica holds of each table the server sends down, for the
/// server to compare with what it records — rows it retired, or rejected,
/// left out, and tables with writes of its own the server has not filed —
/// and the last message it applied, which they are read against.
#[cfg(feature = "sqlite")]
#[derive(Debug)]
pub(crate) struct Held {
    applied: Option<(String, i64)>,
    digests: Vec<envelope::Digest>,
}

/// What this replica [`Held`], read in one transaction of the caller's — a
/// reading one, which writes wait for no longer than it takes (in WAL, not
/// at all): it reads every row the server sends.
#[cfg(feature = "sqlite")]
pub(crate) async fn held<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
) -> Result<Held, Error> {
    let applied = sqlx::query_as("SELECT epoch, sequence FROM sync_applied WHERE id = 1")
        .fetch_optional(&mut *conn)
        .await?;
    let mut digests = Vec::new();
    let setting = |c: &Codec<P>| c.table.mirror == crate::row::Mirror::Setting;
    for codec in codecs.iter().filter(|c| c.sent_down() || setting(c)) {
        // What the replica wrote of it and the server has not filed is
        // about to change what the server records: compared now, the two
        // would differ for nothing.
        let unfiled: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM sync_notes WHERE relation = $1)")
                .bind(codec.table.name)
                .fetch_one(&mut *conn)
                .await?;
        if unfiled {
            continue;
        }
        let rejected: HashSet<String> =
            sqlx::query_scalar("SELECT row_key FROM sync_rejections WHERE table_name = $1")
                .bind(codec.table.name)
                .fetch_all(&mut *conn)
                .await?
                .into_iter()
                .collect();
        let mut keys: Vec<String> = codec
            .live_keys(conn)
            .await?
            .iter()
            .map(key_text)
            .filter(|k| !rejected.contains(k))
            .collect();
        if setting(codec) {
            keys = codec.versions(conn, None).await?;
        } else if codec.table.name == crate::wish::TABLE {
            keys = crate::wish::answers(conn, keys).await?;
        }
        digests.push(envelope::Digest {
            table: codec.table.name.to_owned(),
            count: keys.len() as u64,
            fingerprint: envelope::fingerprint(keys),
        });
    }
    Ok(Held { applied, digests })
}

#[cfg(all(test, feature = "sqlite"))]
#[path = "replica_tests.rs"]
mod tests;
