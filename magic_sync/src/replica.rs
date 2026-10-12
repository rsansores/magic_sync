//! A replica's side: what the server sends, applied table by table through
//! the codecs, and what the replica wrote, gathered to go up — run by the
//! [`Replica`](crate::Replica). What a host does here itself is set up the
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
const MIGRATIONS: [&str; crate::REPLICA_SCHEMA_VERSION as usize] =
    [include_str!("../sql/sqlite/1.sql")];

/// The engine's step to `version` of its tables on a replica, as one SQL
/// script that also records the version it brings them to — `None` for a
/// version no build of the engine has had yet. [`install`] runs every step a
/// database lacks, in order; a host with migrations runs each, in one of its
/// own: the first in the migration that creates its tables, before
/// [`triggers`]' script, and each later one when a release brings it (its
/// changelog says). A step is never edited once released.
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
///   delete unless the table is keyed by the replica; only an update of the
///   columns the replica owns goes up.
///
/// A write the engine makes while it applies what the server sent is noted
/// by none and refused by none.
#[cfg(feature = "sqlite")]
fn generated<P: Sync + 'static>(codecs: &[Codec<P>]) -> Vec<Trigger> {
    use crate::row::Mirror;
    const QUIET: &str = " WHEN NOT EXISTS (SELECT 1 FROM sync_applying)";
    // A row noted again is one note, written once more: a hot row goes up
    // once, as it is by then, and a write since clears what set it aside.
    const NOTED: &str =
        "ON CONFLICT (relation, row_key) DO UPDATE SET writes = writes + 1, set_aside = NULL";
    let mut out = Vec::new();
    for codec in codecs {
        let table = codec.table.name;
        let on = ident(table);
        let key_columns: Vec<&crate::row::Column> = codec
            .table
            .key
            .iter()
            .filter_map(|k| codec.table.columns.iter().find(|c| c.name == *k))
            .collect();
        let key = |row: &str| {
            key_columns
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
        };
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
                    out.push(refuse(event, QUIET, why));
                }
            }
            Mirror::Record => {
                out.push(noting("INSERT", "", "", format!("{guard}{}", note("NEW"))));
                if codec.table.columns.iter().any(|c| c.version.is_some()) {
                    let body = format!("{guard}{}{gone}", note("NEW"));
                    out.push(noting("UPDATE", "", "", body));
                } else {
                    let why = format!(
                        "`{table}` is history: a row of it is written once (a version column makes it editable)"
                    );
                    out.push(refuse("UPDATE", "", why));
                }
                out.push(noting("DELETE", "", "", note("OLD")));
            }
            Mirror::Both => {
                let why =
                    format!("a row of `{table}` begins on the server: a replica only edits it");
                out.push(refuse("INSERT", QUIET, why));
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
                out.push(refuse("UPDATE", &format!("{QUIET} AND ({changed})"), why));
                let of = if owned.is_empty() {
                    String::new()
                } else {
                    let owned: Vec<String> = owned.iter().map(|c| ident(c.name)).collect();
                    format!(" OF {}", owned.join(", "))
                };
                let body = format!("{guard}{}{gone}", note("NEW"));
                out.push(noting("UPDATE", &of, QUIET, body));
                if codec.takes_deletes() {
                    out.push(noting("DELETE", "", QUIET, note("OLD")));
                } else {
                    let why = format!(
                        "a row of `{table}` is let go of by the server: a replica deletes one \
                         only of a table keyed by it"
                    );
                    out.push(refuse("DELETE", QUIET, why));
                }
            }
            Mirror::ServerOnly => {}
        }
    }
    out
}

/// The triggers a replica database needs for `codecs`, as one SQL script: a
/// host with migrations puts it in the one that creates the tables, after
/// the engine's first step ([`migration`]), and again whenever a table is
/// added or changes how it travels, or a release writes them otherwise: each
/// trigger is dropped if it exists before it is created, so the script runs
/// again over what an earlier run wrote. [`check`] refuses a database whose triggers are not these.
#[cfg(feature = "sqlite")]
#[must_use]
pub fn triggers<P: Sync + 'static>(codecs: &[Codec<P>]) -> String {
    generated(codecs)
        .into_iter()
        .map(|t| format!("DROP TRIGGER IF EXISTS {};\n{};\n", ident(&t.name), t.sql))
        .collect()
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
/// not decode, keyed otherwise — with every difference named; triggers that
/// are not the ones [`triggers`] writes — one missing, one that differs, or
/// one left from a table that no longer travels; or a connection the engine
/// cannot keep its word on — `synchronous` below `FULL`, `journal_mode`
/// `OFF` or `MEMORY`, `recursive_triggers` off — which the pool's options
/// set. [`Replica::new`] runs it; a host with migrations also runs it in a
/// test, over the database its migrations build.
///
/// [`Replica::new`]: crate::Replica::new
///
/// # Errors
///
/// [`Error::Misuse`] naming whatever the database is not, as above;
/// [`Error::Database`] when the database fails.
#[cfg(feature = "sqlite")]
pub async fn check<P: Sync + 'static>(conn: &mut Conn, codecs: &[Codec<P>]) -> Result<(), Error> {
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
    let live: HashMap<String, String> =
        sqlx::query_as("SELECT name, sql FROM sqlite_master WHERE type = 'trigger'")
            .fetch_all(&mut *conn)
            .await?
            .into_iter()
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
    let ours = |name: &str, sql: &str| {
        engines(name) || sql.contains("sync_notes") || sql.contains("sync_applying")
    };
    if let Some((name, _)) = live
        .iter()
        .find(|(name, sql)| ours(name, sql) && !wanted.iter().any(|t| t.name == **name))
    {
        return Err(Error::Misuse(format!(
            "the trigger `{name}` is not one of the engine's: a table that no longer travels, or one written by hand — drop it"
        )));
    }
    settled(conn).await?;
    Ok(())
}

/// Whether a trigger of this name is one the engine writes (and [`install`]
/// replaces): a host's own may be named anything else.
#[cfg(feature = "sqlite")]
fn engines(name: &str) -> bool {
    name.starts_with("sync_note_") || name.starts_with("sync_refuse_")
}

/// Refuses a connection the engine cannot keep its word on, each setting of
/// which is the connection's own — so each one a message is applied through
/// is asked: commits that are not durable (`synchronous` below `FULL`), since
/// a message is acknowledged once its commit returns; a journal that cannot
/// roll a transaction back (`journal_mode` `OFF` or `MEMORY`), since a
/// message is applied whole or not at all; and triggers that do not fire on
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
    let recursive: i64 = sqlx::query_scalar("PRAGMA recursive_triggers")
        .fetch_one(&mut *conn)
        .await?;
    let refused = if synchronous < 2 {
        "its commits are not durable (`synchronous` below FULL): a message acknowledged could be lost"
    } else if ["off", "memory"].contains(&journal.to_lowercase().as_str()) {
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
/// trigger [`triggers`] writes, in place of the engine's own it had. Then
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
    let live: Vec<String> =
        sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type = 'trigger'")
            .fetch_all(&mut *tx)
            .await?;
    for name in live.into_iter().filter(|name| engines(name)) {
        sqlx::query(&format!("DROP TRIGGER {}", ident(&name)))
            .execute(&mut *tx)
            .await?;
    }
    for trigger in generated(codecs) {
        sqlx::query(&trigger.sql).execute(&mut *tx).await?;
    }
    check(&mut tx, codecs).await?;
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

/// A message from the server, as the transport delivered it, acted on in the
/// caller's transaction — one begun with `BEGIN IMMEDIATE`, so that what it
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
/// `care`fully, with that row refused. A commit a key refuses anyway is the
/// caller's to answer by applying the message `care`fully, which finds the
/// row.
#[cfg(feature = "sqlite")]
pub(crate) async fn receive<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    bytes: &[u8],
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
    sqlx::query("INSERT OR IGNORE INTO sync_applying (id) VALUES (1)")
        .execute(&mut *conn)
        .await?;
    let received = if care.careful {
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

/// How a message is applied: all at once, or — once a refusal rolled a
/// whole transaction back — `careful`ly, row by row, each in a savepoint of
/// its own, and the rows that did so refused before they are tried.
#[cfg(feature = "sqlite")]
#[derive(Debug, Default)]
pub(crate) struct Care {
    careful: bool,
    culprits: HashMap<(String, Spot), Culprit>,
}

#[cfg(feature = "sqlite")]
impl Care {
    /// After a refusal took the whole transaction with it: row by row from
    /// now on. Answers whether that is news — a careful run that fails so
    /// fails for another reason.
    pub(crate) fn heed(&mut self) -> bool {
        !std::mem::replace(&mut self.careful, true)
    }

    /// Refuses `culprits` before they are tried, next time, all at once —
    /// one refused already is not what failed, and the message stops with an
    /// error.
    pub(crate) fn refuse(&mut self, culprits: Vec<Culprit>) -> Result<(), Error> {
        self.careful = true;
        for culprit in culprits {
            let place = (culprit.table.clone(), culprit.row.clone());
            if self.culprits.contains_key(&place) {
                return Err(Error::Misuse(format!(
                    "a row refused before it was tried failed the message again: {culprit:?}"
                )));
            }
            tracing::error!(
                table = culprit.table,
                "sync: a row the database would refuse the message for — applied again without it: {}",
                culprit.why
            );
            self.culprits.insert(place, culprit);
        }
        Ok(())
    }

    fn culprit(&self, table: &str, row: &Spot) -> Option<&Culprit> {
        self.culprits.get(&(table.to_owned(), row.clone()))
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
fn malformed(why: String) -> Received {
    tracing::error!("sync: a message this build can do nothing with — dropped: {why}");
    Received::Malformed(why)
}

/// `body` acted on carefully, once a refusal took a whole transaction with
/// it — the commit's own included: and then, since a foreign key checked at
/// commit refuses what a row of the message changed, or let go of, under
/// rows that point at it by a column other than its key, the rows that did
/// so are found, to be refused. What the message's tables held that rows
/// point at is read before, and what of it is gone, still pointed at,
/// after.
#[cfg(feature = "sqlite")]
async fn carefully<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    body: Body,
    care: &Care,
) -> Result<Received, Stop> {
    let mut upserts: HashMap<(&str, Key), Spot> = HashMap::new();
    let names: Vec<&str> = match &body {
        Body::Down(down) => {
            for (entry, table) in down.tables.iter().enumerate() {
                let Some(codec) = codec_of(codecs, &table.table) else {
                    continue;
                };
                for (at, key) in parsed(codec, &table.keys).into_iter().enumerate() {
                    if let Some(key) = key {
                        upserts.insert((codec.table.name, key), Spot::Upsert { entry, at });
                    }
                }
            }
            down.tables.iter().map(|t| t.table.as_str()).collect()
        }
        Body::EndSnapshot(end) => end.tables.iter().map(String::as_str).collect(),
        Body::Filed(_) => codecs.iter().map(|c| c.table.name).collect(),
    };
    let mut touched: Vec<&Codec<P>> = Vec::new();
    for codec in codecs.iter().filter(|c| c.sent_down()) {
        if names.contains(&codec.table.name) {
            touched.push(codec);
        }
    }
    let mut before = Vec::with_capacity(touched.len());
    for codec in touched {
        before.push((codec, codec.pointed(conn).await?));
    }
    let received = act(conn, codecs, body, care).await?;
    let mut culprits = Vec::new();
    for (codec, pointed) in &before {
        let table = codec.table.name;
        for key in codec.broken(conn, pointed).await? {
            let row = upserts
                .remove(&(table, key.clone()))
                .unwrap_or_else(|| Spot::LetGo(key_text(&key)));
            culprits.push(Culprit {
                table: table.to_owned(),
                row,
                why: "rows here point at what it changes, through a key checked at commit"
                    .to_owned(),
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
                Ok(sequence) => Ok(Received::Applied(
                    apply(conn, codecs, &down, sequence, care).await?,
                )),
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
                Ok(sequence) => {
                    end_snapshot(conn, codecs, &end, sequence, care).await?;
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
async fn chained(conn: &mut Conn, place: Place<'_>) -> Result<Result<i64, Received>, Error> {
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
        return Ok(Ok(sequence));
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
    Ok(Ok(sequence))
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
/// storage failure is the error. A row the replica takes or lets go of is no
/// longer one to report.
#[cfg(feature = "sqlite")]
async fn apply<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    message: &Down,
    sequence: i64,
    care: &Care,
) -> Result<Vec<Applied>, Stop> {
    let naming = !message.snapshot_id.is_empty();
    let mut taken = Vec::new();
    let mut placed = Vec::new();
    for (entry, table) in message.tables.iter().enumerate() {
        let Some(codec) = codec_of(codecs, &table.table).filter(|c| c.sent_down()) else {
            let why = if codec_of(codecs, &table.table).is_some() {
                "a table the server does not send"
            } else {
                "a table this replica does not mirror"
            };
            reject(conn, &table.table, &foreign_keys(table), why, sequence).await?;
            continue;
        };
        let deletes = keys_of(codec, &table.deletes);
        let_go(conn, codecs, codec, &deletes, Some(sequence), care).await?;
        let beside = parsed(codec, &table.keys);
        // A key twice in a part: the last row beside it is the one.
        let mut last = HashMap::new();
        for (at, key) in beside.iter().enumerate() {
            if let Some(key) = key {
                last.insert(key, at);
            }
        }
        let rows: Vec<usize> = (0..beside.len())
            .filter(|at| beside[*at].as_ref().is_none_or(|key| last[key] == *at))
            .collect();
        let mut held = Vec::new();
        if care.careful {
            let mut pending = Vec::new();
            for at in rows {
                let key = beside[at].as_ref();
                match care.culprit(&table.table, &Spot::Upsert { entry, at }) {
                    Some(culprit) => {
                        let why = &culprit.why;
                        refuse(conn, codec, key, why, naming, sequence).await?;
                    }
                    None => pending.push(at),
                }
            }
            // As taken all at once: one refused is tried again once the
            // rest are in, while that takes one more.
            loop {
                let mut again = Vec::new();
                for at in &pending {
                    row_begun(conn).await?;
                    let took = take(conn, codec, table, &beside, &[*at], naming, sequence).await;
                    let row = Spot::Upsert { entry, at: *at };
                    match row_done(conn, took, &table.table, row).await? {
                        Ok(keys) if keys.is_empty() => again.push(*at),
                        Ok(keys) => held.extend(keys),
                        Err(e) => {
                            let why = e.to_string();
                            refuse(conn, codec, beside[*at].as_ref(), &why, naming, sequence)
                                .await?;
                        }
                    }
                }
                if again.is_empty() || again.len() == pending.len() {
                    break;
                }
                pending = again;
            }
        } else {
            held = take(conn, codec, table, &beside, &rows, naming, sequence).await?;
        }
        taken.push(Applied {
            table: table.table.clone(),
            keys: held.iter().map(|(_, key)| key.clone()).collect(),
        });
        placed.push((entry, codec, held));
    }
    let mut culprits = Vec::new();
    for (entry, codec, held) in placed {
        let keys: Vec<Key> = held.iter().map(|(_, key)| key.clone()).collect();
        let orphans: HashSet<Key> = codec.unparented(conn, &keys).await?.into_iter().collect();
        culprits.extend(
            held.into_iter()
                .filter(|(_, key)| orphans.contains(key))
                .map(|(at, _)| Culprit {
                    table: codec.table.name.to_owned(),
                    row: Spot::Upsert { entry, at },
                    why: "a row it points at, through a foreign key, is not held here".to_owned(),
                }),
        );
    }
    if !culprits.is_empty() {
        return Err(Stop::Culprits(culprits));
    }
    Ok(taken)
}

/// Text the replica keeps of what a server said, at most: a reason, the
/// name and a key of a table it does not know. A rejection always fits a
/// message up, so one never holds back the replica's confirmation; and what
/// either side logs of what the other sent.
const TEXT_CAP: usize = 512;

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
/// `beside`: the ones the replica cannot take are rejected, the rest are no
/// longer ones to report, nor to let go once filed; while a snapshot is
/// `naming` them, it names each, taken or refused. Answers the rows the
/// replica now holds, by their place in `table`, with their keys.
#[cfg(feature = "sqlite")]
async fn take<P: Sync + 'static>(
    conn: &mut Conn,
    codec: &Codec<P>,
    table: &Table,
    beside: &[Option<Key>],
    positions: &[usize],
    naming: bool,
    sequence: i64,
) -> Result<Vec<(usize, Key)>, Stop> {
    let rows = positions
        .iter()
        .map(|at| (table.upserts[*at].clone(), beside[*at].clone()))
        .collect();
    let failed: HashMap<usize, String> = codec
        .apply_down(conn, rows)
        .await?
        .into_iter()
        .map(|refused| (positions[refused.at], refused.reason))
        .collect();
    let mut refused = Vec::new();
    for (at, why) in &failed {
        let key = Vec::from_iter(beside[*at].as_ref().map(key_text));
        reject(conn, &table.table, &key, why, sequence).await?;
        refused.extend(beside[*at].clone());
    }
    let held: Vec<(usize, Key)> = positions
        .iter()
        .filter(|at| !failed.contains_key(at))
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
        let kept = codec.present(conn, &refused).await?;
        let kept = kept.iter().map(key_text);
        named(
            conn,
            &table.table,
            &texts.into_iter().chain(kept).collect::<Vec<_>>(),
        )
        .await?;
    }
    Ok(held)
}

/// The row of `table` at `at`, refused (`why`) before or as it was tried
/// alone: rejected, and named by the snapshot in progress when the replica
/// holds a copy of it.
#[cfg(feature = "sqlite")]
async fn refuse<P: Sync + 'static>(
    conn: &mut Conn,
    codec: &Codec<P>,
    key: Option<&Key>,
    why: &str,
    naming: bool,
    sequence: i64,
) -> Result<(), Error> {
    let table = codec.table.name;
    reject(
        conn,
        table,
        &Vec::from_iter(key.map(key_text)),
        why,
        sequence,
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
    sequence: Option<i64>,
    care: &Care,
) -> Result<(), Stop> {
    if !care.careful {
        let_go_together(conn, codecs, codec, keys, sequence).await?;
        return Ok(());
    }
    let table = codec.table.name;
    let mut pending = Vec::new();
    for key in keys {
        match care.culprit(table, &Spot::LetGo(key_text(key))) {
            Some(culprit) => kept(conn, table, key, &culprit.why, sequence).await?,
            None => pending.push(key.clone()),
        }
    }
    loop {
        let mut again = Vec::new();
        for key in &pending {
            row_begun(conn).await?;
            let one = std::slice::from_ref(key);
            let gone = let_go_together(conn, codecs, codec, one, sequence).await;
            match row_done(conn, gone, table, Spot::LetGo(key_text(key))).await? {
                Ok(refused) => again.extend(refused),
                Err(e) => kept(conn, table, key, &e.to_string(), sequence).await?,
            }
        }
        if again.is_empty() || again.len() == pending.len() {
            return Ok(());
        }
        pending = again;
    }
}

/// Lets go of the rows of `keys`, together ([`Codec::retire`]): the ones the
/// database refuses to let go are [`kept`] — and answered — the rest are no
/// longer ones to report. A row of a table written on both sides whose
/// edits have not reached the server is held retired until they have
/// ([`filed`]), so the edits are not lost.
#[cfg(feature = "sqlite")]
async fn let_go_together<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    codec: &Codec<P>,
    keys: &[Key],
    sequence: Option<i64>,
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
        "INSERT OR IGNORE INTO sync_leaving (relation, row_key) \
         SELECT $1, value FROM json_each($2)",
    )
    .bind(codec.table.name)
    .bind(json(&leaving))
    .execute(&mut *conn)
    .await?;
    for (key, why) in &retired.refused {
        kept(conn, codec.table.name, key, why, sequence).await?;
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

/// A row the replica could not let go of: rejected, as refused by the message
/// numbered `sequence` — the one that let go of it, or, when the replica let
/// go of it once the server filed its edits, the last it applied; logged
/// only when it has applied none.
#[cfg(feature = "sqlite")]
async fn kept(
    conn: &mut Conn,
    table: &str,
    key: &Key,
    why: &str,
    sequence: Option<i64>,
) -> Result<(), Error> {
    let Some(sequence) = sequence else {
        tracing::error!(
            table,
            key = key_text(key),
            "sync: a row let go of, whose edits are filed, the database keeps: {why}"
        );
        return Ok(());
    };
    reject(conn, table, &[key_text(key)], why, sequence).await
}

/// Notes rows the replica could not take, or let go of, for the server to
/// hear of: refused by the message numbered `sequence`.
#[cfg(feature = "sqlite")]
async fn reject(
    conn: &mut Conn,
    table: &str,
    keys: &[String],
    why: &str,
    sequence: i64,
) -> Result<(), Error> {
    tracing::error!(
        table = capped(table),
        rows = keys.len(),
        first = capped(keys.first().map_or("", String::as_str)),
        sequence,
        "sync: rows this replica cannot take or let go of — rejected, for the server to hear of: {}",
        capped(why)
    );
    sqlx::query(
        "INSERT INTO sync_rejections (table_name, row_key, reason, epoch, sequence) \
         SELECT $1, value, $3, (SELECT epoch FROM sync_applied WHERE id = 1), $4 \
           FROM json_each($2) WHERE true \
         ON CONFLICT (table_name, row_key) DO UPDATE SET reason = EXCLUDED.reason, \
           epoch = EXCLUDED.epoch, sequence = EXCLUDED.sequence, \
           rejected_at = EXCLUDED.rejected_at, reported_at = NULL",
    )
    .bind(table)
    .bind(json(keys))
    .bind(capped(why))
    .bind(sequence)
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

/// The snapshot in progress is complete: every row of its tables it did not
/// name goes, children first — the server lists them parents first. What
/// was rejected in those tables before it began is forgotten: it sent again
/// what the replica should hold.
#[cfg(feature = "sqlite")]
async fn end_snapshot<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    message: &EndSnapshot,
    sequence: i64,
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
        let_go(conn, codecs, codec, &stale, Some(sequence), care).await?;
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
    sqlx::query(
        "DELETE FROM sync_notes WHERE upload IN (SELECT value FROM json_each($1)) AND writes = sent",
    )
    .bind(json(&uploads))
    .execute(&mut *conn)
    .await?;
    let done: Vec<(String, String)> = sqlx::query_as(
        "DELETE FROM sync_leaving AS l WHERE NOT EXISTS (SELECT 1 FROM sync_notes n \
           WHERE n.relation = l.relation AND n.row_key = l.row_key) \
         RETURNING relation, row_key",
    )
    .fetch_all(&mut *conn)
    .await?;
    let applied: Option<i64> = sqlx::query_scalar("SELECT sequence FROM sync_applied WHERE id = 1")
        .fetch_optional(&mut *conn)
        .await?;
    for codec in codecs {
        let keys: Vec<Key> = done
            .iter()
            .filter(|(table, _)| table == codec.table.name)
            .filter_map(|(_, key)| {
                let parts: Vec<String> = key.split('|').map(str::to_owned).collect();
                codec.key(&parts).ok()
            })
            .collect();
        let_go(conn, codecs, codec, &keys, applied, care).await?;
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

impl From<&envelope::TableBacklog> for UpProgress {
    fn from(table: &envelope::TableBacklog) -> Self {
        UpProgress {
            table: table.table.clone(),
            unsent: i64::try_from(table.unsent).unwrap_or(i64::MAX),
            unfiled: i64::try_from(table.unfiled).unwrap_or(i64::MAX),
            set_aside: i64::try_from(table.set_aside).unwrap_or(i64::MAX),
        }
    }
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
    let told: Option<(sqlx::types::Json<Vec<UpProgress>>, i64)> =
        sqlx::query_as("SELECT backlog, reported_at FROM sync_backlog WHERE id = 1")
            .fetch_optional(&mut *conn)
            .await?;
    let (before, at) = match told {
        Some((backlog, at)) => (Some(backlog.0), at),
        None => (None, 0),
    };
    let every = i64::try_from(settings.backlog_every.as_millis()).unwrap_or(i64::MAX);
    let clock = chrono::Utc::now().timestamp_millis();
    let stale = clock < at || clock - at >= every;
    let due = if before.as_ref() == Some(&now) {
        // Unchanged: said again now and then while not empty, should the
        // server have lost it.
        !now.is_empty() && stale
    } else {
        let edge = now.is_empty() || before.as_ref().is_none_or(Vec::is_empty);
        sending || edge || stale
    };
    Ok(due.then(|| backlog_of(now)))
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
    /// How large a message the uplink takes: rows per message, and bytes.
    pub limits: envelope::Limits,
    /// How long an upload the server has not answered waits before it goes
    /// again.
    pub resend_after: std::time::Duration,
    /// Rejections per message, at most.
    pub rejections: std::num::NonZeroUsize,
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
            rejections: std::num::NonZeroUsize::new(500).expect("not zero"),
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
        if let Some(codec) = codecs
            .iter()
            .find(|c| c.goes_up() && !named.contains(c.table.name))
        {
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
/// `digests` of what it holds, when given ([`held`]). Run it in a transaction begun with
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
    // number (63 bits, nine bytes), and on the main lane the backlog,
    // whatever it says by the end — per table, its name and three numbers.
    let mut reserved = framed(9);
    let mut rejected = Vec::new();
    let mut untold_gap = false;
    // Whether the last applied is news to the server.
    let mut news = false;
    // Rows and rejections it may carry, at most, after those it passes over.
    let mut items = window.map_or(usize::MAX, |w| w.take);
    let mut skip = window.map_or(0, |w| w.skip);
    if main {
        let tables: usize = progress(conn)
            .await?
            .iter()
            .map(|t| framed(framed(t.table.len()) + 3 * 11))
            .sum();
        reserved += framed(tables);
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
        // One over the cap says whether this message reports them all: the
        // server must hear of a refusal before the message it refused is
        // confirmed, or it would take the refused row for applied.
        let most = settings.rejections.get().min(items);
        let mut pending = unreported(conn, skip.saturating_add(most).saturating_add(1)).await?;
        let passed = skip.min(pending.len());
        pending.drain(..passed);
        skip -= passed;
        let mut all_reported = passed == 0 && pending.len() <= most;
        for (row, sequence) in pending.into_iter().take(most) {
            let weight = framed(row.encoded_len());
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
                if let Some(held) = digests.filter(|held| held.applied == read) {
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
    if main {
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
    let filter = if lane.tables.is_empty() {
        let others = quoted(&mut lanes.iter().flat_map(|l| l.tables.iter()));
        if others.is_empty() {
            String::new()
        } else {
            format!("AND relation NOT IN ({others})")
        }
    } else {
        format!("AND relation IN ({})", quoted(&mut lane.tables.iter()))
    };
    let now = crate::dialect::NOW;
    Ok(sqlx::query_as(&format!(
        "SELECT id, relation, row_key FROM sync_notes \
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
    // Per table, each key with its note, in the order first noted.
    let mut keyed: HashMap<&str, Keyed> = HashMap::new();
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
            .entry(codec.table.name)
            .or_default()
            .push((key, note.id));
    }
    let mut carried = Vec::new();
    let mut tables = Vec::new();
    let mut gone: Vec<(&Codec<P>, Keyed)> = Vec::new();
    let mut full = false;
    for codec in codecs {
        let Some(keys) = keyed.remove(codec.table.name) else {
            continue;
        };
        let wanted: Vec<Key> = keys.iter().map(|(k, _)| k.clone()).collect();
        let encoded = codec.load(conn, &wanted).await?;
        let mut part = Table {
            table: codec.table.name.to_owned(),
            ..Table::default()
        };
        let mut len = framed(part.table.len());
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
            if framed(framed(part.table.len()) + added) > largest {
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
            gone.push((codec, missing));
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
    let rows: Vec<(String, String, String, String, i64)> = sqlx::query_as(
        "SELECT table_name, row_key, reason, epoch, sequence FROM sync_rejections \
          WHERE reported_at IS NULL AND epoch = (SELECT epoch FROM sync_applied WHERE id = 1) \
          ORDER BY rejected_at, table_name, row_key LIMIT $1",
    )
    .bind(i64::try_from(limit).unwrap_or(i64::MAX))
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(table, key, reason, epoch, sequence)| {
            let row = envelope::Rejected {
                table,
                key: Some(envelope::Key {
                    parts: key.split('|').map(str::to_owned).collect(),
                }),
                reason,
                epoch,
                sequence: u64::try_from(sequence).unwrap_or_default(),
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
        let tables: Vec<UpProgress> = backlog.tables.iter().map(UpProgress::from).collect();
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

/// Offers every write set aside to go up again: what this build could not
/// send, a new one may.
#[cfg(feature = "sqlite")]
pub(crate) async fn retry_set_aside(conn: &mut Conn) -> Result<u64, Error> {
    Ok(
        sqlx::query("UPDATE sync_notes SET set_aside = NULL WHERE set_aside IS NOT NULL")
            .execute(&mut *conn)
            .await?
            .rows_affected(),
    )
}

/// What this replica holds of each table the server sends down, for the
/// server to compare with what it records — rows it retired, or rejected,
/// left out — and the last message it applied, which they are read against.
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
    for codec in codecs.iter().filter(|c| c.sent_down()) {
        let rejected: HashSet<String> =
            sqlx::query_scalar("SELECT row_key FROM sync_rejections WHERE table_name = $1")
                .bind(codec.table.name)
                .fetch_all(&mut *conn)
                .await?
                .into_iter()
                .collect();
        let keys: Vec<String> = codec
            .live_keys(conn)
            .await?
            .iter()
            .map(key_text)
            .filter(|k| !rejected.contains(k))
            .collect();
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
