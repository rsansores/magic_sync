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
    crate::envelope::{framed, opened, Body, Down, EndSnapshot, Limits, Table, ToReplica},
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
    wire.iter()
        .filter_map(|key| match codec.key(&key.parts) {
            Ok(key) => Some(key),
            Err(e) => {
                tracing::error!(
                    table = codec.table.name,
                    "sync: a key that does not parse — skipped: {e}"
                );
                None
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The engine's tables and triggers.

/// The engine's migrations, in order: the `n`th brings a replica database
/// from version `n` to `n + 1`.
#[cfg(feature = "sqlite")]
const MIGRATIONS: [&str; crate::SCHEMA_VERSION as usize] = [crate::SQLITE_SCHEMA];

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
/// [`SQLITE_SCHEMA`](crate::SQLITE_SCHEMA), and again whenever a table is
/// added or changes how it travels; [`check`] refuses a database whose
/// triggers are not these.
#[cfg(feature = "sqlite")]
#[must_use]
pub fn triggers<P: Sync + 'static>(codecs: &[Codec<P>]) -> String {
    generated(codecs)
        .into_iter()
        .map(|t| t.sql + ";\n")
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
/// one left from a table that no longer travels; or commits that are not
/// durable (`synchronous` below `FULL`), since a message is acknowledged
/// once its commit returns. [`Replica::new`]
/// runs it; a host with migrations also runs it in a test, over the database
/// its migrations build.
///
/// [`Replica::new`]: crate::Replica::new
///
/// # Errors
///
/// [`Error::Misuse`] naming whatever the database is not, as above;
/// [`Error::Database`] when the database fails.
#[cfg(feature = "sqlite")]
pub async fn check<P: Sync + 'static>(conn: &mut Conn, codecs: &[Codec<P>]) -> Result<(), Error> {
    let (version, needed) = (schema_version(conn).await?, crate::SCHEMA_VERSION);
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
        name.starts_with("sync_") || sql.contains("sync_notes") || sql.contains("sync_applying")
    };
    if let Some((name, _)) = live
        .iter()
        .find(|(name, sql)| ours(name, sql) && !wanted.iter().any(|t| t.name == **name))
    {
        return Err(Error::Misuse(format!(
            "the trigger `{name}` is not one of the engine's: a table that no longer travels, or one written by hand — drop it"
        )));
    }
    durable(conn).await?;
    Ok(())
}

/// Refuses a connection whose commits are not durable (`synchronous` below
/// `FULL`): a message is acknowledged once its commit returns. A setting of
/// each connection, so each one a message is applied through is asked.
#[cfg(feature = "sqlite")]
pub(crate) async fn durable(conn: &mut Conn) -> Result<(), Error> {
    let synchronous: i64 = sqlx::query_scalar("PRAGMA synchronous")
        .fetch_one(&mut *conn)
        .await?;
    if synchronous < 2 {
        return Err(Error::Misuse(
            "the database's commits are not durable (`synchronous` below FULL): a message acknowledged could be lost"
                .to_owned(),
        ));
    }
    Ok(())
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
    let Some(due) = usize::try_from(version)
        .ok()
        .and_then(|v| MIGRATIONS.get(v..))
    else {
        return Err(Error::Misuse(format!(
            "the engine's tables are version {version}, later than this build's {}",
            crate::SCHEMA_VERSION
        )));
    };
    for script in due {
        sqlx::Executor::execute(&mut *tx, *script).await?;
    }
    let ours: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'sync\\_%' ESCAPE '\\'",
    )
    .fetch_all(&mut *tx)
    .await?;
    for name in ours {
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
    /// redelivery, or one a relay that lost its lock sent late. Nothing done.
    AlreadyApplied,
    /// A message that follows one this replica never applied: it missed
    /// one. Nothing done, now or until a snapshot begins; the server is told,
    /// and provisions it again.
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
/// cannot read is [`Received::Malformed`].
#[cfg(feature = "sqlite")]
pub(crate) async fn receive<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    bytes: &[u8],
) -> Result<Received, Error> {
    let message = match ToReplica::decode(bytes) {
        Ok(message) => message,
        Err(e) => return Ok(malformed(format!("bytes that are no message: {e}"))),
    };
    if message.protocol > envelope::PROTOCOL {
        return Err(Error::Protocol {
            theirs: message.protocol,
            ours: envelope::PROTOCOL,
        });
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
    let received = act(conn, codecs, body).await;
    let quiet = sqlx::query("DELETE FROM sync_applying")
        .execute(&mut *conn)
        .await;
    let received = received?;
    quiet?;
    Ok(received)
}

#[cfg(feature = "sqlite")]
fn malformed(why: String) -> Received {
    tracing::error!("sync: a message this build can do nothing with — dropped: {why}");
    Received::Malformed(why)
}

#[cfg(feature = "sqlite")]
async fn act<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    body: Body,
) -> Result<Received, Error> {
    match body {
        Body::Filed(message) => {
            filed(conn, codecs, &message).await?;
            Ok(Received::Filed)
        }
        Body::Down(down) => {
            let place = Place {
                epoch: &down.epoch,
                sequence: down.sequence,
                previous: down.previous,
                snapshot: &down.snapshot_id,
                ends: false,
            };
            match chained(conn, place).await? {
                Ok(sequence) => Ok(Received::Applied(
                    apply(conn, codecs, &down, sequence).await?,
                )),
                Err(skipped) => Ok(skipped),
            }
        }
        Body::EndSnapshot(end) => {
            let place = Place {
                epoch: &end.epoch,
                sequence: end.sequence,
                previous: end.previous,
                snapshot: &end.snapshot_id,
                ends: true,
            };
            match chained(conn, place).await? {
                Ok(sequence) => {
                    end_snapshot(conn, codecs, &end, sequence).await?;
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
    previous: u64,
    snapshot: &'m str,
    /// It ends the snapshot it names.
    ends: bool,
}

/// Whether the message at `place` is the next one to apply, recording it as
/// the last applied when it is: its sequence when it is, and otherwise what
/// it is instead. The next one follows the last applied (`previous`), or
/// begins a snapshot (`previous` 0), whatever came before — of this epoch or
/// another, which is a server installed again, or one that missed nothing
/// this replica must know. One that follows another the replica never
/// applied is a gap: nothing is applied until a snapshot begins. One at or
/// below the last applied of its epoch is a redelivery; and since the
/// server may have lost what it was told, it is told again.
#[cfg(feature = "sqlite")]
async fn chained(conn: &mut Conn, place: Place<'_>) -> Result<Result<i64, Received>, Error> {
    let Some(sequence) = i64::try_from(place.sequence).ok().filter(|s| *s > 0) else {
        return Ok(Err(malformed(format!(
            "sequence {}, outside 1 to 2^63 - 1",
            place.sequence
        ))));
    };
    let Ok(epoch) = Uuid::parse_str(place.epoch).map(|e| e.to_string()) else {
        return Ok(Err(malformed(format!(
            "epoch `{}` is not a uuid",
            place.epoch
        ))));
    };
    let Some(previous) = i64::try_from(place.previous).ok().filter(|p| *p < sequence) else {
        return Ok(Err(malformed(format!(
            "message {sequence} follows {}, which is not before it",
            place.previous
        ))));
    };
    if previous == 0 && place.snapshot.is_empty() {
        return Ok(Err(malformed(
            "a message that follows nothing and begins no snapshot".to_owned(),
        )));
    }
    let last: Option<(String, i64, Option<String>)> =
        sqlx::query_as("SELECT epoch, sequence, snapshot_id FROM sync_applied WHERE id = 1")
            .fetch_optional(&mut *conn)
            .await?;
    if last
        .as_ref()
        .is_some_and(|(e, s, _)| *e == epoch && sequence <= *s)
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
        if last.as_ref().is_some_and(|(e, _, _)| *e != epoch) {
            // A server installed again, which knows nothing it was told.
            sqlx::query("DELETE FROM sync_backlog")
                .execute(&mut *conn)
                .await?;
        }
        sqlx::query("DELETE FROM sync_snapshot_keys")
            .execute(&mut *conn)
            .await?;
        sqlx::query(
            "INSERT INTO sync_applied (id, epoch, sequence, snapshot_id, snapshot_began) \
             VALUES (1, $1, $2, $3, $2) \
             ON CONFLICT (id) DO UPDATE SET epoch = EXCLUDED.epoch, sequence = EXCLUDED.sequence, \
               snapshot_id = EXCLUDED.snapshot_id, snapshot_began = EXCLUDED.snapshot_began, \
               gap = 0, gap_told = 0, \
               acknowledged = CASE WHEN sync_applied.epoch = EXCLUDED.epoch \
                                   THEN sync_applied.acknowledged ELSE 0 END",
        )
        .bind(&epoch)
        .bind(sequence)
        .bind(place.snapshot)
        .execute(&mut *conn)
        .await?;
        return Ok(Ok(sequence));
    }
    let Some((_, _, in_progress)) = last.filter(|(e, s, _)| *e == epoch && *s == previous) else {
        tracing::warn!(
            sequence,
            previous,
            "sync: a message that follows one this replica never applied — it missed one, \
             and waits to be provisioned again"
        );
        sqlx::query(
            "INSERT INTO sync_applied (id, epoch, sequence, gap) VALUES (1, '', 0, 1) \
             ON CONFLICT (id) DO UPDATE SET gap = 1",
        )
        .execute(&mut *conn)
        .await?;
        return Ok(Err(Received::Gap));
    };
    let named = (!place.snapshot.is_empty() || place.ends).then_some(place.snapshot);
    if named.is_some_and(|id| in_progress.as_deref() != Some(id)) {
        return Ok(Err(malformed(format!(
            "message {sequence} of a snapshot not in progress, `{}`",
            place.snapshot
        ))));
    }
    sqlx::query("UPDATE sync_applied SET sequence = $1 WHERE id = 1")
        .bind(sequence)
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

/// The text of a key that came on the wire: parsed by its table's codec and
/// written back in the one form keys are compared in. `None`, logged, when it
/// does not parse.
#[cfg(feature = "sqlite")]
fn wire_key<P: Sync + 'static>(codec: &Codec<P>, key: Option<&envelope::Key>) -> Option<String> {
    let Some(key) = key else {
        tracing::error!(
            table = codec.table.name,
            "sync: a row with no key beside it — not one to report"
        );
        return None;
    };
    codec
        .key(&key.parts)
        .inspect_err(|e| tracing::error!("sync: {e} — not one to report"))
        .ok()
        .map(|key| key_text(&key))
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
) -> Result<Vec<Applied>, Error> {
    let naming = !message.snapshot_id.is_empty();
    let mut taken = Vec::new();
    for table in &message.tables {
        if table.keys.len() != table.upserts.len() {
            tracing::error!(
                table = table.table,
                "sync: {} rows and {} keys beside them — a row that does not decode cannot be named",
                table.upserts.len(),
                table.keys.len()
            );
        }
        let Some(codec) = codec_of(codecs, &table.table).filter(|c| c.sent_down()) else {
            let why = if codec_of(codecs, &table.table).is_some() {
                "a table the server does not send"
            } else {
                "a table this replica does not mirror"
            };
            let keys: Vec<String> = table.keys.iter().map(|k| k.parts.join("|")).collect();
            reject(conn, &table.table, &keys, why, sequence).await?;
            continue;
        };
        let applied = codec.apply_down(conn, table.upserts.clone()).await?;
        let mut refused = Vec::new();
        for row in &applied.rejected {
            let key = match &row.key {
                Some(key) => Some(key_text(key)),
                None => wire_key(codec, table.keys.get(row.at)),
            };
            reject(
                conn,
                &table.table,
                &Vec::from_iter(key.clone()),
                &row.reason,
                sequence,
            )
            .await?;
            refused.extend(key);
        }
        let held: Vec<String> = applied.held.iter().map(key_text).collect();
        cleared(conn, &table.table, &held).await?;
        sqlx::query(
            "DELETE FROM sync_leaving WHERE relation = $1 \
               AND row_key IN (SELECT value FROM json_each($2))",
        )
        .bind(&table.table)
        .bind(json(&held))
        .execute(&mut *conn)
        .await?;
        if naming {
            // Every row it carried, taken or refused: a refused one keeps
            // what the replica had of it, and its rejection, and is not swept.
            let named: Vec<&String> = held.iter().chain(refused.iter()).collect();
            sqlx::query(
                "INSERT OR IGNORE INTO sync_snapshot_keys (table_name, row_key) \
                 SELECT $1, value FROM json_each($2)",
            )
            .bind(&table.table)
            .bind(json(&named))
            .execute(&mut *conn)
            .await?;
        }
        let deletes = keys_of(codec, &table.deletes);
        let_go(conn, codec, &deletes, sequence).await?;
        taken.push(Applied {
            table: table.table.clone(),
            keys: applied.held,
        });
    }
    Ok(taken)
}

/// Lets go of the rows of `keys`: the ones the database refuses to let go
/// are rejected, the rest are no longer ones to report. A row of a table
/// written on both sides whose edits have not reached the server is held
/// retired until they have ([`filed`]), so the edits are not lost.
#[cfg(feature = "sqlite")]
async fn let_go<P: Sync + 'static>(
    conn: &mut Conn,
    codec: &Codec<P>,
    keys: &[Key],
    sequence: i64,
) -> Result<(), Error> {
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
    let mut retired = codec.retire(conn, &free, false).await?;
    let holding = codec.retire(conn, &held, true).await?;
    let leaving: Vec<String> = held
        .iter()
        .filter(|key| !holding.refused.iter().any(|(r, _)| r == *key))
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
    retired.kept += holding.kept;
    retired.refused.extend(holding.refused);
    for (key, why) in &retired.refused {
        reject(conn, codec.table.name, &[key_text(key)], why, sequence).await?;
    }
    let gone: Vec<String> = keys
        .iter()
        .filter(|key| !retired.refused.iter().any(|(r, _)| r == *key))
        .map(key_text)
        .collect();
    cleared(conn, codec.table.name, &gone).await?;
    if retired.kept > 0 {
        tracing::info!(
            table = codec.table.name,
            kept = retired.kept,
            "sync: rows let go but still pointed at here, or with edits on their way — retired"
        );
    }
    tracing::debug!(
        table = codec.table.name,
        deleted = retired.deleted,
        kept = retired.kept,
        refused = retired.refused.len(),
        "sync: rows let go of"
    );
    Ok(())
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
        table,
        ?keys,
        sequence,
        "sync: rows this replica cannot take or let go of — rejected, for the server to hear of: {why}"
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
    .bind(why)
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
) -> Result<(), Error> {
    let began: i64 = sqlx::query_scalar("SELECT snapshot_began FROM sync_applied WHERE id = 1")
        .fetch_one(&mut *conn)
        .await?;
    for table in message.tables.iter().rev() {
        let Some(codec) = codec_of(codecs, table).filter(|c| c.sent_down()) else {
            tracing::error!(
                table,
                "sync: a snapshot of a table this replica does not hold from the server — not swept"
            );
            continue;
        };
        sqlx::query(
            "DELETE FROM sync_rejections WHERE table_name = $1 \
               AND (sequence < $2 OR epoch <> (SELECT epoch FROM sync_applied WHERE id = 1))",
        )
        .bind(table)
        .bind(began)
        .execute(&mut *conn)
        .await?;
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
        let_go(conn, codec, &stale, sequence).await?;
    }
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
) -> Result<(), Error> {
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
    for codec in codecs {
        let keys: Vec<Key> = done
            .iter()
            .filter(|(table, _)| table == codec.table.name)
            .filter_map(|(_, key)| {
                let parts: Vec<String> = key.split('|').map(str::to_owned).collect();
                codec.key(&parts).ok()
            })
            .collect();
        if keys.is_empty() {
            continue;
        }
        let retired = codec.retire(conn, &keys, false).await?;
        for (key, why) in retired.refused {
            tracing::error!(
                table = codec.table.name,
                key = key_text(&key),
                "sync: a row let go of, whose edits are filed, the database keeps — retired: {why}"
            );
        }
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
    let every = i64::try_from(settings.backlog_every.as_secs()).unwrap_or(i64::MAX);
    let clock = chrono::Utc::now().timestamp();
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
            limits: envelope::Limits {
                rows: 500,
                bytes: 1 << 20,
            },
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

/// A message on its way up, and what to record once the transport took it.
#[cfg(feature = "sqlite")]
#[derive(Debug)]
pub(crate) struct Outgoing {
    pub(crate) lane: &'static str,
    pub(crate) up: Up,
    /// The notes its rows carry.
    notes: Vec<i64>,
    /// Its rejections, each as refused by the message numbered so.
    rejected: Vec<(String, String, i64)>,
}

#[cfg(feature = "sqlite")]
impl Outgoing {
    /// As the bytes a transport carries.
    pub(crate) fn bytes(&self) -> Vec<u8> {
        self.up.encode_to_vec()
    }

    /// How many of the replica's writes it carries.
    pub(crate) fn rows(&self) -> usize {
        self.notes.len()
    }
}

/// What this replica has to tell the server on `lane` of `lanes`, as one
/// message within the outbox's limits: `None` when it has nothing. Rows it
/// wrote (as they are now), on every lane; on the first lane — the main one
/// — also the rows it could not take, the last message it applied when the
/// server has not heard of it, the backlog when it is due, and, when
/// `digests` is set, what it holds. Run it in a transaction begun with
/// `BEGIN IMMEDIATE`, and commit before the message goes: it reads all of
/// that as of one moment, and draws the upload's number. Hand the message
/// to the transport, then to [`delivered`] — or, when the transport refuses
/// it for good, to [`set_aside`].
///
/// A row that cannot go as it is — of a table this build does not send up, a
/// key it cannot read, a row that does not encode or is larger than a
/// message — is set aside, and counted in the backlog, until
/// [`retry_set_aside`].
#[cfg(feature = "sqlite")]
pub(crate) async fn outbox<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    lanes: &[Lane],
    lane: usize,
    settings: &Outbox,
    digests: bool,
) -> Result<Option<Outgoing>, Error> {
    check_settings(codecs, lanes)?;
    let Some(this) = lanes.get(lane) else {
        return Err(Error::Misuse(format!("no lane {lane} of {}", lanes.len())));
    };
    let main = lane == 0;
    let mut up = Up {
        protocol: envelope::PROTOCOL,
        ..Up::default()
    };
    let mut budget = settings.limits.largest_row();
    let mut rejected = Vec::new();
    let mut untold_gap = false;
    // Whether the last applied is news to the server.
    let mut news = false;
    if main {
        // Room for the backlog, whatever it says by the end: per table its
        // name and three numbers of ten bytes at most.
        let room: usize = progress(conn)
            .await?
            .iter()
            .map(|t| framed(framed(t.table.len()) + 3 * 11))
            .sum();
        budget = budget.saturating_sub(framed(room) + framed(0));
        if digests {
            up.digests = self::digests(conn, codecs).await?;
            let spent: usize = up.digests.iter().map(|d| framed(d.encoded_len())).sum();
            budget = budget.saturating_sub(spent);
        }
        // One over the cap says whether this message reports them all: the
        // server must hear of a refusal before the message it refused is
        // confirmed, or it would take the refused row for applied.
        let pending = unreported(conn, settings.rejections.get() + 1).await?;
        let mut all_reported = pending.len() <= settings.rejections.get();
        for (row, sequence) in pending.into_iter().take(settings.rejections.get()) {
            let weight = framed(row.encoded_len());
            if weight > budget {
                all_reported = false;
                break;
            }
            budget -= weight;
            let key = row
                .key
                .as_ref()
                .map(|k| k.parts.join("|"))
                .unwrap_or_default();
            rejected.push((row.table.clone(), key, sequence));
            up.rejected.push(row);
        }
        let applied: Option<(String, i64, i64, bool, bool)> = sqlx::query_as(
            "SELECT epoch, sequence, acknowledged, gap = 1, gap_told = 1 FROM sync_applied \
              WHERE id = 1",
        )
        .fetch_optional(&mut *conn)
        .await?;
        if let Some((_, _, _, true, told)) = &applied {
            up.gap = true;
            untold_gap = !*told;
            budget = budget.saturating_sub(2);
        }
        if let (Some((epoch, sequence, acknowledged, gap, _)), true) = (applied, all_reported) {
            // The last applied goes with digests always (they are compared
            // against it), with a gap (the server reads what it missed
            // against it), and otherwise when the server has not heard of it.
            news = sequence > acknowledged;
            if news || !up.digests.is_empty() || gap {
                up.epoch = epoch;
                up.sequence = u64::try_from(sequence).unwrap_or_default();
                budget = budget.saturating_sub(framed(up.epoch.len()) + 11);
            }
        }
    }
    let notes = unsent(conn, lanes, this, settings).await?;
    let gathered = gather(conn, codecs, notes, settings.limits, budget).await?;
    up.tables = gathered.tables;
    if !gathered.carried.is_empty() {
        // A random number, a fresh one on every send: no answer meant for
        // another replica, or for this one before a restore, names its notes.
        let number: i64 = sqlx::query_scalar("SELECT (random() & 9223372036854775807) | 1")
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

/// Up to a message's rows of notes to send on `lane`, oldest first: the ones
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
                 OR sent_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-{} seconds') \
                 OR sent_at > {now}) {filter} \
          ORDER BY id LIMIT $1",
        settings.resend_after.as_secs()
    ))
    .bind(i64::try_from(settings.limits.rows).unwrap_or(i64::MAX))
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
/// message; a note that cannot go as it is is set aside.
#[cfg(feature = "sqlite")]
async fn gather<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    notes: Vec<Note>,
    limits: Limits,
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
            let opening = if part.upserts.is_empty() {
                opened(&part.table)
            } else {
                0
            };
            let weight = framed(bytes.len()) + framed(wire.encoded_len()) + opening;
            if framed(bytes.len()) + framed(wire.encoded_len()) + opened(&part.table)
                > limits.largest_row()
            {
                let why = format!(
                    "a row of {} bytes, more than a message carries",
                    bytes.len()
                );
                aside.push((id, why));
                continue;
            }
            if full || weight > budget {
                full = true;
                continue;
            }
            budget -= weight;
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
        for (key, id) in keys {
            let wire = wire_of(&key);
            let opening = if part.deletes.is_empty() {
                opened(&part.table)
            } else {
                0
            };
            let weight = framed(wire.encoded_len()) + opening;
            if full || weight > budget {
                full = true;
                continue;
            }
            budget -= weight;
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
/// again since), and the server of its epoch knows what was applied. Run it
/// in one transaction.
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
    for (table, key, sequence) in &outgoing.rejected {
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
    if let Some(backlog) = &outgoing.up.backlog {
        let tables: Vec<UpProgress> = backlog.tables.iter().map(UpProgress::from).collect();
        sqlx::query(
            "INSERT INTO sync_backlog (id, backlog, reported_at) VALUES (1, $1, $2) \
             ON CONFLICT (id) DO UPDATE SET backlog = EXCLUDED.backlog, reported_at = EXCLUDED.reported_at",
        )
        .bind(sqlx::types::Json(tables))
        .bind(chrono::Utc::now().timestamp())
        .execute(&mut *conn)
        .await?;
    }
    if outgoing.up.gap {
        sqlx::query("UPDATE sync_applied SET gap_told = 1 WHERE id = 1 AND gap = 1")
            .execute(&mut *conn)
            .await?;
    }
    if outgoing.up.sequence > 0 {
        sqlx::query(
            "UPDATE sync_applied SET acknowledged = max(acknowledged, $2) WHERE id = 1 AND epoch = $1",
        )
        .bind(&outgoing.up.epoch)
        .bind(i64::try_from(outgoing.up.sequence).unwrap_or(i64::MAX))
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

/// The transport refused `outgoing` for good: its rows are set aside, with
/// why, until [`retry_set_aside`]. Everything else it carried goes again.
#[cfg(feature = "sqlite")]
pub(crate) async fn set_aside(
    conn: &mut Conn,
    outgoing: &Outgoing,
    why: &str,
) -> Result<(), Error> {
    sqlx::query(
        "UPDATE sync_notes SET set_aside = $1 \
          WHERE upload = $3 AND id IN (SELECT value FROM json_each($2))",
    )
    .bind(why)
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
/// server to compare with what it records: rows it retired, or rejected,
/// left out.
#[cfg(feature = "sqlite")]
async fn digests<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
) -> Result<Vec<envelope::Digest>, Error> {
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
    Ok(digests)
}

#[cfg(all(test, feature = "sqlite"))]
#[path = "replica_tests.rs"]
mod tests;
