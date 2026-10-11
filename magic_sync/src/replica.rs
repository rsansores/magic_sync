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
/// - a table that goes up notes every write in `sync_notes` — the row's key,
///   in the one form keys are compared in, and on an update that changes it
///   the old key too, which is gone — and refuses a key stored in a form the
///   engine does not read back (a uuid is a 16-byte blob, an integer an
///   integer);
/// - a record without a version is history: an update of one is refused;
/// - a row of a table written on both sides begins on the server: an insert
///   is refused, and only an update of the columns the replica owns goes up.
///
/// A write the engine makes while it applies what the server sent is noted
/// by none and refused by none.
#[cfg(feature = "sqlite")]
fn generated<P: Sync + 'static>(codecs: &[Codec<P>]) -> Vec<Trigger> {
    use crate::row::Mirror;
    const QUIET: &str = " WHEN NOT EXISTS (SELECT 1 FROM sync_applying)";
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
                "INSERT INTO sync_notes (relation, row_key) VALUES ({}, {}); ",
                literal(table),
                key(row)
            )
        };
        let gone = format!(
            "INSERT INTO sync_notes (relation, row_key) SELECT {}, {} WHERE {} IS NOT {}; ",
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
                let owned: Vec<String> = codec
                    .table
                    .columns
                    .iter()
                    .filter(|c| c.replica_owned)
                    .map(|c| ident(c.name))
                    .collect();
                let of = if owned.is_empty() {
                    String::new()
                } else {
                    format!(" OF {}", owned.join(", "))
                };
                let body = format!("{guard}{}{gone}", note("NEW"));
                out.push(noting("UPDATE", &of, QUIET, body));
                out.push(noting("DELETE", "", QUIET, note("OLD")));
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
            filed(conn, &message).await?;
            Ok(Received::Filed)
        }
        Body::Down(down) => match numbered(conn, &down.epoch, down.sequence).await? {
            Ok(sequence) => Ok(Received::Applied(
                apply(conn, codecs, &down, sequence).await?,
            )),
            Err(skipped) => Ok(skipped),
        },
        Body::EndSnapshot(end) => match numbered(conn, &end.epoch, end.sequence).await? {
            Ok(sequence) => {
                end_snapshot(conn, codecs, &end, sequence).await?;
                Ok(Received::SnapshotEnded)
            }
            Err(skipped) => Ok(skipped),
        },
    }
}

/// Whether a message numbered `sequence` by the server installation `epoch`
/// is newer than the last one applied, recording it as the last when it is:
/// its sequence when it is, and otherwise what it did instead — nothing, or
/// nothing because no server numbers a message so.
/// A message of another epoch is of a server installed again, which knows
/// nothing it was told: the replica forgets the snapshot it was following,
/// and says again its backlog and its rejections.
#[cfg(feature = "sqlite")]
async fn numbered(
    conn: &mut Conn,
    epoch: &str,
    sequence: u64,
) -> Result<Result<i64, Received>, Error> {
    let Some(sequence) = i64::try_from(sequence).ok().filter(|s| *s > 0) else {
        return Ok(Err(malformed(format!(
            "sequence {sequence}, outside 1 to 2^63 - 1"
        ))));
    };
    let Ok(epoch) = Uuid::parse_str(epoch).map(|e| e.to_string()) else {
        return Ok(Err(malformed(format!("epoch `{epoch}` is not a uuid"))));
    };
    let last: Option<(String, i64)> =
        sqlx::query_as("SELECT epoch, sequence FROM sync_applied WHERE id = 1")
            .fetch_optional(&mut *conn)
            .await?;
    if last
        .as_ref()
        .is_some_and(|(e, s)| *e == epoch && sequence <= *s)
    {
        tracing::info!(
            sequence,
            "sync: a message at or below the last applied — nothing to do"
        );
        return Ok(Err(Received::AlreadyApplied));
    }
    if last.is_some_and(|(e, _)| e != epoch) {
        for forgotten in [
            "DELETE FROM sync_backlog",
            "DELETE FROM sync_snapshots",
            "DELETE FROM sync_snapshot_keys",
            "UPDATE sync_rejections SET reported_at = NULL, sequence = 0",
        ] {
            sqlx::query(forgotten).execute(&mut *conn).await?;
        }
    }
    sqlx::query(
        "INSERT INTO sync_applied (id, epoch, sequence) VALUES (1, $1, $2) \
         ON CONFLICT (id) DO UPDATE SET epoch = EXCLUDED.epoch, sequence = EXCLUDED.sequence, \
           acknowledged = CASE WHEN sync_applied.epoch = EXCLUDED.epoch THEN sync_applied.acknowledged ELSE 0 END",
    )
    .bind(epoch)
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
    let naming = if message.snapshot_id.is_empty() {
        false
    } else {
        names(conn, &message.snapshot_id, sequence).await?
    };
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
        for refused in &applied.rejected {
            let key = match &refused.key {
                Some(key) => Some(key_text(key)),
                None => wire_key(codec, table.keys.get(refused.at)),
            };
            reject(
                conn,
                &table.table,
                &Vec::from_iter(key),
                &refused.reason,
                sequence,
            )
            .await?;
        }
        let held: Vec<String> = applied.held.iter().map(key_text).collect();
        cleared(conn, &table.table, &held).await?;
        if naming {
            sqlx::query(
                "INSERT OR IGNORE INTO sync_snapshot_keys (table_name, row_key) \
                 SELECT $1, value FROM json_each($2)",
            )
            .bind(&table.table)
            .bind(json(&held))
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
/// are rejected, the rest are no longer ones to report.
#[cfg(feature = "sqlite")]
async fn let_go<P: Sync + 'static>(
    conn: &mut Conn,
    codec: &Codec<P>,
    keys: &[Key],
    sequence: i64,
) -> Result<crate::row::Retired, Error> {
    let retired = codec.retire(conn, keys).await?;
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
            "sync: rows let go but still pointed at here — retired"
        );
    }
    Ok(retired)
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
        "INSERT INTO sync_rejections (table_name, row_key, reason, sequence) \
         SELECT $1, value, $3, $4 FROM json_each($2) WHERE true \
         ON CONFLICT (table_name, row_key) DO UPDATE SET reason = EXCLUDED.reason, \
           sequence = EXCLUDED.sequence, rejected_at = EXCLUDED.rejected_at, reported_at = NULL",
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

/// The state of the snapshot `id`: `None` when never seen, `Some(None)` while
/// in progress, `Some(Some(sequence))` once it ended or was superseded.
#[cfg(feature = "sqlite")]
async fn snapshot(conn: &mut Conn, id: &str) -> Result<Option<Option<i64>>, Error> {
    Ok(
        sqlx::query_scalar("SELECT ended FROM sync_snapshots WHERE snapshot_id = $1")
            .bind(id)
            .fetch_optional(&mut *conn)
            .await?,
    )
}

/// Begins the snapshot `id` with the message numbered `sequence`: the one in
/// progress, if any, is superseded, and what it named forgotten.
#[cfg(feature = "sqlite")]
async fn begin(conn: &mut Conn, id: &str, sequence: i64) -> Result<(), Error> {
    sqlx::query("UPDATE sync_snapshots SET ended = $1 WHERE ended IS NULL")
        .bind(sequence)
        .execute(&mut *conn)
        .await?;
    sqlx::query("DELETE FROM sync_snapshot_keys")
        .execute(&mut *conn)
        .await?;
    sqlx::query("INSERT INTO sync_snapshots (snapshot_id, began) VALUES ($1, $2)")
        .bind(id)
        .bind(sequence)
        .execute(&mut *conn)
        .await?;
    // A handful of ended snapshots is all a late message can refer to.
    sqlx::query(
        "DELETE FROM sync_snapshots WHERE snapshot_id NOT IN \
         (SELECT snapshot_id FROM sync_snapshots ORDER BY began DESC LIMIT 20)",
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Whether a message of the snapshot `id`, numbered `sequence`, names the
/// rows it carries: it does in the snapshot in progress, and in one never
/// seen, which it begins — whatever the snapshot id says, the message's
/// sequence orders it. One that ended names nothing; its rows still apply.
#[cfg(feature = "sqlite")]
async fn names(conn: &mut Conn, id: &str, sequence: i64) -> Result<bool, Error> {
    match snapshot(conn, id).await? {
        Some(None) => Ok(true),
        Some(Some(_)) => Ok(false),
        None => {
            begin(conn, id, sequence).await?;
            Ok(true)
        }
    }
}

/// A snapshot is complete: every row of its tables it did not name goes,
/// children first — the server lists them parents first. The end of one
/// never seen begins and ends it: it named nothing, so every row of its
/// tables goes. The end of one that ended or was superseded does nothing.
/// What was rejected in those tables before it began is forgotten: it sent
/// again what the replica should hold.
#[cfg(feature = "sqlite")]
async fn end_snapshot<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    message: &EndSnapshot,
    sequence: i64,
) -> Result<(), Error> {
    let id = &message.snapshot_id;
    match snapshot(conn, id).await? {
        Some(Some(_)) => {
            tracing::info!(
                snapshot = id,
                "sync: the end of a snapshot that ended — nothing to sweep"
            );
            return Ok(());
        }
        Some(None) => {}
        None => begin(conn, id, sequence).await?,
    }
    let began: i64 = sqlx::query_scalar("SELECT began FROM sync_snapshots WHERE snapshot_id = $1")
        .bind(id)
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
        sqlx::query("DELETE FROM sync_rejections WHERE table_name = $1 AND sequence < $2")
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
        let retired = let_go(conn, codec, &stale, sequence).await?;
        tracing::info!(
            table,
            deleted = retired.deleted,
            kept = retired.kept,
            refused = retired.refused.len(),
            "sync: snapshot swept what it did not name"
        );
    }
    sqlx::query("DELETE FROM sync_snapshot_keys")
        .execute(&mut *conn)
        .await?;
    sqlx::query("UPDATE sync_snapshots SET ended = $2 WHERE snapshot_id = $1")
        .bind(id)
        .bind(sequence)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// The server filed these uploads: the notes they carried are done. A
/// number no upload has is ignored.
#[cfg(feature = "sqlite")]
async fn filed(conn: &mut Conn, message: &envelope::Filed) -> Result<(), Error> {
    let uploads: Vec<i64> = message
        .uploads
        .iter()
        .filter_map(|u| i64::try_from(*u).ok())
        .collect();
    sqlx::query("DELETE FROM sync_notes WHERE upload IN (SELECT value FROM json_each($1))")
        .bind(json(&uploads))
        .execute(&mut *conn)
        .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// What goes up.

/// How far this replica is from having sent the server what it wrote, per
/// table: rows noted and not sent yet, rows sent the server has not said it
/// filed, and rows that cannot go as they are.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
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
    Ok(sqlx::query_as(
        "SELECT relation AS \"table\", \
                sum(set_aside IS NULL AND upload IS NULL) AS unsent, \
                sum(set_aside IS NULL AND upload IS NOT NULL) AS unfiled, \
                sum(set_aside IS NOT NULL) AS set_aside \
           FROM sync_notes GROUP BY relation ORDER BY relation",
    )
    .fetch_all(&mut *conn)
    .await?)
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

/// Refuses lanes a replica cannot send up with: no lane, a table on two
/// lanes or on none, a lane naming a table that does not go up, or two lanes
/// for every other table.
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
        let applied: Option<(String, i64, i64)> =
            sqlx::query_as("SELECT epoch, sequence, acknowledged FROM sync_applied WHERE id = 1")
                .fetch_optional(&mut *conn)
                .await?;
        if let (Some((epoch, sequence, acknowledged)), true) = (applied, all_reported) {
            // The last applied goes with digests always (they are compared
            // against it), and otherwise when the server has not heard of it.
            if sequence > acknowledged || !up.digests.is_empty() {
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
        let number: i64 = sqlx::query_scalar(
            "UPDATE sync_uploads SET last = last + 1 WHERE id = 1 RETURNING last",
        )
        .fetch_one(&mut *conn)
        .await?;
        up.uploads = vec![u64::try_from(number).unwrap_or_default()];
    }
    let sending = !up.tables.is_empty()
        || !up.rejected.is_empty()
        || !up.digests.is_empty()
        || up.sequence > 0;
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
/// never sent, and the ones sent longer than `resend_after` ago — or, by a
/// clock set back since, in the future — that the server never said it
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
            AND (upload IS NULL \
                 OR sent_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-{} seconds') \
                 OR sent_at > {now}) {filter} \
          ORDER BY id LIMIT $1",
        settings.resend_after.as_secs()
    ))
    .bind(i64::try_from(settings.limits.rows).unwrap_or(i64::MAX))
    .fetch_all(&mut *conn)
    .await?)
}

/// Keys of one table, each with the notes of it.
#[cfg(feature = "sqlite")]
type Keyed = Vec<(Key, Vec<i64>)>;

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
    // Per table, each key with the notes of it, in the order first noted.
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
        let keys = keyed.entry(codec.table.name).or_default();
        match keys.iter_mut().find(|(k, _)| *k == key) {
            Some((_, ids)) => ids.push(note.id),
            None => keys.push((key, vec![note.id])),
        }
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
        for (key, ids) in keys {
            if let Some((_, e)) = encoded.failed.iter().find(|(k, _)| *k == key) {
                aside.extend(ids.iter().map(|id| (*id, e.to_string())));
                continue;
            }
            let Some((_, bytes)) = encoded.rows.iter().find(|(k, _)| *k == key) else {
                missing.push((key, ids));
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
                aside.extend(ids.iter().map(|id| (*id, why.clone())));
                continue;
            }
            if full || weight > budget {
                full = true;
                continue;
            }
            budget -= weight;
            part.upserts.push(bytes.clone());
            part.keys.push(wire);
            carried.extend(ids);
        }
        if !part.upserts.is_empty() {
            tables.push(part);
        }
        if codec.takes_deletes() {
            gone.push((codec, missing));
        } else {
            done.extend(missing.into_iter().flat_map(|(_, ids)| ids));
        }
    }
    for (codec, keys) in gone.into_iter().rev() {
        let mut part = Table {
            table: codec.table.name.to_owned(),
            ..Table::default()
        };
        for (key, ids) in keys {
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
            carried.extend(ids);
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

/// The rejections the server has not heard of, oldest first, up to `limit`,
/// each with the message that refused it.
#[cfg(feature = "sqlite")]
async fn unreported(
    conn: &mut Conn,
    limit: usize,
) -> Result<Vec<(envelope::Rejected, i64)>, Error> {
    let rows: Vec<(String, String, String, i64)> = sqlx::query_as(
        "SELECT table_name, row_key, reason, sequence FROM sync_rejections \
          WHERE reported_at IS NULL ORDER BY rejected_at, table_name, row_key LIMIT $1",
    )
    .bind(i64::try_from(limit).unwrap_or(i64::MAX))
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(table, key, reason, sequence)| {
            let row = envelope::Rejected {
                table,
                key: Some(envelope::Key {
                    parts: key.split('|').map(str::to_owned).collect(),
                }),
                reason,
            };
            (row, sequence)
        })
        .collect())
}

/// The transport took `outgoing`: its notes wait for the server's
/// [`Filed`](envelope::Filed), its rejections are reported (unless refused
/// again since), and the server of its epoch knows what was applied. Run it
/// in one transaction.
#[cfg(feature = "sqlite")]
pub(crate) async fn delivered(conn: &mut Conn, outgoing: &Outgoing) -> Result<(), Error> {
    if let Some(number) = outgoing.up.uploads.first() {
        sqlx::query(&format!(
            "UPDATE sync_notes SET upload = $1, sent_at = {} \
              WHERE id IN (SELECT value FROM json_each($2))",
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
        "UPDATE sync_notes SET set_aside = $1 WHERE id IN (SELECT value FROM json_each($2))",
    )
    .bind(why)
    .bind(json(&outgoing.notes))
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
