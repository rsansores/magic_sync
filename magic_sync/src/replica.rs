//! A replica's side: what the server sends, applied table by table through
//! the codecs; what the replica wrote, gathered to go up; and, on the server,
//! what a replica sent, filed. Every function works in the caller's
//! transaction and names no table — the codecs do.

use crate::envelope::{self, Body, Down, EndSnapshot, ToReplica, Up};
use crate::row::{key_text, Conn, Key};
use crate::wire::Codec;
use crate::Error;
use uuid::Uuid;

#[cfg(all(feature = "sqlite", not(feature = "postgres")))]
/// The tables a replica writes that go up: each needs a trigger noting its
/// writes.
fn written_here<P>(codecs: &[Codec<P>]) -> impl Iterator<Item = &Codec<P>> {
    use crate::row::Mirror;
    codecs
        .iter()
        .filter(|c| matches!(c.table.mirror, Mirror::Record | Mirror::Both))
}

#[cfg(all(feature = "sqlite", not(feature = "postgres")))]
/// Refuses a replica database the engine cannot run against: another
/// version of its tables, or a table that goes up with no trigger noting its
/// writes (a write nothing notes never reaches the server). A host with
/// migrations runs it in a test, over the database its migrations build.
pub async fn check<'a, P: 'a>(
    conn: &mut Conn,
    codecs: impl IntoIterator<Item = &'a Codec<P>>,
) -> Result<(), Error> {
    let versioned: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'sync_version')",
    )
    .fetch_one(&mut *conn)
    .await?;
    let version: Option<i32> = if versioned {
        sqlx::query_scalar("SELECT version FROM sync_version")
            .fetch_optional(&mut *conn)
            .await?
    } else {
        None
    };
    if version != Some(crate::SCHEMA_VERSION) {
        return Err(Error::Misuse(format!(
            "the engine's tables are version {}, and this build needs {}: install them",
            version.map_or("none".to_owned(), |v| v.to_string()),
            crate::SCHEMA_VERSION
        )));
    }
    for codec in codecs.into_iter().filter(|c| {
        matches!(
            c.table.mirror,
            crate::row::Mirror::Record | crate::row::Mirror::Both
        )
    }) {
        let noted: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'trigger' \
               AND tbl_name = $1 AND sql LIKE '%sync_notes%')",
        )
        .bind(codec.table.name)
        .fetch_one(&mut *conn)
        .await?;
        if !noted {
            return Err(Error::Misuse(format!(
                "`{}` goes up but nothing notes its writes: give it a trigger into sync_notes",
                codec.table.name
            )));
        }
    }
    Ok(())
}

#[cfg(all(feature = "sqlite", not(feature = "postgres")))]
/// Installs what a replica database lacks: the engine's tables when it has
/// none, and a trigger noting the writes of every table that goes up.
/// Idempotent. For a host with no migrations of its own; run it before
/// anything writes. A host with migrations runs
/// [`SQLITE_SCHEMA`](crate::SQLITE_SCHEMA) in one and writes each table's
/// trigger in the migration that creates it; [`check`] refuses one missing.
pub async fn install<P>(conn: &mut Conn, codecs: &[Codec<P>]) -> Result<(), Error> {
    let installed: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'sync_notes')",
    )
    .fetch_one(&mut *conn)
    .await?;
    if !installed {
        sqlx::raw_sql(crate::SQLITE_SCHEMA)
            .execute(&mut *conn)
            .await?;
    }
    for codec in written_here(codecs) {
        let table = codec.table.name;
        let key = |row: &str| {
            codec
                .table
                .key
                .iter()
                .map(|c| {
                    format!(
                        "CASE typeof({row}.{c}) WHEN 'blob' THEN lower(hex({row}.{c})) \
                         ELSE CAST({row}.{c} AS TEXT) END"
                    )
                })
                .collect::<Vec<_>>()
                .join(" || '|' || ")
        };
        for (event, row) in [("INSERT", "NEW"), ("UPDATE", "NEW"), ("DELETE", "OLD")] {
            sqlx::raw_sql(&format!(
                "CREATE TRIGGER IF NOT EXISTS sync_note_{table}_{} AFTER {event} ON {table} BEGIN \
                   INSERT INTO sync_notes (relation, row_key) VALUES ('{table}', {}); \
                 END",
                event.to_lowercase(),
                key(row)
            ))
            .execute(&mut *conn)
            .await?;
        }
    }
    check(conn, codecs).await
}

/// The codec of `table`, when this build mirrors it.
pub fn codec_of<'a, P>(codecs: &'a [Codec<P>], table: &str) -> Option<&'a Codec<P>> {
    codecs.iter().find(|c| c.table.name == table)
}

/// Per table, the keys a message left the replica holding.
pub type Applied = Vec<(String, Vec<Key>)>;

/// What a message from the server did.
#[derive(Debug)]
pub enum Received {
    /// Rows applied: per table, the keys the replica now holds.
    Applied(Applied),
    /// A snapshot ended, and what it did not name went.
    SnapshotEnded,
    /// Uploads the server filed: their notes are done.
    Filed,
    /// Bytes that are no message at all, dropped. Nothing names what they
    /// carried; the digests find it, if it mattered.
    Unreadable(String),
}

/// A message from the server, as the transport delivered it, acted on in the
/// caller's transaction. The caller commits, then acknowledges the message to
/// the transport — never before: a message acknowledged and not committed
/// would never come again.
///
/// An error means the message must come again: the replica's own storage
/// failed, or the message speaks a later protocol than this build
/// ([`Error::Protocol`]) and waits for it to be updated.
pub async fn receive<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    bytes: &[u8],
) -> Result<Received, Error> {
    use prost::Message as _;
    let message = match ToReplica::decode(bytes) {
        Ok(message) => message,
        Err(e) => {
            tracing::error!("sync: a message that does not decode — dropped: {e}");
            return Ok(Received::Unreadable(e.to_string()));
        }
    };
    if message.protocol > envelope::PROTOCOL {
        return Err(Error::Protocol {
            theirs: message.protocol,
            ours: envelope::PROTOCOL,
        });
    }
    match message.body {
        Some(Body::Down(down)) => Ok(Received::Applied(apply(conn, codecs, &down).await?)),
        Some(Body::EndSnapshot(end)) => {
            end_snapshot(conn, codecs, &end).await?;
            Ok(Received::SnapshotEnded)
        }
        Some(Body::Filed(message)) => {
            filed(conn, &message).await?;
            Ok(Received::Filed)
        }
        None => {
            tracing::error!("sync: a message with nothing in it — dropped");
            Ok(Received::Unreadable(
                "a message with nothing in it".to_owned(),
            ))
        }
    }
}

/// Rows to hold and rows to let go, in the order the server put them.
///
/// A message at or below the last one applied from the same server changes
/// nothing: a redelivery, or one a relay that lost its lock sent late. A row a
/// row of the replica's own still points at is retired rather than deleted.
/// A row the replica cannot take — a table this build does not mirror, a row
/// that does not decode, one the database refuses for its data — is left out
/// and noted as rejected, for [`rejections`] to report, rather than holding
/// back the queue behind it; a storage failure is the error.
pub async fn apply<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    message: &Down,
) -> Result<Applied, Error> {
    if !newer(conn, &message.epoch, message.sequence).await? {
        return Ok(Vec::new());
    }
    let snapshot = if message.snapshot_id.is_empty() {
        None
    } else {
        begun(conn, &message.snapshot_id).await?
    };
    let mut taken = Vec::new();
    for table in &message.tables {
        let key_at = |at: usize| {
            table
                .keys
                .get(at)
                .map(|k| k.parts.join("|"))
                .unwrap_or_default()
        };
        let Some(codec) = codec_of(codecs, &table.table) else {
            tracing::warn!(
                table = table.table,
                "sync: a table this replica does not mirror — rejected"
            );
            for at in 0..table.upserts.len() {
                reject(
                    conn,
                    &table.table,
                    &key_at(at),
                    "a table this replica does not mirror",
                )
                .await?;
            }
            continue;
        };
        let applied = match codec.apply_down(conn, table.upserts.clone()).await {
            Ok(applied) => applied,
            Err(e) if !e.is_transient() => {
                tracing::error!(
                    table = table.table,
                    "sync: rows this replica cannot take — rejected: {e}"
                );
                for at in 0..table.upserts.len() {
                    reject(conn, &table.table, &key_at(at), &e.to_string()).await?;
                }
                continue;
            }
            Err(e) => return Err(e),
        };
        for (at, why) in &applied.rejected {
            reject(conn, &table.table, &key_at(*at), why).await?;
        }
        let deletes = keys_of(codec, &table.deletes);
        let retired = codec.retire(conn, &deletes).await?;
        if retired.kept > 0 {
            tracing::info!(
                table = table.table,
                kept = retired.kept,
                "sync: rows let go but still pointed at here — retired"
            );
        }
        for key in &applied.held {
            // Taken now: no longer one to report.
            sqlx::query("DELETE FROM sync_rejections WHERE table_name = $1 AND row_key = $2")
                .bind(&table.table)
                .bind(key_text(key))
                .execute(&mut *conn)
                .await?;
            if let Some(snapshot) = snapshot {
                sqlx::query(
                    "INSERT INTO sync_snapshot_keys (snapshot_id, table_name, row_key) \
                     VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                )
                .bind(snapshot)
                .bind(&table.table)
                .bind(key_text(key))
                .execute(&mut *conn)
                .await?;
            }
        }
        taken.push((table.table.clone(), applied.held));
    }
    Ok(taken)
}

/// Whether a message numbered `sequence` by the server installation `epoch`
/// is newer than the last one applied, recording it as the last when it is.
/// An unnumbered message (sequence 0) is always applied.
async fn newer(conn: &mut Conn, epoch: &str, sequence: u64) -> Result<bool, Error> {
    if sequence == 0 {
        return Ok(true);
    }
    let sequence = i64::try_from(sequence).unwrap_or(i64::MAX);
    let last: Option<(String, i64)> =
        sqlx::query_as("SELECT epoch, sequence FROM sync_applied WHERE id = 1")
            .fetch_optional(&mut *conn)
            .await?;
    if last.is_some_and(|(e, s)| e == epoch && sequence <= s) {
        tracing::info!(
            sequence,
            "sync: a message at or below the last applied — nothing to do"
        );
        return Ok(false);
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
    Ok(true)
}

/// What to tell the server about the messages this replica applied since it
/// last did: `None` when it is up to date. The server counts what those
/// messages carried as held only once told; [`acknowledged`] records that it
/// was.
pub async fn acknowledgement(conn: &mut Conn, replica: Uuid) -> Result<Option<Up>, Error> {
    let applied: Option<(String, i64, i64)> =
        sqlx::query_as("SELECT epoch, sequence, acknowledged FROM sync_applied WHERE id = 1")
            .fetch_optional(&mut *conn)
            .await?;
    Ok(applied
        .filter(|(_, sequence, acknowledged)| sequence > acknowledged)
        .map(|(epoch, sequence, _)| Up {
            replica_id: replica.to_string(),
            epoch,
            sequence: u64::try_from(sequence).unwrap_or_default(),
            protocol: envelope::PROTOCOL,
            ..Up::default()
        }))
}

/// Records that the server has been told of every message up to `sequence`.
pub async fn acknowledged(conn: &mut Conn, sequence: u64) -> Result<(), Error> {
    sqlx::query("UPDATE sync_applied SET acknowledged = CASE WHEN acknowledged < $1 THEN $1 ELSE acknowledged END WHERE id = 1")
        .bind(i64::try_from(sequence).unwrap_or(i64::MAX))
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Notes a row the replica could not take, for the server to hear of.
async fn reject(conn: &mut Conn, table: &str, key: &str, why: &str) -> Result<(), Error> {
    sqlx::query(
        "INSERT INTO sync_rejections (table_name, row_key, reason) VALUES ($1, $2, $3) \
         ON CONFLICT (table_name, row_key) DO UPDATE SET reason = EXCLUDED.reason, reported_at = NULL",
    )
    .bind(table)
    .bind(key)
    .bind(why)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// The rows this replica could not take that the server has not heard of,
/// up to `limit`, to go up in an [`Up`]; [`reported`] marks them once they
/// have. They stay until the replica takes them, so its digests leave them
/// out as the server's quarantine does.
pub async fn rejections(conn: &mut Conn, limit: i64) -> Result<Vec<envelope::Rejected>, Error> {
    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT table_name, row_key, reason FROM sync_rejections WHERE reported_at IS NULL \
          ORDER BY rejected_at LIMIT $1",
    )
    .bind(limit)
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(table, key, reason)| envelope::Rejected {
            table,
            key: Some(envelope::Key {
                parts: key.split('|').map(str::to_owned).collect(),
            }),
            reason,
        })
        .collect())
}

/// Marks rejections the server has been told of.
pub async fn reported(conn: &mut Conn, rejected: &[envelope::Rejected]) -> Result<(), Error> {
    for row in rejected {
        let key = row
            .key
            .as_ref()
            .map(|k| k.parts.join("|"))
            .unwrap_or_default();
        sqlx::query(&format!(
            "UPDATE sync_rejections SET reported_at = {} WHERE table_name = $1 AND row_key = $2",
            crate::dialect::NOW
        ))
        .bind(&row.table)
        .bind(key)
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

/// The keys of `wire` that parse as `codec`'s; one that does not is logged
/// and left out.
fn keys_of<P: Sync + 'static>(codec: &Codec<P>, wire: &[envelope::Key]) -> Vec<Key> {
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

/// The snapshot a message belongs to, while it is the one in progress: the
/// first message of a newer one supersedes it and drops what it had named.
/// `None` for one that has ended or been superseded — its rows still apply,
/// but it names nothing.
async fn begun<'a>(conn: &mut Conn, snapshot: &'a str) -> Result<Option<&'a str>, Error> {
    let known: Option<Option<String>> =
        sqlx::query_scalar("SELECT ended_at FROM sync_snapshots WHERE snapshot_id = $1")
            .bind(snapshot)
            .fetch_optional(&mut *conn)
            .await?;
    match known {
        Some(None) => return Ok(Some(snapshot)),
        Some(Some(_)) => return Ok(None),
        None => {}
    }
    // Snapshot ids are v7 uuids: a larger one began later.
    let superseded: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM sync_snapshots WHERE snapshot_id > $1)")
            .bind(snapshot)
            .fetch_one(&mut *conn)
            .await?;
    if superseded {
        return Ok(None);
    }
    sqlx::query(&format!(
        "UPDATE sync_snapshots SET ended_at = {} WHERE ended_at IS NULL",
        crate::dialect::NOW
    ))
    .execute(&mut *conn)
    .await?;
    sqlx::query("DELETE FROM sync_snapshot_keys")
        .execute(&mut *conn)
        .await?;
    sqlx::query("INSERT INTO sync_snapshots (snapshot_id) VALUES ($1)")
        .bind(snapshot)
        .execute(&mut *conn)
        .await?;
    // A handful of ended snapshots is all a late message can refer to.
    sqlx::query(
        "DELETE FROM sync_snapshots WHERE snapshot_id NOT IN \
         (SELECT snapshot_id FROM sync_snapshots ORDER BY snapshot_id DESC LIMIT 20)",
    )
    .execute(&mut *conn)
    .await?;
    Ok(Some(snapshot))
}

/// A snapshot is complete: every row of its tables it did not name goes,
/// children first — the server lists them parents first. Only the snapshot
/// in progress sweeps: an end delivered again, or one for a snapshot a newer
/// one superseded, finds nothing to do.
pub async fn end_snapshot<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    message: &EndSnapshot,
) -> Result<(), Error> {
    if !newer(conn, &message.epoch, message.sequence).await? {
        return Ok(());
    }
    let snapshot = &message.snapshot_id;
    let in_progress: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM sync_snapshots WHERE snapshot_id = $1 AND ended_at IS NULL)",
    )
    .bind(snapshot)
    .fetch_one(&mut *conn)
    .await?;
    if !in_progress {
        tracing::info!(
            snapshot,
            "sync: the end of a snapshot not in progress — nothing to sweep"
        );
        return Ok(());
    }
    for table in message.tables.iter().rev() {
        let Some(codec) = codec_of(codecs, table) else {
            continue;
        };
        let named: std::collections::HashSet<String> = sqlx::query_scalar(
            "SELECT row_key FROM sync_snapshot_keys WHERE snapshot_id = $1 AND table_name = $2",
        )
        .bind(snapshot)
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
        let retired = codec.retire(conn, &stale).await?;
        tracing::info!(
            table,
            deleted = retired.deleted,
            kept = retired.kept,
            "sync: snapshot swept what it did not name"
        );
    }
    sqlx::query("DELETE FROM sync_snapshot_keys WHERE snapshot_id = $1")
        .bind(snapshot)
        .execute(&mut *conn)
        .await?;
    sqlx::query(&format!(
        "UPDATE sync_snapshots SET ended_at = {} WHERE snapshot_id = $1",
        crate::dialect::NOW
    ))
    .bind(snapshot)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// What `replica` wrote, as one upload: the noted rows `(table, key)` as they
/// are now, each by its table's codec, and a row that is gone as its key —
/// only for a table keyed by its replica, since history a replica let go of
/// before it went up is not the server's to delete. A note for a table this
/// build does not mirror, or whose key does not parse, is logged and left out.
pub async fn collect<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    replica: Uuid,
    noted: &[(String, String)],
) -> Result<Up, Error> {
    let mut tables = Vec::new();
    for group in noted.chunk_by(|a, b| a.0 == b.0) {
        let table = &group[0].0;
        let Some(codec) = codec_of(codecs, table) else {
            tracing::error!(table, "sync: notes for a table with no codec — dropped");
            continue;
        };
        let wire: Vec<envelope::Key> = group
            .iter()
            .map(|(_, key)| envelope::Key {
                parts: key.split('|').map(str::to_owned).collect(),
            })
            .collect();
        let keys: Vec<Key> = keys_of(codec, &wire)
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        let encoded = codec.load(conn, &keys).await?;
        for (key, e) in &encoded.failed {
            tracing::error!(
                table,
                ?key,
                "sync: a row that does not encode — not uploaded: {e}"
            );
        }
        let loaded = encoded.rows;
        let deletes = keys
            .iter()
            .filter(|_| codec.takes_deletes())
            .filter(|key| !loaded.iter().any(|(held, _)| held == *key))
            .filter(|key| !encoded.failed.iter().any(|(failed, _)| failed == *key))
            .map(|key| envelope::Key {
                parts: key.iter().map(ToString::to_string).collect(),
            })
            .collect();
        tables.push(envelope::Table {
            table: table.clone(),
            upserts: loaded.into_iter().map(|(_, bytes)| bytes).collect(),
            deletes,
            keys: Vec::new(),
        });
    }
    Ok(Up {
        replica_id: replica.to_string(),
        tables,
        protocol: envelope::PROTOCOL,
        ..Up::default()
    })
}

/// A row this replica wrote, noted to go up.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Noted {
    pub id: i64,
    pub relation: String,
    pub row_key: String,
}

/// Up to `limit` notes to upload, oldest first: the ones never sent, and the
/// ones sent longer than `resend_after` ago that the server never said it
/// filed.
pub async fn unsent(
    conn: &mut Conn,
    limit: i64,
    resend_after: std::time::Duration,
) -> Result<Vec<Noted>, Error> {
    unsent_where(conn, limit, resend_after, "").await
}

/// [`unsent`], of the tables `filter` (a condition on `relation`) admits.
async fn unsent_where(
    conn: &mut Conn,
    limit: i64,
    resend_after: std::time::Duration,
    filter: &str,
) -> Result<Vec<Noted>, Error> {
    let filter = if filter.is_empty() {
        String::new()
    } else {
        format!("AND {filter}")
    };
    Ok(sqlx::query_as(&format!(
        "SELECT id, relation, row_key FROM sync_notes \
          WHERE (upload IS NULL OR sent_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-{} seconds')) {filter} \
          ORDER BY id LIMIT $1",
        resend_after.as_secs()
    ))
    .bind(limit)
    .fetch_all(&mut *conn)
    .await?)
}

/// `notes` as one upload, read as they are now ([`collect`]), numbered by the
/// highest note it carries.
pub async fn upload<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    replica: Uuid,
    notes: &[Noted],
) -> Result<Up, Error> {
    let pairs: Vec<(String, String)> = notes
        .iter()
        .map(|n| (n.relation.clone(), n.row_key.clone()))
        .collect();
    let mut up = collect(conn, codecs, replica, &pairs).await?;
    up.uploads = notes
        .iter()
        .map(|n| n.id)
        .max()
        .and_then(|id| u64::try_from(id).ok())
        .into_iter()
        .collect();
    Ok(up)
}

/// Records that `notes` went up in the upload `up` numbers.
pub async fn sent(conn: &mut Conn, notes: &[Noted], up: &Up) -> Result<(), Error> {
    let Some(&number) = up.uploads.first() else {
        return Ok(());
    };
    for note in notes {
        sqlx::query(&format!(
            "UPDATE sync_notes SET upload = $2, sent_at = {} WHERE id = $1",
            crate::dialect::NOW
        ))
        .bind(note.id)
        .bind(i64::try_from(number).unwrap_or(i64::MAX))
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

/// The server filed these uploads: the notes they carried are done.
pub async fn filed(conn: &mut Conn, message: &envelope::Filed) -> Result<(), Error> {
    for upload in &message.uploads {
        sqlx::query("DELETE FROM sync_notes WHERE upload = $1")
            .bind(i64::try_from(*upload).unwrap_or(i64::MAX))
            .execute(&mut *conn)
            .await?;
    }
    Ok(())
}

/// How far this replica is from having sent the server what it wrote, per
/// table: rows noted and not sent yet, and rows sent the server has not said
/// it filed.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
pub struct UpProgress {
    pub table: String,
    pub unsent: i64,
    pub unfiled: i64,
}

/// [`UpProgress`] for every table with something on its way.
pub async fn progress(conn: &mut Conn) -> Result<Vec<UpProgress>, Error> {
    Ok(sqlx::query_as(
        "SELECT relation AS \"table\", \
                sum(CASE WHEN upload IS NULL THEN 1 ELSE 0 END) AS unsent, \
                sum(CASE WHEN upload IS NULL THEN 0 ELSE 1 END) AS unfiled \
           FROM sync_notes GROUP BY relation ORDER BY relation",
    )
    .fetch_all(&mut *conn)
    .await?)
}

/// The backlog to tell the server, when it changed since it was last told:
/// with a message going anyway, when it empties or starts, or when the last
/// time is `backlog_every` ago.
async fn backlog_due(
    conn: &mut Conn,
    settings: &Outbox,
    sending: bool,
) -> Result<Option<envelope::Backlog>, Error> {
    let now = progress(conn).await?;
    let told: Option<(sqlx::types::Json<Vec<UpProgress>>, i64)> =
        sqlx::query_as("SELECT backlog, reported_at FROM sync_backlog WHERE id = 1")
            .fetch_optional(&mut *conn)
            .await?;
    let (before, at) = match told {
        Some((backlog, at)) => (Some(backlog.0), at),
        None => (None, 0),
    };
    if before.as_ref() == Some(&now) {
        return Ok(None);
    }
    let edge = now.is_empty() || before.as_ref().is_none_or(Vec::is_empty);
    let every = i64::try_from(settings.backlog_every.as_secs()).unwrap_or(i64::MAX);
    let due = sending || edge || chrono::Utc::now().timestamp() - at >= every;
    Ok(due.then(|| envelope::Backlog {
        tables: now
            .into_iter()
            .map(|t| envelope::TableBacklog {
                table: t.table,
                unsent: u64::try_from(t.unsent).unwrap_or_default(),
                unfiled: u64::try_from(t.unfiled).unwrap_or_default(),
            })
            .collect(),
    }))
}

/// The server was told `backlog`.
async fn told(conn: &mut Conn, backlog: &envelope::Backlog) -> Result<(), Error> {
    let tables: Vec<UpProgress> = backlog.tables.iter().map(UpProgress::from).collect();
    sqlx::query(
        "INSERT INTO sync_backlog (id, backlog, reported_at) VALUES (1, $1, $2) \
         ON CONFLICT (id) DO UPDATE SET backlog = EXCLUDED.backlog, reported_at = EXCLUDED.reported_at",
    )
    .bind(sqlx::types::Json(tables))
    .bind(chrono::Utc::now().timestamp())
    .execute(&mut *conn)
    .await?;
    Ok(())
}

impl From<&envelope::TableBacklog> for UpProgress {
    fn from(table: &envelope::TableBacklog) -> Self {
        UpProgress {
            table: table.table.clone(),
            unsent: i64::try_from(table.unsent).unwrap_or(i64::MAX),
            unfiled: i64::try_from(table.unfiled).unwrap_or(i64::MAX),
        }
    }
}

/// Which of a replica's writes one stream of uploads carries. A replica may
/// keep a table that writes often (positions, events) on a lane of its own,
/// so it never waits behind the rest, nor the rest behind it.
#[derive(Clone, Debug)]
pub struct Lane {
    /// The transport's name for it.
    pub name: &'static str,
    /// The tables it carries; every table no other lane names when empty.
    pub tables: &'static [&'static str],
}

/// What a replica sends up in one message, and how much.
#[derive(Clone, Debug)]
pub struct Outbox {
    /// Notes per upload.
    pub notes: i64,
    /// How long an upload the server has not answered waits before it goes
    /// again.
    pub resend_after: std::time::Duration,
    /// Rejections per message.
    pub rejections: i64,
    /// How often, at most, the replica tells the server its backlog while it
    /// grows; it says at once when it empties, or starts.
    pub backlog_every: std::time::Duration,
}

impl Default for Outbox {
    fn default() -> Self {
        Outbox {
            notes: 500,
            resend_after: std::time::Duration::from_secs(600),
            rejections: 500,
            backlog_every: std::time::Duration::from_secs(60),
        }
    }
}

/// A message on its way up, and what to record once the transport took it.
#[derive(Debug)]
pub struct Outgoing {
    pub lane: &'static str,
    pub up: Up,
    notes: Vec<Noted>,
}

impl Outgoing {
    /// As the bytes a transport carries.
    pub fn bytes(&self) -> Vec<u8> {
        use prost::Message as _;
        self.up.encode_to_vec()
    }

    /// How many of the replica's writes it carries.
    pub fn rows(&self) -> usize {
        self.notes.len()
    }
}

/// What `replica` has to tell the server on `lane`, as one message: `None`
/// when it has nothing. Rows it wrote (as they are now), on every lane; on the
/// first lane of `lanes` — the main one — also the rows it could not take, the
/// last message it applied when the server has not heard of it, and, when
/// `digests` is set, what it holds. Hand the message to the transport, then
/// to [`delivered`].
pub async fn outbox<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    replica: Uuid,
    lanes: &[Lane],
    lane: usize,
    settings: &Outbox,
    digests: bool,
) -> Result<Option<Outgoing>, Error> {
    let this = &lanes[lane];
    let quoted = |tables: &mut dyn Iterator<Item = &&str>| {
        tables
            .map(|t| format!("'{}'", t.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let filter = if this.tables.is_empty() {
        let others = quoted(&mut lanes.iter().flat_map(|l| l.tables.iter()));
        if others.is_empty() {
            String::new()
        } else {
            format!("relation NOT IN ({others})")
        }
    } else {
        format!("relation IN ({})", quoted(&mut this.tables.iter()))
    };
    let notes = unsent_where(conn, settings.notes, settings.resend_after, &filter).await?;
    let mut up = if notes.is_empty() {
        Up {
            replica_id: replica.to_string(),
            protocol: envelope::PROTOCOL,
            ..Up::default()
        }
    } else {
        upload(conn, codecs, replica, &notes).await?
    };
    if lane == 0 {
        up.rejected = rejections(conn, settings.rejections).await?;
        if digests {
            up.digests = self::digests(conn, codecs, replica).await?.digests;
        }
        let applied: Option<(String, i64, i64)> =
            sqlx::query_as("SELECT epoch, sequence, acknowledged FROM sync_applied WHERE id = 1")
                .fetch_optional(&mut *conn)
                .await?;
        if let Some((epoch, sequence, acknowledged)) = applied {
            // The last applied goes with digests always (they are compared
            // against it), and otherwise when the server has not heard of it.
            if sequence > acknowledged || !up.digests.is_empty() {
                up.epoch = epoch;
                up.sequence = u64::try_from(sequence).unwrap_or_default();
            }
        }
    }
    let sending = !up.tables.is_empty()
        || !up.rejected.is_empty()
        || !up.digests.is_empty()
        || up.sequence > 0;
    if lane == 0 {
        up.backlog = backlog_due(conn, settings, sending).await?;
    }
    let empty = !sending && up.backlog.is_none();
    Ok((!empty).then_some(Outgoing {
        lane: this.name,
        up,
        notes,
    }))
}

/// The transport took `outgoing`: its notes wait for the server's [`Filed`],
/// its rejections are reported, and the server knows what was applied.
pub async fn delivered(conn: &mut Conn, outgoing: &Outgoing) -> Result<(), Error> {
    sent(conn, &outgoing.notes, &outgoing.up).await?;
    reported(conn, &outgoing.up.rejected).await?;
    if let Some(backlog) = &outgoing.up.backlog {
        told(conn, backlog).await?;
    }
    if outgoing.up.sequence > 0 {
        acknowledged(conn, outgoing.up.sequence).await?;
    }
    Ok(())
}

/// What this replica holds of each table the server sends down, with the
/// last message it applied, for the server to compare with what it records:
/// rows it retired, or could not take, left out.
pub async fn digests<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    replica: Uuid,
) -> Result<Up, Error> {
    use crate::row::Mirror;
    let mut digests = Vec::new();
    for codec in codecs
        .iter()
        .filter(|c| matches!(c.table.mirror, Mirror::Catalog | Mirror::Both))
    {
        let rejected: std::collections::HashSet<String> =
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
        digests.push(envelope::Digest {
            table: codec.table.name.to_owned(),
            count: keys.len() as u64,
            fingerprint: envelope::fingerprint(&mut keys),
        });
    }
    let (epoch, sequence): (String, i64) =
        sqlx::query_as("SELECT epoch, sequence FROM sync_applied WHERE id = 1")
            .fetch_optional(&mut *conn)
            .await?
            .unwrap_or_default();
    Ok(Up {
        replica_id: replica.to_string(),
        digests,
        epoch,
        sequence: u64::try_from(sequence).unwrap_or_default(),
        protocol: envelope::PROTOCOL,
        ..Up::default()
    })
}

/// One table of an upload, as the server filed it.
#[derive(Debug)]
pub struct Filed {
    pub table: String,
    pub written: Vec<Key>,
    pub gone: Vec<Key>,
}

/// A row a replica uploaded that the server could not file, as it came.
#[derive(Debug)]
pub struct Unfiled {
    pub table: String,
    pub bytes: Vec<u8>,
    pub reason: String,
}

/// The server filing what `sender` uploaded, in the caller's transaction:
/// each table by its codec's rules, under `sender` and what the server knows
/// of it (`known`), whatever the rows say. A row it cannot file — a table
/// this server does not mirror, a row that does not decode, one the database
/// refuses for its data — is kept as it came in the server's inbox, to be
/// filed again once a fix is deployed; a storage failure is the error, and so
/// is an upload of a later protocol than this build speaks.
pub async fn file<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    upload: &Up,
    sender: Uuid,
    known: &P,
) -> Result<Vec<Filed>, Error> {
    if upload.protocol > envelope::PROTOCOL {
        return Err(Error::Protocol {
            theirs: upload.protocol,
            ours: envelope::PROTOCOL,
        });
    }
    let mut filed = Vec::new();
    let mut unfiled = Vec::new();
    for table in &upload.tables {
        let set_aside = |at: usize, reason: &str| Unfiled {
            table: table.table.clone(),
            bytes: table.upserts[at].clone(),
            reason: reason.to_owned(),
        };
        let Some(codec) = codec_of(codecs, &table.table) else {
            let why = "a table this server does not mirror";
            unfiled.extend((0..table.upserts.len()).map(|at| set_aside(at, why)));
            continue;
        };
        let taken = match codec
            .apply_up(conn, table.upserts.clone(), sender, known)
            .await
        {
            Ok(taken) => taken,
            Err(e) if !e.is_transient() => {
                let why = e.to_string();
                unfiled.extend((0..table.upserts.len()).map(|at| set_aside(at, &why)));
                continue;
            }
            Err(e) => return Err(e),
        };
        unfiled.extend(taken.rejected.iter().map(|(at, why)| set_aside(*at, why)));
        let offered = keys_of(codec, &table.deletes);
        let gone = if offered.is_empty() {
            Vec::new()
        } else {
            match codec.delete_up(conn, &offered, sender).await {
                Ok(gone) => gone,
                Err(e) if !e.is_transient() => {
                    tracing::error!(table = table.table, "upload: deletes refused: {e}");
                    Vec::new()
                }
                Err(e) => return Err(e),
            }
        };
        filed.push(Filed {
            table: table.table.clone(),
            written: taken.held,
            gone,
        });
    }
    #[cfg(all(feature = "postgres", not(feature = "sqlite")))]
    {
        crate::relay::hold(conn, sender, &unfiled).await?;
        // What it refused first: a row it applied a message of and refused
        // is not held, and stays in quarantine.
        crate::relay::rejected(conn, sender, &upload.rejected).await?;
        if upload.sequence > 0 {
            crate::relay::acknowledge(conn, sender, &upload.epoch, upload.sequence).await?;
        }
        crate::relay::compare(conn, sender, upload).await?;
        if let Some(backlog) = &upload.backlog {
            crate::relay::backlog(conn, sender, backlog).await?;
        }
    }
    #[cfg(all(feature = "sqlite", not(feature = "postgres")))]
    for row in &unfiled {
        tracing::error!(
            table = row.table,
            reason = row.reason,
            "upload: a row not filed"
        );
    }
    Ok(filed)
}

#[cfg(all(test, feature = "sqlite", not(feature = "postgres")))]
#[path = "replica_tests.rs"]
mod tests;
