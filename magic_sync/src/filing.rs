//! What a replica sends up, filed on the server: its rows by their codecs'
//! rules, under the replica the message came from; what it could not take;
//! the last message it applied; what it holds; what it wrote that is not
//! filed yet.
//!
//! Everything in an upload is input from the wire and read as such. A key is
//! parsed through its table's codec and stored as the codec writes it, a
//! sequence is checked against what the server sent, and whatever breaks the
//! protocol is malformed: logged and left out, never stored. What the server
//! cannot file for a reason a fix can change — a table it does not mirror, a
//! row that does not decode, a row or a delete its database refuses — is kept
//! in `sync.inbox` as it came, and filed again after a deploy.

use crate::envelope::{self, Up};
use crate::relay::{resync, stored};
use crate::replica::{codec_of, keys_of, UpProgress};
use crate::row::{key_text, Key};
use crate::wire::Codec;
use crate::Conn;
use crate::Error;
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// One table of an upload, as the server filed it: what
/// [`Server::receive`](crate::Server::receive) answers, for the host to act
/// on what its replicas wrote.
#[derive(Debug)]
#[non_exhaustive]
pub struct Filed {
    /// The table.
    pub table: String,
    /// The keys of the rows the upload wrote.
    pub written: Vec<Key>,
    /// The keys of the rows the replica deleted, and the server with it.
    pub gone: Vec<Key>,
}

/// A part of an upload the server could not file — one row, or one delete,
/// of one table — as an upload of its own, to be filed again.
struct Unfiled {
    table: String,
    up: Up,
    reason: String,
}

impl Unfiled {
    fn of(table: &envelope::Table, part: envelope::Table, reason: &str) -> Self {
        Unfiled {
            table: table.table.clone(),
            up: Up {
                tables: vec![part],
                protocol: envelope::PROTOCOL,
                ..Up::default()
            },
            reason: reason.to_owned(),
        }
    }

    /// Row `at` of `table`, with its key when the replica gave one.
    fn row(table: &envelope::Table, at: usize, reason: &str) -> Option<Self> {
        let part = envelope::Table {
            table: table.table.clone(),
            upserts: vec![table.upserts.get(at)?.clone()],
            keys: table.keys.get(at).cloned().into_iter().collect(),
            deletes: Vec::new(),
        };
        Some(Self::of(table, part, reason))
    }

    fn delete(table: &envelope::Table, key: envelope::Key, reason: &str) -> Self {
        let part = envelope::Table {
            table: table.table.clone(),
            deletes: vec![key],
            ..envelope::Table::default()
        };
        Self::of(table, part, reason)
    }
}

/// Where the sender stands with the server, read once, its row locked for
/// the filing: two filings of one replica run one after the other. What it
/// was sent is what the relay committed; a pass in flight has sent nothing.
struct Standing {
    epoch: Uuid,
    /// The last number the server drew for any message: a replica that
    /// applied one past it saw a server this database no longer remembers.
    drawn: i64,
    /// The last message written for it; `None` while none was.
    last: Option<i64>,
    /// The first message of the last snapshot written for it.
    snapshot: Option<i64>,
    drifted_at: Option<DateTime<Utc>>,
}

/// The server filing what `sender` uploaded, in the caller's transaction:
/// each table by its codec's rules, under `sender` and what the server knows
/// of it (`known`), whatever the rows say. What it cannot file — a table this
/// server does not mirror, a row that does not decode, a row or a delete the
/// database refuses for its data — is kept as it came in the server's inbox,
/// to be filed again once a fix is deployed. An error is the storage's, or an
/// upload of a later protocol than this build speaks; a failure of anything
/// else is permanent, and the caller keeps the upload whole
/// ([`Server::receive`](crate::Server::receive) does).
pub(crate) async fn file<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    upload: &Up,
    sender: Uuid,
    known: &P,
) -> Result<Vec<Filed>, Error> {
    let (filed, unfiled) = file_parts(conn, codecs, upload, sender, known).await?;
    keep(conn, sender, &unfiled, None).await?;
    Ok(filed)
}

async fn file_parts<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    upload: &Up,
    sender: Uuid,
    known: &P,
) -> Result<(Vec<Filed>, Vec<Unfiled>), Error> {
    if upload.protocol > envelope::PROTOCOL {
        return Err(Error::Protocol {
            theirs: upload.protocol,
            ours: envelope::PROTOCOL,
        });
    }
    let standing = standing(conn, sender).await?;
    let mut filed = Vec::new();
    let mut unfiled = Vec::new();
    for table in &upload.tables {
        let Some(codec) = codec_of(codecs, &table.table) else {
            let why = "a table this server does not mirror";
            unfiled.extend((0..table.upserts.len()).filter_map(|at| Unfiled::row(table, at, why)));
            unfiled.extend(
                table
                    .deletes
                    .iter()
                    .map(|key| Unfiled::delete(table, key.clone(), why)),
            );
            continue;
        };
        let mut written = Vec::new();
        if !table.upserts.is_empty() {
            match codec
                .apply_up(conn, table.upserts.clone(), sender, known)
                .await
            {
                Ok(taken) => {
                    written = taken.held;
                    unfiled.extend(
                        taken
                            .rejected
                            .iter()
                            .filter_map(|r| Unfiled::row(table, r.at, &r.reason)),
                    );
                }
                Err(e) if e.is_transient() => return Err(e),
                Err(e) => {
                    let why = e.to_string();
                    unfiled.extend(
                        (0..table.upserts.len()).filter_map(|at| Unfiled::row(table, at, &why)),
                    );
                }
            }
        }
        let gone = if table.deletes.is_empty() {
            Vec::new()
        } else if !codec.takes_deletes() {
            tracing::error!(
                %sender,
                table = table.table,
                "upload: deletes of a table not keyed by its replica — malformed, left out"
            );
            Vec::new()
        } else {
            let (gone, refused) =
                delete(conn, codec, &keys_of(codec, &table.deletes), sender).await?;
            unfiled.extend(refused.into_iter().map(|(key, why)| {
                let key = envelope::Key {
                    parts: key.iter().map(ToString::to_string).collect(),
                };
                Unfiled::delete(table, key, &why)
            }));
            gone
        };
        filed.push(Filed {
            table: table.table.clone(),
            written,
            gone,
        });
    }
    // What it refused first: a row it applied a message of and refused is
    // not held, and stays in quarantine.
    rejected(conn, codecs, sender, &upload.rejected).await?;
    if acknowledge(conn, sender, &standing, &upload.epoch, upload.sequence).await? {
        gap(conn, sender, &standing, upload).await?;
        compare(conn, codecs, sender, &standing, upload).await?;
    }
    if let Some(told) = &upload.backlog {
        backlog(conn, sender, told).await?;
    }
    Ok((filed, unfiled))
}

/// What the server records of a sender.
#[derive(Default, sqlx::FromRow)]
struct Recorded {
    last: Option<i64>,
    snapshot: Option<i64>,
    drifted_at: Option<DateTime<Utc>>,
}

async fn standing(conn: &mut Conn, sender: Uuid) -> Result<Standing, Error> {
    let epoch: Uuid = sqlx::query_scalar("SELECT epoch FROM sync.epoch")
        .fetch_one(&mut *conn)
        .await?;
    let drawn: i64 = sqlx::query_scalar(
        "SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM sync.messages",
    )
    .fetch_one(&mut *conn)
    .await?;
    let recorded: Recorded = sqlx::query_as(
        "SELECT c.last_sequence AS last, c.snapshot_sequence AS snapshot, r.drifted_at \
           FROM sync.replicas r LEFT JOIN sync.chains c USING (replica_id) \
          WHERE r.replica_id = $1 FOR UPDATE OF r",
    )
    .bind(sender)
    .fetch_optional(&mut *conn)
    .await?
    .unwrap_or_default();
    Ok(Standing {
        epoch,
        drawn,
        last: recorded.last,
        snapshot: recorded.snapshot,
        drifted_at: recorded.drifted_at,
    })
}

/// The server letting go of `keys`, rows `sender` deleted — of its own rows
/// only — each it refuses for its data (a row of the server's that still
/// points at it) answered apart with why, so one refused delete holds back
/// nothing else.
async fn delete<P: Sync + 'static>(
    conn: &mut Conn,
    codec: &Codec<P>,
    keys: &[Key],
    sender: Uuid,
) -> Result<(Vec<Key>, Vec<(Key, String)>), Error> {
    if keys.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    if let Ok(gone) = delete_guarded(conn, codec, keys, sender).await? {
        return Ok((gone, Vec::new()));
    }
    let mut gone = Vec::new();
    let mut refused = Vec::new();
    for key in keys {
        match delete_guarded(conn, codec, std::slice::from_ref(key), sender).await? {
            Ok(one) => gone.extend(one),
            Err(e) => refused.push((key.clone(), e.to_string())),
        }
    }
    Ok((gone, refused))
}

/// One delete in a savepoint: a refusal comes back as the inner error with
/// the transaction still usable; a transient failure is the outer one.
async fn delete_guarded<P: Sync + 'static>(
    conn: &mut Conn,
    codec: &Codec<P>,
    keys: &[Key],
    sender: Uuid,
) -> Result<Result<Vec<Key>, Error>, Error> {
    sqlx::query("SAVEPOINT sync_filing")
        .execute(&mut *conn)
        .await?;
    let result = codec.delete_up(conn, keys, sender).await;
    if result.is_err() {
        sqlx::query("ROLLBACK TO SAVEPOINT sync_filing")
            .execute(&mut *conn)
            .await?;
    }
    sqlx::query("RELEASE SAVEPOINT sync_filing")
        .execute(&mut *conn)
        .await?;
    match result {
        Err(e) if e.is_transient() => Err(e),
        other => Ok(other),
    }
}

/// Rows `replica` reported it could not take: no longer counted as held, and
/// quarantined, so a retry after a fix sends them again. Only a row the
/// server records sending it: a rejection of anything else — a row it was
/// never sent, one reported twice — names nothing to set aside.
async fn rejected<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    replica: Uuid,
    rejected: &[envelope::Rejected],
) -> Result<(), Error> {
    for row in rejected {
        let Some(codec) = codec_of(codecs, &row.table).filter(|c| c.sent_down()) else {
            tracing::error!(%replica, table = stored(&row.table), "upload: a rejection of a table this server does not send — malformed, left out");
            continue;
        };
        let Some(key) = row
            .key
            .as_ref()
            .and_then(|key| keys_of(codec, std::slice::from_ref(key)).pop())
        else {
            tracing::error!(%replica, table = row.table, "upload: a rejection without a key of its table — malformed, left out");
            continue;
        };
        let key = key_text(&key);
        let held = sqlx::query(
            "DELETE FROM sync.held WHERE replica_id = $1 AND relation = $2 AND row_key = $3",
        )
        .bind(replica)
        .bind(codec.table.name)
        .bind(&key)
        .execute(&mut *conn)
        .await?;
        if held.rows_affected() == 0 {
            tracing::warn!(%replica, table = row.table, key, "upload: a rejection of a row the server does not record sending it — left out");
            continue;
        }
        let reason = stored(&format!("rejected by the replica: {}", row.reason));
        sqlx::query(
            "INSERT INTO sync.quarantine AS q (replica_id, relation, row_key, cause, reason) \
             VALUES ($1, $2, $3, 'replica', $4) \
             ON CONFLICT (replica_id, relation, row_key) DO UPDATE SET cause = EXCLUDED.cause, \
               reason = EXCLUDED.reason, last_at = now(), attempts = q.attempts + 1, carried = NULL",
        )
        .bind(replica)
        .bind(codec.table.name)
        .bind(&key)
        .bind(&reason)
        .execute(&mut *conn)
        .await?;
        tracing::error!(
            %replica,
            table = row.table,
            key,
            reason,
            "sync: a replica could not take a row — quarantined until a fix lets it through"
        );
    }
    Ok(())
}

/// Records that `replica` applied every message up to `sequence` from the
/// server installation `epoch`: the rows those messages carried are held from
/// now on. Sequence 0 says nothing was applied yet. One from another epoch,
/// or behind the last one, is outdated and changes nothing; one past every
/// message the server records sending it is not believed — a replica that
/// lies — and changes nothing either. One past every number the server ever
/// drew, in its epoch, says this database was restored from a copy older
/// than what the replicas applied: the server takes a new epoch and
/// provisions every replica again. Answers whether the server is as the
/// replica last knew it — not restored.
async fn acknowledge(
    conn: &mut Conn,
    replica: Uuid,
    standing: &Standing,
    epoch: &str,
    sequence: u64,
) -> Result<bool, Error> {
    if sequence == 0 {
        return Ok(true);
    }
    let Ok(epoch) = Uuid::parse_str(epoch) else {
        tracing::error!(%replica, epoch = stored(epoch), "upload: an epoch that is no UUID — malformed, left out");
        return Ok(true);
    };
    if epoch != standing.epoch {
        return Ok(true);
    }
    let Ok(sequence) = i64::try_from(sequence) else {
        tracing::error!(%replica, sequence, "upload: a sequence no message has — malformed, left out");
        return Ok(true);
    };
    if sequence > standing.drawn {
        restored(conn, replica, epoch, sequence).await?;
        return Ok(false);
    }
    if standing.last.is_none_or(|last| sequence > last) {
        tracing::warn!(
            %replica,
            sequence,
            last = standing.last,
            "upload: it acknowledges a message the server does not record sending it — left out"
        );
        return Ok(true);
    }
    sqlx::query(
        "UPDATE sync.replicas SET applied_epoch = $2, applied_sequence = $3, applied_at = now(), \
           waiting_since = CASE WHEN $3 >= $4 THEN NULL ELSE now() END \
         WHERE replica_id = $1 AND (applied_epoch IS DISTINCT FROM $2 \
                                    OR applied_sequence IS NULL OR applied_sequence < $3)",
    )
    .bind(replica)
    .bind(epoch)
    .bind(sequence)
    .bind(standing.last)
    .execute(&mut *conn)
    .await?;
    // A row set aside for it leaves the quarantine once it applied the
    // message that carried it again — not when it is sent: it may refuse it
    // again, and an older version it applied says nothing of this one.
    sqlx::query(
        "DELETE FROM sync.quarantine q USING sync.replicas r \
          WHERE q.replica_id = $1 AND r.replica_id = q.replica_id AND r.applied_epoch = $2 \
            AND q.carried <= r.applied_sequence",
    )
    .bind(replica)
    .bind(epoch)
    .execute(&mut *conn)
    .await?;
    Ok(true)
}

/// This database is older than what `replica` applied — restored from a
/// copy — so what it records of every replica is too: it takes a new epoch,
/// unless a filing at once took it already, drops every outbox, and
/// provisions every replica again. Messages of the old epoch still on
/// their way are a server's a replica no longer follows.
async fn restored(conn: &mut Conn, replica: Uuid, epoch: Uuid, sequence: i64) -> Result<(), Error> {
    let rotated = sqlx::query("UPDATE sync.epoch SET epoch = gen_random_uuid() WHERE epoch = $1")
        .bind(epoch)
        .execute(&mut *conn)
        .await?;
    if rotated.rows_affected() == 0 {
        return Ok(());
    }
    tracing::error!(
        %replica,
        sequence,
        "sync ALARM: a replica applied a message this database never numbered — it was restored \
         from an older copy; a new epoch begins and every replica is provisioned again"
    );
    for sql in [
        "DELETE FROM sync.outbox",
        "UPDATE sync.chains SET last_sequence = NULL, snapshot_sequence = NULL",
    ] {
        sqlx::query(sql).execute(&mut *conn).await?;
    }
    // Every replica the engine knows of: one not live is sent nothing.
    let known: Vec<Uuid> = sqlx::query_scalar("SELECT replica_id FROM sync.replicas")
        .fetch_all(&mut *conn)
        .await?;
    for replica in known {
        resync(conn, replica).await?;
    }
    Ok(())
}

/// `replica` says it missed a message: it applies nothing more until a
/// snapshot begins, so it is provisioned again — unless one is on its way,
/// written for it since the last message it applied.
async fn gap(
    conn: &mut Conn,
    replica: Uuid,
    standing: &Standing,
    upload: &Up,
) -> Result<(), Error> {
    if !upload.gap {
        return Ok(());
    }
    let since = Uuid::parse_str(&upload.epoch).ok() == Some(standing.epoch);
    let on_its_way = standing.snapshot.is_some_and(|began| {
        !since || i64::try_from(upload.sequence).is_ok_and(|applied| applied < began)
    });
    if on_its_way {
        tracing::debug!(%replica, "sync: a replica that missed a message has its snapshot on the way");
        return Ok(());
    }
    if resync(conn, replica).await? {
        tracing::warn!(%replica, "sync: a replica missed a message — it is provisioned again");
    }
    Ok(())
}

/// Compares what `replica` says it holds with what the server records it
/// holds, and provisions it again from scratch where they differ: the
/// backstop for whatever else went wrong. Only once the replica has applied
/// every message recorded for it — before that, a difference is just a
/// message on its way. Rows it refused are left out, as it leaves them out.
/// Digests that differ again within a day of the last provisioning they
/// caused are a bug another one would not cure: logged, not acted on.
/// Answers the tables that differed.
async fn compare<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    replica: Uuid,
    standing: &Standing,
    upload: &Up,
) -> Result<Vec<String>, Error> {
    if upload.digests.is_empty() {
        return Ok(Vec::new());
    }
    let mut tables = std::collections::BTreeSet::new();
    for digest in &upload.digests {
        let sent = codec_of(codecs, &digest.table).is_some_and(Codec::sent_down);
        if !sent || !tables.insert(digest.table.as_str()) {
            tracing::error!(%replica, table = stored(&digest.table), "upload: digests of a table this server does not send, or of one twice — malformed, not compared");
            return Ok(Vec::new());
        }
    }
    let settled = Uuid::parse_str(&upload.epoch).ok() == Some(standing.epoch)
        && standing.last.is_some()
        && i64::try_from(upload.sequence).ok() == standing.last;
    if !settled {
        tracing::debug!(%replica, "sync: digests taken with messages still on their way — not compared");
        return Ok(Vec::new());
    }
    let mut differ = Vec::new();
    for digest in &upload.digests {
        let keys: Vec<String> = sqlx::query_scalar(
            "SELECT h.row_key FROM sync.held h WHERE h.replica_id = $1 AND h.relation = $2 \
               AND NOT EXISTS (SELECT 1 FROM sync.quarantine q \
                                WHERE q.replica_id = h.replica_id AND q.relation = h.relation \
                                  AND q.row_key = h.row_key AND q.cause = 'replica')",
        )
        .bind(replica)
        .bind(&digest.table)
        .fetch_all(&mut *conn)
        .await?;
        let count = keys.len() as u64;
        if count != digest.count || envelope::fingerprint(keys) != digest.fingerprint {
            tracing::error!(
                %replica,
                table = digest.table,
                held_here = count,
                held_there = digest.count,
                "sync: a replica holds other than what the server records"
            );
            differ.push(digest.table.clone());
        }
    }
    if differ.is_empty() {
        return Ok(differ);
    }
    if standing
        .drifted_at
        .is_some_and(|at| Utc::now() - at < chrono::Duration::days(1))
    {
        tracing::error!(%replica, "sync ALARM: a replica differs again after being provisioned again — a bug no provisioning cures; deploy a fix");
        return Ok(differ);
    }
    resync(conn, replica).await?;
    sqlx::query("UPDATE sync.replicas SET drifted_at = now() WHERE replica_id = $1")
        .bind(replica)
        .execute(&mut *conn)
        .await?;
    Ok(differ)
}

/// What `replica` says it wrote and the server has not filed.
async fn backlog(conn: &mut Conn, replica: Uuid, told: &envelope::Backlog) -> Result<(), Error> {
    let tables: Option<Vec<UpProgress>> = told
        .tables
        .iter()
        .map(|t| {
            Some(UpProgress {
                table: stored(&t.table),
                unsent: i64::try_from(t.unsent).ok()?,
                unfiled: i64::try_from(t.unfiled).ok()?,
                set_aside: i64::try_from(t.set_aside).ok()?,
            })
        })
        .collect();
    let Some(tables) = tables else {
        tracing::error!(%replica, "upload: a backlog counting more rows than a table has — malformed, left out");
        return Ok(());
    };
    sqlx::query(
        "INSERT INTO sync.replicas AS r (replica_id, backlog, backlog_at) VALUES ($1, $2, now()) \
         ON CONFLICT (replica_id) DO UPDATE SET backlog = EXCLUDED.backlog, backlog_at = now()",
    )
    .bind(replica)
    .bind(sqlx::types::Json(tables))
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// A part of an upload waiting in the inbox.
#[derive(Debug, sqlx::FromRow)]
pub(crate) struct Kept {
    pub(crate) id: i64,
    pub(crate) replica_id: Uuid,
    upload: Vec<u8>,
    first_at: DateTime<Utc>,
    attempts: i32,
}

/// Keeps what of an upload from `replica` could not be filed; `since`, when
/// it was kept before and failed again, keeps its age.
async fn keep(
    conn: &mut Conn,
    replica: Uuid,
    unfiled: &[Unfiled],
    since: Option<&Kept>,
) -> Result<(), Error> {
    use prost::Message as _;
    for part in unfiled {
        tracing::error!(
            %replica,
            table = stored(&part.table),
            reason = part.reason,
            "sync: what a replica uploaded the server could not file — kept until a fix files it"
        );
        insert(
            conn,
            replica,
            Some(&part.table),
            &part.up.encode_to_vec(),
            &part.reason,
            since,
        )
        .await?;
    }
    Ok(())
}

async fn insert(
    conn: &mut Conn,
    replica: Uuid,
    table: Option<&str>,
    upload: &[u8],
    reason: &str,
    since: Option<&Kept>,
) -> Result<(), Error> {
    sqlx::query(
        "INSERT INTO sync.inbox AS i (replica_id, relation, upload, reason, first_at, attempts) \
         VALUES ($1, $2, $3, $4, coalesce($5, now()), $6) \
         ON CONFLICT (replica_id, md5(upload)) DO UPDATE SET \
           reason = EXCLUDED.reason, last_at = now(), attempts = i.attempts + 1",
    )
    .bind(replica)
    .bind(table.map(stored))
    .bind(upload)
    .bind(stored(reason))
    .bind(since.map(|k| k.first_at))
    .bind(since.map_or(1, |k| k.attempts + 1))
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Keeps a whole upload from `replica`, as it came, when filing it failed
/// as a whole for a reason a retry cannot change.
pub(crate) async fn keep_whole(
    conn: &mut Conn,
    replica: Uuid,
    upload: &[u8],
    reason: &str,
) -> Result<(), Error> {
    tracing::error!(
        %replica,
        reason,
        "sync: an upload the server could not file — kept whole until a fix files it"
    );
    insert(conn, replica, None, upload, reason, None).await
}

/// The parts of uploads waiting in the inbox with an id past `after` and up
/// to `through`, oldest first, `limit` of them.
pub(crate) async fn inbox(
    conn: &mut Conn,
    after: i64,
    through: i64,
    limit: i64,
) -> Result<Vec<Kept>, Error> {
    Ok(sqlx::query_as(
        "SELECT id, replica_id, upload, first_at, attempts FROM sync.inbox \
          WHERE id > $1 AND id <= $2 ORDER BY id LIMIT $3",
    )
    .bind(after)
    .bind(through)
    .bind(limit)
    .fetch_all(&mut *conn)
    .await?)
}

/// The newest id in the inbox: a walk over it stops there, so what it keeps
/// again on the way is left for the next one.
pub(crate) async fn newest(conn: &mut Conn) -> Result<i64, Error> {
    let newest: Option<i64> = sqlx::query_scalar("SELECT max(id) FROM sync.inbox")
        .fetch_one(&mut *conn)
        .await?;
    Ok(newest.unwrap_or_default())
}

/// Files `kept` again, in the caller's transaction, under its replica and
/// what the server knows of it now; answers whether all of it was filed. It
/// leaves the inbox, and what still cannot be filed is kept again with its
/// age. Only what it carried is filed: what it said of its moment — the
/// message it had applied, what it held, what it had not sent — is out of
/// date by now.
pub(crate) async fn refile<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    kept: &Kept,
    known: &P,
) -> Result<bool, Error> {
    use prost::Message as _;
    let mut up = Up::decode(kept.upload.as_slice()).map_err(|e| Error::Malformed {
        table: "sync.inbox".to_owned(),
        reason: e.to_string(),
    })?;
    (up.epoch, up.sequence, up.digests, up.backlog) = (String::new(), 0, Vec::new(), None);
    sqlx::query("DELETE FROM sync.inbox WHERE id = $1")
        .bind(kept.id)
        .execute(&mut *conn)
        .await?;
    let (_, unfiled) = file_parts(conn, codecs, &up, kept.replica_id, known).await?;
    keep(conn, kept.replica_id, &unfiled, Some(kept)).await?;
    Ok(unfiled.is_empty())
}

/// `kept` failed to file again, as a whole, with why.
pub(crate) async fn failed_again(conn: &mut Conn, kept: &Kept, why: &str) -> Result<(), Error> {
    sqlx::query(
        "UPDATE sync.inbox SET reason = $2, last_at = now(), attempts = attempts + 1 WHERE id = $1",
    )
    .bind(kept.id)
    .bind(stored(why))
    .execute(&mut *conn)
    .await?;
    Ok(())
}
