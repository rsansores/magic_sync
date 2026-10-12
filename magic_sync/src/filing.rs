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
use std::collections::BTreeMap;
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
/// of one table — as an upload of its own, to be filed again, by the row's
/// key.
struct Unfiled {
    table: String,
    key: String,
    up: Up,
    reason: String,
}

impl Unfiled {
    fn of(table: &envelope::Table, key: String, part: envelope::Table, reason: &str) -> Self {
        Unfiled {
            table: table.table.clone(),
            key,
            up: Up {
                tables: vec![part],
                protocol: envelope::PROTOCOL,
                ..Up::default()
            },
            reason: reason.to_owned(),
        }
    }

    /// Row `at` of `table`, `codec`'s when the server mirrors it; `None`
    /// when the replica gave no key with it, which breaks the protocol.
    fn row<P: Sync + 'static>(
        codec: Option<&Codec<P>>,
        table: &envelope::Table,
        at: usize,
        reason: &str,
    ) -> Option<Self> {
        let (Some(row), Some(key)) = (table.upserts.get(at), table.keys.get(at)) else {
            tracing::error!(
                table = stored(&table.table),
                "upload: a row without its key — malformed, left out"
            );
            return None;
        };
        let part = envelope::Table {
            table: table.table.clone(),
            upserts: vec![row.clone()],
            keys: vec![key.clone()],
            deletes: Vec::new(),
        };
        Some(Self::of(table, kept_key(codec, key), part, reason))
    }

    fn delete<P: Sync + 'static>(
        codec: Option<&Codec<P>>,
        table: &envelope::Table,
        key: envelope::Key,
        reason: &str,
    ) -> Self {
        let text = kept_key(codec, &key);
        let part = envelope::Table {
            table: table.table.clone(),
            deletes: vec![key],
            ..envelope::Table::default()
        };
        Self::of(table, text, part, reason)
    }

    /// Every row and delete of `upload`, each a part of its own.
    fn all<P: Sync + 'static>(codecs: &[Codec<P>], upload: &Up, reason: &str) -> Vec<Self> {
        let mut parts = Vec::new();
        for table in &upload.tables {
            let codec = codec_of(codecs, &table.table);
            parts.extend(
                (0..table.upserts.len()).filter_map(|at| Self::row(codec, table, at, reason)),
            );
            parts.extend(
                table
                    .deletes
                    .iter()
                    .map(|key| Self::delete(codec, table, key.clone(), reason)),
            );
        }
        parts
    }
}

/// A row's key as the inbox keeps it: as its table's codec writes it, or as
/// it came, for a table this server does not mirror or a key that is not
/// its table's.
fn kept_key<P: Sync + 'static>(codec: Option<&Codec<P>>, key: &envelope::Key) -> String {
    codec
        .and_then(|codec| codec.key(&key.parts).ok())
        .map_or_else(|| stored(&key.parts.join("|")), |key| key_text(&key))
}

/// Where the sender stands with the server, read once, its row locked for
/// the filing: two filings of one replica run one after the other. What it
/// was sent is what the relay committed; a pass in flight has sent nothing.
struct Standing {
    epoch: Uuid,
    /// What seals the epoch's messages.
    secret: Vec<u8>,
    /// The last number the server drew for any message: a replica that
    /// applied one past it saw a server this database no longer remembers.
    drawn: i64,
    /// The last message written for it, sequence and nonce; `None` while
    /// none was since it was last provisioned.
    last: Option<(i64, i64)>,
    /// The last message it said it applied in this epoch, sequence and
    /// nonce.
    applied: Option<(i64, i64)>,
    /// The first message of the last snapshot written for it.
    snapshot: Option<i64>,
    /// And the one that ended it.
    snapshot_end: Option<i64>,
    drifted_at: Option<DateTime<Utc>>,
}

/// The server filing what `sender` uploaded, in the caller's transaction:
/// each table by its codec's rules, under `sender` and what the server knows
/// of it (`known`), whatever the rows say. What it cannot file — a table this
/// server does not mirror, a row that does not decode, a row or a delete the
/// database refuses for its data — is kept as it came in the server's inbox,
/// a part per row, to be filed again once a fix is deployed, and so is every
/// row of an upload whose filing fails as a whole — while the sender's part
/// of the inbox holds fewer than `inbox` parts, and less than 64 MiB: past
/// that the upload fails as transient and comes again, waiting on the
/// replica rather than here. A row's upload supersedes what the inbox kept
/// of it. What the upload says of the
/// replica — what it refused, applied, missed, holds and has not sent — is
/// filed apart from its rows, so neither holds back the other. An error is
/// the storage's, a full inbox, or an upload of a later protocol than this
/// build speaks.
pub(crate) async fn file<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    upload: &Up,
    sender: Uuid,
    known: &P,
    inbox: i64,
) -> Result<Vec<Filed>, Error> {
    if upload.protocol > envelope::PROTOCOL {
        return Err(Error::Protocol {
            theirs: upload.protocol,
            ours: envelope::PROTOCOL,
        });
    }
    let standing = standing(conn, sender).await?;
    crate::row::savepoint(conn).await?;
    let filed = file_rows(conn, codecs, upload, sender, known, inbox).await;
    let filed = match crate::row::released(conn, filed).await? {
        Ok(filed) => filed,
        Err(e) => {
            keep_whole(conn, codecs, sender, upload, &e.to_string(), inbox).await?;
            Vec::new()
        }
    };
    crate::row::savepoint(conn).await?;
    let said = said(conn, codecs, sender, &standing, upload).await;
    if let Err(e) = crate::row::released(conn, said).await? {
        tracing::error!(%sender, "sync: what a replica said of itself could not be filed — left out: {e}");
    }
    Ok(filed)
}

/// The rows of `upload` filed; what of them could not be is kept in the
/// inbox. A row's upload supersedes whatever of it the inbox kept: filed,
/// that leaves; kept, it takes its place.
async fn file_rows<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    upload: &Up,
    sender: Uuid,
    known: &P,
    inbox: i64,
) -> Result<Vec<Filed>, Error> {
    let mut room = room(conn, sender, inbox).await?;
    if let Keeping::New { parts, bytes, .. } = &mut room {
        if *parts > 0 {
            let (gone, freed) = supersede(conn, codecs, sender, upload).await?;
            (*parts, *bytes) = (*parts - gone, *bytes - freed);
        }
    }
    let (filed, unfiled) = file_tables(conn, codecs, upload, sender, known).await?;
    keep(conn, sender, &unfiled, &mut room).await?;
    Ok(filed)
}

/// Drops what the inbox kept of the rows `upload` from `sender` carries:
/// older versions of them, or of their deletes. Answers how many parts it
/// dropped, and their bytes.
async fn supersede<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    sender: Uuid,
    upload: &Up,
) -> Result<(i64, i64), Error> {
    let mut rows = (Vec::new(), Vec::new());
    for table in &upload.tables {
        let codec = codec_of(codecs, &table.table);
        for key in table.keys.iter().chain(&table.deletes) {
            rows.0.push(stored(&table.table));
            rows.1.push(kept_key(codec, key));
        }
    }
    Ok(sqlx::query_as(
        "WITH gone AS (DELETE FROM sync.inbox i USING unnest($2::text[], $3::text[]) AS u(t, k) \
                        WHERE i.replica_id = $1 AND i.relation = u.t AND i.row_key = u.k \
                        RETURNING length(i.upload) AS bytes) \
         SELECT count(*), coalesce(sum(bytes), 0)::int8 FROM gone",
    )
    .bind(sender)
    .bind(&rows.0)
    .bind(&rows.1)
    .fetch_one(&mut *conn)
    .await?)
}

async fn file_tables<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    upload: &Up,
    sender: Uuid,
    known: &P,
) -> Result<(Vec<Filed>, Vec<Unfiled>), Error> {
    let mut filed = Vec::new();
    let mut unfiled = Vec::new();
    for table in &upload.tables {
        let Some(codec) = codec_of(codecs, &table.table) else {
            let why = "a table this server does not mirror";
            unfiled.extend(
                (0..table.upserts.len()).filter_map(|at| Unfiled::row::<P>(None, table, at, why)),
            );
            unfiled.extend(
                table
                    .deletes
                    .iter()
                    .map(|key| Unfiled::delete::<P>(None, table, key.clone(), why)),
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
                            .filter_map(|r| Unfiled::row(Some(codec), table, r.at, &r.reason)),
                    );
                }
                Err(e) if e.is_transient() => return Err(e),
                Err(e) => {
                    let why = e.to_string();
                    unfiled.extend(
                        (0..table.upserts.len())
                            .filter_map(|at| Unfiled::row(Some(codec), table, at, &why)),
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
                Unfiled::delete(Some(codec), table, key, &why)
            }));
            gone
        };
        filed.push(Filed {
            table: table.table.clone(),
            written,
            gone,
        });
    }
    Ok((filed, unfiled))
}

/// What `upload` says of its sender: whether the server sent the message
/// it says it applied — or this database was restored from a copy that
/// never did, which ends the filing of what it says — then what it refused,
/// before what it applied, which counts as held what it did not refuse; what
/// it applied, or that it was itself restored; what it missed, what it
/// holds, and what it has not sent.
async fn said<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    sender: Uuid,
    standing: &Standing,
    upload: &Up,
) -> Result<(), Error> {
    let confirmed = confirmation(sender, standing, upload);
    if let Confirmed::Unknown(sequence) = confirmed {
        return restored(conn, sender, standing.epoch, sequence).await;
    }
    rejected(conn, codecs, sender, standing, &upload.rejected).await?;
    match confirmed {
        Confirmed::Applied(sequence, nonce) => {
            acknowledge(conn, sender, standing, sequence, nonce).await?;
        }
        Confirmed::Behind(sequence) => {
            let why = format!("it says it applied {sequence}, less than it said before — restored from an older copy");
            provision_again(conn, sender, standing, &why).await?;
        }
        Confirmed::Elsewhere => {
            let why = "it follows an epoch this database never had — the database was restored \
                       from a copy older than it";
            provision_again(conn, sender, standing, why).await?;
        }
        _ => {}
    }
    gap(conn, sender, standing, upload.gap).await?;
    if let Confirmed::Applied(sequence, _) = confirmed {
        compare(conn, codecs, sender, standing, upload, sequence).await?;
    }
    if let Some(told) = &upload.backlog {
        backlog(conn, sender, told).await?;
    }
    Ok(())
}

/// What the server makes of the last message a replica says it applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Confirmed {
    /// It says nothing of one, or of one of another epoch, or one that does
    /// not parse, or whose seal is not the server's: nothing to act on.
    Nothing,
    /// A message the server sent it, by sequence and nonce.
    Applied(i64, i64),
    /// One older than the last it said it applied: the replica was
    /// restored from an older copy.
    Behind(i64),
    /// One of another epoch, from a replica the server last heard apply one
    /// of its own: a replica goes only forward, so it follows an epoch this
    /// database never had — restored from a copy older than it. Unsealed by
    /// any secret the server keeps, so it provisions that replica alone.
    Elsewhere,
    /// One the server sealed and has no record of sending it: past the last
    /// it sent it, or past every number it drew, or the sequence of one it
    /// sent with another nonce. This database is older than what the
    /// replica applied.
    Unknown(i64),
}

/// The last message `sender` says it applied, judged against what the
/// server sent it (PROTOCOL.md 5.2). A seal that is not the server's is a
/// replica that lies, or a bug: it is left out, and never resets anything.
fn confirmation(sender: Uuid, standing: &Standing, upload: &Up) -> Confirmed {
    if upload.sequence == 0 {
        return Confirmed::Nothing;
    }
    let Ok(epoch) = Uuid::parse_str(&upload.epoch) else {
        tracing::error!(%sender, epoch = stored(&upload.epoch), "upload: an epoch that is no UUID — malformed, left out");
        return Confirmed::Nothing;
    };
    if epoch != standing.epoch {
        return if standing.applied.is_some() {
            Confirmed::Elsewhere
        } else {
            Confirmed::Nothing
        };
    }
    let (Ok(sequence), Ok(nonce)) = (i64::try_from(upload.sequence), i64::try_from(upload.nonce))
    else {
        tracing::error!(%sender, sequence = upload.sequence, "upload: a sequence or nonce no message has — malformed, left out");
        return Confirmed::Nothing;
    };
    let place = (upload.sequence, upload.nonce);
    if !envelope::sealed(&standing.secret, sender, epoch, place, &upload.mac) {
        tracing::error!(%sender, sequence, "upload: it says it applied a message this server did not seal — left out");
        return Confirmed::Nothing;
    }
    let other = |known: Option<(i64, i64)>| known.is_some_and(|(s, n)| s == sequence && n != nonce);
    let past = standing
        .last
        .map_or(sequence > standing.drawn, |(last, _)| sequence > last);
    if past || other(standing.last) || other(standing.applied) {
        return Confirmed::Unknown(sequence);
    }
    if standing
        .applied
        .is_some_and(|(applied, _)| sequence < applied)
    {
        return Confirmed::Behind(sequence);
    }
    Confirmed::Applied(sequence, nonce)
}

/// What the server records of a sender.
#[derive(Default, sqlx::FromRow)]
struct Recorded {
    last: Option<i64>,
    last_nonce: Option<i64>,
    applied: Option<i64>,
    applied_nonce: Option<i64>,
    snapshot: Option<i64>,
    snapshot_end: Option<i64>,
    drifted_at: Option<DateTime<Utc>>,
}

/// Takes `sender`'s row of the replicas for the caller's transaction, so its
/// filings — of uploads, or of the inbox — run one after the other. A sender
/// the relay never saw has a row all the same.
async fn one_at_a_time(conn: &mut Conn, sender: Uuid) -> Result<(), Error> {
    sqlx::query(
        "INSERT INTO sync.replicas (replica_id) VALUES ($1) \
         ON CONFLICT (replica_id) DO UPDATE SET replica_id = EXCLUDED.replica_id",
    )
    .bind(sender)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

async fn standing(conn: &mut Conn, sender: Uuid) -> Result<Standing, Error> {
    let (epoch, secret): (Uuid, Vec<u8>) = sqlx::query_as("SELECT epoch, secret FROM sync.epoch")
        .fetch_one(&mut *conn)
        .await?;
    let drawn: i64 = sqlx::query_scalar(
        "SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM sync.messages",
    )
    .fetch_one(&mut *conn)
    .await?;
    one_at_a_time(conn, sender).await?;
    let recorded: Recorded = sqlx::query_as(
        "SELECT c.last_sequence AS last, c.last_nonce, \
                CASE WHEN r.applied_epoch = $2 THEN r.applied_sequence END AS applied, \
                CASE WHEN r.applied_epoch = $2 THEN r.applied_nonce END AS applied_nonce, \
                c.snapshot_sequence AS snapshot, c.snapshot_end, r.drifted_at \
           FROM sync.replicas r LEFT JOIN sync.chains c USING (replica_id) \
          WHERE r.replica_id = $1 FOR UPDATE OF r",
    )
    .bind(sender)
    .bind(epoch)
    .fetch_optional(&mut *conn)
    .await?
    .unwrap_or_default();
    let pair = |s: Option<i64>, n: Option<i64>| s.map(|s| (s, n.unwrap_or_default()));
    Ok(Standing {
        epoch,
        secret,
        drawn,
        last: pair(recorded.last, recorded.last_nonce),
        applied: pair(recorded.applied, recorded.applied_nonce),
        snapshot: recorded.snapshot,
        snapshot_end: recorded.snapshot_end,
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

/// Rows `replica` reported it could not take, or let go of: each refused —
/// still held, for what takes rows back, and not for what it holds — and
/// quarantined, so a retry after a fix sends it, or its delete, again. Only
/// a refusal of the last message that carried the row: one of an older
/// message (a newer version went since), or of another epoch, names
/// nothing; one of a delete re-records the row it could not let go of. A
/// rejection of anything else — a table the server does not send, no key
/// or one that is not the table's, a message never sent it — is malformed.
async fn rejected<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    replica: Uuid,
    standing: &Standing,
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
        if Uuid::parse_str(&row.epoch).ok() != Some(standing.epoch) {
            tracing::debug!(%replica, table = row.table, key, "upload: a rejection of another epoch's message — left out");
            continue;
        }
        let Some(sequence) = i64::try_from(row.sequence)
            .ok()
            .filter(|s| *s > 0 && standing.last.is_some_and(|(last, _)| *s <= last))
        else {
            tracing::error!(%replica, table = row.table, key, sequence = row.sequence, "upload: a rejection of a message never sent it — malformed, left out");
            continue;
        };
        if standing.snapshot.is_some_and(|began| sequence < began) {
            tracing::debug!(%replica, table = row.table, key, "upload: a rejection from before its last provisioning — left out");
            continue;
        }
        if !refuse(conn, replica, standing, codec.table.name, &key, sequence).await? {
            tracing::debug!(%replica, table = row.table, key, "upload: a rejection of a version that no longer matters — left out");
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
            "sync: a replica could not take a row, or let it go — quarantined until a fix lets it through"
        );
    }
    Ok(())
}

/// Records `key` of `table` refused by `replica`'s message `sequence`, as
/// that message left the row: held — unless a later message carried it
/// since — or taken back by it, which the replica could not carry out, or
/// swept by the end of its snapshot, which it could not either. Answers
/// whether it was; any other row the message did not concern.
async fn refuse(
    conn: &mut Conn,
    replica: Uuid,
    standing: &Standing,
    table: &str,
    key: &str,
    sequence: i64,
) -> Result<bool, Error> {
    let held: Option<i64> = sqlx::query_scalar(
        "SELECT sequence FROM sync.held \
          WHERE replica_id = $1 AND relation = $2 AND row_key = $3 FOR UPDATE",
    )
    .bind(replica)
    .bind(table)
    .bind(key)
    .fetch_optional(&mut *conn)
    .await?;
    match held {
        Some(carried) if carried != sequence => return Ok(false),
        Some(_) => {}
        None => {
            let taken = sqlx::query(
                "DELETE FROM sync.gone \
                  WHERE replica_id = $1 AND relation = $2 AND row_key = $3 AND sequence = $4",
            )
            .bind(replica)
            .bind(table)
            .bind(key)
            .bind(sequence)
            .execute(&mut *conn)
            .await?;
            if taken.rows_affected() == 0 && standing.snapshot_end != Some(sequence) {
                return Ok(false);
            }
        }
    }
    sqlx::query(
        "INSERT INTO sync.held AS h (replica_id, relation, row_key, noted, sequence, refused) \
         VALUES ($1, $2, $3, 0, $4, $4) \
         ON CONFLICT (replica_id, relation, row_key) DO UPDATE SET refused = EXCLUDED.refused",
    )
    .bind(replica)
    .bind(table)
    .bind(key)
    .bind(sequence)
    .execute(&mut *conn)
    .await?;
    Ok(true)
}

/// Records that `replica` applied every message up to `sequence` — nonce
/// `nonce` — of the server's epoch, as [`confirmation`] found it sent: the
/// rows those messages carried are held from now on. Caught up, it has
/// stayed silent no more.
async fn acknowledge(
    conn: &mut Conn,
    replica: Uuid,
    standing: &Standing,
    sequence: i64,
    nonce: i64,
) -> Result<(), Error> {
    sqlx::query(
        "UPDATE sync.replicas SET applied_epoch = $2, applied_sequence = $3, applied_nonce = $4, \
           applied_at = now(), \
           heals = CASE WHEN $3 >= (SELECT last_sequence FROM sync.chains WHERE replica_id = $1) \
                        THEN 0 ELSE heals END \
         WHERE replica_id = $1 AND (applied_epoch IS DISTINCT FROM $2 \
                                    OR applied_sequence IS NULL OR applied_sequence < $3)",
    )
    .bind(replica)
    .bind(standing.epoch)
    .bind(sequence)
    .bind(nonce)
    .execute(&mut *conn)
    .await?;
    sqlx::query("DELETE FROM sync.gone WHERE replica_id = $1 AND sequence <= $2")
        .bind(replica)
        .bind(sequence)
        .execute(&mut *conn)
        .await?;
    // A row it refused is taken once it applied a later message carrying it.
    sqlx::query(
        "UPDATE sync.held SET refused = NULL \
          WHERE replica_id = $1 AND refused IS NOT NULL AND sequence > refused AND sequence <= $2",
    )
    .bind(replica)
    .bind(sequence)
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
    .bind(standing.epoch)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// This database is older than what `replica` applied — restored from a
/// copy — so what it records of every replica is too: it takes a new epoch,
/// and a new secret to seal it, unless a filing at once took it already;
/// drops every outbox, and provisions every replica again. Messages of the
/// old epoch still on their way are a server's a replica no longer follows.
async fn restored(conn: &mut Conn, replica: Uuid, epoch: Uuid, sequence: i64) -> Result<(), Error> {
    let rotated = sqlx::query(
        "UPDATE sync.epoch SET epoch = gen_random_uuid(), \
           secret = uuid_send(gen_random_uuid()) || uuid_send(gen_random_uuid()) \
         WHERE epoch = $1",
    )
    .bind(epoch)
    .execute(&mut *conn)
    .await?;
    if rotated.rows_affected() == 0 {
        return Ok(());
    }
    tracing::error!(
        %replica,
        sequence,
        "sync ALARM: a replica applied a message this database has no record of sending it — \
         it was restored from an older copy; a new epoch begins and every replica is provisioned \
         again"
    );
    for sql in [
        "DELETE FROM sync.outbox",
        "DELETE FROM sync.gone",
        "UPDATE sync.chains SET last_sequence = NULL, last_nonce = NULL, snapshot_sequence = NULL, \
           snapshot_end = NULL",
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

/// `replica` holds less than the server records — `why` — and is
/// provisioned again, unless a snapshot written for it since the last it
/// said it applied is on its way, which replaces whatever it holds.
async fn provision_again(
    conn: &mut Conn,
    replica: Uuid,
    standing: &Standing,
    why: &str,
) -> Result<(), Error> {
    let applied = standing.applied.map_or(0, |(s, _)| s);
    if standing.snapshot.is_some_and(|began| began > applied) {
        tracing::debug!(%replica, why, "sync: a replica that holds less has its snapshot on the way");
        return Ok(());
    }
    asked(conn, replica, why).await
}

/// `replica` says it missed a message — `gap`, the newest it could not
/// follow, 0 for none: it applies nothing more until a snapshot begins, so
/// it is provisioned again, unless one that began after that message is on
/// its way. A message past every one sent it is none it could have missed.
async fn gap(conn: &mut Conn, replica: Uuid, standing: &Standing, gap: u64) -> Result<(), Error> {
    if gap == 0 {
        return Ok(());
    }
    let missed = i64::try_from(gap).unwrap_or(i64::MAX);
    if standing
        .last
        .map_or(missed > standing.drawn, |(last, _)| missed > last)
    {
        tracing::error!(%replica, gap, "upload: a gap past every message sent it — malformed, left out");
        return Ok(());
    }
    if standing.snapshot.is_some_and(|began| began > missed) {
        tracing::debug!(%replica, "sync: a replica that missed a message has its snapshot on the way");
        return Ok(());
    }
    asked(conn, replica, &format!("it missed message {missed}")).await
}

/// How long a replica waits between the provisionings it asks for itself:
/// the first, doubling with every one it asks for while it keeps asking, up
/// to 32 times the first.
const ASKED: std::time::Duration = std::time::Duration::from_secs(60);

/// Provisions `replica` again, `why` as it says it — it missed a message,
/// or holds less than recorded — unless it asked for one too soon: what it
/// says of itself is not sealed, and a replica that says it on every upload
/// must not have the server send it everything on every round trip. Its
/// next upload says it again.
async fn asked(conn: &mut Conn, replica: Uuid, why: &str) -> Result<(), Error> {
    let due: bool = sqlx::query_scalar(
        "SELECT asked_at IS NULL \
             OR asked_at <= now() - make_interval(secs => $2 * power(2, least(asks - 1, 5))) \
           FROM sync.replicas WHERE replica_id = $1",
    )
    .bind(replica)
    .bind(ASKED.as_secs_f64())
    .fetch_optional(&mut *conn)
    .await?
    .unwrap_or(true);
    if !due {
        tracing::warn!(%replica, why, "sync: a replica asks to be provisioned again too soon after the last time — not yet");
        return Ok(());
    }
    if !resync(conn, replica).await? {
        return Ok(());
    }
    tracing::warn!(%replica, why, "sync: a replica holds less than recorded — it is provisioned again");
    // Quiet long enough, it starts over.
    sqlx::query(
        "UPDATE sync.replicas SET asked_at = now(), \
           asks = CASE WHEN asked_at < now() - make_interval(secs => $2 * 64) THEN 1 \
                       ELSE asks + 1 END \
         WHERE replica_id = $1",
    )
    .bind(replica)
    .bind(ASKED.as_secs_f64())
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Compares what `replica` says it holds with what the server records it
/// holds, and provisions it again from scratch where they differ: the
/// backstop for whatever else went wrong. Only once the replica has applied
/// every message recorded for it — `applied`, the last it confirmed, is the
/// last the server sent it — and as the server's record stood then: a pass
/// that writes it more while this is read moves its chain, and the
/// comparison waits for the next digests. Rows it refused are left out, as
/// it leaves them out. Digests that differ again within a day of the last
/// provisioning they caused are a bug another one would not cure: logged,
/// not acted on. Answers the tables that differed.
async fn compare<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    replica: Uuid,
    standing: &Standing,
    upload: &Up,
    applied: i64,
) -> Result<Vec<String>, Error> {
    if upload.digests.is_empty() {
        return Ok(Vec::new());
    }
    let mut tables = std::collections::BTreeSet::new();
    for digest in &upload.digests {
        if !tables.insert(digest.table.as_str()) {
            tracing::error!(%replica, table = stored(&digest.table), "upload: digests of one table twice — malformed, not compared");
            return Ok(Vec::new());
        }
    }
    // A table this server does not send — of a later build's, or one it no
    // longer sends — is the replica's own to hold: not compared.
    let digests: Vec<&envelope::Digest> = upload
        .digests
        .iter()
        .filter(|d| codec_of(codecs, &d.table).is_some_and(Codec::sent_down))
        .collect();
    let names: Vec<&str> = digests.iter().map(|d| d.table.as_str()).collect();
    // One statement, so the keys are those of the chain it reads.
    let (last, held): (
        Option<i64>,
        sqlx::types::Json<BTreeMap<String, Vec<String>>>,
    ) = sqlx::query_as(
        "SELECT (SELECT last_sequence FROM sync.chains WHERE replica_id = $1), \
                    coalesce((SELECT jsonb_object_agg(relation, keys) FROM ( \
                       SELECT relation, jsonb_agg(row_key) AS keys FROM sync.held \
                        WHERE replica_id = $1 AND relation = ANY($2) AND refused IS NULL \
                        GROUP BY relation) t), '{}')",
    )
    .bind(replica)
    .bind(&names)
    .fetch_one(&mut *conn)
    .await?;
    if standing.last.map(|(last, _)| last) != Some(applied) || last != Some(applied) {
        tracing::debug!(%replica, "sync: digests taken with messages still on their way — not compared");
        return Ok(Vec::new());
    }
    let mut held = held.0;
    let mut differ = Vec::new();
    for digest in digests {
        let keys = held.remove(&digest.table).unwrap_or_default();
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

/// Tables a backlog may name: more than any server mirrors.
const BACKLOG_TABLES: usize = 1024;

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
    let Some(tables) = tables.filter(|t| t.len() <= BACKLOG_TABLES) else {
        tracing::error!(%replica, "upload: a backlog counting more rows than a table has, or more tables than a server mirrors — malformed, left out");
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

/// Bytes of a replica's uploads the server keeps, unable to file them,
/// besides the parts its settings count: a few parts as large as an upload
/// may be are as much as a replica may make it hold.
const INBOX_BYTES: i64 = 64 << 20;

/// A part of an upload waiting in the inbox.
#[derive(Debug, sqlx::FromRow)]
pub(crate) struct Kept {
    pub(crate) id: i64,
    pub(crate) replica_id: Uuid,
    upload: Vec<u8>,
    first_at: DateTime<Utc>,
    attempts: i32,
}

/// What is kept in the inbox: a part new to it, while the replica's part of
/// it holds fewer than `bound` parts — it holds `parts`, of `bytes` — or one
/// kept before that failed again, which keeps its age.
enum Keeping<'k> {
    New { bound: i64, parts: i64, bytes: i64 },
    Again(&'k Kept),
}

/// Room for what an upload from `replica` cannot file: what its part of the
/// inbox holds, counted once.
async fn room(conn: &mut Conn, replica: Uuid, bound: i64) -> Result<Keeping<'static>, Error> {
    let (parts, bytes): (i64, i64) = sqlx::query_as(
        "SELECT count(*), coalesce(sum(length(upload)), 0)::int8 FROM sync.inbox \
          WHERE replica_id = $1",
    )
    .bind(replica)
    .fetch_one(&mut *conn)
    .await?;
    Ok(Keeping::New {
        bound,
        parts,
        bytes,
    })
}

/// Keeps what of an upload from `replica` could not be filed: each part in
/// place of whatever the inbox kept of its row.
async fn keep(
    conn: &mut Conn,
    replica: Uuid,
    unfiled: &[Unfiled],
    keeping: &mut Keeping<'_>,
) -> Result<(), Error> {
    use prost::Message as _;
    for part in unfiled {
        tracing::error!(
            %replica,
            table = stored(&part.table),
            reason = part.reason,
            "sync: what a replica uploaded the server could not file — kept until a fix files it"
        );
        let upload = part.up.encode_to_vec();
        let since = match keeping {
            Keeping::Again(kept) => Some(&**kept),
            Keeping::New {
                bound,
                parts,
                bytes,
            } => {
                if *parts >= *bound || *bytes >= INBOX_BYTES {
                    tracing::error!(%replica, parts, bytes, "sync ALARM: a replica's inbox is full — its uploads wait on it until a fix files what is kept");
                    return Err(Error::Host {
                        reason: format!(
                            "the server keeps {parts} parts of this replica's uploads it could \
                             not file, {bytes} bytes, as many as it may: the upload comes again \
                             once a fix files them"
                        ),
                        transient: true,
                    });
                }
                *parts += 1;
                *bytes += i64::try_from(upload.len()).unwrap_or(i64::MAX);
                None
            }
        };
        sqlx::query(
            "INSERT INTO sync.inbox AS i \
               (replica_id, relation, row_key, upload, reason, first_at, attempts) \
             VALUES ($1, $2, $3, $4, $5, coalesce($6, now()), $7) \
             ON CONFLICT (replica_id, relation, row_key) DO UPDATE SET upload = EXCLUDED.upload, \
               reason = EXCLUDED.reason, first_at = EXCLUDED.first_at, last_at = now(), \
               attempts = EXCLUDED.attempts",
        )
        .bind(replica)
        .bind(stored(&part.table))
        .bind(&part.key)
        .bind(&upload)
        .bind(stored(&part.reason))
        .bind(since.map(|k| k.first_at))
        .bind(since.map_or(1, |k| k.attempts + 1))
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

/// Keeps every row and delete of an upload from `replica`, each as a part
/// of its own, when filing it failed as a whole for a reason a retry cannot
/// change — while its part of the inbox holds fewer than `inbox`. What it
/// says of the replica is of its moment, and not kept.
pub(crate) async fn keep_whole<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    replica: Uuid,
    upload: &Up,
    reason: &str,
    inbox: i64,
) -> Result<(), Error> {
    let reason = format!("filing its upload failed as a whole: {reason}");
    let mut room = room(conn, replica, inbox).await?;
    keep(
        conn,
        replica,
        &Unfiled::all(codecs, upload, &reason),
        &mut room,
    )
    .await
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
/// age. Only the rows it carried are filed: what it said of its moment —
/// what it refused, applied, missed, held, had not sent — is out of date by
/// now, and was filed when it came. One filing of a replica runs at a time,
/// its uploads and its inbox alike: a part an upload of its row superseded
/// meanwhile is gone, and files nothing.
pub(crate) async fn refile<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    kept: &Kept,
    known: &P,
) -> Result<bool, Error> {
    use prost::Message as _;
    let up = Up::decode(kept.upload.as_slice()).map_err(|e| Error::Malformed {
        table: "sync.inbox".to_owned(),
        reason: e.to_string(),
    })?;
    if up.protocol > envelope::PROTOCOL {
        return Err(Error::Protocol {
            theirs: up.protocol,
            ours: envelope::PROTOCOL,
        });
    }
    one_at_a_time(conn, kept.replica_id).await?;
    let taken = sqlx::query("DELETE FROM sync.inbox WHERE id = $1")
        .bind(kept.id)
        .execute(&mut *conn)
        .await?;
    if taken.rows_affected() == 0 {
        return Ok(true);
    }
    let (_, unfiled) = file_tables(conn, codecs, &up, kept.replica_id, known).await?;
    keep(conn, kept.replica_id, &unfiled, &mut Keeping::Again(kept)).await?;
    Ok(unfiled.is_empty())
}

/// Drops `kept` once it waited longer than `after` to be filed: its sender
/// is known no more. Answers whether it was.
pub(crate) async fn forget(
    conn: &mut Conn,
    kept: &Kept,
    after: std::time::Duration,
) -> Result<bool, Error> {
    let gone = sqlx::query(
        "DELETE FROM sync.inbox WHERE id = $1 AND first_at < now() - make_interval(secs => $2)",
    )
    .bind(kept.id)
    .bind(after.as_secs_f64())
    .execute(&mut *conn)
    .await?;
    Ok(gone.rows_affected() > 0)
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
