//! One pass of the server's relay, in three steps a host runs in order:
//!
//! 1. [`Relay::prepare`] reads the pending notes and plans them, in a
//!    transaction of its own, closed before anything is sent.
//! 2. [`Relay::publish`] sends each replica what it is owed, with no
//!    transaction open.
//! 3. [`Relay::record`] records what each replica now holds and marks the
//!    notes done, in a transaction that commits only while this process is
//!    the relay (the host's lock).
//!
//! A crash between the steps sends the same rows again, which a replica
//! applies idempotently; a row is never recorded as held that was not sent.
//!
//! Every failure is one of two kinds ([`Error::is_transient`]). A transient
//! one — the database or the transport away — stops the pass, and the same
//! pass runs again. A permanent one is narrowed to what it is about and set
//! aside, so it holds back nothing else:
//!
//! - a note the planner fails on is found by halving the batch and parked;
//! - a row that does not encode, or that the transport refuses, is
//!   quarantined for the replicas that were to get it;
//! - a replica the transport cannot reach waits out a backoff, and what it is
//!   owed is noted again for it alone, while every other replica carries on.
//!
//! Parked notes and quarantined rows are tried again on a timer and when the
//! relay starts ([`unpark`], [`retry_quarantine`]): a deploy that fixes the
//! cause lets them through, and no one repairs data by hand.

use crate::envelope::{self, Body, EndSnapshot, Limits};
use crate::plan::{pending, Delta, Engine, Loaded, Note, Unsent};
use crate::row::Conn;
use crate::wire::Codec;
use crate::Error;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use uuid::Uuid;

/// Where messages go: `message` is a [`ToReplica`](envelope::ToReplica),
/// encoded, for `replica`. `id` is the same for the same message of the same
/// batch, so a transport that deduplicates drops a pass sent again.
///
/// An error is transient when a retry may get it through (the network, the
/// broker away) and permanent when the transport will never take this message;
/// the engine already keeps every message within [`Limits`].
pub trait Transport: Sync {
    fn send(
        &self,
        replica: Uuid,
        message: Vec<u8>,
        id: &str,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}

/// The relay's settings.
pub struct Relay<'a, P> {
    pub engine: &'a Engine,
    pub codecs: &'a [Codec<P>],
    pub limits: Limits,
    /// Notes per pass.
    pub batch: i64,
}

/// Notes planned together, and what they owe.
struct Group {
    notes: Vec<Note>,
    delta: Delta,
    loaded: Loaded,
    /// Replicas left out of this pass: waiting out a failure, or failed now.
    deferred: BTreeSet<Uuid>,
}

/// A pass planned, waiting to be published.
pub struct Prepared {
    groups: Vec<Group>,
    parked: Vec<(i64, String)>,
    /// The pass's place in the message sequence, and the installation it is
    /// from: its messages are numbered from here.
    base: u64,
    epoch: String,
}

/// A pass published, waiting to be recorded.
pub struct Sent {
    groups: Vec<Group>,
    parked: Vec<(i64, String)>,
    /// Replicas a send failed to, with why, and whether a retry may help.
    failed: Vec<(Uuid, String, bool)>,
    /// Replicas sent to, with the last number they were sent.
    sent_to: BTreeMap<Uuid, u64>,
    /// Each row sent, and the number of the message that carried it.
    carried: Vec<(Uuid, String, String, u64)>,
}

/// What a pass did.
#[derive(Debug, Default)]
pub struct Outcome {
    /// Notes done.
    pub done: usize,
    /// Notes the planner set aside.
    pub parked: Vec<(i64, String)>,
    /// Rows quarantined for a replica.
    pub quarantined: usize,
    /// Replicas a send failed to, with why.
    pub failing: Vec<(Uuid, String)>,
}

impl<P: Sync + 'static> Relay<'_, P> {
    /// Reads and plans the pending notes; `None` when there are none. `conn`
    /// must be in a transaction: a plan that fails is rolled back to a
    /// savepoint, so the next can run.
    pub async fn prepare(&self, conn: &mut Conn) -> Result<Option<Prepared>, Error> {
        let notes = pending(conn, self.batch).await?;
        if notes.is_empty() {
            return Ok(None);
        }
        let waiting: BTreeSet<Uuid> =
            sqlx::query_scalar("SELECT replica_id FROM sync.replicas WHERE retry_after > now()")
                .fetch_all(&mut *conn)
                .await?
                .into_iter()
                .collect();
        let mut by_scope: BTreeMap<Option<Uuid>, Vec<Note>> = BTreeMap::new();
        for note in notes {
            by_scope.entry(note.scope).or_default().push(note);
        }
        // Drawn here, in the pass's own transaction: a sequence does not roll
        // back, so a pass that never records still never reuses a number.
        let base: i64 = sqlx::query_scalar("SELECT nextval('sync.messages')")
            .fetch_one(&mut *conn)
            .await?;
        let epoch: Uuid = sqlx::query_scalar("SELECT epoch FROM sync.epoch")
            .fetch_one(&mut *conn)
            .await?;
        let mut prepared = Prepared {
            groups: Vec::new(),
            parked: Vec::new(),
            base: u64::try_from(base).unwrap_or_default() << SEQUENCE_SHIFT,
            epoch: epoch.to_string(),
        };
        for (scope, notes) in by_scope {
            self.plan_isolating(conn, notes, scope, &waiting, &mut prepared)
                .await?;
        }
        Ok(Some(prepared))
    }

    /// Plans `notes` together; when that fails for good, halves them until
    /// the notes it fails on are alone, and parks those.
    fn plan_isolating<'b>(
        &'b self,
        conn: &'b mut Conn,
        mut notes: Vec<Note>,
        scope: Option<Uuid>,
        waiting: &'b BTreeSet<Uuid>,
        prepared: &'b mut Prepared,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'b>> {
        Box::pin(async move {
            match self.plan(conn, &notes, scope, waiting).await {
                Ok(group) => prepared.groups.push(group),
                Err(e) if e.is_transient() => return Err(e),
                Err(e) if notes.len() == 1 => prepared.parked.push((notes[0].id, e.to_string())),
                Err(_) => {
                    let right = notes.split_off(notes.len() / 2);
                    self.plan_isolating(conn, notes, scope, waiting, prepared)
                        .await?;
                    self.plan_isolating(conn, right, scope, waiting, prepared)
                        .await?;
                }
            }
            Ok(())
        })
    }

    async fn plan(
        &self,
        conn: &mut Conn,
        notes: &[Note],
        scope: Option<Uuid>,
        waiting: &BTreeSet<Uuid>,
    ) -> Result<Group, Error> {
        sqlx::query("SAVEPOINT sync_plan")
            .execute(&mut *conn)
            .await?;
        let planned = async {
            let changes: Vec<_> = notes.iter().map(|n| n.change.clone()).collect();
            let mut delta = self.engine.plan(conn, &changes).await?;
            if let Some(replica) = scope {
                delta.keep_only(replica);
            }
            let deferred: BTreeSet<Uuid> = Engine::replicas(&delta)
                .intersection(waiting)
                .copied()
                .collect();
            for replica in &deferred {
                delta.forget(*replica);
            }
            let loaded = self
                .engine
                .load(conn, self.codecs, &mut delta, self.limits.largest_row())
                .await?;
            Ok::<_, Error>(Group {
                notes: notes.to_vec(),
                delta,
                loaded,
                deferred,
            })
        }
        .await;
        let end = if planned.is_ok() {
            "RELEASE SAVEPOINT sync_plan"
        } else {
            "ROLLBACK TO SAVEPOINT sync_plan"
        };
        sqlx::query(end).execute(&mut *conn).await?;
        planned
    }

    /// Sends each replica what it is owed: rows parents first, then deletes
    /// children first, then the end of a snapshot. A replica the transport
    /// fails on is left out of what is recorded.
    pub async fn publish<T: Transport>(&self, transport: &T, prepared: Prepared) -> Sent {
        let mut sent = Sent {
            groups: Vec::new(),
            parked: prepared.parked,
            failed: Vec::new(),
            sent_to: BTreeMap::new(),
            carried: Vec::new(),
        };
        let mut sequence = prepared.base;
        for mut group in prepared.groups {
            let first = group.notes.first().map(|n| n.id).unwrap_or_default();
            let last = group.notes.last().map(|n| n.id).unwrap_or_default();
            for replica in Engine::replicas(&group.delta) {
                let mut messages = self.messages(&group, replica);
                let mut carried = Vec::new();
                for message in &mut messages {
                    sequence += 1;
                    match message {
                        Body::Down(m) => {
                            (m.sequence, m.epoch) = (sequence, prepared.epoch.clone());
                            for table in &m.tables {
                                for key in &table.keys {
                                    let key = key.parts.join("|");
                                    carried.push((replica, table.table.clone(), key, sequence));
                                }
                            }
                        }
                        Body::EndSnapshot(m) => {
                            (m.sequence, m.epoch) = (sequence, prepared.epoch.clone())
                        }
                        Body::Filed(_) => {}
                    }
                }
                let mut failure = None;
                for message in messages {
                    let bytes = message.encoded();
                    let id = format!("{first}-{last}-{replica}-{:016x}", digest(&bytes));
                    if let Err(e) = transport.send(replica, bytes, &id).await {
                        failure = Some(e);
                        break;
                    }
                }
                let Some(e) = failure else {
                    sent.sent_to.insert(replica, sequence);
                    sent.carried.extend(carried);
                    continue;
                };
                if !e.is_transient() {
                    // A message the transport will never take: what it was
                    // to carry waits in quarantine for a fix.
                    for (table, key) in group.delta.upserted(replica) {
                        group.loaded.failed.push(Unsent {
                            replica,
                            table: self.engine.table(table),
                            key,
                            reason: e.to_string(),
                        });
                    }
                } else {
                    group.deferred.insert(replica);
                }
                group.delta.forget(replica);
                sent.failed.push((replica, e.to_string(), e.is_transient()));
            }
            sent.groups.push(group);
        }
        sent
    }

    fn messages(&self, group: &Group, replica: Uuid) -> Vec<Body> {
        let tables = self.engine.rows(&group.delta, &group.loaded, replica);
        let snapshot = group
            .delta
            .snapshots
            .contains(&replica)
            .then(|| Uuid::now_v7().to_string());
        let mut messages: Vec<Body> =
            envelope::pack(tables, snapshot.as_deref().unwrap_or_default(), self.limits)
                .into_iter()
                .map(Body::Down)
                .collect();
        if let Some(snapshot_id) = snapshot {
            messages.push(Body::EndSnapshot(EndSnapshot {
                sequence: 0,
                epoch: String::new(),
                snapshot_id,
                tables: self
                    .engine
                    .snapshot_tables()
                    .into_iter()
                    .map(str::to_owned)
                    .collect(),
            }));
        }
        messages
    }

    /// Records the pass, in the caller's transaction: what each replica now
    /// holds, the notes done, the notes parked, the rows quarantined, the
    /// replicas that failed and what they are still owed.
    pub async fn record(&self, conn: &mut Conn, sent: Sent) -> Result<Outcome, Error> {
        let mut outcome = Outcome {
            parked: sent.parked,
            failing: sent
                .failed
                .iter()
                .map(|(replica, why, _)| (*replica, why.clone()))
                .collect(),
            ..Outcome::default()
        };
        for group in &sent.groups {
            self.engine.record(conn, &group.delta).await?;
            for replica in Engine::replicas(&group.delta) {
                let keys = group.delta.upserted(replica);
                release(conn, self.engine, replica, &keys).await?;
            }
            settle(conn, self.engine, group).await?;
            outcome.quarantined += group.loaded.failed.len();
            quarantine(conn, &group.loaded.failed).await?;
            // Owed to a replica left out: noted again for it alone, unless
            // the note already was.
            let mut done = Vec::new();
            for note in &group.notes {
                match note.scope {
                    Some(replica) if group.deferred.contains(&replica) => {}
                    _ => done.push(note.id),
                }
            }
            let global: Vec<i64> = group
                .notes
                .iter()
                .filter(|n| n.scope.is_none())
                .map(|n| n.id)
                .collect();
            for replica in &group.deferred {
                rescope(conn, &global, *replica).await?;
            }
            crate::plan::mark_done(conn, &done).await?;
            outcome.done += done.len();
        }
        let (replicas, tables, keys, sequences) = sent.carried.into_iter().fold(
            (Vec::new(), Vec::new(), Vec::new(), Vec::new()),
            |(mut r, mut t, mut k, mut s), (replica, table, key, sequence)| {
                r.push(replica);
                t.push(table);
                k.push(key);
                s.push(i64::try_from(sequence).unwrap_or(i64::MAX));
                (r, t, k, s)
            },
        );
        sqlx::query(
            "UPDATE sync.held h SET sequence = u.s \
               FROM unnest($1::uuid[], $2::text[], $3::text[], $4::bigint[]) AS u(r, t, k, s) \
              WHERE h.replica_id = u.r AND h.relation = u.t AND h.row_key = u.k",
        )
        .bind(&replicas)
        .bind(&tables)
        .bind(&keys)
        .bind(&sequences)
        .execute(&mut *conn)
        .await?;
        for (id, why) in &outcome.parked {
            sqlx::query(
                "UPDATE sync.notes SET parked_at = now(), attempts = attempts + 1, last_error = $2 \
                 WHERE id = $1",
            )
            .bind(id)
            .bind(why)
            .execute(&mut *conn)
            .await?;
        }
        for (replica, why, _) in &sent.failed {
            sqlx::query(
                "INSERT INTO sync.replicas AS r (replica_id, failures, retry_after, last_error) \
                 VALUES ($1, 1, now() + interval '2 seconds', $2) \
                 ON CONFLICT (replica_id) DO UPDATE SET \
                   failures = r.failures + 1, last_error = EXCLUDED.last_error, \
                   retry_after = now() + make_interval(secs => least(power(2, r.failures + 1), 900))",
            )
            .bind(replica)
            .bind(why)
            .execute(&mut *conn)
            .await?;
        }
        let (sent_to, last): (Vec<Uuid>, Vec<i64>) = sent
            .sent_to
            .into_iter()
            .map(|(replica, sequence)| (replica, i64::try_from(sequence).unwrap_or(i64::MAX)))
            .unzip();
        sqlx::query(
            "INSERT INTO sync.replicas AS r (replica_id, last_sent_at, last_sequence) \
             SELECT r, now(), s FROM unnest($1::uuid[], $2::bigint[]) AS u(r, s) \
             ON CONFLICT (replica_id) DO UPDATE SET \
               failures = 0, retry_after = NULL, last_error = NULL, last_sent_at = now(), \
               last_sequence = EXCLUDED.last_sequence",
        )
        .bind(&sent_to)
        .bind(&last)
        .execute(&mut *conn)
        .await?;
        Ok(outcome)
    }
}

/// Room for a pass's messages under one draw from the sequence.
const SEQUENCE_SHIFT: u32 = 20;

/// Rows `replica` reported it could not take: no longer counted as held, and
/// quarantined, so a retry after a fix sends them again.
pub async fn rejected(
    conn: &mut Conn,
    replica: Uuid,
    rejected: &[envelope::Rejected],
) -> Result<(), Error> {
    for row in rejected {
        let key = row
            .key
            .as_ref()
            .map(|k| k.parts.join("|"))
            .unwrap_or_default();
        let reason = format!("rejected by the replica: {}", row.reason);
        sqlx::query(
            "DELETE FROM sync.held WHERE replica_id = $1 AND relation = $2 AND row_key = $3",
        )
        .bind(replica)
        .bind(&row.table)
        .bind(&key)
        .execute(&mut *conn)
        .await?;
        sqlx::query(
            "INSERT INTO sync.quarantine AS q (replica_id, relation, row_key, reason) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (replica_id, relation, row_key) DO UPDATE SET \
               reason = EXCLUDED.reason, last_at = now(), attempts = q.attempts + 1",
        )
        .bind(replica)
        .bind(&row.table)
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

/// How one replica is doing, for its status page.
#[derive(Debug, Default, sqlx::FromRow)]
pub struct ReplicaStatus {
    /// Notes owed to it alone, after a send to it failed.
    pub owed: i64,
    /// Rows waiting in quarantine for it.
    pub quarantined: i64,
    /// Sends to it that failed in a row, and the last one's error.
    pub failures: i32,
    pub last_error: Option<String>,
    /// Seconds until it is tried again, while it waits out a failure.
    pub retry_in: Option<f64>,
    /// Seconds since it was last sent to.
    pub sent_ago: Option<f64>,
}

/// How `replica` is doing.
pub async fn replica_status(conn: &mut Conn, replica: Uuid) -> Result<ReplicaStatus, Error> {
    Ok(sqlx::query_as(
        "SELECT \
           (SELECT count(*) FROM sync.notes WHERE scope = $1 AND done_at IS NULL) AS owed, \
           (SELECT count(*) FROM sync.quarantine WHERE replica_id = $1) AS quarantined, \
           coalesce(r.failures, 0) AS failures, r.last_error, \
           extract(epoch FROM r.retry_after - now())::float8 AS retry_in, \
           extract(epoch FROM now() - r.last_sent_at)::float8 AS sent_ago \
         FROM (SELECT 1) one LEFT JOIN sync.replicas r ON r.replica_id = $1",
    )
    .bind(replica)
    .fetch_one(&mut *conn)
    .await?)
}

/// Records that `replica` applied every message up to `sequence` from the
/// server installation `epoch`: the rows those messages carried are held from
/// now on. An acknowledgement from another epoch, or behind the last one, is
/// ignored.
pub async fn acknowledge(
    conn: &mut Conn,
    replica: Uuid,
    epoch: &str,
    sequence: u64,
) -> Result<(), Error> {
    let Ok(epoch) = Uuid::parse_str(epoch) else {
        return Ok(());
    };
    sqlx::query(
        "INSERT INTO sync.replicas AS r (replica_id, applied_epoch, applied_sequence, applied_at) \
         SELECT $1, $2, $3, now() FROM sync.epoch e WHERE e.epoch = $2 \
         ON CONFLICT (replica_id) DO UPDATE SET \
           applied_epoch = EXCLUDED.applied_epoch, applied_sequence = EXCLUDED.applied_sequence, \
           applied_at = now() \
         WHERE r.applied_epoch IS DISTINCT FROM EXCLUDED.applied_epoch \
            OR r.applied_sequence IS NULL OR r.applied_sequence < EXCLUDED.applied_sequence",
    )
    .bind(replica)
    .bind(epoch)
    .bind(i64::try_from(sequence).unwrap_or(i64::MAX))
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Keeps rows `replica` uploaded that could not be filed, as they came.
pub async fn hold(
    conn: &mut Conn,
    replica: Uuid,
    unfiled: &[crate::replica::Unfiled],
) -> Result<(), Error> {
    for row in unfiled {
        tracing::error!(
            %replica,
            table = row.table,
            reason = row.reason,
            "sync: an uploaded row the server could not file — kept until a fix files it"
        );
        sqlx::query(
            "INSERT INTO sync.inbox (replica_id, relation, row_bytes, reason) VALUES ($1, $2, $3, $4)",
        )
        .bind(replica)
        .bind(&row.table)
        .bind(&row.bytes)
        .bind(&row.reason)
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

/// A row waiting in the inbox.
#[derive(Debug, sqlx::FromRow)]
pub struct Inboxed {
    pub id: i64,
    pub replica_id: Uuid,
    pub relation: String,
    pub row_bytes: Vec<u8>,
}

/// Up to `limit` rows waiting in the inbox, oldest first.
pub async fn inbox(conn: &mut Conn, limit: i64) -> Result<Vec<Inboxed>, Error> {
    Ok(sqlx::query_as(
        "SELECT id, replica_id, relation, row_bytes FROM sync.inbox ORDER BY id LIMIT $1",
    )
    .bind(limit)
    .fetch_all(&mut *conn)
    .await?)
}

/// Files a row from the inbox again, in the caller's transaction, under its
/// replica and what the server knows of it now; answers whether it was filed.
/// One filed leaves the inbox; one still refused stays, with why.
pub async fn refile<P: Sync + 'static>(
    conn: &mut Conn,
    codecs: &[Codec<P>],
    row: &Inboxed,
    known: &P,
) -> Result<bool, Error> {
    let why = match crate::replica::codec_of(codecs, &row.relation) {
        None => "a table this server does not mirror".to_owned(),
        Some(codec) => match codec
            .apply_up(conn, vec![row.row_bytes.clone()], row.replica_id, known)
            .await
        {
            Ok(taken) if taken.rejected.is_empty() => {
                sqlx::query("DELETE FROM sync.inbox WHERE id = $1")
                    .bind(row.id)
                    .execute(&mut *conn)
                    .await?;
                return Ok(true);
            }
            Ok(taken) => taken
                .rejected
                .into_iter()
                .map(|(_, why)| why)
                .collect::<Vec<_>>()
                .join("; "),
            Err(e) if !e.is_transient() => e.to_string(),
            Err(e) => return Err(e),
        },
    };
    sqlx::query(
        "UPDATE sync.inbox SET reason = $2, last_at = now(), attempts = attempts + 1 WHERE id = $1",
    )
    .bind(row.id)
    .bind(why)
    .execute(&mut *conn)
    .await?;
    Ok(false)
}

/// Compares what `replica` says it holds with what the server records it
/// holds, and provisions it again from scratch where they differ: the
/// backstop for whatever else went wrong. Only once the replica has applied
/// every message recorded for it — before that, a difference is just a
/// message on its way. Rows quarantined for it are left out, as it leaves
/// out the rows it rejected. Answers the tables that differed.
pub async fn compare(
    conn: &mut Conn,
    replica: Uuid,
    upload: &envelope::Up,
) -> Result<Vec<String>, Error> {
    if upload.digests.is_empty() {
        return Ok(Vec::new());
    }
    let epoch: Uuid = sqlx::query_scalar("SELECT epoch FROM sync.epoch")
        .fetch_one(&mut *conn)
        .await?;
    let last: Option<Option<i64>> =
        sqlx::query_scalar("SELECT last_sequence FROM sync.replicas WHERE replica_id = $1")
            .bind(replica)
            .fetch_optional(&mut *conn)
            .await?;
    let settled = upload.epoch == epoch.to_string()
        && last.flatten().and_then(|s| u64::try_from(s).ok()) == Some(upload.sequence);
    if !settled {
        tracing::debug!(%replica, "sync: digests taken with messages still on their way — not compared");
        return Ok(Vec::new());
    }
    let mut differ = Vec::new();
    for digest in &upload.digests {
        let mut keys: Vec<String> = sqlx::query_scalar(
            "SELECT h.row_key FROM sync.held h WHERE h.replica_id = $1 AND h.relation = $2 \
               AND NOT EXISTS (SELECT 1 FROM sync.quarantine q \
                                WHERE q.replica_id = h.replica_id AND q.relation = h.relation \
                                  AND q.row_key = h.row_key)",
        )
        .bind(replica)
        .bind(&digest.table)
        .fetch_all(&mut *conn)
        .await?;
        let count = keys.len() as u64;
        if count != digest.count || envelope::fingerprint(&mut keys) != digest.fingerprint {
            tracing::error!(
                %replica,
                table = digest.table,
                held_here = count,
                held_there = digest.count,
                "sync: a replica holds other than what the server records — provisioning it again"
            );
            differ.push(digest.table.clone());
        }
    }
    if !differ.is_empty() {
        sqlx::query(
            "INSERT INTO sync.notes (kind, replica_id) SELECT 'resync', $1 \
              WHERE NOT EXISTS (SELECT 1 FROM sync.notes \
                                 WHERE kind = 'resync' AND replica_id = $1 AND done_at IS NULL)",
        )
        .bind(replica)
        .execute(&mut *conn)
        .await?;
    }
    Ok(differ)
}

/// A content fingerprint, the same for the same bytes in the same build.
fn digest(bytes: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

/// Notes `ids` again, owed to `replica` alone, as of the transactions that
/// first noted them.
async fn rescope(conn: &mut Conn, ids: &[i64], replica: Uuid) -> Result<(), Error> {
    sqlx::query(
        "INSERT INTO sync.notes (kind, relation, row_key, replica_id, partition_id, previous, scope, xid, noted_at) \
         SELECT kind, relation, row_key, replica_id, partition_id, previous, $2, xid, noted_at \
           FROM sync.notes WHERE id = ANY($1) ORDER BY id",
    )
    .bind(ids)
    .bind(replica)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Sets `unsent` aside: each row stays owed to its replica until a retry
/// gets it there.
async fn quarantine(conn: &mut Conn, unsent: &[Unsent]) -> Result<(), Error> {
    for row in unsent {
        tracing::error!(
            replica = %row.replica,
            table = row.table,
            key = row.key,
            reason = row.reason,
            "sync: a row quarantined for a replica — fix the cause and it goes on the next retry"
        );
        sqlx::query(
            "INSERT INTO sync.quarantine AS q (replica_id, relation, row_key, reason) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (replica_id, relation, row_key) DO UPDATE SET \
               reason = EXCLUDED.reason, last_at = now(), attempts = q.attempts + 1",
        )
        .bind(row.replica)
        .bind(row.table)
        .bind(&row.key)
        .bind(&row.reason)
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

/// A row tried again for one replica that the plan no longer sends it —
/// deleted, retired, no longer its — is owed to nobody: it leaves the
/// quarantine, unless it failed again.
async fn settle(conn: &mut Conn, engine: &Engine, group: &Group) -> Result<(), Error> {
    for note in &group.notes {
        let (Some(replica), crate::plan::Change::Row { table, key, .. }) =
            (note.scope, &note.change)
        else {
            continue;
        };
        let sent = group
            .delta
            .upserted(replica)
            .iter()
            .any(|(i, k)| engine.table(*i) == table && k == key);
        let failed = group
            .loaded
            .failed
            .iter()
            .any(|u| u.replica == replica && u.table == table && &u.key == key);
        if sent || failed || group.deferred.contains(&replica) {
            continue;
        }
        sqlx::query(
            "DELETE FROM sync.quarantine WHERE replica_id = $1 AND relation = $2 AND row_key = $3",
        )
        .bind(replica)
        .bind(table)
        .bind(key)
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

/// Rows a replica now holds leave the quarantine.
async fn release(
    conn: &mut Conn,
    engine: &Engine,
    replica: Uuid,
    keys: &[(usize, String)],
) -> Result<(), Error> {
    if keys.is_empty() {
        return Ok(());
    }
    let tables: Vec<&str> = keys.iter().map(|(i, _)| engine.table(*i)).collect();
    let keys: Vec<&str> = keys.iter().map(|(_, k)| k.as_str()).collect();
    sqlx::query(
        "DELETE FROM sync.quarantine q USING unnest($2::text[], $3::text[]) AS h(t, k) \
         WHERE q.replica_id = $1 AND q.relation = h.t AND q.row_key = h.k",
    )
    .bind(replica)
    .bind(&tables)
    .bind(&keys)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Lets the parked notes be planned again; answers how many. For when the
/// relay starts — after a deploy, which may have fixed what they failed on —
/// and on a timer.
pub async fn unpark(conn: &mut Conn) -> Result<u64, Error> {
    let done = sqlx::query("UPDATE sync.notes SET parked_at = NULL WHERE parked_at IS NOT NULL")
        .execute(&mut *conn)
        .await?;
    Ok(done.rows_affected())
}

/// Notes every quarantined row again for its replica, unless such a note is
/// already pending; answers how many. When it gets there, it leaves the
/// quarantine.
pub async fn retry_quarantine(conn: &mut Conn) -> Result<u64, Error> {
    let done = sqlx::query(
        "INSERT INTO sync.notes (kind, relation, row_key, scope) \
         SELECT 'row', q.relation, q.row_key, q.replica_id FROM sync.quarantine q \
          WHERE NOT EXISTS (SELECT 1 FROM sync.notes n \
                             WHERE n.done_at IS NULL AND n.kind = 'row' AND n.scope = q.replica_id \
                               AND n.relation = q.relation AND n.row_key = q.row_key)",
    )
    .execute(&mut *conn)
    .await?;
    Ok(done.rows_affected())
}

/// How the engine is doing, for an alarm or a status page.
#[derive(Debug, Default)]
pub struct Health {
    /// Notes waiting to be sent.
    pub pending: i64,
    /// How long the oldest of them has waited.
    pub oldest: Option<std::time::Duration>,
    /// Notes the planner set aside: each one a bug or data no declaration
    /// allows, waiting for a deploy.
    pub parked: Vec<Parked>,
    /// Rows waiting in quarantine, over every replica.
    pub quarantined: i64,
    /// Uploaded rows waiting in the inbox to be filed.
    pub unfiled: i64,
    /// Replicas a send keeps failing to.
    pub failing: Vec<Failing>,
}

/// A parked note.
#[derive(Debug, sqlx::FromRow)]
pub struct Parked {
    pub id: i64,
    pub kind: String,
    pub relation: Option<String>,
    pub row_key: Option<String>,
    pub attempts: i32,
    pub last_error: Option<String>,
}

/// A replica a send keeps failing to.
#[derive(Debug, sqlx::FromRow)]
pub struct Failing {
    pub replica_id: Uuid,
    pub failures: i32,
    pub last_error: Option<String>,
}

/// The engine's health, with up to `limit` parked notes and failing replicas.
pub async fn health(conn: &mut Conn, limit: i64) -> Result<Health, Error> {
    let (pending, oldest): (i64, Option<f64>) = sqlx::query_as(
        "SELECT count(*), extract(epoch FROM now() - min(noted_at))::float8 \
           FROM sync.notes WHERE done_at IS NULL AND parked_at IS NULL",
    )
    .fetch_one(&mut *conn)
    .await?;
    let parked = sqlx::query_as(
        "SELECT id, kind, relation, row_key, attempts, last_error FROM sync.notes \
          WHERE done_at IS NULL AND parked_at IS NOT NULL ORDER BY id LIMIT $1",
    )
    .bind(limit)
    .fetch_all(&mut *conn)
    .await?;
    let quarantined = sqlx::query_scalar("SELECT count(*) FROM sync.quarantine")
        .fetch_one(&mut *conn)
        .await?;
    let unfiled = sqlx::query_scalar("SELECT count(*) FROM sync.inbox")
        .fetch_one(&mut *conn)
        .await?;
    let failing = sqlx::query_as(
        "SELECT replica_id, failures, last_error FROM sync.replicas \
          WHERE failures > 0 ORDER BY failures DESC LIMIT $1",
    )
    .bind(limit)
    .fetch_all(&mut *conn)
    .await?;
    Ok(Health {
        pending,
        oldest: oldest.map(|secs| std::time::Duration::from_secs_f64(secs.max(0.0))),
        parked,
        quarantined,
        unfiled,
        failing,
    })
}

#[cfg(test)]
#[path = "relay_tests.rs"]
mod tests;
