//! One pass of the server's relay, in three steps a host runs in order:
//!
//! 1. [`Relay::prepare`] reads the pending notes and plans them, in a
//!    `REPEATABLE READ` transaction of its own, closed before anything is
//!    sent — after [`Engine::observe`] has looked at the replicas, in one of
//!    the host's lock.
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
//! - a row that does not encode, or that fits no message, is quarantined for
//!   the replicas that were to get it;
//! - a replica a send fails to — the transport cannot reach it, or refuses a
//!   message of it for good — waits out a backoff, and everything it is owed,
//!   rows, deletes and snapshot alike, is noted again for it alone, while
//!   every other replica carries on. Owed more than a pass plans, it is
//!   provisioned again instead.
//!
//! Parked notes and quarantined rows are tried again on a timer and when the
//! relay starts ([`unpark`], [`retry_quarantine`]): a deploy that fixes the
//! cause lets them through, and no one repairs data by hand. A replica that
//! leaves its last messages unconfirmed while nothing new goes to it is
//! provisioned again ([`heal`]): the messages may never have reached it.

use crate::envelope::{self, Body, Down, EndSnapshot, Limits};
use crate::plan::{pending, Delta, Engine, Loaded, Note, Unsent};
use crate::row::Conn;
use crate::wire::Codec;
use crate::Error;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::time::Duration;
use uuid::Uuid;

/// Where messages go: `message` is a [`ToReplica`](envelope::ToReplica),
/// encoded, for `replica`. `id` names the message — `{epoch}-{sequence}`,
/// one of its own for every message the engine sends — for a transport that
/// deduplicates a send it retries itself. A pass sent again after a relay
/// died before recording it is numbered anew, and goes again.
///
/// An error is transient when a retry may get it through (the network, the
/// broker away) and permanent when the transport will never take this message;
/// the engine already keeps every message within [`Limits`]. Either way the
/// replica waits out a backoff and is sent what it is owed again.
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
    /// Notes per pass, and how many a replica left out may be owed alone
    /// before it is provisioned again instead.
    pub batch: i64,
    /// How long one send may take before its replica counts as unreachable.
    pub timeout: Duration,
    /// How long a replica a send failed to waits: the first, doubling with
    /// every failure in a row up to the second.
    pub backoff: (Duration, Duration),
}

/// Notes planned together, and what they owe.
struct Group {
    notes: Vec<Note>,
    delta: Delta,
    loaded: Loaded,
    /// Replicas left out of this pass: waiting out a failure, or failed now.
    deferred: BTreeSet<Uuid>,
    /// Each replica's messages, numbered, in the order they go.
    outgoing: Vec<Outgoing>,
}

/// One replica's messages of a group.
struct Outgoing {
    replica: Uuid,
    /// Each message's number and bytes.
    messages: Vec<(u64, Vec<u8>)>,
    /// Each row sent: its table, its key, and the number of the message that
    /// carries it.
    carried: Vec<(String, String, u64)>,
}

/// A pass planned and numbered, waiting to be published.
pub struct Prepared {
    groups: Vec<Group>,
    parked: Vec<(i64, String)>,
    /// The installation the numbers are from.
    epoch: String,
}

/// A pass published, waiting to be recorded.
pub struct Sent {
    groups: Vec<Group>,
    parked: Vec<(i64, String)>,
    /// Replicas a send failed to, with why.
    failed: BTreeMap<Uuid, String>,
    /// Replicas sent to, with the last number they were sent.
    sent_to: BTreeMap<Uuid, u64>,
    /// Each row sent, by replica, table and key: the number of the last
    /// message that carried it.
    carried: BTreeMap<(Uuid, String, String), u64>,
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
    /// Reads and plans the pending notes, and numbers the messages the plan
    /// sends; `None` when there are none. `conn` must be in a transaction
    /// that may draw from a sequence: a plan that fails is rolled back to a
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
        let epoch: Uuid = sqlx::query_scalar("SELECT epoch FROM sync.epoch")
            .fetch_one(&mut *conn)
            .await?;
        let mut prepared = Prepared {
            groups: Vec::new(),
            parked: Vec::new(),
            epoch: epoch.to_string(),
        };
        for (scope, notes) in by_scope {
            self.plan_isolating(conn, notes, scope, &waiting, &mut prepared)
                .await?;
        }
        self.number(conn, &mut prepared).await?;
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
                outgoing: Vec::new(),
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

    /// Builds every replica's messages — rows parents first, then deletes
    /// children first, then the end of a snapshot — and numbers each with a
    /// draw of its own from the sequence, rising in the order they go. A
    /// sequence does not roll back, so a pass that never records still never
    /// reuses a number.
    async fn number(&self, conn: &mut Conn, prepared: &mut Prepared) -> Result<(), Error> {
        let mut built = Vec::new();
        for (at, group) in prepared.groups.iter().enumerate() {
            for replica in Engine::replicas(&group.delta) {
                let (downs, end) = self.messages(group, replica);
                if !downs.is_empty() || end.is_some() {
                    built.push((at, replica, downs, end));
                }
            }
        }
        let count: usize = built
            .iter()
            .map(|(_, _, downs, end)| downs.len() + usize::from(end.is_some()))
            .sum();
        let mut drawn: Vec<i64> =
            sqlx::query_scalar("SELECT nextval('sync.messages') FROM generate_series(1, $1)")
                .bind(i64::try_from(count).map_err(|_| {
                    Error::Misuse("a pass of more messages than a sequence holds".into())
                })?)
                .fetch_all(&mut *conn)
                .await?;
        drawn.sort_unstable();
        let mut numbers = drawn.into_iter().map(|n| {
            u64::try_from(n).map_err(|_| Error::Misuse(format!("sync.messages drew {n}")))
        });
        let mut next = || {
            numbers.next().unwrap_or_else(|| {
                Err(Error::Misuse(
                    "sync.messages drew fewer numbers than asked".into(),
                ))
            })
        };
        let epoch = &prepared.epoch;
        for (at, replica, downs, end) in built {
            let mut outgoing = Outgoing {
                replica,
                messages: Vec::new(),
                carried: Vec::new(),
            };
            for mut down in downs {
                let sequence = next()?;
                (down.sequence, down.epoch) = (sequence, epoch.clone());
                for table in &down.tables {
                    for key in &table.keys {
                        let key = key.parts.join("|");
                        outgoing.carried.push((table.table.clone(), key, sequence));
                    }
                }
                outgoing
                    .messages
                    .push((sequence, Body::Down(down).encoded()));
            }
            if let Some(mut end) = end {
                let sequence = next()?;
                (end.sequence, end.epoch) = (sequence, epoch.clone());
                outgoing
                    .messages
                    .push((sequence, Body::EndSnapshot(end).encoded()));
            }
            prepared.groups[at].outgoing.push(outgoing);
        }
        Ok(())
    }

    fn messages(&self, group: &Group, replica: Uuid) -> (Vec<Down>, Option<EndSnapshot>) {
        let tables = self.engine.rows(&group.delta, &group.loaded, replica);
        let snapshot = group
            .delta
            .snapshots
            .contains(&replica)
            .then(|| Uuid::now_v7().to_string());
        let downs = envelope::pack(tables, snapshot.as_deref().unwrap_or_default(), self.limits);
        let end = snapshot.map(|snapshot_id| EndSnapshot {
            sequence: 0,
            epoch: String::new(),
            snapshot_id,
            tables: self
                .engine
                .snapshot_tables()
                .into_iter()
                .map(str::to_owned)
                .collect(),
        });
        (downs, end)
    }

    /// Sends each replica its messages, in order. A replica a send fails to
    /// is left out of what is recorded, in this group and every later one of
    /// the pass: a message of a later group must not reach it before what an
    /// earlier one owed it.
    pub async fn publish<T: Transport>(&self, transport: &T, prepared: Prepared) -> Sent {
        let mut sent = Sent {
            groups: Vec::new(),
            parked: prepared.parked,
            failed: BTreeMap::new(),
            sent_to: BTreeMap::new(),
            carried: BTreeMap::new(),
        };
        for mut group in prepared.groups {
            for outgoing in std::mem::take(&mut group.outgoing) {
                let replica = outgoing.replica;
                if !sent.failed.contains_key(&replica) {
                    let (last, failure) = self
                        .send(transport, replica, &prepared.epoch, outgoing.messages)
                        .await;
                    if let Some(last) = last {
                        sent.sent_to.insert(replica, last);
                    }
                    let Some(e) = failure else {
                        for (table, key, sequence) in outgoing.carried {
                            sent.carried.insert((replica, table, key), sequence);
                        }
                        continue;
                    };
                    let why = if e.is_transient() {
                        e.to_string()
                    } else {
                        format!("the transport refuses a message for good: {e}")
                    };
                    sent.failed.insert(replica, why);
                }
                group.deferred.insert(replica);
                group.delta.forget(replica);
            }
            sent.groups.push(group);
        }
        sent
    }

    /// Sends `messages` to `replica` in order, each within the timeout, until
    /// one fails: the number of the last one sent, and the failure.
    async fn send<T: Transport>(
        &self,
        transport: &T,
        replica: Uuid,
        epoch: &str,
        messages: Vec<(u64, Vec<u8>)>,
    ) -> (Option<u64>, Option<Error>) {
        let mut last = None;
        for (sequence, bytes) in messages {
            let id = format!("{epoch}-{sequence}");
            let sent = tokio::time::timeout(self.timeout, transport.send(replica, bytes, &id))
                .await
                .unwrap_or_else(|_| {
                    Err(Error::Transport {
                        reason: format!("no answer within {:?}", self.timeout),
                        transient: true,
                    })
                });
            if let Err(e) = sent {
                return (last, Some(e));
            }
            last = Some(sequence);
        }
        (last, None)
    }

    /// Records the pass, in the caller's transaction: what each replica now
    /// holds, the notes done, the notes parked, the rows quarantined, the
    /// replicas that failed and what they are still owed.
    pub async fn record(&self, conn: &mut Conn, sent: Sent) -> Result<Outcome, Error> {
        // Every replica row this pass writes, locked first and in one order:
        // filing an upload locks its sender's first too, so the two wait for
        // each other rather than deadlock, and a filing never reads a pass
        // half recorded.
        let mut touched: BTreeSet<Uuid> = sent
            .sent_to
            .keys()
            .chain(sent.failed.keys())
            .copied()
            .collect();
        for group in &sent.groups {
            touched.extend(Engine::replicas(&group.delta));
            touched.extend(group.deferred.iter().copied());
            touched.extend(group.loaded.failed.iter().map(|u| u.replica));
            touched.extend(group.notes.iter().filter_map(|n| n.scope));
        }
        sqlx::query(
            "SELECT 1 FROM sync.replicas WHERE replica_id = ANY($1) ORDER BY replica_id FOR UPDATE",
        )
        .bind(touched.into_iter().collect::<Vec<_>>())
        .execute(&mut *conn)
        .await?;
        let mut outcome = Outcome {
            parked: sent.parked,
            failing: sent
                .failed
                .iter()
                .map(|(replica, why)| (*replica, why.clone()))
                .collect(),
            ..Outcome::default()
        };
        for group in &sent.groups {
            self.engine.record(conn, &group.delta).await?;
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
                rescope(conn, &global, *replica, self.batch).await?;
            }
            crate::plan::mark_done(conn, &done).await?;
            outcome.done += done.len();
        }
        let (mut replicas, mut tables, mut keys, mut sequences) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for ((replica, table, key), sequence) in sent.carried {
            replicas.push(replica);
            tables.push(table);
            keys.push(key);
            sequences.push(as_stored(sequence)?);
        }
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
        // A row set aside, carried again: the message that carries it is the
        // one whose application lets it out.
        sqlx::query(
            "UPDATE sync.quarantine q SET carried = u.s \
               FROM unnest($1::uuid[], $2::text[], $3::text[], $4::bigint[]) AS u(r, t, k, s) \
              WHERE q.replica_id = u.r AND q.relation = u.t AND q.row_key = u.k",
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
            .bind(stored(why))
            .execute(&mut *conn)
            .await?;
        }
        // Per replica, the last number it was sent and why a send failed:
        // one may have both, sent part of what it was owed before a failure.
        let replicas: Vec<Uuid> = sent
            .sent_to
            .keys()
            .chain(sent.failed.keys())
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let lasts: Vec<Option<i64>> = replicas
            .iter()
            .map(|r| sent.sent_to.get(r).copied().map(as_stored).transpose())
            .collect::<Result<_, _>>()?;
        let whys: Vec<Option<String>> = replicas
            .iter()
            .map(|r| sent.failed.get(r).map(|why| stored(why)))
            .collect();
        sqlx::query(
            "INSERT INTO sync.replicas AS r \
               (replica_id, last_sent_at, last_sequence, waiting_since, failures, retry_after, last_error) \
             SELECT u.r, CASE WHEN u.s IS NOT NULL THEN now() END, u.s, \
                    CASE WHEN u.s IS NOT NULL THEN now() END, \
                    CASE WHEN u.e IS NOT NULL THEN 1 ELSE 0 END, \
                    CASE WHEN u.e IS NOT NULL THEN now() + make_interval(secs => $4) END, u.e \
               FROM unnest($1::uuid[], $2::bigint[], $3::text[]) AS u(r, s, e) \
             ON CONFLICT (replica_id) DO UPDATE SET \
               last_sent_at = coalesce(EXCLUDED.last_sent_at, r.last_sent_at), \
               last_sequence = greatest(r.last_sequence, EXCLUDED.last_sequence), \
               waiting_since = CASE WHEN EXCLUDED.last_sequence IS NULL THEN r.waiting_since \
                                    ELSE coalesce(r.waiting_since, now()) END, \
               failures = CASE WHEN EXCLUDED.last_error IS NULL THEN 0 ELSE r.failures + 1 END, \
               last_error = EXCLUDED.last_error, \
               retry_after = CASE WHEN EXCLUDED.last_error IS NULL THEN NULL ELSE now() + \
                 make_interval(secs => least($4 * power(2, least(r.failures, 32)), $5)) END",
        )
        .bind(&replicas)
        .bind(&lasts)
        .bind(&whys)
        .bind(self.backoff.0.as_secs_f64())
        .bind(self.backoff.1.as_secs_f64())
        .execute(&mut *conn)
        .await?;
        Ok(outcome)
    }
}

/// A message number as the database keeps it.
fn as_stored(sequence: u64) -> Result<i64, Error> {
    i64::try_from(sequence)
        .map_err(|_| Error::Misuse(format!("message {sequence} past the sequence")))
}

/// Text as the database stores it: `text` takes no NUL, which a replica, a
/// transport or a host may put in a string the engine keeps.
pub(crate) fn stored(text: &str) -> String {
    text.replace('\0', "\u{FFFD}")
}

/// Notes `replica`'s provisioning from scratch, owed to it alone, unless one
/// is pending already: it is sent everything it should hold, and lets go of
/// the rest. Answers whether it was noted now.
pub(crate) async fn resync(conn: &mut Conn, replica: Uuid) -> Result<bool, Error> {
    let noted = sqlx::query(
        "INSERT INTO sync.notes (kind, replica_id, scope) SELECT 'resync', $1, $1 \
          WHERE NOT EXISTS (SELECT 1 FROM sync.notes \
                             WHERE kind = 'resync' AND replica_id = $1 AND done_at IS NULL)",
    )
    .bind(replica)
    .execute(&mut *conn)
    .await?;
    Ok(noted.rows_affected() > 0)
}

/// Notes `ids` again, owed to `replica` alone, as of the transactions that
/// first noted them — unless that would leave it owed more than `bound`
/// notes alone: then it is provisioned again instead, which sends everything
/// those notes would have, and they are done.
async fn rescope(conn: &mut Conn, ids: &[i64], replica: Uuid, bound: i64) -> Result<(), Error> {
    if ids.is_empty() {
        return Ok(());
    }
    let owed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sync.notes WHERE done_at IS NULL AND scope = $1 AND kind <> 'resync'",
    )
    .bind(replica)
    .fetch_one(&mut *conn)
    .await?;
    if owed.saturating_add(i64::try_from(ids.len()).unwrap_or(i64::MAX)) > bound {
        sqlx::query(
            "UPDATE sync.notes SET done_at = now() \
              WHERE done_at IS NULL AND scope = $1 AND kind <> 'resync'",
        )
        .bind(replica)
        .execute(&mut *conn)
        .await?;
        resync(conn, replica).await?;
        return Ok(());
    }
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
/// gets it there. The replica keeps whatever version of it it had.
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
            "INSERT INTO sync.quarantine AS q (replica_id, relation, row_key, cause, reason) \
             VALUES ($1, $2, $3, 'server', $4) \
             ON CONFLICT (replica_id, relation, row_key) DO UPDATE SET cause = EXCLUDED.cause, \
               reason = EXCLUDED.reason, last_at = now(), attempts = q.attempts + 1, carried = NULL",
        )
        .bind(row.replica)
        .bind(row.table)
        .bind(&row.key)
        .bind(stored(&row.reason))
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

/// A row tried again for one replica that the plan no longer sends it —
/// deleted, retired, no longer its — is owed to nobody: it leaves the
/// quarantine, unless it failed again. A group of such notes is planned for
/// that replica alone, so all it sent and failed is that replica's.
async fn settle(conn: &mut Conn, engine: &Engine, group: &Group) -> Result<(), Error> {
    for note in &group.notes {
        let (Some(replica), crate::plan::Change::Row { table, key, .. }) =
            (note.scope, &note.change)
        else {
            continue;
        };
        if group.deferred.contains(&replica) {
            continue;
        }
        let sent = group
            .delta
            .upserted(replica)
            .iter()
            .any(|(i, k)| engine.table(*i) == table && k == key);
        let failed = group
            .loaded
            .failed
            .iter()
            .any(|u| u.table == table && &u.key == key);
        if sent || failed {
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

/// Provisions again every live replica that has left a message unconfirmed
/// with nothing sent to it for `after`, and no send to it failing: a
/// transport that lost the last messages it was sent leaves nothing else to
/// bring them back, since nothing newer goes to it while nothing changes.
/// While it stays silent, the next time waits twice as long as the last, up
/// to 32 times `after`, so a replica that is merely away is not sent
/// everything over and over. Answers how many.
pub async fn heal(conn: &mut Conn, engine: &Engine, after: Duration) -> Result<u64, Error> {
    let healed = sqlx::query(&format!(
        "INSERT INTO sync.notes (kind, replica_id, scope) \
         SELECT 'resync', r.replica_id, r.replica_id FROM sync.replicas r \
           JOIN {} ON s.id = r.replica_id \
          WHERE r.waiting_since IS NOT NULL AND r.failures = 0 \
            AND r.last_sent_at < now() - make_interval(secs => $1 * power(2, least( \
                  (SELECT count(*) FROM sync.notes n WHERE n.kind = 'resync' \
                     AND n.replica_id = r.replica_id AND n.noted_at >= r.waiting_since), 5))) \
            AND NOT EXISTS (SELECT 1 FROM sync.notes n WHERE n.kind = 'resync' \
                             AND n.replica_id = r.replica_id AND n.done_at IS NULL)",
        engine.replicas_sql()
    ))
    .bind(after.as_secs_f64())
    .execute(&mut *conn)
    .await?;
    Ok(healed.rows_affected())
}

#[cfg(test)]
#[path = "relay_tests.rs"]
mod tests;
