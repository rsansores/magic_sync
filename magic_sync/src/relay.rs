//! One pass of the server's relay, in two steps a host runs in order:
//!
//! 1. [`Relay::record`] reads the pending notes, plans them, and — in the
//!    same `REPEATABLE READ` transaction, one snapshot of the database, that
//!    commits only while this process is the relay (on the host's lock) —
//!    records what each replica will hold, marks the notes done, and writes
//!    each replica's messages to its outbox (`sync.outbox`), numbered. It
//!    runs after [`Engine::observe`] has looked at the replicas.
//! 2. [`Relay::publish`] sends each replica its outbox, in order, with no
//!    transaction open: a message leaves the outbox once the transport took
//!    it.
//!
//! What a replica is recorded holding is decided before anything is sent,
//! and every message is written before it goes: a crash, or a lock lost,
//! after the record loses nothing — whoever runs the relay next sends the
//! outbox — and before it, nothing was sent. A message keeps its number
//! however often it goes, so the transport's id for it is stable.
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
//!   message of it for good — keeps the rest of its outbox and waits out a
//!   backoff, while every other replica is sent to. Its messages wait for it
//!   in order; an outbox that outgrows its bound, or a message the transport
//!   refuses again and again, is dropped and the replica provisioned again,
//!   which is an alarm.
//!
//! Parked notes and quarantined rows are tried again on a timer and when the
//! relay starts ([`unpark`], [`retry_quarantine`]): a deploy that fixes the
//! cause lets them through, and no one repairs data by hand. A replica that
//! leaves its last messages unconfirmed while nothing new goes to it is
//! provisioned again ([`heal`]): the messages may never have reached it.

use crate::envelope::{self, Body, EndSnapshot, Limits, Packer};
use crate::plan::{pending, Delta, Engine, Note, Unsent, RECORD_CHUNK};
use crate::wire::Codec;
use crate::Conn;
use crate::Error;
use std::collections::BTreeMap;
use std::future::Future;
use std::time::Duration;
use uuid::Uuid;

/// Where the server's messages go: the host's transport, which provides
/// what [the protocol requires of a transport][contract].
///
/// An error is transient when a retry may get it through (the network, the
/// broker away) and permanent when the transport will never take this message;
/// the engine already keeps every message within [`Limits`]. Either way the
/// message stays first in its replica's outbox, and the replica waits out a
/// backoff before it is tried again.
///
/// [contract]: https://rsansores.github.io/magic_sync/protocol.html#2-what-the-transport-must-provide
pub trait Transport: Send + Sync + 'static {
    /// Sends `message`, one message, encoded, to `replica` — or says why it
    /// could not. `Ok` means the transport took it, and will deliver it.
    ///
    /// `id` names the message, one of its own for every message the engine
    /// sends, and the same every time that message is sent again — after a
    /// failure, or by a relay that took over: what a transport that
    /// deduplicates deduplicates by.
    ///
    /// # Errors
    ///
    /// [`Error::Transport`] (or any other) when the transport did not take
    /// the message, transient or not as above.
    fn send(
        &self,
        replica: Uuid,
        message: Vec<u8>,
        id: &str,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}

/// Rows to send or take back a pass reads at a time.
const PAGE: usize = 20_000;

/// Bytes of rows a pass may hold in the messages it fills before it ends
/// them all early.
const BUFFERED: usize = 64 << 20;

/// How much a replica's outbox may hold before it is provisioned again
/// instead: then what it is owed is cheaper sent whole than in order.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Bound {
    pub(crate) messages: i64,
    pub(crate) bytes: i64,
}

/// Times in a row the transport may refuse a replica's next message for
/// good before the replica is provisioned again — an alarm, never a drop.
const REFUSALS: i32 = 3;

/// The relay's settings.
pub(crate) struct Relay<'a, P> {
    pub(crate) engine: &'a Engine,
    pub(crate) codecs: &'a [Codec<P>],
    pub(crate) limits: Limits,
    /// Notes per pass.
    pub(crate) batch: i64,
    /// How long one send may take before its replica counts as unreachable.
    pub(crate) timeout: Duration,
    /// How long a replica a send failed to waits: the first, doubling with
    /// every failure in a row up to the second.
    pub(crate) backoff: crate::run::Backoff,
    /// How much a replica's outbox may hold.
    pub(crate) bound: Bound,
    /// Replicas sent to at once.
    pub(crate) concurrency: usize,
}

/// What recording a pass did.
#[derive(Debug, Default)]
pub(crate) struct Outcome {
    /// Notes done.
    pub(crate) done: usize,
    /// Notes the planner set aside.
    pub(crate) parked: Vec<(i64, String)>,
    /// Rows quarantined for a replica.
    pub(crate) quarantined: usize,
    /// Messages written to the outboxes.
    pub(crate) written: usize,
    /// Rows they carry to hold, and to let go of.
    pub(crate) upserts: usize,
    pub(crate) deletes: usize,
    /// Replicas whose outbox outgrew its bound: provisioned again.
    pub(crate) collapsed: Vec<Uuid>,
}

/// What publishing did.
#[derive(Debug, Default)]
pub(crate) struct Published {
    /// Messages the transport took.
    pub(crate) sent: usize,
    /// Replicas a send failed to, with why.
    pub(crate) failing: Vec<(Uuid, String)>,
}

/// A message written for a replica, not numbered yet.
struct Ready {
    replica: Uuid,
    body: Body,
}

/// What one pass writes, a chunk at a time: each replica's message being
/// filled, and the messages filled and not yet numbered and written.
struct Writer {
    epoch: Uuid,
    /// What seals the epoch's messages.
    secret: Vec<u8>,
    packers: BTreeMap<Uuid, Packer>,
    /// The snapshot each replica provisioned is sent.
    snapshots: BTreeMap<Uuid, String>,
    ready: Vec<Ready>,
    /// Rows the ready messages carry.
    rows: usize,
    /// Bytes of rows in the messages being filled.
    buffered: usize,
    /// Per replica, the last message written, its sequence and nonce: what
    /// the next one follows. A replica provisioned is absent until its
    /// snapshot's first message, which follows nothing.
    last: BTreeMap<Uuid, (i64, i64)>,
    /// Per replica provisioned, its snapshot's first message, and its last.
    began: BTreeMap<Uuid, i64>,
    ended: BTreeMap<Uuid, i64>,
    written: usize,
    upserts: usize,
    deletes: usize,
}

impl<P: Sync + 'static> Relay<'_, P> {
    /// Plans the pending notes and records the plan, in `conn`'s
    /// transaction — `REPEATABLE READ`, on the relay's lock: what each
    /// replica holds once its messages arrive, the notes done or parked, the
    /// rows quarantined, and every replica's messages written to its outbox,
    /// numbered, in the order they go. Nothing is sent. What it writes,
    /// nothing but a pass writes under it, but a filing that refuses a row
    /// it resends: then it fails to serialize, which is transient.
    pub(crate) async fn record(&self, conn: &mut Conn) -> Result<Outcome, Error> {
        let notes = pending(conn, self.batch).await?;
        if notes.is_empty() {
            return Ok(Outcome::default());
        }
        let (mut delta, notes, parked) = self.plan(conn, notes).await?;
        let mut outcome = Outcome {
            parked,
            ..Outcome::default()
        };
        // Shared: a filing that finds this database restored takes a new
        // epoch only once this pass is done with the old one, or makes it
        // fail to serialize.
        let (epoch, secret): (Uuid, Vec<u8>) =
            sqlx::query_as("SELECT epoch, secret FROM sync.epoch FOR SHARE")
                .fetch_one(&mut *conn)
                .await?;
        // A message follows the last one its replica was sent in this epoch;
        // a replica sent none since it was last provisioned can only begin
        // with a snapshot.
        let touched = self.engine.touched(conn).await?;
        let chains: BTreeMap<Uuid, (i64, i64)> = sqlx::query_as::<_, (Uuid, i64, i64)>(
            "SELECT replica_id, last_sequence, last_nonce FROM sync.chains \
              WHERE replica_id = ANY($1) AND last_sequence IS NOT NULL",
        )
        .bind(touched.iter().copied().collect::<Vec<_>>())
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(|(replica, sequence, nonce)| (replica, (sequence, nonce)))
        .collect();
        let unchained: Vec<Uuid> = touched
            .into_iter()
            .filter(|r| !delta.snapshots.contains(r) && !chains.contains_key(r))
            .collect();
        if !unchained.is_empty() {
            delta = self.engine.provision(conn, delta, &unchained).await?;
        }
        // A provisioning stands for every other one pending for its replica.
        let snapshots: Vec<Uuid> = delta.snapshots.iter().copied().collect();
        let covered: Vec<i64> = sqlx::query_scalar(
            "SELECT id FROM sync.notes WHERE kind = 'resync' AND done_at IS NULL \
               AND parked_at IS NULL AND replica_id = ANY($1)",
        )
        .bind(&snapshots)
        .fetch_all(&mut *conn)
        .await?;
        let written = loop {
            sqlx::query("SAVEPOINT sync_write")
                .execute(&mut *conn)
                .await?;
            let written = self.write(conn, &delta, (epoch, &secret), &chains).await?;
            let end = match written {
                Ok(_) => "RELEASE SAVEPOINT sync_write",
                Err(_) => "ROLLBACK TO SAVEPOINT sync_write",
            };
            sqlx::query(end).execute(&mut *conn).await?;
            match written {
                Ok(written) => break written,
                Err(blocked) => delta = self.engine.replan(conn, delta, blocked).await?,
            }
        };
        (outcome.written, outcome.upserts, outcome.deletes) = written;
        let unsent = self.engine.unsent(&delta);
        outcome.quarantined = unsent.len();
        quarantine(conn, &unsent).await?;
        settle(conn).await?;
        let replicas: Vec<Uuid> = self.engine.touched(conn).await?.into_iter().collect();
        let done: Vec<i64> = notes.iter().map(|n| n.id).chain(covered).collect();
        crate::plan::mark_done(conn, &done).await?;
        outcome.done = notes.len();
        let (ids, whys): (Vec<i64>, Vec<String>) = outcome
            .parked
            .iter()
            .map(|(id, why)| (*id, stored(why)))
            .unzip();
        sqlx::query(
            "UPDATE sync.notes n SET parked_at = now(), attempts = attempts + 1, last_error = p.why \
               FROM unnest($1::bigint[], $2::text[]) AS p(id, why) WHERE n.id = p.id",
        )
        .bind(&ids)
        .bind(&whys)
        .execute(&mut *conn)
        .await?;
        outcome.collapsed = self.bound(conn, &replicas).await?;
        Ok(outcome)
    }

    /// Plans `notes` together. When that fails for good, the note it fails
    /// on is found by halving them, parked, and the rest planned together
    /// again: one plan, so no part of it undoes another. Answers the plan,
    /// the notes it is of, and the notes parked with why.
    async fn plan(
        &self,
        conn: &mut Conn,
        mut notes: Vec<Note>,
    ) -> Result<(Delta, Vec<Note>, Vec<(i64, String)>), Error> {
        let mut parked = Vec::new();
        loop {
            if notes.is_empty() {
                return Ok((self.engine.plan(conn, &notes).await?, notes, parked));
            }
            let why = match self.attempt(conn, &notes).await {
                Ok(delta) => return Ok((delta, notes, parked)),
                Err(e) if e.is_transient() => return Err(e),
                Err(e) => e,
            };
            let (culprit, why) = self.culprit(conn, &notes, why).await?;
            notes.retain(|n| n.id != culprit);
            parked.push((culprit, why));
        }
    }

    /// The note `notes` fail on with `why`, halving them: the half that fails
    /// alone holds it. Two halves that fail only together give up their last.
    async fn culprit(
        &self,
        conn: &mut Conn,
        notes: &[Note],
        mut why: Error,
    ) -> Result<(i64, String), Error> {
        let mut suspects = notes.to_vec();
        while suspects.len() > 1 {
            let right = suspects.split_off(suspects.len() / 2);
            match self.attempt(conn, &suspects).await {
                Err(e) if e.is_transient() => return Err(e),
                Err(e) => {
                    why = e;
                    continue;
                }
                Ok(_) => {}
            }
            match self.attempt(conn, &right).await {
                Err(e) if e.is_transient() => return Err(e),
                Err(e) => {
                    why = e;
                    suspects = right;
                }
                Ok(_) => suspects = right.into_iter().last().into_iter().collect(),
            }
        }
        Ok((suspects[0].id, why.to_string()))
    }

    /// One plan of `notes`, in a savepoint: a plan that fails is rolled
    /// back, so the next can run.
    async fn attempt(&self, conn: &mut Conn, notes: &[Note]) -> Result<Delta, Error> {
        sqlx::query("SAVEPOINT sync_plan")
            .execute(&mut *conn)
            .await?;
        let planned = self.engine.plan(conn, notes).await;
        let end = if planned.is_ok() {
            "RELEASE SAVEPOINT sync_plan"
        } else {
            "ROLLBACK TO SAVEPOINT sync_plan"
        };
        sqlx::query(end).execute(&mut *conn).await?;
        planned
    }

    /// Writes what `delta` sends: each replica's rows, loaded a chunk at a
    /// time, made into messages — rows parents first, then
    /// deletes children first, then the end of a snapshot — numbered with a
    /// draw of their own from the sequence, rising in the order they go,
    /// and written to the replica's outbox with what they carry recorded as
    /// held. Answers how many messages, or the rows that cannot go: then
    /// nothing written counts, and the caller plans around them.
    async fn write(
        &self,
        conn: &mut Conn,
        delta: &Delta,
        (epoch, secret): (Uuid, &[u8]),
        chains: &BTreeMap<Uuid, (i64, i64)>,
    ) -> Result<Result<(usize, usize, usize), BTreeMap<(Uuid, usize, String), String>>, Error> {
        let snapshots: Vec<Uuid> = delta.snapshots.iter().copied().collect();
        if !snapshots.is_empty() {
            // What a snapshot replaces: what it held, and what was on its way.
            for sql in [
                "DELETE FROM sync.held WHERE replica_id = ANY($1)",
                "DELETE FROM sync.gone WHERE replica_id = ANY($1)",
                "DELETE FROM sync.outbox WHERE replica_id = ANY($1)",
            ] {
                sqlx::query(sql)
                    .bind(&snapshots)
                    .execute(&mut *conn)
                    .await?;
            }
        }
        let ids: BTreeMap<Uuid, String> = snapshots
            .iter()
            .map(|replica| (*replica, Uuid::now_v7().to_string()))
            .collect();
        let mut writer = Writer {
            epoch,
            secret: secret.to_vec(),
            packers: self
                .engine
                .touched(conn)
                .await?
                .into_iter()
                .map(|replica| {
                    let snapshot = ids.get(&replica).map_or("", String::as_str);
                    (replica, Packer::new(snapshot, self.limits))
                })
                .collect(),
            snapshots: ids.clone(),
            ready: Vec::new(),
            rows: 0,
            buffered: 0,
            last: chains
                .iter()
                .filter(|(r, _)| !delta.snapshots.contains(r))
                .map(|(r, chain)| (*r, *chain))
                .collect(),
            began: BTreeMap::new(),
            ended: BTreeMap::new(),
            written: 0,
            upserts: 0,
            deletes: 0,
        };
        let mut blocked = BTreeMap::new();
        // Every row sent, by table and key: each key encoded once, a chunk
        // of keys at a time, however many replicas it goes to.
        self.engine.open_sent(conn).await?;
        loop {
            let page = self.engine.next(conn, "sync_sent", PAGE).await?;
            if page.is_empty() {
                break;
            }
            let mut rest = page.as_slice();
            while let Some((i, _, _)) = rest.first() {
                let mut keys: Vec<String> = Vec::new();
                let end = rest
                    .iter()
                    .position(|(t, key, _)| {
                        if t != i {
                            return true;
                        }
                        if keys.last() != Some(key) {
                            if keys.len() == RECORD_CHUNK {
                                return true;
                            }
                            keys.push(key.clone());
                        }
                        false
                    })
                    .unwrap_or(rest.len());
                let (chunk, after) = rest.split_at(end);
                rest = after;
                let encoded = self
                    .engine
                    .encode(conn, self.codecs, *i, &keys, self.limits.largest_row())
                    .await?;
                for (i, key, replica) in chunk {
                    if let Some(why) = encoded.failed.get(key) {
                        blocked.insert((*replica, *i, key.clone()), why.clone());
                    } else if blocked.is_empty() {
                        let table = self.engine.table(*i);
                        writer.push(*replica, table, Some(encoded.rows[key].clone()), key);
                    }
                }
                if blocked.is_empty() {
                    writer.flush(conn, false).await?;
                }
            }
        }
        if !blocked.is_empty() {
            return Ok(Err(blocked));
        }
        self.engine.open_dropped(conn).await?;
        loop {
            let page = self.engine.next(conn, "sync_dropped", PAGE).await?;
            if page.is_empty() {
                break;
            }
            for (i, key, replica) in &page {
                writer.push(*replica, self.engine.table(*i), None, key);
            }
            writer.flush(conn, false).await?;
        }
        writer.end(&self.engine.snapshot_tables());
        writer.flush(conn, true).await?;
        let replicas: Vec<Uuid> = writer.last.keys().copied().collect();
        let (lasts, nonces): (Vec<i64>, Vec<i64>) = writer.last.values().copied().unzip();
        let of = |map: &BTreeMap<Uuid, i64>| -> Vec<Option<i64>> {
            replicas.iter().map(|r| map.get(r).copied()).collect()
        };
        sqlx::query(
            "INSERT INTO sync.chains AS c \
               (replica_id, last_sequence, last_nonce, snapshot_sequence, snapshot_end) \
             SELECT * FROM unnest($1::uuid[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[]) \
             ON CONFLICT (replica_id) DO UPDATE SET last_sequence = EXCLUDED.last_sequence, \
               last_nonce = EXCLUDED.last_nonce, \
               snapshot_sequence = coalesce(EXCLUDED.snapshot_sequence, c.snapshot_sequence), \
               snapshot_end = coalesce(EXCLUDED.snapshot_end, c.snapshot_end)",
        )
        .bind(&replicas)
        .bind(&lasts)
        .bind(&nonces)
        .bind(of(&writer.began))
        .bind(of(&writer.ended))
        .execute(&mut *conn)
        .await?;
        Ok(Ok((writer.written, writer.upserts, writer.deletes)))
    }

    /// Provisions again, with an alarm, each of `replicas` whose outbox
    /// outgrew its bound: the changes queued behind a snapshot, if it begins
    /// with one, which another would only replace. Answers them.
    async fn bound(&self, conn: &mut Conn, replicas: &[Uuid]) -> Result<Vec<Uuid>, Error> {
        let over: Vec<Uuid> = sqlx::query_scalar(
            "SELECT replica_id FROM sync.outbox \
              WHERE replica_id = ANY($1) AND NOT snapshot GROUP BY replica_id \
             HAVING count(*) > $2 OR sum(length(message)) > $3",
        )
        .bind(replicas)
        .bind(self.bound.messages)
        .bind(self.bound.bytes)
        .fetch_all(&mut *conn)
        .await?;
        for replica in &over {
            collapse(
                conn,
                *replica,
                "its outbox outgrew its bound while it could not be reached: provisioned again",
            )
            .await?;
        }
        Ok(over)
    }

    /// Sends every replica its outbox, in order, each message within the
    /// timeout, on `conn` — the relay's lock's — with no transaction open:
    /// each message leaves the outbox once the transport took it. Up to
    /// `concurrency` replicas are sent to at once, each its messages one
    /// after the other, so one slow replica holds back no other. A replica a
    /// send fails to keeps the rest, and waits out its backoff before it is
    /// sent to again; one that is waiting is not sent to.
    pub(crate) async fn publish<T: Transport>(
        &self,
        conn: &mut Conn,
        transport: &T,
    ) -> Result<Published, Error> {
        let epoch: Uuid = sqlx::query_scalar("SELECT epoch FROM sync.epoch")
            .fetch_one(&mut *conn)
            .await?;
        let due: Vec<Uuid> = sqlx::query_scalar(
            "SELECT DISTINCT o.replica_id FROM sync.outbox o \
               LEFT JOIN sync.replicas r USING (replica_id) \
              WHERE r.retry_after IS NULL OR r.retry_after <= now() ORDER BY 1",
        )
        .fetch_all(&mut *conn)
        .await?;
        let conn = tokio::sync::Mutex::new(conn);
        let jobs = due.into_iter().map(|replica| {
            let job: Job<'_, _> = Box::pin(self.publish_to(&conn, transport, replica, epoch));
            job
        });
        let mut published = Published::default();
        for done in at_most(self.concurrency, jobs).await {
            let (sent, failing) = done?;
            published.sent += sent;
            published.failing.extend(failing);
        }
        Ok(published)
    }

    /// Sends `replica` its outbox and records how that went: how many
    /// messages went, and why a send failed, if one did.
    async fn publish_to<T: Transport>(
        &self,
        conn: &tokio::sync::Mutex<&mut Conn>,
        transport: &T,
        replica: Uuid,
        epoch: Uuid,
    ) -> Result<(usize, Option<(Uuid, String)>), Error> {
        let (sent, failure) = self.send(conn, transport, replica, epoch).await?;
        let mut conn = conn.lock().await;
        let conn: &mut Conn = &mut conn;
        let Some(e) = failure else {
            sqlx::query(
                "INSERT INTO sync.replicas AS r (replica_id, last_sent_at) VALUES ($1, now()) \
                 ON CONFLICT (replica_id) DO UPDATE SET last_sent_at = now(), \
                   failures = 0, retry_after = NULL, last_error = NULL, refusals = 0",
            )
            .bind(replica)
            .execute(&mut *conn)
            .await?;
            return Ok((sent, None));
        };
        let permanent = !e.is_transient();
        let why = if permanent {
            format!("the transport refuses a message for good: {e}")
        } else {
            e.to_string()
        };
        sqlx::query(
            "INSERT INTO sync.replicas (replica_id) VALUES ($1) \
             ON CONFLICT (replica_id) DO NOTHING",
        )
        .bind(replica)
        .execute(&mut *conn)
        .await?;
        let refusals: i32 = sqlx::query_scalar(
            "UPDATE sync.replicas SET \
               last_sent_at = CASE WHEN $2 THEN now() ELSE last_sent_at END, \
               failures = failures + 1, last_error = $3, \
               refusals = CASE WHEN $4 THEN refusals + 1 ELSE refusals END, \
               retry_after = now() + make_interval(secs => \
                 least($5 * power(2, least(failures, 32)), $6)) \
             WHERE replica_id = $1 RETURNING refusals",
        )
        .bind(replica)
        .bind(sent > 0)
        .bind(stored(&why))
        .bind(permanent)
        .bind(self.backoff.first.as_secs_f64())
        .bind(self.backoff.max.as_secs_f64())
        .fetch_one(&mut *conn)
        .await?;
        if refusals >= REFUSALS {
            let mut tx = sqlx::Connection::begin(&mut *conn).await?;
            sqlx::query("UPDATE sync.replicas SET refusals = 0 WHERE replica_id = $1")
                .bind(replica)
                .execute(&mut *tx)
                .await?;
            collapse(
                &mut tx,
                replica,
                &format!("the transport refused its next message for good {refusals} times: provisioned again — {why}"),
            )
            .await?;
            tx.commit().await?;
        }
        Ok((sent, Some((replica, why))))
    }

    /// Sends `replica` its outbox in order until a send fails: how many
    /// went, and the failure. The connection is taken only to read and
    /// trim the outbox, never across a send.
    async fn send<T: Transport>(
        &self,
        conn: &tokio::sync::Mutex<&mut Conn>,
        transport: &T,
        replica: Uuid,
        epoch: Uuid,
    ) -> Result<(usize, Option<Error>), Error> {
        let mut sent = 0;
        let mut after = 0_i64;
        loop {
            let page: Vec<(i64, Vec<u8>)> = sqlx::query_as(
                "SELECT sequence, message FROM sync.outbox \
                  WHERE replica_id = $1 AND sequence > $2 ORDER BY sequence LIMIT 64",
            )
            .bind(replica)
            .bind(after)
            .fetch_all(&mut **conn.lock().await)
            .await?;
            if page.is_empty() {
                return Ok((sent, None));
            }
            let mut failure = None;
            let start = after;
            for (sequence, message) in page {
                let id = format!("{epoch}-{sequence}");
                let went =
                    tokio::time::timeout(self.timeout, transport.send(replica, message, &id))
                        .await
                        .unwrap_or_else(|_| {
                            Err(Error::Transport {
                                reason: format!("no answer within {:?}", self.timeout),
                                transient: true,
                            })
                        });
                if let Err(e) = went {
                    failure = Some(e);
                    break;
                }
                after = sequence;
                sent += 1;
            }
            if after > start {
                sqlx::query("DELETE FROM sync.outbox WHERE replica_id = $1 AND sequence <= $2")
                    .bind(replica)
                    .bind(after)
                    .execute(&mut **conn.lock().await)
                    .await?;
            }
            if failure.is_some() {
                return Ok((sent, failure));
            }
        }
    }
}

/// A replica's publishing, boxed so many run side by side.
type Job<'a, T> = std::pin::Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Runs `jobs`, at most `limit` at once, and answers what each ended with.
async fn at_most<'a, T>(limit: usize, jobs: impl IntoIterator<Item = Job<'a, T>>) -> Vec<T> {
    let mut jobs = jobs.into_iter();
    let mut running: Vec<Job<'a, T>> = Vec::new();
    let mut out = Vec::new();
    loop {
        running.extend(jobs.by_ref().take(limit.max(1) - running.len()));
        if running.is_empty() {
            return out;
        }
        std::future::poll_fn(|cx| {
            let before = out.len();
            running.retain_mut(|job| match job.as_mut().poll(cx) {
                std::task::Poll::Ready(done) => {
                    out.push(done);
                    false
                }
                std::task::Poll::Pending => true,
            });
            if out.len() > before {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        })
        .await;
    }
}

impl Writer {
    /// Adds `key` of `table` to `replica`'s messages: the row to hold as
    /// `bytes`, or `None` to let go of.
    fn push(&mut self, replica: Uuid, table: &str, bytes: Option<Vec<u8>>, key: &str) {
        let wire = envelope::Key {
            parts: key.split('|').map(str::to_owned).collect(),
        };
        self.buffered += bytes.as_ref().map_or(0, Vec::len);
        let packer = self
            .packers
            .get_mut(&replica)
            .expect("a packer per replica");
        if let Some(done) = packer.push(table, bytes, wire) {
            self.ready(replica, Body::Down(done));
        }
        if self.buffered > BUFFERED {
            // Every replica's message being filled, ended early: what a
            // pass holds stays bounded however many replicas it fills at once.
            let replicas: Vec<Uuid> = self.packers.keys().copied().collect();
            for replica in replicas {
                if let Some(done) = self.packers.get_mut(&replica).and_then(Packer::finish) {
                    self.ready(replica, Body::Down(done));
                }
            }
        }
    }

    fn ready(&mut self, replica: Uuid, body: Body) {
        if let Body::Down(down) = &body {
            self.rows += down
                .tables
                .iter()
                .map(|t| t.upserts.len() + t.deletes.len())
                .sum::<usize>();
            let carried: usize = down
                .tables
                .iter()
                .flat_map(|t| &t.upserts)
                .map(Vec::len)
                .sum();
            self.buffered = self.buffered.saturating_sub(carried);
        }
        self.ready.push(Ready { replica, body });
    }

    /// Ends every replica's messages: the one being filled, then the end of
    /// its snapshot, naming `tables`, when it is provisioned.
    fn end(&mut self, tables: &[&'static str]) {
        let replicas: Vec<Uuid> = self.packers.keys().copied().collect();
        for replica in replicas {
            let packer = self.packers.get_mut(&replica).expect("its packer");
            if let Some(done) = packer.finish() {
                self.ready(replica, Body::Down(done));
            }
            if let Some(snapshot_id) = self.snapshots.get(&replica).cloned() {
                self.ready(
                    replica,
                    Body::EndSnapshot(EndSnapshot {
                        snapshot_id,
                        tables: tables.iter().map(|t| (*t).to_owned()).collect(),
                        ..EndSnapshot::default()
                    }),
                );
            }
        }
    }

    /// Numbers the ready messages, rising in the order they were made, and
    /// writes each to its replica's outbox with the rows it carries recorded
    /// as held at that message — when they carry a chunk's worth, or `all`.
    async fn flush(&mut self, conn: &mut Conn, all: bool) -> Result<(), Error> {
        if self.ready.is_empty() || (!all && self.rows < RECORD_CHUNK) {
            return Ok(());
        }
        let ready = std::mem::take(&mut self.ready);
        self.rows = 0;
        let count = i64::try_from(ready.len())
            .map_err(|_| Error::Misuse("more messages than a sequence holds".into()))?;
        let mut drawn: Vec<i64> =
            sqlx::query_scalar("SELECT nextval('sync.messages') FROM generate_series(1, $1)")
                .bind(count)
                .fetch_all(&mut *conn)
                .await?;
        drawn.sort_unstable();
        let mut outbox = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let mut held = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let mut gone = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for (Ready { replica, mut body }, sequence) in ready.into_iter().zip(drawn) {
            let number = u64::try_from(sequence)
                .map_err(|_| Error::Misuse(format!("sync.messages drew {sequence}")))?;
            let (previous, previous_nonce) = self.last.get(&replica).map_or((0, 0), |(s, n)| {
                (
                    u64::try_from(*s).unwrap_or_default(),
                    u64::try_from(*n).unwrap_or_default(),
                )
            });
            let nonce = envelope::nonce();
            Place {
                sequence: number,
                epoch: self.epoch.to_string(),
                nonce,
                previous: (previous, previous_nonce),
                mac: envelope::seal(&self.secret, replica, self.epoch, number, nonce),
            }
            .stamp(&mut body);
            match &body {
                Body::Down(down) => {
                    for table in &down.tables {
                        for key in &table.keys {
                            held.0.push(replica);
                            held.1.push(table.table.clone());
                            held.2.push(key.parts.join("|"));
                            held.3.push(sequence);
                        }
                        for key in &table.deletes {
                            gone.0.push(replica);
                            gone.1.push(table.table.clone());
                            gone.2.push(key.parts.join("|"));
                            gone.3.push(sequence);
                        }
                    }
                    if !down.snapshot_id.is_empty() {
                        self.began.entry(replica).or_insert(sequence);
                    }
                }
                Body::EndSnapshot(_) => {
                    self.began.entry(replica).or_insert(sequence);
                    self.ended.insert(replica, sequence);
                }
                Body::Filed(_) => {}
            }
            let nonce = i64::try_from(nonce).expect("a nonce is below 2^63");
            self.last.insert(replica, (sequence, nonce));
            self.written += 1;
            outbox.0.push(replica);
            outbox.1.push(sequence);
            outbox.3.push(match &body {
                Body::Down(down) => !down.snapshot_id.is_empty(),
                _ => true,
            });
            outbox.2.push(body.encoded());
        }
        self.upserts += held.0.len();
        self.deletes += gone.0.len();
        sqlx::query(
            "INSERT INTO sync.outbox (replica_id, sequence, message, snapshot) \
             SELECT * FROM unnest($1::uuid[], $2::bigint[], $3::bytea[], $4::boolean[])",
        )
        .bind(&outbox.0)
        .bind(&outbox.1)
        .bind(&outbox.2)
        .bind(&outbox.3)
        .execute(&mut *conn)
        .await?;
        // Each version recorded with the newest note of its row the pass
        // sees, looked up once per row however many replicas it goes to.
        sqlx::query(
            "WITH u AS (SELECT * FROM unnest($1::uuid[], $2::text[], $3::text[], $4::bigint[]) \
                            AS u(r, t, k, s)), \
                  n AS (SELECT t, k, (SELECT max(id) FROM sync.notes n \
                                       WHERE n.kind = 'row' AND n.relation = r.t AND n.row_key = r.k) AS noted \
                          FROM (SELECT DISTINCT t, k FROM u) AS r) \
             INSERT INTO sync.held (replica_id, relation, row_key, noted, sequence) \
             SELECT u.r, u.t, u.k, coalesce(n.noted, 0), u.s FROM u JOIN n USING (t, k) \
             ON CONFLICT (replica_id, relation, row_key) DO UPDATE SET \
               sent_at = now(), noted = EXCLUDED.noted, sequence = EXCLUDED.sequence",
        )
        .bind(&held.0)
        .bind(&held.1)
        .bind(&held.2)
        .bind(&held.3)
        .execute(&mut *conn)
        .await?;
        // A row set aside, carried again — or let go of again, after the
        // replica refused to — leaves once the message is applied.
        for carried in [&held, &gone] {
            sqlx::query(
                "UPDATE sync.quarantine q SET carried = u.s \
                   FROM unnest($1::uuid[], $2::text[], $3::text[], $4::bigint[]) AS u(r, t, k, s) \
                  WHERE q.replica_id = u.r AND q.relation = u.t AND q.row_key = u.k",
            )
            .bind(&carried.0)
            .bind(&carried.1)
            .bind(&carried.2)
            .bind(&carried.3)
            .execute(&mut *conn)
            .await?;
        }
        sqlx::query(
            "DELETE FROM sync.held h \
              USING unnest($1::uuid[], $2::text[], $3::text[]) AS d(r, t, k) \
              WHERE h.replica_id = d.r AND h.relation = d.t AND h.row_key = d.k",
        )
        .bind(&gone.0)
        .bind(&gone.1)
        .bind(&gone.2)
        .execute(&mut *conn)
        .await?;
        sqlx::query(
            "INSERT INTO sync.gone (replica_id, relation, row_key, sequence) \
             SELECT * FROM unnest($1::uuid[], $2::text[], $3::text[], $4::bigint[]) \
             ON CONFLICT (replica_id, relation, row_key) DO UPDATE SET sequence = EXCLUDED.sequence",
        )
        .bind(&gone.0)
        .bind(&gone.1)
        .bind(&gone.2)
        .bind(&gone.3)
        .execute(&mut *conn)
        .await?;
        Ok(())
    }
}

/// Where a message stands in its replica's chain, and the seal on it.
struct Place {
    sequence: u64,
    epoch: String,
    nonce: u64,
    previous: (u64, u64),
    mac: Vec<u8>,
}

impl Place {
    /// Writes itself into `body`'s fields, a `Down` or an `EndSnapshot`.
    fn stamp(self, body: &mut Body) {
        let Place {
            sequence,
            epoch,
            nonce,
            previous: (previous, previous_nonce),
            mac,
        } = self;
        match body {
            Body::Down(m) => {
                (m.sequence, m.epoch, m.nonce, m.mac) = (sequence, epoch, nonce, mac);
                (m.previous, m.previous_nonce) = (previous, previous_nonce);
            }
            Body::EndSnapshot(m) => {
                (m.sequence, m.epoch, m.nonce, m.mac) = (sequence, epoch, nonce, mac);
                (m.previous, m.previous_nonce) = (previous, previous_nonce);
            }
            Body::Filed(_) => {}
        }
    }
}

/// Drops `replica`'s outbox and forgets what it holds, and provisions it
/// again from scratch — with an alarm, for `why`: what it was owed is sent
/// whole instead.
async fn collapse(conn: &mut Conn, replica: Uuid, why: &str) -> Result<(), Error> {
    tracing::error!(%replica, why, "sync ALARM: a replica's messages dropped — it is provisioned again");
    for sql in [
        "DELETE FROM sync.outbox WHERE replica_id = $1",
        "DELETE FROM sync.held WHERE replica_id = $1",
        "DELETE FROM sync.gone WHERE replica_id = $1",
    ] {
        sqlx::query(sql).bind(replica).execute(&mut *conn).await?;
    }
    sqlx::query(
        "INSERT INTO sync.chains (replica_id, dropped, dropped_at) VALUES ($1, $2, now()) \
         ON CONFLICT (replica_id) DO UPDATE SET last_sequence = NULL, last_nonce = NULL, \
           snapshot_sequence = NULL, snapshot_end = NULL, dropped = EXCLUDED.dropped, \
           dropped_at = now()",
    )
    .bind(replica)
    .bind(stored(why))
    .execute(&mut *conn)
    .await?;
    resync(conn, replica).await?;
    Ok(())
}

/// A row tried again that the plan no longer sends a replica — deleted,
/// retired, no longer its — is owed to it by nobody: it leaves that
/// replica's quarantine, unless it failed again.
async fn settle(conn: &mut Conn) -> Result<(), Error> {
    sqlx::query(
        "DELETE FROM sync.quarantine q USING sync_changed c \
          WHERE q.relation = c.relation AND q.row_key = c.row_key \
            AND NOT EXISTS (SELECT 1 FROM sync_kept k WHERE k.replica_id = q.replica_id \
                               AND k.rel = c.rel AND k.row_key = c.row_key AND k.sends) \
            AND NOT EXISTS (SELECT 1 FROM sync_blocked b WHERE b.replica_id = q.replica_id \
                               AND b.rel = c.rel AND b.row_key = c.row_key)",
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Text as the database stores it: `text` takes no NUL, which a replica, a
/// transport or a host may put in a string the engine keeps.
pub(crate) fn stored(text: &str) -> String {
    text.replace('\0', "\u{FFFD}")
}

/// Notes `replica`'s provisioning from scratch, unless one is pending
/// already: it is sent everything it should hold, and lets go of the rest.
/// Answers whether it was noted now.
pub(crate) async fn resync(conn: &mut Conn, replica: Uuid) -> Result<bool, Error> {
    let noted = sqlx::query(&crate::plan::resync_sql(
        "SELECT $1::uuid, (SELECT partition_id FROM sync.replicas WHERE replica_id = $1)",
    ))
    .bind(replica)
    .execute(&mut *conn)
    .await?;
    Ok(noted.rows_affected() > 0)
}

/// Sets `unsent` aside: each row stays owed to its replica until a retry
/// gets it there. The replica keeps whatever version of it it had.
async fn quarantine(conn: &mut Conn, unsent: &[Unsent]) -> Result<(), Error> {
    let Some(first) = unsent.first() else {
        return Ok(());
    };
    tracing::error!(
        rows = unsent.len(),
        replica = %first.replica,
        table = first.table,
        key = first.key,
        reason = first.reason,
        "sync: rows quarantined for replicas, the first of them named — fix the cause and they go on the next retry"
    );
    let mut rows = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for row in unsent {
        rows.0.push(row.replica);
        rows.1.push(row.table);
        rows.2.push(row.key.as_str());
        rows.3.push(stored(&row.reason));
    }
    sqlx::query(
        "INSERT INTO sync.quarantine AS q (replica_id, relation, row_key, cause, reason) \
         SELECT r, t, k, 'server', w FROM unnest($1::uuid[], $2::text[], $3::text[], $4::text[]) AS u(r, t, k, w) \
         ON CONFLICT (replica_id, relation, row_key) DO UPDATE SET cause = EXCLUDED.cause, \
           reason = EXCLUDED.reason, last_at = now(), attempts = q.attempts + 1, carried = NULL",
    )
    .bind(&rows.0)
    .bind(&rows.1)
    .bind(&rows.2)
    .bind(&rows.3)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Lets the parked notes be planned again; answers how many. For when the
/// relay starts — after a deploy, which may have fixed what they failed on —
/// and on a timer.
pub(crate) async fn unpark(conn: &mut Conn) -> Result<u64, Error> {
    let done = sqlx::query("UPDATE sync.notes SET parked_at = NULL WHERE parked_at IS NOT NULL")
        .execute(&mut *conn)
        .await?;
    Ok(done.rows_affected())
}

/// Notes every quarantined row again, unless such a note is pending; answers
/// how many. A retry, no change: every replica holding a version of the row
/// holds it as it is, so it goes only where it is missing — where it was set
/// aside. When it gets there, it leaves the quarantine.
pub(crate) async fn retry_quarantine(conn: &mut Conn) -> Result<u64, Error> {
    let done = sqlx::query(
        "INSERT INTO sync.notes (kind, relation, row_key) \
         SELECT DISTINCT 'retry', q.relation, q.row_key FROM sync.quarantine q \
          WHERE NOT EXISTS (SELECT 1 FROM sync.notes n \
                             WHERE n.done_at IS NULL AND n.kind = 'retry' \
                               AND n.relation = q.relation AND n.row_key = q.row_key)",
    )
    .execute(&mut *conn)
    .await?;
    Ok(done.rows_affected())
}

/// Whether a replica of `sync.replicas` as `r`, chained as `sync.chains`
/// as `c`, has a message it has not said it applied: one written for it in
/// this epoch past the last it confirmed. Read from what each writer
/// records of its own — the relay its chain, a filing what was confirmed —
/// never from a flag the two would race to set.
pub(crate) const UNCONFIRMED: &str = "(c.last_sequence IS NOT NULL \
     AND (r.applied_epoch IS DISTINCT FROM (SELECT epoch FROM sync.epoch) \
          OR coalesce(r.applied_sequence, 0) < c.last_sequence))";

/// Provisions again every live replica that has left a message unconfirmed
/// with nothing sent to it, and nothing confirmed by it, for `after`, and
/// no send to it failing or waiting: a transport that lost the last messages
/// it was sent leaves nothing else to bring them back, since nothing newer
/// goes to it while nothing changes. While it stays silent, the next time
/// waits twice as long as the last, up to 32 times `after`, so a replica
/// that is merely away is not sent everything over and over; one that
/// caught up since waits `after` again. Answers how many.
pub(crate) async fn heal(conn: &mut Conn, engine: &Engine, after: Duration) -> Result<u64, Error> {
    let healed: i64 = sqlx::query_scalar(&format!(
        "WITH silent AS ( \
             SELECT r.replica_id AS id, s.partition FROM sync.replicas r \
               JOIN sync.chains c USING (replica_id) \
               JOIN {} ON s.id = r.replica_id \
              WHERE {UNCONFIRMED} AND r.failures = 0 \
                AND NOT EXISTS (SELECT 1 FROM sync.outbox o WHERE o.replica_id = r.replica_id) \
                AND greatest(r.last_sent_at, r.applied_at) \
                    < now() - make_interval(secs => $1 * power(2, least(r.heals, 5)))), \
         noted AS ({} RETURNING replica_id), \
         counted AS (UPDATE sync.replicas r SET heals = r.heals + 1 FROM noted n \
                      WHERE r.replica_id = n.replica_id) \
         SELECT count(*) FROM noted",
        engine.replicas_sql(),
        crate::plan::resync_sql("SELECT id, partition FROM silent")
    ))
    .bind(after.as_secs_f64())
    .fetch_one(&mut *conn)
    .await?;
    Ok(u64::try_from(healed).unwrap_or_default())
}

#[cfg(test)]
#[path = "relay_tests.rs"]
mod tests;
