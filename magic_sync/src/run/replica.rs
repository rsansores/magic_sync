//! A replica, run for the host: what the server sends acted on and
//! committed, and what the replica has to say sent up, lane by lane.
//!
//! A host builds a [`Replica`] once, calls [`Replica::run`] in a task of its
//! own, and hands every message the server sends to [`Replica::receive`],
//! acknowledging it to the transport only once that returned `Ok`.

use crate::replica::{self, Care, Lane, Outbox, Received, Stop};
use crate::run::Backoff;
use crate::wire::Codec;
use crate::{Conn, Error};
use sqlx::SqlitePool;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;
use uuid::Uuid;

/// Which replica this is: a `Uuid` the replica knows from the start, or the
/// host's own answer when it learns it later (from a provisioning step, say).
pub trait Identity: Send + Sync + 'static {
    /// The replica's id; `None` while it does not know it yet, and nothing
    /// goes up until it does.
    ///
    /// # Errors
    ///
    /// Whatever kept the host from answering: nothing goes up this turn.
    fn id(&self, conn: &mut Conn) -> impl Future<Output = Result<Option<Uuid>, Error>> + Send;
}

impl Identity for Uuid {
    async fn id(&self, _conn: &mut Conn) -> Result<Option<Uuid>, Error> {
        Ok(Some(*self))
    }
}

/// Where a replica's messages up go: the host's transport, which provides
/// what [the protocol requires of a transport][contract], keeping each lane
/// in order.
///
/// [contract]: https://rsansores.github.io/magic_sync/protocol.html#2-what-the-transport-must-provide
pub trait Uplink: Send + Sync + 'static {
    /// Sends `message`, one message up, encoded, from `replica`, on `lane`.
    /// `Ok` means the transport took it, and will deliver it.
    ///
    /// # Errors
    ///
    /// [`Error::Transport`] (or any other) when the transport did not take
    /// it. A permanent one — the transport will never take this message —
    /// has the replica send half of what it carried, and half again, until
    /// what the transport refuses is alone: that row is set aside, counted
    /// in the backlog, until the replica runs again with a build that may
    /// send it. Anything else goes again after a backoff.
    fn send(
        &self,
        replica: Uuid,
        lane: &'static str,
        message: Vec<u8>,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}

/// How a replica runs: a host changes what it needs of a
/// `Settings::default()`.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Settings {
    /// How much one message up carries.
    pub outbox: Outbox,
    /// The streams up, the main one first: it carries what the engine says
    /// besides the replica's rows.
    pub lanes: Vec<Lane>,
    /// How long the upward loop waits when there is nothing to say.
    pub idle: Duration,
    /// How long a lane waits after a failure.
    pub backoff: Backoff,
    /// When the first digests go, after the replica starts: once it settled.
    pub digests_first: Duration,
    /// How often digests go after that.
    pub digests_every: Duration,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            outbox: Outbox::default(),
            lanes: vec![Lane::new("main", &[])],
            idle: Duration::from_secs(1),
            backoff: Backoff::new(Duration::from_secs(1), Duration::from_secs(60)),
            digests_first: Duration::from_secs(5 * 60),
            digests_every: Duration::from_secs(60 * 60),
        }
    }
}

/// A replica's side of sync, for the host to run: built once, run in a task
/// of its own, and handed every message the server sends.
///
/// ```no_run
/// use magic_sync::run::replica::{Settings, Uplink};
/// use magic_sync::wire::Codec;
/// use magic_sync::{replica, Error, Replica};
/// use std::sync::Arc;
/// use uuid::Uuid;
///
/// /// The host's way to the server.
/// struct Http;
///
/// impl Uplink for Http {
///     async fn send(&self, me: Uuid, _lane: &'static str, message: Vec<u8>) -> Result<(), Error> {
///         // POST `message` to the server, as `me`
///         Ok(())
///     }
/// }
///
/// # async fn start(pool: sqlx::SqlitePool, codecs: impl Fn() -> Vec<Codec<()>>, me: Uuid, message: Vec<u8>) -> Result<(), Error> {
/// // No migrations of its own: the engine sets up its part of the database.
/// replica::install(&mut *pool.acquire().await?, &codecs()).await?;
///
/// let replica = Arc::new(Replica::new(pool, codecs(), me, Http, Settings::default()).await?);
/// let up = replica.clone();
/// tokio::spawn(async move { up.run(std::future::pending()).await });
///
/// // Every message from the server, in order; acknowledged only once applied.
/// replica.receive(&message).await?;
/// # Ok(())
/// # }
/// ```
pub struct Replica<P: 'static, I: Identity, U: Uplink> {
    pool: SqlitePool,
    codecs: Vec<Codec<P>>,
    identity: I,
    uplink: U,
    settings: Settings,
    /// Something new to say: a message applied, or a write noted.
    wake: Notify,
    /// Per lane, how many rows and rejections a message carries at most
    /// while the uplink refuses one for good: 0 for no cap.
    caps: Vec<AtomicUsize>,
}

impl<P: Sync + 'static, I: Identity, U: Uplink> Replica<P, I, U> {
    /// A replica over a database the engine can run against
    /// ([`replica::check`]): `codecs` are the tables that travel, parents
    /// first, as the server lists them; `identity` says which replica this
    /// is; `uplink` how a message reaches the server. Refused, too, when the
    /// settings cannot send up: no lane, a table on two lanes or on none.
    /// What an earlier run set aside, because it could not go up as it was,
    /// is offered again: this build may send it.
    ///
    /// # Errors
    ///
    /// [`Error::Misuse`] for such settings, or a database [`replica::check`]
    /// refuses; [`Error::Database`] when the database fails.
    pub async fn new(
        pool: SqlitePool,
        codecs: Vec<Codec<P>>,
        identity: I,
        uplink: U,
        settings: Settings,
    ) -> Result<Self, Error> {
        replica::check_settings(&codecs, &settings.lanes)?;
        settings.backoff.check("backoff")?;
        let zero = [
            ("idle", settings.idle),
            ("digests_first", settings.digests_first),
            ("digests_every", settings.digests_every),
            ("outbox.resend_after", settings.outbox.resend_after),
            ("outbox.backlog_every", settings.outbox.backlog_every),
        ];
        if let Some((what, _)) = zero.iter().find(|(_, d)| d.is_zero()) {
            return Err(Error::Misuse(format!(
                "`{what}` is zero: the replica would spin, or say the same thing without end"
            )));
        }
        let mut conn = pool.acquire().await?;
        replica::check(&mut conn, &codecs).await?;
        let offered = replica::retry_set_aside(&mut conn).await?;
        if offered > 0 {
            tracing::info!(offered, "sync: writes set aside go up again");
        }
        drop(conn);
        Ok(Replica {
            pool,
            codecs,
            identity,
            uplink,
            caps: settings.lanes.iter().map(|_| AtomicUsize::new(0)).collect(),
            settings,
            wake: Notify::new(),
        })
    }

    /// A message from the server, acted on and committed. Acknowledge it to
    /// the transport only when this returns `Ok`.
    ///
    /// # Errors
    ///
    /// The message must come again: the replica's storage failed
    /// ([`Error::Database`]), or it speaks a later protocol than this build
    /// ([`Error::Protocol`]). Nothing of it was applied.
    pub async fn receive(&self, bytes: &[u8]) -> Result<Received, Error> {
        let mut care = Care::default();
        let received = loop {
            let mut conn = self.pool.acquire().await?;
            let mut tx = sqlx::Connection::begin_with(&mut *conn, "BEGIN IMMEDIATE").await?;
            replica::settled(&mut tx).await?;
            let stop = match replica::receive(&mut tx, &self.codecs, bytes, &care).await {
                Ok(received) => {
                    tx.commit().await?;
                    break received;
                }
                Err(stop) => stop,
            };
            // The database may have rolled the transaction back under it: a
            // connection that no longer knows whether it is in one goes.
            drop(tx);
            conn.close_on_drop();
            match stop {
                Stop::Culprit(culprit) => care.refuse(culprit)?,
                Stop::Failed(e) if e.is_refusal() && care.heed() => {}
                Stop::Failed(e) => return Err(e),
            }
        };
        if matches!(
            received,
            Received::Applied(_) | Received::SnapshotEnded | Received::Gap
        ) {
            // The server counts nothing as held until it hears of it, and
            // provisions a replica that missed a message once it hears.
            self.wake.notify_one();
        }
        Ok(received)
    }

    /// Wakes the upward loop now rather than at its next turn: for a host
    /// that just wrote something it wants to see go up.
    pub fn nudge(&self) {
        self.wake.notify_one();
    }

    /// Sends up what the replica has to say, until `shutdown` resolves. Each
    /// lane keeps its own pace: one that fails waits out its own backoff
    /// while the others go on; one with more to say goes again at once.
    pub async fn run(&self, shutdown: impl Future<Output = ()>) {
        tokio::pin!(shutdown);
        let first = self.settings.backoff.first;
        let lanes = self.settings.lanes.len();
        let start = Instant::now();
        let mut due = vec![start; lanes];
        let mut backoff = vec![first; lanes];
        let mut digests_at = start + self.settings.digests_first;
        loop {
            for lane in 0..lanes {
                let now = Instant::now();
                if due[lane] > now {
                    continue;
                }
                let digests = lane == 0 && now >= digests_at;
                due[lane] = match self.send_lane(lane, digests).await {
                    Ok(went) => {
                        backoff[lane] = first;
                        if went.digests {
                            digests_at = now + self.settings.digests_every;
                        }
                        if went.message {
                            now
                        } else {
                            now + self.settings.idle
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            lane = self.settings.lanes[lane].name,
                            "sync: what this replica has to say did not go up, and goes again: {e}"
                        );
                        let wait = backoff[lane];
                        backoff[lane] = self.settings.backoff.after(wait);
                        now + wait
                    }
                };
            }
            let next = due.iter().min().copied().unwrap_or(start);
            tokio::select! {
                biased;
                () = &mut shutdown => return,
                () = self.wake.notified() => {
                    let now = Instant::now();
                    for (due, backoff) in due.iter_mut().zip(&backoff) {
                        if *backoff == first {
                            *due = now;
                        }
                    }
                }
                () = tokio::time::sleep_until(next) => {}
            }
        }
    }

    /// One message on `lane`, when it has something to say: read and
    /// numbered in one transaction, handed to the uplink with none open,
    /// recorded in another. A message the uplink refuses for good is asked
    /// again with half its rows and rejections, until the one it refuses is
    /// alone: that one is set aside, never the ones beside it.
    pub(crate) async fn send_lane(&self, lane: usize, digests: bool) -> Result<Went, Error> {
        let Some(id) = self.identity.id(&mut *self.pool.acquire().await?).await? else {
            return Ok(Went::default());
        };
        let cap = self.caps[lane].load(Ordering::Relaxed);
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let outgoing = replica::outbox(
            &mut tx,
            &self.codecs,
            &self.settings.lanes,
            lane,
            &self.settings.outbox,
            digests,
            (cap > 0).then_some(cap),
        )
        .await?;
        tx.commit().await?;
        let Some(outgoing) = outgoing else {
            return Ok(Went::default());
        };
        let sent = self.uplink.send(id, outgoing.lane, outgoing.bytes()).await;
        let went = Went {
            message: true,
            digests: !outgoing.up.digests.is_empty(),
        };
        let items = outgoing.items();
        match sent {
            Ok(()) => {
                let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
                replica::delivered(&mut tx, &outgoing).await?;
                tx.commit().await?;
                self.caps[lane].store(0, Ordering::Relaxed);
            }
            Err(e) if !e.is_transient() && items > 1 => {
                tracing::warn!(
                    lane = outgoing.lane,
                    items,
                    "sync: the uplink refuses a message for good — half of it goes, to find why: {e}"
                );
                self.caps[lane].store(items / 2, Ordering::Relaxed);
            }
            Err(e) if !e.is_transient() && items == 1 => {
                tracing::error!(
                    lane = outgoing.lane,
                    "sync: the uplink refuses a row or a rejection for good — set aside: {e}"
                );
                let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
                replica::refused(&mut tx, &outgoing, &e.to_string()).await?;
                tx.commit().await?;
                self.caps[lane].store(0, Ordering::Relaxed);
            }
            Err(e) => return Err(e),
        }
        Ok(went)
    }
}

/// What one turn of a lane sent.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Went {
    /// A message went to the uplink.
    pub(crate) message: bool,
    /// It carried the digests.
    pub(crate) digests: bool,
}

#[cfg(test)]
#[path = "replica_tests.rs"]
mod tests;
