//! A replica, run for the host: what the server sends acted on and
//! committed, and what the replica has to say sent up, lane by lane.
//!
//! A host builds a [`Replica`] once, calls [`Replica::run`] in a task of its
//! own, and hands every message the server sends to [`Replica::receive`],
//! acknowledging it to the transport only once that returned `Ok`.

use crate::envelope::Limits;
use crate::replica::{self, Care, Lane, Outbox, Received, Stop};
use crate::run::Backoff;
use crate::wire::Codec;
pub use crate::wish::{CarryOut, Direct, Outcome, Wish, Wishes};
use crate::wish::{Offered, Run};
use crate::{Conn, Error};
use sqlx::SqlitePool;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::Poll;
use std::time::Duration;
use tokio::sync::{watch, Notify};

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
    /// send it — but only once the transport has taken some other message of
    /// the lane meanwhile, since one that takes none refuses for a reason
    /// that is not the row's, and then nothing is set aside: the replica
    /// logs an error and goes again after a backoff, as it does for anything
    /// else.
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
    /// How much one message from the server carries, at most: rows — its
    /// upserts and deletes together — and tables, or uploads it answers.
    /// One that carries more is malformed. The server's limits, or more.
    pub inbound: Limits,
    /// The streams up, the main one first: it carries what the engine says
    /// besides the replica's rows.
    pub lanes: Vec<Lane>,
    /// How long the upward loop waits when there is nothing to say.
    pub idle: Duration,
    /// How long a lane waits after a failure.
    pub backoff: Backoff,
    /// How long the uplink has to take a message before the send counts as
    /// failed, for a while: a send that hangs holds back only its lane, and
    /// only so long.
    pub send_timeout: Duration,
    /// When the first digests go, after the replica first starts on its
    /// database: once it settled. When they are due is kept in the
    /// database, so a replica that restarts often says them all the same.
    pub digests_first: Duration,
    /// How often digests go after that.
    pub digests_every: Duration,
    /// The handlers that carry out the wishes of the replica's settings: one
    /// per setting table, and none for another.
    pub wishes: Wishes,
    /// How often a wish its handler could not carry out yet is offered
    /// again, besides when [`Replica::offer_wishes`] asks.
    pub wishes_every: Duration,
    /// How long a handler has to carry a wish out: one that takes longer
    /// has the wish wait, and offered again.
    pub wishes_timeout: Duration,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            outbox: Outbox::default(),
            inbound: Limits::default(),
            lanes: vec![Lane::new("main", &[])],
            idle: Duration::from_secs(1),
            backoff: Backoff::new(Duration::from_secs(1), Duration::from_secs(60)),
            send_timeout: Duration::from_secs(30),
            digests_first: Duration::from_secs(5 * 60),
            digests_every: Duration::from_secs(60 * 60),
            wishes: Wishes::new(),
            wishes_every: Duration::from_secs(60),
            wishes_timeout: Duration::from_secs(60),
        }
    }
}

/// A replica's side of sync, for the host to run: built once, run in a task
/// of its own, and handed every message the server sends.
///
/// ```no_run
/// use magic_sync::run::replica::{Replica, Settings, Uplink};
/// use magic_sync::wire::Codec;
/// use magic_sync::{replica, Error};
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
    /// Per lane, something new to say: a message applied, or a write noted.
    wake: Vec<Notify>,
    /// Wishes to offer their handlers: a message changed one, or the host
    /// asked.
    offer: Notify,
    /// Whether [`Replica::run`] runs: one at a time, or two would send the
    /// same rows under two numbers.
    running: AtomicBool,
    /// Per lane, how many rows and rejections a message carries at most
    /// while the uplink refuses one for good: 0 for no cap.
    caps: Vec<AtomicUsize>,
    /// Per lane, whether the transport took a message while it was being
    /// asked for less than the replica had to say, and has not failed since:
    /// proof that a row it then refuses alone is the one at fault, and not
    /// the transport.
    witnessed: Vec<AtomicBool>,
    /// Per lane, how many rows and rejections the next message passes over
    /// while it looks for one the transport takes: 0 when it is not looking.
    skips: Vec<AtomicUsize>,
    /// How many times a message was applied again, with more care, since
    /// this replica began: each a row at fault, or a cost of nothing.
    #[cfg(test)]
    pub(crate) again: AtomicUsize,
}

impl<P: 'static, I: Identity, U: Uplink> std::fmt::Debug for Replica<P, I, U> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tables: Vec<_> = self.codecs.iter().map(|c| c.table.name).collect();
        f.debug_struct("Replica")
            .field("tables", &tables)
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

impl<P: Sync + 'static, I: Identity, U: Uplink> Replica<P, I, U> {
    /// A replica over a database the engine can run against
    /// ([`replica::check`]): `codecs` are the tables that travel, parents
    /// first, as the server lists them; `identity` says which replica this
    /// is — one that already knows is set up, and its triggers refuse from
    /// here on; `uplink` how a message reaches the server. Refused, too, when the
    /// settings cannot send up: no lane, a table on two lanes or on none.
    /// What an earlier run set aside, because it could not go up as it was,
    /// is offered again: this build may send it. What the replica wrote of
    /// a table this build does not send up stays, set aside and counted in
    /// the backlog, for a build that does.
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
        let codecs = crate::wish::with_wishes(codecs)?;
        replica::check_settings(&codecs, &settings.lanes)?;
        settings.wishes.check(&codecs)?;
        settings.backoff.check("backoff")?;
        let zero = [
            ("idle", settings.idle),
            ("send_timeout", settings.send_timeout),
            ("digests_first", settings.digests_first),
            ("digests_every", settings.digests_every),
            ("wishes_every", settings.wishes_every),
            ("wishes_timeout", settings.wishes_timeout),
            ("outbox.resend_after", settings.outbox.resend_after),
            ("outbox.keepalive_every", settings.outbox.keepalive_every),
        ];
        if let Some((what, _)) = zero.iter().find(|(_, d)| d.is_zero()) {
            return Err(Error::Misuse(format!(
                "`{what}` is zero: the replica would spin, or say the same thing without end"
            )));
        }
        if settings.outbox.keepalive_every > std::time::Duration::from_secs(24 * 60 * 60) {
            return Err(Error::Misuse(
                "`outbox.keepalive_every` is over a day: the server would take it for none, \
                 and never know the replica stopped"
                    .into(),
            ));
        }
        if settings.idle >= settings.outbox.keepalive_every {
            return Err(Error::Misuse(
                "`idle` is not under `outbox.keepalive_every`: the replica would sleep past \
                 its keepalive, and be taken for one that stopped"
                    .into(),
            ));
        }
        let mut conn = pool.acquire().await?;
        replica::checked(&mut conn, &codecs.iter().collect::<Vec<_>>()).await?;
        let offered = replica::retry_set_aside(&mut conn).await?;
        if offered > 0 {
            tracing::info!(offered, "sync: writes set aside go up again");
        }
        // One that knows which it is is set up: its triggers refuse from now.
        if let Some(id) = identity.id(&mut conn).await? {
            replica::identified(&mut conn, id).await?;
        }
        drop(conn);
        Ok(Replica {
            pool,
            codecs,
            identity,
            uplink,
            caps: settings.lanes.iter().map(|_| AtomicUsize::new(0)).collect(),
            witnessed: settings
                .lanes
                .iter()
                .map(|_| AtomicBool::new(false))
                .collect(),
            skips: settings.lanes.iter().map(|_| AtomicUsize::new(0)).collect(),
            wake: settings.lanes.iter().map(|_| Notify::new()).collect(),
            offer: Notify::new(),
            running: AtomicBool::new(false),
            #[cfg(test)]
            again: AtomicUsize::new(0),
            settings,
        })
    }

    /// A message from the server, acted on and committed. Acknowledge it to
    /// the transport only when this returns `Ok`.
    ///
    /// # Errors
    ///
    /// The message must come again: the replica's storage failed
    /// ([`Error::Database`]), or it speaks a later protocol than this build
    /// ([`Error::Protocol`]). Nothing of it was applied. Nothing it carries
    /// is an error: what the database refuses of it is rejected, row by row
    /// — every row of it when the database refuses it for what no row
    /// explains — and a message even that does not apply is
    /// [`Received::Malformed`].
    pub async fn receive(&self, bytes: &[u8]) -> Result<Received, Error> {
        let mut care = Care::default();
        let received = loop {
            let mut conn = self.pool.acquire().await?;
            let mut tx = sqlx::Connection::begin_with(&mut *conn, "BEGIN IMMEDIATE").await?;
            if let Some(id) = self.identity.id(&mut tx).await? {
                replica::identified(&mut tx, id).await?;
            }
            replica::settled(&mut tx).await?;
            let limits = &self.settings.inbound;
            let stop = match replica::receive(&mut tx, &self.codecs, bytes, limits, &care).await {
                // A key checked at commit may refuse it: a row of the
                // message is at fault, and found once applied carefully.
                Ok(received) => match tx.commit().await {
                    Ok(()) => break received,
                    Err(e) => Stop::Failed(e.into()),
                },
                Err(stop) => {
                    drop(tx);
                    stop
                }
            };
            // The database may have rolled the transaction back under it: a
            // connection that no longer knows whether it is in one goes.
            conn.close_on_drop();
            let again = match stop {
                Stop::Culprits(culprits) => care.refuse(culprits) || care.heed(),
                Stop::Failed(e) if e.is_refusal() => care.heed(),
                Stop::Failed(e) => return Err(e),
            };
            #[cfg(test)]
            self.again.fetch_add(1, Ordering::Relaxed);
            if !again {
                // Refused with every row of it refused: nothing a message
                // changes is at fault, and this one never applies.
                return Ok(replica::malformed(
                    "the database refuses it even with every row of it refused".to_owned(),
                ));
            }
        };
        if matches!(
            received,
            Received::Applied(_) | Received::SnapshotEnded | Received::Filed
        ) {
            // The message stands whatever this does.
            if let Err(e) = replica::collect(&self.pool, &self.codecs).await {
                tracing::warn!(
                    "sync: retired rows nothing keeps stay, until the next message: {e}"
                );
            }
        }
        let wished = match &received {
            Received::Applied(applied) => applied.iter().any(|a| a.table == crate::wish::TABLE),
            Received::SnapshotEnded => true,
            _ => false,
        };
        if wished {
            self.offer_wishes();
        }
        if matches!(
            received,
            Received::Applied(_) | Received::SnapshotEnded | Received::Gap
        ) {
            // The server counts nothing as held until it hears of it, and
            // provisions a replica that missed a message once it hears.
            self.nudge();
        }
        Ok(received)
    }

    /// Offers every wish not carried out yet to its handler now, rather than
    /// at the next message or `wishes_every`: for a host whose handler said
    /// [`Later`](Outcome::Later) and can now — a module came
    /// back. Wishes are offered while [`Replica::run`] runs.
    pub fn offer_wishes(&self) {
        self.offer.notify_one();
    }

    /// Offers every wish not carried out yet, or waiting, to its handler,
    /// one after another, and sends up what that wrote.
    pub(crate) async fn carry_out_wishes(&self) -> Result<Offered, Error> {
        let Some(me) = self.identity.id(&mut *self.pool.acquire().await?).await? else {
            return Ok(Offered::default());
        };
        let run = Run {
            pool: self.pool.clone(),
            me,
            timeout: self.settings.wishes_timeout,
        };
        let offered = self.settings.wishes.offer(&run, &self.codecs).await?;
        if offered.wrote {
            self.nudge();
        }
        Ok(offered)
    }

    /// The wishes' loop, until `stopped`: at start, whenever a message
    /// changed one or the host asks, and every `wishes_every` while one
    /// waits. A handler under way when it stops is dropped: nothing of it is
    /// recorded, and it is offered again at the next start.
    async fn wishes(&self, mut stopped: watch::Receiver<bool>) {
        if !self
            .codecs
            .iter()
            .any(|c| c.table.name == crate::wish::TABLE)
        {
            return;
        }
        let first = self.settings.backoff.first;
        let mut backoff = first;
        while !*stopped.borrow() {
            let offered = tokio::select! {
                biased;
                _ = stopped.wait_for(|stop| *stop) => return,
                offered = self.carry_out_wishes() => offered,
            };
            let wait = match offered {
                Ok(offered) => {
                    backoff = first;
                    offered.waiting.then_some(self.settings.wishes_every)
                }
                Err(e) => {
                    tracing::warn!("sync: wishes not offered, and offered again: {e}");
                    let wait = backoff;
                    backoff = self.settings.backoff.after(wait);
                    Some(wait)
                }
            };
            tokio::select! {
                biased;
                _ = stopped.wait_for(|stop| *stop) => return,
                () = self.offer.notified() => {}
                () = async {
                    match wait {
                        Some(wait) => tokio::time::sleep(wait).await,
                        None => std::future::pending().await,
                    }
                } => {}
            }
        }
    }

    /// Wakes every lane now rather than at its next turn — but one waiting
    /// out a failure: for a host that just wrote something it wants to see
    /// go up.
    pub fn nudge(&self) {
        for wake in &self.wake {
            wake.notify_one();
        }
    }

    /// Sends up what the replica has to say, and offers the wishes of its
    /// settings to their handlers, until `shutdown` resolves, finishing what
    /// it is sending. Each lane runs on its own: one whose
    /// uplink fails, or hangs (for `send_timeout` at most), waits out its own
    /// backoff while the others go on; one with more to say goes again at
    /// once. A second `run` of the same replica while one runs returns at
    /// once.
    pub async fn run(&self, shutdown: impl Future<Output = ()>) {
        if self.running.swap(true, Ordering::AcqRel) {
            tracing::error!("sync: this replica runs already — a second run returns");
            return;
        }
        let _running = Running(&self.running);
        let (stop, stopped) = watch::channel(false);
        let mut lanes: Vec<Option<Pin<Box<dyn Future<Output = ()> + Send + '_>>>> =
            (0..self.settings.lanes.len())
                .map(|lane| Some(Box::pin(self.lane(lane, stopped.clone())) as Pin<Box<_>>))
                .chain([Some(Box::pin(self.wishes(stopped.clone())) as Pin<Box<_>>)])
                .collect();
        let all = std::future::poll_fn(|cx| {
            for lane in &mut lanes {
                if lane
                    .as_mut()
                    .is_some_and(|f| f.as_mut().poll(cx).is_ready())
                {
                    *lane = None;
                }
            }
            if lanes.iter().all(Option::is_none) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        });
        tokio::pin!(shutdown, all);
        tokio::select! {
            () = &mut shutdown => {
                let _ = stop.send(true);
                all.await;
            }
            () = &mut all => {}
        }
    }

    /// One lane's loop, until `stopped`.
    async fn lane(&self, lane: usize, mut stopped: watch::Receiver<bool>) {
        let first = self.settings.backoff.first;
        let mut backoff = first;
        while !*stopped.borrow() {
            let (wait, failed) = match self.turn(lane).await {
                Ok(went) => {
                    backoff = first;
                    if went.message {
                        continue;
                    }
                    (self.settings.idle, false)
                }
                Err(e) => {
                    tracing::warn!(
                        lane = self.settings.lanes[lane].name,
                        "sync: what this replica has to say did not go up, and goes again: {e}"
                    );
                    let wait = backoff;
                    backoff = self.settings.backoff.after(wait);
                    (wait, true)
                }
            };
            tokio::select! {
                biased;
                _ = stopped.wait_for(|stop| *stop) => return,
                () = self.wake[lane].notified(), if !failed => {}
                () = tokio::time::sleep(wait) => {}
            }
        }
    }

    /// One turn of `lane`: a message, with the digests when they are due
    /// on the main lane.
    async fn turn(&self, lane: usize) -> Result<Went, Error> {
        let settings = &self.settings;
        let digests = lane == 0
            && replica::digests_due(
                &mut *self.pool.acquire().await?,
                settings.digests_first,
                settings.digests_every,
            )
            .await?;
        self.send_lane(lane, digests).await
    }

    /// One message on `lane`, when it has something to say: read and
    /// numbered in one transaction, handed to the uplink with none open,
    /// recorded in another. A message the uplink refuses for good is asked
    /// again with half its rows and rejections, until the one it refuses is
    /// alone: that one is set aside, never the ones beside it — provided the
    /// transport took another message of the lane meanwhile. One that took
    /// none refuses for a reason that is not the row's (a credential, a host
    /// that answers every message so): nothing is set aside, the alarm is
    /// raised, and the lane goes again after its backoff.
    pub(crate) async fn send_lane(&self, lane: usize, digests: bool) -> Result<Went, Error> {
        let Some(id) = self.identity.id(&mut *self.pool.acquire().await?).await? else {
            return Ok(Went::default());
        };
        let cap = self.caps[lane].load(Ordering::Relaxed);
        let skip = self.skips[lane].load(Ordering::Relaxed);
        // Read apart from the write lock: digests read every row it holds.
        let held = if digests {
            let mut tx = self.pool.begin().await?;
            let held = replica::held(&mut tx, &self.codecs).await?;
            tx.commit().await?;
            Some(held)
        } else {
            None
        };
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        replica::identified(&mut tx, id).await?;
        let outgoing = replica::outbox(
            &mut tx,
            &self.codecs,
            &self.settings.lanes,
            lane,
            &self.settings.outbox,
            held,
            (cap > 0).then_some(replica::Window { take: cap, skip }),
        )
        .await?;
        tx.commit().await?;
        let witnessed = &self.witnessed[lane];
        let Some(outgoing) = outgoing.filter(|o| skip == 0 || o.items() > 0) else {
            // Nothing left of what was being narrowed down.
            witnessed.store(false, Ordering::Relaxed);
            if skip > 0 {
                self.skips[lane].store(0, Ordering::Relaxed);
                self.caps[lane].store(0, Ordering::Relaxed);
                tracing::error!(
                    lane = self.settings.lanes[lane].name,
                    "sync: the uplink refuses what this lane sends for good, and has no other \
                     row to try it with — nothing is set aside, and it goes again"
                );
                return Err(Error::transport(
                    true,
                    "the uplink refuses the only messages this lane has".to_owned(),
                ));
            }
            return Ok(Went::default());
        };
        let timeout = self.settings.send_timeout;
        let sent = tokio::time::timeout(
            timeout,
            self.uplink.send(id, outgoing.lane, outgoing.bytes()),
        )
        .await
        .unwrap_or_else(|_| {
            Err(Error::transport(
                true,
                format!("the uplink did not take the message within {timeout:?}"),
            ))
        });
        let mut went = Went {
            message: true,
            digests: false,
        };
        let items = outgoing.items();
        match sent {
            Ok(()) => {
                let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
                replica::delivered(&mut tx, &outgoing).await?;
                went.digests = !outgoing.up.digests.is_empty();
                if went.digests {
                    replica::digested(&mut tx, self.settings.digests_every).await?;
                }
                tx.commit().await?;
                witnessed.store(cap > 0, Ordering::Relaxed);
                self.skips[lane].store(0, Ordering::Relaxed);
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
            Err(e) if !e.is_transient() && items == 1 && witnessed.load(Ordering::Relaxed) => {
                tracing::error!(
                    lane = outgoing.lane,
                    "sync: the uplink refuses a row or a rejection for good — set aside: {e}"
                );
                let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
                replica::refused(&mut tx, &outgoing, &e.to_string()).await?;
                tx.commit().await?;
                witnessed.store(false, Ordering::Relaxed);
                self.skips[lane].store(0, Ordering::Relaxed);
                self.caps[lane].store(0, Ordering::Relaxed);
            }
            Err(e) if !e.is_transient() && items == 1 => {
                tracing::warn!(
                    lane = outgoing.lane,
                    "sync: the uplink refuses a row for good, and has taken nothing else of this \
                     lane meanwhile — another goes, to see whether it is the row: {e}"
                );
                self.skips[lane].store(if skip == 0 { 1 } else { skip * 2 }, Ordering::Relaxed);
                self.caps[lane].store(1, Ordering::Relaxed);
            }
            Err(e) => {
                // The transport may be another by the time it takes again,
                // or refuses with no row to blame: what it took before
                // witnesses nothing after.
                witnessed.store(false, Ordering::Relaxed);
                if !e.is_transient() {
                    tracing::error!(
                        lane = outgoing.lane,
                        items,
                        "sync: the uplink refuses for good a message that carries no row — \
                         nothing to set aside, and it goes again: {e}"
                    );
                }
                return Err(e);
            }
        }
        Ok(went)
    }
}

/// A run under way: over once dropped, however it ends.
struct Running<'a>(&'a AtomicBool);

impl Drop for Running<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
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
