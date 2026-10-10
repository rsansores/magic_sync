//! A replica, run for the host: what the server sends acted on and
//! committed, and what the replica has to say sent up, lane by lane.
//!
//! A host builds a [`Replica`] once, calls [`Replica::run`] in a task of its
//! own, and hands every message the server sends to [`Replica::receive`],
//! acknowledging it to the transport only once that returned `Ok`.

use crate::replica::{self, Lane, Outbox, Received};
use crate::row::Conn;
use crate::wire::Codec;
use crate::Error;
use sqlx::SqlitePool;
use std::future::Future;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;
use uuid::Uuid;

/// Which replica this is.
pub trait Identity: Send + Sync + 'static {
    /// The replica's id; `None` while it does not know it yet, and nothing
    /// goes up until it does.
    fn id(&self, conn: &mut Conn) -> impl Future<Output = Result<Option<Uuid>, Error>> + Send;
}

impl Identity for Uuid {
    async fn id(&self, _conn: &mut Conn) -> Result<Option<Uuid>, Error> {
        Ok(Some(*self))
    }
}

/// Where a replica's messages up go.
pub trait Uplink: Send + Sync + 'static {
    /// Sends `message`, an encoded [`Up`](crate::envelope::Up) from
    /// `replica`, on `lane`. An error means it was not taken, and it goes
    /// again.
    fn send(
        &self,
        replica: Uuid,
        lane: &'static str,
        message: Vec<u8>,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}

/// How a replica runs.
#[derive(Clone, Debug)]
pub struct Settings {
    /// How much one message up carries.
    pub outbox: Outbox,
    /// The streams up, the main one first: it carries what the engine says
    /// besides the replica's rows.
    pub lanes: Vec<Lane>,
    /// How long the upward loop waits when there is nothing to say.
    pub idle: Duration,
    /// How long it waits after a failure, doubling up to the second.
    pub backoff: (Duration, Duration),
    /// When the first digests go, after the replica starts: once it settled.
    pub digests_first: Duration,
    /// How often digests go after that.
    pub digests_every: Duration,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            outbox: Outbox::default(),
            lanes: vec![Lane {
                name: "main",
                tables: &[],
            }],
            idle: Duration::from_secs(1),
            backoff: (Duration::from_secs(1), Duration::from_secs(60)),
            digests_first: Duration::from_secs(5 * 60),
            digests_every: Duration::from_secs(60 * 60),
        }
    }
}

/// A replica's side of sync, for the host to run.
pub struct Replica<P: 'static, I: Identity, U: Uplink> {
    pool: SqlitePool,
    codecs: &'static [Codec<P>],
    identity: I,
    uplink: U,
    settings: Settings,
    /// Something new to say: a message applied, or a write noted.
    wake: Notify,
}

impl<P: Sync + 'static, I: Identity, U: Uplink> Replica<P, I, U> {
    /// A replica over a database the engine can run against
    /// ([`replica::check`]), with settings it can send up with
    /// ([`replica::check_settings`]). What an earlier run set aside, because
    /// it could not go up as it was, is offered again: this build may send it.
    pub async fn new(
        pool: SqlitePool,
        codecs: &'static [Codec<P>],
        identity: I,
        uplink: U,
        settings: Settings,
    ) -> Result<Self, Error> {
        replica::check_settings(codecs, &settings.lanes, &settings.outbox)?;
        let mut conn = pool.acquire().await?;
        replica::check(&mut conn, codecs).await?;
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
            settings,
            wake: Notify::new(),
        })
    }

    /// A message from the server, acted on and committed. Acknowledge it to
    /// the transport only when this returns `Ok`; an error means it must come
    /// again (the replica's storage failed, or it speaks a later protocol than
    /// this build).
    pub async fn receive(&self, bytes: &[u8]) -> Result<Received, Error> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let received = replica::receive(&mut tx, self.codecs, bytes).await?;
        tx.commit().await?;
        if matches!(received, Received::Applied(_) | Received::SnapshotEnded) {
            // The server counts nothing as held until it hears of it.
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
        let (min, max) = self.settings.backoff;
        let lanes = self.settings.lanes.len();
        let start = Instant::now();
        let mut due = vec![start; lanes];
        let mut backoff = vec![min; lanes];
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
                        backoff[lane] = min;
                        if digests {
                            digests_at = now + self.settings.digests_every;
                        }
                        if went {
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
                        backoff[lane] = (wait * 2).min(max);
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
                        if *backoff == min {
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
    /// recorded in another. Answers whether one went. A message the uplink
    /// refuses for good has its rows set aside rather than offered for ever.
    pub async fn send_lane(&self, lane: usize, digests: bool) -> Result<bool, Error> {
        let mut conn = self.pool.acquire().await?;
        let Some(id) = self.identity.id(&mut conn).await? else {
            return Ok(false);
        };
        let mut tx = sqlx::Connection::begin_with(&mut *conn, "BEGIN IMMEDIATE").await?;
        let outgoing = replica::outbox(
            &mut tx,
            self.codecs,
            &self.settings.lanes,
            lane,
            &self.settings.outbox,
            digests,
        )
        .await?;
        tx.commit().await?;
        let Some(outgoing) = outgoing else {
            return Ok(false);
        };
        let sent = self.uplink.send(id, outgoing.lane, outgoing.bytes()).await;
        let mut tx = sqlx::Connection::begin_with(&mut *conn, "BEGIN IMMEDIATE").await?;
        match sent {
            Ok(()) => replica::delivered(&mut tx, &outgoing).await?,
            Err(e) if !e.is_transient() && outgoing.rows() > 0 => {
                tracing::error!(
                    lane = outgoing.lane,
                    rows = outgoing.rows(),
                    "sync: the uplink refuses a message for good — its rows set aside: {e}"
                );
                replica::set_aside(&mut tx, &outgoing, &e.to_string()).await?;
            }
            Err(e) => return Err(e),
        }
        tx.commit().await?;
        Ok(true)
    }
}

#[cfg(test)]
#[path = "replica_tests.rs"]
mod tests;
