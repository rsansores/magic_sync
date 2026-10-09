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
    pub fn new(
        pool: SqlitePool,
        codecs: &'static [Codec<P>],
        identity: I,
        uplink: U,
        settings: Settings,
    ) -> Self {
        Replica {
            pool,
            codecs,
            identity,
            uplink,
            settings,
            wake: Notify::new(),
        }
    }

    /// A message from the server, acted on and committed. Acknowledge it to
    /// the transport only when this returns `Ok`; an error means it must come
    /// again (the replica's storage failed, or it speaks a later protocol than
    /// this build).
    pub async fn receive(&self, bytes: &[u8]) -> Result<Received, Error> {
        let mut tx = self.pool.begin().await?;
        let received = replica::receive(&mut tx, self.codecs, bytes).await?;
        tx.commit().await?;
        if matches!(received, Received::Applied(_)) {
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

    /// Sends up what the replica has to say, until `shutdown` resolves.
    pub async fn run(&self, shutdown: impl Future<Output = ()>) {
        tokio::pin!(shutdown);
        let (min, max) = self.settings.backoff;
        let mut backoff = min;
        let mut digests_at = Instant::now() + self.settings.digests_first;
        loop {
            let digests = Instant::now() >= digests_at;
            let wait = match self.send_up(digests).await {
                Ok(sent) => {
                    backoff = min;
                    if digests {
                        digests_at = Instant::now() + self.settings.digests_every;
                    }
                    if sent > 0 {
                        continue;
                    }
                    self.settings.idle
                }
                Err(e) => {
                    tracing::warn!(
                        "sync: what this replica has to say did not go up, and goes again: {e}"
                    );
                    let wait = backoff;
                    backoff = (backoff * 2).min(max);
                    wait
                }
            };
            tokio::select! {
                () = &mut shutdown => return,
                () = self.wake.notified() => {}
                () = tokio::time::sleep(wait) => {}
            }
        }
    }

    /// One turn of the upward loop: a message per lane with something to say.
    /// Answers how many of the replica's writes went.
    pub async fn send_up(&self, digests: bool) -> Result<usize, Error> {
        let mut conn = self.pool.acquire().await?;
        let Some(id) = self.identity.id(&mut conn).await? else {
            return Ok(0);
        };
        let mut sent = 0;
        for lane in 0..self.settings.lanes.len() {
            let Some(outgoing) = replica::outbox(
                &mut conn,
                self.codecs,
                id,
                &self.settings.lanes,
                lane,
                &self.settings.outbox,
                digests,
            )
            .await?
            else {
                continue;
            };
            self.uplink
                .send(id, outgoing.lane, outgoing.bytes())
                .await?;
            replica::delivered(&mut conn, &outgoing).await?;
            sent += outgoing.rows();
        }
        Ok(sent)
    }
}

#[cfg(test)]
#[path = "replica_tests.rs"]
mod tests;
