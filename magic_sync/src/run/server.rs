//! The server, run for the host: one relay at a time across every process
//! that runs it, passes until nothing is pending, what was set aside tried
//! again, and what replicas send up filed and answered.
//!
//! A host builds a [`Server`] once, calls [`Server::run`] in a task of its
//! own, and hands every message a replica sends up to [`Server::receive`].
//! What the host knows and the engine does not — who a replica is, and the
//! transaction its uploads are filed in — it says through [`Senders`].

use crate::envelope::{Body, Filed, Limits, Up};
use crate::plan::{Engine, Replicas};
use crate::relay::{self, Relay, Transport};
use crate::replica;
use crate::row::Table;
use crate::wire::Codec;
use crate::Error;
use sqlx::{Connection, PgConnection, PgPool, Postgres, Transaction};
use std::future::Future;
use std::time::Duration;
use tokio::time::Instant;
use uuid::Uuid;

/// What the host knows about the replicas that send something up.
pub trait Senders: Send + Sync + 'static {
    /// What a codec stamps on an uploaded row: the `P` of [`Codec<P>`].
    type Known: Send + Sync + 'static;

    /// What the server knows about `replica`; `None` for one it does not know,
    /// whose messages are dropped with a log.
    fn known(
        &self,
        replica: Uuid,
    ) -> impl Future<Output = Result<Option<Self::Known>, Error>> + Send;

    /// The transaction an upload from that replica is filed in: where the
    /// host says who is writing, when its database cares (a role, a setting its
    /// row policies read).
    fn filing(
        &self,
        known: &Self::Known,
    ) -> impl Future<Output = Result<Transaction<'static, Postgres>, Error>> + Send;
}

/// Every replica files as it is, in a plain transaction.
pub struct Anyone(pub PgPool);

impl Senders for Anyone {
    type Known = ();

    async fn known(&self, _replica: Uuid) -> Result<Option<()>, Error> {
        Ok(Some(()))
    }

    async fn filing(&self, _known: &()) -> Result<Transaction<'static, Postgres>, Error> {
        Ok(self.0.begin().await?)
    }
}

/// How the server runs. The defaults suit a server of a few hundred replicas
/// on a transport that takes a megabyte per message.
#[derive(Clone, Debug)]
pub struct Settings {
    /// How large a message the transport takes.
    pub limits: Limits,
    /// Notes planned per pass.
    pub batch: i64,
    /// The advisory lock only the running relay holds.
    pub lock_key: i64,
    /// The role every transaction of the engine's own assumes, when the
    /// database gives the connecting one less than the engine needs.
    pub role: Option<String>,
    /// How long the relay waits when nothing is pending.
    pub idle: Duration,
    /// How long it waits after a failure, doubling up to the second.
    pub backoff: (Duration, Duration),
    /// How often a process that is not the relay asks for the lock.
    pub standby: Duration,
    /// How often what was set aside is tried again, besides when the relay
    /// starts — usually after a deploy, which may have fixed the cause.
    pub retry_every: Duration,
    /// How long a note is kept once it is done.
    pub prune_after: Duration,
    /// How often the engine's health is looked at, and what is wrong logged.
    pub watch_every: Duration,
    /// How long the oldest pending note may wait before that is an alarm.
    pub stuck_after: Duration,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            limits: Limits {
                rows: 500,
                bytes: 512 * 1024,
            },
            batch: 500,
            // "SYNCENGN"
            lock_key: 0x5359_4e43_454e_474e,
            role: None,
            idle: Duration::from_secs(1),
            backoff: (Duration::from_secs(1), Duration::from_secs(60)),
            standby: Duration::from_secs(30),
            retry_every: Duration::from_secs(60 * 60),
            prune_after: Duration::from_secs(30 * 24 * 60 * 60),
            watch_every: Duration::from_secs(5 * 60),
            stuck_after: Duration::from_secs(15 * 60),
        }
    }
}

/// The server's side of sync, for the host to run.
pub struct Server<S: Senders, T: Transport> {
    pool: PgPool,
    engine: Engine,
    codecs: &'static [Codec<S::Known>],
    transport: T,
    senders: S,
    settings: Settings,
}

impl<S: Senders, T: Transport + Send + 'static> Server<S, T> {
    /// Builds the engine from the schema: refused when a declaration cannot
    /// be honoured ([`Engine::new`]).
    pub async fn new(
        pool: PgPool,
        codecs: &'static [Codec<S::Known>],
        replicas: Replicas,
        transport: T,
        senders: S,
        settings: Settings,
    ) -> Result<Self, Error> {
        let tables: Vec<&Table> = codecs.iter().map(|c| &c.table).collect();
        let mut tx = begin(&pool, settings.role.as_deref()).await?;
        let engine = Engine::new(&mut tx, &tables, replicas).await?;
        tx.rollback().await?;
        Ok(Self::with(
            pool, engine, codecs, transport, senders, settings,
        ))
    }

    /// [`Server::new`], having first installed what the database lacks
    /// ([`Engine::install`]): for a host with no migrations of its own. Run it
    /// before anything writes a mirrored table, on a pool whose connections
    /// may create triggers.
    pub async fn install(
        pool: PgPool,
        codecs: &'static [Codec<S::Known>],
        replicas: Replicas,
        transport: T,
        senders: S,
        settings: Settings,
    ) -> Result<Self, Error> {
        let tables: Vec<&Table> = codecs.iter().map(|c| &c.table).collect();
        let mut tx = pool.begin().await?;
        let engine = Engine::install(&mut tx, &tables, replicas).await?;
        tx.commit().await?;
        Ok(Self::with(
            pool, engine, codecs, transport, senders, settings,
        ))
    }

    fn with(
        pool: PgPool,
        engine: Engine,
        codecs: &'static [Codec<S::Known>],
        transport: T,
        senders: S,
        settings: Settings,
    ) -> Self {
        Server {
            pool,
            engine,
            codecs,
            transport,
            senders,
            settings,
        }
    }

    /// The engine, for what a host asks of it: progress, where a row is.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// A transaction of the engine's own, for the host to ask it things in.
    pub async fn begin(&self) -> Result<Transaction<'static, Postgres>, Error> {
        begin(&self.pool, self.settings.role.as_deref()).await
    }

    /// Runs the relay until `shutdown` resolves: stands by while another
    /// process holds the lock, and runs passes while this one does.
    pub async fn run(&self, shutdown: impl Future<Output = ()>) {
        tokio::pin!(shutdown);
        loop {
            let lock = tokio::select! {
                () = &mut shutdown => return,
                lock = self.lock() => lock,
            };
            tokio::select! {
                () = &mut shutdown => return,
                () = self.lead(lock) => {}
            }
        }
    }

    /// A message `replica` sent up, filed in the transaction the host gives
    /// for it and answered once that commits. An error means the message must
    /// come again: the database or the transport is away, or the message
    /// speaks a later protocol than this build. Filing it again is harmless.
    ///
    /// Answers what was filed, per table, for the host to act on.
    pub async fn receive(&self, replica: Uuid, bytes: &[u8]) -> Result<Vec<replica::Filed>, Error> {
        use prost::Message as _;
        let up = match Up::decode(bytes) {
            Ok(up) => up,
            Err(e) => {
                tracing::error!(%replica, "sync: an upload that does not decode — dropped: {e}");
                return Ok(Vec::new());
            }
        };
        let Some(known) = self.senders.known(replica).await? else {
            tracing::warn!(%replica, "sync: an upload from no replica this server knows — dropped");
            return Ok(Vec::new());
        };
        let mut tx = self.senders.filing(&known).await?;
        let filed = replica::file(&mut tx, self.codecs, &up, replica, &known).await?;
        tx.commit().await?;
        if let Some(last) = up.uploads.iter().max() {
            let answer = Body::Filed(Filed {
                uploads: up.uploads.clone(),
            });
            self.transport
                .send(
                    replica,
                    answer.encoded(),
                    &format!("filed-{replica}-{last}"),
                )
                .await?;
        }
        Ok(filed)
    }

    /// One pass now, as the relay, for a host's tests or a tool: what was
    /// done, or `None` when another process runs the relay.
    pub async fn pass_now(&self) -> Result<Option<usize>, Error> {
        let Some(mut lock) = Lock::take(&self.pool, self.settings.lock_key).await? else {
            return Ok(None);
        };
        let done = self.pass(&mut lock).await;
        lock.release(self.settings.lock_key).await;
        done.map(Some)
    }

    /// One pass, as the relay holding `lock`: what was done.
    async fn pass(&self, lock: &mut Lock) -> Result<usize, Error> {
        let relay = Relay {
            engine: &self.engine,
            codecs: self.codecs,
            limits: self.settings.limits,
            batch: self.settings.batch,
        };
        let prepared = {
            let mut tx = self.begin().await?;
            let prepared = relay.prepare(&mut tx).await?;
            tx.rollback().await?;
            prepared
        };
        let Some(prepared) = prepared else {
            return Ok(0);
        };
        let sent = relay.publish(&self.transport, prepared).await;
        let mut tx = lock.fenced(self.settings.role.as_deref()).await?;
        let outcome = relay.record(&mut tx, sent).await?;
        tx.commit().await?;
        for (note, why) in &outcome.parked {
            tracing::error!(
                note,
                why,
                "sync: a note parked — it is planned again after a deploy"
            );
        }
        for (replica, why) in &outcome.failing {
            tracing::warn!(%replica, why, "sync: a replica could not be sent to — it waits, the others carry on");
        }
        Ok(outcome.done)
    }

    /// The relay, for as long as this process holds `lock`.
    async fn lead(&self, mut lock: Lock) {
        tracing::info!("sync: this process runs the relay");
        let (min, max) = self.settings.backoff;
        let mut backoff = min;
        // None: due now. Taking the lock is usually a deploy, which may have
        // fixed what was set aside.
        let (mut retried, mut pruned, mut watched) = (None, None, None);
        loop {
            if !lock.still_held().await {
                tracing::error!("sync: the relay's lock is gone — standing by");
                return;
            }
            if due(retried, self.settings.retry_every) {
                match self.retry(&mut lock).await {
                    Ok(()) => retried = Some(Instant::now()),
                    Err(e) => tracing::warn!("sync: trying again what was set aside failed: {e}"),
                }
            }
            if due(
                pruned,
                self.settings.prune_after.min(Duration::from_secs(60 * 60)),
            ) {
                self.prune().await;
                pruned = Some(Instant::now());
            }
            if due(watched, self.settings.watch_every) {
                self.watch().await;
                watched = Some(Instant::now());
            }
            match self.pass(&mut lock).await {
                Ok(done) => {
                    backoff = min;
                    if done == 0 {
                        tokio::time::sleep(self.settings.idle).await;
                    }
                }
                Err(e) => {
                    tracing::error!("sync: a pass failed, and runs again: {e}");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(max);
                }
            }
        }
    }

    /// The lock, once this process gets it.
    async fn lock(&self) -> Lock {
        loop {
            match Lock::take(&self.pool, self.settings.lock_key).await {
                Ok(Some(lock)) => return lock,
                Ok(None) => tracing::debug!("sync: another process runs the relay — standing by"),
                Err(e) => tracing::warn!("sync: the relay's lock could not be asked for: {e}"),
            }
            tokio::time::sleep(self.settings.standby).await;
        }
    }

    /// Plans the parked notes again, sends the quarantine again, and files
    /// the inbox again.
    async fn retry(&self, lock: &mut Lock) -> Result<(), Error> {
        let mut tx = lock.fenced(self.settings.role.as_deref()).await?;
        let unparked = relay::unpark(&mut tx).await?;
        let retried = relay::retry_quarantine(&mut tx).await?;
        tx.commit().await?;
        if unparked + retried > 0 {
            tracing::info!(unparked, retried, "sync: trying again what was set aside");
        }
        let waiting = {
            let mut tx = self.begin().await?;
            relay::inbox(&mut tx, self.settings.batch).await?
        };
        let mut filed = 0;
        for row in &waiting {
            let Some(known) = self.senders.known(row.replica_id).await? else {
                continue;
            };
            let mut tx = self.senders.filing(&known).await?;
            if relay::refile(&mut tx, self.codecs, row, &known).await? {
                filed += 1;
            }
            tx.commit().await?;
        }
        if filed > 0 {
            tracing::info!(filed, "sync: uploaded rows filed after all");
        }
        Ok(())
    }

    async fn prune(&self) {
        let pruned = async {
            let mut tx = self.begin().await?;
            let pruned = crate::plan::prune(&mut tx, self.settings.prune_after).await?;
            tx.commit().await?;
            Ok::<_, Error>(pruned)
        }
        .await;
        match pruned {
            Ok(0) => {}
            Ok(pruned) => tracing::info!(pruned, "sync: done notes pruned"),
            Err(e) => tracing::warn!("sync: pruning failed: {e}"),
        }
    }

    /// Logs whatever in the engine's health needs someone: each one means
    /// wait (the network) or deploy a fix (a bug), never repair data.
    async fn watch(&self) {
        let health = async {
            let mut tx = self.begin().await?;
            self.engine
                .health(&mut tx, crate::status::Scope::All, 5)
                .await
        }
        .await;
        let health = match health {
            Ok(health) => health,
            Err(e) => {
                tracing::warn!("sync: the engine's health could not be read: {e}");
                return;
            }
        };
        let waited = health
            .oldest
            .and_then(|at| (chrono::Utc::now() - at).to_std().ok());
        if waited.is_some_and(|age| age > self.settings.stuck_after) {
            tracing::error!(
                pending = health.pending,
                oldest_minutes = waited.map(|a| a.as_secs() / 60),
                "sync ALARM: the backlog is not draining"
            );
        }
        for note in &health.parked.first {
            tracing::error!(
                note = note.id,
                kind = note.kind,
                table = note.table,
                key = note.key,
                attempts = note.attempts,
                error = note.last_error,
                "sync ALARM: a parked note — deploy a fix; it is planned again on start"
            );
        }
        if health.quarantine.total > 0 {
            tracing::error!(
                rows = health.quarantine.total,
                "sync ALARM: rows in quarantine — deploy a fix; they are sent again on start"
            );
        }
        if health.inbox.total > 0 {
            tracing::error!(
                rows = health.inbox.total,
                "sync ALARM: uploaded rows not filed — deploy a fix; they are filed again on start"
            );
        }
        for replica in &health.failing.first {
            tracing::warn!(
                replica = %replica.replica,
                failures = replica.failures,
                error = replica.last_error,
                "sync: a replica keeps failing to be reached — it waits, the others carry on"
            );
        }
    }
}

/// A transaction that assumes `role`, when there is one.
async fn begin(pool: &PgPool, role: Option<&str>) -> Result<Transaction<'static, Postgres>, Error> {
    let mut tx = pool.begin().await?;
    if let Some(role) = role {
        sqlx::query(&format!("SET LOCAL ROLE {role}"))
            .execute(&mut *tx)
            .await?;
    }
    Ok(tx)
}

fn due(last: Option<Instant>, every: Duration) -> bool {
    last.is_none_or(|at| at.elapsed() >= every)
}

/// Proof that this process runs the relay: a connection holding the
/// advisory lock. Drop it, or lose the connection, and the lock is free.
struct Lock {
    conn: PgConnection,
}

impl Lock {
    async fn take(pool: &PgPool, key: i64) -> Result<Option<Lock>, Error> {
        let mut conn = pool.acquire().await?;
        let taken: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(key)
            .fetch_one(&mut *conn)
            .await?;
        Ok(taken.then(|| Lock {
            conn: conn.detach(),
        }))
    }

    /// Lets the lock go now, rather than when the connection's end reaches
    /// the database.
    async fn release(mut self, key: i64) {
        let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(key)
            .execute(&mut self.conn)
            .await;
    }

    /// A connection that still answers still holds its session's lock.
    async fn still_held(&mut self) -> bool {
        sqlx::query("SELECT 1")
            .execute(&mut self.conn)
            .await
            .is_ok()
    }

    /// A transaction on the lock's own connection: it commits only while this
    /// process holds the lock, so a relay that lost it cannot write over the
    /// one that took it.
    async fn fenced(&mut self, role: Option<&str>) -> Result<Transaction<'_, Postgres>, Error> {
        let mut tx = self.conn.begin().await?;
        if let Some(role) = role {
            sqlx::query(&format!("SET LOCAL ROLE {role}"))
                .execute(&mut *tx)
                .await?;
        }
        Ok(tx)
    }
}

#[cfg(test)]
#[path = "server_tests.rs"]
mod tests;
