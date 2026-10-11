//! The server, run for the host: one relay at a time across every process
//! that runs it, passes until nothing is pending, what was set aside tried
//! again, and what replicas send up filed and answered.
//!
//! A host builds a [`Server`] once, calls [`Server::run`] in a task of its
//! own, and hands every message a replica sends up to [`Server::receive`].
//! What the host knows and the engine does not — who a replica is, and the
//! transaction its uploads are filed in — it says through [`Senders`].

use crate::envelope::{self, Body, Limits, Up};
use crate::filing;
use crate::plan::{Engine, Replicas};
use crate::relay::{self, Relay};
use crate::row::Table;
use crate::run::Backoff;
use crate::wire::Codec;
use crate::Error;
use sqlx::{Connection, PgConnection, PgPool, Postgres, Transaction};
use std::future::Future;
use std::num::{NonZeroU32, NonZeroU64};
use std::time::Duration;
use tokio::time::Instant;
use uuid::Uuid;

pub use crate::filing::Filed;
pub use crate::relay::Transport;

/// What the host knows about the replicas that send something up.
pub trait Senders: Send + Sync + 'static {
    /// What a codec stamps on an uploaded row: the `P` of [`Codec<P>`].
    type Known: Send + Sync + 'static;

    /// What the server knows about `replica`; `None` for one it does not know,
    /// whose messages are dropped with a log.
    ///
    /// # Errors
    ///
    /// Whatever kept the host from answering ([`Error::Host`], say). A
    /// transient one has the upload come again; any other keeps it whole in
    /// the inbox, filed again after a deploy.
    fn known(
        &self,
        replica: Uuid,
    ) -> impl Future<Output = Result<Option<Self::Known>, Error>> + Send;

    /// The transaction an upload from that replica is filed in: where the
    /// host says who is writing, when its database cares (a role, a setting its
    /// row policies read).
    ///
    /// # Errors
    ///
    /// As [`Senders::known`].
    fn filing(
        &self,
        known: &Self::Known,
    ) -> impl Future<Output = Result<Transaction<'static, Postgres>, Error>> + Send;
}

/// [`Senders`] for a host that knows nothing about its replicas: every one
/// is known, and its uploads are filed in a plain transaction of the pool —
/// so any replica that reaches the transport files rows as itself. A host
/// that authenticates its replicas says which it knows with [`Senders`].
pub struct Anyone(PgPool);

impl Anyone {
    /// Every replica known, its uploads filed in a transaction of `pool`.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Anyone(pool)
    }
}

/// The advisory lock only the running relay holds ("SYNCENGN").
pub const LOCK_KEY: i64 = 0x5359_4e43_454e_474e;

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
/// on a transport that takes half a megabyte per message; a host changes
/// what it needs of a `Settings::default()`.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Settings {
    /// How large a message the transport takes.
    pub limits: Limits,
    /// Notes planned per pass.
    pub batch: NonZeroU32,
    /// Messages a replica's outbox may hold — written for it and not yet
    /// taken by the transport, while it cannot be reached — before they are
    /// dropped and it is provisioned again instead, which is an alarm.
    pub outbox_messages: NonZeroU32,
    /// Bytes its outbox may hold, likewise.
    pub outbox_bytes: NonZeroU64,
    /// The role every transaction of the engine's own assumes, when the
    /// database gives the connecting one less than the engine needs.
    pub role: Option<String>,
    /// How long the relay waits when nothing is pending.
    pub idle: Duration,
    /// How long it waits after a failed pass.
    pub backoff: Backoff,
    /// How long one send may take before its replica counts as unreachable.
    pub send_timeout: Duration,
    /// How long a replica a send failed to waits before it is tried again.
    pub replica_backoff: Backoff,
    /// How long a replica may leave a message unconfirmed, with nothing sent
    /// to it since, before it is provisioned again: the messages may never
    /// have reached it.
    pub unconfirmed_after: Duration,
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
            batch: NonZeroU32::new(500).expect("not zero"),
            outbox_messages: NonZeroU32::new(10_000).expect("not zero"),
            outbox_bytes: NonZeroU64::new(256 << 20).expect("not zero"),
            role: None,
            idle: Duration::from_secs(1),
            backoff: Backoff::new(Duration::from_secs(1), Duration::from_secs(60)),
            send_timeout: Duration::from_secs(30),
            replica_backoff: Backoff::new(Duration::from_secs(2), Duration::from_secs(15 * 60)),
            unconfirmed_after: Duration::from_secs(60 * 60),
            standby: Duration::from_secs(30),
            retry_every: Duration::from_secs(60 * 60),
            prune_after: Duration::from_secs(30 * 24 * 60 * 60),
            watch_every: Duration::from_secs(5 * 60),
            stuck_after: Duration::from_secs(15 * 60),
        }
    }
}

impl Settings {
    /// Refuses settings the server cannot run with.
    fn check(&self) -> Result<(), Error> {
        self.backoff.check("backoff")?;
        self.replica_backoff.check("replica_backoff")?;
        let zero = [
            ("idle", self.idle),
            ("send_timeout", self.send_timeout),
            ("unconfirmed_after", self.unconfirmed_after),
            ("standby", self.standby),
            ("retry_every", self.retry_every),
            ("prune_after", self.prune_after),
            ("watch_every", self.watch_every),
            ("stuck_after", self.stuck_after),
        ];
        if let Some((what, _)) = zero.iter().find(|(_, d)| d.is_zero()) {
            return Err(Error::Misuse(format!(
                "`{what}` is zero: the server would spin, or give up on what it waits for at once"
            )));
        }
        Ok(())
    }
}

/// The server's side of sync, for the host to run: built once, run in a task
/// of its own, and handed every message a replica sends up.
///
/// ```no_run
/// use magic_sync::plan::Replicas;
/// use magic_sync::run::server::{Anyone, Settings, Transport};
/// use magic_sync::wire::Codec;
/// use magic_sync::{Error, Server};
/// use std::sync::Arc;
/// use uuid::Uuid;
///
/// /// The host's way to its replicas.
/// struct Queues;
///
/// impl Transport for Queues {
///     async fn send(&self, register: Uuid, message: Vec<u8>, _id: &str) -> Result<(), Error> {
///         // hand `message` to `register`'s queue
///         Ok(())
///     }
/// }
///
/// # async fn start(pool: sqlx::PgPool, codecs: Vec<Codec<()>>, upload: (Uuid, Vec<u8>)) -> Result<(), Error> {
/// const REGISTERS: Replicas = Replicas::relation("shop.registers").table("registers").live("active");
///
/// let server = Arc::new(
///     Server::new(pool.clone(), codecs, REGISTERS, Queues, Anyone::new(pool), Settings::default())
///         .await?,
/// );
/// let relay = server.clone();
/// tokio::spawn(async move { relay.run(std::future::pending()).await });
///
/// // Every message a register sends up, from whichever register sent it.
/// let (register, bytes) = upload;
/// server.receive(register, &bytes).await?;
/// # Ok(())
/// # }
/// ```
pub struct Server<S: Senders, T: Transport> {
    pool: PgPool,
    engine: Engine,
    codecs: Vec<Codec<S::Known>>,
    transport: T,
    senders: S,
    settings: Settings,
}

impl<S: Senders, T: Transport> Server<S, T> {
    /// The server over a database its migrations set up: `codecs` are the
    /// tables that travel, parents first; `replicas` where it finds the
    /// replicas; `transport` how a message reaches one; `senders` what the
    /// host knows of a replica that sends something up.
    ///
    /// # Errors
    ///
    /// [`Error::Misuse`] when the database is not what the engine needs —
    /// its schema at another version, a table that is not its row column for
    /// column, a trigger missing or different from what [`Engine::triggers`]
    /// prints — or for a declaration the schema cannot honour, or settings
    /// the server cannot run with; [`Error::Database`] when the database
    /// fails.
    pub async fn new(
        pool: PgPool,
        codecs: Vec<Codec<S::Known>>,
        replicas: Replicas,
        transport: T,
        senders: S,
        settings: Settings,
    ) -> Result<Self, Error> {
        settings.check()?;
        let tables: Vec<&Table> = codecs.iter().map(|c| &c.table).collect();
        let mut tx = begin(&pool, "BEGIN", settings.role.as_deref()).await?;
        let engine = Engine::new(&mut tx, &tables, replicas).await?;
        tx.rollback().await?;
        Ok(Self::with(
            pool, engine, codecs, transport, senders, settings,
        ))
    }

    /// [`Server::new`], having first brought the database to what it needs,
    /// for a host with no migrations of its own: in one transaction, and one
    /// process at a time, the engine's schema installed or upgraded and every
    /// trigger missing or different written (any of the engine's on a table
    /// no longer declared dropped). A table mirrored for the first time that
    /// already has rows is sent whole. Run it before anything writes a
    /// mirrored table — a write before its trigger is never noted — on a
    /// pool whose connections may create triggers.
    ///
    /// # Errors
    ///
    /// As [`Server::new`], and [`Error::Misuse`] when the engine's schema is
    /// at a later version than this build's. Whatever fails, nothing is
    /// installed.
    pub async fn install(
        pool: PgPool,
        codecs: Vec<Codec<S::Known>>,
        replicas: Replicas,
        transport: T,
        senders: S,
        settings: Settings,
    ) -> Result<Self, Error> {
        settings.check()?;
        let tables: Vec<&Table> = codecs.iter().map(|c| &c.table).collect();
        let mut tx = begin(&pool, "BEGIN", settings.role.as_deref()).await?;
        let engine = Engine::install(&mut tx, &tables, replicas).await?;
        tx.commit().await?;
        Ok(Self::with(
            pool, engine, codecs, transport, senders, settings,
        ))
    }

    fn with(
        pool: PgPool,
        engine: Engine,
        codecs: Vec<Codec<S::Known>>,
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

    /// The engine, for what a host asks of it: how far along the replicas
    /// are, where a row is ([`status`](crate::status)).
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// A transaction as the engine's own begin — assuming the settings'
    /// `role` — for the host to ask the engine things in.
    ///
    /// # Errors
    ///
    /// [`Error::Database`] when the database fails, or refuses the role.
    pub async fn begin(&self) -> Result<Transaction<'static, Postgres>, Error> {
        begin(&self.pool, "BEGIN", self.settings.role.as_deref()).await
    }

    /// Runs the relay until `shutdown` resolves: stands by while another
    /// process holds the lock, and runs passes while this one does. A pass
    /// `shutdown` cuts short runs again, as after a crash.
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
    /// Anything else that fails is no reason to send it again — the same
    /// bytes would fail the same way — so the upload is kept whole in the
    /// inbox, answered, and filed again after a deploy.
    ///
    /// Answers what was filed, per table, for the host to act on.
    ///
    /// # Errors
    ///
    /// Only an error that is [transient](Error::is_transient): the database,
    /// the host ([`Senders`]) or the transport away, or [`Error::Protocol`].
    pub async fn receive(&self, replica: Uuid, bytes: &[u8]) -> Result<Vec<filing::Filed>, Error> {
        use prost::Message as _;
        let up = match Up::decode(bytes) {
            Ok(up) => up,
            Err(e) => {
                tracing::error!(%replica, "sync: an upload that does not decode — dropped: {e}");
                return Ok(Vec::new());
            }
        };
        if up.protocol > envelope::PROTOCOL {
            return Err(Error::Protocol {
                theirs: up.protocol,
                ours: envelope::PROTOCOL,
            });
        }
        if up.protocol == 0 {
            tracing::error!(%replica, "sync: an upload that names no protocol — dropped");
            return Ok(Vec::new());
        }
        let filed = match self.file(replica, &up).await {
            Ok(Some(filed)) => filed,
            Ok(None) => {
                tracing::warn!(%replica, "sync: an upload from no replica this server knows — dropped");
                return Ok(Vec::new());
            }
            Err(e) if e.is_transient() => return Err(e),
            Err(e) => {
                let mut tx = self.begin().await?;
                filing::keep_whole(&mut tx, replica, bytes, &e.to_string()).await?;
                tx.commit().await?;
                Vec::new()
            }
        };
        if let Some(last) = up.uploads.iter().max() {
            let answer = Body::Filed(envelope::Filed {
                uploads: up.uploads.clone(),
            });
            let id = format!("filed-{replica}-{last}");
            match self.transport.send(replica, answer.encoded(), &id).await {
                Err(e) if e.is_transient() => return Err(e),
                Err(e) => {
                    tracing::error!(%replica, "sync: the transport refuses an upload's answer — the replica sends it again: {e}")
                }
                Ok(()) => {}
            }
        }
        Ok(filed)
    }

    /// Files `up` from `replica` in the transaction the host gives for it;
    /// `None` for a replica the host does not know.
    async fn file(&self, replica: Uuid, up: &Up) -> Result<Option<Vec<filing::Filed>>, Error> {
        let Some(known) = self.senders.known(replica).await? else {
            return Ok(None);
        };
        let mut tx = self.senders.filing(&known).await?;
        let filed = filing::file(&mut tx, &self.codecs, up, replica, &known).await?;
        tx.commit().await?;
        Ok(Some(filed))
    }

    /// One pass now, as the relay, for a host's tests or a tool: how many
    /// notes it planned and messages it sent, or `None` when another
    /// process runs the relay.
    ///
    /// # Errors
    ///
    /// What stopped the pass: the database or the transport away. What the
    /// pass sets aside is not an error.
    pub async fn pass_now(&self) -> Result<Option<usize>, Error> {
        let Some(mut lock) = Lock::take(&self.pool, LOCK_KEY).await? else {
            return Ok(None);
        };
        let done = self.pass(&mut lock).await;
        lock.release(LOCK_KEY).await;
        done.map(Some)
    }

    fn relay(&self) -> Relay<'_, S::Known> {
        Relay {
            engine: &self.engine,
            codecs: &self.codecs,
            limits: self.settings.limits,
            batch: i64::from(self.settings.batch.get()),
            timeout: self.settings.send_timeout,
            backoff: self.settings.replica_backoff,
            bound: relay::Bound {
                messages: i64::from(self.settings.outbox_messages.get()),
                bytes: i64::try_from(self.settings.outbox_bytes.get()).unwrap_or(i64::MAX),
            },
        }
    }

    /// One pass, as the relay holding `lock`: the notes planned and
    /// recorded with the messages they send, then every outbox sent. What
    /// was done: notes, and messages sent.
    async fn pass(&self, lock: &mut Lock) -> Result<usize, Error> {
        let relay = self.relay();
        let mut tx = lock.fenced("BEGIN", self.settings.role.as_deref()).await?;
        self.engine.observe(&mut tx).await?;
        tx.commit().await?;
        // One snapshot for the notes, the plan and the rows it loads, and
        // what it records commits only while this process is the relay.
        let mut tx = lock
            .fenced(
                "BEGIN ISOLATION LEVEL REPEATABLE READ",
                self.settings.role.as_deref(),
            )
            .await?;
        let outcome = relay.record(&mut tx).await?;
        tx.commit().await?;
        for (note, why) in &outcome.parked {
            tracing::error!(
                note,
                why,
                "sync: a note parked — it is planned again after a deploy"
            );
        }
        let published = relay.publish(&mut lock.conn, &self.transport).await?;
        for (replica, why) in &published.failing {
            tracing::warn!(%replica, why, "sync: a replica could not be sent to — it waits, the others carry on");
        }
        Ok(outcome.done + published.sent)
    }

    /// The relay, for as long as this process holds `lock`.
    async fn lead(&self, mut lock: Lock) {
        tracing::info!("sync: this process runs the relay");
        let mut backoff = self.settings.backoff.first;
        // Due now: taking the lock is usually a deploy, which may have fixed
        // what was set aside. A step that failed is due again after the
        // longest backoff, not on every pass.
        let now = Instant::now();
        let (mut retry_at, mut prune_at, mut watch_at) = (now, now, now);
        loop {
            if !lock.still_held().await {
                tracing::error!("sync: the relay's lock is gone — standing by");
                return;
            }
            if Instant::now() >= retry_at {
                retry_at = Instant::now()
                    + match self.retry(&mut lock).await {
                        Ok(()) => self.settings.retry_every,
                        Err(e) => {
                            tracing::warn!("sync: trying again what was set aside failed: {e}");
                            self.settings.backoff.max
                        }
                    };
            }
            if Instant::now() >= prune_at {
                self.prune().await;
                prune_at =
                    Instant::now() + self.settings.prune_after.min(Duration::from_secs(60 * 60));
            }
            if Instant::now() >= watch_at {
                self.watch(&mut lock).await;
                watch_at = Instant::now() + self.settings.watch_every;
            }
            match self.pass(&mut lock).await {
                Ok(done) => {
                    backoff = self.settings.backoff.first;
                    if done == 0 {
                        tokio::time::sleep(self.settings.idle).await;
                    }
                }
                Err(e) => {
                    tracing::error!("sync: a pass failed, and runs again: {e}");
                    tokio::time::sleep(backoff).await;
                    backoff = self.settings.backoff.after(backoff);
                }
            }
        }
    }

    /// The lock, once this process gets it.
    async fn lock(&self) -> Lock {
        loop {
            match Lock::take(&self.pool, LOCK_KEY).await {
                Ok(Some(lock)) => return lock,
                Ok(None) => tracing::debug!("sync: another process runs the relay — standing by"),
                Err(e) => tracing::warn!("sync: the relay's lock could not be asked for: {e}"),
            }
            tokio::time::sleep(self.settings.standby).await;
        }
    }

    /// Plans the parked notes again, sends the quarantine again, and files
    /// the inbox again, all of it: an upload that fails again holds back no
    /// other.
    async fn retry(&self, lock: &mut Lock) -> Result<(), Error> {
        let mut tx = lock.fenced("BEGIN", self.settings.role.as_deref()).await?;
        let unparked = relay::unpark(&mut tx).await?;
        let retried = relay::retry_quarantine(&mut tx).await?;
        tx.commit().await?;
        if unparked + retried > 0 {
            tracing::info!(unparked, retried, "sync: trying again what was set aside");
        }
        let through = filing::newest(&mut *self.begin().await?).await?;
        let (mut after, mut filed) = (0, 0);
        loop {
            let kept = filing::inbox(
                &mut *self.begin().await?,
                after,
                through,
                i64::from(self.settings.batch.get()),
            )
            .await?;
            let Some(last) = kept.last() else {
                break;
            };
            after = last.id;
            for kept in &kept {
                match self.refile(kept).await {
                    Ok(all) => filed += usize::from(all),
                    Err(e) if e.is_transient() && !matches!(e, Error::Protocol { .. }) => {
                        return Err(e)
                    }
                    Err(e) => {
                        let mut tx = self.begin().await?;
                        filing::failed_again(&mut tx, kept, &e.to_string()).await?;
                        tx.commit().await?;
                    }
                }
            }
        }
        if filed > 0 {
            tracing::info!(filed, "sync: uploads filed after all");
        }
        Ok(())
    }

    /// Files `kept` again, in the transaction the host gives for its
    /// replica; answers whether all of it was filed. One whose replica the
    /// host no longer knows waits.
    async fn refile(&self, kept: &filing::Kept) -> Result<bool, Error> {
        let Some(known) = self.senders.known(kept.replica_id).await? else {
            return Ok(false);
        };
        let mut tx = self.senders.filing(&known).await?;
        let all = filing::refile(&mut tx, &self.codecs, kept, &known).await?;
        tx.commit().await?;
        Ok(all)
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

    /// Provisions again the replicas that have left messages unconfirmed
    /// too long ([`relay::heal`]), and logs whatever in the engine's health
    /// needs someone: each one means wait (the network) or deploy a fix (a
    /// bug), never repair data.
    async fn watch(&self, lock: &mut Lock) {
        let healed = async {
            let mut tx = lock.fenced("BEGIN", self.settings.role.as_deref()).await?;
            let healed =
                relay::heal(&mut tx, &self.engine, self.settings.unconfirmed_after).await?;
            tx.commit().await?;
            Ok::<_, Error>(healed)
        }
        .await;
        match healed {
            Ok(0) => {}
            Ok(healed) => tracing::warn!(
                healed,
                "sync: replicas silent on what they were sent — provisioned again"
            ),
            Err(e) => tracing::warn!("sync: provisioning again the silent replicas failed: {e}"),
        }
        let health = async {
            let mut tx = self.begin().await?;
            sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                .execute(&mut *tx)
                .await?;
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
                kind = ?note.kind,
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

/// A transaction begun with `statement`, that assumes `role` when there is one.
async fn begin(
    pool: &PgPool,
    statement: &'static str,
    role: Option<&str>,
) -> Result<Transaction<'static, Postgres>, Error> {
    let mut tx = pool.begin_with(statement).await?;
    assume(&mut tx, role).await?;
    Ok(tx)
}

async fn assume(conn: &mut PgConnection, role: Option<&str>) -> Result<(), Error> {
    if let Some(role) = role {
        let quoted = format!("\"{}\"", role.replace('"', "\"\""));
        sqlx::query(&format!("SET LOCAL ROLE {quoted}"))
            .execute(&mut *conn)
            .await?;
    }
    Ok(())
}

/// Proof that this process runs the relay: a connection holding the
/// advisory lock. Drop it, or lose the connection, and the lock is free.
struct Lock {
    conn: PgConnection,
}

impl Lock {
    async fn take(pool: &PgPool, key: i64) -> Result<Option<Lock>, Error> {
        // Detached before it asks: dropped mid-question, the connection
        // closes and lets go of a lock it may have got, rather than going
        // back to the pool holding it.
        let mut conn = pool.acquire().await?.detach();
        let taken: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(key)
            .fetch_one(&mut conn)
            .await?;
        Ok(taken.then_some(Lock { conn }))
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
    async fn fenced(
        &mut self,
        statement: &'static str,
        role: Option<&str>,
    ) -> Result<Transaction<'_, Postgres>, Error> {
        let mut tx = self.conn.begin_with(statement).await?;
        assume(&mut tx, role).await?;
        Ok(tx)
    }
}

#[cfg(test)]
#[path = "server_tests.rs"]
mod tests;
