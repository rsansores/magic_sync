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
use crate::plan::{Audience, Declared, Engine};
use crate::relay::{self, Relay};
use crate::row::{Key, Row, Setting};
use crate::run::Backoff;
use crate::wire::Codec;
use crate::{Conn, Error};
use sqlx::{Connection, PgConnection, PgPool, Postgres, Transaction};
use std::future::Future;
use std::num::{NonZeroU32, NonZeroU64};
use std::time::Duration;
use tokio::time::Instant;
use uuid::Uuid;

pub use crate::filing::Filed;
pub use crate::relay::Transport;
pub use crate::wish::{WishOf, Wished};

/// What the host knows about the replicas that send something up.
pub trait Senders: Send + Sync + 'static {
    /// What a codec stamps on an uploaded row: the `P` of [`Codec<P>`].
    type Known: Send + Sync + 'static;

    /// What the server knows about `replica`, of the audience named
    /// `audience`; `None` for one it does not know, whose messages are
    /// dropped with a log.
    ///
    /// # Errors
    ///
    /// Whatever kept the host from answering ([`Error::Host`], say). A
    /// transient one has the upload come again; any other keeps its rows in
    /// the inbox, filed again after a deploy.
    fn known(
        &self,
        audience: &str,
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

impl std::fmt::Debug for Anyone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Anyone").finish_non_exhaustive()
    }
}

impl Anyone {
    /// Every replica known, its uploads filed in a transaction of `pool`.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Anyone(pool)
    }
}

/// `audiences`, each with the engine's own tables after its codecs.
fn with_wishes<P: Sync + 'static>(audiences: Vec<Audience<P>>) -> Result<Vec<Audience<P>>, Error> {
    audiences
        .into_iter()
        .map(|a| {
            Ok(Audience {
                codecs: crate::wish::with_wishes(a.codecs)?,
                ..a
            })
        })
        .collect()
}

/// The advisory lock only the running relay holds ("SYNCENGN").
pub const LOCK_KEY: i64 = 0x5359_4e43_454e_474e;

impl Senders for Anyone {
    type Known = ();

    async fn known(&self, _audience: &str, _replica: Uuid) -> Result<Option<()>, Error> {
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
    /// Parts of a replica's uploads the server may keep, unable to file
    /// them, until a fix files them — and never past 64 MiB of them: past
    /// that its uploads fail as transient and come again, waiting on the
    /// replica instead, which is an alarm.
    pub inbox: NonZeroU32,
    /// The role every transaction of the engine's own assumes, when the
    /// database gives the connecting one less than the engine needs: its
    /// `sync` schema, and temporary tables, which the relay plans in.
    pub role: Option<String>,
    /// How long the relay waits when nothing is pending.
    pub idle: Duration,
    /// How long it waits after a failed pass.
    pub backoff: Backoff,
    /// How long one send may take before its replica counts as unreachable.
    pub send_timeout: Duration,
    /// Replicas sent to at once, each its messages in order: so many sends
    /// wait on the transport side by side.
    pub concurrency: NonZeroU32,
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
    /// How long a note is kept once it is done, and how long what a replica
    /// uploaded waits in the inbox for the host to know it again.
    pub prune_after: Duration,
    /// How often the engine's health is looked at, and what is wrong logged.
    pub watch_every: Duration,
    /// How long the oldest pending note may wait before that is an alarm.
    pub stuck_after: Duration,
    /// How long a row a replica wrote may wait to be filed before a status
    /// calls the replica behind (catching up): rows that flow are younger.
    /// Not zero. A replica's `Outbox::keepalive_every` stays well under it:
    /// a replica that writes nothing else says what waits only that often,
    /// so a row of it may be judged by a backlog that old.
    ///
    /// The relay keeps this and `silent_after_keepalives` in the database
    /// when it takes its lock (`sync.settings`), and every status reads them
    /// there: every process judges alike, as the relay last started says.
    pub behind_after: Duration,
    /// How many times the cadence a replica says it keeps
    /// (`Outbox::keepalive_every`) it may say nothing for before a status
    /// calls it silent — counted from when the relay took its lock, if it
    /// was heard before: no replica is silent for a time no server
    /// listened. A replica that says none never is.
    pub silent_after_keepalives: NonZeroU32,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            limits: Limits::default(),
            batch: NonZeroU32::new(500).expect("not zero"),
            outbox_messages: NonZeroU32::new(10_000).expect("not zero"),
            outbox_bytes: NonZeroU64::new(256 << 20).expect("not zero"),
            inbox: NonZeroU32::new(10_000).expect("not zero"),
            role: None,
            idle: Duration::from_secs(1),
            backoff: Backoff::new(Duration::from_secs(1), Duration::from_secs(60)),
            send_timeout: Duration::from_secs(30),
            concurrency: NonZeroU32::new(16).expect("not zero"),
            replica_backoff: Backoff::new(Duration::from_secs(2), Duration::from_secs(15 * 60)),
            unconfirmed_after: Duration::from_secs(60 * 60),
            standby: Duration::from_secs(30),
            retry_every: Duration::from_secs(60 * 60),
            prune_after: Duration::from_secs(30 * 24 * 60 * 60),
            watch_every: Duration::from_secs(5 * 60),
            stuck_after: Duration::from_secs(15 * 60),
            behind_after: Duration::from_secs(2 * 60),
            silent_after_keepalives: NonZeroU32::new(3).expect("not zero"),
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
            ("behind_after", self.behind_after),
        ];
        if let Some((what, _)) = zero.iter().find(|(_, d)| d.is_zero()) {
            return Err(Error::Misuse(format!(
                "`{what}` is zero: the server would spin, or give up on what it waits for at once"
            )));
        }
        Ok(())
    }

    /// What a status judges a replica by: past a century, `behind_after`
    /// is a century, which no row waits for.
    fn judging(&self) -> crate::status::Judging {
        let century = chrono::TimeDelta::days(36_525);
        crate::status::Judging {
            behind_after: chrono::TimeDelta::from_std(self.behind_after)
                .map_or(century, |d| d.min(century)),
            silent_after: i32::try_from(self.silent_after_keepalives.get()).unwrap_or(i32::MAX),
            started_at: None,
        }
    }
}

/// The server's side of sync, for the host to run: built once, run in a task
/// of its own, and handed every message a replica sends up.
///
/// ```no_run
/// use magic_sync::plan::{Audience, Replicas};
/// use magic_sync::run::server::{Anyone, Server, Settings, Transport};
/// use magic_sync::wire::Codec;
/// use magic_sync::Error;
/// use std::sync::Arc;
/// use uuid::Uuid;
///
/// /// The host's way to its replicas.
/// struct Queues;
///
/// impl Transport for Queues {
///     async fn send(&self, _audience: &str, register: Uuid, message: Vec<u8>, _id: &str) -> Result<(), Error> {
///         // hand `message` to `register`'s queue
///         Ok(())
///     }
/// }
///
/// # async fn start(pool: sqlx::PgPool, codecs: Vec<Codec<()>>, upload: (Uuid, Vec<u8>)) -> Result<(), Error> {
/// const REGISTERS: Replicas = Replicas::new("shop.registers", "registers").live("active");
///
/// let audiences = vec![Audience::new("registers", REGISTERS, codecs)];
/// let server = Arc::new(
///     Server::new(pool.clone(), audiences, Queues, Anyone::new(pool), Settings::default())
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
    /// Each beside its planner in the engine, its codecs with the engine's
    /// own tables after them.
    audiences: Vec<Audience<S::Known>>,
    transport: T,
    senders: S,
    settings: Settings,
}

impl<S: Senders, T: Transport> std::fmt::Debug for Server<S, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("engine", &self.engine)
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

impl<S: Senders, T: Transport> Server<S, T> {
    /// The server over a database its migrations set up: `audiences` are
    /// the kinds of replica it serves — where it finds each kind, and the
    /// tables they hold, parents first; `transport` how a message reaches a
    /// replica; `senders` what the host knows of a replica that sends
    /// something up. It writes nothing: a process that only reads a status,
    /// or files what replicas send, builds one too, and what a status
    /// judges by is written by the relay, when [`Server::run`] takes its
    /// lock.
    ///
    /// # Errors
    ///
    /// [`Error::Misuse`] when the database is not what the engine needs —
    /// its schema at another version, a table that is not its row column for
    /// column, a trigger missing or different from what [`Engine::triggers`]
    /// prints, an index [`Engine::indexes`] prints — or for a declaration the schema cannot honour,
    /// two audiences of one name, or settings the server cannot run with;
    /// [`Error::Database`] when the database fails.
    pub async fn new(
        pool: PgPool,
        audiences: Vec<Audience<S::Known>>,
        transport: T,
        senders: S,
        settings: Settings,
    ) -> Result<Self, Error> {
        settings.check()?;
        let audiences = with_wishes(audiences)?;
        let mut tx = begin(&pool, "BEGIN", settings.role.as_deref()).await?;
        let engine = Engine::new(&mut tx, &Declared::of(&audiences)).await?;
        tx.commit().await?;
        Ok(Self::with(
            pool, engine, audiences, transport, senders, settings,
        ))
    }

    /// [`Server::new`], having first brought the database to what it needs,
    /// for a host with no migrations of its own: in one transaction, and one
    /// process at a time, the engine's schema installed or upgraded and every
    /// trigger missing or different written (any of the engine's on a table
    /// no longer declared dropped), and every index the planner needs that
    /// none serves created. A table mirrored for the first time that
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
        audiences: Vec<Audience<S::Known>>,
        transport: T,
        senders: S,
        settings: Settings,
    ) -> Result<Self, Error> {
        settings.check()?;
        let audiences = with_wishes(audiences)?;
        let mut tx = begin(&pool, "BEGIN", settings.role.as_deref()).await?;
        let engine = Engine::install(&mut tx, &Declared::of(&audiences)).await?;
        tx.commit().await?;
        Ok(Self::with(
            pool, engine, audiences, transport, senders, settings,
        ))
    }

    fn with(
        pool: PgPool,
        engine: Engine,
        audiences: Vec<Audience<S::Known>>,
        transport: T,
        senders: S,
        settings: Settings,
    ) -> Self {
        Server {
            pool,
            engine,
            audiences,
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

    /// Wishes the `#[row(desired)]` columns of `wanted` on the replica its
    /// origin names, in the caller's transaction, so the wish commits with
    /// whatever else the host writes beside it: it replaces any wish of that
    /// row, under a version of its own, and goes to the replica with the next
    /// pass. The replica carries it out through its handler for the table,
    /// and answers. With `apply = "if_unchanged"`, `wanted`'s version is the
    /// one the caller saw — the row as the server holds it, its desired
    /// columns set — and the replica carries the wish out only while it still
    /// holds that version; with `always`, its other columns are not read.
    ///
    /// `note` is the host's to say who asked, or why: the replica's handler
    /// is handed it. Wishing the same values again is a new wish, carried
    /// out again. A wish of a replica that is not live waits for it.
    ///
    /// # Errors
    ///
    /// [`Error::Misuse`] when `R` is not one of the server's tables, or a row
    /// that does not encode; [`Error::Database`] when the database fails.
    pub async fn desire<R: Setting>(
        &self,
        conn: &mut Conn,
        wanted: R,
        note: Option<&str>,
    ) -> Result<Wished, Error> {
        let codecs: Vec<&[Codec<S::Known>]> =
            self.audiences.iter().map(|a| a.codecs.as_slice()).collect();
        crate::wish::desire(conn, &self.engine, &codecs, wanted, note).await
    }

    /// Withdraws the wish of the setting row `key`, answered or not, in the
    /// caller's transaction: the replica lets go of it, and carries it out no
    /// more if it has not. Answers whether there was one.
    ///
    /// # Errors
    ///
    /// [`Error::Database`] when the database fails.
    pub async fn withdraw<R: Setting>(&self, conn: &mut Conn, key: &Key) -> Result<bool, Error> {
        crate::wish::withdraw::<R, _>(conn, self.holding::<R>(), key).await
    }

    /// The wishes of the setting rows `keys` — those that have one — with
    /// where each stands.
    ///
    /// # Errors
    ///
    /// [`Error::Misuse`] when `R` is not one of the server's tables;
    /// [`Error::Malformed`] for a wish that no longer decodes as `R`;
    /// [`Error::Database`] when the database fails.
    pub async fn wishes<R: Setting>(
        &self,
        conn: &mut Conn,
        keys: &[Key],
    ) -> Result<Vec<WishOf<R>>, Error> {
        crate::wish::wishes(conn, self.holding::<R>(), keys).await
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
    /// `shutdown` cuts short runs again, as after a crash. Once it holds the
    /// lock it keeps what a status judges by — `Settings::behind_after`,
    /// `Settings::silent_after_keepalives`, and now as when a server started
    /// listening — for every process to read.
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
    /// bytes would fail the same way — so its rows are kept in the inbox,
    /// what it says of its replica is filed, it is answered, and the rows
    /// are filed again after a deploy.
    ///
    /// Answers what was filed, per table, for the host to act on.
    ///
    /// # Errors
    ///
    /// Only an error that is [transient](Error::is_transient): the database,
    /// the host ([`Senders`]) or the transport away, or [`Error::Protocol`].
    ///
    /// An upload beyond the settings' [`Limits`] — more bytes, or more rows
    /// and rejections together — breaks the protocol: it is dropped unanswered, with an
    /// error logged. A replica's limits are the server's, or less.
    pub async fn receive(&self, replica: Uuid, bytes: &[u8]) -> Result<Vec<filing::Filed>, Error> {
        use prost::Message as _;
        let limits = self.settings.limits;
        if bytes.len() > limits.bytes() {
            tracing::error!(%replica, bytes = bytes.len(), limit = limits.bytes(), "sync: an upload larger than the server's limits — dropped, unanswered");
            return Ok(Vec::new());
        }
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
        // A rejection names a row: it counts as one.
        let rows: usize = up
            .tables
            .iter()
            .map(|t| t.upserts.len() + t.deletes.len())
            .sum::<usize>()
            + up.rejected.len();
        if rows > limits.rows() {
            tracing::error!(%replica, rows, rejected = up.rejected.len(), limit = limits.rows(), "sync: an upload of more rows and rejections than the server's limits — dropped, unanswered");
            return Ok(Vec::new());
        }
        let audience = {
            let mut tx = self.begin().await?;
            let audience = self.engine.audience_of(&mut tx, replica).await?;
            tx.commit().await?;
            audience
        };
        let Some(audience) = audience else {
            tracing::warn!(%replica, "sync: an upload from no replica this server knows — dropped");
            return Ok(Vec::new());
        };
        let codecs = &self.audiences[audience].codecs;
        let name = self.audiences[audience].name;
        let filed = match self.file(audience, replica, &up).await {
            Ok(Some(filed)) => filed,
            Ok(None) => {
                tracing::warn!(%replica, "sync: an upload from no replica this server knows — dropped");
                return Ok(Vec::new());
            }
            Err(e) if e.is_transient() => return Err(e),
            Err(e) => {
                let mut tx = self.begin().await?;
                if self.engine.audience_of(&mut tx, replica).await? == Some(audience) {
                    filing::file_failed(
                        &mut tx,
                        codecs,
                        &up,
                        replica,
                        &e.to_string(),
                        self.inbox(),
                    )
                    .await?;
                } else {
                    filing::keep_whole(&mut tx, codecs, replica, &up, &e.to_string(), self.inbox())
                        .await?;
                }
                tx.commit().await?;
                Vec::new()
            }
        };
        if let Some(last) = up.uploads.iter().max() {
            let answer = Body::Filed(envelope::Filed {
                uploads: up.uploads.clone(),
            });
            let id = format!("filed-{replica}-{last}");
            let sent = tokio::time::timeout(
                self.settings.send_timeout,
                self.transport.send(name, replica, answer.encoded(), &id),
            )
            .await
            .unwrap_or_else(|_| {
                Err(Error::transport(
                    true,
                    format!("no answer within {:?}", self.settings.send_timeout),
                ))
            });
            match sent {
                Err(e) if e.is_transient() => return Err(e),
                Err(e) => {
                    tracing::error!(%replica, "sync: the transport refuses an upload's answer — the replica sends it again: {e}")
                }
                Ok(()) => {}
            }
        }
        Ok(filed)
    }

    /// Files `up` from `replica`, of the audience at `audience`, by its
    /// tables, in the transaction the host gives for it; `None` for a
    /// replica the host does not know, or that is no row of the audience's
    /// replicas any more.
    async fn file(
        &self,
        audience: usize,
        replica: Uuid,
        up: &Up,
    ) -> Result<Option<Vec<filing::Filed>>, Error> {
        let of = &self.audiences[audience];
        let Some(known) = self.senders.known(of.name, replica).await? else {
            return Ok(None);
        };
        let mut tx = self.senders.filing(&known).await?;
        if !self.engine.audiences[audience]
            .is_replica(&mut tx, replica)
            .await?
        {
            return Ok(None);
        }
        let filed = filing::file(&mut tx, &of.codecs, up, replica, &known, self.inbox()).await?;
        tx.commit().await?;
        Ok(Some(filed))
    }

    /// The codecs of the first audience that holds `R`'s table; none when
    /// none does.
    fn holding<R: Row>(&self) -> &[Codec<S::Known>] {
        self.audiences
            .iter()
            .find(|a| a.codecs.iter().any(|c| c.table.name == R::TABLE))
            .map_or(&[], |a| a.codecs.as_slice())
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

    fn inbox(&self) -> i64 {
        i64::from(self.settings.inbox.get())
    }

    fn relay(&self) -> Relay<'_, S::Known> {
        Relay {
            engine: &self.engine,
            codecs: self.audiences.iter().map(|a| a.codecs.as_slice()).collect(),
            limits: self.settings.limits,
            batch: i64::from(self.settings.batch.get()),
            timeout: self.settings.send_timeout,
            backoff: self.settings.replica_backoff,
            bound: relay::Bound {
                messages: i64::from(self.settings.outbox_messages.get()),
                bytes: i64::try_from(self.settings.outbox_bytes.get()).unwrap_or(i64::MAX),
            },
            concurrency: usize::try_from(self.settings.concurrency.get()).unwrap_or(usize::MAX),
            buffered: relay::BUFFERED,
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
        let mut refiling = None;
        let mut silenced = Silenced::default();
        let mut judging_kept = false;
        loop {
            if !lock.still_held().await {
                tracing::error!("sync: the relay's lock is gone — standing by");
                return;
            }
            // Before the first watch: until it is kept, a replica would be
            // judged silent for the time no server listened to it.
            if !judging_kept {
                judging_kept = self.keep_judging(&mut lock).await;
            }
            if Instant::now() >= retry_at {
                retry_at = Instant::now()
                    + match self.retry(&mut lock).await {
                        Ok(walk) => {
                            refiling = Some(walk);
                            self.settings.retry_every
                        }
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
            if judging_kept && Instant::now() >= watch_at {
                self.watch(&mut lock, &mut silenced).await;
                watch_at = Instant::now() + self.settings.watch_every;
            }
            if let Some(walk) = refiling {
                refiling = match self.refile_page(walk).await {
                    Ok(next) => next,
                    Err(e) => {
                        tracing::warn!("sync: filing the inbox again failed, and goes on: {e}");
                        Some(walk)
                    }
                };
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

    /// Keeps what a status judges by, as the relay holding `lock`; answers
    /// whether it did.
    async fn keep_judging(&self, lock: &mut Lock) -> bool {
        let kept = async {
            let mut tx = lock.fenced("BEGIN", self.settings.role.as_deref()).await?;
            self.settings.judging().keep(&mut tx).await?;
            tx.commit().await?;
            Ok::<_, Error>(())
        }
        .await;
        if let Err(e) = &kept {
            tracing::warn!("sync: keeping what a status judges by failed, and is tried again: {e}");
        }
        kept.is_ok()
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

    /// Plans the parked notes again and sends the quarantine again — once
    /// what the engine declares is observed, so nothing of a table it no
    /// longer mirrors is; answers the walk that files the inbox again, a
    /// page at a time beside the passes, up to what it holds now.
    async fn retry(&self, lock: &mut Lock) -> Result<Refiling, Error> {
        let mut tx = lock.fenced("BEGIN", self.settings.role.as_deref()).await?;
        self.engine.observe(&mut tx).await?;
        let unparked = relay::unpark(&mut tx).await?;
        let retried = relay::retry_quarantine(&mut tx).await?;
        tx.commit().await?;
        if unparked + retried > 0 {
            tracing::info!(unparked, retried, "sync: trying again what was set aside");
        }
        Ok(Refiling {
            after: 0,
            through: filing::newest(&mut *self.begin().await?).await?,
            filed: 0,
        })
    }

    /// Files again the next page of the inbox `walk` goes over: an upload
    /// that fails again holds back no other. Answers the rest of the walk,
    /// `None` once it is over.
    async fn refile_page(&self, mut walk: Refiling) -> Result<Option<Refiling>, Error> {
        let kept = filing::inbox(
            &mut *self.begin().await?,
            walk.after,
            walk.through,
            i64::from(self.settings.batch.get()),
        )
        .await?;
        let Some(last) = kept.last() else {
            if walk.filed > 0 {
                tracing::info!(filed = walk.filed, "sync: uploads filed after all");
            }
            return Ok(None);
        };
        for kept in &kept {
            match self.refile(kept).await {
                Ok(all) => walk.filed += usize::from(all),
                Err(e) if e.is_transient() && !matches!(e, Error::Protocol { .. }) => {
                    return Err(e)
                }
                Err(e) => {
                    let mut tx = self.begin().await?;
                    filing::failed_again(&mut tx, kept, &e.to_string()).await?;
                    tx.commit().await?;
                }
            }
            walk.after = kept.id;
        }
        walk.after = last.id;
        Ok(Some(walk))
    }

    /// Files `kept` again, in the transaction the host gives for its
    /// replica; answers whether all of it was filed. One from a sender the
    /// host no longer knows, or that is no replica any more, waits — and is
    /// dropped, an alarm, once it waited `prune_after`.
    async fn refile(&self, kept: &filing::Kept) -> Result<bool, Error> {
        let audience = {
            let mut tx = self.begin().await?;
            let audience = self.engine.audience_of(&mut tx, kept.replica_id).await?;
            tx.commit().await?;
            audience
        };
        let known = match audience {
            Some(of) => {
                self.senders
                    .known(self.audiences[of].name, kept.replica_id)
                    .await?
            }
            None => None,
        };
        let mut tx = match &known {
            Some(known) => self.senders.filing(known).await?,
            None => self.begin().await?,
        };
        let replica = match audience {
            Some(of) => {
                self.engine.audiences[of]
                    .is_replica(&mut tx, kept.replica_id)
                    .await?
            }
            None => false,
        };
        let (Some(known), Some(of), true) = (known, audience, replica) else {
            if filing::forget(&mut tx, kept, self.settings.prune_after).await? {
                tracing::error!(replica = %kept.replica_id, "sync ALARM: what a sender no longer known uploaded waited too long to be filed — dropped");
            }
            tx.commit().await?;
            return Ok(false);
        };
        let all = filing::refile(&mut tx, &self.audiences[of].codecs, kept, &known).await?;
        tx.commit().await?;
        Ok(all)
    }

    async fn prune(&self) {
        let pruned = async {
            let mut pruned = 0;
            loop {
                let mut tx = self.begin().await?;
                let now =
                    crate::plan::prune(&mut tx, self.settings.prune_after, PRUNED_AT_ONCE).await?;
                tx.commit().await?;
                pruned += now;
                if now < PRUNED_AT_ONCE.unsigned_abs() {
                    return Ok::<_, Error>(pruned);
                }
            }
        }
        .await;
        let swept = async { crate::relay::sweep_bodies(&mut *self.begin().await?).await }.await;
        if let Err(e) = swept {
            tracing::warn!("sync: sweeping the outbox's bodies failed: {e}");
        }
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
    async fn watch(&self, lock: &mut Lock, silenced: &mut Silenced) {
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
            let health = self
                .engine
                .health(&mut tx, crate::status::Scope::All, 5)
                .await?;
            let silent = self
                .engine
                .silent(&mut tx, crate::status::Scope::All)
                .await?;
            Ok::<_, Error>((health, silent))
        }
        .await;
        let (health, silent) = match health {
            Ok(read) => read,
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
        let (fell, more) = silenced.since(&silent);
        for replica in fell {
            tracing::warn!(
                replica = %replica.replica,
                last_heard_at = %replica.last_heard_at,
                "sync: a replica has said nothing for longer than it said it would — off, or cut off"
            );
        }
        if more > 0 {
            tracing::warn!(
                more,
                silent = silent.len(),
                "sync: more replicas fell silent than are named"
            );
        }
    }
}

/// The silent replicas the watch has seen, so that it warns of each as it
/// falls silent, not on every pass for as long as it stays so.
#[derive(Debug, Default)]
struct Silenced {
    seen: std::collections::HashSet<Uuid>,
}

impl Silenced {
    /// How many newly silent replicas the watch names on one pass.
    const NAMED: usize = 5;

    /// Of every silent replica, the ones that were not the last time, up to
    /// [`Silenced::NAMED`] of them; and how many more there are.
    fn since<'a>(
        &mut self,
        silent: &'a [crate::status::Silent],
    ) -> (Vec<&'a crate::status::Silent>, usize) {
        let fell: Vec<&crate::status::Silent> = silent
            .iter()
            .filter(|s| !self.seen.contains(&s.replica))
            .collect();
        self.seen = silent.iter().map(|s| s.replica).collect();
        let more = fell.len().saturating_sub(Self::NAMED);
        (fell.into_iter().take(Self::NAMED).collect(), more)
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

/// A walk over the inbox filing again what it kept: past the id `after`,
/// up to `through`, and how many uploads it filed whole so far.
#[derive(Clone, Copy, Debug)]
struct Refiling {
    after: i64,
    through: i64,
    filed: usize,
}

/// Done notes pruned a transaction at a time.
const PRUNED_AT_ONCE: i64 = 10_000;

/// Whether the server process the statement runs on holds the advisory lock
/// `$1`: a connection through a pooler in transaction mode may run each
/// transaction on another, which does not.
const HELD_HERE: &str = "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' \
       AND pid = pg_backend_pid() AND granted AND objsubid = 1 \
       AND classid = ($1::bigint >> 32)::oid AND objid = ($1::bigint & 4294967295)::oid)";

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
        sqlx::query_scalar::<_, bool>(HELD_HERE)
            .bind(LOCK_KEY)
            .fetch_one(&mut self.conn)
            .await
            .unwrap_or(false)
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
        let here: bool = sqlx::query_scalar(HELD_HERE)
            .bind(LOCK_KEY)
            .fetch_one(&mut *tx)
            .await?;
        if !here {
            return Err(Error::Misuse(
                "the relay's lock is not held by the server process its transaction runs on: \
                 a pooler in transaction mode hands each transaction to any server process. \
                 The relay needs session pooling, or a direct connection"
                    .to_owned(),
            ));
        }
        assume(&mut tx, role).await?;
        Ok(tx)
    }
}

#[cfg(test)]
#[path = "server_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "audience_tests.rs"]
mod audience_tests;

#[cfg(test)]
#[path = "routed_tests.rs"]
pub(crate) mod routed_tests;
