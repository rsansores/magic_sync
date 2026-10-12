//! Wishes: what the server wants a replica's setting to hold.
//!
//! A setting (`mirror = "setting"`) is a row a replica owns and acts on — a
//! module's configuration, a screen's brightness — and goes up as a record
//! does. The server may wish its `#[row(desired)]` columns other values: one
//! wish per setting row, which the next replaces. The replica carries it out
//! through the host's handler for the table, once per wish, and answers
//! `applied`, `refused`, `superseded` (the setting changed on the replica
//! since the wish was based on it) or, until it can, `waiting`. Once the
//! system settles, the server and the replica show the same value of every
//! setting, and the same answer to every wish.
//!
//! A wish travels as a row of the engine's own table `sync_wishes`, written
//! on both sides: the server owns what is wished, the replica its answer. So
//! it is recorded, sent, confirmed, provisioned again and fingerprinted as
//! every row is, and a replica that predates wishes rejects it as a table it
//! does not know, which shows as a row set aside for it.

use crate::envelope::{self, Key as WireKey};
use crate::wire::{Codec, MapError};
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// The engine's table of wishes, as both sides hold it.
pub(crate) const TABLE: &str = "sync_wishes";

/// A row of `sync_wishes`.
#[derive(sqlx::FromRow, Clone, Debug, PartialEq, crate::Row)]
#[row(
    crate = "crate",
    table = "sync_wishes",
    schema = "sync",
    mirror = "both",
    sync = "assigned(replica_id)",
    retired = "retired_at",
    unmirrored = "wished_at"
)]
pub(crate) struct WishRow {
    pub(crate) id: Uuid,
    pub(crate) replica_id: Uuid,
    pub(crate) relation: String,
    pub(crate) row_key: String,
    pub(crate) wanted: Vec<u8>,
    pub(crate) version: i64,
    pub(crate) if_unchanged: bool,
    pub(crate) note: Option<String>,
    #[row(replica_owned)]
    pub(crate) answered: Option<i64>,
    #[row(replica_owned)]
    pub(crate) outcome: Option<String>,
    #[row(replica_owned)]
    pub(crate) reason: Option<String>,
    #[row(replica_owned)]
    pub(crate) answered_at: Option<DateTime<Utc>>,
}

/// The answers a replica gives, as they are written.
#[cfg(feature = "postgres")]
pub(crate) const OUTCOMES: [&str; 4] = ["applied", "refused", "superseded", "waiting"];

fn to_wire(row: WishRow) -> Result<envelope::Wish, MapError> {
    // The answer is the replica's: the server never sends one down, so a
    // replica that takes a wish it did not hold carries it out.
    #[cfg(feature = "postgres")]
    let (answered, outcome, reason, answered_at) = (0, String::new(), String::new(), String::new());
    #[cfg(feature = "sqlite")]
    let (answered, outcome, reason, answered_at) = (
        row.answered.map(u64::try_from).transpose()?.unwrap_or(0),
        row.outcome.unwrap_or_default(),
        row.reason.unwrap_or_default(),
        row.answered_at
            .map(|at| at.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
            .unwrap_or_default(),
    );
    Ok(envelope::Wish {
        id: row.id.to_string(),
        replica_id: row.replica_id.to_string(),
        table: row.relation,
        key: Some(WireKey {
            parts: row.row_key.split('|').map(str::to_owned).collect(),
        }),
        wanted: row.wanted,
        version: u64::try_from(row.version)?,
        if_unchanged: row.if_unchanged,
        note: row.note.unwrap_or_default(),
        answered,
        outcome,
        reason,
        answered_at,
    })
}

/// A number of the wire's that is 1 to 2^63 - 1, or none.
fn positive(number: u64, what: &str) -> Result<Option<i64>, MapError> {
    match number {
        0 => Ok(None),
        n => i64::try_from(n)
            .map(Some)
            .map_err(|_| MapError::new(format!("{what} {n} is past 2^63 - 1"))),
    }
}

/// A replica's answer, as the server reads it off a wish it sent up.
#[cfg(feature = "postgres")]
type Answer = (
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<DateTime<Utc>>,
);

#[cfg(feature = "postgres")]
fn answer_of(wish: &envelope::Wish) -> Result<Answer, MapError> {
    let outcome = (!wish.outcome.is_empty()).then(|| wish.outcome.clone());
    if let Some(outcome) = outcome.as_deref() {
        if !OUTCOMES.contains(&outcome) {
            return Err(MapError::new(format!(
                "an answer this build does not know: `{outcome}`"
            )));
        }
    }
    let answered_at = match wish.answered_at.as_str() {
        "" => None,
        at => Some(DateTime::parse_from_rfc3339(at)?.with_timezone(&Utc)),
    };
    Ok((
        positive(wish.answered, "an answer to version")?,
        outcome,
        (!wish.reason.is_empty()).then(|| wish.reason.clone()),
        answered_at,
    ))
}

fn from_wire(wish: envelope::Wish) -> Result<WishRow, MapError> {
    let parts = wish
        .key
        .as_ref()
        .map(|k| k.parts.clone())
        .unwrap_or_default();
    if parts.is_empty() {
        return Err(MapError::new("a wish of no row"));
    }
    let version =
        positive(wish.version, "a version")?.ok_or_else(|| MapError::new("a wish of version 0"))?;
    // A replica takes no answer from the server: its own are the only ones,
    // and a wish it did not hold is one it has not answered.
    #[cfg(feature = "postgres")]
    let (answered, outcome, reason, answered_at) = answer_of(&wish)?;
    #[cfg(feature = "sqlite")]
    let (answered, outcome, reason, answered_at) = (None, None, None, None);
    Ok(WishRow {
        id: wish.id.parse()?,
        replica_id: wish.replica_id.parse()?,
        relation: wish.table,
        row_key: parts.join("|"),
        wanted: wish.wanted,
        version,
        if_unchanged: wish.if_unchanged,
        note: (!wish.note.is_empty()).then_some(wish.note),
        answered,
        outcome,
        reason,
        answered_at,
    })
}

/// The codec of `sync_wishes`, which the engine adds to a host's tables
/// whenever one of them is a setting.
fn codec<P: Sync + 'static>() -> Codec<P> {
    Codec::new::<WishRow, envelope::Wish>(to_wire, from_wire, None)
}

/// `codecs`, with the engine's table of wishes after them when one of them
/// is a setting.
///
/// # Errors
///
/// [`crate::Error::Misuse`] for a host table named as the engine's.
pub(crate) fn with_wishes<P: Sync + 'static>(
    mut codecs: Vec<Codec<P>>,
) -> Result<Vec<Codec<P>>, crate::Error> {
    unclaimed(&codecs)?;
    codecs.extend(wishes_for(&codecs));
    Ok(codecs)
}

/// Refuses a host table named as the engine's table of wishes.
///
/// # Errors
///
/// [`crate::Error::Misuse`] for one.
pub(crate) fn unclaimed<P>(codecs: &[Codec<P>]) -> Result<(), crate::Error> {
    if codecs.iter().any(|c| c.table.name == TABLE) {
        return Err(crate::Error::Misuse(format!(
            "`{TABLE}` is the engine's table of wishes: a table of the host's needs another name"
        )));
    }
    Ok(())
}

/// The engine's table of wishes, when a host's `codecs` hold a setting.
pub(crate) fn wishes_for<P: Sync + 'static>(codecs: &[Codec<P>]) -> Option<Codec<P>> {
    codecs
        .iter()
        .any(|c| c.table.mirror == crate::row::Mirror::Setting)
        .then(codec)
}

#[cfg(feature = "postgres")]
pub use server::*;

#[cfg(feature = "postgres")]
mod server {
    use super::TABLE;
    use crate::row::{key_text, Key, Setting};
    use crate::status::WishState;
    use crate::wire::Codec;
    use crate::{Conn, Error};
    use chrono::{DateTime, Utc};
    use uuid::Uuid;

    /// A wish [`Server::desire`](crate::run::server::Server::desire) wrote.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    #[non_exhaustive]
    pub struct Wished {
        /// The wish: one per setting row.
        pub id: Uuid,
        /// This wish of it, drawn at random: the replica's answer names it.
        pub version: i64,
    }

    /// A setting row's wish, as the server holds it.
    #[derive(Clone, Debug)]
    #[non_exhaustive]
    pub struct WishOf<R> {
        /// The setting row's key.
        pub key: Key,
        /// The row as wished: its desired columns are the wish.
        pub wanted: R,
        /// Where the wish stands.
        pub state: WishState,
        /// Why, for an answer that says: refused, superseded or waiting.
        pub reason: Option<String>,
        /// What the host said of it when it wished it.
        pub note: Option<String>,
        /// When it was wished, by the server's clock.
        pub wished_at: DateTime<Utc>,
        /// When the replica answered this wish, by its clock.
        pub answered_at: Option<DateTime<Utc>>,
    }

    /// The id of the wish of the row `row_key` of `relation`: one per row, so
    /// wishing a row again replaces its wish.
    pub(crate) fn id_of(relation: &str, row_key: &str) -> Uuid {
        use sha2::Digest as _;
        let digest = sha2::Sha256::new()
            .chain_update(relation)
            .chain_update([0])
            .chain_update(row_key)
            .finalize();
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&digest[..16]);
        uuid::Builder::from_custom_bytes(bytes).into_uuid()
    }

    /// Where a wish stands, as SQL over `sync.sync_wishes w`: the replica's
    /// answer when it is of this wish, pending otherwise.
    pub(crate) const STATE: &str =
        "CASE WHEN w.answered IS NOT DISTINCT FROM w.version THEN coalesce(w.outcome, 'pending') \
              ELSE 'pending' END";

    pub(crate) fn state_of(text: &str) -> WishState {
        match text {
            "applied" => WishState::Applied,
            "refused" => WishState::Refused,
            "superseded" => WishState::Superseded,
            "waiting" => WishState::Waiting,
            _ => WishState::Pending,
        }
    }

    fn codec_of<R: Setting, P: Sync + 'static>(codecs: &[Codec<P>]) -> Result<&Codec<P>, Error> {
        codecs
            .iter()
            .find(|c| c.table.name == R::TABLE)
            .ok_or_else(|| {
                Error::Misuse(format!(
                    "`{}` is no table this server mirrors: list its codec",
                    R::TABLE
                ))
            })
    }

    /// The replica a setting row belongs to: its origin.
    fn replica_of<R: Setting>(key: &Key) -> Result<Uuid, Error> {
        let origin = R::ORIGIN.expect("a setting has an origin");
        let at = R::KEY
            .iter()
            .position(|k| *k == origin)
            .expect("a setting's origin is a key column");
        match key.get(at) {
            Some(crate::row::KeyPart::Uuid(replica)) => Ok(*replica),
            _ => Err(Error::Misuse(format!(
                "`{}`'s origin `{origin}` is not a uuid",
                R::TABLE
            ))),
        }
    }

    pub(crate) async fn desire<R: Setting, P: Sync + 'static>(
        conn: &mut Conn,
        codecs: &[Codec<P>],
        wanted: R,
        note: Option<&str>,
    ) -> Result<Wished, Error> {
        let codec = codec_of::<R, P>(codecs)?;
        let key = wanted.key();
        let replica = replica_of::<R>(&key)?;
        let row_key = key_text(&key);
        let id = id_of(R::TABLE, &row_key);
        let version = i64::try_from(crate::envelope::nonce()).expect("a nonce is below 2^63");
        let bytes = codec.encode_row(wanted)?;
        sqlx::query(&format!(
            "INSERT INTO sync.{TABLE} AS w \
                    (id, replica_id, relation, row_key, wanted, version, if_unchanged, note) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             ON CONFLICT (id) DO UPDATE SET wanted = EXCLUDED.wanted, version = EXCLUDED.version, \
                    if_unchanged = EXCLUDED.if_unchanged, note = EXCLUDED.note, wished_at = now()"
        ))
        .bind(id)
        .bind(replica)
        .bind(R::TABLE)
        .bind(&row_key)
        .bind(bytes)
        .bind(version)
        .bind(R::APPLY == crate::row::Apply::IfUnchanged)
        .bind(note)
        .execute(&mut *conn)
        .await?;
        Ok(Wished { id, version })
    }

    pub(crate) async fn withdraw<R: Setting>(conn: &mut Conn, key: &Key) -> Result<bool, Error> {
        let id = id_of(R::TABLE, &key_text(key));
        let gone = sqlx::query(&format!("DELETE FROM sync.{TABLE} WHERE id = $1"))
            .bind(id)
            .execute(&mut *conn)
            .await?
            .rows_affected();
        Ok(gone > 0)
    }

    /// One stored wish, before its row is decoded.
    type Stored = (
        Vec<u8>,
        String,
        Option<String>,
        Option<String>,
        DateTime<Utc>,
        Option<DateTime<Utc>>,
    );

    pub(crate) async fn wishes<R: Setting, P: Sync + 'static>(
        conn: &mut Conn,
        codecs: &[Codec<P>],
        keys: &[Key],
    ) -> Result<Vec<WishOf<R>>, Error> {
        let codec = codec_of::<R, P>(codecs)?;
        let texts: Vec<String> = keys.iter().map(key_text).collect();
        let stored: Vec<Stored> = sqlx::query_as(&format!(
            "SELECT w.wanted, {STATE}, \
                    CASE WHEN w.answered IS NOT DISTINCT FROM w.version THEN w.reason END, \
                    w.note, w.wished_at, \
                    CASE WHEN w.answered IS NOT DISTINCT FROM w.version THEN w.answered_at END \
               FROM sync.{TABLE} w \
              WHERE w.relation = $1 AND w.row_key = ANY($2) \
              ORDER BY w.row_key"
        ))
        .bind(R::TABLE)
        .bind(&texts)
        .fetch_all(&mut *conn)
        .await?;
        let mut out = Vec::with_capacity(stored.len());
        for (bytes, state, reason, note, wished_at, answered_at) in stored {
            let wanted: R = codec.decode_row(&bytes)?;
            out.push(WishOf {
                key: wanted.key(),
                wanted,
                state: state_of(&state),
                reason,
                note,
                wished_at,
                answered_at,
            });
        }
        Ok(out)
    }
}

#[cfg(feature = "sqlite")]
pub use replica::*;

#[cfg(feature = "sqlite")]
mod replica {
    use super::{WishRow, TABLE};
    use crate::row::{KeyPart, Setting};
    use crate::wire::Codec;
    use crate::{Conn, Error};
    use sqlx::SqlitePool;
    use std::any::{Any, TypeId};
    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use uuid::Uuid;

    /// What the server wishes one setting row to hold, handed to the
    /// replica's handler for the table.
    #[derive(Clone, Debug)]
    #[non_exhaustive]
    pub struct Wish<R> {
        /// The row as wished: its `#[row(desired)]` columns are the wish.
        pub wanted: R,
        /// The row as the replica holds it now.
        pub held: R,
        /// What the server's host said of the wish: who asked, why.
        pub note: Option<String>,
        /// This wish of the row, drawn at random by the server: a wish
        /// carried out again — after a crash before its answer was recorded,
        /// or by a replica restored from an older copy — has the same one.
        pub version: i64,
    }

    impl<R> Wish<R> {
        /// A wish of `held` to become `wanted`, as its handler is handed one:
        /// for a host's tests of its own handler.
        #[must_use]
        pub fn new(wanted: R, held: R, version: i64) -> Self {
            Wish {
                wanted,
                held,
                note: None,
                version,
            }
        }

        /// The wish, with what the server's host said of it.
        #[must_use]
        pub fn with_note(self, note: impl Into<String>) -> Self {
            Wish {
                note: Some(note.into()),
                ..self
            }
        }
    }

    /// What a replica's handler did with a wish.
    #[derive(Clone, Debug, PartialEq, Eq)]
    #[non_exhaustive]
    pub enum Outcome {
        /// The setting holds what was wished: the engine writes the desired
        /// columns onto the row, with its version moved on, in the
        /// transaction that records the answer — and both go up.
        Applied,
        /// It will not hold it, for this reason: final for this wish.
        Refused(String),
        /// It cannot carry it out now, for this reason: the wish waits, and
        /// is offered again — at [`Replica::offer_wishes`], after the next
        /// message that changes a wish, at start, and every
        /// `Settings::wishes_every` while one waits.
        ///
        /// [`Replica::offer_wishes`]: crate::run::replica::Replica::offer_wishes
        Later(String),
    }

    /// Carries a setting's wishes out: the host's, one per setting table,
    /// handed to the replica in [`Wishes`]. Called with no transaction open,
    /// in a task of its own — it may take its time, up to
    /// `Settings::wishes_timeout`, and talk to hardware — once per wish, and
    /// again only if the replica stopped before it recorded the answer, or
    /// was restored from an older copy: carrying the same wish out twice
    /// must leave the setting as once does. A handler that panics, or does
    /// not answer in time, has the wish wait, and offered again.
    ///
    /// It is handed only a wish of a row the replica holds — one of a row it
    /// does not hold yet waits for it — and, with `apply = "if_unchanged"`,
    /// only while the row is at the version the wish is based on. That is
    /// compared before the handler runs: a write of the row's own while it
    /// runs is overwritten with the wished columns when it answers
    /// [`Outcome::Applied`], whatever the row's other columns became.
    pub trait CarryOut<R: Setting>: Send + Sync + 'static {
        /// Carries `wish` out, or says why not.
        fn carry_out(&self, wish: Wish<R>) -> impl Future<Output = Outcome> + Send;
    }

    /// The handler of a setting that lives only in the database: what is
    /// wished is applied as it comes, and the engine writes it.
    #[derive(Clone, Copy, Debug, Default)]
    pub struct Direct;

    impl<R: Setting> CarryOut<R> for Direct {
        async fn carry_out(&self, _wish: Wish<R>) -> Outcome {
            Outcome::Applied
        }
    }

    type Fut<T> = Pin<Box<dyn Future<Output = T> + Send>>;

    /// How a wish's handler is run.
    #[derive(Clone)]
    pub(crate) struct Run {
        pub(crate) pool: SqlitePool,
        /// The replica, whose rows alone its wishes are of.
        pub(crate) me: Uuid,
        /// How long a handler has to answer.
        pub(crate) timeout: Duration,
    }

    /// A table's handler, its row type erased.
    trait Carrier: Send + Sync {
        /// The row type it carries wishes of out.
        fn row(&self) -> TypeId;
        /// Carries `wish` out — `wanted`, its row decoded — and records the
        /// answer.
        fn carry(
            self: Arc<Self>,
            run: Run,
            wanted: Box<dyn Any + Send>,
            wish: WishRow,
        ) -> Fut<Result<Offered, Error>>;
    }

    struct Typed<R, H> {
        handler: H,
        /// Outcomes its handler gave whose recording failed for a while:
        /// recorded again without running the handler again.
        given: Mutex<HashMap<(Uuid, i64), Outcome>>,
        row: std::marker::PhantomData<fn() -> R>,
    }

    /// The handlers of a replica's settings, one per setting table: every
    /// setting the replica holds needs one, which
    /// [`Replica::new`](crate::run::replica::Replica::new) checks.
    ///
    /// ```
    /// use magic_sync::wish::{CarryOut, Direct, Outcome, Wish, Wishes};
    /// # #[derive(sqlx::FromRow, Clone, magic_sync::Row)]
    /// # #[row(table = "screens", mirror = "setting", key = "replica_id, id", origin = "replica_id")]
    /// # pub struct Screen {
    /// #     pub replica_id: uuid::Uuid,
    /// #     pub id: uuid::Uuid,
    /// #     #[row(desired)]
    /// #     pub level: i64,
    /// #     #[row(version)]
    /// #     pub revision: i64,
    /// # }
    /// # #[derive(sqlx::FromRow, Clone, magic_sync::Row)]
    /// # #[row(table = "footers", mirror = "setting", key = "replica_id, id", origin = "replica_id")]
    /// # pub struct Footer {
    /// #     pub replica_id: uuid::Uuid,
    /// #     pub id: uuid::Uuid,
    /// #     #[row(desired)]
    /// #     pub text: String,
    /// #     #[row(version)]
    /// #     pub revision: i64,
    /// # }
    ///
    /// /// The screen's backlight, which the hardware has to take.
    /// struct Backlight;
    ///
    /// impl CarryOut<Screen> for Backlight {
    ///     async fn carry_out(&self, wish: Wish<Screen>) -> Outcome {
    ///         match set_backlight(wish.wanted.level) {
    ///             Ok(()) => Outcome::Applied,
    ///             Err(why) => Outcome::Later(why),
    ///         }
    ///     }
    /// }
    /// # fn set_backlight(_: i64) -> Result<(), String> { Ok(()) }
    ///
    /// // A receipt's footer lives only in the database: the engine writes it.
    /// let wishes = Wishes::new().on::<Screen>(Backlight).on::<Footer>(Direct);
    /// ```
    #[derive(Clone, Default)]
    pub struct Wishes {
        carriers: HashMap<&'static str, Arc<dyn Carrier>>,
    }

    impl std::fmt::Debug for Wishes {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let mut tables: Vec<_> = self.carriers.keys().collect();
            tables.sort_unstable();
            f.debug_struct("Wishes").field("tables", &tables).finish()
        }
    }

    /// What one round of offering did.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub(crate) struct Offered {
        /// Something was written: an answer, a setting.
        pub(crate) wrote: bool,
        /// A wish waits, to be offered again.
        pub(crate) waiting: bool,
    }

    impl Wishes {
        /// No handler yet.
        #[must_use]
        pub fn new() -> Self {
            Self::default()
        }

        /// `handler` carries out the wishes of the setting `R`, in place of
        /// one given before.
        #[must_use]
        pub fn on<R: Setting>(mut self, handler: impl CarryOut<R>) -> Self {
            self.carriers.insert(
                R::TABLE,
                Arc::new(Typed {
                    handler,
                    given: Mutex::new(HashMap::new()),
                    row: std::marker::PhantomData::<fn() -> R>,
                }),
            );
            self
        }

        /// Refuses handlers that do not meet `codecs`' settings one for one,
        /// each of the row type its table's codec decodes.
        pub(crate) fn check<P>(&self, codecs: &[Codec<P>]) -> Result<(), Error> {
            for codec in codecs {
                let setting = codec.table.mirror == crate::row::Mirror::Setting;
                match self.carriers.get(codec.table.name) {
                    None if setting => {
                        return Err(Error::Misuse(format!(
                            "`{}` is a setting, and nothing carries its wishes out: give it a \
                             handler in `Settings::wishes` (`Direct` for one that lives only in \
                             the database)",
                            codec.table.name
                        )))
                    }
                    Some(carrier) if setting && carrier.row() != codec.row => {
                        return Err(Error::Misuse(format!(
                            "the handler of `{}`'s wishes is for another row type than its \
                             codec's",
                            codec.table.name
                        )))
                    }
                    _ => {}
                }
            }
            for table in self.carriers.keys() {
                let setting = codecs.iter().any(|c| {
                    c.table.name == *table && c.table.mirror == crate::row::Mirror::Setting
                });
                if !setting {
                    return Err(Error::Misuse(format!(
                        "a handler of wishes for `{table}`, which is no setting this replica holds"
                    )));
                }
            }
            Ok(())
        }

        /// Offers every wish not carried out yet, or waiting, to its table's
        /// handler, one at a time, each read again just before: a wish
        /// replaced or withdrawn while another's handler ran is offered as it
        /// is by then, or not at all. Nothing while a snapshot is in
        /// progress, which may let go of the wishes it does not name: its
        /// end offers them. A wish that fails is logged, and the rest go on.
        pub(crate) async fn offer<P: Sync + 'static>(
            &self,
            run: &Run,
            codecs: &[Codec<P>],
        ) -> Result<Offered, Error> {
            let snapshot: Option<Option<String>> =
                sqlx::query_scalar("SELECT snapshot_id FROM sync_applied WHERE id = 1")
                    .fetch_optional(&run.pool)
                    .await?;
            if snapshot.flatten().is_some_and(|s| !s.is_empty()) {
                return Ok(Offered::default());
            }
            let mut offered = Offered::default();
            let mut after = (String::new(), String::new());
            loop {
                let next: Option<WishRow> = sqlx::query_as(&format!(
                    "SELECT id, replica_id, relation, row_key, wanted, version, if_unchanged, \
                            note, answered, outcome, reason, answered_at \
                       FROM {TABLE} \
                      WHERE retired_at IS NULL \
                        AND (answered IS NOT version OR outcome = 'waiting') \
                        AND (relation, row_key) > ($1, $2) \
                      ORDER BY relation, row_key LIMIT 1"
                ))
                .bind(&after.0)
                .bind(&after.1)
                .fetch_optional(&run.pool)
                .await?;
                let Some(wish) = next else {
                    return Ok(offered);
                };
                after = (wish.relation.clone(), wish.row_key.clone());
                let at = format!("{} {}", wish.relation, wish.row_key);
                match self.settle(run, codecs, wish).await {
                    Ok(settled) => {
                        offered.wrote |= settled.wrote;
                        offered.waiting |= settled.waiting;
                    }
                    Err(e) => {
                        offered.waiting = true;
                        tracing::warn!(wish = at, "sync: a wish not settled, offered again: {e}");
                    }
                }
            }
        }

        /// Carries one wish out, or answers why not.
        async fn settle<P: Sync + 'static>(
            &self,
            run: &Run,
            codecs: &[Codec<P>],
            wish: WishRow,
        ) -> Result<Offered, Error> {
            let codec = codecs.iter().find(|c| c.table.name == wish.relation);
            let (Some(carrier), Some(codec)) = (self.carriers.get(wish.relation.as_str()), codec)
            else {
                let why = format!(
                    "this build carries out no wish of `{}`: a build that holds it as a setting \
                     does",
                    wish.relation
                );
                return answer_alone(&run.pool, &wish, "waiting", Some(&why)).await;
            };
            // The key as the wire wrote it, read as the table's: in the one
            // form keys are compared in.
            let parts: Vec<String> = wish.row_key.split('|').map(str::to_owned).collect();
            let Ok(key) = codec.key(&parts) else {
                let why = "a key that is not the table's";
                return answer_alone(&run.pool, &wish, "refused", Some(why)).await;
            };
            let wish = WishRow {
                row_key: crate::row::key_text(&key),
                ..wish
            };
            match codec.decode_any(&wish.wanted) {
                Ok(wanted) => carrier.clone().carry(run.clone(), wanted, wish).await,
                Err(e) => {
                    let why = format!("this build cannot read the row wished: {e}");
                    answer_alone(&run.pool, &wish, "waiting", Some(&why)).await
                }
            }
        }
    }

    /// Records `outcome` as the answer to `wish` in `conn`'s transaction —
    /// while it is still the wish the replica holds, and only when the
    /// answer is new: a wish waiting again for the same reason sends nothing
    /// up. Answers whether it wrote it.
    async fn answer(
        conn: &mut Conn,
        wish: &WishRow,
        outcome: &str,
        reason: Option<&str>,
    ) -> Result<bool, Error> {
        let written = sqlx::query(&format!(
            "UPDATE {TABLE} SET answered = $2, outcome = $3, reason = $4, \
                    answered_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
              WHERE id = $1 AND version = $2 AND retired_at IS NULL \
                AND (answered IS NOT $2 OR outcome IS NOT $3 OR reason IS NOT $4)"
        ))
        .bind(wish.id)
        .bind(wish.version)
        .bind(outcome)
        .bind(reason)
        .execute(&mut *conn)
        .await?;
        Ok(written.rows_affected() > 0)
    }

    /// [`answer`], in a transaction of its own.
    async fn answer_alone(
        pool: &SqlitePool,
        wish: &WishRow,
        outcome: &str,
        reason: Option<&str>,
    ) -> Result<Offered, Error> {
        let mut conn = pool.acquire().await?;
        let mut tx = sqlx::Connection::begin_with(&mut *conn, "BEGIN IMMEDIATE").await?;
        let wrote = answer(&mut tx, wish, outcome, reason).await?;
        tx.commit().await?;
        Ok(Offered {
            wrote,
            waiting: outcome == "waiting",
        })
    }

    /// A task that is aborted when dropped: a handler cut short by shutdown
    /// does not run on behind it.
    struct Aborting<T>(tokio::task::JoinHandle<T>);

    impl<T> Drop for Aborting<T> {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    impl<R: Setting, H: CarryOut<R>> Typed<R, H> {
        /// The handler's outcome for the wish `id`, run in a task of its own
        /// — or the one it gave before, when only its recording failed.
        async fn outcome(self: &Arc<Self>, run: &Run, id: Uuid, wish: Wish<R>) -> Outcome {
            let key = (id, wish.version);
            if let Some(outcome) = self.given().get(&key).cloned() {
                return outcome;
            }
            let this = Arc::clone(self);
            let mut task = Aborting(tokio::spawn(
                async move { this.handler.carry_out(wish).await },
            ));
            match tokio::time::timeout(run.timeout, &mut task.0).await {
                Ok(Ok(outcome)) => outcome,
                Ok(Err(e)) if e.is_panic() => Outcome::Later("its handler panicked".to_owned()),
                Ok(Err(_)) => Outcome::Later("its handler was cancelled".to_owned()),
                Err(_) => Outcome::Later(format!(
                    "its handler did not answer within {:?}",
                    run.timeout
                )),
            }
        }

        fn given(&self) -> std::sync::MutexGuard<'_, HashMap<(Uuid, i64), Outcome>> {
            self.given
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }

        /// Records `outcome`, the answer to `wish` of the row `key`: an
        /// applied one written onto the row as it is now, in the same
        /// transaction. A write the database refuses is the row's answer:
        /// refused, with why.
        async fn record(
            &self,
            run: &Run,
            wish: &WishRow,
            key: &crate::row::Key,
            wanted: R,
            outcome: &Outcome,
        ) -> Result<Offered, Error> {
            let mut conn = run.pool.acquire().await?;
            let mut tx = sqlx::Connection::begin_with(&mut *conn, "BEGIN IMMEDIATE").await?;
            let (outcome, why) = match outcome {
                Outcome::Applied => {
                    let now = crate::row::get_by_keys::<R>(&mut tx, std::slice::from_ref(key))
                        .await?
                        .pop();
                    let Some(mut row) = now else {
                        drop(tx);
                        return answer_alone(&run.pool, wish, "waiting", Some(NO_ROW)).await;
                    };
                    let was = row.version_text();
                    row.take_desired(wanted);
                    row.raise_version();
                    if row.version_text() == was {
                        let why = format!("its version cannot move past {was}");
                        let wrote = answer(&mut tx, wish, "refused", Some(&why)).await?;
                        tx.commit().await?;
                        return Ok(Offered {
                            wrote,
                            waiting: false,
                        });
                    }
                    crate::row::savepoint(&mut tx).await?;
                    let written = crate::row::overwrite_except(&mut tx, vec![row], &[]).await;
                    if let Err(e) = crate::row::released(&mut tx, written).await? {
                        let why = format!("this replica's database refuses it: {e}");
                        let wrote = answer(&mut tx, wish, "refused", Some(&why)).await?;
                        tx.commit().await?;
                        return Ok(Offered {
                            wrote,
                            waiting: false,
                        });
                    }
                    ("applied", None)
                }
                Outcome::Refused(why) => ("refused", Some(why.as_str())),
                Outcome::Later(why) => ("waiting", Some(why.as_str())),
            };
            let answered = answer(&mut tx, wish, outcome, why).await?;
            tx.commit().await?;
            Ok(Offered {
                wrote: answered || outcome == "applied",
                waiting: outcome == "waiting",
            })
        }
    }

    /// Why a wish waits that is of a row the replica does not hold.
    const NO_ROW: &str = "this replica holds no such row yet";

    impl<R: Setting, H: CarryOut<R>> Carrier for Typed<R, H> {
        fn row(&self) -> TypeId {
            TypeId::of::<R>()
        }

        fn carry(
            self: Arc<Self>,
            run: Run,
            wanted: Box<dyn Any + Send>,
            wish: WishRow,
        ) -> Fut<Result<Offered, Error>> {
            Box::pin(async move { self.settle(&run, wanted, wish).await })
        }
    }

    impl<R: Setting, H: CarryOut<R>> Typed<R, H> {
        async fn settle(
            self: Arc<Self>,
            run: &Run,
            wanted: Box<dyn Any + Send>,
            wish: WishRow,
        ) -> Result<Offered, Error> {
            let wanted: R = *wanted
                .downcast::<R>()
                .expect("Replica::new checks a handler's row is its codec's");
            let key = wanted.key();
            let origin = R::ORIGIN.and_then(|o| R::KEY.iter().position(|k| *k == o));
            if wanted.key_text() != wish.row_key
                || origin.and_then(|at| key.get(at)) != Some(&KeyPart::Uuid(run.me))
            {
                let why = "the row wished is not the row the wish names, of this replica";
                return answer_alone(&run.pool, &wish, "refused", Some(why)).await;
            }
            let held = {
                let mut conn = run.pool.acquire().await?;
                crate::row::get_by_keys::<R>(&mut conn, std::slice::from_ref(&key))
                    .await?
                    .pop()
            };
            let Some(held) = held else {
                return answer_alone(&run.pool, &wish, "waiting", Some(NO_ROW)).await;
            };
            let based = wanted.version_text();
            if wish.if_unchanged && held.version_text() != based {
                let why = format!(
                    "this replica holds version {} of it, and the wish is based on {based}",
                    held.version_text()
                );
                return answer_alone(&run.pool, &wish, "superseded", Some(&why)).await;
            }
            let handed = Wish {
                wanted: wanted.clone(),
                held,
                note: wish.note.clone(),
                version: wish.version,
            };
            let outcome = self.outcome(run, wish.id, handed).await;
            let recorded = self.record(run, &wish, &key, wanted, &outcome).await;
            let mut given = self.given();
            match recorded {
                Ok(_) => {
                    given.remove(&(wish.id, wish.version));
                }
                Err(_) => {
                    given.insert((wish.id, wish.version), outcome);
                }
            }
            recorded
        }
    }
}
