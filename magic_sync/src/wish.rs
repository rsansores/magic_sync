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

fn from_wire(wish: envelope::Wish) -> Result<WishRow, MapError> {
    let parts = wish.key.map(|k| k.parts).unwrap_or_default();
    if parts.is_empty() {
        return Err(MapError::new("a wish of no row"));
    }
    let version =
        positive(wish.version, "a version")?.ok_or_else(|| MapError::new("a wish of version 0"))?;
    let outcome = (!wish.outcome.is_empty()).then_some(wish.outcome);
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
    Ok(WishRow {
        id: wish.id.parse()?,
        replica_id: wish.replica_id.parse()?,
        relation: wish.table,
        row_key: parts.join("|"),
        wanted: wish.wanted,
        version,
        if_unchanged: wish.if_unchanged,
        note: (!wish.note.is_empty()).then_some(wish.note),
        answered: positive(wish.answered, "an answer to version")?,
        outcome,
        reason: (!wish.reason.is_empty()).then_some(wish.reason),
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
