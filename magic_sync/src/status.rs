//! What the engine answers about itself, for a dashboard or an alarm: whether
//! each replica holds what it should, how far along it is, where one row
//! stands, and what waits for a fix. Every read takes a [`Scope`], so a host
//! shows each audience its own replicas and nothing else; the engine knows
//! replicas only by id, and the host puts names on them.
//!
//! Each read is one snapshot: called on a connection outside a transaction,
//! it opens a `REPEATABLE READ` one of its own; called inside one, that
//! transaction must be `REPEATABLE READ` or `SERIALIZABLE`.
//!
//! Every answer a dashboard reads serializes as the JSON of contract
//! [`CONTRACT`], and carries that number.

use crate::plan::{parse_key, stale, Engine, REFUSED};

pub use crate::replica::UpProgress;
use crate::Error;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Connection, Database, TransactionManager};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use uuid::Uuid;

use crate::Conn;

/// The version of the JSON the answers serialize as. A field added does not
/// change it, nor does a variant added to a value that is one of a few words
/// (`ReplicaState`, `RowState`, `NoteKind`): a reader shows a word it does not
/// know as unknown, and counts it. A field removed or renamed, a variant
/// removed or renamed, or a meaning changed — a word that now stands for
/// something else — does change it, and a dashboard refuses an answer of a
/// contract it does not read.
pub const CONTRACT: u32 = 1;

/// Which of a listing's entries a read answers: up to `limit` of them, from
/// `offset`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Page {
    /// Entries skipped.
    pub offset: u32,
    /// Entries answered, at most.
    pub limit: u32,
}

impl Page {
    /// Up to `limit` entries, from `offset`.
    #[must_use]
    pub const fn new(offset: u32, limit: u32) -> Self {
        Page { offset, limit }
    }
}

/// Which replicas a read covers. Only live replicas are counted: a partition
/// or every replica means its live ones, and a list is narrowed to them.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum Scope<'a> {
    /// These replicas: the ones a caller may see.
    Replicas(&'a [Uuid]),
    /// Every replica of one partition.
    Partition(Uuid),
    /// Every replica.
    All,
}

/// [`Engine::progress`]: how far each replica of a scope is from holding
/// what it should.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct Progress {
    /// The contract this answer serializes as: `CONTRACT`.
    pub contract: u32,
    /// Each replica of the scope.
    pub replicas: Vec<ReplicaProgress>,
}

/// How far one replica is from holding what it should.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct ReplicaProgress {
    /// The replica.
    pub replica: Uuid,
    /// Each table the server sends down, parents first.
    pub tables: Vec<TableProgress>,
}

/// One table on one replica, counted from what the replica acknowledged
/// applying. `holds - outdated` of `should` is how far along it is; the
/// numbers never count a row as held that the replica has not confirmed.
/// Every row the engine sent it is in exactly one of `holds`, `in_flight`
/// and `to_remove`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct TableProgress {
    /// The table.
    pub table: String,
    /// Rows its rules say it should hold. For a table that only travels with
    /// others, what the engine decided to send it.
    pub should: i64,
    /// Rows it should hold and has confirmed it holds.
    pub holds: i64,
    /// Of those, rows changed since: a newer version is on its way or owed.
    pub outdated: i64,
    /// Rows it should hold, sent and not confirmed yet.
    pub in_flight: i64,
    /// Rows it was sent and should no longer hold: on their way out.
    pub to_remove: i64,
    /// Rows set aside for it until a fix.
    pub quarantined: i64,
}

impl TableProgress {
    /// Rows it holds as they should be.
    #[must_use]
    pub fn up_to_date(&self) -> i64 {
        self.holds - self.outdated
    }
}

/// [`Engine::row_progress`]: where one row stands on each replica of a scope
/// that holds it, should, or is owed it.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct RowProgress {
    /// The contract this answer serializes as: `CONTRACT`.
    pub contract: u32,
    /// Each replica of the scope the row is something to.
    pub places: Vec<RowPlace>,
}

/// Where one row stands on one replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct RowPlace {
    /// The replica.
    pub replica: Uuid,
    /// What the row is to it.
    pub state: RowState,
}

/// What one row is to one replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RowState {
    /// It holds the row as it is.
    Holds,
    /// It holds an older version; the newer one is on its way or owed.
    Outdated,
    /// Sent, not yet confirmed.
    OnItsWay,
    /// It should hold the row and does not yet.
    Missing,
    /// It was sent the row and should no longer hold it.
    Leaving,
    /// Set aside for it until a fix.
    Quarantined,
}

/// Where a wish stands: what its replica answered to it, or that it has
/// not yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum WishState {
    /// No answer to it yet: on its way, or carried out and the answer on its
    /// way back.
    Pending,
    /// The replica cannot carry it out yet — the hardware away, a build that
    /// cannot read it — and tries again; the reason says why.
    Waiting,
    /// Carried out: the setting holds what was wished.
    Applied,
    /// The replica will not carry it out; the reason says why. Wish again to
    /// try again.
    Refused,
    /// Based on a version of the setting the replica no longer held: not
    /// carried out, and the replica's value stands.
    Superseded,
}

/// How many of a replica's wishes stand where: those it carried out are
/// the rest.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct WishCounts {
    /// Not answered yet.
    pub pending: i64,
    /// Waiting until the replica can carry them out.
    pub waiting: i64,
    /// Refused by the replica.
    pub refused: i64,
    /// Superseded: the setting changed on the replica first.
    pub superseded: i64,
}

/// One wish, as [`Engine::wishes`] lists it.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct WishEntry {
    /// The replica it is for.
    pub replica: Uuid,
    /// The setting's table.
    pub table: String,
    /// The setting row's key.
    pub key: String,
    /// Where it stands.
    pub state: WishState,
    /// Why, for an answer that says: refused, superseded or waiting.
    pub reason: Option<String>,
    /// What the host said of it when it wished it.
    pub note: Option<String>,
    /// When it was wished.
    pub wished_at: DateTime<Utc>,
    /// When the replica answered it, by its clock.
    pub answered_at: Option<DateTime<Utc>>,
}

/// [`Engine::wishes`]: one page of the wishes of a scope's replicas, those
/// that need attention first.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct WishListing {
    /// The contract this answer serializes as: `CONTRACT`.
    pub contract: u32,
    /// The server's time when the answer was read.
    pub now: DateTime<Utc>,
    /// The state the page was asked for (`None`: every state).
    pub state: Option<WishState>,
    /// Where in the list the page begins.
    pub offset: u32,
    /// The page.
    pub wishes: Listed<WishEntry>,
}

/// Where a replica stands, in one word: the first of these that applies.
/// Ordered worst first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ReplicaState {
    /// Something of its own is set aside until a fix is deployed: a row it
    /// is owed and cannot be sent or could not take, a row it sent that
    /// could not be filed, or a change of it alone the planner could not
    /// plan.
    AwaitingFix,
    /// Sends to it fail; it is tried again at `retry_at`.
    Unreachable,
    /// Changes are on their way to it, or from it — or held back: by a
    /// change the planner set aside that may concern it, or by the replica,
    /// a row of its own it deleted that waits to be sent again — or a wish
    /// of its settings it has not carried out yet.
    CatchingUp,
    /// It holds what it should, and the server has filed everything it last
    /// said it wrote.
    Synced,
}

/// How many replicas are in each state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct ReplicaCounts {
    /// Replicas whose state is `awaiting_fix`.
    pub awaiting_fix: i64,
    /// Replicas whose state is `unreachable`.
    pub unreachable: i64,
    /// Replicas whose state is `catching_up`.
    pub catching_up: i64,
    /// Replicas whose state is `synced`.
    pub synced: i64,
}

impl ReplicaCounts {
    fn of(&mut self, state: ReplicaState) -> &mut i64 {
        match state {
            ReplicaState::AwaitingFix => &mut self.awaiting_fix,
            ReplicaState::Unreachable => &mut self.unreachable,
            ReplicaState::CatchingUp => &mut self.catching_up,
            ReplicaState::Synced => &mut self.synced,
        }
    }
}

/// One replica, from what the engine records about it: nothing here counts
/// the rows it should hold, so it costs the same for a fleet as for one.
/// Whether it is synced is exact all the same: a change it lacks is either a
/// note not yet planned, a message it has not confirmed, or set aside.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct ReplicaOverview {
    /// The replica.
    pub replica: Uuid,
    /// Where it stands, in one word.
    pub state: ReplicaState,
    /// Changes noted and not yet planned that may concern it.
    pub pending: i64,
    /// Messages written for it that the transport has not taken yet: what
    /// waits for it, in order, while it cannot be reached.
    pub owed: i64,
    /// Changes the planner set aside that may concern it.
    pub parked: i64,
    /// Whether it has messages it has not said it applied.
    pub unconfirmed: bool,
    /// Rows set aside for it until a fix.
    pub quarantined: i64,
    /// Rows it sent that wait in the inbox until a fix.
    pub unfiled: i64,
    /// Sends to it that failed in a row.
    pub failures: i32,
    /// The last failed send's error — or, failing none, why what waited for
    /// it was last dropped and it was provisioned again.
    pub last_error: Option<String>,
    /// When it is tried again, while it waits out a failure.
    pub retry_at: Option<DateTime<Utc>>,
    /// When it was last sent a message.
    pub sent_at: Option<DateTime<Utc>>,
    /// When it last said it applied one.
    pub applied_at: Option<DateTime<Utc>>,
    /// While it has messages it has not said it applied: since it last
    /// moved — its last send or its last confirmation, whichever came later.
    /// How long a replica has not answered.
    pub waiting_since: Option<DateTime<Utc>>,
    /// What it last said it wrote and the server has not filed, per table:
    /// `None` until it says.
    pub backlog: Option<Backlog>,
    /// Its settings' wishes, by where they stand.
    pub wishes: WishCounts,
}

/// What a replica wrote and the server has not filed, as of when it said so.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct Backlog {
    /// Each table with something on its way.
    pub tables: Vec<UpProgress>,
    /// When the replica said so.
    pub at: DateTime<Utc>,
}

/// One table over the replicas of a scope: every figure zero means each of
/// them holds it as it should. It is read without counting what any replica
/// should hold, or which of them a pending change will reach.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct TableOverview {
    /// The table.
    pub table: String,
    /// Every figure below is zero.
    pub synced: bool,
    /// Changes to its rows not yet planned.
    pub pending: i64,
    /// Changes to its rows the planner set aside until a fix.
    pub parked: i64,
    /// Rows sent and not confirmed yet.
    pub in_flight: i64,
    /// Rows set aside until a fix.
    pub quarantined: i64,
    /// Uploaded rows waiting in the inbox until a fix.
    pub unfiled: i64,
    /// Replicas with a row of it in flight, set aside or unfiled. A change
    /// not yet planned is counted in `pending`, not here: who it goes to is
    /// what planning decides.
    pub replicas: i64,
}

/// What the engine is waiting on, over a scope: each entry is either the
/// infrastructure (wait) or a bug (deploy a fix) — never data to repair by
/// hand. Lists are cut at the limit the read was given; `total` is uncut.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct Health {
    /// Nothing waits for a fix. A replica out of reach is not unhealthy — it
    /// is the network's to settle, and listed in `failing`; nor is a
    /// backlog, unless it stops draining, which `oldest` shows.
    pub healthy: bool,
    /// Changes noted and not yet planned.
    pub pending: i64,
    /// When the oldest change not yet planned was noted.
    pub oldest: Option<DateTime<Utc>>,
    /// Changes the planner set aside until a fix.
    pub parked: Listed<Parked>,
    /// Rows set aside for a replica until a fix.
    pub quarantine: Listed<Quarantined>,
    /// What replicas sent that waits in the inbox until a fix.
    pub inbox: Listed<Unfiled>,
    /// Replicas that sends keep failing to.
    pub failing: Listed<Failing>,
}

impl Default for Health {
    /// Nothing waits, so nothing needs a fix.
    fn default() -> Self {
        Health {
            healthy: true,
            pending: 0,
            oldest: None,
            parked: Listed::default(),
            quarantine: Listed::default(),
            inbox: Listed::default(),
            failing: Listed::default(),
        }
    }
}

/// The entries of a list a read asked for, and how many there are in all.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct Listed<T> {
    /// How many there are in all.
    pub total: i64,
    /// The first of them, up to the limit the read was given.
    pub first: Vec<T>,
}

impl<T> Default for Listed<T> {
    fn default() -> Self {
        Listed {
            total: 0,
            first: Vec::new(),
        }
    }
}

/// What a note asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum NoteKind {
    /// A row of `table` changed.
    Row,
    /// A row joined or left a group.
    Members,
    /// A group was given to or taken from `replica`.
    Assigned,
    /// `replica` is provisioned again.
    Resync,
}

/// A change the planner set aside.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct Parked {
    /// The note's number.
    pub id: i64,
    /// What it asks for.
    pub kind: NoteKind,
    /// The table it is of, for a row or a group.
    pub table: Option<String>,
    /// The row's key, for a row.
    pub key: Option<String>,
    /// The replica it is about.
    pub replica: Option<Uuid>,
    /// How many times it was planned and failed.
    pub attempts: i32,
    /// Why it failed last.
    pub last_error: Option<String>,
    /// When it was set aside.
    pub parked_at: Option<DateTime<Utc>>,
}

/// A row set aside for one replica.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct Quarantined {
    /// The replica it is set aside for.
    pub replica: Uuid,
    /// Its table.
    pub table: String,
    /// Its key.
    pub key: String,
    /// Why it could not go, or could not be taken.
    pub reason: String,
    /// When it was first set aside.
    pub first_at: DateTime<Utc>,
    /// When it last failed.
    pub last_at: DateTime<Utc>,
    /// How many times it failed.
    pub attempts: i32,
}

/// What a replica sent that the server could not file: a row, or a delete.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct Unfiled {
    /// Its number in the inbox.
    pub id: i64,
    /// The replica that sent it.
    pub replica: Uuid,
    /// The table of the row, or of the delete.
    pub table: String,
    /// Why it could not be filed.
    pub reason: String,
    /// When it first could not be filed.
    pub first_at: DateTime<Utc>,
    /// When filing it last failed.
    pub last_at: DateTime<Utc>,
    /// How many times filing it failed.
    pub attempts: i32,
}

/// A replica that sends keep failing to.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct Failing {
    /// The replica.
    pub replica: Uuid,
    /// Sends to it that failed in a row.
    pub failures: i32,
    /// The last failed send's error.
    pub last_error: Option<String>,
    /// When it is tried again.
    pub retry_at: Option<DateTime<Utc>>,
}

/// The stored forms of the answers' entries: the types are the contract's,
/// and how a table's row becomes one stays out of the API.
mod stored {
    use super::{Failing, NoteKind, Parked, Quarantined, Unfiled};
    use sqlx::postgres::PgRow;
    use sqlx::Row as _;

    /// A note of a kind this build does not know, a later build's, is
    /// `None`: the listing reads none — it asks for what [`READABLE`]
    /// admits — and the count counts it.
    ///
    /// [`READABLE`]: crate::plan::READABLE
    pub(super) fn parked(row: PgRow) -> sqlx::Result<Option<Parked>> {
        let kind = match row.try_get::<String, _>("kind")?.as_str() {
            "row" | "retry" => NoteKind::Row,
            "members" => NoteKind::Members,
            "assigned" => NoteKind::Assigned,
            "resync" => NoteKind::Resync,
            _ => return Ok(None),
        };
        Ok(Some(Parked {
            id: row.try_get("id")?,
            kind,
            table: row.try_get("table")?,
            key: row.try_get("key")?,
            replica: row.try_get("replica")?,
            attempts: row.try_get("attempts")?,
            last_error: row.try_get("last_error")?,
            parked_at: row.try_get("parked_at")?,
        }))
    }

    pub(super) fn quarantined(row: PgRow) -> sqlx::Result<Quarantined> {
        Ok(Quarantined {
            replica: row.try_get("replica")?,
            table: row.try_get("table")?,
            key: row.try_get("key")?,
            reason: row.try_get("reason")?,
            first_at: row.try_get("first_at")?,
            last_at: row.try_get("last_at")?,
            attempts: row.try_get("attempts")?,
        })
    }

    pub(super) fn unfiled(row: PgRow) -> sqlx::Result<Unfiled> {
        Ok(Unfiled {
            id: row.try_get("id")?,
            replica: row.try_get("replica")?,
            table: row.try_get("table")?,
            reason: row.try_get("reason")?,
            first_at: row.try_get("first_at")?,
            last_at: row.try_get("last_at")?,
            attempts: row.try_get("attempts")?,
        })
    }

    /// A wish as [`Engine::wishes`](super::Engine::wishes) reads it, with
    /// how many its listing has in all.
    #[derive(sqlx::FromRow)]
    pub(super) struct Wished {
        replica: uuid::Uuid,
        table: String,
        key: String,
        state: String,
        reason: Option<String>,
        note: Option<String>,
        wished_at: chrono::DateTime<chrono::Utc>,
        answered_at: Option<chrono::DateTime<chrono::Utc>>,
        pub(super) total: i64,
    }

    impl Wished {
        pub(super) fn entry(self) -> super::WishEntry {
            super::WishEntry {
                replica: self.replica,
                table: self.table,
                key: self.key,
                state: crate::wish::state_of(&self.state),
                reason: self.reason,
                note: self.note,
                wished_at: self.wished_at,
                answered_at: self.answered_at,
            }
        }
    }

    pub(super) fn failing(row: PgRow) -> sqlx::Result<Failing> {
        Ok(Failing {
            replica: row.try_get("replica")?,
            failures: row.try_get("failures")?,
            last_error: row.try_get("last_error")?,
            retry_at: row.try_get("retry_at")?,
        })
    }
}

/// [`Engine::overview`]: everything a dashboard shows first, in one read.
/// Its size does not grow with the fleet: the replicas are counted per
/// state, and listed worst first up to the limit; [`Engine::listing`] pages
/// through the rest.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct Overview {
    /// The contract this answer serializes as: `CONTRACT`.
    pub contract: u32,
    /// The server's time when the answer was read: every time in it is the
    /// server's, and how long ago one was is measured from this.
    pub now: DateTime<Utc>,
    /// What the engine waits on.
    pub health: Health,
    /// How many replicas are in each state.
    pub states: ReplicaCounts,
    /// The replicas that need attention most, worst first.
    pub replicas: Listed<ReplicaOverview>,
    /// Each table the server sends down, parents first.
    pub tables: Vec<TableOverview>,
}

/// [`Engine::listing`]: one page of the replicas, worst first.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct Listing {
    /// The contract this answer serializes as: `CONTRACT`.
    pub contract: u32,
    /// The server's time when the answer was read.
    pub now: DateTime<Utc>,
    /// The state the page was asked for (`None`: every state).
    pub state: Option<ReplicaState>,
    /// Where in the list the page begins.
    pub offset: u32,
    /// The page.
    pub replicas: Listed<ReplicaOverview>,
}

/// A row of `sync.quarantine` that waits for a fix: one a replica held back
/// — a row of its own it deleted, the delete on its way — waits only for
/// the next retry, and raises no alarm.
const AWAITING_FIX: &str = "cause <> 'held_back'";

/// Whether a row a replica holds is confirmed: the message that carried it
/// is one the replica said it applied, in this epoch.
const CONFIRMED: &str =
    "coalesce(h.sequence <= r.applied_sequence AND r.applied_epoch = e.epoch, false)";

/// The newest pending change of each row of the table `$1`, joined once
/// to the rows replicas hold of it, `h`, as `p`: a held row is outdated when
/// it is older ([`stale`](crate::plan::stale)).
const PENDING: &str = "LEFT JOIN (SELECT row_key, max(id) AS noted FROM sync.notes \
                                  WHERE kind = 'row' AND done_at IS NULL AND relation = $1 \
                                  GROUP BY row_key) p ON p.row_key = h.row_key";

/// The pending notes a scope sees: `$1` every one, else those of the
/// partitions `$2` (or of none), about the replicas `$3` (or any).
const NOTES_IN_SCOPE: &str = "n.done_at IS NULL AND ($1 OR ( \
       (n.partition_id IS NULL OR n.partition_id = ANY($2)) \
   AND (n.replica_id IS NULL OR n.replica_id = ANY($3))))";

/// The live replicas a scope names, with their partitions.
struct Scoped {
    replicas: Vec<(Uuid, Option<Uuid>)>,
    /// Every replica: nothing narrows the notes.
    all: bool,
    /// The partitions the notes are narrowed to.
    partitions: Vec<Uuid>,
}

impl Scoped {
    fn ids(&self) -> Vec<Uuid> {
        self.replicas.iter().map(|(r, _)| *r).collect()
    }
}

/// Pending notes, counted by what they may concern.
#[derive(sqlx::FromRow)]
struct NoteGroup {
    table: Option<String>,
    partition: Option<Uuid>,
    replica: Option<Uuid>,
    parked: bool,
    count: i64,
    oldest: DateTime<Utc>,
}

/// What the pending notes add up to for the replicas they may concern.
#[derive(Clone, Copy, Default)]
struct NoteCount {
    pending: i64,
    parked: i64,
    /// Parked, and about this replica alone.
    own_parked: i64,
}

impl std::ops::AddAssign for NoteCount {
    fn add_assign(&mut self, other: Self) {
        self.pending += other.pending;
        self.parked += other.parked;
        self.own_parked += other.own_parked;
    }
}

/// The notes folded once: what concerns every replica, each partition's,
/// and each replica's own. A replica's count is the sum of the three it
/// belongs to, so the fold costs the notes plus the replicas, never their
/// product.
#[derive(Default)]
struct Folded {
    everyone: NoteCount,
    partitions: HashMap<Uuid, NoteCount>,
    replicas: HashMap<Uuid, NoteCount>,
}

impl Folded {
    fn of(notes: &[NoteGroup], partition_of: &HashMap<Uuid, Option<Uuid>>) -> Self {
        let mut folded = Folded::default();
        for note in notes {
            let pending = if note.parked { 0 } else { note.count };
            let parked = note.count - pending;
            let shared = NoteCount {
                pending,
                parked,
                ..NoteCount::default()
            };
            match note.replica {
                Some(replica) => {
                    let partition = partition_of.get(&replica).copied().flatten();
                    if note.partition.is_none_or(|p| Some(p) == partition) {
                        *folded.replicas.entry(replica).or_default() += NoteCount {
                            own_parked: parked,
                            ..shared
                        };
                    }
                }
                None => match note.partition {
                    Some(p) => *folded.partitions.entry(p).or_default() += shared,
                    None => folded.everyone += shared,
                },
            }
        }
        folded
    }

    fn count(&self, replica: Uuid, partition: Option<Uuid>) -> NoteCount {
        let mut count = self.everyone;
        if let Some(p) = partition.and_then(|p| self.partitions.get(&p)) {
            count += *p;
        }
        if let Some(own) = self.replicas.get(&replica) {
            count += *own;
        }
        count
    }
}

/// One replica as `sync.replicas` and the counts beside it record it.
#[derive(sqlx::FromRow)]
struct Recorded {
    replica: Uuid,
    failures: Option<i32>,
    last_error: Option<String>,
    retry_at: Option<DateTime<Utc>>,
    sent_at: Option<DateTime<Utc>>,
    applied_at: Option<DateTime<Utc>>,
    unconfirmed: bool,
    backlog: Option<sqlx::types::Json<Vec<UpProgress>>>,
    backlog_at: Option<DateTime<Utc>>,
    waiting_since: Option<DateTime<Utc>>,
    quarantined: i64,
    /// Of those, the ones that wait for a fix.
    awaiting: i64,
    unfiled: i64,
    owed: i64,
    wishes_pending: i64,
    wishes_waiting: i64,
    wishes_refused: i64,
    wishes_superseded: i64,
}

/// Every replica of a scope, worst first, with how many are in each state.
struct Fleet {
    states: ReplicaCounts,
    replicas: Vec<ReplicaOverview>,
}

impl Fleet {
    /// `limit` of the replicas in `state` (any, when `None`) from `offset`.
    fn page(
        &self,
        state: Option<ReplicaState>,
        offset: u32,
        limit: u32,
    ) -> Listed<ReplicaOverview> {
        let mut states = self.states;
        Listed {
            total: match state {
                Some(state) => *states.of(state),
                None => {
                    states.awaiting_fix + states.unreachable + states.catching_up + states.synced
                }
            },
            first: self
                .replicas
                .iter()
                .filter(|r| state.is_none_or(|s| r.state == s))
                .skip(offset as usize)
                .take(limit as usize)
                .cloned()
                .collect(),
        }
    }
}

/// The read's one snapshot: its own `REPEATABLE READ` transaction, or a
/// savepoint in the caller's, which must give one snapshot too.
async fn snapshot(conn: &mut Conn) -> Result<sqlx::Transaction<'_, crate::Db>, Error> {
    type Manager = <crate::Db as Database>::TransactionManager;
    if Manager::get_transaction_depth(conn) == 0 {
        return Ok(conn
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await?);
    }
    let isolation: String = sqlx::query_scalar("SHOW transaction_isolation")
        .fetch_one(&mut *conn)
        .await?;
    if isolation == "read committed" || isolation == "read uncommitted" {
        return Err(Error::Misuse(
            "a status read is one snapshot: call it outside a transaction, or in a \
             REPEATABLE READ one"
                .to_owned(),
        ));
    }
    Ok(conn.begin().await?)
}

impl Engine {
    /// How far each replica of `scope` is from holding what it should, per
    /// table, counted from what each has acknowledged applying — never from
    /// what was merely sent. See [`TableProgress`]. It counts the rows each
    /// replica should hold: ask it for the replicas a person is looking at.
    ///
    /// # Errors
    ///
    /// [`Error::Database`] when the database fails; [`Error::Misuse`] when
    /// called inside a transaction weaker than `REPEATABLE READ`, or for a
    /// [`Scope::Partition`] when the replicas have no partition.
    pub async fn progress(&self, conn: &mut Conn, scope: Scope<'_>) -> Result<Progress, Error> {
        let mut tx = snapshot(conn).await?;
        let replicas = self.scoped(&mut tx, scope).await?.ids();
        let quarantined: HashMap<(Uuid, String), i64> = sqlx::query_as(
            "SELECT replica_id, relation, count(*) FROM sync.quarantine \
              WHERE replica_id = ANY($1) GROUP BY 1, 2",
        )
        .bind(&replicas)
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(|(replica, table, n): (Uuid, String, i64)| ((replica, table), n))
        .collect();
        let mut out: BTreeMap<Uuid, Vec<TableProgress>> =
            replicas.iter().map(|r| (*r, Vec::new())).collect();
        for &i in &self.order {
            let table = self.plans[i].table;
            let (sql, ruled) = self.progress_sql(i);
            let rows: Vec<(Uuid, i64, i64, i64, i64, i64)> = sqlx::query_as(&sql)
                .bind(table)
                .bind(&replicas)
                .bind(ruled)
                .fetch_all(&mut *tx)
                .await?;
            let mut counted: HashMap<Uuid, (i64, i64, i64, i64, i64)> = rows
                .into_iter()
                .map(|(r, should, holds, outdated, in_flight, to_remove)| {
                    (r, (should, holds, outdated, in_flight, to_remove))
                })
                .collect();
            for (replica, tables) in out.iter_mut() {
                let (should, holds, outdated, in_flight, to_remove) =
                    counted.remove(replica).unwrap_or_default();
                let quarantined = quarantined
                    .get(&(*replica, table.to_owned()))
                    .copied()
                    .unwrap_or(0);
                tables.push(TableProgress {
                    table: table.to_owned(),
                    should: should + if ruled { 0 } else { quarantined },
                    holds,
                    outdated,
                    in_flight,
                    to_remove,
                    quarantined,
                });
            }
        }
        tx.commit().await?;
        Ok(Progress {
            contract: CONTRACT,
            replicas: out
                .into_iter()
                .map(|(replica, tables)| ReplicaProgress { replica, tables })
                .collect(),
        })
    }

    /// How far the replicas `$2` are from holding what they should of the
    /// table `i` (named `$1`; `$3`: whether it has a rule): per replica,
    /// what it should hold, holds, holds outdated, has in flight and holds
    /// to be removed — each read once, joined as sets, never asked row by
    /// row. Whether the table has a rule.
    pub(crate) fn progress_sql(&self, i: usize) -> (String, bool) {
        // A table that only travels with others is owed what the engine
        // decided to send it: there is no rule to count it from.
        let wanted = self.wanted_sql(i, "true");
        let ruled = wanted.is_some();
        let wanted = wanted.unwrap_or_else(|| {
            "SELECT NULL::uuid AS replica_id, NULL::text AS key WHERE false".to_owned()
        });
        let sql = format!(
            "WITH w AS (SELECT replica_id, key FROM ({wanted}) x WHERE replica_id = ANY($2)), \
             h AS (SELECT h.replica_id, h.row_key AS key, {CONFIRMED} AS confirmed, \
                          {} AS outdated \
                     FROM sync.held h CROSS JOIN sync.epoch e \
                     LEFT JOIN sync.replicas r ON r.replica_id = h.replica_id {PENDING} \
                    WHERE h.relation = $1 AND h.replica_id = ANY($2) AND NOT {REFUSED}) \
             SELECT coalesce(w.replica_id, h.replica_id), \
                    count(w.key) + CASE WHEN $3 THEN 0 ELSE count(h.key) END, \
                    count(*) FILTER (WHERE (NOT $3 OR w.key IS NOT NULL) AND h.confirmed), \
                    count(*) FILTER (WHERE (NOT $3 OR w.key IS NOT NULL) AND h.confirmed AND h.outdated), \
                    count(*) FILTER (WHERE (NOT $3 OR w.key IS NOT NULL) AND NOT h.confirmed), \
                    count(*) FILTER (WHERE $3 AND w.key IS NULL AND h.key IS NOT NULL) \
               FROM w FULL JOIN h ON w.replica_id = h.replica_id AND w.key = h.key \
              GROUP BY 1",
            stale("p.noted")
        );
        (sql, ruled)
    }

    /// Where one row stands on every replica of `scope` that holds it,
    /// should, or is owed it. `key` is the row's key as text, in the form
    /// [`Row::key_text`](crate::Row::key_text) writes: the parts of a key of
    /// several columns joined by `|`, a uuid lowercase and hyphenated.
    ///
    /// # Errors
    ///
    /// As the other reads, and besides: [`Error::Misuse`] when `table` is not
    /// mirrored, [`Error::Malformed`] when `key` is not a key of it.
    pub async fn row_progress(
        &self,
        conn: &mut Conn,
        scope: Scope<'_>,
        table: &str,
        key: &str,
    ) -> Result<RowProgress, Error> {
        let i = self
            .index(table)
            .ok_or_else(|| Error::Misuse(format!("`{table}` is not mirrored")))?;
        parse_key(&self.plans[i].key, key)?;
        let mut tx = snapshot(conn).await?;
        let scoped = self.scoped(&mut tx, scope).await?.ids();
        let keys = [key.to_owned()];
        let wanted: BTreeSet<Uuid> = self
            .wanted(&mut tx, i, Some(&keys), Some(&scoped))
            .await?
            .into_iter()
            .map(|(r, _)| r)
            .collect();
        let ruled = self.wanted_sql(i, "true").is_some();
        let held: Vec<(Uuid, bool, bool)> = sqlx::query_as(&format!(
            "SELECT h.replica_id, {CONFIRMED}, {} \
               FROM sync.held h CROSS JOIN sync.epoch e \
               LEFT JOIN sync.replicas r ON r.replica_id = h.replica_id {PENDING} \
              WHERE h.relation = $1 AND h.row_key = $2 AND h.replica_id = ANY($3)",
            stale("p.noted")
        ))
        .bind(table)
        .bind(key)
        .bind(&scoped)
        .fetch_all(&mut *tx)
        .await?;
        let quarantined: Vec<Uuid> = sqlx::query_scalar(
            "SELECT replica_id FROM sync.quarantine \
              WHERE relation = $1 AND row_key = $2 AND replica_id = ANY($3)",
        )
        .bind(table)
        .bind(key)
        .bind(&scoped)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        let mut out: BTreeMap<Uuid, RowState> = wanted
            .iter()
            .map(|replica| (*replica, RowState::Missing))
            .collect();
        for (replica, confirmed, outdated) in held {
            let state = if ruled && !wanted.contains(&replica) {
                RowState::Leaving
            } else if !confirmed {
                RowState::OnItsWay
            } else if outdated {
                RowState::Outdated
            } else {
                RowState::Holds
            };
            out.insert(replica, state);
        }
        for replica in quarantined {
            out.insert(replica, RowState::Quarantined);
        }
        Ok(RowProgress {
            contract: CONTRACT,
            places: out
                .into_iter()
                .map(|(replica, state)| RowPlace { replica, state })
                .collect(),
        })
    }

    /// Everything a dashboard shows first, over `scope`: the engine's health
    /// with up to `limit` entries of each list, how many replicas are in each
    /// state with up to `limit` of them worst first, and each table's state.
    ///
    /// # Errors
    ///
    /// [`Error::Database`] when the database fails; [`Error::Misuse`] when
    /// called inside a transaction weaker than `REPEATABLE READ`, or for a
    /// [`Scope::Partition`] when the replicas have no partition.
    pub async fn overview(
        &self,
        conn: &mut Conn,
        scope: Scope<'_>,
        limit: u32,
    ) -> Result<Overview, Error> {
        let mut tx = snapshot(conn).await?;
        let now = Self::now(&mut tx).await?;
        let scoped = self.scoped(&mut tx, scope).await?;
        let notes = self.notes(&mut tx, &scoped).await?;
        let health = self.health_of(&mut tx, &scoped, &notes, limit).await?;
        let fleet = self.fleet(&mut tx, &scoped, &notes).await?;
        let tables = self.tables_of(&mut tx, &scoped, &notes, &fleet).await?;
        tx.commit().await?;
        Ok(Overview {
            contract: CONTRACT,
            now,
            health,
            states: fleet.states,
            replicas: fleet.page(None, 0, limit),
            tables,
        })
    }

    /// The replicas of `scope` in `state` (every one, when `None`), worst
    /// first: the `page` of them asked for. Worst first is by state, then
    /// the longest silent, then by id.
    ///
    /// # Errors
    ///
    /// [`Error::Database`] when the database fails; [`Error::Misuse`] when
    /// called inside a transaction weaker than `REPEATABLE READ`, or for a
    /// [`Scope::Partition`] when the replicas have no partition.
    pub async fn listing(
        &self,
        conn: &mut Conn,
        scope: Scope<'_>,
        state: Option<ReplicaState>,
        page: Page,
    ) -> Result<Listing, Error> {
        let Page { offset, limit } = page;
        let mut tx = snapshot(conn).await?;
        let now = Self::now(&mut tx).await?;
        let scoped = self.scoped(&mut tx, scope).await?;
        let notes = self.notes(&mut tx, &scoped).await?;
        let fleet = self.fleet(&mut tx, &scoped, &notes).await?;
        tx.commit().await?;
        Ok(Listing {
            contract: CONTRACT,
            now,
            state,
            offset,
            replicas: fleet.page(state, offset, limit),
        })
    }

    /// The wishes of the settings of `scope`'s replicas in `state` (every
    /// one, when `None`): the `page` of them asked for, those that need
    /// attention first — refused, superseded, waiting, pending, then the
    /// ones carried out — each the latest wished first.
    ///
    /// # Errors
    ///
    /// [`Error::Database`] when the database fails; [`Error::Misuse`] when
    /// called inside a transaction weaker than `REPEATABLE READ`, or for a
    /// [`Scope::Partition`] when the replicas have no partition.
    pub async fn wishes(
        &self,
        conn: &mut Conn,
        scope: Scope<'_>,
        state: Option<WishState>,
        page: Page,
    ) -> Result<WishListing, Error> {
        let Page { offset, limit } = page;
        let mut tx = snapshot(conn).await?;
        let now = Self::now(&mut tx).await?;
        let scoped = self.scoped(&mut tx, scope).await?;
        let wanted = state.map(|s| {
            serde_json::to_value(s)
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_default()
        });
        let state_sql = crate::wish::STATE;
        let rows: Vec<stored::Wished> = sqlx::query_as(&format!(
            "WITH w AS ( \
                 SELECT w.replica_id AS replica, w.relation AS table, w.row_key AS key, \
                        {state_sql} AS state, \
                        CASE WHEN w.answered IS NOT DISTINCT FROM w.version THEN w.reason END AS reason, \
                        w.note, w.wished_at, \
                        CASE WHEN w.answered IS NOT DISTINCT FROM w.version THEN w.answered_at END \
                          AS answered_at \
                   FROM sync.sync_wishes w WHERE w.replica_id = ANY($1)) \
             SELECT *, count(*) OVER () AS total FROM w \
              WHERE $2::text IS NULL OR w.state = $2 \
              ORDER BY array_position(ARRAY['refused', 'superseded', 'waiting', 'pending', 'applied'], \
                                      w.state), w.wished_at DESC, w.replica, w.table, w.key \
              OFFSET $3 LIMIT $4"
        ))
        .bind(scoped.ids())
        .bind(&wanted)
        .bind(i64::from(offset))
        .bind(i64::from(limit))
        .fetch_all(&mut *tx)
        .await?;
        let total = match rows.first() {
            Some(row) => row.total,
            None => {
                sqlx::query_scalar(&format!(
                    "SELECT count(*) FROM sync.sync_wishes w \
                      WHERE w.replica_id = ANY($1) AND ($2::text IS NULL OR {state_sql} = $2)"
                ))
                .bind(scoped.ids())
                .bind(&wanted)
                .fetch_one(&mut *tx)
                .await?
            }
        };
        tx.commit().await?;
        Ok(WishListing {
            contract: CONTRACT,
            now,
            state,
            offset,
            wishes: Listed {
                total,
                first: rows.into_iter().map(stored::Wished::entry).collect(),
            },
        })
    }

    /// What the engine is waiting on over `scope`, with up to `limit`
    /// entries of each list.
    ///
    /// # Errors
    ///
    /// [`Error::Database`] when the database fails; [`Error::Misuse`] when
    /// called inside a transaction weaker than `REPEATABLE READ`, or for a
    /// [`Scope::Partition`] when the replicas have no partition.
    pub async fn health(
        &self,
        conn: &mut Conn,
        scope: Scope<'_>,
        limit: u32,
    ) -> Result<Health, Error> {
        let mut tx = snapshot(conn).await?;
        let scoped = self.scoped(&mut tx, scope).await?;
        let notes = self.notes(&mut tx, &scoped).await?;
        let health = self.health_of(&mut tx, &scoped, &notes, limit).await?;
        tx.commit().await?;
        Ok(health)
    }

    async fn now(conn: &mut Conn) -> Result<DateTime<Utc>, Error> {
        Ok(sqlx::query_scalar("SELECT now()")
            .fetch_one(&mut *conn)
            .await?)
    }

    /// The live replicas a scope names.
    async fn scoped(&self, conn: &mut Conn, scope: Scope<'_>) -> Result<Scoped, Error> {
        let live = self.replicas_sql();
        let (all, partition, ids): (bool, Option<Uuid>, Option<&[Uuid]>) = match scope {
            Scope::All => (true, None, None),
            Scope::Partition(partition) => {
                if self.replicas.partition.is_none() {
                    return Err(Error::Misuse("the replicas have no partition".to_owned()));
                }
                (false, Some(partition), None)
            }
            Scope::Replicas(ids) => (false, None, Some(ids)),
        };
        let replicas: Vec<(Uuid, Option<Uuid>)> = sqlx::query_as(&format!(
            "SELECT s.id, s.partition FROM {live} \
              WHERE $1 OR s.partition = $2 OR s.id = ANY($3) ORDER BY s.id"
        ))
        .bind(all)
        .bind(partition)
        .bind(ids)
        .fetch_all(&mut *conn)
        .await?;
        let partitions = match partition {
            Some(p) => vec![p],
            None => replicas
                .iter()
                .filter_map(|(_, p)| *p)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        };
        Ok(Scoped {
            replicas,
            all,
            partitions,
        })
    }

    /// The pending notes the scope sees, counted by what they may concern.
    async fn notes(&self, conn: &mut Conn, scoped: &Scoped) -> Result<Vec<NoteGroup>, Error> {
        Ok(sqlx::query_as(&format!(
            "SELECT n.relation AS table, n.partition_id AS partition, \
                    n.replica_id AS replica, n.parked_at IS NOT NULL AS parked, \
                    count(*) AS count, min(n.noted_at) AS oldest \
               FROM sync.notes n WHERE {NOTES_IN_SCOPE} GROUP BY 1, 2, 3, 4"
        ))
        .bind(scoped.all)
        .bind(&scoped.partitions)
        .bind(scoped.ids())
        .fetch_all(&mut *conn)
        .await?)
    }

    async fn health_of(
        &self,
        conn: &mut Conn,
        scoped: &Scoped,
        notes: &[NoteGroup],
        limit: u32,
    ) -> Result<Health, Error> {
        let limit = i64::from(limit);
        let waiting = notes.iter().filter(|n| !n.parked);
        let pending = waiting.clone().map(|n| n.count).sum();
        let oldest = waiting.map(|n| n.oldest).min();
        let parked = Listed {
            total: notes.iter().filter(|n| n.parked).map(|n| n.count).sum(),
            first: sqlx::query(&format!(
                "SELECT n.id, n.kind, n.relation AS table, n.row_key AS key, \
                        n.replica_id AS replica, n.attempts, n.last_error, n.parked_at \
                   FROM sync.notes n WHERE {NOTES_IN_SCOPE} AND n.parked_at IS NOT NULL \
                    AND {} \
                  ORDER BY n.id LIMIT $4",
                crate::plan::READABLE
            ))
            .bind(scoped.all)
            .bind(&scoped.partitions)
            .bind(scoped.ids())
            .bind(limit)
            .try_map(stored::parked)
            .fetch_all(&mut *conn)
            .await?
            .into_iter()
            .flatten()
            .collect(),
        };
        let ids = scoped.ids();
        // Live replicas only, as every read: one that is no longer sent to
        // is no one's to wait for.
        let mine = "replica_id = ANY($1)";
        let quarantine = Listed {
            total: sqlx::query_scalar(&format!(
                "SELECT count(*) FROM sync.quarantine WHERE {mine} AND {AWAITING_FIX}"
            ))
            .bind(&ids)
            .fetch_one(&mut *conn)
            .await?,
            first: sqlx::query(&format!(
                "SELECT replica_id AS replica, relation AS table, row_key AS key, reason, \
                        first_at, last_at, attempts \
                   FROM sync.quarantine WHERE {mine} AND {AWAITING_FIX} \
                  ORDER BY first_at, relation, row_key, replica_id LIMIT $2"
            ))
            .bind(&ids)
            .bind(limit)
            .try_map(stored::quarantined)
            .fetch_all(&mut *conn)
            .await?,
        };
        let inbox = Listed {
            total: sqlx::query_scalar(&format!("SELECT count(*) FROM sync.inbox WHERE {mine}"))
                .bind(&ids)
                .fetch_one(&mut *conn)
                .await?,
            first: sqlx::query(&format!(
                "SELECT id, replica_id AS replica, relation AS table, reason, \
                        first_at, last_at, attempts \
                   FROM sync.inbox WHERE {mine} ORDER BY id LIMIT $2"
            ))
            .bind(&ids)
            .bind(limit)
            .try_map(stored::unfiled)
            .fetch_all(&mut *conn)
            .await?,
        };
        let failing = Listed {
            total: sqlx::query_scalar(&format!(
                "SELECT count(*) FROM sync.replicas WHERE failures > 0 AND {mine}"
            ))
            .bind(&ids)
            .fetch_one(&mut *conn)
            .await?,
            first: sqlx::query(&format!(
                "SELECT replica_id AS replica, failures, last_error, retry_after AS retry_at \
                   FROM sync.replicas WHERE failures > 0 AND {mine} \
                  ORDER BY failures DESC, replica_id LIMIT $2"
            ))
            .bind(&ids)
            .bind(limit)
            .try_map(stored::failing)
            .fetch_all(&mut *conn)
            .await?,
        };
        Ok(Health {
            healthy: parked.total == 0 && quarantine.total == 0 && inbox.total == 0,
            pending,
            oldest,
            parked,
            quarantine,
            inbox,
            failing,
        })
    }

    /// Every replica of the scope, its state settled, worst first.
    async fn fleet(
        &self,
        conn: &mut Conn,
        scoped: &Scoped,
        notes: &[NoteGroup],
    ) -> Result<Fleet, Error> {
        let recorded: Vec<Recorded> = sqlx::query_as(&format!(
            "SELECT s.id AS replica, r.failures, coalesce(r.last_error, c.dropped) AS last_error, \
                    r.retry_after AS retry_at, \
                    r.last_sent_at AS sent_at, r.applied_at, \
                    coalesce({}, false) AS unconfirmed, \
                    r.backlog, r.backlog_at, greatest(r.last_sent_at, r.applied_at) AS waiting_since, \
                    coalesce(q.n, 0) AS quarantined, coalesce(q.fix, 0) AS awaiting, \
                    coalesce(i.n, 0) AS unfiled, \
                    coalesce(o.n, 0) AS owed, \
                    coalesce(w.pending, 0) AS wishes_pending, \
                    coalesce(w.waiting, 0) AS wishes_waiting, \
                    coalesce(w.refused, 0) AS wishes_refused, \
                    coalesce(w.superseded, 0) AS wishes_superseded \
               FROM unnest($1::uuid[]) AS s(id) \
               LEFT JOIN sync.replicas r ON r.replica_id = s.id \
               LEFT JOIN sync.chains c ON c.replica_id = s.id \
               LEFT JOIN (SELECT replica_id, count(*) AS n, \
                                 count(*) FILTER (WHERE {AWAITING_FIX}) AS fix \
                            FROM sync.quarantine \
                           WHERE replica_id = ANY($1) GROUP BY 1) q ON q.replica_id = s.id \
               LEFT JOIN (SELECT replica_id, count(*) AS n FROM sync.inbox \
                           WHERE replica_id = ANY($1) GROUP BY 1) i ON i.replica_id = s.id \
               LEFT JOIN (SELECT replica_id, count(*) AS n FROM sync.outbox \
                           WHERE replica_id = ANY($1) GROUP BY 1) o ON o.replica_id = s.id \
               LEFT JOIN (SELECT replica_id, \
                                 count(*) FILTER (WHERE state = 'pending') AS pending, \
                                 count(*) FILTER (WHERE state = 'waiting') AS waiting, \
                                 count(*) FILTER (WHERE state = 'refused') AS refused, \
                                 count(*) FILTER (WHERE state = 'superseded') AS superseded \
                            FROM (SELECT w.replica_id, {} AS state FROM sync.sync_wishes w \
                                   WHERE w.replica_id = ANY($1)) ws \
                           GROUP BY 1) w ON w.replica_id = s.id",
            crate::relay::UNCONFIRMED,
            crate::wish::STATE,
        ))
        .bind(scoped.ids())
        .fetch_all(&mut *conn)
        .await?;
        let partitions: HashMap<Uuid, Option<Uuid>> = scoped.replicas.iter().copied().collect();
        let folded = Folded::of(notes, &partitions);
        let mut states = ReplicaCounts::default();
        let mut replicas: Vec<ReplicaOverview> = recorded
            .into_iter()
            .map(|r| {
                let notes = folded.count(r.replica, partitions.get(&r.replica).copied().flatten());
                let failures = r.failures.unwrap_or(0);
                let backlog = r.backlog.zip(r.backlog_at).map(|(tables, at)| Backlog {
                    tables: tables.0,
                    at,
                });
                let behind = backlog.as_ref().is_some_and(|b| !b.tables.is_empty());
                let state = if r.awaiting > 0 || r.unfiled > 0 || notes.own_parked > 0 {
                    ReplicaState::AwaitingFix
                } else if failures > 0 {
                    ReplicaState::Unreachable
                } else if notes.pending > 0
                    || notes.parked > 0
                    || r.unconfirmed
                    || behind
                    || r.quarantined > 0
                    || r.wishes_pending > 0
                    || r.wishes_waiting > 0
                {
                    ReplicaState::CatchingUp
                } else {
                    ReplicaState::Synced
                };
                *states.of(state) += 1;
                ReplicaOverview {
                    replica: r.replica,
                    state,
                    pending: notes.pending,
                    owed: r.owed,
                    parked: notes.parked,
                    unconfirmed: r.unconfirmed,
                    quarantined: r.quarantined,
                    unfiled: r.unfiled,
                    failures,
                    last_error: r.last_error,
                    retry_at: r.retry_at,
                    sent_at: r.sent_at,
                    applied_at: r.applied_at,
                    waiting_since: r.waiting_since.filter(|_| r.unconfirmed),
                    backlog,
                    wishes: WishCounts {
                        pending: r.wishes_pending,
                        waiting: r.wishes_waiting,
                        refused: r.wishes_refused,
                        superseded: r.wishes_superseded,
                    },
                }
            })
            .collect();
        replicas.sort_by_key(|r| {
            (
                r.state,
                r.waiting_since.is_none(),
                r.waiting_since,
                r.replica,
            )
        });
        Ok(Fleet { states, replicas })
    }

    async fn tables_of(
        &self,
        conn: &mut Conn,
        scoped: &Scoped,
        notes: &[NoteGroup],
        fleet: &Fleet,
    ) -> Result<Vec<TableOverview>, Error> {
        let blank = |table: &str| TableOverview {
            table: table.to_owned(),
            ..TableOverview::default()
        };
        let mut out: BTreeMap<String, TableOverview> = self
            .order
            .iter()
            .map(|&i| (self.plans[i].table.to_owned(), blank(self.plans[i].table)))
            .collect();
        for note in notes {
            let Some(table) = &note.table else { continue };
            let entry = out.entry(table.clone()).or_insert_with(|| blank(table));
            if note.parked {
                entry.parked += note.count;
            } else {
                entry.pending += note.count;
            }
        }
        // Only the replicas with a message unconfirmed have a row in flight,
        // and only the rows past what each confirmed are read.
        let unconfirmed: Vec<Uuid> = fleet
            .replicas
            .iter()
            .filter(|r| r.unconfirmed)
            .map(|r| r.replica)
            .collect();
        let counted: Vec<(String, i64, i64, i64, i64)> = sqlx::query_as(&format!(
            "WITH c AS ( \
                 SELECT h.relation, h.replica_id, count(*) AS in_flight, 0 AS quarantined, 0 AS unfiled \
                   FROM sync.epoch e CROSS JOIN unnest($1::uuid[]) AS u(replica_id) \
                   LEFT JOIN sync.replicas r ON r.replica_id = u.replica_id \
                   JOIN sync.held h ON h.replica_id = u.replica_id \
                                   AND h.sequence > CASE WHEN r.applied_epoch = e.epoch \
                                                         THEN coalesce(r.applied_sequence, 0) ELSE 0 END \
                  WHERE NOT {REFUSED} \
                  GROUP BY 1, 2 \
                 UNION ALL \
                 SELECT relation, replica_id, 0, count(*), 0 FROM sync.quarantine \
                  WHERE replica_id = ANY($2) GROUP BY 1, 2 \
                 UNION ALL \
                 SELECT relation, replica_id, 0, 0, count(*) FROM sync.inbox \
                  WHERE replica_id = ANY($2) AND relation IS NOT NULL GROUP BY 1, 2) \
             SELECT relation, sum(in_flight)::int8, sum(quarantined)::int8, sum(unfiled)::int8, \
                    count(DISTINCT replica_id) \
               FROM c GROUP BY 1"),
        )
        .bind(&unconfirmed)
        .bind(scoped.ids())
        .fetch_all(&mut *conn)
        .await?;
        for (table, in_flight, quarantined, unfiled, replicas) in counted {
            let entry = out.entry(table.clone()).or_insert_with(|| blank(&table));
            entry.in_flight = in_flight;
            entry.quarantined = quarantined;
            entry.unfiled = unfiled;
            entry.replicas = replicas;
        }
        for entry in out.values_mut() {
            entry.synced = entry.pending == 0
                && entry.parked == 0
                && entry.in_flight == 0
                && entry.quarantined == 0
                && entry.unfiled == 0;
        }
        // Parents first, as the engine sends them; tables only an upload
        // names after.
        let mut ordered: Vec<TableOverview> = self
            .order
            .iter()
            .filter_map(|&i| out.remove(self.plans[i].table))
            .collect();
        ordered.extend(out.into_values());
        Ok(ordered)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Holds `text` against `dashboard/src/core/contract/<file>`, the copy
    /// the dashboard builds and tests against; `UPDATE_CONTRACT=1` writes it.
    pub(crate) fn matches_contract(file: &str, text: &str) {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../dashboard/src/core/contract")
            .join(file);
        if std::env::var_os("UPDATE_CONTRACT").is_some() {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, text).unwrap();
        }
        let written = std::fs::read_to_string(&path).unwrap_or_default();
        assert!(
            written == text,
            "{file} is not what the engine answers: UPDATE_CONTRACT=1 rewrites it"
        );
    }

    /// The dashboard's types for the answers are these types, generated:
    /// a field renamed, added or typed otherwise here changes them there,
    /// and the dashboard's type-check reads its fixtures against them.
    #[test]
    fn the_dashboards_types_are_generated_from_these() {
        use ts_rs::TS;
        let cfg = ts_rs::Config::new().with_large_int("number");
        let decls = [
            (ReplicaState::docs(), ReplicaState::decl(&cfg)),
            (RowState::docs(), RowState::decl(&cfg)),
            (NoteKind::docs(), NoteKind::decl(&cfg)),
            (UpProgress::docs(), UpProgress::decl(&cfg)),
            (Backlog::docs(), Backlog::decl(&cfg)),
            (ReplicaOverview::docs(), ReplicaOverview::decl(&cfg)),
            (ReplicaCounts::docs(), ReplicaCounts::decl(&cfg)),
            (TableOverview::docs(), TableOverview::decl(&cfg)),
            (Listed::<()>::docs(), Listed::<()>::decl(&cfg)),
            (Parked::docs(), Parked::decl(&cfg)),
            (Quarantined::docs(), Quarantined::decl(&cfg)),
            (Unfiled::docs(), Unfiled::decl(&cfg)),
            (Failing::docs(), Failing::decl(&cfg)),
            (Health::docs(), Health::decl(&cfg)),
            (Overview::docs(), Overview::decl(&cfg)),
            (Listing::docs(), Listing::decl(&cfg)),
            (TableProgress::docs(), TableProgress::decl(&cfg)),
            (ReplicaProgress::docs(), ReplicaProgress::decl(&cfg)),
            (Progress::docs(), Progress::decl(&cfg)),
            (RowPlace::docs(), RowPlace::decl(&cfg)),
            (RowProgress::docs(), RowProgress::decl(&cfg)),
            (WishState::docs(), WishState::decl(&cfg)),
            (WishCounts::docs(), WishCounts::decl(&cfg)),
            (WishEntry::docs(), WishEntry::decl(&cfg)),
            (WishListing::docs(), WishListing::decl(&cfg)),
        ];
        let mut text = format!(
            "// Generated from magic_sync::status by its test \
             `the_dashboards_types_are_generated_from_these`: UPDATE_CONTRACT=1 rewrites it.\n\n\
             /** The contract these types describe: an answer of another is refused. */\n\
             export const CONTRACT = {CONTRACT}\n"
        );
        for (docs, decl) in decls {
            text.push('\n');
            // Rust's intra-doc links read as plain code in TypeScript.
            text.push_str(
                &docs
                    .unwrap_or_default()
                    .replace("[`", "`")
                    .replace("`]", "`"),
            );
            text.push_str("export ");
            text.push_str(&decl);
            text.push('\n');
        }
        matches_contract("types.ts", &text);
    }

    /// An empty answer is one that waits for nothing.
    #[test]
    fn a_health_with_nothing_in_it_is_healthy() {
        let health = Health::default();
        assert!(health.healthy);
        assert_eq!(
            health.pending + health.parked.total + health.failing.total,
            0
        );
    }

    fn note(partition: Option<Uuid>, replica: Option<Uuid>, parked: bool) -> NoteGroup {
        NoteGroup {
            table: None,
            partition,
            replica,
            parked,
            count: 1,
            oldest: Utc::now(),
        }
    }

    /// The fold gives each replica what the scan over every note did: a note
    /// concerns a replica when its partition and the replica it is about each
    /// match or are absent.
    #[test]
    fn folding_the_notes_counts_what_may_concern_each_replica() {
        let (p, q) = (Uuid::now_v7(), Uuid::now_v7());
        let (a, b, c) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
        let partition_of = HashMap::from([(a, Some(p)), (b, Some(p)), (c, Some(q))]);
        let mut notes = Vec::new();
        for partition in [None, Some(p), Some(q)] {
            for replica in [None, Some(a), Some(b), Some(c)] {
                for parked in [false, true] {
                    notes.push(note(partition, replica, parked));
                }
            }
        }
        let folded = Folded::of(&notes, &partition_of);
        for (&replica, &partition) in &partition_of {
            let concerns = |n: &&NoteGroup| {
                n.partition.is_none_or(|x| Some(x) == partition)
                    && n.replica.is_none_or(|r| r == replica)
            };
            let sum = |f: &dyn Fn(&NoteGroup) -> bool| -> i64 {
                notes
                    .iter()
                    .filter(concerns)
                    .filter(|n| f(n))
                    .map(|n| n.count)
                    .sum()
            };
            let counted = folded.count(replica, partition);
            assert_eq!(counted.pending, sum(&|n| !n.parked), "{replica}");
            assert_eq!(counted.parked, sum(&|n| n.parked), "{replica}");
            assert_eq!(
                counted.own_parked,
                sum(&|n| n.parked && n.replica.is_some()),
                "{replica}"
            );
        }
    }
}
