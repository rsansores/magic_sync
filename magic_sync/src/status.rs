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

use crate::plan::{parse_key, Engine};

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
pub struct Page {
    /// Entries skipped.
    pub offset: u32,
    /// Entries answered, at most.
    pub limit: u32,
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
    /// The contract this answer serializes as: [`CONTRACT`].
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
    /// The contract this answer serializes as: [`CONTRACT`].
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

/// Where a replica stands, in one word: the first of these that applies.
/// Ordered worst first. It reads back from its JSON too, for a host that
/// takes the state to list from a request.
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
    /// Changes are on their way to it, or from it — or held back by a change
    /// the planner set aside that may concern it.
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
    /// Replicas [`ReplicaState::AwaitingFix`].
    pub awaiting_fix: i64,
    /// Replicas [`ReplicaState::Unreachable`].
    pub unreachable: i64,
    /// Replicas [`ReplicaState::CatchingUp`].
    pub catching_up: i64,
    /// Replicas [`ReplicaState::Synced`].
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
    /// While it has messages it has not said it applied: since when it has
    /// not moved — the first one it did not confirm, or its last confirmation
    /// that left it behind. How long a replica has not answered.
    pub waiting_since: Option<DateTime<Utc>>,
    /// What it last said it wrote and the server has not filed, per table:
    /// `None` until it says.
    pub backlog: Option<Backlog>,
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
    /// backlog, unless it stops draining, which the host says when.
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
    /// Replicas sends keep failing to.
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

/// What a replica sent that the server could not file: a row, a delete, or a
/// whole upload.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct Unfiled {
    /// Its number in the inbox.
    pub id: i64,
    /// The replica that sent it.
    pub replica: Uuid,
    /// Empty for a whole upload kept as it came.
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

/// A replica sends to keep failing to.
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

    pub(super) fn parked(row: PgRow) -> sqlx::Result<Parked> {
        let kind = match row.try_get::<String, _>("kind")?.as_str() {
            "row" => NoteKind::Row,
            "members" => NoteKind::Members,
            "assigned" => NoteKind::Assigned,
            "resync" => NoteKind::Resync,
            other => {
                return Err(sqlx::Error::Decode(
                    format!("`{other}` is no kind of note").into(),
                ))
            }
        };
        Ok(Parked {
            id: row.try_get("id")?,
            kind,
            table: row.try_get("table")?,
            key: row.try_get("key")?,
            replica: row.try_get("replica")?,
            attempts: row.try_get("attempts")?,
            last_error: row.try_get("last_error")?,
            parked_at: row.try_get("parked_at")?,
        })
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
    /// The contract this answer serializes as: [`CONTRACT`].
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
    /// The contract this answer serializes as: [`CONTRACT`].
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

/// Whether a row a replica holds is confirmed: the message that carried it
/// is one the replica said it applied, in this epoch.
const CONFIRMED: &str =
    "coalesce(h.sequence <= r.applied_sequence AND r.applied_epoch = e.epoch, false)";

/// Whether a row a replica holds `h` has changed since it was sent: a
/// pending change of it by a transaction its snapshot does not see.
const OUTDATED: &str =
    "EXISTS (SELECT 1 FROM sync.notes n WHERE n.done_at IS NULL AND n.kind = 'row' \
                          AND n.relation = h.relation AND n.row_key = h.row_key \
                          AND NOT pg_visible_in_snapshot(n.xid, h.snapshot))";

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
    unfiled: i64,
    owed: i64,
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
            // A table that only travels with others is owed what the engine
            // decided to send it: there is no rule to count it from.
            let wanted = self.wanted_sql(i, "true");
            let ruled = wanted.is_some();
            let wanted = wanted.unwrap_or_else(|| {
                "SELECT NULL::uuid AS replica_id, NULL::text AS key WHERE false".to_owned()
            });
            let rows: Vec<(Uuid, i64, i64, i64, i64, i64)> = sqlx::query_as(&format!(
                "WITH w AS (SELECT replica_id, key FROM ({wanted}) x WHERE replica_id = ANY($2)), \
                 h AS (SELECT h.replica_id, h.row_key AS key, {CONFIRMED} AS confirmed, \
                              {OUTDATED} AS outdated \
                         FROM sync.held h CROSS JOIN sync.epoch e \
                         LEFT JOIN sync.replicas r ON r.replica_id = h.replica_id \
                        WHERE h.relation = $1 AND h.replica_id = ANY($2) AND h.refused IS NULL) \
                 SELECT coalesce(w.replica_id, h.replica_id), \
                        count(w.key) + CASE WHEN $3 THEN 0 ELSE count(h.key) END, \
                        count(*) FILTER (WHERE (NOT $3 OR w.key IS NOT NULL) AND h.confirmed), \
                        count(*) FILTER (WHERE (NOT $3 OR w.key IS NOT NULL) AND h.confirmed AND h.outdated), \
                        count(*) FILTER (WHERE (NOT $3 OR w.key IS NOT NULL) AND NOT h.confirmed), \
                        count(*) FILTER (WHERE $3 AND w.key IS NULL AND h.key IS NOT NULL) \
                   FROM w FULL JOIN h ON w.replica_id = h.replica_id AND w.key = h.key \
                  GROUP BY 1"
            ))
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

    /// Where one row stands on every replica of `scope` that holds it,
    /// should, or is owed it.
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
            "SELECT h.replica_id, {CONFIRMED}, \
                    {OUTDATED} \
               FROM sync.held h CROSS JOIN sync.epoch e \
               LEFT JOIN sync.replicas r ON r.replica_id = h.replica_id \
              WHERE h.relation = $1 AND h.row_key = $2 AND h.replica_id = ANY($3)"
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
                  ORDER BY n.id LIMIT $4"
            ))
            .bind(scoped.all)
            .bind(&scoped.partitions)
            .bind(scoped.ids())
            .bind(limit)
            .try_map(stored::parked)
            .fetch_all(&mut *conn)
            .await?,
        };
        let ids = scoped.ids();
        // Live replicas only, as every read: one that is no longer sent to
        // is no one's to wait for.
        let mine = "replica_id = ANY($1)";
        let quarantine = Listed {
            total: sqlx::query_scalar(&format!(
                "SELECT count(*) FROM sync.quarantine WHERE {mine}"
            ))
            .bind(&ids)
            .fetch_one(&mut *conn)
            .await?,
            first: sqlx::query(&format!(
                "SELECT replica_id AS replica, relation AS table, row_key AS key, reason, \
                        first_at, last_at, attempts \
                   FROM sync.quarantine WHERE {mine} \
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
                "SELECT id, replica_id AS replica, coalesce(relation, '') AS table, reason, \
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
        let recorded: Vec<Recorded> = sqlx::query_as(
            "SELECT s.id AS replica, r.failures, coalesce(r.last_error, c.dropped) AS last_error, \
                    r.retry_after AS retry_at, \
                    r.last_sent_at AS sent_at, r.applied_at, \
                    coalesce(c.last_sequence IS NOT NULL \
                             AND (r.applied_epoch IS DISTINCT FROM e.epoch \
                                  OR coalesce(r.applied_sequence, 0) < c.last_sequence), false) \
                      AS unconfirmed, \
                    r.backlog, r.backlog_at, r.waiting_since, \
                    coalesce(q.n, 0) AS quarantined, coalesce(i.n, 0) AS unfiled, \
                    coalesce(o.n, 0) AS owed \
               FROM unnest($1::uuid[]) AS s(id) \
               LEFT JOIN sync.replicas r ON r.replica_id = s.id \
               LEFT JOIN sync.chains c ON c.replica_id = s.id \
               LEFT JOIN sync.epoch e ON true \
               LEFT JOIN (SELECT replica_id, count(*) AS n FROM sync.quarantine \
                           WHERE replica_id = ANY($1) GROUP BY 1) q ON q.replica_id = s.id \
               LEFT JOIN (SELECT replica_id, count(*) AS n FROM sync.inbox \
                           WHERE replica_id = ANY($1) GROUP BY 1) i ON i.replica_id = s.id \
               LEFT JOIN (SELECT replica_id, count(*) AS n FROM sync.outbox \
                           WHERE replica_id = ANY($1) GROUP BY 1) o ON o.replica_id = s.id",
        )
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
                let state = if r.quarantined > 0 || r.unfiled > 0 || notes.own_parked > 0 {
                    ReplicaState::AwaitingFix
                } else if failures > 0 {
                    ReplicaState::Unreachable
                } else if notes.pending > 0 || notes.parked > 0 || r.unconfirmed || behind {
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
        // Only the replicas with a message unconfirmed have a row in flight:
        // the others' held rows are not read.
        let unconfirmed: Vec<Uuid> = fleet
            .replicas
            .iter()
            .filter(|r| r.unconfirmed)
            .map(|r| r.replica)
            .collect();
        let counted: Vec<(String, i64, i64, i64, i64)> = sqlx::query_as(&format!(
            "WITH c AS ( \
                 SELECT h.relation, h.replica_id, count(*) AS in_flight, 0 AS quarantined, 0 AS unfiled \
                   FROM sync.held h CROSS JOIN sync.epoch e \
                   LEFT JOIN sync.replicas r ON r.replica_id = h.replica_id \
                  WHERE h.replica_id = ANY($1) AND h.refused IS NULL AND NOT {CONFIRMED} \
                  GROUP BY 1, 2 \
                 UNION ALL \
                 SELECT relation, replica_id, 0, count(*), 0 FROM sync.quarantine \
                  WHERE replica_id = ANY($2) GROUP BY 1, 2 \
                 UNION ALL \
                 SELECT relation, replica_id, 0, 0, count(*) FROM sync.inbox \
                  WHERE replica_id = ANY($2) AND relation IS NOT NULL GROUP BY 1, 2) \
             SELECT relation, sum(in_flight)::int8, sum(quarantined)::int8, sum(unfiled)::int8, \
                    count(DISTINCT replica_id) \
               FROM c GROUP BY 1"
        ))
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
