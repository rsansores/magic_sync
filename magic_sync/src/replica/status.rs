//! What a replica can tell about itself, from its own database alone: for a
//! screen on the replica, read where it stands — without the server, which
//! is the one thing a replica that is out of sync may not reach.
//!
//! [`status`] reads the facts; [`ReplicaStatus::standing`] judges them, in
//! one word, the same way for every host.

use super::{owed, Outbox, UpProgress};
use crate::wish::WishState;
use crate::{Conn, Error};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::time::Duration;
use uuid::Uuid;

/// Everything a replica records about how its sync stands. Read it with
/// [`status`]; judge it with [`ReplicaStatus::standing`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct ReplicaStatus {
    /// Which replica this is; `None` until it first runs knowing, and
    /// nothing goes up until then.
    pub replica: Option<Uuid>,
    /// What it wrote that has not reached the server, per table with
    /// something on its way.
    pub up: Vec<UpProgress>,
    /// When the transport first took the oldest upload the server has not
    /// filed: a resend does not move it, so it is how long the server has
    /// kept the replica waiting.
    pub unfiled_since: Option<DateTime<Utc>>,
    /// When the transport last took a message of the replica's, on any
    /// lane.
    pub spoken_at: Option<DateTime<Utc>>,
    /// When the replica last received a message from the server it could
    /// read, whatever it said. A server with nothing to send sends nothing,
    /// so a time long past is no fault by itself.
    pub heard_at: Option<DateTime<Utc>>,
    /// When it last applied rows the server sent, or the end of a snapshot.
    pub applied_at: Option<DateTime<Utc>>,
    /// When the server last said it filed an upload of the replica's.
    pub filed_at: Option<DateTime<Utc>>,
    /// Where it stands in what the server sends it.
    pub receiving: Receiving,
    /// It applied a message the server has not been told of yet: the
    /// server counts it unconfirmed until the replica next says so.
    pub unconfirmed: bool,
    /// Rows the server sent that the replica could not take, per table: the
    /// server quarantines them and sends them again once a fix is deployed.
    pub refused: Vec<Refused>,
    /// Rows the replica wrote that cannot go up as they are, per table, with
    /// why: they go once a build that can send them runs. The same rows
    /// `up` counts as `set_aside`.
    pub set_aside: Vec<SetAside>,
    /// The settings the server wished that the replica has not carried out,
    /// per table and answer; those carried out are not counted.
    pub wishes: Vec<HeldWishes>,
}

/// Where a replica stands in what the server sends it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Receiving {
    /// Nothing received yet: the server has not provisioned it.
    Unprovisioned,
    /// It follows the server message by message.
    Following,
    /// A snapshot is in progress: the server is sending it everything again.
    Snapshot,
    /// It missed a message and applies nothing until the server provisions
    /// it again, which the server has been told.
    Missed,
}

/// Rows of one table the replica could not take.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct Refused {
    /// The table.
    pub table: String,
    /// How many rows.
    pub rows: i64,
    /// Why the latest of them was refused.
    pub reason: String,
    /// When the oldest was refused.
    pub since: DateTime<Utc>,
    /// Whether the server has been told of every one of them.
    pub reported: bool,
}

/// Rows of one table the replica wrote that wait for a build that can send
/// them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct SetAside {
    /// The table.
    pub table: String,
    /// How many rows.
    pub rows: i64,
    /// Why, as the most recently noted of them says.
    pub reason: String,
}

/// The wishes of one table's settings that stand at one answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct HeldWishes {
    /// The settings' table.
    pub table: String,
    /// Where they stand: never `applied`.
    pub state: WishState,
    /// How many.
    pub rows: i64,
    /// Why, as the latest of their answers that gives a reason says.
    pub reason: Option<String>,
}

/// The state of the replica's link to the server, which only the host knows:
/// the engine sees messages, not the transport under them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Link {
    /// The transport is connected.
    Up,
    /// The transport is not connected.
    Down,
    /// The host cannot tell.
    Unknown,
}

/// How long [`ReplicaStatus::standing`] lets things wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Waits {
    patience: Duration,
    resend_after: Duration,
    behind_after: Duration,
}

impl Waits {
    /// The waits of a replica sending through `outbox`: an upload may go
    /// unfiled for two of its `resend_after` — a healthy one that was lost
    /// is sent again after one — and a row is behind once it has waited
    /// the server's default `Settings::behind_after`.
    #[must_use]
    pub fn new(outbox: &Outbox) -> Self {
        Waits {
            patience: outbox.resend_after.saturating_mul(2),
            resend_after: outbox.resend_after,
            behind_after: crate::BEHIND_AFTER,
        }
    }

    /// How long the server may keep an upload unfiled before it is not
    /// answering: never less than the outbox's `resend_after`, which a lost
    /// upload waits out.
    #[must_use]
    pub fn patience(self, patience: Duration) -> Self {
        Waits {
            patience: patience.max(self.resend_after),
            ..self
        }
    }

    /// How long a row may wait before it is behind: the server's own
    /// `Settings::behind_after`, so both say the same of the replica.
    #[must_use]
    pub fn behind_after(self, behind_after: Duration) -> Self {
        Waits {
            behind_after,
            ..self
        }
    }
}

/// Where a replica stands, in one word: the first of these that applies,
/// ordered worst first. `awaiting_fix`, `catching_up` and `synced` are the
/// server's words (`status::ReplicaState`), judged by the same rule over the
/// half of the facts a replica holds: what it wrote and what it applied. The
/// server also counts what it has yet to send, which the replica cannot see,
/// so a replica can be synced by its own account while the dashboard has it
/// catching up. With the server's `behind_after` ([`Waits::behind_after`])
/// and clocks that agree, never the other way about. The rest only the
/// replica can tell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Standing {
    /// It does not know which replica it is yet: nothing syncs until it is
    /// set up.
    NotLinked,
    /// Rows the server sent wait for a fix to be deployed: ones the replica
    /// could not take.
    AwaitingFix,
    /// The host says its link to the server is down.
    Offline,
    /// It knows which it is, and the server has sent it nothing yet: the
    /// server has not provisioned it.
    NotProvisioned,
    /// The link is up, or cannot be told, an upload has gone unfiled for
    /// longer than the patience of its [`Waits`], and the server has filed
    /// nothing in that time: a row written faster than the server answers
    /// waits for ever on one upload after another, and is not this.
    NotAnswered,
    /// The server is sending it everything again: a snapshot is in progress,
    /// or it missed a message and waits for one.
    Resyncing,
    /// Changes wait: rows it wrote that have waited longer than
    /// `behind_after`, or that cannot go as they are; a message it applied
    /// that the server has not been told of; or a wished setting it has not
    /// answered.
    CatchingUp,
    /// Nothing waits: what it wrote is filed, or flows.
    Synced,
}

impl Standing {
    /// Every standing, worst first.
    pub const ALL: [Standing; 8] = [
        Standing::NotLinked,
        Standing::AwaitingFix,
        Standing::Offline,
        Standing::NotProvisioned,
        Standing::NotAnswered,
        Standing::Resyncing,
        Standing::CatchingUp,
        Standing::Synced,
    ];

    /// The word it is written as in JSON.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Standing::NotLinked => "not_linked",
            Standing::AwaitingFix => "awaiting_fix",
            Standing::Offline => "offline",
            Standing::NotProvisioned => "not_provisioned",
            Standing::NotAnswered => "not_answered",
            Standing::Resyncing => "resyncing",
            Standing::CatchingUp => "catching_up",
            Standing::Synced => "synced",
        }
    }
}

impl ReplicaStatus {
    /// Where the replica stands at `now`, by `link` — the host's word on its
    /// transport — and the waits `waits` allows.
    #[must_use]
    pub fn standing(&self, link: Link, now: DateTime<Utc>, waits: Waits) -> Standing {
        let ago = |wait: Duration| {
            chrono::Duration::from_std(wait)
                .ok()
                .and_then(|wait| now.checked_sub_signed(wait))
        };
        let patience = ago(waits.patience);
        if self.replica.is_none() {
            Standing::NotLinked
        } else if !self.refused.is_empty() {
            Standing::AwaitingFix
        } else if link == Link::Down {
            Standing::Offline
        } else if self.receiving == Receiving::Unprovisioned {
            Standing::NotProvisioned
        } else if patience.is_some_and(|limit| {
            self.unfiled_since.is_some_and(|since| since < limit)
                && self.filed_at.is_none_or(|filed| filed < limit)
        }) {
            Standing::NotAnswered
        } else if matches!(self.receiving, Receiving::Snapshot | Receiving::Missed) {
            Standing::Resyncing
        } else if self.unconfirmed
            || self.up.iter().any(|t| t.waits(ago(waits.behind_after)))
            || self.wishes.iter().any(|w| w.state == WishState::Pending)
        {
            Standing::CatchingUp
        } else {
            Standing::Synced
        }
    }
}

/// How the replica's sync stands, from its own database: the reads it makes
/// are each consistent, and together as consistent as the connection's
/// transaction — call it in one for a single snapshot of them.
///
/// # Errors
///
/// [`Error::Database`] when the database fails.
pub async fn status(conn: &mut Conn) -> Result<ReplicaStatus, Error> {
    let replica: Option<Uuid> = sqlx::query_scalar("SELECT replica FROM sync_replica WHERE id = 1")
        .fetch_optional(&mut *conn)
        .await?;
    let up = owed(conn, 0).await?;
    let unfiled_since: Option<i64> =
        sqlx::query_scalar("SELECT min(first_sent_at) FROM sync_notes WHERE set_aside IS NULL")
            .fetch_one(&mut *conn)
            .await?;
    let spoken_at: Option<i64> = sqlx::query_scalar("SELECT at FROM sync_spoken WHERE id = 1")
        .fetch_optional(&mut *conn)
        .await?;
    let heard: Option<(Option<i64>, Option<i64>, Option<i64>)> =
        sqlx::query_as("SELECT heard_at, applied_at, filed_at FROM sync_heard WHERE id = 1")
            .fetch_optional(&mut *conn)
            .await?;
    let (heard_at, applied_at, filed_at) = heard.unwrap_or_default();
    let applied: Option<(Option<String>, i64, bool)> = sqlx::query_as(
        "SELECT snapshot_id, gap, acknowledged < sequence FROM sync_applied WHERE id = 1",
    )
    .fetch_optional(&mut *conn)
    .await?;
    let receiving = match applied {
        None => Receiving::Unprovisioned,
        Some((_, gap, _)) if gap > 0 => Receiving::Missed,
        Some((Some(_), _, _)) => Receiving::Snapshot,
        Some((None, _, _)) => Receiving::Following,
    };
    let unconfirmed = applied.is_some_and(|(_, _, unconfirmed)| unconfirmed);
    let refused = sqlx::query_as::<_, (String, i64, String, DateTime<Utc>, bool)>(
        "SELECT table_name, count(*), \
                (SELECT reason FROM sync_rejections l \
                  WHERE l.table_name = r.table_name AND l.held_back = 0 \
                  ORDER BY rejected_at DESC, rowid DESC LIMIT 1), \
                min(rejected_at), \
                min(reported_at IS NOT NULL) \
           FROM sync_rejections r WHERE held_back = 0 \
          GROUP BY table_name ORDER BY table_name",
    )
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .map(|(table, rows, reason, since, reported)| Refused {
        table,
        rows,
        reason,
        since,
        reported,
    })
    .collect();
    let set_aside = sqlx::query_as::<_, (String, i64, String)>(
        "SELECT relation, count(*), \
                (SELECT set_aside FROM sync_notes l \
                  WHERE l.relation = n.relation AND l.set_aside IS NOT NULL \
                  ORDER BY id DESC LIMIT 1) \
           FROM sync_notes n WHERE set_aside IS NOT NULL \
          GROUP BY relation ORDER BY relation",
    )
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .map(|(table, rows, reason)| SetAside {
        table,
        rows,
        reason,
    })
    .collect();
    // Each wish not carried out, as its answer stands: none to its version
    // is pending, whatever an earlier version's answer said.
    let answers = sqlx::query_as::<_, (String, Option<String>, Option<String>)>(
        "SELECT relation, CASE WHEN answered IS version THEN outcome END, \
                CASE WHEN answered IS version THEN reason END \
           FROM sync_wishes \
          WHERE retired_at IS NULL \
            AND (answered IS NOT version OR outcome IS NOT 'applied') \
          ORDER BY relation, answered_at",
    )
    .fetch_all(&mut *conn)
    .await?;
    let mut wishes: Vec<HeldWishes> = Vec::new();
    for (table, outcome, reason) in answers {
        // No answer to the wish's version is pending.
        let state = WishState::shown(outcome.as_deref().unwrap_or("pending"));
        match wishes
            .iter_mut()
            .find(|w| w.table == table && w.state == state)
        {
            Some(held) => {
                held.rows += 1;
                held.reason = reason.or(held.reason.take());
            }
            None => wishes.push(HeldWishes {
                table,
                state,
                rows: 1,
                reason,
            }),
        }
    }
    wishes.sort_by(|a, b| (&a.table, a.state).cmp(&(&b.table, b.state)));
    let at = |ms: Option<i64>| ms.and_then(DateTime::from_timestamp_millis);
    Ok(ReplicaStatus {
        replica,
        up,
        unfiled_since: at(unfiled_since),
        spoken_at: at(spoken_at),
        heard_at: at(heard_at),
        applied_at: at(applied_at),
        filed_at: at(filed_at),
        receiving,
        unconfirmed,
        refused,
        set_aside,
        wishes,
    })
}
