//! What a replica can tell about itself, from its own database alone: for a
//! screen on the replica, read where it stands — without the server, which
//! is the one thing a replica that is out of sync may not reach.
//!
//! [`status`] reads the facts; [`ReplicaStatus::standing`] judges them, in
//! one word, the same way for every host.

use super::{owed, UpProgress};
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
    /// When the oldest upload the server has not said it filed was taken by
    /// the transport: how long the server has kept the replica waiting.
    pub unfiled_since: Option<DateTime<Utc>>,
    /// When the transport last took a message of the replica's, on any
    /// lane.
    pub spoken_at: Option<DateTime<Utc>>,
    /// When the replica last received a message from the server it could
    /// read, whatever it said.
    pub heard_at: Option<DateTime<Utc>>,
    /// When it last applied rows the server sent, or the end of a snapshot.
    pub applied_at: Option<DateTime<Utc>>,
    /// When the server last said it filed an upload of the replica's.
    pub filed_at: Option<DateTime<Utc>>,
    /// Where it stands in what the server sends it.
    pub receiving: Receiving,
    /// Rows the server sent that the replica could not take, per table: the
    /// server quarantines them and sends them again once a fix is deployed.
    pub refused: Vec<Refused>,
    /// Rows the replica wrote that cannot go up as they are, per table: they
    /// go once a build that can send them runs.
    pub set_aside: Vec<SetAside>,
    /// The settings the server wished, not carried out yet or not carried out
    /// at all; those carried out are not listed.
    pub wishes: Vec<HeldWish>,
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
    pub since: Option<DateTime<Utc>>,
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
    /// Why, as the latest of them was set aside.
    pub reason: String,
}

/// A setting the server wished, as the replica holds the wish.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct HeldWish {
    /// The setting's table.
    pub table: String,
    /// The setting row's key, its parts joined by `|`.
    pub key: String,
    /// Where it stands.
    pub state: WishAnswer,
    /// Why, for an answer that says.
    pub reason: Option<String>,
    /// What the server's host said of it when it wished it.
    pub note: Option<String>,
}

/// Where a wish the replica has not carried out stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum WishAnswer {
    /// Not carried out yet: its handler has not been offered it, or the
    /// answer has not been written.
    Pending,
    /// The replica cannot carry it out yet and tries again.
    Waiting,
    /// The replica will not carry it out.
    Refused,
    /// Based on a version of the setting the replica no longer held.
    Superseded,
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

/// Where a replica stands, in one word: the first of these that applies,
/// ordered worst first. It shares the server's words where they mean the
/// same (`awaiting_fix`, `catching_up`, `synced`), so whoever stands at the
/// replica and whoever reads the server's dashboard say the same thing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Standing {
    /// It does not know which replica it is yet: nothing syncs until it is
    /// set up.
    NotLinked,
    /// Rows wait for a fix to be deployed: ones the server sent that it could
    /// not take, or ones it wrote that this build cannot send.
    AwaitingFix,
    /// The host says its link to the server is down.
    Offline,
    /// The link is up, or cannot be told, and the server has not said it
    /// filed what the replica sent it for longer than the host's patience.
    NotAnswered,
    /// The server is sending it everything again: a snapshot is in progress,
    /// or it missed a message and waits for one.
    Resyncing,
    /// Changes are on their way: rows it wrote not yet filed, or a wished
    /// setting not carried out yet.
    CatchingUp,
    /// The server has filed everything it wrote, and nothing waits.
    Synced,
}

impl Standing {
    /// The word it is written as in JSON.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Standing::NotLinked => "not_linked",
            Standing::AwaitingFix => "awaiting_fix",
            Standing::Offline => "offline",
            Standing::NotAnswered => "not_answered",
            Standing::Resyncing => "resyncing",
            Standing::CatchingUp => "catching_up",
            Standing::Synced => "synced",
        }
    }
}

impl ReplicaStatus {
    /// Where the replica stands, by `link` — the host's word on its
    /// transport — at `now`. `patience` is how long an upload may go unfiled
    /// before the server is not answering: a few times the replica's
    /// keepalive (`Outbox::keepalive_every`) is a fair measure.
    #[must_use]
    pub fn standing(&self, link: Link, now: DateTime<Utc>, patience: Duration) -> Standing {
        let patience = chrono::Duration::from_std(patience).unwrap_or(chrono::Duration::MAX);
        if self.replica.is_none() {
            Standing::NotLinked
        } else if !self.refused.is_empty() || !self.set_aside.is_empty() {
            Standing::AwaitingFix
        } else if link == Link::Down {
            Standing::Offline
        } else if self
            .unfiled_since
            .is_some_and(|since| now.signed_duration_since(since) > patience)
        {
            Standing::NotAnswered
        } else if matches!(self.receiving, Receiving::Snapshot | Receiving::Missed) {
            Standing::Resyncing
        } else if self.up.iter().any(|t| t.unsent + t.unfiled > 0)
            || self
                .wishes
                .iter()
                .any(|w| matches!(w.state, WishAnswer::Pending | WishAnswer::Waiting))
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
    let unfiled_since: Option<DateTime<Utc>> = sqlx::query_scalar(
        "SELECT min(sent_at) FROM sync_notes \
          WHERE set_aside IS NULL AND sent_at IS NOT NULL AND writes = sent",
    )
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
    let applied: Option<(Option<String>, i64)> =
        sqlx::query_as("SELECT snapshot_id, gap FROM sync_applied WHERE id = 1")
            .fetch_optional(&mut *conn)
            .await?;
    let receiving = match applied {
        None => Receiving::Unprovisioned,
        Some((_, gap)) if gap > 0 => Receiving::Missed,
        Some((Some(_), _)) => Receiving::Snapshot,
        Some((None, _)) => Receiving::Following,
    };
    let refused = sqlx::query_as::<_, (String, i64, String, Option<DateTime<Utc>>, bool)>(
        "SELECT table_name, count(*), \
                (SELECT reason FROM sync_rejections l \
                  WHERE l.table_name = r.table_name AND l.held_back = 0 \
                  ORDER BY rejected_at DESC LIMIT 1), \
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
    let wishes = sqlx::query_as::<
        _,
        (
            String,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
        ),
    >(
        "SELECT relation, row_key, \
                CASE WHEN answered IS version THEN outcome END, reason, note \
           FROM sync_wishes \
          WHERE retired_at IS NULL \
            AND (answered IS NOT version OR outcome IS NOT 'applied') \
          ORDER BY relation, row_key",
    )
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .map(|(table, key, outcome, reason, note)| HeldWish {
        table,
        key,
        state: match outcome.as_deref() {
            Some("waiting") => WishAnswer::Waiting,
            Some("refused") => WishAnswer::Refused,
            Some("superseded") => WishAnswer::Superseded,
            _ => WishAnswer::Pending,
        },
        reason,
        note,
    })
    .collect();
    let at = |ms: Option<i64>| ms.and_then(DateTime::from_timestamp_millis);
    Ok(ReplicaStatus {
        replica,
        up,
        unfiled_since,
        spoken_at: at(spoken_at),
        heard_at: at(heard_at),
        applied_at: at(applied_at),
        filed_at: at(filed_at),
        receiving,
        refused,
        set_aside,
        wishes,
    })
}
