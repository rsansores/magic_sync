//! What the engine answers about itself, for a dashboard or an alarm: whether
//! each replica holds what it should, how far along it is, where one row
//! stands, and what waits for a fix. Every read takes a [`Scope`], so a host
//! shows each audience its own replicas and nothing else; the engine knows
//! replicas only by id, and the host puts names on them.
//!
//! Every answer serializes, as the JSON a dashboard reads.

use crate::plan::{parse_key, Engine};
use crate::replica::UpProgress;
use crate::Error;
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use uuid::Uuid;

type Conn = crate::row::Conn;

/// Which replicas a read covers. Only live replicas are counted: a partition
/// or every replica means its live ones, and a list is narrowed to them.
#[derive(Clone, Copy, Debug)]
pub enum Scope<'a> {
    Replicas(&'a [Uuid]),
    Partition(Uuid),
    All,
}

/// How far one replica is from holding what it should.
#[derive(Debug, Clone, Serialize)]
pub struct ReplicaProgress {
    pub replica: Uuid,
    pub tables: Vec<TableProgress>,
}

/// One table on one replica, counted from what the replica acknowledged
/// applying. `holds - outdated` of `should` is how far along it is; the
/// numbers never count a row as held that the replica has not confirmed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct TableProgress {
    pub table: &'static str,
    /// Rows its rules say it should hold. For a table that only travels with
    /// others, what the engine decided to send it.
    pub should: i64,
    /// Rows it should hold and has confirmed it holds.
    pub holds: i64,
    /// Of those, rows changed since: a newer version is on its way or owed.
    pub outdated: i64,
    /// Rows sent and not confirmed yet.
    pub in_flight: i64,
    /// Rows it holds and should no longer: on their way out.
    pub to_remove: i64,
    /// Rows set aside for it until a fix.
    pub quarantined: i64,
}

impl TableProgress {
    /// Rows it holds as they should be.
    pub fn up_to_date(&self) -> i64 {
        self.holds - self.outdated
    }
}

/// Where one row stands on one replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RowState {
    /// It holds the row as it is.
    Holds,
    /// It holds an older version; the newer one is on its way or owed.
    Outdated,
    /// Sent, not yet confirmed.
    OnItsWay,
    /// It should hold the row and does not yet.
    Missing,
    /// It holds the row and should no longer.
    Leaving,
    /// Set aside for it until a fix.
    Quarantined,
}

/// Where a replica stands, in one word: the first of these that applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicaState {
    /// Something is set aside for it until a fix is deployed: a row it is
    /// owed and cannot be sent or could not take, a row it sent that could
    /// not be filed, or a change the planner could not plan.
    AwaitingFix,
    /// Sends to it fail; it is tried again at `retry_at`.
    Unreachable,
    /// Changes are on their way to it, or from it.
    CatchingUp,
    /// It holds what it should, and the server has filed everything it last
    /// said it wrote.
    Synced,
}

/// One replica, from what the engine records about it: nothing here counts
/// the rows it should hold, so it costs the same for a fleet as for one.
/// Whether it is synced is exact all the same: a change it lacks is either a
/// note not yet planned, a message it has not confirmed, or set aside.
#[derive(Debug, Clone, Serialize)]
pub struct ReplicaOverview {
    pub replica: Uuid,
    pub state: ReplicaState,
    /// Changes noted and not yet planned that may concern it.
    pub pending: i64,
    /// Of those, the ones owed to it alone after a send to it failed.
    pub owed: i64,
    /// Changes the planner set aside that may concern it.
    pub parked: i64,
    /// Whether it has messages it has not said it applied.
    pub unconfirmed: bool,
    /// Rows set aside for it until a fix.
    pub quarantined: i64,
    /// Rows it sent that wait in the inbox until a fix.
    pub unfiled: i64,
    /// Sends to it that failed in a row, and the last one's error.
    pub failures: i32,
    pub last_error: Option<String>,
    /// When it is tried again, while it waits out a failure.
    pub retry_at: Option<DateTime<Utc>>,
    /// When it was last sent a message, and when it last said it applied one.
    pub sent_at: Option<DateTime<Utc>>,
    pub applied_at: Option<DateTime<Utc>>,
    /// What it last said it wrote and the server has not filed, per table:
    /// `None` until it says.
    pub backlog: Option<Backlog>,
}

/// What a replica wrote and the server has not filed, as of when it said so.
#[derive(Debug, Clone, Serialize)]
pub struct Backlog {
    pub tables: Vec<UpProgress>,
    pub at: DateTime<Utc>,
}

impl ReplicaOverview {
    fn settle(&mut self) {
        let behind = self.backlog.as_ref().is_some_and(|b| !b.tables.is_empty());
        self.state = if self.quarantined > 0 || self.parked > 0 || self.unfiled > 0 {
            ReplicaState::AwaitingFix
        } else if self.failures > 0 {
            ReplicaState::Unreachable
        } else if self.pending > 0 || self.unconfirmed || behind {
            ReplicaState::CatchingUp
        } else {
            ReplicaState::Synced
        };
    }
}

/// One table over the replicas of a scope: every figure zero means each of
/// them holds it as it should. It is read without counting what any replica
/// should hold, or which of them a pending change will reach.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct TableOverview {
    pub table: String,
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
#[derive(Debug, Clone, Default, Serialize)]
pub struct Health {
    /// Changes noted and not yet planned, and when the oldest was noted.
    pub pending: i64,
    pub oldest: Option<DateTime<Utc>>,
    pub parked: Listed<Parked>,
    pub quarantine: Listed<Quarantined>,
    pub inbox: Listed<Unfiled>,
    pub failing: Listed<Failing>,
}

impl Health {
    /// Nothing waits for a fix and every replica can be reached. A backlog is
    /// not unhealthy; one that stops draining is, and the host says when.
    pub fn healthy(&self) -> bool {
        self.parked.total == 0
            && self.quarantine.total == 0
            && self.inbox.total == 0
            && self.failing.total == 0
    }
}

/// The first entries of a list, and how many there are.
#[derive(Debug, Clone, Serialize)]
pub struct Listed<T> {
    pub total: i64,
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

/// A change the planner set aside.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct Parked {
    pub id: i64,
    pub kind: String,
    pub table: Option<String>,
    pub key: Option<String>,
    pub replica: Option<Uuid>,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub parked_at: Option<DateTime<Utc>>,
}

/// A row set aside for one replica.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct Quarantined {
    pub replica: Uuid,
    pub table: String,
    pub key: String,
    pub reason: String,
    pub first_at: DateTime<Utc>,
    pub last_at: DateTime<Utc>,
    pub attempts: i32,
}

/// A row a replica sent that the server could not file.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct Unfiled {
    pub id: i64,
    pub replica: Uuid,
    pub table: String,
    pub reason: String,
    pub first_at: DateTime<Utc>,
    pub last_at: DateTime<Utc>,
    pub attempts: i32,
}

/// A replica sends to keep failing to.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct Failing {
    pub replica: Uuid,
    pub failures: i32,
    pub last_error: Option<String>,
    pub retry_at: Option<DateTime<Utc>>,
}

/// Everything a dashboard shows first, in one read.
#[derive(Debug, Clone, Serialize)]
pub struct Overview {
    pub health: Health,
    pub replicas: Vec<ReplicaOverview>,
    pub tables: Vec<TableOverview>,
}

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

    /// Whether a note — of a partition, owed to one replica, or about one —
    /// may concern `replica` of `partition`.
    fn concerns(note: &NoteGroup, replica: Uuid, partition: Option<Uuid>) -> bool {
        note.partition.is_none_or(|p| Some(p) == partition)
            && note.scope.is_none_or(|s| s == replica)
            && note.replica.is_none_or(|r| r == replica)
    }
}

/// Rows of a table, counted for one replica.
type Counted = (String, Uuid, i64);

/// Pending notes, counted by what they may concern.
#[derive(sqlx::FromRow)]
struct NoteGroup {
    table: Option<String>,
    partition: Option<Uuid>,
    scope: Option<Uuid>,
    replica: Option<Uuid>,
    parked: bool,
    count: i64,
    oldest: DateTime<Utc>,
}

/// The pending notes a scope sees: `$1` every one, else those of the
/// partitions `$2` (or of none), owed to and about the replicas `$3` (or any).
const NOTES_IN_SCOPE: &str = "n.done_at IS NULL AND ($1 OR ( \
       (n.partition_id IS NULL OR n.partition_id = ANY($2)) \
   AND (n.scope IS NULL OR n.scope = ANY($3)) \
   AND (n.replica_id IS NULL OR n.replica_id = ANY($3))))";

impl Engine {
    /// How far each replica of `scope` is from holding what it should, per
    /// table, counted from what each has acknowledged applying — never from
    /// what was merely sent. See [`TableProgress`].
    pub async fn progress(
        &self,
        conn: &mut Conn,
        scope: Scope<'_>,
    ) -> Result<Vec<ReplicaProgress>, Error> {
        let replicas = self.scoped(conn, scope).await?.ids();
        let epoch: Option<Uuid> = sqlx::query_scalar("SELECT epoch FROM sync.epoch")
            .fetch_optional(&mut *conn)
            .await?;
        let mut out: BTreeMap<Uuid, ReplicaProgress> = replicas
            .iter()
            .map(|r| {
                (
                    *r,
                    ReplicaProgress {
                        replica: *r,
                        tables: Vec::new(),
                    },
                )
            })
            .collect();
        let quarantined: Vec<(Uuid, String, i64)> = sqlx::query_as(
            "SELECT replica_id, relation, count(*) FROM sync.quarantine \
              WHERE replica_id = ANY($1) GROUP BY 1, 2",
        )
        .bind(&replicas)
        .fetch_all(&mut *conn)
        .await?;
        for &i in &self.order {
            let plan = &self.plans[i];
            // A table that only travels with others is owed what the engine
            // decided to send it: there is no rule to count it from.
            let wanted = self.wanted_sql(i, "true").unwrap_or_else(|| {
                "SELECT NULL::uuid AS replica_id, NULL::text AS key WHERE false".to_owned()
            });
            let rows: Vec<(Uuid, i64, i64, i64, i64, i64)> = sqlx::query_as(&format!(
                "WITH w AS (SELECT replica_id, key FROM ({wanted}) x WHERE replica_id = ANY($2)), \
                 p AS (SELECT DISTINCT ON (n.row_key) n.row_key, n.xid FROM sync.notes n \
                        WHERE n.done_at IS NULL AND n.kind = 'row' AND n.relation = $1 \
                        ORDER BY n.row_key, n.xid DESC), \
                 h AS (SELECT h.replica_id, h.row_key AS key, \
                              coalesce(h.sequence <= r.applied_sequence AND r.applied_epoch = $3, false) AS confirmed, \
                              coalesce(p.xid >= h.horizon, false) AS outdated \
                         FROM sync.held h LEFT JOIN sync.replicas r ON r.replica_id = h.replica_id \
                         LEFT JOIN p ON p.row_key = h.row_key \
                        WHERE h.relation = $1 AND h.replica_id = ANY($2)) \
                 SELECT coalesce(w.replica_id, h.replica_id), \
                        count(w.key) + CASE WHEN $4 THEN count(h.key) ELSE 0 END, \
                        count(*) FILTER (WHERE ($4 OR w.key IS NOT NULL) AND h.confirmed), \
                        count(*) FILTER (WHERE ($4 OR w.key IS NOT NULL) AND h.confirmed AND h.outdated), \
                        count(*) FILTER (WHERE h.key IS NOT NULL AND NOT h.confirmed), \
                        count(*) FILTER (WHERE NOT $4 AND w.key IS NULL AND h.confirmed) \
                   FROM w FULL JOIN h ON w.replica_id = h.replica_id AND w.key = h.key \
                  GROUP BY 1"
            ))
            .bind(plan.table)
            .bind(&replicas)
            .bind(epoch)
            .bind(self.wanted_sql(i, "true").is_none())
            .fetch_all(&mut *conn)
            .await?;
            let counted: BTreeMap<Uuid, (i64, i64, i64, i64, i64)> = rows
                .into_iter()
                .map(|(r, should, holds, outdated, in_flight, to_remove)| {
                    (r, (should, holds, outdated, in_flight, to_remove))
                })
                .collect();
            for (replica, progress) in out.iter_mut() {
                let (should, holds, outdated, in_flight, to_remove) =
                    counted.get(replica).copied().unwrap_or_default();
                let quarantined = quarantined
                    .iter()
                    .find(|(r, t, _)| r == replica && t == plan.table)
                    .map_or(0, |(_, _, n)| *n);
                progress.tables.push(TableProgress {
                    table: plan.table,
                    should: should
                        + if self.wanted_sql(i, "true").is_none() {
                            quarantined
                        } else {
                            0
                        },
                    holds,
                    outdated,
                    in_flight,
                    to_remove,
                    quarantined,
                });
            }
        }
        Ok(out.into_values().collect())
    }

    /// Where one row stands on every replica of `scope` that holds it,
    /// should, or is owed it.
    pub async fn row_progress(
        &self,
        conn: &mut Conn,
        scope: Scope<'_>,
        table: &str,
        key: &str,
    ) -> Result<Vec<(Uuid, RowState)>, Error> {
        let scoped: BTreeSet<Uuid> = self.scoped(conn, scope).await?.ids().into_iter().collect();
        let i = self
            .index(table)
            .ok_or_else(|| Error::Misuse(format!("`{table}` is not mirrored")))?;
        parse_key(&self.plans[i].key, key)?;
        let keys = [key.to_owned()];
        let wanted: BTreeSet<Uuid> = self
            .wanted(conn, i, Some(&keys), None)
            .await?
            .into_iter()
            .map(|(r, _)| r)
            .collect();
        let rule = self.wanted_sql(i, "true").is_some();
        let held: Vec<(Uuid, bool, bool)> = sqlx::query_as(
            "SELECT h.replica_id, \
                    coalesce(h.sequence <= r.applied_sequence AND r.applied_epoch = e.epoch, false), \
                    EXISTS (SELECT 1 FROM sync.notes n WHERE n.done_at IS NULL AND n.kind = 'row' \
                             AND n.relation = h.relation AND n.row_key = h.row_key AND n.xid >= h.horizon) \
               FROM sync.held h CROSS JOIN sync.epoch e \
               LEFT JOIN sync.replicas r ON r.replica_id = h.replica_id \
              WHERE h.relation = $1 AND h.row_key = $2",
        )
        .bind(table)
        .bind(key)
        .fetch_all(&mut *conn)
        .await?;
        let quarantined: BTreeSet<Uuid> = sqlx::query_scalar(
            "SELECT replica_id FROM sync.quarantine WHERE relation = $1 AND row_key = $2",
        )
        .bind(table)
        .bind(key)
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .collect();
        let mut out: BTreeMap<Uuid, RowState> = BTreeMap::new();
        for replica in &wanted {
            out.insert(*replica, RowState::Missing);
        }
        for (replica, confirmed, outdated) in held {
            let state = match (confirmed, outdated) {
                (false, _) => RowState::OnItsWay,
                (true, _) if rule && !wanted.contains(&replica) => RowState::Leaving,
                (true, true) => RowState::Outdated,
                (true, false) => RowState::Holds,
            };
            out.insert(replica, state);
        }
        for replica in quarantined {
            out.insert(replica, RowState::Quarantined);
        }
        Ok(out
            .into_iter()
            .filter(|(replica, _)| scoped.contains(replica))
            .collect())
    }

    /// Everything a dashboard shows first, over `scope`: the engine's health,
    /// each replica's state, and each table's, with up to `limit` entries of
    /// each list in the health.
    pub async fn overview(
        &self,
        conn: &mut Conn,
        scope: Scope<'_>,
        limit: i64,
    ) -> Result<Overview, Error> {
        let scoped = self.scoped(conn, scope).await?;
        let notes = self.notes(conn, &scoped).await?;
        let health = self.health_of(conn, &scoped, &notes, limit).await?;
        let replicas = self.replicas_of(conn, &scoped, &notes).await?;
        let tables = self.tables_of(conn, &scoped, &notes, &replicas).await?;
        Ok(Overview {
            health,
            replicas,
            tables,
        })
    }

    /// What the engine is waiting on over `scope`, with up to `limit`
    /// entries of each list.
    pub async fn health(
        &self,
        conn: &mut Conn,
        scope: Scope<'_>,
        limit: i64,
    ) -> Result<Health, Error> {
        let scoped = self.scoped(conn, scope).await?;
        let notes = self.notes(conn, &scoped).await?;
        self.health_of(conn, &scoped, &notes, limit).await
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
            "SELECT n.relation AS table, n.partition_id AS partition, n.scope, \
                    n.replica_id AS replica, n.parked_at IS NOT NULL AS parked, \
                    count(*) AS count, min(n.noted_at) AS oldest \
               FROM sync.notes n WHERE {NOTES_IN_SCOPE} GROUP BY 1, 2, 3, 4, 5"
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
        limit: i64,
    ) -> Result<Health, Error> {
        let waiting = notes.iter().filter(|n| !n.parked);
        let pending = waiting.clone().map(|n| n.count).sum();
        let oldest = waiting.map(|n| n.oldest).min();
        let parked = Listed {
            total: notes.iter().filter(|n| n.parked).map(|n| n.count).sum(),
            first: sqlx::query_as(&format!(
                "SELECT n.id, n.kind, n.relation AS table, n.row_key AS key, \
                        n.replica_id AS replica, n.attempts, n.last_error, n.parked_at \
                   FROM sync.notes n WHERE {NOTES_IN_SCOPE} AND n.parked_at IS NOT NULL \
                  ORDER BY n.id LIMIT $4"
            ))
            .bind(scoped.all)
            .bind(&scoped.partitions)
            .bind(scoped.ids())
            .bind(limit)
            .fetch_all(&mut *conn)
            .await?,
        };
        let ids = scoped.ids();
        // Every replica's, when the scope is all of them: a row may wait for
        // one that is no longer live.
        let mine = "($1 OR replica_id = ANY($2))";
        let quarantine = Listed {
            total: sqlx::query_scalar(&format!(
                "SELECT count(*) FROM sync.quarantine WHERE {mine}"
            ))
            .bind(scoped.all)
            .bind(&ids)
            .fetch_one(&mut *conn)
            .await?,
            first: sqlx::query_as(&format!(
                "SELECT replica_id AS replica, relation AS table, row_key AS key, reason, \
                        first_at, last_at, attempts \
                   FROM sync.quarantine WHERE {mine} ORDER BY first_at LIMIT $3"
            ))
            .bind(scoped.all)
            .bind(&ids)
            .bind(limit)
            .fetch_all(&mut *conn)
            .await?,
        };
        let inbox = Listed {
            total: sqlx::query_scalar(&format!("SELECT count(*) FROM sync.inbox WHERE {mine}"))
                .bind(scoped.all)
                .bind(&ids)
                .fetch_one(&mut *conn)
                .await?,
            first: sqlx::query_as(&format!(
                "SELECT id, replica_id AS replica, relation AS table, reason, \
                        first_at, last_at, attempts \
                   FROM sync.inbox WHERE {mine} ORDER BY id LIMIT $3"
            ))
            .bind(scoped.all)
            .bind(&ids)
            .bind(limit)
            .fetch_all(&mut *conn)
            .await?,
        };
        let failing = Listed {
            total: sqlx::query_scalar(&format!(
                "SELECT count(*) FROM sync.replicas WHERE failures > 0 AND {mine}"
            ))
            .bind(scoped.all)
            .bind(&ids)
            .fetch_one(&mut *conn)
            .await?,
            first: sqlx::query_as(&format!(
                "SELECT replica_id AS replica, failures, last_error, retry_after AS retry_at \
                   FROM sync.replicas WHERE failures > 0 AND {mine} \
                  ORDER BY failures DESC, replica_id LIMIT $3"
            ))
            .bind(scoped.all)
            .bind(&ids)
            .bind(limit)
            .fetch_all(&mut *conn)
            .await?,
        };
        Ok(Health {
            pending,
            oldest,
            parked,
            quarantine,
            inbox,
            failing,
        })
    }

    async fn replicas_of(
        &self,
        conn: &mut Conn,
        scoped: &Scoped,
        notes: &[NoteGroup],
    ) -> Result<Vec<ReplicaOverview>, Error> {
        type Row = (
            Uuid,
            Option<i32>,
            Option<String>,
            Option<DateTime<Utc>>,
            Option<DateTime<Utc>>,
            Option<DateTime<Utc>>,
            bool,
            Option<serde_json::Value>,
            Option<DateTime<Utc>>,
            i64,
            i64,
        );
        let rows: Vec<Row> = sqlx::query_as(
            "SELECT s.id, r.failures, r.last_error, r.retry_after, r.last_sent_at, r.applied_at, \
                    coalesce(r.last_sequence IS NOT NULL \
                             AND (r.applied_epoch IS DISTINCT FROM e.epoch \
                                  OR coalesce(r.applied_sequence, 0) < r.last_sequence), false), \
                    r.backlog, r.backlog_at, \
                    (SELECT count(*) FROM sync.quarantine q WHERE q.replica_id = s.id), \
                    (SELECT count(*) FROM sync.inbox i WHERE i.replica_id = s.id) \
               FROM unnest($1::uuid[]) AS s(id) \
               LEFT JOIN sync.replicas r ON r.replica_id = s.id \
               LEFT JOIN sync.epoch e ON true \
              ORDER BY s.id",
        )
        .bind(scoped.ids())
        .fetch_all(&mut *conn)
        .await?;
        let partitions: HashMap<Uuid, Option<Uuid>> = scoped.replicas.iter().copied().collect();
        Ok(rows
            .into_iter()
            .map(
                |(
                    replica,
                    failures,
                    last_error,
                    retry_at,
                    sent_at,
                    applied_at,
                    unconfirmed,
                    backlog,
                    backlog_at,
                    quarantined,
                    unfiled,
                )| {
                    let partition = partitions.get(&replica).copied().flatten();
                    let mine = notes
                        .iter()
                        .filter(|n| Scoped::concerns(n, replica, partition));
                    let count = |parked: bool, owed: bool| -> i64 {
                        mine.clone()
                            .filter(|n| n.parked == parked && (!owed || n.scope == Some(replica)))
                            .map(|n| n.count)
                            .sum()
                    };
                    let backlog = match (backlog, backlog_at) {
                        (Some(tables), Some(at)) => serde_json::from_value(tables)
                            .ok()
                            .map(|tables| Backlog { tables, at }),
                        _ => None,
                    };
                    let mut overview = ReplicaOverview {
                        replica,
                        state: ReplicaState::Synced,
                        pending: count(false, false),
                        owed: count(false, true),
                        parked: count(true, false),
                        unconfirmed,
                        quarantined,
                        unfiled,
                        failures: failures.unwrap_or(0),
                        last_error,
                        retry_at,
                        sent_at,
                        applied_at,
                        backlog,
                    };
                    overview.settle();
                    overview
                },
            )
            .collect())
    }

    async fn tables_of(
        &self,
        conn: &mut Conn,
        scoped: &Scoped,
        notes: &[NoteGroup],
        replicas: &[ReplicaOverview],
    ) -> Result<Vec<TableOverview>, Error> {
        let mut out: BTreeMap<String, TableOverview> = self
            .order
            .iter()
            .map(|&i| {
                let table = self.plans[i].table.to_owned();
                (
                    table.clone(),
                    TableOverview {
                        table,
                        ..TableOverview::default()
                    },
                )
            })
            .collect();
        for note in notes {
            let Some(table) = &note.table else { continue };
            let entry = out.entry(table.clone()).or_insert_with(|| TableOverview {
                table: table.clone(),
                ..TableOverview::default()
            });
            if note.parked {
                entry.parked += note.count;
            } else {
                entry.pending += note.count;
            }
        }
        let ids = scoped.ids();
        // Only the replicas with a message unconfirmed have a row in flight:
        // the others' held rows are not read.
        let unconfirmed: Vec<Uuid> = replicas
            .iter()
            .filter(|r| r.unconfirmed)
            .map(|r| r.replica)
            .collect();
        let confirmed =
            "coalesce(h.sequence <= r.applied_sequence AND r.applied_epoch = e.epoch, false)";
        let in_flight: Vec<Counted> = sqlx::query_as(&format!(
            "SELECT h.relation, h.replica_id, count(*) FROM sync.held h \
               LEFT JOIN sync.replicas r ON r.replica_id = h.replica_id \
               LEFT JOIN sync.epoch e ON true \
              WHERE h.replica_id = ANY($1) AND NOT {confirmed} GROUP BY 1, 2"
        ))
        .bind(&unconfirmed)
        .fetch_all(&mut *conn)
        .await?;
        let quarantined: Vec<Counted> = sqlx::query_as(
            "SELECT relation, replica_id, count(*) FROM sync.quarantine \
              WHERE replica_id = ANY($1) GROUP BY 1, 2",
        )
        .bind(&ids)
        .fetch_all(&mut *conn)
        .await?;
        let unfiled: Vec<Counted> = sqlx::query_as(
            "SELECT relation, replica_id, count(*) FROM sync.inbox \
              WHERE replica_id = ANY($1) GROUP BY 1, 2",
        )
        .bind(&ids)
        .fetch_all(&mut *conn)
        .await?;
        let mut behind: BTreeMap<String, BTreeSet<Uuid>> = BTreeMap::new();
        type Figure = fn(&mut TableOverview) -> &mut i64;
        let figures: [(&[Counted], Figure); 3] = [
            (&in_flight, |t| &mut t.in_flight),
            (&quarantined, |t| &mut t.quarantined),
            (&unfiled, |t| &mut t.unfiled),
        ];
        for (rows, figure) in figures {
            for (table, replica, count) in rows {
                let entry = out.entry(table.clone()).or_insert_with(|| TableOverview {
                    table: table.clone(),
                    ..TableOverview::default()
                });
                *figure(entry) += count;
                behind.entry(table.clone()).or_default().insert(*replica);
            }
        }
        for (table, replicas) in behind {
            if let Some(entry) = out.get_mut(&table) {
                entry.replicas = replicas.len() as i64;
            }
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
