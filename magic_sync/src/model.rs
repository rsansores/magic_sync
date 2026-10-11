//! A replica as PROTOCOL.md says one behaves, and a transport that keeps
//! each replica's messages in a queue of its own: the other half of the
//! system the relay's tests drive. Written from the protocol, not from the
//! engine's replica (the other build): where the two disagree, one of them
//! breaks the protocol, and the conformance suite says which.

use crate::envelope::{Body, ToReplica};
use prost::Message as _;
use std::collections::{BTreeMap, BTreeSet};
#[cfg(feature = "postgres")]
use {
    crate::envelope::{Rejected, Up},
    crate::relay::Transport,
    crate::testkit::Walk,
    crate::Error,
    std::collections::VecDeque,
    std::future::Future,
    std::sync::Mutex,
    uuid::Uuid,
};

/// A row, by its table and its key's text.
pub(crate) type Row = (String, String);

/// What a replica's data is: which rows it refuses to take, and which rows
/// each points at.
pub(crate) struct Rules {
    /// Whether the replica's database refuses this row of `table`.
    pub(crate) refuses: fn(&str, &[u8]) -> bool,
    /// The rows a row of `table` points at.
    pub(crate) parents: fn(&str, &[u8]) -> Vec<Row>,
}

impl Rules {
    /// A replica that takes everything, of rows that point at nothing.
    #[cfg(feature = "postgres")]
    pub(crate) const ANY: Rules = Rules {
        refuses: |_, _| false,
        parents: |_, _| Vec::new(),
    };
}

/// A row the replica could not take or let go of, until it does.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Rejection {
    pub(crate) reason: String,
    pub(crate) epoch: String,
    pub(crate) sequence: u64,
    pub(crate) reported: bool,
}

/// The snapshot in progress: the message that began it, and what it named.
#[derive(Clone, Debug, Default)]
struct Snapshot {
    id: String,
    began: u64,
    named: BTreeSet<Row>,
}

/// What a message did.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Received {
    Applied,
    Ended,
    Filed,
    Already,
    Gap,
    Malformed,
}

/// One replica: what it holds, and what it has to tell the server.
#[derive(Clone, Debug, Default)]
pub(crate) struct Model {
    /// Every row it holds, as the bytes that carried it.
    pub(crate) rows: BTreeMap<Row, Vec<u8>>,
    /// The last message it applied, and from which epoch.
    pub(crate) applied: Option<(String, u64)>,
    /// The last one the server heard of.
    acknowledged: Option<(String, u64)>,
    snapshot: Option<Snapshot>,
    /// It missed a message, and waits for a snapshot to begin.
    pub(crate) gap: bool,
    pub(crate) rejections: BTreeMap<Row, Rejection>,
    /// Rows a row of its own points at, in a table with nothing to retire
    /// them by: it cannot let them go.
    pub(crate) pinned: BTreeSet<Row>,
}

impl Model {
    /// `bytes` from the server, applied as section 4 says, in one step: the
    /// transport is acknowledged once this returns.
    pub(crate) fn receive(&mut self, bytes: &[u8], rules: &Rules) -> Received {
        let Ok(message) = ToReplica::decode(bytes) else {
            return Received::Malformed;
        };
        if message.protocol == 0 {
            return Received::Malformed;
        }
        let Some(body) = message.body else {
            return Received::Malformed;
        };
        let (epoch, sequence, previous, snapshot_id) = match &body {
            Body::Filed(_) => return Received::Filed,
            Body::Down(d) => (
                d.epoch.clone(),
                d.sequence,
                d.previous,
                d.snapshot_id.clone(),
            ),
            Body::EndSnapshot(e) => (
                e.epoch.clone(),
                e.sequence,
                e.previous,
                e.snapshot_id.clone(),
            ),
        };
        if sequence == 0 || previous >= sequence || (previous == 0 && snapshot_id.is_empty()) {
            return Received::Malformed;
        }
        if let Some((e, s)) = &self.applied {
            if *e == epoch && sequence <= *s {
                // The server may have lost what it was told: say it again.
                self.acknowledged = None;
                return Received::Already;
            }
        }
        if previous == 0 {
            self.snapshot = Some(Snapshot {
                id: snapshot_id.clone(),
                began: sequence,
                named: BTreeSet::new(),
            });
            self.gap = false;
        } else if self.applied != Some((epoch.clone(), previous)) {
            self.gap = true;
            return Received::Gap;
        } else if !snapshot_id.is_empty()
            && self.snapshot.as_ref().is_none_or(|s| s.id != snapshot_id)
        {
            return Received::Malformed;
        }
        self.applied = Some((epoch, sequence));
        match body {
            Body::Filed(_) => Received::Filed,
            Body::Down(down) => {
                let naming = !down.snapshot_id.is_empty();
                for table in down.tables {
                    for (bytes, key) in table.upserts.iter().zip(&table.keys) {
                        let row = (table.table.clone(), key.parts.join("|"));
                        if naming {
                            if let Some(snapshot) = &mut self.snapshot {
                                snapshot.named.insert(row.clone());
                            }
                        }
                        if (rules.refuses)(&table.table, bytes) {
                            self.reject(row, "refused", sequence);
                            continue;
                        }
                        self.rejections.remove(&row);
                        self.rows.insert(row, bytes.clone());
                    }
                    for key in &table.deletes {
                        let row = (table.table.clone(), key.parts.join("|"));
                        self.let_go(row, sequence, rules);
                    }
                }
                Received::Applied
            }
            Body::EndSnapshot(end) => {
                let snapshot = self.snapshot.take().expect("begun, or malformed");
                for table in end.tables.iter().rev() {
                    let epoch = self.applied.as_ref().map(|(e, _)| e.clone());
                    self.rejections.retain(|(t, _), r| {
                        t != table
                            || (Some(&r.epoch) == epoch.as_ref() && r.sequence >= snapshot.began)
                    });
                    let stale: Vec<Row> = self
                        .rows
                        .keys()
                        .filter(|row| row.0 == *table && !snapshot.named.contains(*row))
                        .cloned()
                        .collect();
                    for row in stale {
                        self.let_go(row, sequence, rules);
                    }
                }
                Received::Ended
            }
        }
    }

    /// Lets go of `row`, unless a row it holds points at it: then it is
    /// rejected, as a database's foreign key would refuse the delete.
    fn let_go(&mut self, row: Row, sequence: u64, rules: &Rules) {
        let pointed = self.pinned.contains(&row)
            || self
                .rows
                .iter()
                .any(|((table, _), bytes)| (rules.parents)(table, bytes).contains(&row));
        if pointed {
            self.reject(row, "pointed at", sequence);
            return;
        }
        self.rows.remove(&row);
        self.rejections.remove(&row);
    }

    fn reject(&mut self, row: Row, reason: &str, sequence: u64) {
        let epoch = self
            .applied
            .as_ref()
            .map(|(e, _)| e.clone())
            .unwrap_or_default();
        self.rejections.insert(
            row,
            Rejection {
                reason: reason.to_owned(),
                epoch,
                sequence,
                reported: false,
            },
        );
    }

    /// What it has to tell the server on its first lane: its rejections not
    /// reported yet, and the last message it applied when the server has not
    /// heard of it — never before every rejection of it is reported, which
    /// one `Up` here always does — or always, while it has a gap to say.
    /// `None` when it has nothing to say.
    #[cfg(feature = "postgres")]
    pub(crate) fn up(&self) -> Option<Up> {
        let rejected: Vec<Rejected> = self
            .rejections
            .iter()
            .filter(|(_, r)| !r.reported)
            .filter(|(_, r)| self.applied.as_ref().is_some_and(|(e, _)| *e == r.epoch))
            .map(|((table, key), r)| Rejected {
                table: table.clone(),
                key: Some(crate::envelope::Key {
                    parts: key.split('|').map(str::to_owned).collect(),
                }),
                reason: r.reason.clone(),
                epoch: r.epoch.clone(),
                sequence: r.sequence,
            })
            .collect();
        let mut up = Up {
            protocol: crate::envelope::PROTOCOL,
            rejected,
            gap: self.gap,
            ..Up::default()
        };
        if self.applied != self.acknowledged || self.gap {
            if let Some((epoch, sequence)) = &self.applied {
                (up.epoch, up.sequence) = (epoch.clone(), *sequence);
            }
        }
        (!up.rejected.is_empty() || up.sequence > 0 || up.gap).then_some(up)
    }

    /// The uplink took `up`.
    #[cfg(feature = "postgres")]
    pub(crate) fn delivered(&mut self, up: &Up) {
        for rejected in &up.rejected {
            let key = rejected
                .key
                .as_ref()
                .map(|k| k.parts.join("|"))
                .unwrap_or_default();
            if let Some(r) = self.rejections.get_mut(&(rejected.table.clone(), key)) {
                // Unless refused again since.
                r.reported |= r.sequence == rejected.sequence;
            }
        }
        if up.sequence > 0 {
            self.acknowledged = Some((up.epoch.clone(), up.sequence));
        }
    }

    /// Rows it holds that point at a row it neither holds nor refused.
    #[cfg(feature = "postgres")]
    pub(crate) fn orphans(&self, rules: &Rules) -> Vec<(Row, Row)> {
        let mut out = Vec::new();
        for ((table, key), bytes) in &self.rows {
            for parent in (rules.parents)(table, bytes) {
                if !self.rows.contains_key(&parent) && !self.rejections.contains_key(&parent) {
                    out.push(((table.clone(), key.clone()), parent));
                }
            }
        }
        out
    }
}

/// How the transport misbehaves, out of 100 sends.
#[cfg(feature = "postgres")]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Faults {
    /// Sends it does not take.
    pub(crate) fail: usize,
    /// Of those, one in so many it refuses for good.
    pub(crate) refuse_one_in: usize,
}

/// The messages waiting for one replica, in order: each one's id and bytes.
#[cfg(feature = "postgres")]
type Queue = VecDeque<(String, Vec<u8>)>;

/// A transport that keeps each replica's messages in a queue of its own, in
/// the order it took them, and fails sends as `faults` says.
#[cfg(feature = "postgres")]
pub(crate) struct Wire {
    pub(crate) queues: Mutex<BTreeMap<Uuid, Queue>>,
    pub(crate) faults: Mutex<Faults>,
    walk: Mutex<Walk>,
    /// Sends it did not take: transient, refused for good.
    pub(crate) failed: Mutex<(usize, usize)>,
}

#[cfg(feature = "postgres")]
impl Wire {
    pub(crate) fn new(seed: u64) -> Self {
        Wire {
            queues: Mutex::new(BTreeMap::new()),
            faults: Mutex::new(Faults::default()),
            walk: Mutex::new(Walk(seed)),
            failed: Mutex::new((0, 0)),
        }
    }

    /// Whether nothing waits to be delivered.
    pub(crate) fn empty(&self) -> bool {
        self.queues.lock().unwrap().values().all(VecDeque::is_empty)
    }

    /// The next message for `replica`, left first in its queue until
    /// [`Wire::acknowledge`].
    pub(crate) fn next(&self, replica: Uuid) -> Option<Vec<u8>> {
        self.queues
            .lock()
            .unwrap()
            .get(&replica)
            .and_then(|q| q.front())
            .map(|(_, bytes)| bytes.clone())
    }

    /// `replica` acknowledged its next message: the transport lets it go.
    pub(crate) fn acknowledge(&self, replica: Uuid) {
        if let Some(q) = self.queues.lock().unwrap().get_mut(&replica) {
            q.pop_front();
        }
    }

    /// The ids of what waits for `replica`.
    pub(crate) fn ids(&self, replica: Uuid) -> Vec<String> {
        self.queues
            .lock()
            .unwrap()
            .get(&replica)
            .map(|q| q.iter().map(|(id, _)| id.clone()).collect())
            .unwrap_or_default()
    }
}

#[cfg(feature = "postgres")]
impl Transport for Wire {
    fn send(
        &self,
        replica: Uuid,
        message: Vec<u8>,
        id: &str,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        let faults = *self.faults.lock().unwrap();
        let mut walk = self.walk.lock().unwrap();
        let fails = faults.fail > 0 && walk.below(100) < faults.fail;
        let refused = fails && faults.refuse_one_in > 0 && walk.below(faults.refuse_one_in) == 0;
        drop(walk);
        let result = if fails {
            let mut failed = self.failed.lock().unwrap();
            if refused {
                failed.1 += 1;
            } else {
                failed.0 += 1;
            }
            Err(Error::Transport {
                reason: if refused {
                    "refused\0 for good"
                } else {
                    "the network"
                }
                .into(),
                transient: !refused,
            })
        } else {
            self.queues
                .lock()
                .unwrap()
                .entry(replica)
                .or_default()
                .push_back((id.to_owned(), message));
            Ok(())
        };
        async move { result }
    }
}
