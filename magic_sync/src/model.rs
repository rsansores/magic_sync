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
    /// Whether rows of `table` can be retired (4.3).
    pub(crate) retires: fn(&str) -> bool,
    /// The key a row of `table` holds, read off the row: `None` takes the
    /// key beside it on trust.
    pub(crate) key: fn(&str, &[u8]) -> Option<String>,
}

impl Rules {
    /// A replica that takes everything, of rows that point at nothing.
    pub(crate) const ANY: Rules = Rules {
        refuses: |_, _| false,
        parents: |_, _| Vec::new(),
        retires: |_| false,
        key: |_, _| None,
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

impl Received {
    /// What the corpora call it.
    pub(crate) fn outcome(&self) -> &'static str {
        match self {
            Received::Applied => "applied",
            Received::Ended => "ended",
            Received::Filed => "filed",
            Received::Already => "already",
            Received::Gap => "gap",
            Received::Malformed => "malformed",
        }
    }
}

/// One replica: what it holds, and what it has to tell the server.
#[derive(Clone, Debug, Default)]
pub(crate) struct Model {
    /// Every row it holds, as the bytes that carried it.
    pub(crate) rows: BTreeMap<Row, Vec<u8>>,
    /// The last message it applied, and from which epoch.
    pub(crate) applied: Option<(String, u64)>,
    /// That message's nonce and seal, given back with it.
    nonce: u64,
    mac: Vec<u8>,
    /// The last one the server heard of.
    acknowledged: Option<(String, u64)>,
    snapshot: Option<Snapshot>,
    /// The newest message it could not follow, while it waits for a
    /// snapshot to begin; 0 when it missed none.
    pub(crate) gap: u64,
    /// The gap the server was last told of.
    gap_told: u64,
    pub(crate) rejections: BTreeMap<Row, Rejection>,
    /// Rows a row of its own points at, in a table with nothing to retire
    /// them by — or that its database refuses to delete: it cannot let them
    /// go.
    pub(crate) pinned: BTreeSet<Row>,
    /// Rows a row of its own points at, in a table it retires by: let go
    /// of, they are retired — kept, and out of what it holds.
    pub(crate) retiring: BTreeSet<Row>,
    /// Rows it retired, as the bytes that carried them: sent again, they
    /// are live again; let go of again, they go, or are refused.
    pub(crate) retired: BTreeMap<Row, Vec<u8>>,
    /// What the last message it applied took, per table entry: the keys of
    /// the rows it holds from it.
    pub(crate) taken: Vec<(String, Vec<String>)>,
}

/// The largest number a server sends: 2^63 - 1.
const MOST: u64 = i64::MAX as u64;

/// Where a message stands in the server's chain.
struct Place {
    epoch: String,
    sequence: u64,
    nonce: u64,
    previous: u64,
    previous_nonce: u64,
    mac: Vec<u8>,
    snapshot_id: String,
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
        let (place, ends) = match &body {
            Body::Filed(_) => return Received::Filed,
            Body::Down(d) => {
                if d.tables.iter().any(|t| t.keys.len() != t.upserts.len()) {
                    return Received::Malformed;
                }
                let place = Place {
                    epoch: d.epoch.clone(),
                    sequence: d.sequence,
                    nonce: d.nonce,
                    previous: d.previous,
                    previous_nonce: d.previous_nonce,
                    mac: d.mac.clone(),
                    snapshot_id: d.snapshot_id.clone(),
                };
                (place, false)
            }
            Body::EndSnapshot(e) => {
                let place = Place {
                    epoch: e.epoch.clone(),
                    sequence: e.sequence,
                    nonce: e.nonce,
                    previous: e.previous,
                    previous_nonce: e.previous_nonce,
                    mac: e.mac.clone(),
                    snapshot_id: e.snapshot_id.clone(),
                };
                (place, true)
            }
        };
        // Numbers no server sends (4.1).
        let Ok(epoch) = uuid::Uuid::parse_str(&place.epoch).map(|e| e.to_string()) else {
            return Received::Malformed;
        };
        if !place.snapshot_id.is_empty() && uuid::Uuid::parse_str(&place.snapshot_id).is_err() {
            return Received::Malformed;
        }
        let sequence = place.sequence;
        if !(1..=MOST).contains(&sequence)
            || place.previous >= sequence
            || !(1..=MOST).contains(&place.nonce)
            || place.previous_nonce > MOST
            || (place.previous == 0) != (place.previous_nonce == 0)
            || (place.previous == 0 && place.snapshot_id.is_empty())
            || place.mac.len() != 16
        {
            return Received::Malformed;
        }
        if let Some((e, s)) = &self.applied {
            if *e == epoch && sequence <= *s {
                // The server may have lost what it was told: say it again.
                self.acknowledged = None;
                return Received::Already;
            }
        }
        if place.previous == 0 {
            self.snapshot = Some(Snapshot {
                id: place.snapshot_id.clone(),
                began: sequence,
                named: BTreeSet::new(),
            });
            (self.gap, self.gap_told) = (0, 0);
        } else {
            let follows = self.gap == 0
                && self.applied == Some((epoch.clone(), place.previous))
                && self.nonce == place.previous_nonce;
            if !follows {
                self.gap = self.gap.max(sequence);
                return Received::Gap;
            }
            let named = (!place.snapshot_id.is_empty() || ends).then_some(&place.snapshot_id);
            if named != self.snapshot.as_ref().map(|s| &s.id) {
                return Received::Malformed;
            }
        }
        self.applied = Some((epoch, sequence));
        (self.nonce, self.mac) = (place.nonce, place.mac);
        match body {
            Body::Filed(_) => Received::Filed,
            Body::Down(down) => {
                let naming = !down.snapshot_id.is_empty();
                self.taken.clear();
                for table in down.tables {
                    let gone = table
                        .deletes
                        .iter()
                        .map(|key| (table.table.clone(), key.parts.join("|")))
                        .collect();
                    self.let_go(gone, sequence, rules);
                    let mut taken = Vec::new();
                    for (bytes, key) in table.upserts.iter().zip(&table.keys) {
                        let row = (table.table.clone(), key.parts.join("|"));
                        if naming {
                            if let Some(snapshot) = &mut self.snapshot {
                                snapshot.named.insert(row.clone());
                            }
                        }
                        let beside =
                            (rules.key)(&table.table, bytes).is_some_and(|own| own != row.1);
                        if beside || (rules.refuses)(&table.table, bytes) {
                            self.reject(row, "refused", sequence);
                            continue;
                        }
                        self.rejections.remove(&row);
                        taken.push(row.1.clone());
                        self.retired.remove(&row);
                        self.rows.insert(row, bytes.clone());
                    }
                    self.taken.push((table.table, taken));
                }
                Received::Applied
            }
            Body::EndSnapshot(end) => {
                let snapshot = self.snapshot.take().expect("in progress, or malformed");
                for table in end.tables.iter().rev() {
                    let epoch = self.applied.as_ref().map(|(e, _)| e.clone());
                    self.rejections.retain(|(t, _), r| {
                        t != table
                            || (Some(&r.epoch) == epoch.as_ref() && r.sequence >= snapshot.began)
                    });
                    let stale: Vec<Row> = self
                        .rows
                        .keys()
                        .chain(self.retired.keys())
                        .filter(|row| row.0 == *table && !snapshot.named.contains(*row))
                        .cloned()
                        .collect();
                    self.let_go(stale, sequence, rules);
                }
                Received::Ended
            }
        }
    }

    /// Lets go of `rows`, together (4.1): one that a row staying for the
    /// replica points at — a row of its own, or one it retired — is retired
    /// where its table can be, and otherwise kept, rejected; and so in turn
    /// is what a row retired so points at among them. The rest go, but one
    /// the database refuses — kept by a trigger, or pointed at by a row of
    /// the server's that stays — which is kept, rejected.
    fn let_go(&mut self, rows: Vec<Row>, sequence: u64, rules: &Rules) {
        let asked: BTreeSet<Row> = rows.into_iter().collect();
        let held: BTreeSet<Row> = asked
            .iter()
            .filter(|row| self.rows.contains_key(*row) || self.retired.contains_key(*row))
            .cloned()
            .collect();
        for row in asked.difference(&held) {
            self.rejections.remove(row);
        }
        // Rows kept for the replica: retired, or — where their table cannot
        // be — kept as they are.
        let mut retiring: BTreeSet<Row> = held
            .iter()
            .filter(|row| self.retiring.contains(*row))
            .cloned()
            .collect();
        let mut kept = BTreeSet::new();
        loop {
            let more: BTreeSet<Row> = self
                .retired
                .iter()
                .filter(|(row, _)| !asked.contains(*row) || retiring.contains(*row))
                .chain(self.rows.iter().filter(|(row, _)| retiring.contains(*row)))
                .flat_map(|(row, bytes)| (rules.parents)(&row.0, bytes))
                .filter(|parent| {
                    held.contains(parent) && !retiring.contains(parent) && !kept.contains(parent)
                })
                .collect();
            if more.is_empty() {
                break;
            }
            let (retire, keep): (Vec<Row>, Vec<Row>) =
                more.into_iter().partition(|row| (rules.retires)(&row.0));
            retiring.extend(retire);
            kept.extend(keep);
        }
        let mut going: BTreeSet<Row> = held
            .iter()
            .filter(|row| {
                !retiring.contains(*row) && !kept.contains(*row) && !self.pinned.contains(*row)
            })
            .cloned()
            .collect();
        loop {
            let refused: BTreeSet<Row> = self
                .rows
                .iter()
                .filter(|(row, _)| !going.contains(*row) && !retiring.contains(*row))
                .flat_map(|(row, bytes)| (rules.parents)(&row.0, bytes))
                .filter(|parent| going.contains(parent))
                .collect();
            if refused.is_empty() {
                break;
            }
            going.retain(|row| !refused.contains(row));
        }
        for row in held {
            if retiring.contains(&row) {
                if let Some(bytes) = self.rows.remove(&row) {
                    self.retired.insert(row.clone(), bytes);
                }
                self.rejections.remove(&row);
            } else if going.contains(&row) {
                self.rows.remove(&row);
                self.retired.remove(&row);
                self.rejections.remove(&row);
            } else {
                self.reject(row, "pointed at", sequence);
            }
        }
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
        if self.applied != self.acknowledged || self.gap > 0 {
            self.confirm(&mut up);
        }
        let news = self.applied != self.acknowledged || self.gap > self.gap_told;
        (!up.rejected.is_empty() || news).then_some(up)
    }

    /// The last message it applied, as it gives it back to the server.
    #[cfg(feature = "postgres")]
    fn confirm(&self, up: &mut Up) {
        if let Some((epoch, sequence)) = &self.applied {
            (up.epoch, up.sequence) = (epoch.clone(), *sequence);
            (up.nonce, up.mac) = (self.nonce, self.mac.clone());
        }
    }

    /// What it holds of each of `tables`, with the last message it applied:
    /// the digests a replica reports now and then (PROTOCOL.md section 8),
    /// leaving out the rows it rejected.
    #[cfg(feature = "postgres")]
    pub(crate) fn digests(&self, tables: &[&str]) -> Up {
        let mut up = Up {
            protocol: crate::envelope::PROTOCOL,
            ..Up::default()
        };
        self.confirm(&mut up);
        for table in tables {
            let keys: Vec<String> = self
                .rows
                .keys()
                .filter(|row| row.0 == *table && !self.rejections.contains_key(*row))
                .map(|(_, key)| key.clone())
                .collect();
            up.digests.push(crate::envelope::Digest {
                table: (*table).to_owned(),
                count: keys.len() as u64,
                fingerprint: crate::envelope::fingerprint(keys),
            });
        }
        up
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
        self.gap_told = self.gap_told.max(up.gap);
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

/// The model is written from PROTOCOL.md, and the corpora say what the
/// protocol makes of each message: the two agree, line by line.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::{golden, nonce_of, EPOCH, GOLDEN, MALFORMED};
    use crate::envelope::EndSnapshot;

    #[test]
    fn the_model_makes_of_every_corpus_line_what_it_says() {
        let provisioned = Body::EndSnapshot(EndSnapshot {
            snapshot_id: crate::conformance::EPOCH.to_owned(),
            sequence: 100,
            epoch: EPOCH.to_owned(),
            nonce: nonce_of(100),
            mac: crate::conformance::SEAL.to_vec(),
            ..EndSnapshot::default()
        })
        .encoded();
        let mut model = Model::default();
        assert_eq!(model.receive(&provisioned, &Rules::ANY), Received::Ended);
        let corpus = std::fs::read_to_string(MALFORMED).expect("the corpus");
        for (case, outcome, bytes) in golden(&corpus) {
            let got = model.receive(&bytes, &Rules::ANY);
            assert_eq!(got.outcome(), outcome, "{case}");
        }
        let mut model = Model::default();
        let sent = std::fs::read_to_string(GOLDEN).expect("the golden messages");
        for (step, outcome, bytes) in golden(&sent) {
            if outcome != "lost" {
                let got = model.receive(&bytes, &Rules::ANY);
                assert_eq!(got.outcome(), outcome, "{step}");
            }
        }
    }
}
