//! What the engine sends a replica and a replica sends back, as protobuf
//! messages. A host carries them as bytes on whatever transport it has: one
//! [`ToReplica`] down, one [`Up`] up. A row inside one is its table's own
//! message, encoded by the table's codec.

/// The version of the protocol these messages speak. A side that receives a
/// message of a later version than its own does not act on it: it waits to be
/// updated, and the message is delivered again.
pub const PROTOCOL: u32 = 1;

/// Room a message keeps for everything but its rows: a row larger than the
/// byte limit less this can never travel, and is set aside instead.
pub const ENVELOPE_ROOM: usize = 4096;

/// Everything the server sends a replica, as one message.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ToReplica {
    /// [`PROTOCOL`], as the server speaks it.
    #[prost(uint32, tag = "1")]
    pub protocol: u32,
    #[prost(oneof = "Body", tags = "2, 3, 4")]
    pub body: Option<Body>,
}

/// What a [`ToReplica`] carries.
#[derive(Clone, PartialEq, prost::Oneof)]
pub enum Body {
    #[prost(message, tag = "2")]
    Down(Down),
    #[prost(message, tag = "3")]
    EndSnapshot(EndSnapshot),
    #[prost(message, tag = "4")]
    Filed(Filed),
}

impl Body {
    /// As the bytes a transport carries.
    pub fn encoded(self) -> Vec<u8> {
        use prost::Message as _;
        ToReplica {
            protocol: PROTOCOL,
            body: Some(self),
        }
        .encode_to_vec()
    }
}

/// A row's key, its parts as text in key order.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, prost::Message)]
pub struct Key {
    #[prost(string, repeated, tag = "1")]
    pub parts: Vec<String>,
}

/// One table's part of a message: rows to hold, then rows to let go.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Table {
    #[prost(string, tag = "1")]
    pub table: String,
    #[prost(bytes = "vec", repeated, tag = "2")]
    pub upserts: Vec<Vec<u8>>,
    #[prost(message, repeated, tag = "3")]
    pub deletes: Vec<Key>,
    /// The key of each upsert, in the same order: a replica that cannot read
    /// a row can still say which one it was.
    #[prost(message, repeated, tag = "4")]
    pub keys: Vec<Key>,
}

/// Server to replica: rows to hold and rows to let go, in the order given —
/// parents' upserts first, children's deletes first — applied in one
/// transaction. `snapshot_id` is set while the replica is provisioned from
/// scratch: it remembers every row the snapshot names.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Down {
    #[prost(string, tag = "1")]
    pub snapshot_id: String,
    #[prost(message, repeated, tag = "2")]
    pub tables: Vec<Table>,
    /// Rises with every message the engine sends, whichever relay sends it:
    /// a replica applies nothing at or below the last it applied, so neither
    /// a redelivery nor a relay that lost its lock can set it back.
    #[prost(uint64, tag = "3")]
    pub sequence: u64,
    /// The server installation the sequence is from.
    #[prost(string, tag = "4")]
    pub epoch: String,
}

/// Server to replica: a snapshot is complete. Every row of `tables` the
/// replica holds and the snapshot did not name goes.
#[derive(Clone, PartialEq, prost::Message)]
pub struct EndSnapshot {
    #[prost(string, tag = "1")]
    pub snapshot_id: String,
    #[prost(string, repeated, tag = "2")]
    pub tables: Vec<String>,
    /// As [`Down::sequence`].
    #[prost(uint64, tag = "3")]
    pub sequence: u64,
    /// As [`Down::epoch`].
    #[prost(string, tag = "4")]
    pub epoch: String,
}

/// Replica to server: rows the replica wrote, the keys of its own rows it let
/// go of, and the rows the server sent that it could not take. The server
/// files them under the replica the message came from, whatever they say.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Up {
    #[prost(string, tag = "1")]
    pub replica_id: String,
    #[prost(message, repeated, tag = "2")]
    pub tables: Vec<Table>,
    #[prost(message, repeated, tag = "3")]
    pub rejected: Vec<Rejected>,
    /// What the replica holds of each table sent down, to compare with what
    /// the server records it holds.
    #[prost(message, repeated, tag = "4")]
    pub digests: Vec<Digest>,
    /// The last message the replica applied when it took the digests: they
    /// are only comparable once it has applied every message recorded for it.
    #[prost(string, tag = "5")]
    pub epoch: String,
    #[prost(uint64, tag = "6")]
    pub sequence: u64,
    /// The uploads this message carries rows of, each numbered by the
    /// highest note it carries: the server answers with [`Filed`] once it has
    /// filed them.
    #[prost(uint64, repeated, tag = "7")]
    pub uploads: Vec<u64>,
    /// [`PROTOCOL`], as the replica speaks it.
    #[prost(uint32, tag = "8")]
    pub protocol: u32,
    /// What the replica wrote and the server has not filed, when it says:
    /// on the first lane, when it changed. `None`: it did not say this time.
    #[prost(message, optional, tag = "9")]
    pub backlog: Option<Backlog>,
}

/// What a replica wrote that the server has not filed yet, per table.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Backlog {
    #[prost(message, repeated, tag = "1")]
    pub tables: Vec<TableBacklog>,
}

/// One table's part of a [`Backlog`].
#[derive(Clone, PartialEq, prost::Message)]
pub struct TableBacklog {
    #[prost(string, tag = "1")]
    pub table: String,
    /// Rows not sent yet.
    #[prost(uint64, tag = "2")]
    pub unsent: u64,
    /// Rows sent the server has not said it filed.
    #[prost(uint64, tag = "3")]
    pub unfiled: u64,
}

/// Server to replica: these uploads are filed (or kept, if a row could not
/// be): the notes they carried are done.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Filed {
    #[prost(uint64, repeated, tag = "1")]
    pub uploads: Vec<u64>,
}

/// What a replica holds of one table: how many rows, and a fingerprint of
/// their keys ([`fingerprint`]). Rows it retired, or could not take, are left
/// out — the server leaves out the same.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Digest {
    #[prost(string, tag = "1")]
    pub table: String,
    #[prost(uint64, tag = "2")]
    pub count: u64,
    #[prost(uint64, tag = "3")]
    pub fingerprint: u64,
}

/// A fingerprint of a set of keys, as text: the same on every side and every
/// build (FNV-1a over the keys sorted, each ended by a newline).
pub fn fingerprint(keys: &mut [String]) -> u64 {
    keys.sort_unstable();
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for key in keys.iter() {
        for byte in key.bytes().chain(std::iter::once(b'\n')) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    hash
}

/// A row a replica could not take, and why. The server stops counting it as
/// held and quarantines it, so a retry after the cause is fixed sends it.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Rejected {
    #[prost(string, tag = "1")]
    pub table: String,
    #[prost(message, optional, tag = "2")]
    pub key: Option<Key>,
    #[prost(string, tag = "3")]
    pub reason: String,
}

impl Up {
    /// Several uploads as one per replica, each table's rows together.
    pub fn by_replica(uploads: Vec<Up>) -> Vec<Up> {
        let mut merged: Vec<Up> = Vec::new();
        for upload in uploads {
            let Some(into) = merged
                .iter_mut()
                .find(|m| m.replica_id == upload.replica_id)
            else {
                merged.push(upload);
                continue;
            };
            into.rejected.extend(upload.rejected);
            into.uploads.extend(upload.uploads);
            for table in upload.tables {
                match into.tables.iter_mut().find(|t| t.table == table.table) {
                    Some(same) => {
                        same.upserts.extend(table.upserts);
                        same.deletes.extend(table.deletes);
                        same.keys.extend(table.keys);
                    }
                    None => into.tables.push(table),
                }
            }
        }
        merged
    }
}

/// How large a message the transport takes.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub rows: usize,
    pub bytes: usize,
}

impl Limits {
    /// The largest row a message can carry.
    pub fn largest_row(&self) -> usize {
        self.bytes.saturating_sub(ENVELOPE_ROOM)
    }
}

/// One replica's rows as messages within `limits`, in their order: a new
/// message begins when the current one is full. A row larger than the byte
/// limit goes alone. Each row keeps its key beside it, and both count.
pub fn pack(tables: Vec<Table>, snapshot: &str, limits: Limits) -> Vec<Down> {
    let mut messages: Vec<Vec<Table>> = vec![Vec::new()];
    let (mut count, mut bytes) = (0usize, 0usize);
    let mut slot = |messages: &mut Vec<Vec<Table>>, weight: usize, table: &str| -> usize {
        if count > 0 && (count >= limits.rows || bytes + weight > limits.bytes) {
            messages.push(Vec::new());
            count = 0;
            bytes = 0;
        }
        count += 1;
        bytes += weight;
        let current = messages.last_mut().expect("never empty");
        if current.last().is_none_or(|t| t.table != table) {
            current.push(Table {
                table: table.to_owned(),
                ..Table::default()
            });
        }
        messages.len() - 1
    };
    for table in tables {
        let mut keys = table.keys.into_iter();
        for row in table.upserts {
            let key = keys.next();
            let weight = row.len() + key.as_ref().map_or(0, key_weight);
            let at = slot(&mut messages, weight, &table.table);
            let current = messages[at].last_mut().expect("a slot");
            current.upserts.push(row);
            current.keys.extend(key);
        }
        for key in table.deletes {
            let at = slot(&mut messages, key_weight(&key), &table.table);
            messages[at].last_mut().expect("a slot").deletes.push(key);
        }
    }
    messages
        .into_iter()
        .filter(|tables| !tables.is_empty())
        .map(|tables| Down {
            snapshot_id: snapshot.to_owned(),
            tables,
            sequence: 0,
            epoch: String::new(),
        })
        .collect()
}

/// What a key adds to a message, about.
fn key_weight(key: &Key) -> usize {
    key.parts.iter().map(|p| p.len() + 2).sum::<usize>() + 2
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(table: &str, sizes: &[usize]) -> Table {
        Table {
            table: table.to_owned(),
            upserts: sizes.iter().map(|n| vec![0; *n]).collect(),
            deletes: Vec::new(),
            keys: (0..sizes.len())
                .map(|n| Key {
                    parts: vec![n.to_string()],
                })
                .collect(),
        }
    }

    #[test]
    fn a_message_ends_at_either_limit_and_keeps_the_order() {
        // Each row weighs its bytes and five for its key.
        let limits = Limits { rows: 3, bytes: 21 };
        let packed = pack(
            vec![rows("parents", &[4, 4]), rows("children", &[4, 1, 1, 1])],
            "",
            limits,
        );
        let shape: Vec<Vec<(String, usize)>> = packed
            .iter()
            .map(|m| {
                m.tables
                    .iter()
                    .map(|t| (t.table.clone(), t.upserts.len()))
                    .collect()
            })
            .collect();
        assert_eq!(
            shape,
            vec![
                vec![("parents".to_owned(), 2)],
                vec![("children".to_owned(), 3)],
                vec![("children".to_owned(), 1)],
            ]
        );
    }

    #[test]
    fn each_row_keeps_its_key_across_messages() {
        let packed = pack(
            vec![rows("t", &[1, 2, 3])],
            "",
            Limits {
                rows: 2,
                bytes: 100,
            },
        );
        let keys: Vec<Vec<String>> = packed
            .iter()
            .map(|m| {
                m.tables[0]
                    .keys
                    .iter()
                    .map(|k| k.parts[0].clone())
                    .collect()
            })
            .collect();
        assert_eq!(keys, vec![vec!["0", "1"], vec!["2"]]);
    }

    #[test]
    fn a_row_past_the_byte_limit_goes_alone() {
        let packed = pack(
            vec![rows("big", &[1, 50, 1])],
            "",
            Limits {
                rows: 10,
                bytes: 10,
            },
        );
        let sizes: Vec<usize> = packed.iter().map(|m| m.tables[0].upserts.len()).collect();
        assert_eq!(sizes, vec![1, 1, 1]);
    }

    /// The field numbers a message's bytes carry.
    fn on_the_wire(mut bytes: &[u8]) -> std::collections::BTreeSet<u64> {
        fn varint(bytes: &mut &[u8]) -> u64 {
            let (mut value, mut shift) = (0u64, 0);
            loop {
                let byte = bytes[0];
                *bytes = &bytes[1..];
                value |= u64::from(byte & 0x7f) << shift;
                if byte & 0x80 == 0 {
                    return value;
                }
                shift += 7;
            }
        }
        let mut tags = std::collections::BTreeSet::new();
        while !bytes.is_empty() {
            let key = varint(&mut bytes);
            tags.insert(key >> 3);
            match key & 7 {
                0 => {
                    varint(&mut bytes);
                }
                2 => {
                    let len = varint(&mut bytes) as usize;
                    bytes = &bytes[len..];
                }
                other => panic!("wire type {other}"),
            }
        }
        tags
    }

    /// The field numbers `sync.proto` declares for `message`.
    fn declared(message: &str) -> std::collections::BTreeSet<u64> {
        let proto = include_str!("../proto/sync.proto");
        let start = proto
            .find(&format!("message {message} {{"))
            .unwrap_or_else(|| panic!("{message} is not in sync.proto"));
        let body = &proto[start..start + proto[start..].find('}').expect("a closing brace")];
        body.lines()
            .filter_map(|line| line.split('=').nth(1))
            .filter_map(|tag| tag.trim().trim_end_matches(';').parse().ok())
            .collect()
    }

    /// A replica in another language is generated from `proto/sync.proto`:
    /// every field of every message here is there, under the same number.
    #[test]
    fn the_messages_are_the_ones_sync_proto_declares() {
        use prost::Message as _;
        let key = Key {
            parts: vec!["k".into()],
        };
        let table = Table {
            table: "t".into(),
            upserts: vec![vec![1]],
            deletes: vec![key.clone()],
            keys: vec![key.clone()],
        };
        let cases: Vec<(&str, Vec<u8>)> = vec![
            ("Key", key.encode_to_vec()),
            ("Table", table.encode_to_vec()),
            (
                "Down",
                Down {
                    snapshot_id: "s".into(),
                    tables: vec![table.clone()],
                    sequence: 1,
                    epoch: "e".into(),
                }
                .encode_to_vec(),
            ),
            (
                "EndSnapshot",
                EndSnapshot {
                    snapshot_id: "s".into(),
                    tables: vec!["t".into()],
                    sequence: 1,
                    epoch: "e".into(),
                }
                .encode_to_vec(),
            ),
            ("Filed", Filed { uploads: vec![1] }.encode_to_vec()),
            (
                "Up",
                Up {
                    replica_id: "r".into(),
                    tables: vec![table.clone()],
                    rejected: vec![Rejected::default()],
                    digests: vec![Digest::default()],
                    epoch: "e".into(),
                    sequence: 1,
                    uploads: vec![1],
                    protocol: PROTOCOL,
                    backlog: Some(Backlog::default()),
                }
                .encode_to_vec(),
            ),
            (
                "Rejected",
                Rejected {
                    table: "t".into(),
                    key: Some(key.clone()),
                    reason: "r".into(),
                }
                .encode_to_vec(),
            ),
            (
                "Backlog",
                Backlog {
                    tables: vec![TableBacklog::default()],
                }
                .encode_to_vec(),
            ),
            (
                "TableBacklog",
                TableBacklog {
                    table: "t".into(),
                    unsent: 1,
                    unfiled: 1,
                }
                .encode_to_vec(),
            ),
            (
                "Digest",
                Digest {
                    table: "t".into(),
                    count: 1,
                    fingerprint: 1,
                }
                .encode_to_vec(),
            ),
        ];
        for (message, bytes) in cases {
            assert_eq!(on_the_wire(&bytes), declared(message), "{message}");
        }
        // One body at a time: the oneof's fields over all three.
        let bodies = [
            Body::Down(Down::default()),
            Body::EndSnapshot(EndSnapshot::default()),
            Body::Filed(Filed::default()),
        ];
        let tags: std::collections::BTreeSet<u64> = bodies
            .into_iter()
            .flat_map(|body| on_the_wire(&body.encoded()))
            .collect();
        assert_eq!(tags, declared("ToReplica"), "ToReplica");
    }

    #[test]
    fn a_fingerprint_is_of_the_set_not_its_order() {
        let mut one = vec!["b".to_owned(), "a".to_owned()];
        let mut other = vec!["a".to_owned(), "b".to_owned()];
        let mut fewer = vec!["a".to_owned()];
        assert_eq!(fingerprint(&mut one), fingerprint(&mut other));
        assert_ne!(fingerprint(&mut one), fingerprint(&mut fewer));
        // Pinned: a replica and a server of different builds must agree.
        assert_eq!(fingerprint(&mut []), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fingerprint(&mut ["a".to_owned()]), 0x089b_dc07_b544_e7b2);
    }

    #[test]
    fn uploads_merge_per_replica_and_table() {
        let up = |replica: &str, table: &str| Up {
            replica_id: replica.to_owned(),
            tables: vec![rows(table, &[1])],
            rejected: vec![Rejected {
                table: table.to_owned(),
                key: None,
                reason: "why".to_owned(),
            }],
            ..Up::default()
        };
        let merged = Up::by_replica(vec![up("a", "x"), up("b", "x"), up("a", "x"), up("a", "y")]);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].tables.len(), 2);
        assert_eq!(merged[0].tables[0].upserts.len(), 2);
        assert_eq!(merged[0].rejected.len(), 3);
    }
}
