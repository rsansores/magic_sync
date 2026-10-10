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

// Generated from proto/sync.proto: the messages are the file's, field for
// field, and `the_messages_are_generated_from_sync_proto` fails while they
// differ from what it generates.
#[rustfmt::skip]
mod sync;

pub use sync::to_replica::Body;
pub use sync::{
    Backlog, Digest, Down, EndSnapshot, Filed, Key, Rejected, Table, TableBacklog, ToReplica, Up,
};

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

/// A fingerprint of a set of keys, as text: the same on every side and every
/// build (FNV-1a over the keys sorted, each ended by a newline).
pub fn fingerprint(keys: impl IntoIterator<Item = String>) -> u64 {
    let mut keys: Vec<String> = keys.into_iter().collect();
    keys.sort_unstable();
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for key in &keys {
        for byte in key.bytes().chain(std::iter::once(b'\n')) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    hash
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
/// message begins when the current one is full. A row larger than a message
/// can carry goes alone. Each row keeps its key beside it, and every byte the
/// rows add to a message counts — their framing and their tables' included —
/// so a message never outgrows the limit by more than [`ENVELOPE_ROOM`] holds.
pub fn pack(tables: Vec<Table>, snapshot: &str, limits: Limits) -> Vec<Down> {
    use prost::Message as _;
    let budget = limits.largest_row();
    let mut messages: Vec<Vec<Table>> = Vec::new();
    let (mut count, mut bytes) = (0usize, 0usize);
    for table in tables {
        let opening = opened(&table.table);
        let mut keys = table.keys.into_iter();
        let upserts = table
            .upserts
            .into_iter()
            .map(|row| (Some(row), keys.next()));
        let deletes = table.deletes.into_iter().map(|key| (None, Some(key)));
        for (row, key) in upserts.chain(deletes) {
            let weight = row.as_ref().map_or(0, |row| framed(row.len()))
                + key.as_ref().map_or(0, |key| framed(key.encoded_len()));
            let open = messages
                .last()
                .and_then(|m| m.last())
                .is_some_and(|t| t.table == table.table);
            let added = weight + if open { 0 } else { opening };
            if messages.is_empty()
                || (count > 0 && (count >= limits.rows || bytes + added > budget))
            {
                messages.push(Vec::new());
                (count, bytes) = (0, 0);
            }
            let current = messages.last_mut().expect("one begun");
            if current.last().is_none_or(|t| t.table != table.table) {
                current.push(Table {
                    table: table.table.clone(),
                    ..Table::default()
                });
                bytes += opening;
            }
            count += 1;
            bytes += weight;
            let part = current.last_mut().expect("its table");
            match row {
                Some(row) => {
                    part.upserts.push(row);
                    part.keys.extend(key);
                }
                None => part.deletes.extend(key),
            }
        }
    }
    messages
        .into_iter()
        .map(|tables| Down {
            snapshot_id: snapshot.to_owned(),
            tables,
            sequence: 0,
            epoch: String::new(),
        })
        .collect()
}

/// What `len` bytes weigh as one field of a message: its tag, its length,
/// and themselves.
pub fn framed(len: usize) -> usize {
    1 + prost::length_delimiter_len(len) + len
}

/// What a table's part weighs in a message before it holds anything: its
/// name, and its own tag and length (at most five bytes of length).
pub fn opened(table: &str) -> usize {
    framed(table.len()) + 6
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
        // A row of four bytes weighs 6 and its key 5; `parents` opens with 15.
        let limits = Limits {
            rows: 3,
            bytes: ENVELOPE_ROOM + 15 + 2 * 11,
        };
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
                vec![("children".to_owned(), 2)],
                vec![("children".to_owned(), 2)],
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
                bytes: ENVELOPE_ROOM + 100,
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
                bytes: ENVELOPE_ROOM + 30,
            },
        );
        let sizes: Vec<usize> = packed.iter().map(|m| m.tables[0].upserts.len()).collect();
        assert_eq!(sizes, vec![1, 1, 1]);
    }

    /// Whatever the rows, keys and tables, every message as the transport
    /// carries it — numbered, with its snapshot and epoch — fits the limit,
    /// unless it is one row that alone does not.
    #[test]
    fn every_message_packed_fits_the_limit_framing_and_all() {
        let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut next = |below: usize| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            usize::try_from(seed % below as u64).expect("small")
        };
        for _ in 0..200 {
            let limits = Limits {
                rows: 1 + next(50),
                bytes: ENVELOPE_ROOM + 1 + next(3000),
            };
            let tables: Vec<Table> = (0..1 + next(6))
                .map(|t| {
                    let n = next(40);
                    let key = |at: usize| Key {
                        parts: vec![uuid::Uuid::now_v7().to_string(), at.to_string()],
                    };
                    Table {
                        table: format!("table_{t}_{}", "x".repeat(next(30))),
                        upserts: (0..n).map(|_| vec![7; next(400)]).collect(),
                        keys: (0..n).map(key).collect(),
                        deletes: (0..next(20)).map(key).collect(),
                    }
                })
                .collect();
            let offered: usize = tables
                .iter()
                .map(|t| t.upserts.len() + t.deletes.len())
                .sum();
            let packed = pack(tables, &uuid::Uuid::now_v7().to_string(), limits);
            let mut carried = 0;
            for mut down in packed {
                down.sequence = u64::MAX;
                down.epoch = uuid::Uuid::now_v7().to_string();
                let rows: usize = down
                    .tables
                    .iter()
                    .map(|t| t.upserts.len() + t.deletes.len())
                    .sum();
                carried += rows;
                assert!(rows <= limits.rows);
                let len = Body::Down(down).encoded().len();
                assert!(
                    len <= limits.bytes || rows == 1,
                    "{len} bytes over a limit of {}",
                    limits.bytes
                );
            }
            assert_eq!(carried, offered, "every row, once");
        }
    }

    /// The messages are generated from `proto/sync.proto`, the file a replica
    /// in another language generates its own from: regenerated here, they are
    /// the ones checked in. `MAGIC_SYNC_BLESS=1` writes them anew.
    #[test]
    fn the_messages_are_generated_from_sync_proto() {
        let root = env!("CARGO_MANIFEST_DIR");
        let files = protox::compile(["sync.proto"], [format!("{root}/proto")])
            .expect("sync.proto compiles");
        let requests = files
            .file
            .into_iter()
            .map(|file| {
                (
                    prost_build::Module::from_protobuf_package_name(file.package()),
                    file,
                )
            })
            .collect();
        let generated = prost_build::Config::new()
            .generate(requests)
            .expect("prost generates the messages")
            .into_values()
            .next()
            .expect("one package");
        let path = format!("{root}/src/envelope/sync.rs");
        if std::env::var_os("MAGIC_SYNC_BLESS").is_some() {
            std::fs::write(&path, &generated).expect("write the messages");
        }
        let checked_in = std::fs::read_to_string(&path).unwrap_or_default();
        assert!(
            checked_in == generated,
            "src/envelope/sync.rs is not what proto/sync.proto generates: \
             run this test with MAGIC_SYNC_BLESS=1 and commit the result"
        );
    }

    #[test]
    fn a_fingerprint_is_of_the_set_not_its_order() {
        let set = |keys: &[&str]| fingerprint(keys.iter().map(|k| (*k).to_owned()));
        assert_eq!(set(&["b", "a"]), set(&["a", "b"]));
        assert_ne!(set(&["b", "a"]), set(&["a"]));
        // Pinned: a replica and a server of different builds must agree.
        assert_eq!(set(&[]), 0xcbf2_9ce4_8422_2325);
        assert_eq!(set(&["a"]), 0x089b_dc07_b544_e7b2);
    }
}
