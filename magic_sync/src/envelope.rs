//! What the engine sends a replica and a replica sends back. A host carries
//! them as bytes on whatever transport it has, and never looks inside: the
//! messages are protobuf, as `proto/sync.proto` declares them for a replica
//! in another language, and the
//! [replica protocol](https://github.com/rsansores/magic_sync/blob/main/magic_sync/PROTOCOL.md)
//! says what they mean. What a host sets is how large one may be:
//! [`Limits`].

use crate::Error;

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

pub(crate) use sync::to_replica::Body;
pub(crate) use sync::{
    Backlog, Digest, Down, EndSnapshot, Filed, Key, Rejected, Table, TableBacklog, ToReplica, Up,
};

#[cfg(any(feature = "postgres", test))]
impl Body {
    /// As the bytes a transport carries.
    pub(crate) fn encoded(self) -> Vec<u8> {
        use prost::Message as _;
        ToReplica {
            protocol: PROTOCOL,
            body: Some(self),
        }
        .encode_to_vec()
    }
}

/// The server's seal on its message `sequence` of `epoch`, nonce `nonce`, to
/// `replica`: HMAC-SHA256 under the epoch's secret, cut to [`SEAL`] bytes. A
/// replica gives it back as it came when it says it applied the message, so
/// the server knows it sent that message — this installation, before or
/// after a restore of its database — and no one made it up.
#[cfg(feature = "postgres")]
pub(crate) fn seal(
    secret: &[u8],
    replica: uuid::Uuid,
    epoch: uuid::Uuid,
    sequence: u64,
    nonce: u64,
) -> Vec<u8> {
    use hmac::Mac as _;
    sealer(secret, replica, epoch, sequence, nonce)
        .finalize()
        .into_bytes()[..SEAL]
        .to_vec()
}

/// Whether `mac` is the server's seal on that message ([`seal`]), compared
/// in a time that says nothing of where it differs.
#[cfg(feature = "postgres")]
pub(crate) fn sealed(
    secret: &[u8],
    replica: uuid::Uuid,
    epoch: uuid::Uuid,
    (sequence, nonce): (u64, u64),
    mac: &[u8],
) -> bool {
    use hmac::Mac as _;
    mac.len() == SEAL
        && sealer(secret, replica, epoch, sequence, nonce)
            .verify_truncated_left(mac)
            .is_ok()
}

/// The bytes of a seal.
#[cfg(feature = "postgres")]
const SEAL: usize = 16;

#[cfg(feature = "postgres")]
fn sealer(
    secret: &[u8],
    replica: uuid::Uuid,
    epoch: uuid::Uuid,
    sequence: u64,
    nonce: u64,
) -> hmac::Hmac<sha2::Sha256> {
    use hmac::Mac as _;
    let mut mac =
        hmac::Hmac::<sha2::Sha256>::new_from_slice(secret).expect("HMAC takes a key of any length");
    mac.update(replica.as_bytes());
    mac.update(epoch.as_bytes());
    mac.update(&sequence.to_be_bytes());
    mac.update(&nonce.to_be_bytes());
    mac
}

#[cfg(all(test, feature = "postgres"))]
thread_local! {
    /// The nonces a test pins, in order from this one, instead of drawing
    /// them: so the same build sends the same bytes.
    pub(crate) static PINNED_NONCES: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

/// A message's nonce, drawn at random from 1 to 2^63 - 1.
#[cfg(feature = "postgres")]
pub(crate) fn nonce() -> u64 {
    #[cfg(test)]
    if let Some(pinned) = PINNED_NONCES.with(|p| p.replace(p.get().map(|n| n + 1))) {
        return pinned;
    }
    (rand::random::<u64>() >> 1).max(1)
}

/// A fingerprint of a set of keys, as text: the same on every side and every
/// build (FNV-1a over the keys sorted, each ended by a newline).
pub(crate) fn fingerprint(keys: impl IntoIterator<Item = String>) -> u64 {
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

/// How large a message the transport takes: the engine never hands it a
/// larger one. A row too large for any message is set aside rather than
/// sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    pub(crate) rows: usize,
    pub(crate) bytes: usize,
}

/// What both sides take by default, so that a replica of default settings
/// never builds an upload a server of default settings drops: 500 rows, in
/// half a megabyte.
impl Default for Limits {
    fn default() -> Self {
        Limits {
            rows: 500,
            bytes: 512 * 1024,
        }
    }
}

impl Limits {
    /// At most `rows` rows and `bytes` bytes a message. Refused unless a
    /// message can carry a row: `rows` at least one, and `bytes` more than
    /// [`ENVELOPE_ROOM`].
    ///
    /// # Errors
    ///
    /// [`Error::Misuse`] for limits no message could carry a row within.
    pub fn new(rows: usize, bytes: usize) -> Result<Self, Error> {
        if rows == 0 || bytes <= ENVELOPE_ROOM {
            return Err(Error::Misuse(format!(
                "a message carries a row at least, in more than {ENVELOPE_ROOM} bytes: \
                 {rows} rows in {bytes} bytes cannot"
            )));
        }
        Ok(Limits { rows, bytes })
    }

    /// Rows a message carries, at most.
    #[must_use]
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Bytes a message takes, at most.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// The largest row a message can carry.
    #[cfg(feature = "postgres")]
    pub(crate) fn largest_row(&self) -> usize {
        self.bytes.saturating_sub(ENVELOPE_ROOM)
    }
}

/// One replica's rows made into messages within `limits`, a row at a time,
/// in the order given: a new message begins when the current one is full,
/// and a row larger than a message can carry goes alone. Each row keeps its
/// key beside it, and every byte the rows add to a message counts — their
/// framing and their tables' included — so a message never outgrows the
/// limit by more than [`ENVELOPE_ROOM`] holds. It holds one message at most:
/// what a replica is sent is never in memory whole.
#[cfg(feature = "postgres")]
pub(crate) struct Packer {
    limits: Limits,
    snapshot: String,
    tables: Vec<Table>,
    count: usize,
    bytes: usize,
}

#[cfg(feature = "postgres")]
impl Packer {
    /// Messages of the snapshot `snapshot` (empty: of none).
    pub(crate) fn new(snapshot: &str, limits: Limits) -> Self {
        Packer {
            limits,
            snapshot: snapshot.to_owned(),
            tables: Vec::new(),
            count: 0,
            bytes: 0,
        }
    }

    /// Adds a row of `table` — `Some` bytes to hold, `None` to let go of —
    /// and answers the message it completed, when there was no room left
    /// for it in the current one.
    pub(crate) fn push(&mut self, table: &str, row: Option<Vec<u8>>, key: Key) -> Option<Down> {
        use prost::Message as _;
        let weight = row.as_ref().map_or(0, |row| framed(row.len())) + framed(key.encoded_len());
        let open = self.tables.last().is_some_and(|t| t.table == table);
        let opening = opened(table);
        let added = weight + if open { 0 } else { opening };
        let full = self.count > 0
            && (self.count >= self.limits.rows || self.bytes + added > self.limits.largest_row());
        let done = full.then(|| self.finish()).flatten();
        if self.tables.last().is_none_or(|t| t.table != table) {
            self.tables.push(Table {
                table: table.to_owned(),
                ..Table::default()
            });
            self.bytes += opening;
        }
        self.count += 1;
        self.bytes += weight;
        let part = self.tables.last_mut().expect("its table");
        match row {
            Some(row) => {
                part.upserts.push(row);
                part.keys.push(key);
            }
            None => part.deletes.push(key),
        }
        done
    }

    /// The message begun and not yet answered, if any.
    pub(crate) fn finish(&mut self) -> Option<Down> {
        if self.tables.is_empty() {
            return None;
        }
        (self.count, self.bytes) = (0, 0);
        Some(Down {
            snapshot_id: self.snapshot.clone(),
            tables: std::mem::take(&mut self.tables),
            ..Down::default()
        })
    }
}

/// One replica's rows as messages within `limits`, in their order, as a
/// [`Packer`] makes them.
#[cfg(all(test, feature = "postgres"))]
pub(crate) fn pack(tables: Vec<Table>, snapshot: &str, limits: Limits) -> Vec<Down> {
    let mut packer = Packer::new(snapshot, limits);
    let mut out = Vec::new();
    for table in tables {
        let mut keys = table.keys.into_iter();
        let upserts = table
            .upserts
            .into_iter()
            .map(|row| (Some(row), keys.next()));
        let deletes = table.deletes.into_iter().map(|key| (None, Some(key)));
        for (row, key) in upserts.chain(deletes) {
            out.extend(packer.push(&table.table, row, key.unwrap_or_default()));
        }
    }
    out.extend(packer.finish());
    out
}

/// What `len` bytes weigh as one field of a message: its tag, its length,
/// and themselves.
pub(crate) fn framed(len: usize) -> usize {
    1 + prost::length_delimiter_len(len) + len
}

/// What a table's part weighs in a message before it holds anything: its
/// name, and its own tag and length (at most five bytes of length).
#[cfg(feature = "postgres")]
pub(crate) fn opened(table: &str) -> usize {
    framed(table.len()) + 6
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "postgres")]
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

    #[cfg(feature = "postgres")]
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

    #[cfg(feature = "postgres")]
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

    #[cfg(feature = "postgres")]
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
    #[cfg(feature = "postgres")]
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
                (down.sequence, down.previous) = (u64::MAX, u64::MAX);
                (down.nonce, down.previous_nonce) = (u64::MAX, u64::MAX);
                down.mac = vec![0xff; 16];
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
    fn limits_are_what_they_were_built_with_and_carry_a_row() {
        let limits = Limits::new(3, ENVELOPE_ROOM + 7).unwrap();
        assert_eq!((limits.rows(), limits.bytes()), (3, ENVELOPE_ROOM + 7));
        for (rows, bytes) in [(0, 1 << 20), (1, ENVELOPE_ROOM)] {
            match Limits::new(rows, bytes) {
                Err(Error::Misuse(why)) => assert!(why.contains("cannot"), "{why}"),
                other => panic!("{rows} rows in {bytes} bytes: {other:?}"),
            }
        }
    }

    /// Pinned: a replica and a server of different builds both take it.
    #[test]
    fn the_default_limits_are_500_rows_in_half_a_megabyte() {
        assert_eq!(
            (Limits::default().rows(), Limits::default().bytes()),
            (500, 512 * 1024)
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
