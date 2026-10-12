//! A mirrored table on the wire: its rows encoded as the table's own
//! message, and applied on the other side by the rules its row declares.
//!
//! A [`Codec`] erases the row type, so the engine holds every table in one
//! list — the host's, in the order the tables are sent, parents first — and
//! dispatches on the table name. Nothing here names a table or a column.

#[cfg(feature = "postgres")]
use crate::row::Mirror;
use crate::row::{self, Key, KeyPart, Row, Table};
use crate::{Conn, Error};
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;

type Fut<'a, T> = Pin<Box<dyn Future<Output = Result<T, Error>> + Send + 'a>>;

/// Files a row a replica sent under that replica, whatever the row said: it
/// is handed the row, the replica's id, and what the server knows of it
/// (`P`, the host's: the server's `Senders` says what it is).
pub type Place<R, P> = fn(&mut R, Uuid, &P);

/// One mirrored table: how its rows turn into bytes and back, and how each
/// side writes the other's, without its row type. `P` is what the server
/// knows of a replica that uploads, handed to the table's [`Place`].
///
/// The host lists its codecs once, parents before children, and hands the
/// list to the runner of its side ([`run`](crate::run)): the same list on
/// both sides.
///
/// ```
/// use magic_sync::wire::{Codec, MapError};
/// use uuid::Uuid;
///
/// #[derive(sqlx::FromRow, Clone, serde::Serialize, serde::Deserialize, magic_sync::Row)]
/// #[row(table = "receipts", schema = "shop", mirror = "record")]
/// pub struct Receipt {
///     pub id: Uuid,
///     pub register_id: Uuid,
///     pub total_cents: i64,
/// }
///
/// /// A receipt is filed under the register that sent it, whatever it says.
/// fn place(receipt: &mut Receipt, register: Uuid, _: &()) {
///     receipt.register_id = register;
/// }
///
/// /// The same table as a protobuf message of the host's own.
/// #[derive(Clone, PartialEq, prost::Message)]
/// pub struct ReceiptRow {
///     #[prost(string, tag = "1")]
///     pub id: String,
///     #[prost(string, tag = "2")]
///     pub register_id: String,
///     #[prost(int64, tag = "3")]
///     pub total_cents: i64,
/// }
///
/// fn to_wire(r: Receipt) -> Result<ReceiptRow, MapError> {
///     Ok(ReceiptRow {
///         id: r.id.to_string(),
///         register_id: r.register_id.to_string(),
///         total_cents: r.total_cents,
///     })
/// }
///
/// fn from_wire(r: ReceiptRow) -> Result<Receipt, MapError> {
///     Ok(Receipt {
///         id: r.id.parse()?,
///         register_id: r.register_id.parse()?,
///         total_cents: r.total_cents,
///     })
/// }
///
/// let as_json: Codec<()> = Codec::json::<Receipt>(Some(place));
/// let as_protobuf: Codec<()> = Codec::new::<Receipt, ReceiptRow>(to_wire, from_wire, Some(place));
/// ```
pub struct Codec<P> {
    pub(crate) table: Table,
    ops: Box<dyn Ops<P>>,
}

impl<P> std::fmt::Debug for Codec<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Codec")
            .field("table", &self.table.name)
            .finish_non_exhaustive()
    }
}

/// Why a row could not be mapped to its message, or a message back to its
/// row: what a [`Codec::new`] mapper fails with. Any error converts into it
/// with `?`.
pub struct MapError(String);

impl MapError {
    /// A mapping that failed for `reason`.
    #[must_use]
    pub fn new(reason: impl std::fmt::Display) -> Self {
        MapError(reason.to_string())
    }
}

impl<E: std::error::Error> From<E> for MapError {
    fn from(e: E) -> Self {
        MapError(e.to_string())
    }
}

impl std::fmt::Display for MapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::fmt::Debug for MapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("MapError").field(&self.0).finish()
    }
}

impl<P: Sync + 'static> Codec<P> {
    /// A table that travels as `M`, a protobuf message of the host's: `to`
    /// and `from` map a row to its message and back, and `place` stamps a
    /// row a replica uploads. A row of a table keyed by its replica
    /// (`origin`) must name its sender there once placed: one that names
    /// another is malformed, and left out. A mapping that fails is the row's: a row that
    /// does not encode is set aside, one that does not decode refused.
    pub fn new<R, M>(
        to: fn(R) -> Result<M, MapError>,
        from: fn(M) -> Result<R, MapError>,
        place: Option<Place<R, P>>,
    ) -> Self
    where
        R: Row + Clone + Send + Sync,
        M: prost::Message + Default + 'static,
    {
        Self::typed(
            std::any::type_name::<M>(),
            Box::new(move |row: R| {
                to(row)
                    .map(|m| m.encode_to_vec())
                    .map_err(|e| Error::Misuse(format!("`{}` does not encode: {e}", R::TABLE)))
            }),
            Box::new(move |bytes: &[u8]| {
                let message = M::decode(bytes).map_err(|e| malformed(R::TABLE, e))?;
                from(message).map_err(|e| malformed(R::TABLE, e))
            }),
            place,
        )
    }

    /// A table that travels as JSON, with no message of its own to declare:
    /// a row is an object of its columns, keyed by their names, each value as
    /// the field's serde writes it — but an `i64` field as its decimal text,
    /// which a JSON number would round past 2^53 for a replica that reads
    /// numbers as doubles. A field serde names otherwise than itself does not
    /// encode. That holds for `#[serde(rename)]` and `rename_all` on the
    /// row, and for a part flattened in with `#[sqlx(flatten)]` unless the
    /// field also says `#[serde(flatten)]`, so that serde writes the part's
    /// fields beside the row's: otherwise every row of the table fails to
    /// encode ([`Error::Misuse`]), the first time one travels.
    ///
    /// `place` stamps a row a replica uploads; one of a table keyed by its
    /// replica must name its sender, as with [`Codec::new`].
    pub fn json<R>(place: Option<Place<R, P>>) -> Self
    where
        R: Row + Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned,
    {
        let columns = Table::of::<R>().columns;
        let columns_back = columns.clone();
        Self::typed(
            "json",
            Box::new(move |row: R| json_encode(R::TABLE, &columns, &row)),
            Box::new(move |bytes: &[u8]| json_decode(R::TABLE, &columns_back, bytes)),
            place,
        )
    }

    /// The codec of `R`, encoded and decoded so.
    fn typed<R>(
        encoding: &'static str,
        encode: Encode<R>,
        decode: Decode<R>,
        place: Option<Place<R, P>>,
    ) -> Self
    where
        R: Row + Clone + Send + Sync,
    {
        // A replica files nothing: placing a row is the server's.
        #[cfg(feature = "sqlite")]
        let place = {
            let _ = place;
            std::marker::PhantomData
        };
        // What a row carries is the server's to compare across deploys.
        #[cfg(feature = "sqlite")]
        let _ = encoding;
        let table = Table {
            #[cfg(feature = "postgres")]
            encoding,
            ..Table::of::<R>()
        };
        Self {
            ops: Box::new(Typed {
                owned: table.replica_owned(),
                #[cfg(feature = "postgres")]
                origin: table
                    .origin
                    .and_then(|origin| table.key.iter().position(|k| *k == origin)),
                encode,
                decode,
                place,
            }),
            table,
        }
    }

    /// A key of this table from its parts' text, as its key columns type them.
    pub(crate) fn key(&self, parts: &[String]) -> Result<Key, Error> {
        if parts.len() != self.table.key.len() {
            return Err(Error::Malformed {
                table: self.table.name.to_owned(),
                reason: format!(
                    "a key of {} parts, and the table is keyed by {:?}",
                    parts.len(),
                    self.table.key
                ),
            });
        }
        self.table
            .key
            .iter()
            .zip(parts)
            .map(|(name, part)| {
                let column = self
                    .table
                    .columns
                    .iter()
                    .find(|c| c.name == *name)
                    .expect("a key column is a column");
                KeyPart::parse(column.key, part).ok_or_else(|| Error::Malformed {
                    table: self.table.name.to_owned(),
                    reason: format!("`{part}` is not a value of the key column `{name}`"),
                })
            })
            .collect()
    }

    /// `row`, encoded as this table's: refused when `R` is not the table's
    /// row.
    #[cfg(feature = "postgres")]
    pub(crate) fn encode_row<R: Row>(&self, row: R) -> Result<Vec<u8>, Error> {
        self.ops.encode_any(Box::new(row))
    }

    /// `bytes`, decoded as this table's row: refused when `R` is not the
    /// table's row, or the bytes do not decode ([`Error::Malformed`]).
    #[cfg(feature = "postgres")]
    pub(crate) fn decode_row<R: Row>(&self, bytes: &[u8]) -> Result<R, Error> {
        self.decode_any(bytes)?
            .downcast::<R>()
            .map(|row| *row)
            .map_err(|_| self.not_its_row::<R>())
    }

    #[cfg(feature = "postgres")]
    fn not_its_row<R: Row>(&self) -> Error {
        Error::Misuse(format!(
            "`{}` is not the row of the table `{}`",
            std::any::type_name::<R>(),
            self.table.name
        ))
    }

    /// `bytes`, decoded as this table's row, its type erased.
    pub(crate) fn decode_any(&self, bytes: &[u8]) -> Result<Box<dyn std::any::Any + Send>, Error> {
        self.ops.decode_any(bytes)
    }

    /// The rows of `keys`, retired or not, encoded; a key with no row is
    /// left out, which is how its absence is seen. A row that does not encode
    /// is answered apart, so one row cannot keep its table from travelling.
    pub(crate) fn load<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, Encoded> {
        self.ops.load(conn, keys)
    }

    /// A replica writing what the server sent, each row beside the key it
    /// came with (`None` when that key does not parse): every column the
    /// replica does not own, whatever the version — and a row it had retired
    /// is offered again. A record is refused. Answers the rows it could not
    /// take — that do not decode, that came beside a key not their own, or
    /// that the database refuses for their data — by their place in `rows`,
    /// with why: the rest it now holds, under the keys they came beside.
    #[cfg(feature = "sqlite")]
    pub(crate) fn apply_down<'a>(
        &'a self,
        conn: &'a mut Conn,
        rows: Vec<(Vec<u8>, Option<Key>)>,
    ) -> Fut<'a, Vec<Refused>> {
        self.ops.apply_down(conn, rows)
    }

    /// The server writing what a replica sent: a record first-delivery-wins
    /// (or newest-wins when it carries a version), a two-way table only the
    /// columns the replica owns, of a row the server has and records `sender`
    /// holding; a catalog is refused. `sender` is the replica
    /// the message came from; `known` is the server's knowledge of it. Answers
    /// the keys written, and the rows it could not take — that do not
    /// decode, or that the database refuses for their data — by their place in
    /// `rows`, with why.
    #[cfg(feature = "postgres")]
    pub(crate) fn apply_up<'a>(
        &'a self,
        conn: &'a mut Conn,
        rows: Vec<Vec<u8>>,
        sender: Uuid,
        known: &'a P,
    ) -> Fut<'a, Taken> {
        self.ops.apply_up(conn, rows, sender, known)
    }

    /// A replica letting rows go together: deleted, or retired where a row
    /// that stays for the replica still points at them — of its own, or one
    /// of `server`'s tables (each with its retired column) it retired — or
    /// refused one by one ([`row::retire_by_keys`]); the rows of `held`
    /// retired, none deleted.
    #[cfg(feature = "sqlite")]
    pub(crate) fn retire<'a>(
        &'a self,
        conn: &'a mut Conn,
        keys: &'a [Key],
        held: &'a [Key],
        server: &'a [(&'a str, Option<&'a str>)],
    ) -> Fut<'a, row::Retired> {
        self.ops.retire(conn, keys, held, server)
    }

    /// A replica letting go of the retired rows of `keys` nothing that
    /// stays points at ([`row::collect_by_keys`]): answers how many went.
    #[cfg(feature = "sqlite")]
    pub(crate) fn collect<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, u64> {
        self.ops.collect(conn, keys)
    }

    /// The keys of `keys` this replica holds, retired or not.
    #[cfg(feature = "sqlite")]
    pub(crate) fn present<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, Vec<Key>> {
        self.ops.present(conn, keys)
    }

    /// What every foreign key into this table points at, recorded in
    /// temporary tables named from `name` on ([`row::pointing`]).
    #[cfg(feature = "sqlite")]
    pub(crate) fn pointing<'a>(
        &'a self,
        conn: &'a mut Conn,
        name: usize,
    ) -> Fut<'a, Vec<row::Pointing>> {
        self.ops.pointing(conn, name)
    }

    /// The keys of the rows `pointing` recorded that rows still point at
    /// through a key the database finds broken ([`row::broken`]).
    #[cfg(feature = "sqlite")]
    pub(crate) fn broken<'a>(
        &'a self,
        conn: &'a mut Conn,
        pointing: &'a [row::Pointing],
    ) -> Fut<'a, Vec<Key>> {
        self.ops.broken(conn, pointing)
    }

    /// The keys of `keys` whose row the database finds pointing at a row it
    /// does not hold ([`row::orphaned`]).
    #[cfg(feature = "sqlite")]
    pub(crate) fn orphaned<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, Vec<Key>> {
        self.ops.orphaned(conn, keys)
    }

    /// The keys of `keys` whose row points at one this replica does not hold
    /// ([`row::unparented`]).
    #[cfg(feature = "sqlite")]
    pub(crate) fn unparented<'a>(
        &'a self,
        conn: &'a mut Conn,
        keys: &'a [Key],
    ) -> Fut<'a, Vec<Key>> {
        self.ops.unparented(conn, keys)
    }

    /// Every key this side holds.
    #[cfg(feature = "sqlite")]
    pub(crate) fn keys<'a>(&'a self, conn: &'a mut Conn) -> Fut<'a, Vec<Key>> {
        self.ops.keys(conn)
    }

    /// Every key this side holds that is not retired.
    #[cfg(feature = "sqlite")]
    pub(crate) fn live_keys<'a>(&'a self, conn: &'a mut Conn) -> Fut<'a, Vec<Key>> {
        self.ops.live_keys(conn)
    }

    /// Every key this side holds retired.
    #[cfg(feature = "sqlite")]
    pub(crate) fn retired_keys<'a>(&'a self, conn: &'a mut Conn) -> Fut<'a, Vec<Key>> {
        self.ops.retired_keys(conn)
    }

    /// Whether the server sends this table's rows down.
    pub(crate) fn sent_down(&self) -> bool {
        self.table.mirror.sent_down()
    }

    /// Whether a replica's writes to this table go up.
    #[cfg(feature = "sqlite")]
    pub(crate) fn goes_up(&self) -> bool {
        self.table.mirror.goes_up()
    }

    /// Where the replica sits in this table's key: only a table keyed by it
    /// takes a replica's deletes, each replica's rows its own.
    fn origin_part(&self) -> Option<usize> {
        let origin = self.table.origin?;
        self.table.key.iter().position(|k| *k == origin)
    }

    /// Whether a replica's deletes reach the server: history a replica lets
    /// go of before it went up is simply not sent.
    pub(crate) fn takes_deletes(&self) -> bool {
        self.origin_part().is_some()
    }

    /// The server letting go of rows a replica deleted — of `sender`'s rows
    /// only, whatever the keys name. Answers the keys it let go of.
    #[cfg(feature = "postgres")]
    pub(crate) async fn delete_up(
        &self,
        conn: &mut Conn,
        keys: &[Key],
        sender: Uuid,
    ) -> Result<Vec<Key>, Error> {
        if !self.table.mirror.goes_up() {
            return Err(Error::Misuse(format!(
                "`{}` is the server's: a replica deletes none of it",
                self.table.name
            )));
        }
        let Some(part) = self.origin_part() else {
            return Err(Error::Misuse(format!(
                "`{}` is not keyed by its replica: a replica deletes nothing from it",
                self.table.name
            )));
        };
        let own: Vec<Key> = keys
            .iter()
            .filter(|key| key.get(part) == Some(&KeyPart::Uuid(sender)))
            .cloned()
            .collect();
        self.ops.delete(conn, &own).await?;
        Ok(own)
    }
}

/// Things by their place in a message's rows.
type Placed<T> = Vec<(usize, T)>;

/// What [`Codec::apply_up`] did with a message's rows.
#[cfg(feature = "postgres")]
#[derive(Debug, Default)]
pub(crate) struct Taken {
    /// The keys of the rows this side now holds.
    pub(crate) held: Vec<Key>,
    /// The rows it could not take.
    pub(crate) rejected: Vec<Refused>,
}

/// A row of a message a side could not take.
#[derive(Debug)]
pub(crate) struct Refused {
    /// Its place in the message's rows.
    pub(crate) at: usize,
    /// Why.
    pub(crate) reason: String,
}

/// Rows as [`Codec::load`] read them: encoded, and the ones that did not
/// encode, with why.
#[derive(Debug, Default)]
pub(crate) struct Encoded {
    pub(crate) rows: Vec<(Key, Vec<u8>)>,
    pub(crate) failed: Vec<(Key, Error)>,
}

trait Ops<P>: Send + Sync {
    fn load<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, Encoded>;
    #[cfg(feature = "postgres")]
    fn encode_any(&self, row: Box<dyn std::any::Any + Send>) -> Result<Vec<u8>, Error>;
    fn decode_any(&self, bytes: &[u8]) -> Result<Box<dyn std::any::Any + Send>, Error>;
    #[cfg(feature = "sqlite")]
    fn apply_down<'a>(
        &'a self,
        conn: &'a mut Conn,
        rows: Vec<(Vec<u8>, Option<Key>)>,
    ) -> Fut<'a, Vec<Refused>>;
    #[cfg(feature = "postgres")]
    fn apply_up<'a>(
        &'a self,
        conn: &'a mut Conn,
        rows: Vec<Vec<u8>>,
        sender: Uuid,
        known: &'a P,
    ) -> Fut<'a, Taken>;
    #[cfg(feature = "sqlite")]
    fn retire<'a>(
        &'a self,
        conn: &'a mut Conn,
        keys: &'a [Key],
        held: &'a [Key],
        server: &'a [(&'a str, Option<&'a str>)],
    ) -> Fut<'a, row::Retired>;
    #[cfg(feature = "sqlite")]
    fn collect<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, u64>;
    #[cfg(feature = "postgres")]
    fn delete<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, u64>;
    #[cfg(feature = "sqlite")]
    fn unparented<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, Vec<Key>>;
    #[cfg(feature = "sqlite")]
    fn present<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, Vec<Key>>;
    #[cfg(feature = "sqlite")]
    fn pointing<'a>(&'a self, conn: &'a mut Conn, name: usize) -> Fut<'a, Vec<row::Pointing>>;
    #[cfg(feature = "sqlite")]
    fn broken<'a>(&'a self, conn: &'a mut Conn, pointing: &'a [row::Pointing])
        -> Fut<'a, Vec<Key>>;
    #[cfg(feature = "sqlite")]
    fn orphaned<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, Vec<Key>>;
    #[cfg(feature = "sqlite")]
    fn keys<'a>(&'a self, conn: &'a mut Conn) -> Fut<'a, Vec<Key>>;
    #[cfg(feature = "sqlite")]
    fn live_keys<'a>(&'a self, conn: &'a mut Conn) -> Fut<'a, Vec<Key>>;
    #[cfg(feature = "sqlite")]
    fn retired_keys<'a>(&'a self, conn: &'a mut Conn) -> Fut<'a, Vec<Key>>;
}

/// How a side writes rows of the other's.
#[derive(Clone, Copy)]
enum How {
    /// A replica taking the server's: every column but its own, newest wins.
    #[cfg(feature = "sqlite")]
    AllButOwned,
    /// The server taking a two-way table's: only the columns the replica
    /// owns, of a row the replica named is recorded holding.
    #[cfg(feature = "postgres")]
    OnlyOwned(Uuid),
    /// The server taking state a replica reports: newest wins.
    #[cfg(feature = "postgres")]
    Newest,
    /// The server taking a setting: what the replica sent last wins,
    /// whatever its version — a replica restored from an older copy holds
    /// an older version, and that is what the setting is.
    #[cfg(feature = "postgres")]
    Latest,
    /// The server taking history a replica records: first delivery wins.
    #[cfg(feature = "postgres")]
    First,
}

type Encode<R> = Box<dyn Fn(R) -> Result<Vec<u8>, Error> + Send + Sync>;
type Decode<R> = Box<dyn Fn(&[u8]) -> Result<R, Error> + Send + Sync>;

struct Typed<R, P> {
    /// The columns the replicas own.
    owned: Vec<&'static str>,
    /// Where the replica that wrote a row sits in its key, for a table keyed
    /// by it.
    #[cfg(feature = "postgres")]
    origin: Option<usize>,
    encode: Encode<R>,
    decode: Decode<R>,
    /// Stamps a row a replica uploads: the server's alone.
    #[cfg(feature = "postgres")]
    place: Option<Place<R, P>>,
    #[cfg(feature = "sqlite")]
    place: std::marker::PhantomData<fn(&P)>,
}

fn malformed(table: &str, e: impl std::fmt::Display) -> Error {
    Error::Malformed {
        table: table.to_owned(),
        reason: format!("a row that does not decode: {e}"),
    }
}

/// A row as [`Codec::json`] writes it: its columns by name, a 64-bit integer
/// as text.
pub(crate) fn json_encode<R: serde::Serialize>(
    table: &str,
    columns: &[row::Column],
    row: &R,
) -> Result<Vec<u8>, Error> {
    use serde_json::Value;
    let misuse = |why: String| Error::Misuse(format!("`{table}` as JSON: {why}"));
    let Value::Object(mut fields) = serde_json::to_value(row).map_err(|e| misuse(e.to_string()))?
    else {
        return Err(misuse("serde does not write the row as an object".into()));
    };
    let mut out = serde_json::Map::new();
    for column in columns {
        let value = fields.remove(column.field).ok_or_else(|| {
            misuse(format!(
                "serde does not write the field `{}` by its name",
                column.field
            ))
        })?;
        let value = match (column.int64, value) {
            (true, Value::Number(n)) => Value::String(n.to_string()),
            (_, value) => value,
        };
        out.insert(column.name.to_owned(), value);
    }
    serde_json::to_vec(&Value::Object(out)).map_err(|e| misuse(e.to_string()))
}

/// A row [`json_encode`] wrote, read back.
pub(crate) fn json_decode<R: serde::de::DeserializeOwned>(
    table: &str,
    columns: &[row::Column],
    bytes: &[u8],
) -> Result<R, Error> {
    use serde_json::Value;
    let Value::Object(mut fields) =
        serde_json::from_slice(bytes).map_err(|e| malformed(table, e))?
    else {
        return Err(malformed(table, "not a JSON object"));
    };
    let mut out = serde_json::Map::new();
    for column in columns {
        let value = fields.remove(column.name).unwrap_or(Value::Null);
        let value = match (column.int64, value) {
            (true, Value::String(text)) => Value::Number(
                text.parse::<i64>()
                    .map_err(|_| {
                        malformed(table, format!("`{}` is not a 64-bit integer", column.name))
                    })?
                    .into(),
            ),
            (_, value) => value,
        };
        out.insert(column.field.to_owned(), value);
    }
    serde_json::from_value(Value::Object(out)).map_err(|e| malformed(table, e))
}

impl<R, P> Typed<R, P>
where
    R: Row + Clone + Send + Sync,
{
    /// Every row that decodes, by its place in `rows`; one that does not is
    /// answered apart, so it cannot take the rest of its table down with it.
    fn decode(&self, rows: Vec<Vec<u8>>) -> (Placed<R>, Vec<Refused>) {
        let mut decoded = Vec::new();
        let mut failed = Vec::new();
        for (at, bytes) in rows.into_iter().enumerate() {
            let row = (self.decode)(bytes.as_slice());
            match row {
                Ok(row) => decoded.push((at, row)),
                Err(e) => {
                    failed.push(Refused {
                        at,
                        reason: e.to_string(),
                    });
                }
            }
        }
        (decoded, failed)
    }

    async fn write(&self, conn: &mut Conn, rows: Vec<R>, how: How) -> Result<Vec<Key>, Error> {
        match how {
            #[cfg(feature = "sqlite")]
            // A retired row sent again is live again in the same write: one
            // whose unique values a live row holds is refused with it.
            How::AllButOwned => {
                let keys = row::overwrite_except(conn, rows, &self.owned).await?;
                row::unretire_by_keys::<R>(conn, &keys).await?;
                Ok(keys)
            }
            #[cfg(feature = "postgres")]
            How::OnlyOwned(sender) => row::update_held(conn, rows, &self.owned, sender).await,
            #[cfg(feature = "postgres")]
            How::Newest => row::upsert(conn, rows).await,
            #[cfg(feature = "postgres")]
            How::Latest => row::overwrite(conn, rows).await,
            #[cfg(feature = "postgres")]
            How::First => row::insert_ignore(conn, rows).await,
        }
    }

    /// Writes `rows`, and when the database refuses them for their data (a
    /// duplicate, a dangling reference) writes them one by one, leaving out
    /// each it refuses — a message must not be refused forever for one row;
    /// the caller says what becomes of it. Answers the keys written, and the places of the rows
    /// refused with why.
    async fn guarded(
        &self,
        conn: &mut Conn,
        rows: Placed<R>,
        how: How,
    ) -> Result<(Vec<Key>, Vec<Refused>), Error> {
        if rows.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let all: Vec<R> = rows.iter().map(|(_, row)| row.clone()).collect();
        let all = match self.attempt(conn, all, how).await? {
            Ok(written) => return Ok((written, Vec::new())),
            Err(e) => e,
        };
        tracing::warn!(
            table = R::TABLE,
            "rows refused together, written one by one: {all}"
        );
        let mut written = Vec::new();
        let mut refused = Vec::new();
        for (at, one) in rows {
            match self.attempt(conn, vec![one], how).await? {
                Ok(keys) => written.extend(keys),
                Err(e) => refused.push(Refused {
                    at,
                    reason: e.to_string(),
                }),
            }
        }
        Ok((written, refused))
    }

    /// One write in a savepoint ([`row::released`]): a refusal comes back as
    /// the inner error, for the rows to be tried one by one.
    async fn attempt(
        &self,
        conn: &mut Conn,
        rows: Vec<R>,
        how: How,
    ) -> Result<Result<Vec<Key>, Error>, Error> {
        row::savepoint(conn).await?;
        let result = self.write(conn, rows, how).await;
        row::released(conn, result).await
    }
}

/// What becomes of the edits of a two-way table's rows `sender` uploaded
/// that the server did not write, of rows it does not record `sender`
/// holding. While a provisioning of `sender` is pending, or written and not
/// confirmed, the server records nothing of what it held: each waits in the
/// inbox, answered by its place in the upload, until it does. Otherwise the
/// row is one `sender` no longer holds — the server deleted it, or it left
/// `sender`'s scope, and either wins — and the edit is left out; or one the
/// server never sent it, which breaks the protocol, and is left out too.
#[cfg(feature = "postgres")]
async fn unheld<R: Row>(
    conn: &mut Conn,
    unwritten: Vec<(usize, Key)>,
    sender: Uuid,
) -> Result<Vec<Refused>, Error> {
    if unwritten.is_empty() {
        return Ok(Vec::new());
    }
    let (provisioning, sent): (bool, Vec<String>) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM sync.notes WHERE kind = 'resync' AND replica_id = $1 \
                          AND done_at IS NULL) \
             OR EXISTS (SELECT 1 FROM sync.chains c CROSS JOIN sync.epoch e \
                          LEFT JOIN sync.replicas r USING (replica_id) \
                         WHERE c.replica_id = $1 AND c.snapshot_end IS NOT NULL \
                           AND (r.applied_epoch IS DISTINCT FROM e.epoch \
                                OR coalesce(r.applied_sequence, 0) < c.snapshot_end)), \
                ARRAY(SELECT row_key FROM sync.held \
                       WHERE replica_id = $1 AND relation = $2 AND row_key = ANY($3) \
                      UNION SELECT row_key FROM sync.gone \
                       WHERE replica_id = $1 AND relation = $2 AND row_key = ANY($3))",
    )
    .bind(sender)
    .bind(R::TABLE)
    .bind(
        unwritten
            .iter()
            .map(|(_, key)| row::key_text(key))
            .collect::<Vec<_>>(),
    )
    .fetch_one(&mut *conn)
    .await?;
    if provisioning {
        return Ok(unwritten
            .into_iter()
            .map(|(at, _)| Refused {
                at,
                reason: "an edit of a row the server does not record the replica holding, \
                         while it provisions it again"
                    .to_owned(),
            })
            .collect());
    }
    for (_, key) in unwritten {
        let text = row::key_text(&key);
        if sent.contains(&text) {
            tracing::debug!(%sender, table = R::TABLE, key = text, "upload: an edit of a two-way row the replica no longer holds — left out");
        } else {
            tracing::error!(%sender, table = R::TABLE, key = text, "upload: an edit of a two-way row never sent it — malformed, left out");
        }
    }
    Ok(Vec::new())
}

impl<R, P: Sync> Ops<P> for Typed<R, P>
where
    R: Row + Clone + Send + Sync,
{
    fn load<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, Encoded> {
        Box::pin(async move {
            let mut encoded = Encoded::default();
            for row in row::get_by_keys::<R>(conn, keys).await? {
                let key = row.key();
                match (self.encode)(row) {
                    Ok(bytes) => encoded.rows.push((key, bytes)),
                    Err(e) => encoded.failed.push((key, e)),
                }
            }
            Ok(encoded)
        })
    }

    #[cfg(feature = "postgres")]
    fn encode_any(&self, row: Box<dyn std::any::Any + Send>) -> Result<Vec<u8>, Error> {
        let row = row.downcast::<R>().map_err(|_| {
            Error::Misuse(format!(
                "a row that is not `{}`'s, to encode as one",
                R::TABLE
            ))
        })?;
        (self.encode)(*row)
    }

    fn decode_any(&self, bytes: &[u8]) -> Result<Box<dyn std::any::Any + Send>, Error> {
        Ok(Box::new((self.decode)(bytes)?))
    }

    #[cfg(feature = "sqlite")]
    fn apply_down<'a>(
        &'a self,
        conn: &'a mut Conn,
        rows: Vec<(Vec<u8>, Option<Key>)>,
    ) -> Fut<'a, Vec<Refused>> {
        Box::pin(async move {
            if !R::MIRROR.sent_down() {
                return Err(Error::Misuse(format!(
                    "`{}` is not sent down: a replica takes none of it",
                    R::TABLE
                )));
            }
            let (bytes, beside): (Vec<Vec<u8>>, Vec<Option<Key>>) = rows.into_iter().unzip();
            let (decoded, mut refused) = self.decode(bytes);
            let mut pending = Vec::new();
            for (at, row) in decoded {
                if beside[at].as_ref() == Some(&row.key()) {
                    pending.push((at, row));
                } else {
                    refused.push(Refused {
                        at,
                        reason: "a row beside a key that is not its own".to_owned(),
                    });
                }
            }
            let (_, again) = self.guarded(conn, pending, How::AllButOwned).await?;
            refused.extend(again);
            Ok(refused)
        })
    }

    #[cfg(feature = "postgres")]
    fn apply_up<'a>(
        &'a self,
        conn: &'a mut Conn,
        rows: Vec<Vec<u8>>,
        sender: Uuid,
        known: &'a P,
    ) -> Fut<'a, Taken> {
        Box::pin(async move {
            let how = match R::MIRROR {
                Mirror::Both => How::OnlyOwned(sender),
                Mirror::Record if R::VERSION.is_some() => How::Newest,
                Mirror::Setting => How::Latest,
                Mirror::Record => How::First,
                _ => {
                    return Err(Error::Misuse(format!(
                        "`{}` is the server's: a replica writes none of it",
                        R::TABLE
                    )))
                }
            };
            let (mut rows, mut rejected) = self.decode(rows);
            if let Some(place) = self.place {
                for (_, row) in &mut rows {
                    place(row, sender, known);
                }
            }
            // A row keyed by its replica is the sender's, whatever `place`
            // did: one that names another is no row this sender may write.
            if let Some(part) = self.origin {
                rows.retain(|(_, row)| {
                    let own = row.key().get(part) == Some(&KeyPart::Uuid(sender));
                    if !own {
                        tracing::error!(
                            %sender,
                            table = R::TABLE,
                            "upload: a row of another replica — malformed, left out"
                        );
                    }
                    own
                });
            }
            let offered: Vec<(usize, Key)> =
                rows.iter().map(|(at, row)| (*at, row.key())).collect();
            let (written, refused) = self.guarded(conn, rows, how).await?;
            rejected.extend(refused);
            if let How::OnlyOwned(_) = how {
                let unwritten: Vec<(usize, Key)> = offered
                    .into_iter()
                    .filter(|(at, key)| {
                        !written.contains(key) && !rejected.iter().any(|r| r.at == *at)
                    })
                    .collect();
                rejected.extend(unheld::<R>(conn, unwritten, sender).await?);
            }
            Ok(Taken {
                held: written,
                rejected,
            })
        })
    }

    #[cfg(feature = "sqlite")]
    fn retire<'a>(
        &'a self,
        conn: &'a mut Conn,
        keys: &'a [Key],
        held: &'a [Key],
        server: &'a [(&'a str, Option<&'a str>)],
    ) -> Fut<'a, row::Retired> {
        Box::pin(row::retire_by_keys::<R>(conn, keys, held, server))
    }

    #[cfg(feature = "sqlite")]
    fn collect<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, u64> {
        Box::pin(row::collect_by_keys::<R>(conn, keys))
    }

    #[cfg(feature = "postgres")]
    fn delete<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, u64> {
        Box::pin(row::delete_by_keys::<R>(conn, keys))
    }

    #[cfg(feature = "sqlite")]
    fn keys<'a>(&'a self, conn: &'a mut Conn) -> Fut<'a, Vec<Key>> {
        Box::pin(row::keys::<R>(conn))
    }

    #[cfg(feature = "sqlite")]
    fn live_keys<'a>(&'a self, conn: &'a mut Conn) -> Fut<'a, Vec<Key>> {
        Box::pin(row::live_keys::<R>(conn))
    }

    #[cfg(feature = "sqlite")]
    fn retired_keys<'a>(&'a self, conn: &'a mut Conn) -> Fut<'a, Vec<Key>> {
        Box::pin(row::retired_keys::<R>(conn))
    }

    #[cfg(feature = "sqlite")]
    fn unparented<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, Vec<Key>> {
        Box::pin(row::unparented::<R>(conn, keys))
    }

    #[cfg(feature = "sqlite")]
    fn present<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, Vec<Key>> {
        Box::pin(row::key_select::<R>(conn, keys))
    }

    #[cfg(feature = "sqlite")]
    fn pointing<'a>(&'a self, conn: &'a mut Conn, name: usize) -> Fut<'a, Vec<row::Pointing>> {
        Box::pin(row::pointing::<R>(conn, name))
    }

    #[cfg(feature = "sqlite")]
    fn broken<'a>(
        &'a self,
        conn: &'a mut Conn,
        pointing: &'a [row::Pointing],
    ) -> Fut<'a, Vec<Key>> {
        Box::pin(row::broken::<R>(conn, pointing))
    }

    #[cfg(feature = "sqlite")]
    fn orphaned<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, Vec<Key>> {
        Box::pin(row::orphaned::<R>(conn, keys))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[derive(
        sqlx::FromRow, Clone, Debug, PartialEq, crate::Columns, serde::Serialize, serde::Deserialize,
    )]
    struct Label {
        label: String,
    }

    /// A part flattened in for sqlx, and for serde too.
    #[derive(
        sqlx::FromRow, Clone, Debug, PartialEq, crate::Row, serde::Serialize, serde::Deserialize,
    )]
    #[row(table = "flat", mirror = "catalog")]
    struct Flat {
        id: Uuid,
        #[sqlx(flatten)]
        #[serde(flatten)]
        part: Label,
    }

    /// A part flattened in for sqlx only: serde nests it.
    #[derive(
        sqlx::FromRow, Clone, Debug, PartialEq, crate::Row, serde::Serialize, serde::Deserialize,
    )]
    #[row(table = "nested", mirror = "catalog")]
    struct Nested {
        id: Uuid,
        #[sqlx(flatten)]
        part: Label,
    }

    /// A field serde writes under another name.
    #[derive(
        sqlx::FromRow, Clone, Debug, PartialEq, crate::Row, serde::Serialize, serde::Deserialize,
    )]
    #[row(table = "renamed", mirror = "catalog")]
    struct Renamed {
        id: Uuid,
        #[serde(rename = "name")]
        title: String,
    }

    fn encoded<R: Row + serde::Serialize>(row: &R) -> Result<Vec<u8>, Error> {
        json_encode(R::TABLE, &Table::of::<R>().columns, row)
    }

    #[test]
    fn a_part_flattened_for_serde_too_travels_as_json() {
        let row = Flat {
            id: Uuid::nil(),
            part: Label { label: "x".into() },
        };
        let bytes = encoded(&row).unwrap();
        let columns = Table::of::<Flat>().columns;
        assert_eq!(json_decode::<Flat>("flat", &columns, &bytes).unwrap(), row);
    }

    #[test]
    fn a_field_serde_writes_otherwise_than_its_name_does_not_encode() {
        let renamed = Renamed {
            id: Uuid::nil(),
            title: "x".into(),
        };
        let nested = Nested {
            id: Uuid::nil(),
            part: Label { label: "x".into() },
        };
        for error in [
            encoded(&renamed).unwrap_err(),
            encoded(&nested).unwrap_err(),
        ] {
            assert!(
                matches!(&error, Error::Misuse(why) if why.contains("by its name")),
                "{error}"
            );
        }
    }
}
