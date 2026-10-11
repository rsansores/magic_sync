//! A mirrored table on the wire: its rows encoded as the table's own
//! message, and applied on the other side by the rules its row declares.
//!
//! A [`Codec`] erases the row type, so the engine holds every table in one
//! list — the host's, in the order the tables are sent, parents first — and
//! dispatches on the table name. Nothing here names a table or a column.

use crate::row::{self, Key, KeyPart, Mirror, Row, Table};
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
    /// row a replica uploads. A mapping that fails is the row's: a row that
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
    /// encode. `place` stamps a row a replica uploads.
    pub fn json<R>(place: Option<Place<R, P>>) -> Self
    where
        R: Row + Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned,
    {
        let columns = Table::of::<R>().columns;
        let columns_back = columns.clone();
        Self::typed(
            Box::new(move |row: R| json_encode(R::TABLE, &columns, &row)),
            Box::new(move |bytes: &[u8]| json_decode(R::TABLE, &columns_back, bytes)),
            place,
        )
    }

    /// The codec of `R`, encoded and decoded so. A server-only row has none,
    /// and does not build.
    fn typed<R>(encode: Encode<R>, decode: Decode<R>, place: Option<Place<R, P>>) -> Self
    where
        R: Row + Clone + Send + Sync,
    {
        const {
            assert!(
                !matches!(R::MIRROR, Mirror::ServerOnly),
                "a server-only row does not travel: it has no codec"
            );
        }
        // A replica files nothing: placing a row is the server's.
        #[cfg(feature = "sqlite")]
        let place = {
            let _ = place;
            std::marker::PhantomData
        };
        let table = Table::of::<R>();
        Self {
            ops: Box::new(Typed {
                owned: table.replica_owned(),
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

    /// The rows of `keys`, retired or not, encoded; a key with no row is
    /// left out, which is how its absence is seen. A row that does not encode
    /// is answered apart, so one row cannot keep its table from travelling.
    pub(crate) fn load<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, Encoded> {
        self.ops.load(conn, keys)
    }

    /// A replica writing what the server sent: every column the replica does
    /// not own, unless it holds a newer version — and a row it had retired is
    /// offered again. A record is refused. Answers the keys the replica now
    /// holds, and the rows it could not take — that do not decode, or that the
    /// database refuses for their data — by their place in `rows`, with why.
    #[cfg(feature = "sqlite")]
    pub(crate) fn apply_down<'a>(
        &'a self,
        conn: &'a mut Conn,
        rows: Vec<Vec<u8>>,
    ) -> Fut<'a, Taken> {
        self.ops.apply_down(conn, rows)
    }

    /// The server writing what a replica sent: a record first-delivery-wins
    /// (or newest-wins when it carries a version), a two-way table only the
    /// columns the replica owns; a catalog is refused. `sender` is the replica
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

    /// A replica letting rows go: deleted, or retired where a row of its own
    /// still points at them, or refused one by one ([`row::retire_by_keys`]).
    #[cfg(feature = "sqlite")]
    pub(crate) fn retire<'a>(
        &'a self,
        conn: &'a mut Conn,
        keys: &'a [Key],
    ) -> Fut<'a, row::Retired> {
        self.ops.retire(conn, keys)
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

    /// Whether the server sends this table's rows down.
    pub(crate) fn sent_down(&self) -> bool {
        matches!(self.table.mirror, Mirror::Catalog | Mirror::Both)
    }

    /// Whether a replica's writes to this table go up.
    #[cfg(feature = "sqlite")]
    pub(crate) fn goes_up(&self) -> bool {
        matches!(self.table.mirror, Mirror::Record | Mirror::Both)
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
        if !matches!(self.table.mirror, Mirror::Record | Mirror::Both) {
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

/// What [`Codec::apply_down`] or [`Codec::apply_up`] did with a message's
/// rows.
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
    /// Its key, read off the row: `None` when the row does not decode.
    #[cfg(feature = "sqlite")]
    pub(crate) key: Option<Key>,
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
    #[cfg(feature = "sqlite")]
    fn apply_down<'a>(&'a self, conn: &'a mut Conn, rows: Vec<Vec<u8>>) -> Fut<'a, Taken>;
    #[cfg(feature = "postgres")]
    fn apply_up<'a>(
        &'a self,
        conn: &'a mut Conn,
        rows: Vec<Vec<u8>>,
        sender: Uuid,
        known: &'a P,
    ) -> Fut<'a, Taken>;
    #[cfg(feature = "sqlite")]
    fn retire<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, row::Retired>;
    #[cfg(feature = "postgres")]
    fn delete<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, u64>;
    #[cfg(feature = "sqlite")]
    fn keys<'a>(&'a self, conn: &'a mut Conn) -> Fut<'a, Vec<Key>>;
    #[cfg(feature = "sqlite")]
    fn live_keys<'a>(&'a self, conn: &'a mut Conn) -> Fut<'a, Vec<Key>>;
}

/// How a side writes rows of the other's.
#[derive(Clone, Copy)]
enum How {
    /// A replica taking the server's: every column but its own, newest wins.
    #[cfg(feature = "sqlite")]
    AllButOwned,
    /// The server taking a two-way table's: only the columns the replica owns.
    #[cfg(feature = "postgres")]
    OnlyOwned,
    /// The server taking state a replica reports: newest wins.
    #[cfg(feature = "postgres")]
    Newest,
    /// The server taking history a replica records: first delivery wins.
    #[cfg(feature = "postgres")]
    First,
}

type Encode<R> = Box<dyn Fn(R) -> Result<Vec<u8>, Error> + Send + Sync>;
type Decode<R> = Box<dyn Fn(&[u8]) -> Result<R, Error> + Send + Sync>;

struct Typed<R, P> {
    /// The columns the replicas own.
    owned: Vec<&'static str>,
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
fn json_decode<R: serde::de::DeserializeOwned>(
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
                        #[cfg(feature = "sqlite")]
                        key: None,
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
            How::AllButOwned => row::overwrite_except(conn, rows, &self.owned).await,
            #[cfg(feature = "postgres")]
            How::OnlyOwned => row::update_only(conn, rows, &self.owned).await,
            #[cfg(feature = "postgres")]
            How::Newest => row::upsert(conn, rows).await,
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
            #[cfg(feature = "sqlite")]
            let key = one.key();
            match self.attempt(conn, vec![one], how).await? {
                Ok(keys) => written.extend(keys),
                Err(e) => refused.push(Refused {
                    at,
                    #[cfg(feature = "sqlite")]
                    key: Some(key),
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

    #[cfg(feature = "sqlite")]
    fn apply_down<'a>(&'a self, conn: &'a mut Conn, rows: Vec<Vec<u8>>) -> Fut<'a, Taken> {
        Box::pin(async move {
            if !matches!(R::MIRROR, Mirror::Catalog | Mirror::Both) {
                return Err(Error::Misuse(format!(
                    "`{}` is not sent down: a replica takes none of it",
                    R::TABLE
                )));
            }
            let (rows, mut rejected) = self.decode(rows);
            let offered: Vec<(usize, Key)> =
                rows.iter().map(|(at, row)| (*at, row.key())).collect();
            let (_, refused) = self.guarded(conn, rows, How::AllButOwned).await?;
            // Held, whether written now or already newer here.
            let held: Vec<Key> = offered
                .into_iter()
                .filter(|(at, _)| !refused.iter().any(|r| r.at == *at))
                .map(|(_, key)| key)
                .collect();
            row::unretire_by_keys::<R>(conn, &held).await?;
            rejected.extend(refused);
            Ok(Taken { held, rejected })
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
                Mirror::Both => How::OnlyOwned,
                Mirror::Record if R::VERSION.is_some() => How::Newest,
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
            let (written, refused) = self.guarded(conn, rows, how).await?;
            rejected.extend(refused);
            Ok(Taken {
                held: written,
                rejected,
            })
        })
    }

    #[cfg(feature = "sqlite")]
    fn retire<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, row::Retired> {
        Box::pin(row::retire_by_keys::<R>(conn, keys))
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
}
