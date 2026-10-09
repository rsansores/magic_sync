//! A mirrored table on the wire: its rows encoded as the table's message, and
//! applied on the other side by the rules its row declares.
//!
//! [`Codec`] erases the row type so sync can hold every table in one list —
//! the list `wire_gen` generates, in the order the tables are sent — and
//! dispatch on the table name. Nothing here names a table or a column.

use crate::row::{self, Conn, Key, KeyPart, Mirror, Row, Table};
use crate::Error;
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;

type Fut<'a, T> = Pin<Box<dyn Future<Output = Result<T, Error>> + Send + 'a>>;

/// Files a row a replica sent under that replica, whatever the row said: the
/// replica's id and what the server knows of it (`P`, the host's).
pub type Place<R, P> = fn(&mut R, Uuid, &P);

/// One mirrored table, encoded and applied without its row type. `P` is what
/// the server knows of a replica that uploads, handed to [`Place`].
pub struct Codec<P> {
    pub table: Table,
    ops: Box<dyn Ops<P>>,
}

impl<P: Sync + 'static> Codec<P> {
    /// `to` and `from` are the mappers between the row and its message.
    /// `place` stamps a row a replica uploads.
    pub fn new<R, M>(
        to: fn(R) -> Result<M, magic_map::MappingError>,
        from: fn(M) -> Result<R, magic_map::MappingError>,
        place: Option<Place<R, P>>,
    ) -> Self
    where
        R: Row + Clone + Send + Sync,
        M: prost::Message + Default + 'static,
    {
        Self {
            table: Table::of::<R>(),
            ops: Box::new(Typed {
                encode: Box::new(move |row: R| {
                    to(row)
                        .map(|m| m.encode_to_vec())
                        .map_err(|e| mapping(R::TABLE, e))
                }),
                decode: Box::new(move |bytes: &[u8]| {
                    let message = M::decode(bytes)
                        .map_err(|e| Error::Misuse(format!("`{}` on the wire: {e}", R::TABLE)))?;
                    from(message).map_err(|e| mapping(R::TABLE, e))
                }),
                place,
            }),
        }
    }

    /// A table that travels as JSON: its row type's own serde shape, with no
    /// message of its own to declare. For a replica in another language, a
    /// row is a JSON object of its columns.
    pub fn json<R>(place: Option<Place<R, P>>) -> Self
    where
        R: Row + Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned,
    {
        Self {
            table: Table::of::<R>(),
            ops: Box::new(Typed {
                encode: Box::new(|row: R| {
                    serde_json::to_vec(&row)
                        .map_err(|e| Error::Misuse(format!("`{}` as JSON: {e}", R::TABLE)))
                }),
                decode: Box::new(|bytes: &[u8]| {
                    serde_json::from_slice(bytes)
                        .map_err(|e| Error::Misuse(format!("`{}` as JSON: {e}", R::TABLE)))
                }),
                place,
            }),
        }
    }

    /// A key of this table from its parts' text, as its key columns type them.
    pub fn key(&self, parts: &[String]) -> Result<Key, Error> {
        if parts.len() != self.table.key.len() {
            return Err(Error::Misuse(format!(
                "`{}` is keyed by {:?}, not {} parts",
                self.table.name,
                self.table.key,
                parts.len()
            )));
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
                crate::row::KeyPart::parse(column, part)
            })
            .collect()
    }

    /// The rows of `keys`, retired or not, encoded; a key with no row is
    /// left out, which is how its absence is seen. A row that does not encode
    /// is answered apart, so one row cannot keep its table from travelling.
    pub fn load<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, Encoded> {
        self.ops.load(conn, keys)
    }

    /// A replica writing what the server sent: every column the replica does
    /// not own, unless it holds a newer version — and a row it had retired is
    /// offered again. A record is refused. Answers the keys the replica now
    /// holds, and the rows it could not take — that do not decode, or that the
    /// database refuses for their data — by their place in `rows`, with why.
    pub fn apply_down<'a>(&'a self, conn: &'a mut Conn, rows: Vec<Vec<u8>>) -> Fut<'a, Taken> {
        self.ops.apply_down(conn, rows)
    }

    /// The server writing what a replica sent: a record first-delivery-wins
    /// (or newest-wins when it carries a version), a two-way table only the
    /// columns the replica owns; a catalog is refused. `sender` is the replica
    /// the message came from; `known` is the server's knowledge of it. Answers
    /// the keys written, and the rows it could not take — that do not
    /// decode, or that the database refuses for their data — by their place in
    /// `rows`, with why.
    pub fn apply_up<'a>(
        &'a self,
        conn: &'a mut Conn,
        rows: Vec<Vec<u8>>,
        sender: Uuid,
        known: &'a P,
    ) -> Fut<'a, Taken> {
        self.ops.apply_up(conn, rows, sender, known)
    }

    /// A replica letting rows go: deleted, or retired where a row of its own
    /// still points at them.
    pub fn retire<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, row::Retired> {
        self.ops.retire(conn, keys)
    }

    pub fn keys<'a>(&'a self, conn: &'a mut Conn) -> Fut<'a, Vec<Key>> {
        self.ops.keys(conn)
    }

    /// Every key this side holds that is not retired.
    pub fn live_keys<'a>(&'a self, conn: &'a mut Conn) -> Fut<'a, Vec<Key>> {
        self.ops.live_keys(conn)
    }

    /// Where the replica sits in this table's key: only a table keyed by it
    /// takes a replica's deletes, each replica's rows its own.
    fn origin_part(&self) -> Option<usize> {
        let origin = self.table.origin?;
        self.table.key.iter().position(|k| *k == origin)
    }

    /// Whether a replica's deletes reach the server: history a replica lets
    /// go of before it went up is simply not sent.
    pub fn takes_deletes(&self) -> bool {
        self.origin_part().is_some()
    }

    /// State a replica reports rather than history it records: a record keyed
    /// by its replica, the newest write winning.
    pub fn reports_state(&self) -> bool {
        self.table.mirror == Mirror::Record
            && self.origin_part().is_some()
            && self.table.version.is_some()
    }

    /// The server letting go of rows a replica deleted — of `sender`'s rows
    /// only, whatever the keys name. Answers the keys it let go of.
    pub async fn delete_up(
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

    /// Everything `replica` reported, when another takes its place and
    /// reports its own.
    pub async fn forget(&self, conn: &mut Conn, replica: Uuid) -> Result<u64, Error> {
        if !self.reports_state() {
            return Ok(0);
        }
        self.ops.forget(conn, replica).await
    }
}

/// Things by their place in a message's rows.
type Placed<T> = Vec<(usize, T)>;

/// What [`Codec::apply_down`] did with a message's rows.
#[derive(Debug, Default)]
pub struct Taken {
    pub held: Vec<Key>,
    /// The rows it could not take, by their place in the message, with why.
    pub rejected: Placed<String>,
}

/// Rows as [`Codec::load`] read them: encoded, and the ones that did not
/// encode, with why.
#[derive(Debug, Default)]
pub struct Encoded {
    pub rows: Vec<(Key, Vec<u8>)>,
    pub failed: Vec<(Key, Error)>,
}

trait Ops<P>: Send + Sync {
    fn load<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, Encoded>;
    fn apply_down<'a>(&'a self, conn: &'a mut Conn, rows: Vec<Vec<u8>>) -> Fut<'a, Taken>;
    fn apply_up<'a>(
        &'a self,
        conn: &'a mut Conn,
        rows: Vec<Vec<u8>>,
        sender: Uuid,
        known: &'a P,
    ) -> Fut<'a, Taken>;
    fn retire<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, row::Retired>;
    fn delete<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, u64>;
    fn forget<'a>(&'a self, conn: &'a mut Conn, replica: Uuid) -> Fut<'a, u64>;
    fn keys<'a>(&'a self, conn: &'a mut Conn) -> Fut<'a, Vec<Key>>;
    fn live_keys<'a>(&'a self, conn: &'a mut Conn) -> Fut<'a, Vec<Key>>;
}

/// How a side writes rows of the other's.
#[derive(Clone, Copy)]
enum How {
    /// A replica taking the server's: every column but its own, newest wins.
    Down,
    /// The server taking a two-way table's: only the columns the replica owns.
    UpOwned,
    /// The server taking state a replica reports: newest wins.
    UpNewest,
    /// The server taking history a replica records: first delivery wins.
    UpFirst,
}

type Encode<R> = Box<dyn Fn(R) -> Result<Vec<u8>, Error> + Send + Sync>;
type Decode<R> = Box<dyn Fn(&[u8]) -> Result<R, Error> + Send + Sync>;

struct Typed<R, P> {
    encode: Encode<R>,
    decode: Decode<R>,
    place: Option<Place<R, P>>,
}

fn mapping(table: &str, e: magic_map::MappingError) -> Error {
    Error::Misuse(format!("`{table}` on the wire: {e}"))
}

impl<R, P> Typed<R, P>
where
    R: Row + Clone + Send + Sync,
{
    /// Every row that decodes, by its place in `rows`; one that does not is
    /// answered apart, so it cannot take the rest of its table down with it.
    fn decode(&self, rows: Vec<Vec<u8>>) -> (Placed<R>, Placed<String>) {
        let mut decoded = Vec::new();
        let mut failed = Vec::new();
        for (at, bytes) in rows.into_iter().enumerate() {
            let row = (self.decode)(bytes.as_slice());
            match row {
                Ok(row) => decoded.push((at, row)),
                Err(e) => {
                    tracing::error!(
                        table = R::TABLE,
                        "a row that does not decode — skipped: {e}"
                    );
                    failed.push((at, e.to_string()));
                }
            }
        }
        (decoded, failed)
    }

    async fn write(conn: &mut Conn, rows: Vec<R>, how: How) -> Result<Vec<Key>, Error> {
        match how {
            How::Down => row::overwrite_except(conn, rows, &Self::owned(true)).await,
            How::UpOwned => row::update_only(conn, rows, &Self::owned(true)).await,
            How::UpNewest => row::upsert_keys(conn, rows).await,
            How::UpFirst => row::insert_ignore_keys(conn, rows).await,
        }
    }

    /// Writes `rows`, and when the database refuses them for their data (a
    /// duplicate, a dangling reference) writes them one by one, leaving out
    /// and logging each it refuses — a message must not be refused forever
    /// for one row. Answers the keys written, and the places of the rows
    /// refused with why.
    async fn guarded(
        conn: &mut Conn,
        rows: Placed<R>,
        how: How,
    ) -> Result<(Vec<Key>, Placed<String>), Error> {
        if rows.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let all: Vec<R> = rows.iter().map(|(_, row)| row.clone()).collect();
        let all = match Self::attempt(conn, all, how).await? {
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
            let key = one.key();
            match Self::attempt(conn, vec![one], how).await? {
                Ok(keys) => written.extend(keys),
                Err(e) => {
                    tracing::error!(
                        table = R::TABLE,
                        ?key,
                        "a row the database refuses — skipped: {e}"
                    );
                    refused.push((at, e.to_string()));
                }
            }
        }
        Ok((written, refused))
    }

    /// One write in a savepoint: a constraint refusal comes back as the inner
    /// error with the transaction still usable; any other failure is the
    /// outer one.
    async fn attempt(
        conn: &mut Conn,
        rows: Vec<R>,
        how: How,
    ) -> Result<Result<Vec<Key>, Error>, Error> {
        sqlx::query("SAVEPOINT wire_rows")
            .execute(&mut *conn)
            .await?;
        let result = Self::write(conn, rows, how).await;
        if result.is_err() {
            sqlx::query("ROLLBACK TO SAVEPOINT wire_rows")
                .execute(&mut *conn)
                .await?;
        }
        sqlx::query("RELEASE SAVEPOINT wire_rows")
            .execute(&mut *conn)
            .await?;
        match result {
            Err(e) if row::is_constraint_violation(&e) => Ok(Err(e)),
            other => other.map(Ok),
        }
    }

    fn owned(replica: bool) -> Vec<&'static str> {
        Table::of::<R>()
            .columns
            .iter()
            .filter(|c| c.replica_owned == replica)
            .map(|c| c.name)
            .collect()
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
            let (_, refused) = Self::guarded(conn, rows, How::Down).await?;
            // Held, whether written now or already newer here.
            let held: Vec<Key> = offered
                .into_iter()
                .filter(|(at, _)| !refused.iter().any(|(r, _)| r == at))
                .map(|(_, key)| key)
                .collect();
            row::unretire_by_keys::<R>(conn, &held).await?;
            rejected.extend(refused);
            Ok(Taken { held, rejected })
        })
    }

    fn apply_up<'a>(
        &'a self,
        conn: &'a mut Conn,
        rows: Vec<Vec<u8>>,
        sender: Uuid,
        known: &'a P,
    ) -> Fut<'a, Taken> {
        Box::pin(async move {
            let how = match R::MIRROR {
                Mirror::Both => How::UpOwned,
                Mirror::Record if row::version::<R>().is_some() => How::UpNewest,
                Mirror::Record => How::UpFirst,
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
            let (written, refused) = Self::guarded(conn, rows, how).await?;
            rejected.extend(refused);
            Ok(Taken {
                held: written,
                rejected,
            })
        })
    }

    fn retire<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, row::Retired> {
        Box::pin(row::retire_by_keys::<R>(conn, keys))
    }

    fn delete<'a>(&'a self, conn: &'a mut Conn, keys: &'a [Key]) -> Fut<'a, u64> {
        Box::pin(row::delete_by_keys::<R>(conn, keys))
    }

    fn forget<'a>(&'a self, conn: &'a mut Conn, replica: Uuid) -> Fut<'a, u64> {
        Box::pin(async move {
            let Some(origin) = R::ORIGIN else {
                return Ok(0);
            };
            let sql = format!("DELETE FROM {} WHERE {origin} = $1", R::QUALIFIED);
            let done = sqlx::query(&sql).bind(replica).execute(&mut *conn).await?;
            Ok(done.rows_affected())
        })
    }

    fn keys<'a>(&'a self, conn: &'a mut Conn) -> Fut<'a, Vec<Key>> {
        Box::pin(row::keys::<R>(conn))
    }

    fn live_keys<'a>(&'a self, conn: &'a mut Conn) -> Fut<'a, Vec<Key>> {
        Box::pin(row::live_keys::<R>(conn))
    }
}
