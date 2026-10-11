//! A table for the engine's own tests: one row type, its message, and a codec
//! whose encoding has a bug that refuses one value, on either backend.

use crate::wire::MapError;
use std::cell::Cell;
use uuid::Uuid;

#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
#[row(
    table = "items",
    schema = "fixture",
    mirror = "catalog",
    sync = "broadcast",
    active = "items",
    retired = "retired_at"
)]
pub struct Item {
    pub id: Uuid,
    pub partition_id: Uuid,
    pub name: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ItemRow {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(string, tag = "2")]
    pub partition_id: String,
    #[prost(string, tag = "3")]
    pub name: String,
}

thread_local! {
    /// Whether this build's encoding has the bug that refuses "poison".
    pub static BUGGY: Cell<bool> = const { Cell::new(true) };
}

pub fn to_wire(item: Item) -> Result<ItemRow, MapError> {
    if item.name == "poison" && BUGGY.get() {
        return Err(MapError::new("a value this build cannot encode"));
    }
    Ok(ItemRow {
        id: item.id.to_string(),
        partition_id: item.partition_id.to_string(),
        name: item.name,
    })
}

pub fn from_wire(row: ItemRow) -> Result<Item, MapError> {
    Ok(Item {
        id: row.id.parse()?,
        partition_id: row.partition_id.parse()?,
        name: row.name,
    })
}

/// A deterministic walk: the same seed replays the same failure.
#[cfg(feature = "postgres")]
pub struct Walk(pub u64);

#[cfg(feature = "postgres")]
impl Walk {
    pub fn next(&mut self) -> u64 {
        // Xorshift stays at zero from zero: that seed walks from another.
        if self.0 == 0 {
            self.0 = 0x9E37_79B9_7F4A_7C15;
        }
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    pub fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    pub fn pick<T: Copy>(&mut self, from: &[T]) -> Option<T> {
        (!from.is_empty()).then(|| from[self.below(from.len())])
    }

    /// A uuid the seed decides, so a replay writes the same rows.
    pub fn uuid(&mut self) -> Uuid {
        let (high, low) = (self.next(), self.next());
        uuid::Builder::from_random_bytes(((u128::from(high) << 64) | u128::from(low)).to_be_bytes())
            .into_uuid()
    }
}

/// A record: written on a replica, filed on the server.
#[cfg(feature = "postgres")]
pub mod records {
    use super::*;

    /// A record: written on a replica, filed on the server under it.
    #[derive(sqlx::FromRow, Clone, Debug, crate::Row)]
    #[row(table = "readings", schema = "fixture", mirror = "record")]
    pub struct Reading {
        pub id: Uuid,
        pub replica_id: Uuid,
        pub value: String,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ReadingRow {
        #[prost(string, tag = "1")]
        pub id: String,
        #[prost(string, tag = "2")]
        pub replica_id: String,
        #[prost(string, tag = "3")]
        pub value: String,
    }

    pub fn reading_to_wire(reading: Reading) -> Result<ReadingRow, MapError> {
        Ok(ReadingRow {
            id: reading.id.to_string(),
            replica_id: reading.replica_id.to_string(),
            value: reading.value,
        })
    }

    /// The server's decoding has the bug that refuses "poison", while `BUGGY`.
    pub fn reading_from_wire(row: ReadingRow) -> Result<Reading, MapError> {
        if row.value == "poison" && BUGGY.get() {
            return Err(MapError::new("a value this build cannot decode"));
        }
        Ok(Reading {
            id: row.id.parse()?,
            replica_id: row.replica_id.parse()?,
            value: row.value,
        })
    }
}
