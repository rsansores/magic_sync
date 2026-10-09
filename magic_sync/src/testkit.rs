//! A table for the engine's own tests: one row type, its message, and a codec
//! whose encoding has a bug that refuses one value, on either backend.

use magic_map::MappingError;
use std::cell::Cell;
use uuid::Uuid;

#[derive(sqlx::FromRow, Clone, Debug, crate::Row)]
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

pub fn to_wire(item: Item) -> Result<ItemRow, MappingError> {
    if item.name == "poison" && BUGGY.get() {
        return Err(MappingError::Custom(
            "a value this build cannot encode".into(),
        ));
    }
    Ok(ItemRow {
        id: item.id.to_string(),
        partition_id: item.partition_id.to_string(),
        name: item.name,
    })
}

pub fn from_wire(row: ItemRow) -> Result<Item, MappingError> {
    let uuid =
        |field, text: &str| Uuid::parse_str(text).map_err(|_| MappingError::InvalidUuid { field });
    Ok(Item {
        id: uuid("id", &row.id)?,
        partition_id: uuid("partition_id", &row.partition_id)?,
        name: row.name,
    })
}

/// A deterministic walk: the same seed replays the same failure.
#[cfg(all(feature = "postgres", not(feature = "sqlite")))]
pub struct Walk(pub u64);

#[cfg(all(feature = "postgres", not(feature = "sqlite")))]
impl Walk {
    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    pub fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    pub fn pick(&mut self, from: &[Uuid]) -> Option<Uuid> {
        (!from.is_empty()).then(|| from[self.below(from.len())])
    }
}

/// A record: written on a replica, filed on the server.
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

    pub fn reading_to_wire(reading: Reading) -> Result<ReadingRow, MappingError> {
        Ok(ReadingRow {
            id: reading.id.to_string(),
            replica_id: reading.replica_id.to_string(),
            value: reading.value,
        })
    }

    /// The server's decoding has the bug that refuses "poison", while `BUGGY`.
    pub fn reading_from_wire(row: ReadingRow) -> Result<Reading, MappingError> {
        if row.value == "poison" && BUGGY.get() {
            return Err(MappingError::Custom(
                "a value this build cannot decode".into(),
            ));
        }
        let uuid = |field, text: &str| {
            Uuid::parse_str(text).map_err(|_| MappingError::InvalidUuid { field })
        };
        Ok(Reading {
            id: uuid("id", &row.id)?,
            replica_id: uuid("replica_id", &row.replica_id)?,
            value: row.value,
        })
    }
}
