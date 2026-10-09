//! A table for the engine's own tests: one row type, its message, and a codec
//! whose encoding has a bug that refuses one value, on either backend.

use crate::row::{Column, Columns, Key, KeyPart, Mirror, Row, Sync, Wire};
use crate::Db;
use magic_map::MappingError;
use sqlx::query_builder::Separated;
use std::cell::Cell;
use uuid::Uuid;

#[derive(sqlx::FromRow, Clone, Debug)]
pub struct Item {
    pub id: Uuid,
    pub partition_id: Uuid,
    pub name: String,
}

impl Columns for Item {
    fn columns(out: &mut Vec<Column>) {
        for (name, wire) in [
            ("id", Wire::String),
            ("partition_id", Wire::String),
            ("name", Wire::String),
        ] {
            out.push(Column {
                name,
                field: name,
                nullable: false,
                generated: false,
                replica_owned: false,
                wire,
                compatible: |_| true,
            });
        }
    }

    fn push_binds<'args>(self, sep: &mut Separated<'_, 'args, Db, &'static str>) {
        sep.push_bind(self.id);
        sep.push_bind(self.partition_id);
        sep.push_bind(self.name);
    }
}

impl Row for Item {
    const TABLE: &'static str = "items";
    const KEY: &'static [&'static str] = &["id"];
    const SCHEMA: &'static str = "fixture";
    const QUALIFIED: &'static str = crate::qualified!("fixture", "items");
    const MIRROR: Mirror = Mirror::Catalog;
    const SYNC: Sync = Sync::Broadcast;
    const ACTIVE: Option<&'static str> = Some("items");
    const RETIRED: Option<&'static str> = Some("retired_at");

    fn key_repr(&self) -> String {
        self.id.to_string()
    }

    fn key(&self) -> Key {
        vec![KeyPart::Uuid(self.id)]
    }
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
    #[derive(sqlx::FromRow, Clone, Debug)]
    pub struct Reading {
        pub id: Uuid,
        pub replica_id: Uuid,
        pub value: String,
    }

    impl Columns for Reading {
        fn columns(out: &mut Vec<Column>) {
            for (name, wire) in [
                ("id", Wire::String),
                ("replica_id", Wire::String),
                ("value", Wire::String),
            ] {
                out.push(Column {
                    name,
                    field: name,
                    nullable: false,
                    generated: false,
                    replica_owned: false,
                    wire,
                    compatible: |_| true,
                });
            }
        }

        fn push_binds<'args>(self, sep: &mut Separated<'_, 'args, Db, &'static str>) {
            sep.push_bind(self.id);
            sep.push_bind(self.replica_id);
            sep.push_bind(self.value);
        }
    }

    impl Row for Reading {
        const TABLE: &'static str = "readings";
        const KEY: &'static [&'static str] = &["id"];
        const SCHEMA: &'static str = "fixture";
        const QUALIFIED: &'static str = crate::qualified!("fixture", "readings");
        const MIRROR: Mirror = Mirror::Record;
        const SYNC: Sync = Sync::Referenced;

        fn key_repr(&self) -> String {
            self.id.to_string()
        }

        fn key(&self) -> Key {
            vec![KeyPart::Uuid(self.id)]
        }
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
