//! A host's newtypes are keys and versions once they say how they read.

use magic_sync::row::{KeyKind, KeyPart, KeyValue, Version, VersionKind};

#[derive(Clone, Copy, sqlx::Type)]
#[sqlx(transparent)]
pub struct RegionId(uuid::Uuid);

impl KeyValue for RegionId {
    const KIND: KeyKind = KeyKind::Uuid;

    fn key_part(&self) -> KeyPart {
        self.0.key_part()
    }
}

#[derive(Clone, Copy, sqlx::Type)]
#[sqlx(transparent)]
pub struct Revision(i64);

impl Version for Revision {
    const KIND: VersionKind = VersionKind::Integer;
}

#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "regions", mirror = "catalog", sync = "broadcast")]
pub struct Region {
    pub id: RegionId,
    pub name: String,
    #[row(version)]
    pub revision: Revision,
}

fn main() {}
