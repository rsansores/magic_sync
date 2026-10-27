//! A record filed into a table partitioned by when it was taken.

use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(
    table = "readings",
    schema = "fleet",
    mirror = "record",
    key = "replica_id, id",
    conflict = "replica_id, id, taken_at",
    origin = "replica_id"
)]
pub struct Reading {
    pub replica_id: Uuid,
    pub id: Uuid,
    pub taken_at: DateTime<Utc>,
    pub value: f64,
    #[row(version)]
    pub revision: i64,
}

fn main() {}
