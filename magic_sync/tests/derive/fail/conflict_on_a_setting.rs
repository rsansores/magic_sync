#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(
    table = "things",
    mirror = "setting",
    key = "replica_id, id",
    origin = "replica_id",
    conflict = "replica_id, id, at"
)]
pub struct Thing {
    pub replica_id: uuid::Uuid,
    pub id: uuid::Uuid,
    pub at: chrono::DateTime<chrono::Utc>,
    #[row(desired)]
    pub level: i32,
    #[row(version)]
    pub changed_at: chrono::DateTime<chrono::Utc>,
}

fn main() {}
