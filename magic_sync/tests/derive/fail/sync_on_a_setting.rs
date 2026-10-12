#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "screens", mirror = "setting", key = "replica_id, id", origin = "replica_id", sync = "assigned(replica_id)")]
pub struct Screen {
    pub replica_id: uuid::Uuid,
    pub id: uuid::Uuid,
    #[row(desired)]
    pub level: i64,
    #[row(version)]
    pub revision: i64,
}

fn main() {}
