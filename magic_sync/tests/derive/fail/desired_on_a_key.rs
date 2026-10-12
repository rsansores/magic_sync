#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "screens", mirror = "setting", key = "replica_id, id", origin = "replica_id")]
pub struct Screen {
    pub replica_id: uuid::Uuid,
    #[row(desired)]
    pub id: uuid::Uuid,
    #[row(version)]
    pub revision: i64,
}

fn main() {}
