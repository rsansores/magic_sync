#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog", key = "replica_id, id", origin = "replica_id")]
pub struct Thing {
    pub replica_id: uuid::Uuid,
    pub id: uuid::Uuid,
}

fn main() {}
