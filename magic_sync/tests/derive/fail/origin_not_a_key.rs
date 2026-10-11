#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "record", origin = "replica_id")]
pub struct Thing {
    pub id: uuid::Uuid,
    pub replica_id: uuid::Uuid,
}

fn main() {}
