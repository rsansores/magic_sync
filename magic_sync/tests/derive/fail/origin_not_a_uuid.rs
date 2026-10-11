#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "record", key = "replica, id", origin = "replica")]
pub struct Thing {
    pub replica: i64,
    pub id: uuid::Uuid,
}

fn main() {}
