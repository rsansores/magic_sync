#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog", sync = "broadcast(id)")]
pub struct Thing {
    pub id: uuid::Uuid,
}

fn main() {}
