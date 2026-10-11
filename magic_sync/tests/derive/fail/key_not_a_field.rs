#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog", key = "id, slot")]
pub struct Thing {
    pub id: uuid::Uuid,
}

fn main() {}
