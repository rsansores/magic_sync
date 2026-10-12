#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog", sync = "assigned(store_id")]
pub struct Thing {
    pub id: uuid::Uuid,
    pub store_id: uuid::Uuid,
}

fn main() {}
