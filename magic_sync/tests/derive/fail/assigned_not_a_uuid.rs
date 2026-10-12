#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog", sync = "assigned(store)")]
pub struct Thing {
    pub id: uuid::Uuid,
    pub store: i64,
}

fn main() {}
