#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog", retired = "gone", unmirrored = "gone")]
pub struct Thing {
    pub id: uuid::Uuid,
}

fn main() {}
