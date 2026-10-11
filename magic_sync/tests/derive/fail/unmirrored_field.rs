#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog", unmirrored = "label")]
pub struct Thing {
    pub id: uuid::Uuid,
    pub label: String,
}

fn main() {}
