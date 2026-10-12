#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog")]
pub struct Thing {
    pub id: uuid::Uuid,
    pub label: String,
    #[sqlx(rename = "label")]
    pub title: String,
}

fn main() {}
