#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "record", conflict = "id, at")]
pub struct Thing {
    pub id: uuid::Uuid,
    pub at: Option<chrono::DateTime<chrono::Utc>>,
}

fn main() {}
