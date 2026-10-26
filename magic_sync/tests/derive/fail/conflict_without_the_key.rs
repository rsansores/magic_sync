#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "record", conflict = "at")]
pub struct Thing {
    pub id: uuid::Uuid,
    pub at: chrono::DateTime<chrono::Utc>,
}

fn main() {}
