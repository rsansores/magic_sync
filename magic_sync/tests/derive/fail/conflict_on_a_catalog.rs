#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog", sync = "broadcast", conflict = "id, at")]
pub struct Thing {
    pub id: uuid::Uuid,
    pub at: chrono::DateTime<chrono::Utc>,
}

fn main() {}
