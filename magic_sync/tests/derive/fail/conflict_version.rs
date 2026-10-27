#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "record", conflict = "id, at")]
pub struct Thing {
    pub id: uuid::Uuid,
    #[row(version)]
    pub at: chrono::DateTime<chrono::Utc>,
}

fn main() {}
