#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog", sync = "broadcast")]
pub struct Thing {
    pub id: uuid::Uuid,
    #[row(version)]
    pub at: i64,
}

fn main() {}
