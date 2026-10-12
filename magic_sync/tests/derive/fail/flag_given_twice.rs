#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog")]
pub struct Thing {
    pub id: uuid::Uuid,
    #[row(version)]
    #[row(version)]
    pub at: i64,
}

fn main() {}
