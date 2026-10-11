#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog")]
pub struct Thing {
    pub id: uuid::Uuid,
    #[row(version)]
    pub a: i64,
    #[row(version)]
    pub b: i64,
}

fn main() {}
