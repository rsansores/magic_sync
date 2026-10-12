#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog")]
pub struct Thing {
    pub id: uuid::Uuid,
    #[row(created)]
    pub a: i64,
    #[row(created)]
    pub b: i64,
}

fn main() {}
