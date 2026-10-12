#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog")]
pub struct Thing {
    pub id: uuid::Uuid,
    #[row(created)]
    #[sqlx(skip)]
    pub at: i64,
}

fn main() {}
