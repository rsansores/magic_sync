#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog")]
pub struct Thing {
    pub id: uuid::Uuid,
    #[sqlx(skip, rename = "cached")]
    pub at: i64,
}

fn main() {}
