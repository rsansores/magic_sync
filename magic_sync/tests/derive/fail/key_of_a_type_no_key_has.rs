#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog")]
pub struct Thing {
    pub id: String,
}

fn main() {}
