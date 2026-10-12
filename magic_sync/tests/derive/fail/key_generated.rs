#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog")]
pub struct Thing {
    #[row(generated)]
    pub id: i64,
}

fn main() {}
