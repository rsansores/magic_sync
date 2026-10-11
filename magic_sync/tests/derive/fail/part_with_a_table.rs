#[derive(sqlx::FromRow, Clone, magic_sync::Columns)]
#[row(table = "stamps")]
pub struct Stamps {
    pub at: i64,
}

fn main() {}
