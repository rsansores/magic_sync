#[derive(sqlx::FromRow, Clone, magic_sync::Columns)]
pub struct Level {
    #[row(desired)]
    pub level: i64,
}

fn main() {}
