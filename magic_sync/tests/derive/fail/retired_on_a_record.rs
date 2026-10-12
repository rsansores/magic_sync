#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "record", retired = "retired_at")]
pub struct Thing {
    pub id: uuid::Uuid,
}

fn main() {}
