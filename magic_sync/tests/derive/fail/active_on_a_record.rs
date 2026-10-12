#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "record", active = "live")]
pub struct Thing {
    pub id: uuid::Uuid,
}

fn main() {}
