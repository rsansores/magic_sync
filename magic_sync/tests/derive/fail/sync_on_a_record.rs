#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "record", sync = "broadcast")]
pub struct Thing {
    pub id: uuid::Uuid,
}

fn main() {}
