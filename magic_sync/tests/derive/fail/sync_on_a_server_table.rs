#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "server", sync = "broadcast")]
pub struct Thing {
    pub id: uuid::Uuid,
}

fn main() {}
