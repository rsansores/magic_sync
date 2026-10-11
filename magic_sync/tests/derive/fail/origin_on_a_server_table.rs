#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "server", origin = "id")]
pub struct Thing {
    pub id: uuid::Uuid,
}

fn main() {}
