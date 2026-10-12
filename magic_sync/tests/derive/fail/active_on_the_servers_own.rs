#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "server", active = "live_things")]
pub struct Thing {
    pub id: uuid::Uuid,
}

fn main() {}
