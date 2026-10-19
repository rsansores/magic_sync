#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog", active = "live_things", from = "things_seen")]
pub struct Thing {
    pub id: uuid::Uuid,
}

fn main() {}
