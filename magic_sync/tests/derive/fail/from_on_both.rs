#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "both", from = "things_seen")]
pub struct Thing {
    pub id: uuid::Uuid,
}

fn main() {}
