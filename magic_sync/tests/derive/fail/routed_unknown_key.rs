#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog", sync = "routed(view = a, row = id, replica = r, owner = c)")]
pub struct Thing {
    pub id: uuid::Uuid,
}

fn main() {}
