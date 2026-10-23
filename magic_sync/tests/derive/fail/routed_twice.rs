#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog", sync = "routed(view = a, row = id, row = id, replica = r)")]
pub struct Thing {
    pub id: uuid::Uuid,
}

fn main() {}
