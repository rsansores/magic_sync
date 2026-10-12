#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog", sync = "follows(parent)")]
pub struct Thing {
    pub id: uuid::Uuid,
}

fn main() {}
