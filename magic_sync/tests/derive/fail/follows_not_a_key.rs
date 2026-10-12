#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog", sync = "follows(parent)")]
pub struct Thing {
    pub id: uuid::Uuid,
    pub parent: String,
}

fn main() {}
