#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog", sync = "assigned(a, b)")]
pub struct Thing {
    pub id: uuid::Uuid,
    pub a: uuid::Uuid,
    pub b: uuid::Uuid,
}

fn main() {}
