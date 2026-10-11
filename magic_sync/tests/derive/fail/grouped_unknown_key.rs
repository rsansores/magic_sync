#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog", sync = "grouped(members = a, assigned = b, owner = c)")]
pub struct Thing {
    pub id: uuid::Uuid,
}

fn main() {}
