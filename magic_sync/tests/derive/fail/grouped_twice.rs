#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog", sync = "grouped(members = a, members = b, assigned = c)")]
pub struct Thing {
    pub id: uuid::Uuid,
}

fn main() {}
