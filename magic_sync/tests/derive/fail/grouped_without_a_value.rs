#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog", sync = "grouped(members, assigned = b)")]
pub struct Thing {
    pub id: uuid::Uuid,
}

fn main() {}
