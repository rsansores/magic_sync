#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog", from = "a.b.c")]
pub struct Thing {
    pub id: uuid::Uuid,
}

fn main() {}
