#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "record", depends = "list_id")]
pub struct Thing {
    pub id: uuid::Uuid,
    pub list_id: uuid::Uuid,
}

fn main() {}
