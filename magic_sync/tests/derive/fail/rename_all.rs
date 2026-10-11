#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[sqlx(rename_all = "camelCase")]
#[row(table = "things", mirror = "catalog")]
pub struct Thing {
    pub id: uuid::Uuid,
}

fn main() {}
