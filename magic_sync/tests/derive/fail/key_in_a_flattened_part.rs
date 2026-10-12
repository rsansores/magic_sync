#[derive(sqlx::FromRow, Clone, magic_sync::Columns)]
pub struct Ids {
    pub id: uuid::Uuid,
}

#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog")]
pub struct Thing {
    #[sqlx(flatten)]
    pub ids: Ids,
}

fn main() {}
