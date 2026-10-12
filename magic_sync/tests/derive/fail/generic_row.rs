#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog")]
pub struct Thing<T> {
    pub id: uuid::Uuid,
    pub value: T,
}

fn main() {}
