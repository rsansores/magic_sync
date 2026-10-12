#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(mirror = "catalog")]
pub struct Thing {
    pub id: uuid::Uuid,
}

fn main() {}
