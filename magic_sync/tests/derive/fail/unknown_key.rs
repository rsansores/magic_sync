#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(tabel = "things", mirror = "catalog")]
pub struct Thing {
    pub id: uuid::Uuid,
}

fn main() {}
