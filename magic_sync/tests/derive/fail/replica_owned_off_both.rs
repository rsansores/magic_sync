#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog")]
pub struct Thing {
    pub id: uuid::Uuid,
    #[row(replica_owned)]
    pub done: bool,
}

fn main() {}
