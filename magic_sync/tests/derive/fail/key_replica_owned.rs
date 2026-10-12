#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "both", key = "id")]
pub struct Thing {
    #[row(replica_owned)]
    pub id: uuid::Uuid,
}

fn main() {}
