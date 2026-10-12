#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "both", sync = "broadcast")]
pub struct Thing {
    pub id: uuid::Uuid,
    #[row(replica_owned)]
    pub done: bool,
    #[row(version)]
    pub at: i64,
}

fn main() {}
