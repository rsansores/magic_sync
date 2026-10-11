#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "both")]
pub struct Thing {
    pub id: uuid::Uuid,
    #[row(version, replica_owned)]
    pub at: i64,
}

fn main() {}
