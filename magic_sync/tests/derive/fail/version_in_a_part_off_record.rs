#[derive(sqlx::FromRow, Clone, magic_sync::Columns)]
pub struct Stamps {
    #[row(version)]
    pub at: i64,
}

#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog", sync = "broadcast")]
pub struct Thing {
    pub id: uuid::Uuid,
    #[sqlx(flatten)]
    pub stamps: Stamps,
}

fn main() {}
