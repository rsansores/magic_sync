#[derive(sqlx::FromRow, Clone, magic_sync::Columns)]
pub struct Stamps {
    #[row(version)]
    pub at: i64,
}

#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "record")]
pub struct Thing {
    pub id: uuid::Uuid,
    #[row(version)]
    pub revision: i64,
    #[sqlx(flatten)]
    pub stamps: Stamps,
}

fn main() {}
