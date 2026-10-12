#[derive(sqlx::FromRow, Clone, magic_sync::Columns)]
pub struct Revised {
    #[row(version)]
    pub revision: i64,
}

#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "screens", mirror = "setting", key = "replica_id, id", origin = "replica_id")]
pub struct Screen {
    pub replica_id: uuid::Uuid,
    pub id: uuid::Uuid,
    #[row(desired)]
    pub level: i64,
    #[sqlx(flatten)]
    pub revised: Revised,
}

fn main() {}
