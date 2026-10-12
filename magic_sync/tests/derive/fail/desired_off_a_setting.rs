#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "screens", mirror = "record")]
pub struct Screen {
    pub id: uuid::Uuid,
    #[row(desired)]
    pub level: i64,
}

fn main() {}
