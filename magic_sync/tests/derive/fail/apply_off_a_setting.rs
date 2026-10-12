#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "screens", mirror = "record", apply = "always")]
pub struct Screen {
    pub id: uuid::Uuid,
}

fn main() {}
