#[derive(sqlx::FromRow, Clone, magic_sync::Columns)]
pub struct Part {
    pub label: String,
}

#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog")]
pub struct Thing {
    pub id: uuid::Uuid,
    #[row(created)]
    #[sqlx(flatten)]
    pub part: Part,
}

fn main() {}
