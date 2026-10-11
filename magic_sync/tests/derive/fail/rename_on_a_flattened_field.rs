#[derive(sqlx::FromRow, Clone, magic_sync::Columns)]
pub struct Part {
    pub label: String,
}

#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog")]
pub struct Thing {
    pub id: uuid::Uuid,
    #[sqlx(flatten, rename = "part")]
    pub part: Part,
}

fn main() {}
