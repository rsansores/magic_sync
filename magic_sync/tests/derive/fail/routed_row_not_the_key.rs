#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(
    table = "things",
    mirror = "catalog",
    key = "shelf, id",
    sync = "routed(view = a, row = id, replica = r)"
)]
pub struct Thing {
    pub shelf: i32,
    pub id: uuid::Uuid,
}

fn main() {}
