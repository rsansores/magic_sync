#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog")]
pub struct Thing
where
    u8: Copy,
{
    pub id: uuid::Uuid,
}

fn main() {}
