#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "a_table_whose_name_is_longer_than_postgresql_keeps_of_a_name_at_all", mirror = "catalog")]
pub struct Thing {
    pub id: uuid::Uuid,
}

fn main() {}
