#[derive(sqlx::FromRow, Clone, magic_sync::Columns)]
pub struct State {
    #[row(replica_owned)]
    pub done: bool,
}

#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog")]
pub struct Thing {
    pub id: uuid::Uuid,
    #[sqlx(flatten)]
    pub state: State,
}

fn main() {}
