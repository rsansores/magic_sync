#[derive(Clone, magic_sync::Row)]
#[row(table = "things", mirror = "catalog")]
pub struct Thing(uuid::Uuid);

fn main() {}
