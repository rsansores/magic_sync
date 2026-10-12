//! A host that reaches magic_sync through a crate of its own names the path.

mod platform {
    pub use magic_sync as sync;
}

#[derive(sqlx::FromRow, Clone, platform::sync::Row)]
#[row(crate = "platform::sync", table = "regions", mirror = "catalog", sync = "broadcast")]
pub struct Region {
    pub id: uuid::Uuid,
    pub name: String,
}

fn main() {}
