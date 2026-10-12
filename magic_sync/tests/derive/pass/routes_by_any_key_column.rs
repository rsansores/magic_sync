//! A route names a column of a type the engine can read as a key: a replica
//! is a uuid, nullable or not; a parent is a uuid or an integer.

use uuid::Uuid;

#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "lamps", mirror = "catalog", sync = "assigned(shop_id)")]
pub struct Lamp {
    pub id: Uuid,
    pub shop_id: Option<Uuid>,
}

#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "bulbs", mirror = "catalog", sync = "follows(lamp)")]
pub struct Bulb {
    pub id: Uuid,
    pub lamp: Option<i32>,
}

fn main() {}
