//! The recipes' rows.

use chrono::{DateTime, Utc};
use uuid::Uuid;

// ANCHOR: follows
/// A product's nutrition facts: wherever the product goes.
#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(
    table = "nutrition",
    schema = "shop",
    mirror = "catalog",
    key = "product_id",
    sync = "follows(product_id)"
)]
pub struct Nutrition {
    pub product_id: Uuid,
    pub calories: i32,
}
// ANCHOR_END: follows

// ANCHOR: assigned
/// An order: to the register it is assigned to.
#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(
    table = "orders",
    schema = "shop",
    mirror = "catalog",
    sync = "assigned(register_id)"
)]
pub struct Order {
    pub id: Uuid,
    pub register_id: Option<Uuid>,
    pub product_id: Uuid,
    pub quantity: i32,
}
// ANCHOR_END: assigned

// ANCHOR: grouped
/// A customer: to every register given a list it is on.
#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(
    table = "customers",
    schema = "shop",
    mirror = "catalog",
    sync = "grouped(members = list_members, assigned = register_lists)"
)]
pub struct Customer {
    pub id: Uuid,
    pub name: String,
}
// ANCHOR_END: grouped

// ANCHOR: both
/// A register's state: the head office writes its label, the register its
/// battery.
#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(
    table = "register_state",
    schema = "shop",
    mirror = "both",
    sync = "assigned(id)"
)]
pub struct RegisterState {
    pub id: Uuid,
    pub label: String,
    #[row(replica_owned)]
    pub battery: Option<i32>,
    #[row(version)]
    pub updated_at: DateTime<Utc>,
}
// ANCHOR_END: both

// ANCHOR: active
/// A product, as replicas hold it: never an archived one.
#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(
    table = "products",
    schema = "shop",
    mirror = "catalog",
    sync = "broadcast",
    active = "shop.live_products",
    unmirrored = "archived_at"
)]
pub struct LiveProduct {
    pub id: Uuid,
    pub name: String,
    #[row(version)]
    pub updated_at: DateTime<Utc>,
}
// ANCHOR_END: active
