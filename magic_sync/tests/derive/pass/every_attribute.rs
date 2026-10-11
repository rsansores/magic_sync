//! Every attribute, on every kind of table, in one build.

use chrono::{DateTime, NaiveDateTime, Utc};
use uuid::Uuid;

type Cents = i64;

/// Written on the server; read through a view; kept by a replica that still
/// points at a row; with columns one side has and the row leaves out.
#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(
    table = "products",
    schema = "shop",
    mirror = "catalog",
    sync = "assigned(store_id)",
    active = "shop.live_products",
    retired = "retired_at",
    unmirrored = "archived_at, notes"
)]
pub struct Product {
    pub id: Uuid,
    pub store_id: Uuid,
    pub r#type: String,
    #[sqlx(rename = "Display Name")]
    pub display_name: String,
    pub price: Cents,
    #[row(generated)]
    pub search: Option<String>,
    #[sqlx(skip)]
    pub cached: bool,
    #[row(version)]
    pub updated_at: DateTime<Utc>,
}

/// Its creation, flattened into rows that share it.
#[derive(sqlx::FromRow, Clone, magic_sync::Columns)]
pub struct Stamps {
    #[row(created)]
    pub created_at: NaiveDateTime,
}

/// A replica's reports, keyed by the replica: editable, newest wins.
#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(
    table = "readings",
    schema = "fleet",
    mirror = "record",
    key = "replica_id, sensor, seq",
    origin = "replica_id"
)]
pub struct Reading {
    pub replica_id: Uuid,
    pub sensor: i16,
    pub seq: i32,
    pub value: f64,
    #[row(version)]
    pub at: i64,
    #[sqlx(flatten)]
    pub stamps: Stamps,
}

/// Written on both sides, column by column.
#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "tasks", mirror = "both", sync = "grouped(members = crew_tasks, assigned = crews)")]
pub struct Task {
    pub id: Uuid,
    pub title: String,
    #[row(replica_owned)]
    pub done: bool,
    #[row(version)]
    pub updated_at: DateTime<Utc>,
}

/// A row that goes wherever its parent goes.
#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "task_notes", mirror = "catalog", key = "task_id", sync = "follows(task_id)")]
pub struct TaskNote {
    pub task_id: Uuid,
    pub text: String,
}

/// The server's own.
#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "audits", mirror = "server")]
pub struct Audit {
    pub id: i64,
}

fn main() {
    let _ = magic_sync::wire::Codec::<()>::new::<Product, ProductRow>(to, from, None);
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProductRow {
    #[prost(string, tag = "1")]
    pub id: String,
}

fn to(_: Product) -> Result<ProductRow, magic_sync::wire::MapError> {
    Err(magic_sync::wire::MapError::new("not this one"))
}

fn from(_: ProductRow) -> Result<Product, magic_sync::wire::MapError> {
    Err(magic_sync::wire::MapError::new("not this one"))
}
