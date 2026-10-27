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
    pub updated_at: DateTime<Utc>,
}

/// A replica's setting, whose level the server may wish: carried out only
/// while the replica holds the version the wish is based on.
#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(
    table = "screens",
    mirror = "setting",
    key = "replica_id, id",
    origin = "replica_id",
    apply = "if_unchanged",
    unmirrored = "installed_at"
)]
pub struct Screen {
    pub replica_id: Uuid,
    pub id: Uuid,
    #[row(desired)]
    pub level: i32,
    #[row(desired)]
    #[sqlx(rename = "Mode")]
    pub mode: Option<String>,
    #[row(version)]
    pub at: NaiveDateTime,
    #[sqlx(flatten)]
    pub stamps: Stamps,
}

/// A row held where a view pairs it with replicas, by its key of two
/// columns, in key order.
#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(
    table = "documents",
    schema = "library",
    mirror = "catalog",
    key = "shelf, number",
    sync = "routed(view = library.document_readers, row = (shelf, document_number), replica = reader_id)"
)]
pub struct Document {
    pub shelf: i32,
    pub number: i64,
    pub title: String,
}

/// A row that goes wherever its parent goes.
#[derive(sqlx::FromRow, Clone, magic_sync::Row)]
#[row(table = "task_notes", mirror = "catalog", key = "task_id", sync = "follows(task_id)")]
pub struct TaskNote {
    pub task_id: Uuid,
    pub text: String,
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
