//! The shop's tables, as both sides hold them: the same structs, so the same
//! rows, on the server's PostgreSQL and on every register's SQLite.

use chrono::{DateTime, Utc};
use magic_sync::wire::Codec;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ANCHOR: rows
/// A product: written at the head office, held by every register.
#[derive(sqlx::FromRow, Clone, Debug, Serialize, Deserialize, magic_sync::Row)]
#[row(
    table = "products",
    schema = "shop",
    mirror = "catalog",
    sync = "broadcast",
    retired = "retired_at"
)]
pub struct Product {
    pub id: Uuid,
    pub name: String,
    pub price_cents: i64,
    #[row(version)]
    pub updated_at: DateTime<Utc>,
}

/// A receipt: written by a register, filed at the head office.
#[derive(sqlx::FromRow, Clone, Debug, Serialize, Deserialize, magic_sync::Row)]
#[row(table = "receipts", schema = "shop", mirror = "record")]
pub struct Receipt {
    pub id: Uuid,
    pub register_id: Uuid,
    pub product_id: Uuid,
    pub quantity: i32,
    pub total_cents: i64,
    #[row(created)]
    pub created_at: DateTime<Utc>,
}
// ANCHOR_END: rows

// ANCHOR: registers
/// The registers, as the engine finds them on the server.
#[cfg(feature = "server")]
pub const REGISTERS: magic_sync::plan::Replicas =
    magic_sync::plan::Replicas::relation("shop.registers").table("registers").live("active");
// ANCHOR_END: registers

// ANCHOR: codecs
/// A receipt is filed under the register that sent it, whatever it says.
fn place_receipt(receipt: &mut Receipt, register: Uuid, _: &()) {
    receipt.register_id = register;
}

/// Every table that travels, parents first.
pub fn codecs() -> Vec<Codec<()>> {
    vec![
        Codec::json::<Product>(None),
        Codec::json::<Receipt>(Some(place_receipt)),
    ]
}
// ANCHOR_END: codecs
