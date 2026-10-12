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

// ANCHOR: footer
/// What a register prints under every receipt: the register's own setting,
/// which the head office may wish another — carried out only while the
/// register has not changed it since the head office looked.
#[derive(sqlx::FromRow, Clone, Debug, Serialize, Deserialize, magic_sync::Row)]
#[row(
    table = "footers",
    schema = "shop",
    mirror = "setting",
    key = "register_id",
    origin = "register_id",
    apply = "if_unchanged"
)]
pub struct Footer {
    pub register_id: Uuid,
    #[row(desired)]
    pub text: String,
    #[row(version)]
    pub changed_at: DateTime<Utc>,
}
// ANCHOR_END: footer

// ANCHOR: registers
/// The registers, as the engine finds them on the server.
#[cfg(feature = "server")]
pub const REGISTERS: magic_sync::plan::Replicas =
    magic_sync::plan::Replicas::new("shop.registers", "registers").live("active");
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
        Codec::json::<Footer>(None),
    ]
}
// ANCHOR_END: codecs
