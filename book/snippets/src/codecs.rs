//! A table that travels as a protobuf message of its own.

use chrono::{DateTime, Utc};
use magic_sync::wire::{Codec, MapError};
use shop_rows::Product;
use uuid::Uuid;

// ANCHOR: message
/// A product on the wire.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ProductRow {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(string, tag = "2")]
    pub name: String,
    #[prost(int64, tag = "3")]
    pub price_cents: i64,
    #[prost(string, tag = "4")]
    pub updated_at: String,
}
// ANCHOR_END: message

// ANCHOR: codec
pub fn product() -> Codec<()> {
    Codec::new::<Product, ProductRow>(product_to_wire, product_from_wire, None)
}
// ANCHOR_END: codec

// ANCHOR: mapping
fn product_to_wire(p: Product) -> Result<ProductRow, MapError> {
    Ok(ProductRow {
        id: p.id.to_string(),
        name: p.name,
        price_cents: p.price_cents,
        updated_at: p.updated_at.to_rfc3339(),
    })
}

fn product_from_wire(m: ProductRow) -> Result<Product, MapError> {
    Ok(Product {
        id: Uuid::parse_str(&m.id)?,
        name: m.name,
        price_cents: m.price_cents,
        updated_at: DateTime::parse_from_rfc3339(&m.updated_at)?.with_timezone(&Utc),
    })
}
// ANCHOR_END: mapping
