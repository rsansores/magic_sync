//! What a server files with the rows a replica sends.

// ANCHOR: filed
use magic_sync::wire::Codec;
use magic_sync::{Conn, Error};
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;

/// A sale a register records.
#[derive(sqlx::FromRow, Clone, serde::Serialize, serde::Deserialize, magic_sync::Row)]
#[row(table = "sales", schema = "shop", mirror = "record")]
pub struct Sale {
    pub id: Uuid,
    pub register_id: Uuid,
    pub total_cents: i64,
}

/// Each sale filed moves its register's takings, in the transaction that
/// files it.
pub fn takings<'c>(
    conn: &'c mut Conn,
    sales: &'c [Sale],
    _: &'c (),
) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'c>> {
    Box::pin(async move {
        for sale in sales {
            sqlx::query("UPDATE shop.takings SET cents = cents + $2 WHERE register_id = $1")
                .bind(sale.register_id)
                .bind(sale.total_cents)
                .execute(&mut *conn)
                .await?;
        }
        Ok(())
    })
}

pub fn sales() -> Codec<()> {
    Codec::json::<Sale>(None).filed(takings)
}
// ANCHOR_END: filed
