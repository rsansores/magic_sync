//! Several kinds of replica on one server.

use magic_sync::plan::{Audience, Replicas};
use magic_sync::run::server::{Anyone, Server, Settings, Transport};
use magic_sync::wire::Codec;
use magic_sync::Error;
use shop_rows::{codecs, REGISTERS};
use sqlx::PgPool;
use uuid::Uuid;

// ANCHOR: shape
/// A product as the books hold it: its category's tax code joined in, and
/// only while it is on sale. Read from a view over `shop.products`; noted,
/// keyed and placed as a product all the same.
#[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, magic_sync::Row)]
#[row(
    table = "products",
    schema = "shop",
    mirror = "catalog",
    sync = "broadcast",
    from = "books.products",
    depends = "category_id"
)]
pub struct BookedProduct {
    pub id: Uuid,
    pub name: String,
    pub price_cents: i64,
    pub tax_code: String,
}
// ANCHOR_END: shape

// ANCHOR: audiences
/// The ledgers of the shop's accounting system: one replica each.
pub const LEDGERS: Replicas = Replicas::new("books.ledgers", "ledgers");

pub async fn start<T: Transport>(pool: PgPool, transport: T) -> Result<Server<Anyone, T>, Error> {
    Server::new(
        pool.clone(),
        vec![
            // The registers, every table as it is.
            Audience::new("registers", REGISTERS, codecs()),
            // The books: the products alone, in their own shape.
            Audience::new("books", LEDGERS, vec![Codec::json::<BookedProduct>(None)]),
        ],
        transport,
        Anyone::new(pool),
        Settings::default(),
    )
    .await
}
// ANCHOR_END: audiences

// ANCHOR: routes
/// One transport for both kinds: each message goes the way of its audience.
pub struct Routes;

impl Transport for Routes {
    async fn send(
        &self,
        audience: &str,
        _replica: Uuid,
        _message: Vec<u8>,
        _id: &str,
    ) -> Result<(), Error> {
        match audience {
            "books" => {
                // hand the message to the ledger's queue on the
                // accounting system's broker
            }
            _ => {
                // hand the message to the register's queue
            }
        }
        Ok(())
    }
}
// ANCHOR_END: routes
