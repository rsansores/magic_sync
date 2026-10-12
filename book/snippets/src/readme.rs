//! The README's code, compiled; a test holds the README to it.

// The rows both sides share.
pub mod rows {
    use chrono::{DateTime, Utc};
    use uuid::Uuid;

    // One struct is one table, and says how it travels. Both sides share it.
    #[derive(sqlx::FromRow, Clone, serde::Serialize, serde::Deserialize, magic_sync::Row)]
    #[row(
        table = "products",
        schema = "shop",
        mirror = "catalog",
        sync = "broadcast"
    )]
    pub struct Product {
        pub id: Uuid,
        pub name: String,
        pub price_cents: i64,
        #[row(version)]
        pub updated_at: DateTime<Utc>,
    }
}

#[cfg(feature = "server")]
pub mod server {
    use magic_sync::run::server::{Anyone, Settings, Transport};
    use magic_sync::{Error, Server};
    use shop_rows::{codecs, REGISTERS};
    use sqlx::PgPool;
    use std::future::Future;
    use std::sync::Arc;
    use uuid::Uuid;

    pub async fn start(
        pool: PgPool,
        transport: impl Transport,
        shutdown: impl Future<Output = ()> + Send + 'static,
        replica: Uuid,
        bytes: Vec<u8>,
    ) -> Result<(), Error> {
        // The server (feature `postgres`), on a database your migrations set up.
        // You say how bytes reach a replica; the engine runs itself.
        let server = Arc::new(
            Server::new(
                pool.clone(),
                codecs(),
                REGISTERS,
                transport,
                Anyone::new(pool),
                Settings::default(),
            )
            .await?,
        );
        tokio::spawn({
            let server = server.clone();
            async move { server.run(shutdown).await }
        });
        // Every message a replica sends up, from the replica your transport says sent it.
        server.receive(replica, &bytes).await?;
        Ok(())
    }
}

#[cfg(feature = "replica")]
pub mod replica {
    use magic_sync::run::replica::{Settings, Uplink};
    use magic_sync::{Error, Replica};
    use shop_rows::codecs;
    use sqlx::SqlitePool;
    use std::future::Future;
    use std::sync::Arc;
    use uuid::Uuid;

    pub async fn start(
        sqlite: SqlitePool,
        me: Uuid,
        uplink: impl Uplink,
        shutdown: impl Future<Output = ()> + Send + 'static,
        bytes: Vec<u8>,
    ) -> Result<(), Error> {
        // A replica (feature `sqlite`): its way up runs itself. Hand it what arrives,
        // and acknowledge it to your transport once `receive` returned Ok.
        let replica =
            Arc::new(Replica::new(sqlite, codecs(), me, uplink, Settings::default()).await?);
        tokio::spawn({
            let replica = replica.clone();
            async move { replica.run(shutdown).await }
        });
        replica.receive(&bytes).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    /// Each line, without the indentation it has where it is.
    fn lines(text: &str) -> Vec<&str> {
        text.lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .collect()
    }

    /// Every Rust block of the README is code of this file, line for line:
    /// what a stranger reads first compiles.
    #[test]
    fn the_readme_shows_code_that_compiles() {
        let readme = include_str!("../../../README.md");
        let source = lines(include_str!("readme.rs"));
        let blocks: Vec<&str> = readme
            .split("```rust\n")
            .skip(1)
            .map(|rest| rest.split("```").next().unwrap_or_default())
            .collect();
        assert!(!blocks.is_empty(), "the README shows no code");
        for block in blocks {
            let block = lines(block);
            assert!(
                source.windows(block.len()).any(|w| w == block.as_slice()),
                "the README shows code that src/readme.rs does not have:\n{}",
                block.join("\n")
            );
        }
    }
}
