//! A register: SQLite, its own page, and the sync replica. It sells from the
//! products it holds and keeps selling while it is unplugged; its receipts go
//! up when it is plugged back in.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, Json};
use axum::routing::{get, post};
use axum::Router;
use magic_sync::replica::{self, Received};
use magic_sync::run::replica::{Settings, Uplink};
use magic_sync::{Error, Replica};
use serde::{Deserialize, Serialize};
use shop_rows::{codecs, Product};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS me (id BLOB PRIMARY KEY NOT NULL);
CREATE TABLE IF NOT EXISTS products (
    id          BLOB PRIMARY KEY NOT NULL,
    name        TEXT NOT NULL,
    price_cents INTEGER NOT NULL,
    updated_at  TEXT NOT NULL,
    retired_at  TEXT
);
CREATE TABLE IF NOT EXISTS receipts (
    id          BLOB PRIMARY KEY NOT NULL,
    register_id BLOB NOT NULL,
    product_id  BLOB NOT NULL REFERENCES products (id),
    quantity    INTEGER NOT NULL,
    total_cents INTEGER NOT NULL,
    created_at  TEXT NOT NULL
);
";

/// The way up: a POST to the head office, unless the register is unplugged.
struct Http {
    client: reqwest::Client,
    server: String,
    online: Arc<AtomicBool>,
}

impl Uplink for Http {
    async fn send(&self, register: Uuid, _lane: &'static str, message: Vec<u8>) -> Result<(), Error> {
        if !self.online.load(Ordering::Relaxed) {
            return Err(away("unplugged"));
        }
        self.client
            .post(format!("{}/sync/{register}/up", self.server))
            .body(message)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(away)?;
        Ok(())
    }
}

fn away(e: impl std::fmt::Display) -> Error {
    Error::Transport {
        reason: e.to_string(),
        transient: true,
    }
}

type ShopReplica = Replica<(), Uuid, Http>;

#[derive(Clone)]
struct App {
    pool: SqlitePool,
    replica: Arc<ShopReplica>,
    me: Uuid,
    name: String,
    online: Arc<AtomicBool>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info,sqlx=warn".to_owned()),
        )
        .init();
    let args: Vec<String> = std::env::args().collect();
    let arg = |flag: &str, default: &str| {
        args.iter()
            .position(|a| a == flag)
            .and_then(|i| args.get(i + 1).cloned())
            .unwrap_or_else(|| default.to_owned())
    };
    let name = arg("--name", "Register 1");
    let port = arg("--port", "8081");
    let server = arg("--server", "http://127.0.0.1:8080");
    // `--bind 0.0.0.0` to reach its page from another machine: it has no login.
    let bind = arg("--bind", "127.0.0.1");
    let file = arg("--db", &format!("register-{port}.db"));

    let pool = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&file)
            .create_if_missing(true)
            .foreign_keys(true),
    )
    .await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    // No migrations of its own: the engine sets up what the database lacks,
    // including the trigger that notes every receipt written.
    replica::install(&mut *pool.acquire().await?, codecs()).await?;
    let me: Uuid = match sqlx::query_scalar("SELECT id FROM me").fetch_optional(&pool).await? {
        Some(id) => id,
        None => {
            let id = Uuid::now_v7();
            sqlx::query("INSERT INTO me VALUES ($1)").bind(id).execute(&pool).await?;
            id
        }
    };

    let online = Arc::new(AtomicBool::new(true));
    let client = reqwest::Client::new();
    let replica = Arc::new(Replica::new(
        pool.clone(),
        codecs(),
        me,
        Http {
            client: client.clone(),
            server: server.clone(),
            online: online.clone(),
        },
        Settings {
            idle: Duration::from_millis(300),
            ..Settings::default()
        },
    ));
    let up = replica.clone();
    tokio::spawn(async move { up.run(std::future::pending()).await });
    tokio::spawn(receive(client.clone(), server.clone(), me, replica.clone(), online.clone()));

    // Introduce itself to the head office, which provisions it the first time.
    let url = format!("http://127.0.0.1:{port}");
    let hello = serde_json::json!({ "id": me, "name": name, "url": url });
    while client
        .post(format!("{server}/api/registers"))
        .json(&hello)
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .is_err()
    {
        tracing::warn!("the head office is not answering yet");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    let app = App {
        pool,
        replica,
        me,
        name,
        online,
    };
    let router = Router::new()
        .route("/", get(|| async { Html(include_str!("index.html")) }))
        .route("/api/state", get(state))
        .route("/api/sell", post(sell))
        .route("/api/plug", post(plug))
        .with_state(app);
    let listener = tokio::net::TcpListener::bind(format!("{bind}:{port}")).await?;
    tracing::info!("this register is at {url}");
    axum::serve(listener, router).await?;
    Ok(())
}

/// Takes the head office's messages one at a time: applied and committed,
/// then acknowledged — never before.
async fn receive(
    client: reqwest::Client,
    server: String,
    me: Uuid,
    replica: Arc<ShopReplica>,
    online: Arc<AtomicBool>,
) {
    loop {
        if !online.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(300)).await;
            continue;
        }
        let next = client
            .get(format!("{server}/sync/{me}/next"))
            .send()
            .await
            .and_then(reqwest::Response::error_for_status);
        let message = match next {
            Ok(r) if r.status() == reqwest::StatusCode::OK => r.bytes().await.ok(),
            _ => None,
        };
        let Some(message) = message else {
            tokio::time::sleep(Duration::from_millis(300)).await;
            continue;
        };
        match replica.receive(&message).await {
            Ok(received) => {
                if let Received::Applied(tables) = &received {
                    tracing::info!(?tables, "applied");
                }
                let _ = client.post(format!("{server}/sync/{me}/ack")).send().await;
            }
            Err(e) => {
                tracing::warn!("not applied, and comes again: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

#[derive(Serialize)]
struct RegisterState {
    name: String,
    online: bool,
    products: Vec<Product>,
    sold: i64,
    unsent: i64,
    unfiled: i64,
}

async fn state(State(app): State<App>) -> Result<Json<RegisterState>, (StatusCode, String)> {
    let fail = |e: sqlx::Error| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
    let products = sqlx::query_as(
        "SELECT id, name, price_cents, updated_at FROM products WHERE retired_at IS NULL ORDER BY name",
    )
    .fetch_all(&app.pool)
    .await
    .map_err(fail)?;
    let sold: i64 = sqlx::query_scalar("SELECT count(*) FROM receipts")
        .fetch_one(&app.pool)
        .await
        .map_err(fail)?;
    let mut conn = app.pool.acquire().await.map_err(fail)?;
    let up = replica::progress(&mut conn)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(RegisterState {
        name: app.name.clone(),
        online: app.online.load(Ordering::Relaxed),
        products,
        sold,
        unsent: up.iter().map(|t| t.unsent).sum(),
        unfiled: up.iter().map(|t| t.unfiled).sum(),
    }))
}

#[derive(Deserialize)]
struct Sale {
    product_id: Uuid,
}

/// A sale: a receipt written here. The engine's trigger notes it, and it goes
/// up as soon as the register can reach the head office.
async fn sell(State(app): State<App>, Json(sale): Json<Sale>) -> Result<StatusCode, (StatusCode, String)> {
    let fail = |e: sqlx::Error| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
    let price: i64 = sqlx::query_scalar("SELECT price_cents FROM products WHERE id = $1")
        .bind(sale.product_id)
        .fetch_one(&app.pool)
        .await
        .map_err(fail)?;
    sqlx::query("INSERT INTO receipts VALUES ($1, $2, $3, 1, $4, $5)")
        .bind(Uuid::now_v7())
        .bind(app.me)
        .bind(sale.product_id)
        .bind(price)
        .bind(chrono::Utc::now())
        .execute(&app.pool)
        .await
        .map_err(fail)?;
    app.replica.nudge();
    Ok(StatusCode::CREATED)
}

/// Unplugs the register, or plugs it back in.
async fn plug(State(app): State<App>) -> StatusCode {
    app.online.fetch_xor(true, Ordering::Relaxed);
    app.replica.nudge();
    StatusCode::NO_CONTENT
}
