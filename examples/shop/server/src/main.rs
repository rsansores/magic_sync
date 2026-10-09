//! The head office: PostgreSQL, the catalog editor, and the sync server.
//!
//! The transport is the simplest one that keeps the engine's one rule — each
//! register gets its messages in order, the next only after it acknowledged
//! the last: a queue per register, held in memory, read over HTTP.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json};
use axum::routing::{get, post, put};
use axum::Router;
use magic_sync::relay::Transport;
use magic_sync::run::server::{Anyone, Settings};
use magic_sync::{Error, Server};
use serde::{Deserialize, Serialize};
use shop_rows::{codecs, Product, Receipt, REGISTERS};
use sqlx::PgPool;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

const SCHEMA: &str = "
CREATE SCHEMA IF NOT EXISTS shop;
CREATE TABLE IF NOT EXISTS shop.registers (
    id     UUID PRIMARY KEY,
    name   TEXT NOT NULL,
    url    TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true
);
CREATE TABLE IF NOT EXISTS shop.products (
    id          UUID PRIMARY KEY,
    name        TEXT NOT NULL,
    price_cents BIGINT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS shop.receipts (
    id          UUID PRIMARY KEY,
    register_id UUID NOT NULL,
    product_id  UUID NOT NULL,
    quantity    INTEGER NOT NULL,
    total_cents BIGINT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL
);
";

/// A queue of messages per register.
#[derive(Clone, Default)]
struct Queues(Arc<Mutex<HashMap<Uuid, VecDeque<Vec<u8>>>>>);

impl Transport for Queues {
    async fn send(&self, register: Uuid, message: Vec<u8>, _id: &str) -> Result<(), Error> {
        self.0
            .lock()
            .unwrap()
            .entry(register)
            .or_default()
            .push_back(message);
        Ok(())
    }
}

type ShopServer = Server<Anyone, Queues>;

#[derive(Clone)]
struct App {
    pool: PgPool,
    server: Arc<ShopServer>,
    queues: Queues,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info,sqlx=warn".to_owned()),
        )
        .init();
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://shop:shop@localhost:5433/shop".to_owned());
    let pool = PgPool::connect(&url).await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;

    // No migrations of its own: the engine sets up what the database lacks.
    let queues = Queues::default();
    let server = Arc::new(
        Server::install(
            pool.clone(),
            codecs(),
            REGISTERS,
            queues.clone(),
            Anyone(pool.clone()),
            Settings {
                idle: Duration::from_millis(200),
                ..Settings::default()
            },
        )
        .await?,
    );
    let relay = server.clone();
    tokio::spawn(async move { relay.run(std::future::pending()).await });

    let app = App {
        pool,
        server,
        queues,
    };
    let router = Router::new()
        .route("/", get(|| async { Html(include_str!("index.html")) }))
        .route("/api/state", get(state))
        .route("/api/products", post(add_product))
        .route("/api/products/{id}", put(edit_product).delete(delete_product))
        .route("/api/registers", post(register))
        .route("/sync/{register}/next", get(next))
        .route("/sync/{register}/ack", post(ack))
        .route("/sync/{register}/up", post(up))
        .with_state(app);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    tracing::info!("the head office is at http://127.0.0.1:8080");
    axum::serve(listener, router).await?;
    Ok(())
}

fn fail(e: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

#[derive(Serialize)]
struct RegisterState {
    id: Uuid,
    name: String,
    url: String,
    /// Products it confirmed it holds, of the ones it should.
    holds: i64,
    should: i64,
    in_flight: i64,
    /// Messages waiting in its queue.
    queued: usize,
}

#[derive(Serialize)]
struct ShopState {
    products: Vec<Product>,
    receipts: Vec<Receipt>,
    registers: Vec<RegisterState>,
}

async fn state(State(app): State<App>) -> Result<Json<ShopState>, (StatusCode, String)> {
    let products = sqlx::query_as("SELECT * FROM shop.products ORDER BY name")
        .fetch_all(&app.pool)
        .await
        .map_err(fail)?;
    let receipts = sqlx::query_as("SELECT * FROM shop.receipts ORDER BY created_at DESC LIMIT 20")
        .fetch_all(&app.pool)
        .await
        .map_err(fail)?;
    let known: Vec<(Uuid, String, String)> =
        sqlx::query_as("SELECT id, name, url FROM shop.registers ORDER BY name")
            .fetch_all(&app.pool)
            .await
            .map_err(fail)?;
    let mut tx = app.server.begin().await.map_err(fail)?;
    let progress = app
        .server
        .engine()
        .progress(&mut tx, magic_sync::plan::Scope::All)
        .await
        .map_err(fail)?;
    let queues = app.queues.0.lock().unwrap().clone();
    let registers = known
        .into_iter()
        .map(|(id, name, url)| {
            let products = progress
                .iter()
                .find(|p| p.replica == id)
                .and_then(|p| p.tables.iter().find(|t| t.table == "products"));
            RegisterState {
                id,
                name,
                url,
                holds: products.map_or(0, |t| t.up_to_date()),
                should: products.map_or(0, |t| t.should),
                in_flight: products.map_or(0, |t| t.in_flight),
                queued: queues.get(&id).map_or(0, VecDeque::len),
            }
        })
        .collect();
    Ok(Json(ShopState {
        products,
        receipts,
        registers,
    }))
}

#[derive(Deserialize)]
struct ProductForm {
    name: String,
    price_cents: i64,
}

async fn add_product(
    State(app): State<App>,
    Json(form): Json<ProductForm>,
) -> Result<StatusCode, (StatusCode, String)> {
    // The clock at the write, never `now()`: a version only moves forward.
    sqlx::query("INSERT INTO shop.products VALUES ($1, $2, $3, clock_timestamp())")
        .bind(Uuid::now_v7())
        .bind(form.name)
        .bind(form.price_cents)
        .execute(&app.pool)
        .await
        .map_err(fail)?;
    Ok(StatusCode::CREATED)
}

async fn edit_product(
    State(app): State<App>,
    Path(id): Path<Uuid>,
    Json(form): Json<ProductForm>,
) -> Result<StatusCode, (StatusCode, String)> {
    sqlx::query(
        "UPDATE shop.products SET name = $2, price_cents = $3, updated_at = clock_timestamp() \
         WHERE id = $1",
    )
    .bind(id)
    .bind(form.name)
    .bind(form.price_cents)
    .execute(&app.pool)
    .await
    .map_err(fail)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_product(
    State(app): State<App>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, (StatusCode, String)> {
    sqlx::query("DELETE FROM shop.products WHERE id = $1")
        .bind(id)
        .execute(&app.pool)
        .await
        .map_err(fail)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct RegisterForm {
    id: Uuid,
    name: String,
    url: String,
}

/// A register introduces itself. The first time, it is provisioned: sent
/// everything it should hold.
async fn register(
    State(app): State<App>,
    Json(form): Json<RegisterForm>,
) -> Result<StatusCode, (StatusCode, String)> {
    let mut tx = app.pool.begin().await.map_err(fail)?;
    let new = sqlx::query(
        "INSERT INTO shop.registers (id, name, url) VALUES ($1, $2, $3) \
         ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, url = EXCLUDED.url",
    )
    .bind(form.id)
    .bind(&form.name)
    .bind(&form.url)
    .execute(&mut *tx)
    .await
    .map_err(fail)?;
    let known: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM sync.held WHERE replica_id = $1)")
        .bind(form.id)
        .fetch_one(&mut *tx)
        .await
        .map_err(fail)?;
    if new.rows_affected() > 0 && !known {
        magic_sync::plan::note_resync(&mut tx, None, form.id)
            .await
            .map_err(fail)?;
    }
    tx.commit().await.map_err(fail)?;
    Ok(StatusCode::NO_CONTENT)
}

/// The register's next message, left at the head of its queue until it
/// acknowledges it.
async fn next(State(app): State<App>, Path(register): Path<Uuid>) -> impl IntoResponse {
    match app
        .queues
        .0
        .lock()
        .unwrap()
        .get(&register)
        .and_then(|q| q.front().cloned())
    {
        Some(message) => (StatusCode::OK, message).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    }
}

async fn ack(State(app): State<App>, Path(register): Path<Uuid>) -> StatusCode {
    if let Some(queue) = app.queues.0.lock().unwrap().get_mut(&register) {
        queue.pop_front();
    }
    StatusCode::NO_CONTENT
}

/// What a register sends up: filed, and answered once filed.
async fn up(
    State(app): State<App>,
    Path(register): Path<Uuid>,
    body: Bytes,
) -> Result<StatusCode, (StatusCode, String)> {
    app.server
        .receive(register, &body)
        .await
        .map_err(fail)?;
    Ok(StatusCode::NO_CONTENT)
}
