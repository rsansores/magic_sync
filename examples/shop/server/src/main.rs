//! The head office: PostgreSQL, the catalog editor, and the sync server.
//!
//! The transport is the simplest one that keeps the engine's one rule — each
//! register gets its messages in order, the next only after it acknowledged
//! the last: a queue per register, held in memory, read over HTTP.

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json};
use axum::routing::{get, post, put};
use axum::Router;
use magic_sync::relay::Transport;
use magic_sync::run::server::{Anyone, Settings};
use magic_sync::status::{Listing, Overview, Progress, ReplicaState, RowProgress, Scope};
use magic_sync::{Error, Server};
use serde::{Deserialize, Serialize};
use shop_rows::{codecs, Product, Receipt, REGISTERS};
use sqlx::PgPool;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tower_http::services::ServeDir;
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

/// A queue of messages per register, and the registers whose line is cut:
/// sending to one of those fails, as a network would.
#[derive(Clone, Default)]
struct Queues {
    messages: Arc<Mutex<HashMap<Uuid, VecDeque<Vec<u8>>>>>,
    cut: Arc<Mutex<HashSet<Uuid>>>,
}

impl Queues {
    fn is_cut(&self, register: Uuid) -> bool {
        self.cut.lock().unwrap().contains(&register)
    }
}

impl Transport for Queues {
    async fn send(&self, register: Uuid, message: Vec<u8>, _id: &str) -> Result<(), Error> {
        if self.is_cut(register) {
            return Err(Error::Transport {
                reason: "the line to it is cut".to_owned(),
                transient: true,
            });
        }
        self.messages
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
                // A fix deployed (a register restarted without its bug) gets
                // through within seconds, not the hour a real server waits.
                retry_every: Duration::from_secs(10),
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
        .route("/api/state", get(state))
        .route("/api/sync/overview", get(overview))
        .route("/api/sync/replicas", get(listing))
        .route("/api/sync/progress/{register}", get(progress))
        .route("/api/sync/rows/{table}/{key}", get(row))
        .route("/api/products", post(add_product))
        .route("/api/products/{id}", put(edit_product).delete(delete_product))
        .route("/api/registers", post(register))
        .route("/api/registers/{register}/line", post(line))
        .route("/sync/{register}/next", get(next))
        .route("/sync/{register}/ack", post(ack))
        .route("/sync/{register}/up", post(up))
        .with_state(app)
        // The page: the Vue app in ../web, built.
        .fallback_service(ServeDir::new(PAGE).fallback(get(unbuilt)));
    // `--bind 0.0.0.0:8080` to reach it from another machine: it has no login.
    let args: Vec<String> = std::env::args().collect();
    let bind = args
        .iter()
        .position(|a| a == "--bind")
        .and_then(|i| args.get(i + 1).cloned())
        .unwrap_or_else(|| "127.0.0.1:8080".to_owned());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!("the head office is at http://{bind}");
    axum::serve(listener, router).await?;
    Ok(())
}

/// Where the page is built to.
const PAGE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../web/dist");

async fn unbuilt() -> Html<&'static str> {
    Html(
        "<p>The page is not built yet: in <code>examples/shop/web</code>, run \
         <code>pnpm install && pnpm build</code>, then reload.</p>",
    )
}

fn fail(e: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

#[derive(Serialize, sqlx::FromRow)]
struct RegisterState {
    id: Uuid,
    name: String,
    url: String,
    /// Its line is cut: the head office cannot reach it.
    #[sqlx(skip)]
    cut: bool,
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
    let mut registers: Vec<RegisterState> =
        sqlx::query_as("SELECT id, name, url FROM shop.registers ORDER BY name")
            .fetch_all(&app.pool)
            .await
            .map_err(fail)?;
    for register in &mut registers {
        register.cut = app.queues.is_cut(register.id);
    }
    Ok(Json(ShopState {
        products,
        receipts,
        registers,
    }))
}

// What the dashboard reads: the engine's answers, as they are. A shop with a
// login would ask them over the registers the caller may see
// (`Scope::Replicas`); this one has none, so every register. Each read is one
// snapshot of its own, so each takes a connection, not a transaction.

async fn overview(State(app): State<App>) -> Result<Json<Overview>, (StatusCode, String)> {
    let mut conn = app.pool.acquire().await.map_err(fail)?;
    let overview = app
        .server
        .engine()
        .overview(&mut conn, Scope::All, 20)
        .await
        .map_err(fail)?;
    Ok(Json(overview))
}

#[derive(Deserialize)]
struct Page {
    state: Option<ReplicaState>,
    #[serde(default)]
    offset: u32,
}

async fn listing(
    State(app): State<App>,
    Query(page): Query<Page>,
) -> Result<Json<Listing>, (StatusCode, String)> {
    let mut conn = app.pool.acquire().await.map_err(fail)?;
    let listing = app
        .server
        .engine()
        .listing(&mut conn, Scope::All, page.state, page.offset, 20)
        .await
        .map_err(fail)?;
    Ok(Json(listing))
}

async fn progress(
    State(app): State<App>,
    Path(register): Path<Uuid>,
) -> Result<Json<Progress>, (StatusCode, String)> {
    let mut conn = app.pool.acquire().await.map_err(fail)?;
    let progress = app
        .server
        .engine()
        .progress(&mut conn, Scope::Replicas(&[register]))
        .await
        .map_err(fail)?;
    Ok(Json(progress))
}

async fn row(
    State(app): State<App>,
    Path((table, key)): Path<(String, String)>,
) -> Result<Json<RowProgress>, (StatusCode, String)> {
    let mut conn = app.pool.acquire().await.map_err(fail)?;
    let places = app
        .server
        .engine()
        .row_progress(&mut conn, Scope::All, &table, &key)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(Json(places))
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

/// Cuts the line to a register, or mends it: a demo of a replica the server
/// cannot reach.
async fn line(State(app): State<App>, Path(register): Path<Uuid>) -> StatusCode {
    let mut cut = app.queues.cut.lock().unwrap();
    if !cut.remove(&register) {
        cut.insert(register);
    }
    StatusCode::NO_CONTENT
}

/// The register's next message, left at the head of its queue until it
/// acknowledges it.
async fn next(State(app): State<App>, Path(register): Path<Uuid>) -> impl IntoResponse {
    if app.queues.is_cut(register) {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    match app
        .queues
        .messages
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
    if let Some(queue) = app.queues.messages.lock().unwrap().get_mut(&register) {
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
    if app.queues.is_cut(register) {
        return Err((StatusCode::SERVICE_UNAVAILABLE, "the line is cut".to_owned()));
    }
    app.server
        .receive(register, &body)
        .await
        .map_err(fail)?;
    Ok(StatusCode::NO_CONTENT)
}
