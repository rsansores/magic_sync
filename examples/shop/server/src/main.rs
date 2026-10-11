//! The head office: PostgreSQL, the catalog editor, and the sync server.
//!
//! The transport is a queue per register in the head office's own database,
//! read over HTTP: a register takes the message at the head of its queue,
//! applies it, and acknowledges it by its id, which takes it out. That keeps
//! what the engine needs of a transport (PROTOCOL.md, section 2): nothing
//! sent is lost, a restart included; a register gets its messages in order,
//! the next only once it acknowledged the last; and a message it did not
//! acknowledge comes again.

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json, Response};
use axum::routing::{get, post, put};
use axum::Router;
use magic_sync::run::server::{Anyone, Settings, Transport};
use magic_sync::status::{Listing, Overview, Progress, ReplicaState, RowProgress, Scope};
use magic_sync::{Error, Server};
use serde::{Deserialize, Serialize};
use shop_rows::{codecs, Product, Receipt, REGISTERS};
use sqlx::PgPool;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{watch, Notify};
use tower_http::services::ServeDir;
use uuid::Uuid;

/// How long a register asking for its next message waits for one when there
/// is none, before it is answered that there is none.
const WAIT_FOR_NEXT: Duration = Duration::from_secs(20);

/// The header that names the message a register is given: what it
/// acknowledges.
const MESSAGE_ID: &str = "message-id";

// ANCHOR: transport
/// The head office's way to its registers: a queue per register, in the
/// database (`shop.deliveries`). The registers whose line is cut are a demo
/// of a network failing: sending to one fails.
#[derive(Clone)]
struct Deliveries {
    pool: PgPool,
    /// Wakes the registers waiting for their next message.
    arrived: Arc<Notify>,
    cut: Arc<Mutex<HashSet<Uuid>>>,
}

impl Transport for Deliveries {
    async fn send(&self, register: Uuid, message: Vec<u8>, id: &str) -> Result<(), Error> {
        if self.is_cut(register) {
            // A retry may get it through: the network's failure, not the
            // message's.
            return Err(Error::Transport {
                reason: "the line to it is cut".to_owned(),
                transient: true,
            });
        }
        // The same message sent twice is queued once: `id` names it.
        sqlx::query(
            "INSERT INTO shop.deliveries (register_id, id, message) VALUES ($1, $2, $3) \
             ON CONFLICT (register_id, id) DO NOTHING",
        )
        .bind(register)
        .bind(id)
        .bind(message)
        .execute(&self.pool)
        .await?;
        self.arrived.notify_waiters();
        Ok(())
    }
}
// ANCHOR_END: transport

impl Deliveries {
    fn is_cut(&self, register: Uuid) -> bool {
        self.cut.lock().unwrap().contains(&register)
    }
}

type ShopServer = Server<Anyone, Deliveries>;

#[derive(Clone)]
struct App {
    pool: PgPool,
    server: Arc<ShopServer>,
    deliveries: Deliveries,
    /// Becomes `true` when the head office is stopping.
    stopping: watch::Receiver<bool>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info,sqlx=warn".to_owned()))
        .init();
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://shop:shop@localhost:5433/shop".to_owned());
    let pool = PgPool::connect(&url).await?;
    sqlx::raw_sql(include_str!("../schema.sql"))
        .execute(&pool)
        .await?;

    // ANCHOR: build
    let deliveries = Deliveries {
        pool: pool.clone(),
        arrived: Arc::new(Notify::new()),
        cut: Arc::default(),
    };
    let mut settings = Settings::default();
    settings.idle = Duration::from_millis(200);
    // A fix deployed (a register restarted without its bug) gets through
    // within seconds, not the hour a real server waits.
    settings.retry_every = Duration::from_secs(10);
    // No migrations of its own: the engine sets up what the database lacks.
    let server = Arc::new(
        Server::install(
            pool.clone(),
            codecs(),
            REGISTERS,
            deliveries.clone(),
            Anyone(pool.clone()),
            settings,
        )
        .await?,
    );
    let (stop, stopping) = watch::channel(false);
    let relay = tokio::spawn({
        let server = server.clone();
        let stopping = stopping.clone();
        async move { server.run(stopped(stopping)).await }
    });
    // ANCHOR_END: build

    let app = App {
        pool: pool.clone(),
        server,
        deliveries,
        stopping,
    };
    let router = Router::new()
        .route("/api/state", get(state))
        .route("/api/sync/overview", get(overview))
        .route("/api/sync/replicas", get(listing))
        .route("/api/sync/progress/{register}", get(progress))
        .route("/api/sync/rows/{table}/{key}", get(row))
        .route("/api/products", post(add_product))
        .route(
            "/api/products/{id}",
            put(edit_product).delete(delete_product),
        )
        .route("/api/registers", post(register))
        .route("/api/registers/{register}/line", post(line))
        .route("/sync/{register}/next", get(next))
        .route("/sync/{register}/ack/{id}", post(ack))
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

    // ANCHOR: shutdown
    // On Ctrl-C: no new request, the registers waiting for a message are
    // answered, and the relay stops — a pass it cuts short runs again.
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            interrupted().await;
            stop.send_replace(true);
        })
        .await?;
    relay.await?;
    pool.close().await;
    // ANCHOR_END: shutdown
    Ok(())
}

/// Resolves once the head office is stopping.
async fn stopped(mut stopping: watch::Receiver<bool>) {
    // An error is the sender gone, which only happens on the way out too.
    let _ = stopping.wait_for(|stopping| *stopping).await;
}

/// Resolves on Ctrl-C; never, where it cannot be listened for.
async fn interrupted() {
    if let Err(e) = tokio::signal::ctrl_c().await {
        tracing::error!("Ctrl-C cannot be listened for ({e}): stop the process to stop it");
        std::future::pending::<()>().await;
    }
}

/// Where the page is built to.
const PAGE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../web/dist");

async fn unbuilt() -> Html<&'static str> {
    Html(
        "<p>The page is not built yet: in <code>examples/shop/web</code>, run \
         <code>pnpm install && pnpm build</code>, then reload.</p>",
    )
}

// ANCHOR: failure
/// A failure, answered with the status that tells the caller whether asking
/// again may help: 503 when the database (or what the engine waits on) is
/// away, 500 when the same request would fail the same way.
struct Failure(StatusCode, String);

impl From<Error> for Failure {
    fn from(e: Error) -> Self {
        let status = if e.is_transient() {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        };
        Failure(status, e.to_string())
    }
}

impl From<sqlx::Error> for Failure {
    fn from(e: sqlx::Error) -> Self {
        Error::from(e).into()
    }
}

impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        (self.0, self.1).into_response()
    }
}
// ANCHOR_END: failure

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

async fn state(State(app): State<App>) -> Result<Json<ShopState>, Failure> {
    let products = sqlx::query_as("SELECT * FROM shop.products ORDER BY name")
        .fetch_all(&app.pool)
        .await?;
    let receipts = sqlx::query_as("SELECT * FROM shop.receipts ORDER BY created_at DESC LIMIT 20")
        .fetch_all(&app.pool)
        .await?;
    let mut registers: Vec<RegisterState> =
        sqlx::query_as("SELECT id, name, url FROM shop.registers ORDER BY name")
            .fetch_all(&app.pool)
            .await?;
    for register in &mut registers {
        register.cut = app.deliveries.is_cut(register.id);
    }
    Ok(Json(ShopState {
        products,
        receipts,
        registers,
    }))
}

// ANCHOR: dashboard
// What the dashboard reads: the engine's answers, as they are. A shop with a
// login would ask them over the registers the caller may see
// (`Scope::Replicas`); this one has none, so every register. Each read is one
// snapshot of its own, so each takes a connection, not a transaction.

async fn overview(State(app): State<App>) -> Result<Json<Overview>, Failure> {
    let mut conn = app.pool.acquire().await?;
    let overview = app
        .server
        .engine()
        .overview(&mut conn, Scope::All, 20)
        .await?;
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
) -> Result<Json<Listing>, Failure> {
    let mut conn = app.pool.acquire().await?;
    let listing = app
        .server
        .engine()
        .listing(&mut conn, Scope::All, page.state, page.offset, 20)
        .await?;
    Ok(Json(listing))
}

async fn progress(
    State(app): State<App>,
    Path(register): Path<Uuid>,
) -> Result<Json<Progress>, Failure> {
    let mut conn = app.pool.acquire().await?;
    let progress = app
        .server
        .engine()
        .progress(&mut conn, Scope::Replicas(&[register]))
        .await?;
    Ok(Json(progress))
}

async fn row(
    State(app): State<App>,
    Path((table, key)): Path<(String, String)>,
) -> Result<Json<RowProgress>, Failure> {
    let mut conn = app.pool.acquire().await?;
    let places = app
        .server
        .engine()
        .row_progress(&mut conn, Scope::All, &table, &key)
        .await
        .map_err(|e| {
            if e.is_transient() {
                Failure::from(e)
            } else {
                // A table that does not travel, a key that is not one: the
                // caller's.
                Failure(StatusCode::BAD_REQUEST, e.to_string())
            }
        })?;
    Ok(Json(places))
}
// ANCHOR_END: dashboard

#[derive(Deserialize)]
struct ProductForm {
    name: String,
    price_cents: i64,
}

async fn add_product(
    State(app): State<App>,
    Json(form): Json<ProductForm>,
) -> Result<StatusCode, Failure> {
    // The clock at the write, never `now()`: a version only moves forward.
    sqlx::query("INSERT INTO shop.products VALUES ($1, $2, $3, clock_timestamp())")
        .bind(Uuid::now_v7())
        .bind(form.name)
        .bind(form.price_cents)
        .execute(&app.pool)
        .await?;
    Ok(StatusCode::CREATED)
}

async fn edit_product(
    State(app): State<App>,
    Path(id): Path<Uuid>,
    Json(form): Json<ProductForm>,
) -> Result<StatusCode, Failure> {
    sqlx::query(
        "UPDATE shop.products SET name = $2, price_cents = $3, updated_at = clock_timestamp() \
         WHERE id = $1",
    )
    .bind(id)
    .bind(form.name)
    .bind(form.price_cents)
    .execute(&app.pool)
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_product(
    State(app): State<App>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, Failure> {
    sqlx::query("DELETE FROM shop.products WHERE id = $1")
        .bind(id)
        .execute(&app.pool)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct RegisterForm {
    id: Uuid,
    name: String,
    url: String,
}

/// A register introduces itself. The engine sees it live on its next pass,
/// and provisions it: sends it everything it should hold.
async fn register(
    State(app): State<App>,
    Json(form): Json<RegisterForm>,
) -> Result<StatusCode, Failure> {
    sqlx::query(
        "INSERT INTO shop.registers (id, name, url) VALUES ($1, $2, $3) \
         ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, url = EXCLUDED.url",
    )
    .bind(form.id)
    .bind(&form.name)
    .bind(&form.url)
    .execute(&app.pool)
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Cuts the line to a register, or mends it: a demo of a replica the server
/// cannot reach.
async fn line(State(app): State<App>, Path(register): Path<Uuid>) -> StatusCode {
    let mut cut = app.deliveries.cut.lock().unwrap();
    if !cut.remove(&register) {
        cut.insert(register);
    }
    StatusCode::NO_CONTENT
}

// ANCHOR: next
/// The register's next message, named in the `message-id` header, and left
/// at the head of its queue until the register acknowledges it. When there is
/// none, the register is answered once one arrives, or after a while that
/// there is none (204).
async fn next(State(app): State<App>, Path(register): Path<Uuid>) -> Result<Response, Failure> {
    if app.deliveries.is_cut(register) {
        return Ok(StatusCode::SERVICE_UNAVAILABLE.into_response());
    }
    let deadline = tokio::time::Instant::now() + WAIT_FOR_NEXT;
    loop {
        // Listening before looking: a message queued in between still wakes
        // this request.
        let arrived = app.deliveries.arrived.notified();
        tokio::pin!(arrived);
        arrived.as_mut().enable();
        let head: Option<(String, Vec<u8>)> = sqlx::query_as(
            "SELECT id, message FROM shop.deliveries WHERE register_id = $1 ORDER BY seq LIMIT 1",
        )
        .bind(register)
        .fetch_optional(&app.pool)
        .await?;
        if let Some((id, message)) = head {
            return Ok(([(MESSAGE_ID, id)], message).into_response());
        }
        tokio::select! {
            () = arrived => {}
            () = tokio::time::sleep_until(deadline) => break,
            () = stopped(app.stopping.clone()) => break,
        }
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// The register applied the message `id` and committed it: it leaves the
/// queue, and the next one is given. An acknowledgement names its message, so
/// one that comes twice, or late, never takes another message out.
async fn ack(
    State(app): State<App>,
    Path((register, id)): Path<(Uuid, String)>,
) -> Result<StatusCode, Failure> {
    sqlx::query("DELETE FROM shop.deliveries WHERE register_id = $1 AND id = $2")
        .bind(register)
        .bind(id)
        .execute(&app.pool)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
// ANCHOR_END: next

// ANCHOR: up
/// What a register sends up, filed under the register the path names, and
/// answered once filed. An error is one worth sending again (503): the
/// engine keeps whatever a resend could not cure, and answers it.
async fn up(
    State(app): State<App>,
    Path(register): Path<Uuid>,
    body: Bytes,
) -> Result<StatusCode, Failure> {
    if app.deliveries.is_cut(register) {
        return Err(Failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "the line is cut".to_owned(),
        ));
    }
    app.server.receive(register, &body).await?;
    Ok(StatusCode::NO_CONTENT)
}
// ANCHOR_END: up
