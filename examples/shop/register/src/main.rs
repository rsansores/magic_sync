//! A register: SQLite, its own page, and the sync replica. It sells from the
//! products it holds and keeps selling while it is unplugged; its receipts go
//! up when it is plugged back in.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, Json};
use axum::routing::{get, post};
use axum::Router;
use magic_sync::replica::{self, Received};
use magic_sync::run::replica::{Replica, Settings, Uplink};
use magic_sync::Error;
use serde::{Deserialize, Serialize};
use shop_rows::{codecs, Product};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use uuid::Uuid;

/// How long a register waits after the head office failed to answer.
const AFTER_A_FAILURE: Duration = Duration::from_secs(1);

// ANCHOR: uplink
/// The way up: a POST to the head office, unless the register is unplugged.
struct Http {
    client: reqwest::Client,
    server: String,
    online: watch::Receiver<bool>,
}

impl Uplink for Http {
    async fn send(
        &self,
        register: Uuid,
        _lane: &'static str,
        message: Vec<u8>,
    ) -> Result<(), Error> {
        if !*self.online.borrow() {
            return Err(unreachable("unplugged"));
        }
        let response = self
            .client
            .post(format!("{}/sync/{register}/up", self.server))
            .body(message)
            .send()
            .await
            .map_err(unreachable)?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        // A server error, or one that asks to come back later, may go
        // through next time; any other refusal is of this message, for good.
        Err(Error::Transport {
            reason: format!("the head office answered {status}"),
            transient: status.is_server_error()
                || status == reqwest::StatusCode::TOO_MANY_REQUESTS
                || status == reqwest::StatusCode::REQUEST_TIMEOUT,
        })
    }
}

/// The head office out of reach: a retry may get through.
fn unreachable(e: impl std::fmt::Display) -> Error {
    Error::Transport {
        reason: e.to_string(),
        transient: true,
    }
}
// ANCHOR_END: uplink

type ShopReplica = Replica<(), Uuid, Http>;

#[derive(Clone)]
struct App {
    pool: SqlitePool,
    replica: Arc<ShopReplica>,
    me: Uuid,
    name: String,
    online: Arc<watch::Sender<bool>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info,sqlx=warn".to_owned()))
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

    // ANCHOR: pool
    let pool = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&file)
            .create_if_missing(true)
            .foreign_keys(true)
            .pragma("recursive_triggers", "ON"),
    )
    .await?;
    // ANCHOR_END: pool
    sqlx::raw_sql(include_str!("../schema.sql"))
        .execute(&pool)
        .await?;
    // `--refuse <product>`: this build has a bug that refuses one product, to
    // see a row set aside until a fix. Started again without it, the fix is
    // deployed, and the head office's next retry gets the product through.
    sqlx::query("DROP TRIGGER IF EXISTS a_bug")
        .execute(&pool)
        .await?;
    let refuse = arg("--refuse", "");
    if !refuse.is_empty() {
        let name = refuse.replace('\'', "''");
        sqlx::raw_sql(&format!(
            "CREATE TRIGGER a_bug BEFORE INSERT ON products WHEN NEW.name = '{name}' \
             BEGIN SELECT RAISE(ABORT, 'this build cannot take \"{name}\"'); END"
        ))
        .execute(&pool)
        .await?;
    }
    let me: Uuid = match sqlx::query_scalar("SELECT id FROM me")
        .fetch_optional(&pool)
        .await?
    {
        Some(id) => id,
        None => {
            let id = Uuid::now_v7();
            sqlx::query("INSERT INTO me VALUES ($1)")
                .bind(id)
                .execute(&pool)
                .await?;
            id
        }
    };

    // ANCHOR: build
    // No migrations of its own: the engine sets up what the database lacks,
    // including the trigger that notes every receipt written.
    replica::install(&mut *pool.acquire().await?, &codecs()).await?;

    let (online, plugged) = watch::channel(true);
    let client = reqwest::Client::new();
    let mut settings = Settings::default();
    settings.idle = Duration::from_millis(300);
    let replica = Arc::new(
        Replica::new(
            pool.clone(),
            codecs(),
            me,
            Http {
                client: client.clone(),
                server: server.clone(),
                online: plugged.clone(),
            },
            settings,
        )
        .await?,
    );
    let (stop, stopping) = watch::channel(false);
    let up = tokio::spawn({
        let replica = replica.clone();
        let stopping = stopping.clone();
        async move { replica.run(stopped(stopping)).await }
    });
    let down = tokio::spawn(receive(
        client.clone(),
        server.clone(),
        me,
        replica.clone(),
        plugged,
        stopping,
    ));
    // ANCHOR_END: build

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
        tokio::time::sleep(AFTER_A_FAILURE).await;
    }

    let app = App {
        pool: pool.clone(),
        replica,
        me,
        name,
        online: Arc::new(online),
    };
    let router = Router::new()
        .route("/", get(|| async { Html(include_str!("index.html")) }))
        .route("/api/state", get(state))
        .route("/api/sell", post(sell))
        .route("/api/plug", post(plug))
        .with_state(app);
    let listener = tokio::net::TcpListener::bind(format!("{bind}:{port}")).await?;
    tracing::info!("this register is at {url}");
    // On Ctrl-C: no new request, the message being applied is committed, and
    // the way up ends what it is sending.
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            interrupted().await;
            stop.send_replace(true);
        })
        .await?;
    up.await?;
    down.await?;
    pool.close().await;
    Ok(())
}

/// Resolves once the register is stopping.
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

// ANCHOR: receive
/// Takes the head office's messages one at a time: each applied and
/// committed, then acknowledged by its id — never before.
async fn receive(
    client: reqwest::Client,
    server: String,
    me: Uuid,
    replica: Arc<ShopReplica>,
    plugged: watch::Receiver<bool>,
    stopping: watch::Receiver<bool>,
) {
    loop {
        let next = tokio::select! {
            next = next_while_plugged(&client, &server, me, plugged.clone()) => next,
            () = stopped(stopping.clone()) => return,
        };
        let (id, message) = match next {
            Ok(Some(next)) => next,
            Ok(None) => continue,
            Err(e) => {
                tracing::warn!("no message from the head office: {e}");
                tokio::time::sleep(AFTER_A_FAILURE).await;
                continue;
            }
        };
        match replica.receive(&message).await {
            Ok(received) => {
                if let Received::Applied(tables) = &received {
                    tracing::info!(?tables, "applied");
                }
                // Lost, the acknowledgement is harmless: the message comes
                // again, and changes nothing the second time.
                let acked = client
                    .post(format!("{server}/sync/{me}/ack/{id}"))
                    .send()
                    .await
                    .and_then(reqwest::Response::error_for_status);
                if let Err(e) = acked {
                    tracing::warn!("not acknowledged, and comes again: {e}");
                }
            }
            Err(e) => {
                // Not acknowledged: it comes again.
                tracing::warn!("not applied, and comes again: {e}");
                tokio::time::sleep(AFTER_A_FAILURE).await;
            }
        }
    }
}

/// The next message and its id, once the register is plugged in; `None` when
/// the head office had none for a while, or the register was unplugged
/// while it waited.
async fn next_while_plugged(
    client: &reqwest::Client,
    server: &str,
    me: Uuid,
    mut plugged: watch::Receiver<bool>,
) -> Result<Option<(String, Vec<u8>)>, String> {
    // An error is the sender gone: the register is stopping.
    let _ = plugged.wait_for(|plugged| *plugged).await;
    tokio::select! {
        next = take(client, server, me) => next,
        _ = plugged.wait_for(|plugged| !*plugged) => Ok(None),
    }
}

/// Asks the head office for the next message, which it answers as soon as
/// there is one.
async fn take(
    client: &reqwest::Client,
    server: &str,
    me: Uuid,
) -> Result<Option<(String, Vec<u8>)>, String> {
    let response = client
        .get(format!("{server}/sync/{me}/next"))
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|e| e.to_string())?;
    if response.status() == reqwest::StatusCode::NO_CONTENT {
        return Ok(None);
    }
    let id = response
        .headers()
        .get("message-id")
        .and_then(|id| id.to_str().ok())
        .ok_or("a message without an id")?
        .to_owned();
    let message = response.bytes().await.map_err(|e| e.to_string())?;
    Ok(Some((id, message.to_vec())))
}
// ANCHOR_END: receive

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
    // ANCHOR: progress
    let mut conn = app.pool.acquire().await.map_err(fail)?;
    let up = replica::progress(&mut conn)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let unsent = up.iter().map(|t| t.unsent).sum();
    let unfiled = up.iter().map(|t| t.unfiled).sum();
    // ANCHOR_END: progress
    Ok(Json(RegisterState {
        name: app.name.clone(),
        online: *app.online.borrow(),
        products,
        sold,
        unsent,
        unfiled,
    }))
}

#[derive(Deserialize)]
struct Sale {
    product_id: Uuid,
}

/// A sale: a receipt written here. The engine's trigger notes it, and it goes
/// up as soon as the register can reach the head office.
async fn sell(
    State(app): State<App>,
    Json(sale): Json<Sale>,
) -> Result<StatusCode, (StatusCode, String)> {
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
    app.online.send_modify(|online| *online = !*online);
    app.replica.nudge();
    StatusCode::NO_CONTENT
}
