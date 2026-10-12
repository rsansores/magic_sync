//! The server's side.

pub mod partitioned;

use magic_sync::envelope::Limits;
use magic_sync::plan::Engine;
use magic_sync::run::server::{Senders, Settings};
use magic_sync::status::{ReplicaState, RowState, Scope};
use magic_sync::Error;
use magic_sync::Row;
use shop_rows::{codecs, Product, REGISTERS};
use sqlx::{PgPool, Postgres, Transaction};
use std::fmt::Display;
use uuid::Uuid;

// ANCHOR: triggers
pub async fn print_triggers(pool: &PgPool) -> Result<(), Error> {
    let mut conn = pool.acquire().await?;
    println!(
        "{}",
        Engine::triggers(&mut conn, &codecs(), REGISTERS).await?
    );
    println!("{}", Engine::indexes(&mut conn, &codecs(), REGISTERS).await?);
    Ok(())
}
// ANCHOR_END: triggers

// ANCHOR: transport_errors
/// The network: a retry may get the message through.
pub fn unreachable(e: impl Display) -> Error {
    Error::Transport {
        reason: e.to_string(),
        transient: true,
    }
}

/// This message, never: the transport will not take it.
pub fn refused(e: impl Display) -> Error {
    Error::Transport {
        reason: e.to_string(),
        transient: false,
    }
}
// ANCHOR_END: transport_errors

// ANCHOR: settings
pub fn settings() -> Result<Settings, Error> {
    let mut settings = Settings::default();
    settings.limits = Limits::new(200, 256 * 1024)?; // rows and bytes your transport takes
    settings.role = Some("sync_engine".to_owned());
    Ok(settings)
}
// ANCHOR_END: settings

// ANCHOR: senders
/// What the server knows of a register that sends something up: its company.
pub struct Company(pub Uuid);

pub struct Companies(pub PgPool);

impl Senders for Companies {
    type Known = Company; // the `P` of `Codec<P>`

    async fn known(&self, register: Uuid) -> Result<Option<Company>, Error> {
        let company = sqlx::query_scalar("SELECT company_id FROM shop.registers WHERE id = $1")
            .bind(register)
            .fetch_optional(&self.0)
            .await?;
        Ok(company.map(Company))
    }

    async fn filing(&self, company: &Company) -> Result<Transaction<'static, Postgres>, Error> {
        let mut tx = self.0.begin().await?;
        // What the row policies read: who is writing.
        sqlx::query("SELECT set_config('shop.company', $1, true)")
            .bind(company.0.to_string())
            .execute(&mut *tx)
            .await?;
        Ok(tx)
    }
}
// ANCHOR_END: senders

// ANCHOR: resync
/// An operator's "send it everything again".
pub async fn send_everything_again(pool: &PgPool, register: Uuid) -> Result<(), Error> {
    let mut tx = pool.begin().await?;
    magic_sync::plan::note_resync(&mut tx, None, register).await?;
    tx.commit().await?;
    Ok(())
}
// ANCHOR_END: resync

// ANCHOR: overview
pub async fn overview(engine: &Engine, pool: &PgPool, customer: Uuid) -> Result<(), Error> {
    let mut conn = pool.acquire().await?;
    let overview = engine
        .overview(&mut conn, Scope::Partition(customer), 20)
        .await?;
    // Whether nothing waits for a fix.
    println!("healthy: {}", overview.health.healthy);
    // How many replicas are in each state.
    println!(
        "{} synced, {} catching up",
        overview.states.synced, overview.states.catching_up
    );
    // The 20 that need attention most, worst first.
    for r in &overview.replicas.first {
        let why = match r.state {
            ReplicaState::Synced => "holds what it should; all it wrote is filed".to_owned(),
            ReplicaState::CatchingUp => format!("{} changes on their way", r.pending),
            ReplicaState::Unreachable => format!("{} sends failed: {:?}", r.failures, r.last_error),
            ReplicaState::AwaitingFix => format!("{} rows set aside", r.quarantined),
            _ => "a state of a later version".to_owned(),
        };
        println!("{}: {why}", r.replica);
    }
    // Which tables are not where they should be yet, and what they wait on.
    for t in overview.tables.iter().filter(|t| !t.synced) {
        println!(
            "{}: {} pending, {} in flight, {} set aside, {} not filed",
            t.table, t.pending, t.in_flight, t.quarantined, t.unfiled
        );
    }
    Ok(())
}
// ANCHOR_END: overview

// ANCHOR: progress
pub async fn progress(engine: &Engine, pool: &PgPool, register: Uuid) -> Result<(), Error> {
    let mut conn = pool.acquire().await?;
    let progress = engine
        .progress(&mut conn, Scope::Replicas(&[register]))
        .await?;
    for register in progress.replicas {
        for t in register.tables {
            println!(
                "{}: {} of {} ({} on their way, {} set aside)",
                t.table,
                t.holds - t.outdated,
                t.should,
                t.in_flight,
                t.quarantined
            );
        }
    }
    Ok(())
}
// ANCHOR_END: progress

// ANCHOR: row
pub async fn who_holds(engine: &Engine, pool: &PgPool, product: &Product) -> Result<(), Error> {
    let mut conn = pool.acquire().await?;
    let row = engine
        .row_progress(&mut conn, Scope::All, "products", &product.key_text())
        .await?;
    for place in row.places {
        let state = match place.state {
            RowState::Holds => "holds it",
            RowState::Outdated => "holds an older version",
            RowState::OnItsWay => "has it on its way",
            RowState::Missing => "lacks it",
            RowState::Leaving => "is letting it go",
            RowState::Quarantined => "has it set aside",
            _ => "is in a state of a later version",
        };
        println!("{} {state}", place.replica);
    }
    Ok(())
}
// ANCHOR_END: row
