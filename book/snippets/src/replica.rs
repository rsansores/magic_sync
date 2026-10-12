//! A replica's side.

use magic_sync::replica::Lane;
use magic_sync::run::replica::Settings;
use shop_rows::codecs;
use sqlx::SqlitePool;
use uuid::Uuid;

// ANCHOR: triggers
pub fn print_triggers() {
    println!("{}", magic_sync::replica::triggers(&codecs()));
}
// ANCHOR_END: triggers

// ANCHOR: lanes
pub fn settings() -> Settings {
    let mut settings = Settings::default();
    settings.lanes = vec![
        Lane::new("main", &[]), // everything else, and what the engine says
        Lane::new("telemetry", &["positions"]),
    ];
    settings
}
// ANCHOR_END: lanes

// ANCHOR: apply_desired
/// Carries out every wish of the head office that is newer than the last one
/// carried out for that setting, and reports what the setting then is.
pub async fn apply_desired(
    pool: &SqlitePool,
    me: Uuid,
    apply: impl Fn(Uuid, &str),
) -> Result<(), sqlx::Error> {
    let wishes: Vec<(Uuid, String, i64)> = sqlx::query_as(
        "SELECT d.id, d.value, d.revision FROM desired_settings d \
           LEFT JOIN applied_settings a ON a.id = d.id \
          WHERE a.revision IS NULL OR a.revision < d.revision",
    )
    .fetch_all(pool)
    .await?;
    for (id, value, revision) in wishes {
        apply(id, &value);
        let mut tx = pool.begin().await?;
        sqlx::query(
            "INSERT INTO applied_settings (id, revision) VALUES ($1, $2) \
             ON CONFLICT (id) DO UPDATE SET revision = excluded.revision",
        )
        .bind(id)
        .bind(revision)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO reported_settings (register_id, id, value, reported_at) \
             VALUES ($1, $2, $3, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) \
             ON CONFLICT (register_id, id) DO UPDATE \
             SET value = excluded.value, reported_at = excluded.reported_at",
        )
        .bind(me)
        .bind(id)
        .bind(&value)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
    }
    Ok(())
}
// ANCHOR_END: apply_desired

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[test]
    fn a_wish_is_carried_out_once_per_revision_and_reported() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect("sqlite::memory:")
                .await
                .unwrap();
            sqlx::raw_sql(
                "CREATE TABLE desired_settings (register_id BLOB NOT NULL, id BLOB NOT NULL,
                     value TEXT NOT NULL, revision INTEGER NOT NULL, PRIMARY KEY (register_id, id));
                 CREATE TABLE reported_settings (register_id BLOB NOT NULL, id BLOB NOT NULL,
                     value TEXT NOT NULL, reported_at TEXT NOT NULL, PRIMARY KEY (register_id, id));
                 CREATE TABLE applied_settings (id BLOB PRIMARY KEY NOT NULL, revision INTEGER NOT NULL);",
            )
            .execute(&pool)
            .await
            .unwrap();
            let (me, setting) = (Uuid::now_v7(), Uuid::now_v7());
            let wish = |value: &str, revision: i64| {
                let value = value.to_owned();
                let pool = pool.clone();
                async move {
                    sqlx::query(
                        "INSERT INTO desired_settings VALUES ($1, $2, $3, $4)
                         ON CONFLICT (register_id, id) DO UPDATE
                         SET value = excluded.value, revision = excluded.revision",
                    )
                    .bind(me)
                    .bind(setting)
                    .bind(value)
                    .bind(revision)
                    .execute(&pool)
                    .await
                    .unwrap();
                }
            };
            let carried_out = RefCell::new(Vec::new());
            let run = || apply_desired(&pool, me, |_, value| carried_out.borrow_mut().push(value.to_owned()));

            wish("70", 1).await;
            run().await.unwrap();
            run().await.unwrap();
            assert_eq!(*carried_out.borrow(), ["70"], "once");
            wish("40", 2).await;
            run().await.unwrap();
            assert_eq!(*carried_out.borrow(), ["70", "40"], "a newer revision");
            let reported: String = sqlx::query_scalar("SELECT value FROM reported_settings")
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(reported, "40");
        });
    }
}
