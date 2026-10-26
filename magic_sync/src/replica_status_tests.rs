//! What a replica tells of itself: read from its own database as messages
//! come and uploads go, and judged in one word.

use super::*;
use crate::replica::{Link, Receiving, Standing, WishAnswer};
use std::time::Duration;

const PATIENCE: Duration = Duration::from_secs(600);

impl World {
    async fn status(&self) -> ReplicaStatus {
        status(&mut self.pool.acquire().await.unwrap())
            .await
            .expect("a status")
    }

    async fn standing(&self) -> Standing {
        self.status().await.standing(Link::Up, Utc::now(), PATIENCE)
    }

    async fn write_event(&self) {
        sqlx::query("INSERT INTO events (id, what) VALUES ($1, 'written')")
            .bind(Uuid::now_v7())
            .execute(&self.pool)
            .await
            .unwrap();
    }
}

/// Provisioned and with nothing on its way, a replica is synced, knows
/// which it is, and when it heard the server.
#[sqlx::test(migrations = false)]
async fn a_provisioned_replica_is_synced(pool: SqlitePool) {
    let w = world(pool).await;
    let status = w.status().await;
    assert_eq!(status.replica, Some(w.me));
    assert_eq!(status.receiving, Receiving::Following);
    assert!(status.heard_at.is_some() && status.applied_at.is_some());
    assert_eq!(status.filed_at, None, "nothing filed yet");
    assert!(status.up.is_empty() && status.refused.is_empty() && status.wishes.is_empty());
    assert_eq!(w.standing().await, Standing::Synced);
}

/// A database the engine is installed on, before a replica first runs
/// knowing which it is: not linked, and nothing received.
#[sqlx::test(migrations = false)]
async fn a_replica_that_does_not_know_which_it_is_is_not_linked(pool: SqlitePool) {
    let pool = settled(pool).await;
    sqlx::raw_sql(TABLES).execute(&pool).await.unwrap();
    install(&mut pool.acquire().await.unwrap(), &codecs())
        .await
        .unwrap();
    let status = status(&mut pool.acquire().await.unwrap()).await.unwrap();
    assert_eq!(status.replica, None);
    assert_eq!(status.receiving, Receiving::Unprovisioned);
    assert_eq!(status.heard_at, None);
    assert_eq!(
        status.standing(Link::Up, Utc::now(), PATIENCE),
        Standing::NotLinked
    );
}

/// A write is on its way until the server files it: unsent, then sent and
/// waiting — for longer than the host's patience, not answered — and once
/// filed, synced, with when it was.
#[sqlx::test(migrations = false)]
async fn a_write_catches_up_until_the_server_files_it(pool: SqlitePool) {
    let w = world(pool).await;
    w.write_event().await;
    let written = w.status().await;
    assert_eq!(
        (written.up[0].table.as_str(), written.up[0].unsent),
        ("events", 1)
    );
    assert_eq!(written.unfiled_since, None, "not sent yet");
    assert_eq!(w.standing().await, Standing::CatchingUp);

    let up = w.up(0, false).await.expect("the write");
    let sent = w.status().await;
    assert_eq!((sent.up[0].unsent, sent.up[0].unfiled), (0, 1));
    let since = sent.unfiled_since.expect("waiting since it was sent");
    assert!(sent.spoken_at.is_some());
    assert_eq!(
        sent.standing(Link::Up, since + chrono::Duration::seconds(1), PATIENCE),
        Standing::CatchingUp,
        "within patience"
    );
    assert_eq!(
        sent.standing(Link::Up, since + chrono::Duration::hours(1), PATIENCE),
        Standing::NotAnswered
    );
    assert_eq!(
        sent.standing(Link::Unknown, since + chrono::Duration::hours(1), PATIENCE),
        Standing::NotAnswered,
        "a link the host cannot tell is not taken for down"
    );
    assert_eq!(
        sent.standing(Link::Down, since + chrono::Duration::hours(1), PATIENCE),
        Standing::Offline,
        "the host's link down says why"
    );

    w.receive(
        &Body::Filed(Filed {
            uploads: up.uploads.clone(),
        })
        .encoded(),
    )
    .await;
    let filed = w.status().await;
    assert!(filed.up.is_empty());
    assert_eq!(filed.unfiled_since, None);
    assert!(filed.filed_at.is_some());
    assert_eq!(w.standing().await, Standing::Synced);
}

/// A row the replica could not take waits for a fix, whatever the link,
/// counted per table with why — reported once the server is told.
#[sqlx::test(migrations = false)]
async fn a_row_the_replica_could_not_take_awaits_a_fix(pool: SqlitePool) {
    let w = world(pool).await;
    let [good, broken] = ids();
    w.receive(&down(
        1,
        "",
        vec![upserts(
            "items",
            vec![
                item(good, "good"),
                (key(&[&broken.to_string()]), vec![0xff, 0xff]),
            ],
        )],
    ))
    .await;
    let status = w.status().await;
    assert_eq!(status.refused.len(), 1);
    let refused = &status.refused[0];
    assert_eq!((refused.table.as_str(), refused.rows), ("items", 1));
    assert!(!refused.reason.is_empty() && refused.since.is_some());
    assert!(!refused.reported);
    for link in [Link::Up, Link::Down, Link::Unknown] {
        assert_eq!(
            status.standing(link, Utc::now(), PATIENCE),
            Standing::AwaitingFix
        );
    }
}

/// What this build cannot send waits for a build that can: a fix.
#[sqlx::test(migrations = false)]
async fn a_write_this_build_cannot_send_awaits_a_fix(pool: SqlitePool) {
    let w = world(pool).await;
    w.write_event().await;
    w.exec("UPDATE sync_notes SET set_aside = 'a later build sends it'")
        .await
        .unwrap();
    let status = w.status().await;
    assert_eq!(status.set_aside.len(), 1);
    assert_eq!(
        (
            status.set_aside[0].table.as_str(),
            status.set_aside[0].rows,
            status.set_aside[0].reason.as_str()
        ),
        ("events", 1, "a later build sends it")
    );
    assert_eq!(w.standing().await, Standing::AwaitingFix);
}

/// A replica that missed a message waits for the server to send everything
/// again, and so does one whose snapshot is in progress: resyncing, until
/// the snapshot ends.
#[sqlx::test(migrations = false)]
async fn a_missed_message_or_a_snapshot_is_resyncing(pool: SqlitePool) {
    let w = world(pool).await;
    let [a, b] = ids();
    w.receive(&down(2, "", vec![upserts("items", vec![item(a, "a")])]))
        .await;
    let missed = w.status().await;
    assert_eq!(missed.receiving, Receiving::Missed);
    assert_eq!(w.standing().await, Standing::Resyncing);

    let s = Uuid::now_v7().to_string();
    w.receive(&begin(3, &s, vec![upserts("items", vec![item(b, "b")])]))
        .await;
    assert_eq!(w.status().await.receiving, Receiving::Snapshot);
    assert_eq!(w.standing().await, Standing::Resyncing);
    w.receive(&end(4, &s, &["items"])).await;
    assert_eq!(w.status().await.receiving, Receiving::Following);
    assert_eq!(w.standing().await, Standing::Synced);
}

/// A message rolled back to come again was not heard: the stamps are made
/// in the transaction the message is acted on in.
#[sqlx::test(migrations = false)]
async fn heard_is_when_a_message_was_read(pool: SqlitePool) {
    let w = world(pool).await;
    w.exec("DELETE FROM sync_heard").await.unwrap();
    assert!(matches!(
        w.receive(b"not a message").await,
        Received::Malformed(_)
    ));
    assert_eq!(w.status().await.heard_at, None, "bytes that are no message");
    let a = Uuid::now_v7();
    w.receive(&down(1, "", vec![upserts("items", vec![item(a, "a")])]))
        .await;
    let status = w.status().await;
    assert!(status.heard_at.is_some() && status.applied_at.is_some());
    w.receive(&down(1, "", vec![])).await;
    let again = w.status().await;
    assert!(again.heard_at >= status.heard_at, "a redelivery is heard");
    assert_eq!(again.applied_at, status.applied_at, "and applies nothing");
}

/// A replica of version 3 is brought to 4 having heard nothing it
/// recorded: what it hears from then on is.
#[sqlx::test(migrations = false)]
async fn a_replica_of_version_3_has_heard_nothing_yet(pool: SqlitePool) {
    let pool = settled(pool).await;
    let mut conn = pool.acquire().await.unwrap();
    for step in 1..=3 {
        let step = migration(step).expect("a released step");
        sqlx::raw_sql(&step).execute(&mut *conn).await.unwrap();
    }
    sqlx::raw_sql(TABLES).execute(&mut *conn).await.unwrap();
    install(&mut conn, &codecs()).await.expect("upgraded");
    let status = status(&mut conn).await.unwrap();
    assert_eq!(
        (status.heard_at, status.applied_at, status.filed_at),
        (None, None, None)
    );
}

/// Facts judged, worst first: each standing wins over every one after it.
#[test]
fn the_worst_that_applies_is_the_standing() {
    let now = Utc::now();
    let synced = ReplicaStatus {
        replica: Some(Uuid::now_v7()),
        up: Vec::new(),
        unfiled_since: None,
        spoken_at: None,
        heard_at: None,
        applied_at: None,
        filed_at: None,
        receiving: Receiving::Following,
        refused: Vec::new(),
        set_aside: Vec::new(),
        wishes: Vec::new(),
    };
    let wish = |state| HeldWish {
        table: "screens".into(),
        key: "1".into(),
        state,
        reason: None,
        note: None,
    };
    let judged = |status: &ReplicaStatus, link| status.standing(link, now, PATIENCE);
    assert_eq!(judged(&synced, Link::Up), Standing::Synced);
    assert_eq!(judged(&synced, Link::Down), Standing::Offline);

    let wished = ReplicaStatus {
        wishes: vec![wish(WishAnswer::Pending)],
        ..synced.clone()
    };
    assert_eq!(judged(&wished, Link::Up), Standing::CatchingUp);
    for settled in [WishAnswer::Refused, WishAnswer::Superseded] {
        let answered = ReplicaStatus {
            wishes: vec![wish(settled)],
            ..synced.clone()
        };
        assert_eq!(
            judged(&answered, Link::Up),
            Standing::Synced,
            "an answer is no change on its way"
        );
    }
    let resyncing = ReplicaStatus {
        receiving: Receiving::Snapshot,
        ..wished.clone()
    };
    assert_eq!(judged(&resyncing, Link::Up), Standing::Resyncing);
    let unanswered = ReplicaStatus {
        unfiled_since: Some(now - chrono::Duration::hours(1)),
        ..resyncing.clone()
    };
    assert_eq!(judged(&unanswered, Link::Up), Standing::NotAnswered);
    assert_eq!(judged(&unanswered, Link::Down), Standing::Offline);
    let broken = ReplicaStatus {
        set_aside: vec![SetAside {
            table: "events".into(),
            rows: 1,
            reason: "later".into(),
        }],
        ..unanswered.clone()
    };
    assert_eq!(judged(&broken, Link::Down), Standing::AwaitingFix);
    let unlinked = ReplicaStatus {
        replica: None,
        ..broken
    };
    assert_eq!(judged(&unlinked, Link::Down), Standing::NotLinked);
    assert!(
        Standing::NotLinked < Standing::Synced,
        "ordered worst first"
    );
    assert_eq!(Standing::NotAnswered.as_str(), "not_answered");
    assert_eq!(
        serde_json::to_value(Standing::AwaitingFix).unwrap(),
        "awaiting_fix",
        "the word is the JSON's"
    );
}
