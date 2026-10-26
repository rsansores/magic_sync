//! What a replica tells of itself: read from its own database as messages
//! come and uploads go, and judged in one word.

use super::*;
use crate::replica::{HeldWishes, Link, Receiving, Standing, Waits, WishState};
use std::time::Duration;

/// The default outbox's waits — ten minutes' resend, twenty of patience —
/// with rows behind once they waited a minute.
fn waits() -> Waits {
    Waits::new(&Outbox::default()).behind_after(Duration::from_secs(60))
}

impl World {
    async fn status(&self) -> ReplicaStatus {
        status(&mut self.pool.acquire().await.unwrap())
            .await
            .expect("a status")
    }

    async fn standing(&self) -> Standing {
        self.standing_at(Utc::now()).await
    }

    async fn standing_at(&self, now: chrono::DateTime<Utc>) -> Standing {
        self.status().await.standing(Link::Up, now, &waits())
    }

    async fn write_event(&self) {
        sqlx::query("INSERT INTO events (id, what) VALUES ($1, 'written')")
            .bind(Uuid::now_v7())
            .execute(&self.pool)
            .await
            .unwrap();
    }

    /// `sql` run as if the engine applied it: no trigger of the engine's
    /// notes or refuses it.
    async fn as_engine(&self, sql: &str) {
        self.exec(&format!(
            "INSERT INTO sync_applying (id) VALUES (1); {sql}; DELETE FROM sync_applying;"
        ))
        .await
        .expect(sql);
    }
}

/// A provisioned replica that has told the server what it applied.
async fn told(pool: SqlitePool) -> World {
    let w = world(pool).await;
    w.up(0, false).await.expect("the provisioning confirmed");
    w
}

fn later(minutes: i64) -> chrono::DateTime<Utc> {
    Utc::now() + chrono::Duration::minutes(minutes)
}

/// Provisioned and with nothing on its way, a replica is synced, knows
/// which it is, and when it heard the server.
#[sqlx::test(migrations = false)]
async fn a_provisioned_replica_is_synced(pool: SqlitePool) {
    let w = told(pool).await;
    let status = w.status().await;
    assert_eq!(status.replica, Some(w.me));
    assert_eq!(status.receiving, Receiving::Following);
    assert!(status.heard_at.is_some() && status.applied_at.is_some());
    assert_eq!(status.filed_at, None, "nothing filed yet");
    assert!(status.up.is_empty() && status.refused.is_empty() && status.wishes.is_empty());
    assert_eq!(w.standing().await, Standing::Synced);
}

/// A database the engine is installed on, before a replica first runs
/// knowing which it is: not linked; and once it runs, until the server sends
/// it anything, not provisioned.
#[sqlx::test(migrations = false)]
async fn a_replica_is_linked_then_provisioned(pool: SqlitePool) {
    let pool = settled(pool).await;
    sqlx::raw_sql(TABLES).execute(&pool).await.unwrap();
    install(&mut pool.acquire().await.unwrap(), &codecs())
        .await
        .unwrap();
    let read = || async { status(&mut pool.acquire().await.unwrap()).await.unwrap() };
    let unlinked = read().await;
    assert_eq!(unlinked.replica, None);
    assert_eq!(unlinked.receiving, Receiving::Unprovisioned);
    assert_eq!(unlinked.heard_at, None);
    assert_eq!(
        unlinked.standing(Link::Up, Utc::now(), &waits()),
        Standing::NotLinked
    );
    let me = Uuid::now_v7();
    let _replica = Replica::new(
        pool.clone(),
        codecs(),
        me,
        Uplinked::default(),
        Settings::default(),
    )
    .await
    .unwrap();
    let linked = read().await;
    assert_eq!(linked.replica, Some(me));
    assert_eq!(
        linked.standing(Link::Up, Utc::now(), &waits()),
        Standing::NotProvisioned,
        "a device the server has not set up holds nothing: not synced"
    );
}

/// A write flows until it has waited longer than `behind_after`, and is
/// then catching up until the server files it; sent and unfiled for longer
/// than the patience, not answered — offline, if the host's link is down —
/// and once filed, synced, with when it was.
#[sqlx::test(migrations = false)]
async fn a_write_catches_up_until_the_server_files_it(pool: SqlitePool) {
    let w = told(pool).await;
    w.write_event().await;
    let written = w.status().await;
    assert_eq!(
        (written.up[0].table.as_str(), written.up[0].unsent),
        ("events", 1)
    );
    assert_eq!(written.unfiled_since, None, "not sent yet");
    assert_eq!(w.standing().await, Standing::Synced, "it flows");
    assert_eq!(w.standing_at(later(2)).await, Standing::CatchingUp);

    let up = w.up(0, false).await.expect("the write");
    let sent = w.status().await;
    assert_eq!((sent.up[0].unsent, sent.up[0].unfiled), (0, 1));
    let since = sent.unfiled_since.expect("waiting since it was sent");
    assert!(sent.spoken_at.is_some());
    let an_hour_on = since + chrono::Duration::hours(1);
    assert_eq!(
        sent.standing(Link::Up, an_hour_on, &waits()),
        Standing::NotAnswered
    );
    assert_eq!(
        sent.standing(Link::Unknown, an_hour_on, &waits()),
        Standing::NotAnswered,
        "a link the host cannot tell is not taken for down"
    );
    assert_eq!(
        sent.standing(Link::Down, an_hour_on, &waits()),
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
    assert_eq!(w.standing_at(later(60)).await, Standing::Synced);
}

/// A resend does not move how long the server has kept an upload waiting:
/// a server gone silent is not answering, however often the replica tries.
#[sqlx::test(migrations = false)]
async fn a_resend_keeps_how_long_the_server_kept_it_waiting(pool: SqlitePool) {
    let settings = Settings {
        outbox: Outbox {
            resend_after: Duration::from_millis(50),
            ..Outbox::default()
        },
        ..Settings::default()
    };
    let w = world_with(pool, settings).await;
    w.write_event().await;
    w.up(0, false).await.expect("the write");
    let first = w.status().await.unfiled_since.expect("sent");
    for _ in 0..2 {
        tokio::time::sleep(Duration::from_millis(80)).await;
        w.up(0, false).await.expect("sent again");
        assert_eq!(w.status().await.unfiled_since, Some(first));
    }
}

/// A row the replica could not take waits for a fix, whatever the link,
/// counted per table with why — reported once the server is told.
#[sqlx::test(migrations = false)]
async fn a_row_the_replica_could_not_take_awaits_a_fix(pool: SqlitePool) {
    let w = told(pool).await;
    let [good] = ids();
    w.receive(&down(
        1,
        "",
        vec![upserts("items", vec![item(good, "good")])],
    ))
    .await;
    // A copy held, refused an update: the rejection is kept once told.
    w.receive(&down(
        2,
        "",
        vec![upserts(
            "items",
            vec![(key(&[&good.to_string()]), vec![0xff, 0xff])],
        )],
    ))
    .await;
    let status = w.status().await;
    assert_eq!(status.refused.len(), 1);
    let refused = &status.refused[0];
    assert_eq!((refused.table.as_str(), refused.rows), ("items", 1));
    assert!(!refused.reason.is_empty());
    assert!(!refused.reported);
    for link in [Link::Up, Link::Down, Link::Unknown] {
        assert_eq!(
            status.standing(link, Utc::now(), &waits()),
            Standing::AwaitingFix
        );
    }
    w.up(0, false).await.expect("the refusal");
    let told = w.status().await;
    assert_eq!(
        told.refused.len(),
        1,
        "kept: the replica holds an older copy"
    );
    assert!(told.refused[0].reported);
}

/// Of a table's refused and set-aside rows, the reason is the latest's, and
/// the count all of them.
#[sqlx::test(migrations = false)]
async fn the_reason_is_the_latest(pool: SqlitePool) {
    let w = told(pool).await;
    w.exec(&format!(
        "INSERT INTO sync_rejections (table_name, row_key, reason, epoch, sequence, rejected_at) VALUES \
           ('items', 'a', 'older', '{EPOCH}', 1, '2026-01-01T00:00:00.000Z'), \
           ('items', 'b', 'newer', '{EPOCH}', 2, '2026-01-02T00:00:00.000Z'), \
           ('items', 'c', 'held back', '{EPOCH}', 3, '2026-01-03T00:00:00.000Z')"
    ))
    .await
    .unwrap();
    w.exec("UPDATE sync_rejections SET held_back = 1 WHERE row_key = 'c'")
        .await
        .unwrap();
    w.write_event().await;
    w.write_event().await;
    w.exec("UPDATE sync_notes SET set_aside = 'first'")
        .await
        .unwrap();
    w.exec("UPDATE sync_notes SET set_aside = 'last' WHERE id = (SELECT max(id) FROM sync_notes)")
        .await
        .unwrap();
    let status = w.status().await;
    let refused = &status.refused[0];
    assert_eq!(
        (refused.rows, refused.reason.as_str()),
        (2, "newer"),
        "a row held back is nothing to fix"
    );
    assert_eq!(refused.since.to_rfc3339(), "2026-01-01T00:00:00+00:00");
    assert_eq!(
        (
            status.set_aside[0].rows,
            status.set_aside[0].reason.as_str()
        ),
        (2, "last")
    );
    assert_eq!(status.up[0].set_aside, 2, "the same rows");
}

/// What this build cannot send waits for a build that can: catching up, as
/// the server says of it.
#[sqlx::test(migrations = false)]
async fn a_write_this_build_cannot_send_is_catching_up(pool: SqlitePool) {
    let w = told(pool).await;
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
    assert_eq!(
        status.unfiled_since, None,
        "nothing it waits on the server for"
    );
    assert_eq!(w.standing().await, Standing::CatchingUp);
}

/// The wishes not carried out, per table and answer: an answer to an
/// earlier version is no answer to this one, and what was carried out, or
/// is retired, is not counted.
#[sqlx::test(migrations = false)]
async fn the_wishes_not_carried_out_are_counted(pool: SqlitePool) {
    let w = told(pool).await;
    let wish = |n: u8, version: i64, answered: &str, outcome: &str, reason: &str, retired: &str| {
        format!(
            "(x'{n:032x}', 'screens', '{n}', x'', {version}, 0, NULL, {answered}, {outcome}, {reason}, \
              NULL, {retired})"
        )
    };
    w.as_engine(&format!(
        "INSERT INTO sync_wishes (id, relation, row_key, wanted, version, if_unchanged, note, \
                                  answered, outcome, reason, answered_at, retired_at) VALUES {}",
        [
            wish(1, 1, "NULL", "NULL", "NULL", "NULL"),
            wish(2, 2, "1", "'refused'", "'old'", "NULL"),
            wish(3, 1, "1", "'refused'", "'no screen'", "NULL"),
            wish(4, 1, "1", "'applied'", "NULL", "NULL"),
            wish(5, 1, "1", "'waiting'", "'away'", "NULL"),
            wish(6, 1, "1", "'superseded'", "NULL", "NULL"),
            wish(7, 1, "NULL", "NULL", "NULL", "'2026-01-01'"),
            wish(8, 1, "1", "'later build'", "NULL", "NULL"),
        ]
        .join(", ")
    ))
    .await;
    let held = |state, rows, reason: Option<&str>| HeldWishes {
        table: "screens".into(),
        state,
        rows,
        reason: reason.map(Into::into),
    };
    let status = w.status().await;
    assert_eq!(
        status.wishes,
        [
            held(WishState::Pending, 3, None),
            held(WishState::Waiting, 1, Some("away")),
            held(WishState::Refused, 1, Some("no screen")),
            held(WishState::Superseded, 1, None),
        ],
        "1 unanswered, 2 answered an earlier version, 8 a word this build does not know"
    );
    assert_eq!(w.standing().await, Standing::CatchingUp, "the pending ones");
    w.as_engine(
        "UPDATE sync_wishes SET answered = version, outcome = 'refused' \
          WHERE answered IS NOT version OR outcome = 'later build'",
    )
    .await;
    assert_eq!(
        w.standing().await,
        Standing::Synced,
        "a wish waiting for the replica is no change on its way, as the server says"
    );
}

/// A replica that missed a message waits for the server to send everything
/// again, and so does one whose snapshot is in progress: resyncing, until
/// the snapshot ends.
#[sqlx::test(migrations = false)]
async fn a_missed_message_or_a_snapshot_is_resyncing(pool: SqlitePool) {
    let w = told(pool).await;
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
    w.up(0, false).await.expect("the snapshot confirmed");
    assert_eq!(w.standing().await, Standing::Synced);
}

/// Bytes that are no message are not hearing from the server; a message
/// delivered again is, and applies nothing.
#[sqlx::test(migrations = false)]
async fn heard_is_when_a_message_was_read(pool: SqlitePool) {
    let w = told(pool).await;
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
/// recorded, and a note it had sent waiting since that send.
#[sqlx::test(migrations = false)]
async fn a_replica_of_version_3_has_heard_nothing_yet(pool: SqlitePool) {
    let pool = settled(pool).await;
    let mut conn = pool.acquire().await.unwrap();
    for step in 1..=3 {
        let step = migration(step).expect("a released step");
        sqlx::raw_sql(&step).execute(&mut *conn).await.unwrap();
    }
    sqlx::raw_sql(TABLES).execute(&mut *conn).await.unwrap();
    sqlx::raw_sql(
        "INSERT INTO sync_notes (relation, row_key, upload, sent, sent_at) \
         VALUES ('events', 'sent', 1, 1, '2026-01-01T00:00:00.000Z')",
    )
    .execute(&mut *conn)
    .await
    .unwrap();
    install(&mut conn, &codecs()).await.expect("upgraded");
    let status = status(&mut conn).await.unwrap();
    assert_eq!(
        (status.heard_at, status.applied_at, status.filed_at),
        (None, None, None)
    );
    assert_eq!(
        status.unfiled_since.map(|at| at.to_rfc3339()),
        Some("2026-01-01T00:00:00+00:00".to_owned()),
        "a note sent before waits since its last send"
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
        unconfirmed: false,
        refused: Vec::new(),
        set_aside: Vec::new(),
        wishes: Vec::new(),
    };
    let wish = |state| HeldWishes {
        table: "screens".into(),
        state,
        rows: 1,
        reason: None,
    };
    let judged = |status: &ReplicaStatus, link| status.standing(link, now, &waits());
    assert_eq!(judged(&synced, Link::Up), Standing::Synced);
    assert_eq!(judged(&synced, Link::Down), Standing::Offline);

    let wished = ReplicaStatus {
        wishes: vec![wish(WishState::Pending)],
        ..synced.clone()
    };
    assert_eq!(judged(&wished, Link::Up), Standing::CatchingUp);
    let unconfirmed = ReplicaStatus {
        unconfirmed: true,
        ..synced.clone()
    };
    assert_eq!(judged(&unconfirmed, Link::Up), Standing::CatchingUp);
    for settled in [
        WishState::Waiting,
        WishState::Refused,
        WishState::Superseded,
    ] {
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
    let unprovisioned = ReplicaStatus {
        receiving: Receiving::Unprovisioned,
        ..unanswered.clone()
    };
    assert_eq!(judged(&unprovisioned, Link::Up), Standing::NotProvisioned);
    assert_eq!(
        judged(&unprovisioned, Link::Down),
        Standing::Offline,
        "a new device without its cable is told of the cable"
    );
    let broken = ReplicaStatus {
        refused: vec![Refused {
            table: "items".into(),
            rows: 1,
            reason: "no".into(),
            since: now,
            reported: true,
        }],
        ..unprovisioned.clone()
    };
    assert_eq!(judged(&broken, Link::Down), Standing::AwaitingFix);
    let unlinked = ReplicaStatus {
        replica: None,
        ..broken
    };
    assert_eq!(judged(&unlinked, Link::Down), Standing::NotLinked);
    let mut sorted = Standing::ALL;
    sorted.sort();
    assert_eq!(sorted, Standing::ALL, "listed worst first");
    for standing in Standing::ALL {
        assert_eq!(
            serde_json::to_value(standing).unwrap(),
            standing.as_str(),
            "the word is the JSON's"
        );
    }
}

/// Patience never drops under the outbox's resend: a healthy upload that
/// was lost, and waits to go again, is not taken for a server gone quiet.
#[test]
fn patience_waits_out_a_resend() {
    let outbox = Outbox {
        resend_after: Duration::from_secs(600),
        ..Outbox::default()
    };
    let hasty = Waits::new(&outbox).patience(Duration::from_secs(1));
    let now = Utc::now();
    let status = ReplicaStatus {
        replica: Some(Uuid::now_v7()),
        up: Vec::new(),
        unfiled_since: Some(now - chrono::Duration::minutes(5)),
        spoken_at: None,
        heard_at: None,
        applied_at: None,
        filed_at: None,
        receiving: Receiving::Following,
        unconfirmed: false,
        refused: Vec::new(),
        set_aside: Vec::new(),
        wishes: Vec::new(),
    };
    assert_eq!(status.standing(Link::Up, now, &hasty), Standing::Synced);
    assert_eq!(
        status.standing(Link::Up, now + chrono::Duration::minutes(6), &hasty),
        Standing::NotAnswered
    );
}

/// A snapshot begun re-states the replica's settings, to go again to a
/// server that may be another: what one waited for before is not what it
/// waits for now.
#[sqlx::test(migrations = false)]
async fn a_snapshot_begun_waits_for_no_setting_sent_before(pool: SqlitePool) {
    let w = world_of(
        pool,
        &format!("{TABLES}{SCREENS}"),
        Settings {
            wishes: crate::wish::Wishes::new().on::<Screen>(crate::wish::Direct),
            ..Settings::default()
        },
        golden_codecs(),
    )
    .await;
    sqlx::query("INSERT INTO screens (replica_id, id, level, revision) VALUES ($1, $2, 1, 1)")
        .bind(w.me)
        .bind(Uuid::now_v7())
        .execute(&w.pool)
        .await
        .unwrap();
    w.up(0, false).await.expect("the setting");
    assert!(w.status().await.unfiled_since.is_some());
    let s = Uuid::now_v7().to_string();
    w.receive(&begin(1, &s, vec![])).await;
    assert_eq!(w.status().await.unfiled_since, None);
}

/// A message applied is unconfirmed until the replica tells the server.
#[sqlx::test(migrations = false)]
async fn an_applied_message_is_unconfirmed_until_told(pool: SqlitePool) {
    let w = told(pool).await;
    w.up(0, false).await;
    let a = Uuid::now_v7();
    w.receive(&down(1, "", vec![upserts("items", vec![item(a, "a")])]))
        .await;
    assert!(w.status().await.unconfirmed);
    assert_eq!(w.standing().await, Standing::CatchingUp);
    w.up(0, false).await.expect("the confirmation");
    assert!(!w.status().await.unconfirmed);
    assert_eq!(w.standing().await, Standing::Synced);
}
