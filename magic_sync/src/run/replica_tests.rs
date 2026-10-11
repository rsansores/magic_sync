//! A replica as a host runs it: a message received is committed and the
//! server hears of it without a line of loop in the test; what the uplink
//! refuses for a while goes again.

use super::*;
use crate::conformance::{codecs, settled, Uplinked};
use crate::envelope::{self, Body, Down, Key as WireKey, Table};
use crate::testkit::ItemRow;
use prost::Message as _;

const TABLES: &str = "
CREATE TABLE items (id BLOB PRIMARY KEY NOT NULL, partition_id BLOB NOT NULL, name TEXT NOT NULL, retired_at TEXT);
CREATE TABLE shelves (aisle BLOB NOT NULL, slot INTEGER NOT NULL, label TEXT NOT NULL, revision INTEGER NOT NULL,
    PRIMARY KEY (aisle, slot));
CREATE TABLE prices (id BLOB PRIMARY KEY NOT NULL, cents INTEGER NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE tasks (id BLOB PRIMARY KEY NOT NULL, title TEXT NOT NULL, done INTEGER NOT NULL, updated_at TEXT NOT NULL,
    retired_at TEXT);
CREATE TABLE readings (replica_id BLOB NOT NULL, id BLOB NOT NULL, value INTEGER NOT NULL, at INTEGER NOT NULL,
    PRIMARY KEY (replica_id, id));
CREATE TABLE events (id BLOB PRIMARY KEY NOT NULL, what TEXT NOT NULL);
";

async fn pool(pool: SqlitePool) -> SqlitePool {
    let pool = settled(pool).await;
    sqlx::raw_sql(TABLES).execute(&pool).await.unwrap();
    replica::install(&mut pool.acquire().await.unwrap(), &codecs())
        .await
        .unwrap();
    pool
}

/// Message `sequence` of the snapshot `s`, following `previous` — or, after
/// none, beginning it.
fn rows(sequence: u64, previous: u64, name: &str) -> Vec<u8> {
    let id = Uuid::now_v7();
    Body::Down(Down {
        tables: vec![Table {
            table: "items".to_owned(),
            upserts: vec![ItemRow {
                id: id.to_string(),
                partition_id: Uuid::nil().to_string(),
                name: name.to_owned(),
            }
            .encode_to_vec()],
            keys: vec![WireKey {
                parts: vec![id.to_string()],
            }],
            ..Table::default()
        }],
        sequence,
        previous,
        snapshot_id: "s".to_owned(),
        epoch: Uuid::nil().to_string(),
        nonce: crate::conformance::nonce_of(sequence),
        previous_nonce: if previous == 0 {
            0
        } else {
            crate::conformance::nonce_of(previous)
        },
        mac: Vec::new(),
    })
    .encoded()
}

fn settings() -> Settings {
    Settings {
        idle: Duration::from_millis(10),
        backoff: crate::run::Backoff::new(Duration::from_millis(10), Duration::from_millis(20)),
        ..Settings::default()
    }
}

/// Runs `replica` until `done` holds, or fails after five seconds.
async fn until<I: Identity, U: Uplink>(replica: &Replica<(), I, U>, done: impl Fn() -> bool) {
    let (stop, mut stopped) = tokio::sync::watch::channel(false);
    let run = replica.run(async move {
        let _ = stopped.wait_for(|stop| *stop).await;
    });
    let check = async {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !done() {
            assert!(Instant::now() < deadline, "not done within five seconds");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        stop.send(true).unwrap();
    };
    tokio::join!(run, check);
}

fn acknowledged(uplink: &Uplinked) -> Vec<u64> {
    uplink
        .sent
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, _, up)| up.sequence > 0)
        .map(|(_, _, up)| up.sequence)
        .collect()
}

#[sqlx::test(migrations = false)]
async fn what_the_replica_applied_reaches_the_server_even_through_an_outage(pool_: SqlitePool) {
    let pool = pool(pool_).await;
    let me = Uuid::now_v7();
    let uplink = Uplinked::default();
    let replica = Replica::new(pool.clone(), codecs(), me, uplink.clone(), settings())
        .await
        .unwrap();

    let received = replica.receive(&rows(4, 0, "four")).await.unwrap();
    assert!(matches!(received, Received::Applied(_)));
    let names: Vec<String> = sqlx::query_scalar("SELECT name FROM items")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(names, ["four"], "committed before it returns");

    *uplink.refusing.lock().unwrap() = Some(true);
    replica.receive(&rows(5, 4, "five")).await.unwrap();
    let outage = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        *uplink.refusing.lock().unwrap() = None;
    };
    let (_, ()) = tokio::join!(outage, until(&replica, || acknowledged(&uplink) == [5]));
    assert!(uplink
        .sent
        .lock()
        .unwrap()
        .iter()
        .all(|(r, lane, up)| *r == me && *lane == "main" && up.protocol == envelope::PROTOCOL));
}

/// A replica that does not know which one it is yet sends nothing up.
#[sqlx::test(migrations = false)]
async fn nothing_goes_up_until_the_replica_knows_who_it_is(pool_: SqlitePool) {
    struct Nobody;
    impl Identity for Nobody {
        async fn id(&self, _conn: &mut Conn) -> Result<Option<Uuid>, Error> {
            Ok(None)
        }
    }
    let pool = pool(pool_).await;
    let uplink = Uplinked::default();
    let replica = Replica::new(pool, codecs(), Nobody, uplink.clone(), settings())
        .await
        .unwrap();
    replica.receive(&rows(1, 0, "one")).await.unwrap();
    assert!(!replica.send_lane(0, true).await.unwrap().message);
    assert!(uplink.sent.lock().unwrap().is_empty());
}

/// A replica refuses to start over a database it cannot run against.
#[sqlx::test(migrations = false)]
async fn a_replica_does_not_start_over_a_database_it_cannot_run_against(pool: SqlitePool) {
    sqlx::raw_sql(TABLES).execute(&pool).await.unwrap();
    let refused = Replica::new(pool, codecs(), Uuid::nil(), Uplinked::default(), settings()).await;
    match refused {
        Err(Error::Misuse(why)) => assert!(why.contains("tables are version 0"), "{why}"),
        Err(e) => panic!("refused for another reason: {e}"),
        Ok(_) => panic!("started"),
    }
}

/// Settings a replica cannot run with are refused when it is built: a wait
/// of nothing, which would spin or say the same thing without end, and two
/// lanes of one name.
#[sqlx::test(migrations = false)]
async fn a_replica_refuses_settings_it_cannot_run_with(pool_: SqlitePool) {
    let pool = pool(pool_).await;
    let mut cases: Vec<(&str, Settings)> = Vec::new();
    for (what, set) in [
        (
            "idle",
            (|s: &mut Settings| s.idle = Duration::ZERO) as fn(&mut Settings),
        ),
        ("digests_first", |s| s.digests_first = Duration::ZERO),
        ("digests_every", |s| s.digests_every = Duration::ZERO),
        ("outbox.resend_after", |s| {
            s.outbox.resend_after = Duration::ZERO
        }),
        ("outbox.backlog_every", |s| {
            s.outbox.backlog_every = Duration::ZERO
        }),
    ] {
        let mut settings = settings();
        set(&mut settings);
        cases.push((what, settings));
    }
    let mut twice = settings();
    twice.lanes = vec![
        replica::Lane::new("main", &[]),
        replica::Lane::new("main", &["events"]),
    ];
    cases.push(("main", twice));
    for (what, settings) in cases {
        let refused = Replica::new(
            pool.clone(),
            codecs(),
            Uuid::nil(),
            Uplinked::default(),
            settings,
        )
        .await;
        match refused {
            Err(Error::Misuse(why)) => assert!(why.contains(what), "{what}: {why}"),
            Err(e) => panic!("{what}: refused for another reason: {e}"),
            Ok(_) => panic!("{what}: not refused"),
        }
    }
}

/// A lane whose uplink hangs holds back no other lane, and only for the
/// send timeout: then it goes again, and its message goes once the uplink
/// takes it.
#[sqlx::test(migrations = false)]
async fn a_lane_whose_uplink_hangs_holds_back_nothing_for_long(pool_: SqlitePool) {
    let pool = pool(pool_).await;
    let uplink = Uplinked::default();
    let settings = Settings {
        lanes: vec![
            replica::Lane::new("main", &[]),
            replica::Lane::new("fast", &["readings"]),
        ],
        send_timeout: Duration::from_millis(50),
        ..settings()
    };
    let me = Uuid::now_v7();
    let replica = Replica::new(pool.clone(), codecs(), me, uplink.clone(), settings)
        .await
        .unwrap();
    uplink.hanging.lock().unwrap().push("main");
    replica.receive(&rows(1, 0, "one")).await.unwrap();
    sqlx::query("INSERT INTO readings (replica_id, id, value, at) VALUES ($1, $2, 1, 1)")
        .bind(me)
        .bind(Uuid::now_v7())
        .execute(&pool)
        .await
        .unwrap();
    let went = |lane: &str| {
        uplink
            .sent
            .lock()
            .unwrap()
            .iter()
            .any(|(_, l, _)| *l == lane)
    };
    let unhang = async {
        until_true(|| went("fast")).await;
        assert!(!went("main"));
        uplink.hanging.lock().unwrap().clear();
    };
    let (_, ()) = tokio::join!(unhang, until(&replica, || went("main")));
    assert_eq!(acknowledged(&uplink), [1]);
}

/// A nudge wakes an idle lane at once, rather than at its next turn.
#[sqlx::test(migrations = false)]
async fn a_nudge_sends_what_was_written_at_once(pool_: SqlitePool) {
    let pool = pool(pool_).await;
    let uplink = Uplinked::default();
    let settings = Settings {
        idle: Duration::from_secs(60),
        ..settings()
    };
    let me = Uuid::now_v7();
    let replica = Replica::new(pool.clone(), codecs(), me, uplink.clone(), settings)
        .await
        .unwrap();
    let write = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        sqlx::query("INSERT INTO events (id, what) VALUES ($1, 'now')")
            .bind(Uuid::now_v7())
            .execute(&pool)
            .await
            .unwrap();
        replica.nudge();
    };
    let carried = || {
        uplink
            .sent
            .lock()
            .unwrap()
            .iter()
            .any(|(_, _, up)| !up.tables.is_empty())
    };
    let (_, ()) = tokio::join!(write, until(&replica, carried));
}

/// A lane that fails waits out its backoff, doubling up to its longest —
/// and a nudge does not cut the wait short.
#[sqlx::test(migrations = false)]
async fn a_failing_lane_backs_off_whatever_nudges_it(pool_: SqlitePool) {
    let pool = pool(pool_).await;
    let uplink = Uplinked::default();
    let settings = Settings {
        backoff: crate::run::Backoff::new(Duration::from_millis(50), Duration::from_millis(100)),
        ..settings()
    };
    let replica = Replica::new(pool, codecs(), Uuid::now_v7(), uplink.clone(), settings)
        .await
        .unwrap();
    replica.receive(&rows(1, 0, "one")).await.unwrap();
    *uplink.refusing.lock().unwrap() = Some(true);
    let (stop, mut stopped) = tokio::sync::watch::channel(false);
    let run = replica.run(async move {
        let _ = stopped.wait_for(|stop| *stop).await;
    });
    let nudge = async {
        for _ in 0..60 {
            replica.nudge();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        stop.send(true).unwrap();
    };
    tokio::join!(run, nudge);
    // Sends at 0, 50, 150, 250, 350, 450, 550 ms, give or take.
    let attempts = uplink.attempts.load(std::sync::atomic::Ordering::Relaxed);
    assert!((5..=9).contains(&attempts), "{attempts} sends in 600 ms");
}

/// A second run of a replica that runs already returns at once: two would
/// send the same rows under two numbers. One that ended frees the next.
#[sqlx::test(migrations = false)]
async fn a_replica_runs_once_at_a_time(pool_: SqlitePool) {
    let pool = pool(pool_).await;
    let uplink = Uplinked::default();
    let replica = Replica::new(pool, codecs(), Uuid::now_v7(), uplink.clone(), settings())
        .await
        .unwrap();
    let (stop, mut stopped) = tokio::sync::watch::channel(false);
    let first = replica.run(async move {
        let _ = stopped.wait_for(|stop| *stop).await;
    });
    let second = async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        tokio::time::timeout(Duration::from_secs(1), replica.run(std::future::pending()))
            .await
            .expect("the second run returns at once");
        stop.send(true).unwrap();
    };
    tokio::join!(first, second);
    replica.receive(&rows(1, 0, "one")).await.unwrap();
    until(&replica, || !uplink.sent.lock().unwrap().is_empty()).await;
}

/// Waits until `done` holds, or fails after five seconds.
async fn until_true(done: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !done() {
        assert!(Instant::now() < deadline, "not done within five seconds");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Digests wait for the replica to settle — `digests_first` — and then go
/// every `digests_every`.
#[sqlx::test(migrations = false)]
async fn digests_go_once_the_replica_settled(pool_: SqlitePool) {
    let pool = pool(pool_).await;
    let uplink = Uplinked::default();
    let settings = Settings {
        digests_first: Duration::from_millis(300),
        digests_every: Duration::from_secs(3600),
        ..settings()
    };
    let replica = Replica::new(pool, codecs(), Uuid::now_v7(), uplink.clone(), settings)
        .await
        .unwrap();
    replica.receive(&rows(1, 0, "one")).await.unwrap();
    let digested = || {
        uplink
            .sent
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, _, up)| !up.digests.is_empty())
            .count()
    };
    let started = Instant::now();
    until(&replica, || started.elapsed() > Duration::from_millis(700)).await;
    assert_eq!(digested(), 1, "once settled, and not again within the hour");
    let first = uplink.sent.lock().unwrap()[0].2.clone();
    assert_eq!(
        (first.sequence, first.digests.len()),
        (1, 0),
        "confirmed at once, without them"
    );
}

/// A row the uplink fails to take for a while is not set aside: it goes
/// again once the uplink is back.
#[sqlx::test(migrations = false)]
async fn a_row_the_uplink_fails_for_a_while_goes_again(pool_: SqlitePool) {
    let pool = pool(pool_).await;
    let uplink = Uplinked::default();
    let replica = Replica::new(
        pool.clone(),
        codecs(),
        Uuid::now_v7(),
        uplink.clone(),
        settings(),
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO events (id, what) VALUES ($1, 'x')")
        .bind(Uuid::now_v7())
        .execute(&pool)
        .await
        .unwrap();
    *uplink.refusing.lock().unwrap() = Some(true);
    assert!(replica.send_lane(0, false).await.is_err());
    let aside: i64 =
        sqlx::query_scalar("SELECT count(*) FROM sync_notes WHERE set_aside IS NOT NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(aside, 0);
    *uplink.refusing.lock().unwrap() = None;
    assert!(replica.send_lane(0, false).await.unwrap().message);
    assert_eq!(uplink.sent.lock().unwrap()[0].2.tables.len(), 1);
}
