//! Wishes on a replica: taken as rows of a table written on both sides,
//! carried out through the host's handler once each, and answered — the
//! answer and the setting it changed going up together.

use super::*;
use crate::envelope::Wish as WireWish;
use crate::wish::{CarryOut, Direct, Outcome, Wish, Wishes};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

/// A handler that answers as it is told, and remembers what it was handed.
/// While `held` it waits, once it said it began, until it is let go.
#[derive(Clone, Default)]
struct Hand {
    answers: Arc<Mutex<VecDeque<Outcome>>>,
    seen: Arc<Mutex<Vec<Wish<Screen>>>>,
    held: Arc<Mutex<bool>>,
    began: Arc<Notify>,
    go: Arc<Notify>,
}

impl CarryOut<Screen> for Hand {
    async fn carry_out(&self, wish: Wish<Screen>) -> Outcome {
        self.seen.lock().unwrap().push(wish);
        if *self.held.lock().unwrap() {
            self.began.notify_one();
            self.go.notified().await;
        }
        self.answers
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Outcome::Applied)
    }
}

impl Hand {
    fn answering(&self, outcome: Outcome) {
        self.answers.lock().unwrap().push_back(outcome);
    }

    fn seen(&self) -> Vec<(i64, Option<i64>, i64)> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .map(|w| (w.wanted.level, w.held.as_ref().map(|h| h.level), w.version))
            .collect()
    }
}

async fn wish_world(pool: SqlitePool, hand: &Hand) -> World {
    let settings = Settings {
        wishes: Wishes::new().on::<Screen>(hand.clone()),
        ..Settings::default()
    };
    world_of(
        pool,
        &format!("{TABLES}{SCREENS}"),
        settings,
        golden_codecs(),
    )
    .await
}

/// The wish of the row `screen` (by its key), as a server sends it.
fn wished(screen: &Screen, version: u64, if_unchanged: bool) -> (WireKey, Vec<u8>) {
    let id = Uuid::from_u128(0xa_0000 + screen.id.as_u128());
    let wish = WireWish {
        id: id.to_string(),
        replica_id: screen.replica_id.to_string(),
        table: "screens".to_owned(),
        key: Some(key(&[
            &screen.replica_id.to_string(),
            &screen.id.to_string(),
        ])),
        wanted: json(screen),
        version,
        if_unchanged,
        note: "ops".to_owned(),
        ..WireWish::default()
    };
    (key(&[&id.to_string()]), wish.encode_to_vec())
}

fn wishes(rows: Vec<(WireKey, Vec<u8>)>) -> Table {
    upserts("sync_wishes", rows)
}

impl World {
    fn screen(&self, level: i64, revision: i64) -> Screen {
        Screen {
            replica_id: self.me,
            id: Uuid::from_u128(1),
            level,
            revision,
        }
    }

    /// The replica's own write of its setting.
    async fn set(&self, level: i64, revision: i64) {
        sqlx::query(
            "INSERT INTO screens (replica_id, id, level, revision) VALUES ($1, $2, $3, $4) \
             ON CONFLICT (replica_id, id) DO UPDATE SET level = excluded.level, \
                    revision = excluded.revision",
        )
        .bind(self.me)
        .bind(Uuid::from_u128(1))
        .bind(level)
        .bind(revision)
        .execute(&self.pool)
        .await
        .unwrap();
    }

    async fn setting(&self) -> Option<(i64, i64)> {
        sqlx::query_as("SELECT level, revision FROM screens")
            .fetch_optional(&self.pool)
            .await
            .unwrap()
    }

    /// The answer the replica holds to its one wish.
    async fn answered(&self) -> Option<(Option<i64>, Option<String>, Option<String>)> {
        sqlx::query_as("SELECT answered, outcome, reason FROM sync_wishes")
            .fetch_optional(&self.pool)
            .await
            .unwrap()
    }

    async fn offer(&self) -> usize {
        self.replica.carry_out_wishes().await.expect("offered")
    }
}

/// A wish is carried out once, through the handler, which is handed the
/// row as held; the engine writes the wished columns onto the setting with
/// its version moved on, and answers — both going up together. Delivered
/// again, or offered again, it is not carried out again.
#[sqlx::test(migrations = false)]
async fn a_wish_is_carried_out_once_and_its_answer_goes_up_with_the_setting(pool: SqlitePool) {
    let hand = Hand::default();
    let w = wish_world(pool, &hand).await;
    w.set(40, 1).await;
    let message = down(
        1,
        "",
        vec![wishes(vec![wished(&w.screen(70, 1), 5, false)])],
    );
    let received = w.receive(&message).await;
    assert!(
        matches!(&received, Received::Applied(a) if a.iter().any(|a| a.table == "sync_wishes")),
        "{received:?}"
    );
    assert_eq!(
        w.answered().await,
        Some((None, None, None)),
        "held, unanswered"
    );
    assert_eq!(w.offer().await, 1);
    assert_eq!(hand.seen(), [(70, Some(40), 5)]);
    assert_eq!(w.setting().await, Some((70, 2)), "the wish, a version on");
    assert_eq!(
        w.answered().await,
        Some((Some(5), Some("applied".to_owned()), None))
    );
    assert_eq!(w.offer().await, 0);
    assert!(matches!(
        w.receive(&message).await,
        Received::AlreadyApplied
    ));
    assert_eq!(w.offer().await, 0);
    assert_eq!(hand.seen().len(), 1, "carried out once");

    let up = w.up(0, false).await.expect("something to say");
    let tables: BTreeSet<&str> = up.tables.iter().map(|t| t.table.as_str()).collect();
    assert_eq!(tables, BTreeSet::from(["screens", "sync_wishes"]));
    let answer = up.tables.iter().find(|t| t.table == "sync_wishes").unwrap();
    let wish = WireWish::decode(answer.upserts[0].as_slice()).unwrap();
    assert_eq!((wish.answered, wish.outcome.as_str()), (5, "applied"));
    assert!(!wish.answered_at.is_empty());
}

/// A refusal is final for the wish, and leaves the setting as it is; a wish
/// based on a version the replica no longer holds is superseded without
/// being carried out; one based on the version it holds is.
#[sqlx::test(migrations = false)]
async fn a_wish_refused_or_superseded_leaves_the_setting_as_it_is(pool: SqlitePool) {
    let hand = Hand::default();
    let w = wish_world(pool, &hand).await;
    w.set(40, 1).await;
    hand.answering(Outcome::Refused("out of range".to_owned()));
    w.receive(&down(
        1,
        "",
        vec![wishes(vec![wished(&w.screen(900, 1), 5, false)])],
    ))
    .await;
    w.offer().await;
    assert_eq!(w.setting().await, Some((40, 1)));
    let refused = Some((
        Some(5),
        Some("refused".to_owned()),
        Some("out of range".to_owned()),
    ));
    assert_eq!(w.answered().await, refused);
    assert_eq!(w.offer().await, 0, "not offered again");

    w.set(45, 2).await;
    w.receive(&down(
        2,
        "",
        vec![wishes(vec![wished(&w.screen(70, 1), 6, true)])],
    ))
    .await;
    w.offer().await;
    assert_eq!(hand.seen().len(), 1, "superseded without the handler");
    assert_eq!(
        w.setting().await,
        Some((45, 2)),
        "the replica's value stands"
    );
    let (answered, outcome, reason) = w.answered().await.unwrap();
    assert_eq!(
        (answered, outcome.as_deref()),
        (Some(6), Some("superseded"))
    );
    assert!(reason.unwrap().contains("version 2"));

    w.receive(&down(
        3,
        "",
        vec![wishes(vec![wished(&w.screen(70, 2), 7, true)])],
    ))
    .await;
    w.offer().await;
    assert_eq!(hand.seen().len(), 2);
    assert_eq!(w.setting().await, Some((70, 3)), "based on what it held");
    assert_eq!(w.answered().await.unwrap().1.as_deref(), Some("applied"));
}

/// A wish the handler cannot carry out yet waits, and says why once — not
/// on every offer — until it can.
#[sqlx::test(migrations = false)]
async fn a_wish_waits_until_its_handler_can(pool: SqlitePool) {
    let hand = Hand::default();
    let w = wish_world(pool, &hand).await;
    w.set(40, 1).await;
    hand.answering(Outcome::Later("module away".to_owned()));
    hand.answering(Outcome::Later("module away".to_owned()));
    w.receive(&down(
        1,
        "",
        vec![wishes(vec![wished(&w.screen(70, 1), 5, false)])],
    ))
    .await;
    assert_eq!(w.offer().await, 1);
    let waiting = Some((
        Some(5),
        Some("waiting".to_owned()),
        Some("module away".to_owned()),
    ));
    assert_eq!(w.answered().await, waiting);
    let up = w.up(0, false).await.expect("the waiting goes up");
    assert!(up.tables.iter().any(|t| t.table == "sync_wishes"));
    assert_eq!(w.offer().await, 0, "the same answer, said once");
    assert!(w.up(0, false).await.is_none(), "nothing new to say");
    assert_eq!(w.offer().await, 1);
    assert_eq!(hand.seen().len(), 3);
    assert_eq!(w.setting().await, Some((70, 2)));
    assert_eq!(w.answered().await.unwrap().1.as_deref(), Some("applied"));
}

/// A replica that stops after its handler carried a wish out and before it
/// recorded the answer carries it out again, the same wish: recorded once.
#[sqlx::test(migrations = false)]
async fn a_wish_cut_short_before_its_answer_is_carried_out_again(pool: SqlitePool) {
    let hand = Hand::default();
    let w = wish_world(pool, &hand).await;
    w.set(40, 1).await;
    w.receive(&down(
        1,
        "",
        vec![wishes(vec![wished(&w.screen(70, 1), 5, false)])],
    ))
    .await;
    *hand.held.lock().unwrap() = true;
    {
        let offering = w.offer();
        tokio::pin!(offering);
        tokio::select! {
            _ = &mut offering => panic!("the handler is held"),
            () = hand.began.notified() => {}
        }
    }
    assert_eq!(w.answered().await, Some((None, None, None)), "cut short");
    *hand.held.lock().unwrap() = false;
    assert_eq!(w.offer().await, 1);
    assert_eq!(hand.seen(), [(70, Some(40), 5), (70, Some(40), 5)]);
    assert_eq!(w.setting().await, Some((70, 2)), "written once");
}

/// A wish replaced while its handler carries it out is not answered — the
/// replica holds the new one — but what the handler set is what the setting
/// is; the new wish is offered next.
#[sqlx::test(migrations = false)]
async fn a_wish_replaced_while_carried_out_is_not_answered(pool: SqlitePool) {
    let hand = Hand::default();
    let w = wish_world(pool, &hand).await;
    w.set(40, 1).await;
    w.receive(&down(
        1,
        "",
        vec![wishes(vec![wished(&w.screen(70, 1), 5, false)])],
    ))
    .await;
    *hand.held.lock().unwrap() = true;
    let offering = w.offer();
    tokio::pin!(offering);
    tokio::select! {
        _ = &mut offering => panic!("the handler is held"),
        () = hand.began.notified() => {}
    }
    w.receive(&down(
        2,
        "",
        vec![wishes(vec![wished(&w.screen(80, 1), 6, false)])],
    ))
    .await;
    *hand.held.lock().unwrap() = false;
    hand.go.notify_one();
    assert_eq!(offering.await, 0, "the answer was to a wish gone");
    assert_eq!(w.setting().await, Some((70, 2)), "what the handler set");
    assert_eq!(w.answered().await, Some((None, None, None)));
    assert_eq!(w.offer().await, 1);
    assert_eq!(hand.seen().last(), Some(&(80, Some(70), 6)));
    assert_eq!(w.setting().await, Some((80, 3)));
}

/// A wish the server sends with an answer — none does — is taken without
/// it, and carried out; a wish of a row the replica holds none of is
/// answered, and writes nothing.
#[sqlx::test(migrations = false)]
async fn a_wish_takes_no_answer_from_the_server(pool: SqlitePool) {
    let hand = Hand::default();
    let w = wish_world(pool, &hand).await;
    let (key, bytes) = wished(&w.screen(70, 1), 5, false);
    let mut wish = WireWish::decode(bytes.as_slice()).unwrap();
    wish.answered = 5;
    wish.outcome = "applied".to_owned();
    w.receive(&down(
        1,
        "",
        vec![wishes(vec![(key, wish.encode_to_vec())])],
    ))
    .await;
    assert_eq!(w.answered().await, Some((None, None, None)));
    assert_eq!(w.offer().await, 1);
    assert_eq!(hand.seen(), [(70, None, 5)]);
    assert_eq!(w.setting().await, None, "no row to write onto");
    assert_eq!(w.answered().await.unwrap().1.as_deref(), Some("applied"));
}

/// A wish withdrawn while its answer is on its way up is kept retired until
/// the answer is filed, and is not offered meanwhile.
#[sqlx::test(migrations = false)]
async fn a_wish_withdrawn_before_its_answer_is_filed_waits_retired(pool: SqlitePool) {
    let hand = Hand::default();
    let w = wish_world(pool, &hand).await;
    w.set(40, 1).await;
    let (key, row) = wished(&w.screen(70, 1), 5, false);
    hand.answering(Outcome::Later("away".to_owned()));
    w.receive(&down(1, "", vec![wishes(vec![(key.clone(), row)])]))
        .await;
    w.offer().await;
    let up = w.up(0, false).await.expect("the answer");
    w.receive(&down(2, "", vec![deletes("sync_wishes", vec![key])]))
        .await;
    assert_eq!(
        w.count("SELECT count(*) FROM sync_wishes WHERE retired_at IS NOT NULL")
            .await,
        1
    );
    assert_eq!(w.offer().await, 0);
    assert_eq!(hand.seen().len(), 1);
    let filed = Body::Filed(Filed {
        uploads: up.uploads.clone(),
    })
    .encoded();
    w.receive(&filed).await;
    assert_eq!(w.count("SELECT count(*) FROM sync_wishes").await, 0);
}

/// A wish of a table this build holds as no setting waits for one that
/// does, and says so.
#[sqlx::test(migrations = false)]
async fn a_wish_of_a_table_this_build_does_not_hold_waits(pool: SqlitePool) {
    let hand = Hand::default();
    let w = wish_world(pool, &hand).await;
    let (key, bytes) = wished(&w.screen(70, 1), 5, false);
    let mut wish = WireWish::decode(bytes.as_slice()).unwrap();
    wish.table = "dials".to_owned();
    w.receive(&down(
        1,
        "",
        vec![wishes(vec![(key, wish.encode_to_vec())])],
    ))
    .await;
    assert_eq!(w.offer().await, 1);
    let (_, outcome, reason) = w.answered().await.unwrap();
    assert_eq!(outcome.as_deref(), Some("waiting"));
    assert!(reason.unwrap().contains("`dials`"));
    assert!(hand.seen().is_empty());
}

/// Every setting needs a handler, and a handler a setting.
#[sqlx::test(migrations = false)]
async fn a_replica_refuses_a_setting_with_no_handler_and_a_handler_of_none(pool: SqlitePool) {
    let pool = settled(pool).await;
    sqlx::raw_sql(&format!("{TABLES}{SCREENS}"))
        .execute(&pool)
        .await
        .unwrap();
    install(&mut pool.acquire().await.unwrap(), &golden_codecs())
        .await
        .unwrap();
    let new = |codecs: Vec<Codec<()>>, wishes: Wishes| {
        let pool = pool.clone();
        async move {
            let settings = Settings {
                wishes,
                ..Settings::default()
            };
            Replica::new(pool, codecs, Uuid::now_v7(), Uplinked::default(), settings).await
        }
    };
    let refused = new(golden_codecs(), Wishes::new()).await;
    assert!(
        matches!(&refused, Err(Error::Misuse(why)) if why.contains("nothing carries")),
        "{refused:?}"
    );
    let refused = new(codecs(), Wishes::new().on::<Screen>(Direct)).await;
    assert!(
        matches!(&refused, Err(Error::Misuse(why)) if why.contains("no setting")),
        "{refused:?}"
    );
    new(golden_codecs(), Wishes::new().on::<Screen>(Direct))
        .await
        .expect("a setting and its handler");
}

/// A handler that cannot carry anything out until it is told it can.
#[derive(Clone, Default)]
struct Until(Arc<std::sync::atomic::AtomicBool>);

impl CarryOut<Screen> for Until {
    async fn carry_out(&self, _: Wish<Screen>) -> Outcome {
        if self.0.load(std::sync::atomic::Ordering::Relaxed) {
            Outcome::Applied
        } else {
            Outcome::Later("away".to_owned())
        }
    }
}

/// The handlers a replica runs with are offered wishes while it runs: one
/// a message brings, and one waiting, when the host offers it again.
#[sqlx::test(migrations = false)]
async fn a_running_replica_offers_the_wishes_a_message_brings(pool: SqlitePool) {
    let ready = Until::default();
    let settings = Settings {
        wishes: Wishes::new().on::<Screen>(ready.clone()),
        ..Settings::default()
    };
    let w = Arc::new(
        world_of(
            pool,
            &format!("{TABLES}{SCREENS}"),
            settings,
            golden_codecs(),
        )
        .await,
    );
    w.set(40, 1).await;
    let stop = Arc::new(Notify::new());
    let running = {
        let (w, stop) = (w.clone(), stop.clone());
        tokio::spawn(async move { w.replica.run(stop.notified()).await })
    };
    w.receive(&down(
        1,
        "",
        vec![wishes(vec![wished(&w.screen(70, 1), 5, false)])],
    ))
    .await;
    let until = |answer: &'static str| {
        let w = w.clone();
        async move {
            for _ in 0..500 {
                if w.answered()
                    .await
                    .and_then(|(_, outcome, _)| outcome)
                    .as_deref()
                    == Some(answer)
                {
                    return true;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            false
        }
    };
    assert!(until("waiting").await, "offered once the message applied");
    ready.0.store(true, std::sync::atomic::Ordering::Relaxed);
    w.replica.offer_wishes();
    assert!(until("applied").await, "offered again when the host asks");
    assert_eq!(w.setting().await, Some((70, 2)));
    stop.notify_one();
    running.await.unwrap();
}

/// A snapshot beginning — the server provisioning this replica again, after
/// either side was restored from an older copy, say — has the replica say
/// again what each setting is and how it answered each wish, all of it
/// filed before: the server's record of them may be behind.
#[sqlx::test(migrations = false)]
async fn a_snapshot_beginning_has_the_replica_say_its_settings_and_answers_again(pool: SqlitePool) {
    let hand = Hand::default();
    let w = wish_world(pool, &hand).await;
    w.set(40, 1).await;
    w.receive(&down(
        1,
        "",
        vec![wishes(vec![wished(&w.screen(70, 1), 5, false)])],
    ))
    .await;
    w.offer().await;
    let up = w.up(0, false).await.expect("the answer and the setting");
    w.receive(
        &Body::Filed(Filed {
            uploads: up.uploads,
        })
        .encoded(),
    )
    .await;
    assert_eq!(
        w.count("SELECT count(*) FROM sync_notes").await,
        0,
        "all filed"
    );

    let (key, row) = wished(&w.screen(70, 1), 5, false);
    w.receive(&begin(2, "again", vec![wishes(vec![(key, row)])]))
        .await;
    let noted = w
        .texts("SELECT relation FROM sync_notes ORDER BY relation")
        .await;
    assert_eq!(noted, ["screens", "sync_wishes"]);
    w.receive(&end(3, "again", &["sync_wishes"])).await;
    assert_eq!(w.offer().await, 0, "the wish it held, answered");
    let up = w.up(0, false).await.expect("said again");
    let said: BTreeMap<&str, usize> = up
        .tables
        .iter()
        .map(|t| (t.table.as_str(), t.upserts.len()))
        .collect();
    assert_eq!(said, BTreeMap::from([("screens", 1), ("sync_wishes", 1)]));
    let wish = WireWish::decode(
        up.tables
            .iter()
            .find(|t| t.table == "sync_wishes")
            .unwrap()
            .upserts[0]
            .as_slice(),
    )
    .unwrap();
    assert_eq!((wish.answered, wish.outcome.as_str()), (5, "applied"));
}

// ---------------------------------------------------------------------------
// The replica against the protocol's model of it, at random: wishes sent,
// replaced and withdrawn, snapshots, the host's own writes, wishes carried
// out and refused and waiting, uploads answered — the same messages and the
// same decisions to both, and after every step the replica holds the
// settings, the wishes and the answers the model does, and has noted for
// the server what the model has.

/// What a handler decides of a wish, the same for the replica and the
/// model: of its version, and of the round.
fn decide(version: u64, round: u64) -> crate::model::Handled {
    use crate::model::Handled;
    match (version ^ round) % 7 {
        0 | 1 => Handled::Refused,
        2 => Handled::Later,
        _ => Handled::Applied,
    }
}

/// A handler that decides as [`decide`] does, the round its own.
#[derive(Clone, Default)]
struct Decides(Arc<Mutex<u64>>);

impl CarryOut<Screen> for Decides {
    async fn carry_out(&self, wish: Wish<Screen>) -> Outcome {
        let round = *self.0.lock().unwrap();
        match decide(u64::try_from(wish.version).unwrap(), round) {
            crate::model::Handled::Applied => Outcome::Applied,
            crate::model::Handled::Refused => Outcome::Refused("no".to_owned()),
            crate::model::Handled::Later => Outcome::Later("away".to_owned()),
        }
    }
}

fn read_screen(_: &str, bytes: &[u8]) -> Option<(String, i64, i64)> {
    let row: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    let number = |field: &str| row[field].as_str()?.parse::<i64>().ok();
    Some((
        format!("{}|{}", row["replica_id"].as_str()?, row["id"].as_str()?),
        number("level")?,
        number("revision")?,
    ))
}

fn write_screen(_: &str, key: &str, level: i64, revision: i64) -> Vec<u8> {
    let (replica, id) = key.split_once('|').expect("a screen's key");
    json(&Screen {
        replica_id: replica.parse().unwrap(),
        id: id.parse().unwrap(),
        level,
        revision,
    })
}

#[sqlx::test(migrations = false)]
async fn a_replica_carries_wishes_out_as_the_protocol_says_whatever_comes(pool: SqlitePool) {
    walk_wishes(pool, 3).await;
}

#[sqlx::test(migrations = false)]
async fn a_replica_carries_wishes_out_as_the_protocol_says_another_way(pool: SqlitePool) {
    walk_wishes(pool, 11).await;
}

#[sqlx::test(migrations = false)]
async fn a_replica_carries_wishes_out_as_the_protocol_says_a_third_way(pool: SqlitePool) {
    walk_wishes(pool, 29).await;
}

async fn walk_wishes(pool: SqlitePool, seed: u64) {
    use crate::model::{held_wishes, Holder, Model, Row};
    let round = Decides::default();
    let settings = Settings {
        wishes: Wishes::new().on::<Screen>(round.clone()),
        ..Settings::default()
    };
    let w = world_of(
        pool,
        &format!("{TABLES}{SCREENS}"),
        settings,
        golden_codecs(),
    )
    .await;
    let mut walk = crate::testkit::Walk(seed);
    let mut model = Model::default();
    model.receive(
        &ending(BASE, 0, "provisioned", &[]),
        &crate::model::Rules::ANY,
    );
    let mut holder = Holder::default();
    // The server's wishes: by screen, the version and what it wishes.
    let mut asked: BTreeMap<u128, (u64, Screen, bool)> = BTreeMap::new();
    let mut n = 0;
    let screen = |id: u128, level: i64, revision: i64| Screen {
        replica_id: w.me,
        id: Uuid::from_u128(id),
        level,
        revision,
    };
    let row_of = |id: u128| {
        (
            "screens".to_owned(),
            format!("{}|{}", w.me, Uuid::from_u128(id)),
        )
    };
    for step in 0..120 {
        let at = format!("seed {seed}, step {step}");
        let id = 1 + walk.below(3) as u128;
        let row = row_of(id);
        match walk.below(10) {
            // The server wishes a screen, or withdraws its wish.
            0..=2 => {
                n += 1;
                let tables = if walk.below(4) == 0 {
                    asked.remove(&id);
                    vec![deletes(
                        "sync_wishes",
                        vec![wished(&screen(id, 0, 0), 1, false).0],
                    )]
                } else {
                    let held = holder.settings.get(&row).map_or(1, |(_, v)| *v);
                    let based = if walk.below(4) == 0 { held - 1 } else { held };
                    let wanted = screen(id, walk.below(100) as i64, based);
                    let version = walk.next() >> 1;
                    let unchanged = walk.below(2) == 0;
                    asked.insert(id, (version, wanted.clone(), unchanged));
                    vec![wishes(vec![wished(&wanted, version, unchanged)])]
                };
                let message = down(n, "", tables);
                w.receive(&message).await;
                model.receive(&message, &crate::model::Rules::ANY);
                holder.forget(&model);
            }
            // The server provisions it again: every wish it has, in a
            // snapshot.
            3 => {
                let name = format!("s{step}");
                let rows = asked
                    .values()
                    .map(|(version, wanted, unchanged)| wished(wanted, *version, *unchanged))
                    .collect();
                n += 1;
                let begun = numbered(BASE + n, 0, &name, vec![wishes(rows)]);
                w.receive(&begun).await;
                model.receive(&begun, &crate::model::Rules::ANY);
                holder.began(&model);
                n += 1;
                let ended = end(n, &name, &["sync_wishes"]);
                w.receive(&ended).await;
                model.receive(&ended, &crate::model::Rules::ANY);
                holder.forget(&model);
            }
            // The host writes a screen of its own.
            4 => {
                holder.write(&row, walk.below(100) as i64);
                let (level, revision) = holder.settings[&row];
                sqlx::query(
                    "INSERT INTO screens (replica_id, id, level, revision) VALUES ($1, $2, $3, $4) \
                     ON CONFLICT (replica_id, id) DO UPDATE SET level = excluded.level, \
                            revision = excluded.revision",
                )
                .bind(w.me)
                .bind(Uuid::from_u128(id))
                .bind(level)
                .bind(revision)
                .execute(&w.pool)
                .await
                .unwrap();
            }
            // The replica carries out what it holds.
            5..=7 => {
                let r = walk.next();
                *round.0.lock().unwrap() = r;
                w.offer().await;
                holder.carry(&model, read_screen, |wish| decide(wish.version, r));
            }
            // What it wrote goes up, and is filed.
            _ => {
                let real = w.up(0, false).await;
                let modelled = holder.up(write_screen);
                let said = |up: Option<&Up>| -> BTreeSet<(String, String)> {
                    up.map(|up| {
                        up.tables
                            .iter()
                            .filter(|t| t.table == "screens" || t.table == "sync_wishes")
                            .flat_map(|t| {
                                t.keys.iter().map(|k| (t.table.clone(), k.parts.join("|")))
                            })
                            .collect()
                    })
                    .unwrap_or_default()
                };
                assert_eq!(
                    said(real.as_ref()),
                    said(modelled.as_ref()),
                    "{at}: what goes up"
                );
                if let Some(up) = real {
                    w.receive(
                        &Body::Filed(Filed {
                            uploads: up.uploads,
                        })
                        .encoded(),
                    )
                    .await;
                }
                if let Some(up) = modelled {
                    holder.filed(&up.uploads);
                    holder.forget(&model);
                }
            }
        }
        let settings: BTreeMap<Row, (i64, i64)> = sqlx::query_as::<_, (Uuid, Uuid, i64, i64)>(
            "SELECT replica_id, id, level, revision FROM screens",
        )
        .fetch_all(&w.pool)
        .await
        .unwrap()
        .into_iter()
        .map(|(r, i, l, v)| (("screens".to_owned(), format!("{r}|{i}")), (l, v)))
        .collect();
        assert_eq!(settings, holder.settings, "{at}: the settings");
        let held: BTreeMap<String, (u64, Option<(u64, String)>)> =
            sqlx::query_as::<_, (Uuid, i64, Option<i64>, Option<String>)>(
                "SELECT id, version, answered, outcome FROM sync_wishes WHERE retired_at IS NULL",
            )
            .fetch_all(&w.pool)
            .await
            .unwrap()
            .into_iter()
            .map(|(id, version, answered, outcome)| {
                let answer = answered.zip(outcome).map(|(a, o)| (a as u64, o));
                (id.to_string(), (version as u64, answer))
            })
            .collect();
        let modelled: BTreeMap<String, (u64, Option<(u64, String)>)> = held_wishes(&model)
            .into_iter()
            .map(|wish| {
                let answer = holder
                    .answers
                    .get(&wish.id)
                    .map(|(a, o, _)| (*a, o.clone()));
                (wish.id, (wish.version, answer))
            })
            .collect();
        assert_eq!(held, modelled, "{at}: the wishes and their answers");
        let noted: BTreeSet<(String, String)> =
            sqlx::query_as("SELECT relation, row_key FROM sync_notes")
                .fetch_all(&w.pool)
                .await
                .unwrap()
                .into_iter()
                .collect();
        assert_eq!(noted, holder.noted(), "{at}: what is noted");
    }
}
