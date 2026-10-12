//! Wishes on the server: written by the host, sent to the replica a setting
//! belongs to like any row, answered by it, and filed only while they stand.

use super::*;
pub(super) use crate::conformance::Screen;
use crate::envelope::{Key as WireKey, Table as WireTable, Up, Wish};
use crate::model;
use crate::row::Row as _;
use crate::status::WishState;
use crate::wish::{WishRow, TABLE};

const SCREENS: &str = "
CREATE TABLE fixture.screens (
    replica_id UUID NOT NULL, id UUID NOT NULL, level BIGINT NOT NULL, revision BIGINT NOT NULL,
    PRIMARY KEY (replica_id, id));
";

/// The world, with a setting: its replicas provisioned and confirmed.
pub(super) async fn world(pool: crate::DbPool) -> World {
    let (replicas, partition) = founded(&pool, SCREENS).await;
    let codecs = vec![
        Codec::new::<Item, ItemRow>(to_wire, from_wire, None),
        Codec::json::<Screen>(None),
    ];
    provisioned(pool, codecs, replicas, partition).await
}

impl World {
    pub(super) async fn desire(&self, wanted: Screen, note: Option<&str>) -> crate::wish::Wished {
        let mut tx = self.pool.begin().await.unwrap();
        let wished = crate::wish::desire(&mut tx, &self.engine, &self.codecs, wanted, note)
            .await
            .expect("wished");
        tx.commit().await.unwrap();
        wished
    }

    pub(super) async fn wishes(
        &self,
        keys: &[crate::row::Key],
    ) -> Vec<crate::wish::WishOf<Screen>> {
        let mut conn = self.pool.acquire().await.unwrap();
        crate::wish::wishes(&mut conn, &self.codecs, keys)
            .await
            .unwrap()
    }

    /// Passes, delivering to `model` what `replica` is sent, until nothing
    /// is, and confirms it.
    pub(super) async fn catch_up(&self, replica: Uuid, model: &mut Model) {
        let wire = Wire::new(0);
        self.pass(&wire).await;
        while let Some(bytes) = wire.next(replica) {
            model.receive(&bytes, &Rules::ANY);
            wire.acknowledge(replica);
        }
        self.acknowledge(replica, self.last_sent(replica).await)
            .await;
    }
}

pub(super) use crate::model::held_wishes;

/// `replica`'s answer to the wish `wish`, as it sends it up.
pub(super) fn answer(wish: &Wish, outcome: &str, reason: &str) -> Up {
    let answered = Wish {
        answered: wish.version,
        outcome: outcome.to_owned(),
        reason: reason.to_owned(),
        answered_at: "2026-10-12T10:00:00.000000Z".to_owned(),
        ..wish.clone()
    };
    Up {
        tables: vec![WireTable {
            table: TABLE.to_owned(),
            upserts: vec![answered.encode_to_vec()],
            keys: vec![WireKey {
                parts: vec![wish.id.clone()],
            }],
            ..WireTable::default()
        }],
        uploads: vec![7],
        protocol: envelope::PROTOCOL,
        ..Up::default()
    }
}

fn screen(replica: Uuid, level: i64) -> Screen {
    Screen {
        replica_id: replica,
        id: Uuid::from_u128(1),
        level,
        revision: 1,
    }
}

/// A wish reaches the replica its setting belongs to, and no other, as the
/// row as wished — never with an answer, whatever the server holds of one.
#[sqlx::test(migrations = false)]
async fn a_wish_goes_to_the_replica_its_setting_belongs_to(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, b] = world.replicas;
    let (mut at_a, mut at_b) = (world.follower(a), world.follower(b));
    let wished = world.desire(screen(a, 70), Some("ops@example.com")).await;
    sqlx::query("UPDATE sync.sync_wishes SET answered = 5, outcome = 'applied'")
        .execute(&world.pool)
        .await
        .unwrap();
    world.catch_up(a, &mut at_a).await;
    world.catch_up(b, &mut at_b).await;
    let wishes = held_wishes(&at_a);
    assert_eq!(wishes.len(), 1, "{wishes:?}");
    let wish = &wishes[0];
    assert_eq!(wish.id, wished.id.to_string());
    assert_eq!(wish.table, "screens");
    assert_eq!(
        wish.key.as_ref().unwrap().parts,
        [a.to_string(), Uuid::from_u128(1).to_string()]
    );
    assert_eq!(wish.version, wished.version as u64);
    assert_eq!(wish.note, "ops@example.com");
    assert!(!wish.if_unchanged, "`always`, as declared");
    let wanted: Screen =
        crate::wire::json_decode("screens", &Table::of::<Screen>().columns, &wish.wanted).unwrap();
    assert_eq!(wanted, screen(a, 70));
    assert_eq!(
        (
            wish.answered,
            wish.outcome.as_str(),
            wish.answered_at.as_str()
        ),
        (0, "", ""),
        "an answer is the replica's, never sent down"
    );
    assert!(held_wishes(&at_b).is_empty(), "another replica's setting");
}

/// Wishing a row again replaces its wish, under a new version; withdrawing
/// it takes it back from the replica.
#[sqlx::test(migrations = false)]
async fn a_wish_wished_again_is_replaced_and_one_withdrawn_is_taken_back(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, _] = world.replicas;
    let mut model = world.follower(a);
    let first = world.desire(screen(a, 70), None).await;
    world.catch_up(a, &mut model).await;
    let again = world.desire(screen(a, 70), None).await;
    assert_eq!(again.id, first.id, "one wish per setting row");
    assert_ne!(
        again.version, first.version,
        "a new wish, carried out again"
    );
    world.catch_up(a, &mut model).await;
    assert_eq!(held_wishes(&model)[0].version, again.version as u64);
    let key = screen(a, 0).key();
    let mut tx = world.pool.begin().await.unwrap();
    assert!(
        crate::wish::withdraw::<Screen, ()>(&mut tx, &world.codecs, &key)
            .await
            .unwrap()
    );
    assert!(
        !crate::wish::withdraw::<Screen, ()>(&mut tx, &world.codecs, &key)
            .await
            .unwrap()
    );
    tx.commit().await.unwrap();
    world.catch_up(a, &mut model).await;
    assert!(held_wishes(&model).is_empty());
    assert!(world.wishes(&[key]).await.is_empty());
}

/// The replica's answer is filed in the wish, and says where it stands —
/// only while it is of this wish: one of an earlier wish of the row leaves
/// it pending, and one of a wish withdrawn is left out.
#[sqlx::test(migrations = false)]
async fn a_wish_stands_as_its_replica_answered_it(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, b] = world.replicas;
    let mut model = world.follower(a);
    let key = screen(a, 0).key();
    world.desire(screen(a, 70), Some("ops")).await;
    let state = || async {
        let wishes = world.wishes(std::slice::from_ref(&key)).await;
        wishes
            .into_iter()
            .map(|w| (w.state, w.reason, w.wanted.level))
            .collect::<Vec<_>>()
    };
    assert_eq!(state().await, [(WishState::Pending, None, 70)]);
    world.catch_up(a, &mut model).await;
    let first = held_wishes(&model).remove(0);
    crate::testkit::file(
        &world.pool,
        &world.codecs,
        &answer(&first, "waiting", "away"),
        a,
        &(),
    )
    .await;
    assert_eq!(
        state().await,
        [(WishState::Waiting, Some("away".to_owned()), 70)]
    );
    crate::testkit::file(
        &world.pool,
        &world.codecs,
        &answer(&first, "applied", ""),
        a,
        &(),
    )
    .await;
    assert_eq!(state().await, [(WishState::Applied, None, 70)]);

    world.desire(screen(a, 80), None).await;
    assert_eq!(
        state().await,
        [(WishState::Pending, None, 80)],
        "a new wish"
    );
    crate::testkit::file(
        &world.pool,
        &world.codecs,
        &answer(&first, "refused", "no"),
        a,
        &(),
    )
    .await;
    assert_eq!(
        state().await,
        [(WishState::Pending, None, 80)],
        "an answer to the wish before"
    );
    world.catch_up(a, &mut model).await;
    let second = held_wishes(&model).remove(0);
    crate::testkit::file(
        &world.pool,
        &world.codecs,
        &answer(&second, "applied", ""),
        b,
        &(),
    )
    .await;
    assert_eq!(
        state().await,
        [(WishState::Pending, None, 80)],
        "another replica cannot answer it"
    );
    crate::testkit::file(
        &world.pool,
        &world.codecs,
        &answer(&second, "superseded", "changed here"),
        a,
        &(),
    )
    .await;
    assert_eq!(
        state().await,
        [(WishState::Superseded, Some("changed here".to_owned()), 80)]
    );

    let mut tx = world.pool.begin().await.unwrap();
    crate::wish::withdraw::<Screen, ()>(&mut tx, &world.codecs, &key)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    world.catch_up(a, &mut model).await;
    crate::testkit::file(
        &world.pool,
        &world.codecs,
        &answer(&second, "applied", ""),
        a,
        &(),
    )
    .await;
    assert!(state().await.is_empty(), "the server's withdrawal wins");
    assert_eq!(world.count("SELECT count(*) FROM sync.inbox").await, 0);
}

/// A final answer — applied, refused, superseded — stands against a later
/// upload of another answer to the same wish, which the transport may have
/// held back; one the replica re-states, as a snapshot begins, is what it
/// holds now, and is taken.
#[sqlx::test(migrations = false)]
async fn a_final_answer_stands_against_a_late_one_unless_restated(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, _] = world.replicas;
    let mut model = world.follower(a);
    let key = screen(a, 0).key();
    world.desire(screen(a, 70), None).await;
    world.catch_up(a, &mut model).await;
    let wish = held_wishes(&model).remove(0);
    let file = |up: Up| {
        let world = &world;
        async move { crate::testkit::file(&world.pool, &world.codecs, &up, a, &()).await }
    };
    let state = || async {
        let wishes = world.wishes(std::slice::from_ref(&key)).await;
        wishes.into_iter().map(|w| w.state).collect::<Vec<_>>()
    };
    file(answer(&wish, "applied", "")).await;
    file(answer(&wish, "waiting", "away")).await;
    assert_eq!(state().await, [WishState::Applied], "a waiting from before");
    let mut restated = answer(&wish, "waiting", "away");
    restated.tables[0].restated = true;
    file(restated).await;
    assert_eq!(state().await, [WishState::Waiting], "what it holds now");
    assert_eq!(world.count("SELECT count(*) FROM sync.inbox").await, 0);
}

/// A replica's settings and answers are compared as the server holds them:
/// one that differs — a server restored from a copy older than what the
/// replica uploaded since — is provisioned again, and the server lets go of
/// what it holds of the setting that differs, for the replica to state
/// again as its snapshot begins.
#[sqlx::test(migrations = false)]
async fn a_replica_whose_settings_or_answers_differ_is_provisioned_again(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, _] = world.replicas;
    let mut model = world.follower(a);
    let mut holder = crate::model::Holder::default();
    let row = ("screens".to_owned(), format!("{a}|{}", Uuid::from_u128(1)));
    holder.write(&row, 40);
    world.desire(screen(a, 70), None).await;
    world.catch_up(a, &mut model).await;
    holder.carry(&model, read_setting, |_| crate::model::Handled::Applied);
    let up = holder.up(write_setting).unwrap();
    crate::testkit::file(&world.pool, &world.codecs, &up, a, &()).await;
    holder.filed(&up.uploads);
    let held = || async {
        sqlx::query_as::<_, (i64, i64)>("SELECT level, revision FROM fixture.screens")
            .fetch_all(&world.pool)
            .await
            .unwrap()
    };
    assert_eq!(held().await, [(70, 2)]);

    let resyncs = "SELECT count(*) FROM sync.notes WHERE kind = 'resync' AND done_at IS NULL";
    let compare = |held: &crate::model::Holder| {
        let mut up = model.digests(&[]);
        up.digests = held.digests(&model, &["screens", TABLE]);
        let world = &world;
        async move {
            sqlx::raw_sql(
                "UPDATE sync.replicas SET compared_at = NULL, drifted_at = NULL; \
                 UPDATE sync.notes SET done_at = now() WHERE kind = 'resync'",
            )
            .execute(&world.pool)
            .await
            .unwrap();
            crate::testkit::file(&world.pool, &world.codecs, &up, a, &()).await;
            world.count(resyncs).await
        }
    };
    assert_eq!(
        compare(&holder).await,
        0,
        "it holds what the server records"
    );

    let mut before = holder.clone();
    before.answers.clear();
    assert_eq!(compare(&before).await, 1, "answers it does not hold");
    assert_eq!(held().await, [(70, 2)], "a setting that agrees is kept");

    let mut older = holder.clone();
    older.settings.insert(row.clone(), (40, 1));
    assert_eq!(compare(&older).await, 1, "a setting of another version");
    assert!(
        held().await.is_empty(),
        "the server lets go of the setting that differs"
    );
}

/// A wish that does not decode as the row it says — another build's
/// encoding, say — is no answer the server files, nor a wish it reads.
#[sqlx::test(migrations = false)]
async fn an_answer_that_does_not_read_is_kept_for_a_fix(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, _] = world.replicas;
    let mut model = world.follower(a);
    world.desire(screen(a, 70), None).await;
    world.catch_up(a, &mut model).await;
    let wish = held_wishes(&model).remove(0);
    crate::testkit::file(
        &world.pool,
        &world.codecs,
        &answer(&wish, "forgotten", ""),
        a,
        &(),
    )
    .await;
    assert_eq!(world.count("SELECT count(*) FROM sync.inbox").await, 1);
    let stored: Vec<WishRow> = sqlx::query_as("SELECT * FROM sync.sync_wishes")
        .fetch_all(&world.pool)
        .await
        .unwrap();
    assert_eq!(stored[0].outcome, None);
}

/// A host table cannot take a name the engine keeps for its own.
#[test]
fn a_table_named_as_the_engines_is_refused() {
    #[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
    #[row(table = "sync_mine", mirror = "catalog", sync = "broadcast")]
    struct Mine {
        id: Uuid,
    }
    let refused = crate::wish::with_wishes(vec![Codec::<()>::json::<Mine>(None)]);
    assert!(
        matches!(&refused, Err(Error::Misuse(why)) if why.contains("is the engine's")),
        "{refused:?}"
    );
    let none = crate::wish::with_wishes(vec![Codec::<()>::json::<Mine2>(None)]).unwrap();
    assert_eq!(none.len(), 1, "no setting, no table of wishes");
    #[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
    #[row(table = "plain", mirror = "catalog", sync = "broadcast")]
    struct Mine2 {
        id: Uuid,
    }
}

/// A replica with a wish it has not answered is catching up, whatever
/// else it holds; one that answered, even waiting, is not. Each replica counts
/// its wishes by where they stand, and the listing puts those that need
/// attention first.
#[sqlx::test(migrations = false)]
async fn a_replica_with_a_wish_unanswered_is_catching_up(pool: crate::DbPool) {
    use crate::status::{Page, ReplicaState, WishCounts};
    let world = world(pool).await;
    let [a, b] = world.replicas;
    let (mut at_a, mut at_b) = (world.follower(a), world.follower(b));
    let other = |id: u128, level: i64| Screen {
        id: Uuid::from_u128(id),
        ..screen(a, level)
    };
    world.desire(screen(a, 70), None).await;
    world.desire(other(2, 10), Some("night")).await;
    world.desire(other(3, 20), Some("module")).await;
    world.desire(other(4, 30), Some("stale")).await;
    world.desire(screen(b, 50), None).await;
    world.catch_up(a, &mut at_a).await;
    world.catch_up(b, &mut at_b).await;
    let state_of = |overview: &crate::status::Overview, replica: Uuid| {
        let r = overview
            .replicas
            .first
            .iter()
            .find(|r| r.replica == replica)
            .unwrap();
        (r.state, r.wishes)
    };
    let overview = world.overview(Scope::All).await;
    assert_eq!(
        state_of(&overview, a),
        (
            ReplicaState::CatchingUp,
            WishCounts {
                pending: 4,
                ..WishCounts::default()
            }
        )
    );
    let held = held_wishes(&at_a);
    let noted = |note: &str| held.iter().find(|w| w.note == note).unwrap();
    for (wish, outcome) in [
        (noted("night"), "refused"),
        (noted(""), "applied"),
        (noted("module"), "waiting"),
        (noted("stale"), "superseded"),
    ] {
        crate::testkit::file(
            &world.pool,
            &world.codecs,
            &answer(wish, outcome, "why"),
            a,
            &(),
        )
        .await;
    }
    let overview = world.overview(Scope::All).await;
    assert_eq!(
        state_of(&overview, a),
        (
            ReplicaState::Synced,
            WishCounts {
                waiting: 1,
                refused: 1,
                superseded: 1,
                ..WishCounts::default()
            }
        ),
        "every wish answered — one waiting for its host, which the replica tries again"
    );
    assert_eq!(state_of(&overview, b).0, ReplicaState::CatchingUp);

    let mut conn = world.pool.acquire().await.unwrap();
    let listing = world
        .engine
        .wishes(&mut conn, Scope::All, None, Page::new(0, 10))
        .await
        .unwrap();
    let states: Vec<(Uuid, WishState)> = listing
        .wishes
        .first
        .iter()
        .map(|w| (w.replica, w.state))
        .collect();
    assert_eq!(
        states,
        [
            (a, WishState::Refused),
            (a, WishState::Superseded),
            (a, WishState::Waiting),
            (b, WishState::Pending),
            (a, WishState::Applied)
        ]
    );
    assert_eq!(listing.wishes.total, 5);
    let only_b = world
        .engine
        .wishes(&mut conn, Scope::Replicas(&[b]), None, Page::new(0, 10))
        .await
        .unwrap();
    assert_eq!(only_b.wishes.total, 1, "a scope sees its replicas' only");
    let pending = world
        .engine
        .wishes(
            &mut conn,
            Scope::All,
            Some(WishState::Pending),
            Page::new(5, 10),
        )
        .await
        .unwrap();
    assert_eq!((pending.wishes.total, pending.wishes.first.len()), (1, 0));

    let mut stable = super::status::Stable::default();
    let json =
        serde_json::to_string_pretty(&stable.of(serde_json::to_value(&listing).unwrap())).unwrap();
    let text = format!(
        "// Generated by magic_sync's `a_replica_with_a_wish_unanswered_is_catching_up`: \
         UPDATE_CONTRACT=1 rewrites it.\n\
         import type {{ WishListing }} from '../types.js'\n\n\
         export default {json} satisfies WishListing\n"
    );
    crate::status::tests::matches_contract("fixtures/wishes.ts", &text);
}

// ---------------------------------------------------------------------------
// The walk: wishes, local writes, answers and restores of either side, at
// random, against replicas written from the protocol; and at every point the
// system settles, both sides show the same.

/// A setting carried out only while unchanged.
#[derive(
    sqlx::FromRow, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, crate::Row,
)]
#[row(
    table = "dials",
    schema = "fixture",
    mirror = "setting",
    key = "replica_id, id",
    origin = "replica_id",
    apply = "if_unchanged"
)]
struct Dial {
    replica_id: Uuid,
    id: Uuid,
    #[row(desired)]
    level: i64,
    #[row(version)]
    revision: i64,
}

const DIALS: &str = "
CREATE TABLE fixture.dials (
    replica_id UUID NOT NULL, id UUID NOT NULL, level BIGINT NOT NULL, revision BIGINT NOT NULL,
    PRIMARY KEY (replica_id, id));
";

/// Both settings' rows travel as the same JSON: their key, level and
/// revision.
fn read_setting(_: &str, bytes: &[u8]) -> Option<(String, i64, i64)> {
    let row: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    let number = |field: &str| row[field].as_str()?.parse::<i64>().ok();
    Some((
        format!("{}|{}", row["replica_id"].as_str()?, row["id"].as_str()?),
        number("level")?,
        number("revision")?,
    ))
}

fn write_setting(_: &str, key: &str, level: i64, revision: i64) -> Vec<u8> {
    let (replica, id) = key.split_once('|').expect("a setting's key");
    serde_json::to_vec(&serde_json::json!({
        "replica_id": replica, "id": id,
        "level": level.to_string(), "revision": revision.to_string(),
    }))
    .unwrap()
}

/// A replica of the walk: what the protocol says it holds, and its own.
#[derive(Clone, Default)]
struct Walked {
    model: Model,
    holder: crate::model::Holder,
}

impl World {
    /// Passes, deliveries and uploads until nothing moves: each replica
    /// says what it applied and sends what it wrote, the server files it and
    /// answers; a snapshot beginning has a replica say its settings again.
    async fn settle_wishes(
        &self,
        wire: &Wire,
        replicas: &mut BTreeMap<Uuid, Walked>,
        walk: &mut crate::testkit::Walk,
    ) {
        // Time enough passes for every wait to be over: a replica that
        // asks to be provisioned again too soon is, then, and digests are
        // compared, and acted on, again.
        sqlx::raw_sql(
            "UPDATE sync.replicas SET retry_after = NULL, asked_at = NULL, compared_at = NULL, \
                    drifted_at = NULL; \
             UPDATE sync.notes SET noted_at = now() WHERE noted_at > now()",
        )
        .execute(&self.pool)
        .await
        .unwrap();
        for _ in 0..80 {
            let passed = self.pass(wire).await;
            let mut moved = passed.done + passed.sent > 0;
            for (replica, walked) in replicas.iter_mut() {
                while let Some(bytes) = wire.next(*replica) {
                    let begins = match envelope::ToReplica::decode(bytes.as_slice())
                        .ok()
                        .and_then(|m| m.body)
                    {
                        Some(Body::Down(d)) => d.previous == 0,
                        Some(Body::EndSnapshot(e)) => e.previous == 0,
                        _ => false,
                    };
                    let received = walked.model.receive(&bytes, &Rules::ANY);
                    if begins
                        && matches!(
                            received,
                            crate::model::Received::Applied | crate::model::Received::Ended
                        )
                    {
                        walked.holder.began(&walked.model);
                    }
                    walked.holder.forget(&walked.model);
                    wire.acknowledge(*replica);
                    moved = true;
                }
                // What it applied and what it wrote, in either order — its
                // writes now and then before its confirmation — and the
                // answer to an upload now and then lost: it goes again.
                let first = walk.below(2) == 0;
                for confirming in [first, !first] {
                    if confirming {
                        if let Some(up) = walked.model.up() {
                            crate::testkit::file(&self.pool, &self.codecs, &up, *replica, &())
                                .await;
                            walked.model.delivered(&up);
                            moved = true;
                        }
                    } else if let Some(up) = walked.holder.up(write_setting) {
                        crate::testkit::file(&self.pool, &self.codecs, &up, *replica, &()).await;
                        if walk.below(5) > 0 {
                            walked.holder.filed(&up.uploads);
                            walked.holder.forget(&walked.model);
                        }
                        moved = true;
                    }
                }
            }
            if !moved {
                return;
            }
        }
        panic!("did not settle");
    }

    /// The server's copy of `replica`'s settings.
    async fn settings_of(&self, replica: Uuid) -> BTreeMap<model::Row, (i64, i64)> {
        let mut out = BTreeMap::new();
        for table in ["screens", "dials"] {
            let rows: Vec<(Uuid, Uuid, i64, i64)> = sqlx::query_as(&format!(
                "SELECT replica_id, id, level, revision FROM fixture.{table} WHERE replica_id = $1"
            ))
            .bind(replica)
            .fetch_all(&self.pool)
            .await
            .unwrap();
            for (replica, id, level, revision) in rows {
                out.insert(
                    (table.to_owned(), format!("{replica}|{id}")),
                    (level, revision),
                );
            }
        }
        out
    }

    /// The server's wishes for `replica`: id, version, state, reason.
    async fn wishes_of(&self, replica: Uuid) -> BTreeMap<String, (u64, String)> {
        let rows: Vec<(Uuid, i64, String)> = sqlx::query_as(&format!(
            "SELECT w.id, w.version, {} FROM sync.sync_wishes w WHERE w.replica_id = $1",
            crate::wish::STATE
        ))
        .bind(replica)
        .fetch_all(&self.pool)
        .await
        .unwrap();
        rows.into_iter()
            .map(|(id, version, state)| (id.to_string(), (version as u64, state)))
            .collect()
    }
}

/// Wishes of two settings — one carried out always, one only while
/// unchanged — wished, wished again and withdrawn at random, while the
/// replicas write their settings themselves, carry wishes out or refuse them
/// or cannot yet, and either side is restored from an older copy: once the
/// system settles, every setting is the same on the server and on its
/// replica, every wish the replica holds is the one the server has, and its
/// answer the one the server shows; and no replica carried a wish out twice
/// but after it was restored from an older copy.
#[sqlx::test(migrations = false)]
async fn wishes_walked_at_random_leave_both_sides_showing_the_same(pool: crate::DbPool) {
    walk_wishes(pool, 5).await;
}

#[sqlx::test(migrations = false)]
async fn wishes_walked_at_random_another_way_leave_both_sides_showing_the_same(
    pool: crate::DbPool,
) {
    walk_wishes(pool, 7).await;
}

#[sqlx::test(migrations = false)]
async fn wishes_walked_at_random_a_third_way_leave_both_sides_showing_the_same(
    pool: crate::DbPool,
) {
    walk_wishes(pool, 42).await;
}

async fn walk_wishes(pool: crate::DbPool, seed: u64) {
    let (replicas, partition) = founded(&pool, &format!("{SCREENS}{DIALS}")).await;
    let codecs = vec![
        Codec::new::<Item, ItemRow>(to_wire, from_wire, None),
        Codec::json::<Screen>(None),
        Codec::json::<Dial>(None),
    ];
    let world = &provisioned(pool, codecs, replicas, partition).await;
    use crate::model::Handled;
    let mut walk = crate::testkit::Walk(seed);
    let wire = Wire::new(seed);
    let mut walked: BTreeMap<Uuid, Walked> = world
        .replicas
        .iter()
        .map(|r| {
            (
                *r,
                Walked {
                    model: world.follower(*r),
                    holder: crate::model::Holder::default(),
                },
            )
        })
        .collect();
    let rows = |replica: Uuid| -> Vec<model::Row> {
        ["screens", "dials"]
            .into_iter()
            .flat_map(|table| {
                (1..=3_u128).map(move |id| {
                    (
                        table.to_owned(),
                        format!("{replica}|{}", Uuid::from_u128(id)),
                    )
                })
            })
            .collect()
    };
    for (replica, w) in &mut walked {
        for row in rows(*replica)
            .into_iter()
            .filter(|(_, key)| !key.ends_with('3'))
        {
            w.holder.write(&row, 0);
        }
    }
    world.settle_wishes(&wire, &mut walked, &mut walk).await;
    // Per replica, each wish it answered for good since its database was
    // last restored: its handler is never handed one of them again.
    let mut answered: BTreeMap<Uuid, BTreeSet<(String, u64)>> = BTreeMap::new();
    let mut copies: BTreeMap<Uuid, Walked> = BTreeMap::new();
    // What the walk did, so that a seed that misses a case says so.
    let mut did: BTreeSet<&str> = BTreeSet::new();
    let mut backed_up = false;
    let mut uploaded = false;
    for _ in 0..240 {
        let replica = walk.pick(&world.replicas).unwrap();
        let all = rows(replica);
        let row = all[walk.below(all.len())].clone();
        let (table, key) = (row.0.clone(), row.1.clone());
        let id = Uuid::parse_str(key.split_once('|').unwrap().1).unwrap();
        match walk.below(100) {
            0..30 => {
                let level = walk.below(100) as i64 + 1;
                let seen = world
                    .settings_of(replica)
                    .await
                    .get(&row)
                    .map_or(1, |(_, v)| *v);
                let based = if walk.below(4) == 0 { seen - 1 } else { seen };
                let mut tx = world.pool.begin().await.unwrap();
                if table == "screens" {
                    let wanted = Screen {
                        replica_id: replica,
                        id,
                        level,
                        revision: based,
                    };
                    crate::wish::desire(&mut tx, &world.engine, &world.codecs, wanted, None)
                        .await
                        .unwrap();
                } else {
                    let wanted = Dial {
                        replica_id: replica,
                        id,
                        level,
                        revision: based,
                    };
                    crate::wish::desire(&mut tx, &world.engine, &world.codecs, wanted, None)
                        .await
                        .unwrap();
                }
                tx.commit().await.unwrap();
            }
            30..38 => {
                let key = vec![
                    crate::row::KeyPart::Uuid(replica),
                    crate::row::KeyPart::Uuid(id),
                ];
                let mut tx = world.pool.begin().await.unwrap();
                if table == "screens" {
                    crate::wish::withdraw::<Screen, ()>(&mut tx, &world.codecs, &key)
                        .await
                        .unwrap();
                } else {
                    crate::wish::withdraw::<Dial, ()>(&mut tx, &world.codecs, &key)
                        .await
                        .unwrap();
                }
                tx.commit().await.unwrap();
            }
            38..47 => {
                let level = walk.below(100) as i64 + 1;
                walked.get_mut(&replica).unwrap().holder.write(&row, level);
            }
            47..50 => walked.get_mut(&replica).unwrap().holder.delete(&row),
            50..52 => {
                // Its writes go up, and nothing comes down: a server
                // restored right after has forgotten them, and only a
                // digest says so.
                let w = walked.get_mut(&replica).unwrap();
                if let Some(up) = w.holder.up(write_setting) {
                    crate::testkit::file(&world.pool, &world.codecs, &up, replica, &()).await;
                    w.holder.filed(&up.uploads);
                    w.holder.forget(&w.model);
                    uploaded |= backed_up;
                }
            }
            52..53 => {
                let w = &walked[&replica];
                let mut up = w.model.digests(&[]);
                up.digests = w
                    .holder
                    .digests(&w.model, &["screens", "dials", crate::wish::TABLE]);
                crate::testkit::file(&world.pool, &world.codecs, &up, replica, &()).await;
            }
            53..73 => {
                let w = walked.get_mut(&replica).unwrap();
                let choice = walk.below(100);
                let handled = w.holder.carry(&w.model, read_setting, |_| match choice {
                    0..55 => Handled::Applied,
                    55..80 => Handled::Refused,
                    _ => Handled::Later,
                });
                let done = answered.entry(replica).or_default();
                for wish in handled {
                    assert!(
                        !done.contains(&wish),
                        "seed {seed}: {replica} carried {wish:?} out again"
                    );
                }
                for (id, (version, outcome, _)) in &w.holder.answers {
                    if let Some(said) = ["applied", "refused", "superseded"]
                        .into_iter()
                        .find(|o| o == outcome)
                    {
                        done.insert((id.clone(), *version));
                        did.insert(said);
                    }
                }
            }
            73..90 => world.settle_wishes(&wire, &mut walked, &mut walk).await,
            90..93 => {
                copies.insert(replica, walked[&replica].clone());
            }
            93..96 => {
                if let Some(copy) = copies.remove(&replica) {
                    // Its database, as the copy was; what it answered
                    // since is forgotten with it.
                    walked.insert(replica, copy);
                    answered.remove(&replica);
                    did.insert("a replica restored");
                }
            }
            _ => {
                if !backed_up {
                    crate::testkit::back_up(
                        &mut world.pool.acquire().await.unwrap(),
                        &["fixture", "sync"],
                    )
                    .await;
                    backed_up = true;
                } else {
                    backed_up = false;
                    if walk.below(2) == 0 {
                        let w = walked.get_mut(&replica).unwrap();
                        w.holder.write(&row, walk.below(100) as i64 + 1);
                        let up = w.holder.up(write_setting).unwrap();
                        crate::testkit::file(&world.pool, &world.codecs, &up, replica, &()).await;
                        w.holder.filed(&up.uploads);
                        w.holder.forget(&w.model);
                        uploaded = true;
                    }
                    if std::mem::take(&mut uploaded) {
                        did.insert("uploads only, then the server restored");
                    }
                    // A server restored from an older copy wishes again
                    // what it wished then: a wish answered since may be
                    // carried out again.
                    answered.clear();
                    did.insert("the server restored");
                    crate::testkit::restore(
                        &mut world.pool.acquire().await.unwrap(),
                        &["fixture", "sync"],
                    )
                    .await;
                }
            }
        }
    }

    // Settled: every wish carried out or refused — none waits — and every
    // replica says what it holds, which finds a server restored from an
    // older copy.
    for _ in 0..10 {
        world.settle_wishes(&wire, &mut walked, &mut walk).await;
        let mut moved = false;
        for (replica, w) in &mut walked {
            let mut up = w.model.digests(&[]);
            up.digests = w
                .holder
                .digests(&w.model, &["screens", "dials", crate::wish::TABLE]);
            crate::testkit::file(&world.pool, &world.codecs, &up, *replica, &()).await;
            let handled = w.holder.carry(&w.model, read_setting, |wish| {
                if wish.version % 3 == 0 {
                    did.insert("refused");
                    Handled::Refused
                } else {
                    Handled::Applied
                }
            });
            moved |= !handled.is_empty();
        }
        world.settle_wishes(&wire, &mut walked, &mut walk).await;
        if !moved && walked.values().all(|w| w.holder.noted().is_empty()) {
            break;
        }
    }
    assert_eq!(
        did,
        BTreeSet::from([
            "a replica restored",
            "the server restored",
            "uploads only, then the server restored",
            "applied",
            "refused",
            "superseded"
        ]),
        "seed {seed}: what the walk did"
    );
    for (replica, w) in &walked {
        assert!(
            w.holder.noted().is_empty(),
            "seed {seed}: {replica} has writes unfiled"
        );
        assert_eq!(
            world.settings_of(*replica).await,
            w.holder.settings,
            "seed {seed}: {replica}'s settings, server and replica"
        );
        let held: BTreeMap<String, u64> = crate::model::held_wishes(&w.model)
            .into_iter()
            .map(|wish| (wish.id, wish.version))
            .collect();
        let server = world.wishes_of(*replica).await;
        assert_eq!(
            server
                .iter()
                .map(|(id, (v, _))| (id.clone(), *v))
                .collect::<BTreeMap<_, _>>(),
            held,
            "seed {seed}: {replica}'s wishes, server and replica; it applied {:?}, gap {}; the server: {:?}",
            w.model.applied,
            w.model.gap,
            sqlx::query_as::<_, (Option<i64>, Option<i64>, Option<String>)>(
                "SELECT c.last_sequence, r.applied_sequence, r.last_error FROM sync.chains c \
                   FULL JOIN sync.replicas r USING (replica_id) WHERE replica_id = $1"
            )
            .bind(replica)
            .fetch_optional(&world.pool)
            .await
            .unwrap(),
        );
        for (id, (version, state)) in &server {
            let (answered, outcome, _) = &w.holder.answers[id];
            assert_eq!(
                (answered, outcome),
                (version, state),
                "seed {seed}: {replica}'s answer to {id}, replica and server"
            );
        }
    }
}
