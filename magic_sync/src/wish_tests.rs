//! Wishes on the server: written by the host, sent to the replica a setting
//! belongs to like any row, answered by it, and filed only while they stand.

use super::*;
use crate::envelope::{Key as WireKey, Table as WireTable, Up, Wish};
use crate::row::Row as _;
use crate::status::WishState;
use crate::wish::{WishRow, TABLE};

/// A setting: a replica's screen, whose level the server may wish.
#[derive(
    sqlx::FromRow, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, crate::Row,
)]
#[row(
    table = "screens",
    schema = "fixture",
    mirror = "setting",
    key = "replica_id, id",
    origin = "replica_id"
)]
pub(super) struct Screen {
    pub(super) replica_id: Uuid,
    pub(super) id: Uuid,
    #[row(desired)]
    pub(super) level: i64,
    #[row(version)]
    pub(super) revision: i64,
}

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
        let wished = crate::wish::desire(&mut tx, &self.codecs, wanted, note)
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

/// The wishes a modelled replica holds, as the server sent them.
pub(super) fn held_wishes(model: &Model) -> Vec<Wish> {
    model
        .rows
        .iter()
        .filter(|((table, _), _)| table == TABLE)
        .map(|(_, bytes)| Wish::decode(bytes.as_slice()).expect("a wish"))
        .collect()
}

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
    assert_eq!(wish.replica_id, a.to_string());
    assert_eq!(wish.table, "screens");
    assert_eq!(
        wish.key.as_ref().unwrap().parts,
        [a.to_string(), Uuid::from_u128(1).to_string()]
    );
    assert_eq!(wish.version, wished.version as u64);
    assert_eq!(wish.note, "ops@example.com");
    assert!(!wish.if_unchanged, "`always`, as declared");
    let screens = world
        .codecs
        .iter()
        .find(|c| c.table.name == "screens")
        .unwrap();
    let wanted: Screen = screens.decode_row(&wish.wanted).unwrap();
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
    assert!(crate::wish::withdraw::<Screen>(&mut tx, &key)
        .await
        .unwrap());
    assert!(!crate::wish::withdraw::<Screen>(&mut tx, &key)
        .await
        .unwrap());
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
    crate::wish::withdraw::<Screen>(&mut tx, &key)
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

/// A host table cannot take the name of the engine's own.
#[test]
fn a_table_named_as_the_engines_wishes_is_refused() {
    #[derive(sqlx::FromRow, Clone, Debug, serde::Serialize, serde::Deserialize, crate::Row)]
    #[row(table = "sync_wishes", mirror = "catalog", sync = "broadcast")]
    struct Mine {
        id: Uuid,
    }
    let refused = crate::wish::with_wishes(vec![Codec::<()>::json::<Mine>(None)]);
    assert!(
        matches!(&refused, Err(Error::Misuse(why)) if why.contains("engine's table of wishes")),
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
