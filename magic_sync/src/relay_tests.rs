//! The relay against failures, one at a time: whatever fails is set aside,
//! everything else carries on, and the set-aside part gets through once the
//! cause is gone.

use super::*;
use crate::plan::Replicas;
use crate::row::Table;
use crate::status::{ReplicaState, RowState, Scope};
use crate::testkit::{from_wire, to_wire, Item, ItemRow, BUGGY};
use prost::Message as _;
use sqlx::Connection as _;
use std::sync::Mutex;

const SCHEMA: &str = "
CREATE SCHEMA fixture;
CREATE TABLE fixture.t_replicas (id UUID PRIMARY KEY, partition_id UUID NOT NULL, live BOOLEAN NOT NULL);
CREATE TABLE fixture.items (
    id UUID PRIMARY KEY, partition_id UUID NOT NULL, name TEXT NOT NULL, retired_at TIMESTAMPTZ);
CREATE VIEW items AS SELECT * FROM fixture.items WHERE retired_at IS NULL;
";

/// How a pass reads: the notes, the plan and its rows in one snapshot.
const SNAPSHOT: &str = "BEGIN ISOLATION LEVEL REPEATABLE READ";

const REPLICAS: Replicas = Replicas {
    relation: "fixture.t_replicas",
    table: "t_replicas",
    live: "live",
    partition: Some("partition_id"),
};

/// A transport that records what it is given, and fails for the replicas
/// told to: `true` as a network would, `false` as a refusal for good — or
/// for the next few sends to a replica only, as a network would.
#[derive(Default)]
struct Fake {
    sent: Mutex<Vec<(Uuid, Body)>>,
    failing: Mutex<BTreeMap<Uuid, bool>>,
    fail_next: Mutex<BTreeMap<Uuid, usize>>,
}

impl Transport for Fake {
    fn send(
        &self,
        replica: Uuid,
        message: Vec<u8>,
        _id: &str,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        let message = body(&message);
        let next = self
            .fail_next
            .lock()
            .unwrap()
            .get_mut(&replica)
            .is_some_and(|n| {
                let fails = *n > 0;
                *n = n.saturating_sub(1);
                fails
            });
        let failing = self.failing.lock().unwrap().get(&replica).copied();
        let result = match failing.or(next.then_some(true)) {
            Some(transient) => Err(Error::Transport {
                reason: format!("{replica} unreachable"),
                transient,
            }),
            None => {
                self.sent.lock().unwrap().push((replica, message));
                Ok(())
            }
        };
        async move { result }
    }
}

/// What a message carries, as a replica reads it.
fn body(bytes: &[u8]) -> Body {
    let message = envelope::ToReplica::decode(bytes).expect("a message");
    assert_eq!(message.protocol, envelope::PROTOCOL);
    message.body.expect("a body")
}

impl Fake {
    /// The item names each replica was sent, taken.
    fn delivered(&self) -> BTreeMap<Uuid, BTreeSet<String>> {
        let mut out: BTreeMap<Uuid, BTreeSet<String>> = BTreeMap::new();
        for (replica, message) in self.sent.lock().unwrap().drain(..) {
            let Body::Down(down) = message else {
                continue;
            };
            for table in down.tables {
                for bytes in table.upserts {
                    let row = ItemRow::decode(bytes.as_slice()).expect("an item");
                    out.entry(replica).or_default().insert(row.name);
                }
            }
        }
        out
    }
}

struct World {
    pool: crate::DbPool,
    engine: Engine,
    codecs: Vec<Codec<()>>,
    replicas: [Uuid; 2],
    partition: Uuid,
}

async fn world(pool: crate::DbPool) -> World {
    let mut conn = pool.acquire().await.expect("a connection");
    crate::plan::tests::install(&mut conn).await;
    sqlx::raw_sql(SCHEMA)
        .execute(&mut *conn)
        .await
        .expect("schema");
    let partition = Uuid::now_v7();
    let replicas = [Uuid::now_v7(), Uuid::now_v7()];
    for replica in replicas {
        sqlx::query("INSERT INTO fixture.t_replicas VALUES ($1, $2, true)")
            .bind(replica)
            .bind(partition)
            .execute(&mut *conn)
            .await
            .expect("a replica");
    }
    let table = Table::of::<Item>();
    let mut tx = conn.begin().await.expect("a transaction");
    let engine = Engine::install(&mut tx, &[&table], REPLICAS)
        .await
        .expect("the engine");
    tx.commit().await.expect("installed");
    World {
        pool,
        engine,
        codecs: vec![Codec::new::<Item, ItemRow>(to_wire, from_wire, None)],
        replicas,
        partition,
    }
}

impl World {
    fn relay(&self) -> Relay<'_, ()> {
        Relay {
            engine: &self.engine,
            codecs: &self.codecs,
            limits: Limits {
                rows: 100,
                bytes: 1 << 20,
            },
            batch: 100,
            timeout: std::time::Duration::from_secs(5),
            backoff: (
                std::time::Duration::from_secs(2),
                std::time::Duration::from_secs(900),
            ),
        }
    }

    /// One pass, as a host runs it.
    async fn pass(&self, transport: &Fake) -> Outcome {
        self.pass_with(transport).await
    }

    async fn pass_with<T: Transport>(&self, transport: &T) -> Outcome {
        self.pass_relay(&self.relay(), transport).await
    }

    async fn pass_relay<T: Transport>(&self, relay: &Relay<'_, ()>, transport: &T) -> Outcome {
        let mut tx = self.pool.begin_with(SNAPSHOT).await.expect("a transaction");
        let prepared = relay.prepare(&mut tx).await.expect("prepared");
        tx.rollback().await.expect("rollback");
        let Some(prepared) = prepared else {
            return Outcome::default();
        };
        let sent = relay.publish(transport, prepared).await;
        let mut tx = self.pool.begin().await.expect("a transaction");
        let outcome = relay.record(&mut tx, sent).await.expect("recorded");
        tx.commit().await.expect("commit");
        outcome
    }

    async fn item(&self, name: &str) -> Uuid {
        let id = Uuid::now_v7();
        self.exec(
            "INSERT INTO fixture.items (id, partition_id, name) VALUES ($1, $2, $3)",
            id,
            name,
        )
        .await;
        id
    }

    async fn exec(&self, sql: &str, id: Uuid, text: &str) {
        sqlx::query(sql)
            .bind(id)
            .bind(self.partition)
            .bind(text)
            .execute(&self.pool)
            .await
            .expect(sql);
    }

    async fn count(&self, sql: &str) -> i64 {
        sqlx::query_scalar(sql)
            .fetch_one(&self.pool)
            .await
            .expect(sql)
    }

    async fn held(&self, replica: Uuid) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM sync.held WHERE replica_id = $1")
            .bind(replica)
            .fetch_one(&self.pool)
            .await
            .expect("held")
    }
}

fn names(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|n| (*n).to_owned()).collect()
}

#[sqlx::test(migrations = false)]
async fn a_row_that_does_not_encode_waits_in_quarantine_and_holds_back_nothing(
    pool: crate::DbPool,
) {
    let world = world(pool).await;
    let [a, b] = world.replicas;
    let wire = Fake::default();
    world.item("good").await;
    world.item("poison").await;

    let outcome = world.pass(&wire).await;

    let delivered = wire.delivered();
    assert_eq!(delivered[&a], names(&["good"]));
    assert_eq!(delivered[&b], names(&["good"]));
    assert_eq!(
        outcome.quarantined, 2,
        "the bad row, for each replica owed it"
    );
    assert_eq!(world.held(a).await, 1, "what was not sent is not held");
    assert_eq!(
        world
            .count("SELECT count(*) FROM sync.notes WHERE done_at IS NULL")
            .await,
        0
    );

    // A deploy fixes the encoding; the retry gets it there and empties the
    // quarantine, with no one touching the data.
    BUGGY.set(false);
    assert_eq!(
        retry_quarantine(&mut world.pool.acquire().await.unwrap())
            .await
            .unwrap(),
        2
    );
    world.pass(&wire).await;

    let delivered = wire.delivered();
    assert_eq!(delivered[&a], names(&["poison"]));
    assert_eq!(delivered[&b], names(&["poison"]));
    for replica in [a, b] {
        world
            .acknowledge(replica, world.last_sent(replica).await)
            .await;
    }
    assert_eq!(world.count("SELECT count(*) FROM sync.quarantine").await, 0);
    assert_eq!(world.held(a).await, 2);
    BUGGY.set(true);
}

/// A row no message could carry is set aside like one that does not encode,
/// rather than handed to a transport that would refuse it for ever.
#[sqlx::test(migrations = false)]
async fn a_row_larger_than_any_message_waits_in_quarantine(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, b] = world.replicas;
    let wire = Fake::default();
    world.item("small").await;
    world.item(&"x".repeat(envelope::ENVELOPE_ROOM)).await;
    let relay = Relay {
        limits: Limits {
            rows: 100,
            bytes: 2 * envelope::ENVELOPE_ROOM - 1,
        },
        ..world.relay()
    };

    let mut tx = world.pool.begin_with(SNAPSHOT).await.unwrap();
    let prepared = relay.prepare(&mut tx).await.unwrap().expect("a pass");
    tx.rollback().await.unwrap();
    let sent = relay.publish(&wire, prepared).await;
    let mut tx = world.pool.begin().await.unwrap();
    let outcome = relay.record(&mut tx, sent).await.unwrap();
    tx.commit().await.unwrap();

    let delivered = wire.delivered();
    assert_eq!(delivered[&a], names(&["small"]));
    assert_eq!(delivered[&b], names(&["small"]));
    assert_eq!(outcome.quarantined, 2, "the large row, for each replica");
    assert!(outcome.failing.is_empty(), "no replica waits for it");
    let reason: String = sqlx::query_scalar("SELECT reason FROM sync.quarantine LIMIT 1")
        .fetch_one(&world.pool)
        .await
        .unwrap();
    assert!(reason.contains("larger than any message"), "{reason}");
}

#[sqlx::test(migrations = false)]
async fn a_note_the_planner_fails_on_is_parked_alone(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, _] = world.replicas;
    let wire = Fake::default();
    world.item("before").await;
    sqlx::query(
        "INSERT INTO sync.notes (kind, relation, row_key) VALUES ('row', 'items', 'not-a-key')",
    )
    .execute(&world.pool)
    .await
    .expect("a note no build can plan");
    world.item("after").await;

    let outcome = world.pass(&wire).await;

    assert_eq!(wire.delivered()[&a], names(&["before", "after"]));
    assert_eq!(outcome.parked.len(), 1, "{outcome:?}");
    let parked: (String, i32) =
        sqlx::query_as("SELECT row_key, attempts FROM sync.notes WHERE parked_at IS NOT NULL")
            .fetch_one(&world.pool)
            .await
            .expect("the parked note");
    assert_eq!(parked, ("not-a-key".to_owned(), 1));

    // Unparked when the relay starts again; it fails the same way and is
    // parked again, still alone.
    unpark(&mut world.pool.acquire().await.unwrap())
        .await
        .unwrap();
    world.item("later").await;
    let outcome = world.pass(&wire).await;
    assert_eq!(wire.delivered()[&a], names(&["later"]));
    assert_eq!(outcome.parked.len(), 1);
}

#[sqlx::test(migrations = false)]
async fn a_replica_the_transport_cannot_reach_waits_while_the_others_carry_on(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, b] = world.replicas;
    let wire = Fake::default();
    wire.failing.lock().unwrap().insert(a, true);
    world.item("one").await;

    let outcome = world.pass(&wire).await;

    assert_eq!(outcome.failing.len(), 1);
    let delivered = wire.delivered();
    assert_eq!(delivered[&b], names(&["one"]));
    assert!(!delivered.contains_key(&a));
    assert_eq!((world.held(a).await, world.held(b).await), (0, 1));
    let owed = "SELECT count(*) FROM sync.notes WHERE done_at IS NULL AND scope IS NOT NULL";
    assert_eq!(
        world.count(owed).await,
        1,
        "noted again for the replica alone"
    );

    let overview = world
        .engine
        .overview(
            &mut world.pool.acquire().await.unwrap(),
            Scope::Replicas(&[a]),
            5,
        )
        .await
        .unwrap();
    let status = &overview.replicas.first[0];
    assert_eq!(
        (status.owed, status.failures, status.state),
        (1, 1, ReplicaState::Unreachable)
    );
    assert!(
        status.retry_at.is_some_and(|at| at > chrono::Utc::now()),
        "{status:?}"
    );

    // While it waits, the others go on getting changes, and it is owed them.
    world.item("two").await;
    world.pass(&wire).await;
    assert_eq!(wire.delivered()[&b], names(&["two"]));
    assert_eq!(world.count(owed).await, 2);

    // Back, and its wait over: it gets what it is owed, and only it does.
    wire.failing.lock().unwrap().clear();
    sqlx::query("UPDATE sync.replicas SET retry_after = now()")
        .execute(&world.pool)
        .await
        .unwrap();
    world.pass(&wire).await;
    let delivered = wire.delivered();
    assert_eq!(delivered[&a], names(&["one", "two"]));
    assert!(
        !delivered.contains_key(&b),
        "already holds them: {delivered:?}"
    );
    assert_eq!(world.count(owed).await, 0);
    assert_eq!(world.held(a).await, 2);
    let failures: i32 =
        sqlx::query_scalar("SELECT failures FROM sync.replicas WHERE replica_id = $1")
            .bind(a)
            .fetch_one(&world.pool)
            .await
            .unwrap();
    assert_eq!(failures, 0, "its failures forgotten");
}

/// A message the transport refuses for good holds back nothing it was to
/// carry: its replica waits like an unreachable one, the refusal shows, and
/// once the transport takes it again every row, delete and snapshot it was
/// owed arrives.
#[sqlx::test(migrations = false)]
async fn a_message_the_transport_refuses_for_good_leaves_its_replica_owed_everything(
    pool: crate::DbPool,
) {
    let world = world(pool).await;
    let [a, b] = world.replicas;
    let wire = Fake::default();
    let gone = world.item("gone").await;
    world.pass(&wire).await;
    wire.delivered();
    wire.failing.lock().unwrap().insert(a, false);
    world.item("big").await;
    world
        .exec(
            "DELETE FROM fixture.items WHERE id = $1 AND partition_id = $2 AND $3 <> ''",
            gone,
            "x",
        )
        .await;
    sqlx::query("INSERT INTO sync.notes (kind, replica_id) VALUES ('resync', $1)")
        .bind(a)
        .execute(&world.pool)
        .await
        .unwrap();

    let outcome = world.pass(&wire).await;

    assert_eq!(outcome.quarantined, 0, "nothing is set aside for a refusal");
    assert_eq!(wire.delivered()[&b], names(&["big"]));
    let (failures, why): (i32, String) =
        sqlx::query_as("SELECT failures, last_error FROM sync.replicas WHERE replica_id = $1")
            .bind(a)
            .fetch_one(&world.pool)
            .await
            .unwrap();
    assert_eq!(failures, 1);
    assert!(why.contains("for good"), "{why}");
    assert!(
        world
            .count("SELECT count(*) FROM sync.notes WHERE done_at IS NULL AND scope IS NOT NULL")
            .await
            > 0,
        "owed to it alone"
    );

    wire.failing.lock().unwrap().clear();
    sqlx::query("UPDATE sync.replicas SET retry_after = now()")
        .execute(&world.pool)
        .await
        .unwrap();
    world.pass(&wire).await;
    let sent: Vec<Body> = wire
        .sent
        .lock()
        .unwrap()
        .drain(..)
        .filter(|(r, _)| *r == a)
        .map(|(_, m)| m)
        .collect();
    let mut model = Modelled::default();
    model.items.insert(gone, "gone".to_owned());
    for message in sent {
        model.apply(message);
    }
    assert_eq!(
        model.items.values().cloned().collect::<BTreeSet<_>>(),
        names(&["big"]),
        "the delete and the snapshot arrived too"
    );
}

/// A replica a send fails to gets nothing more in that pass: a later group's
/// message must not reach it before what an earlier one owed it.
#[sqlx::test(migrations = false)]
async fn a_replica_a_send_fails_to_is_sent_nothing_more_that_pass(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, b] = world.replicas;
    let wire = Fake::default();
    let first = world.item("first").await;
    world.pass(&wire).await;
    wire.delivered();
    // Owed to a alone, planned in a group of its own after the global one.
    sqlx::query(
        "INSERT INTO sync.notes (kind, relation, row_key, scope) VALUES ('row', 'items', $1, $2)",
    )
    .bind(first.to_string())
    .bind(a)
    .execute(&world.pool)
    .await
    .unwrap();
    sqlx::query("DELETE FROM sync.held WHERE replica_id = $1")
        .bind(a)
        .execute(&world.pool)
        .await
        .unwrap();
    world.item("second").await;
    wire.fail_next.lock().unwrap().insert(a, 1);

    let outcome = world.pass(&wire).await;

    assert_eq!(outcome.failing.len(), 1);
    let delivered = wire.delivered();
    assert!(!delivered.contains_key(&a), "{delivered:?}");
    assert_eq!(delivered[&b], names(&["second"]));
}

/// A send that never answers holds back no other replica: it times out, and
/// its replica waits like an unreachable one.
#[sqlx::test(migrations = false)]
async fn a_send_that_never_answers_holds_back_no_other_replica(pool: crate::DbPool) {
    struct Stuck {
        replica: Uuid,
        fake: Fake,
    }
    impl Transport for Stuck {
        async fn send(&self, replica: Uuid, message: Vec<u8>, id: &str) -> Result<(), Error> {
            if replica == self.replica {
                std::future::pending::<()>().await;
            }
            self.fake.send(replica, message, id).await
        }
    }
    let world = world(pool).await;
    let [a, b] = world.replicas;
    let stuck = Stuck {
        replica: a,
        fake: Fake::default(),
    };
    world.item("one").await;
    let relay = Relay {
        timeout: std::time::Duration::from_millis(50),
        ..world.relay()
    };
    let mut tx = world.pool.begin_with(SNAPSHOT).await.unwrap();
    let prepared = relay.prepare(&mut tx).await.unwrap().expect("a pass");
    tx.rollback().await.unwrap();
    let sent = relay.publish(&stuck, prepared).await;
    let mut tx = world.pool.begin().await.unwrap();
    let outcome = relay.record(&mut tx, sent).await.unwrap();
    tx.commit().await.unwrap();

    assert_eq!(outcome.failing.len(), 1);
    assert_eq!(outcome.failing[0].0, a);
    assert_eq!(stuck.fake.delivered()[&b], names(&["one"]));
}

/// Every message carries a number of its own, drawn once per message: unique,
/// rising in the order a replica is sent them, and never past what a replica
/// can store, however far the sequence has run.
#[sqlx::test(migrations = false)]
async fn every_message_has_a_number_of_its_own_rising_as_they_go(pool: crate::DbPool) {
    let world = world(pool).await;
    let wire = Fake::default();
    let relay = Relay {
        limits: Limits {
            rows: 2,
            bytes: 1 << 20,
        },
        ..world.relay()
    };
    let numbers = |wire: &Fake| -> Vec<(Uuid, u64)> {
        wire.sent
            .lock()
            .unwrap()
            .drain(..)
            .map(|(r, m)| match m {
                Body::Down(d) => (r, d.sequence),
                Body::EndSnapshot(e) => (r, e.sequence),
                Body::Filed(_) => unreachable!("the relay answers no upload"),
            })
            .collect()
    };
    // A sequence that has run far: a pass's numbers must not overflow.
    sqlx::query("SELECT setval('sync.messages', $1)")
        .bind(i64::MAX - 1_000)
        .execute(&world.pool)
        .await
        .unwrap();
    let mut seen = Vec::new();
    for round in 0..3 {
        for n in 0..5 {
            world.item(&format!("r{round}-{n}")).await;
        }
        sqlx::query("INSERT INTO sync.notes (kind, replica_id) VALUES ('resync', $1)")
            .bind(world.replicas[round % 2])
            .execute(&world.pool)
            .await
            .unwrap();
        let before: i64 = sqlx::query_scalar("SELECT last_value FROM sync.messages")
            .fetch_one(&world.pool)
            .await
            .unwrap();
        world.pass_relay(&relay, &wire).await;
        let drawn: i64 = sqlx::query_scalar("SELECT last_value FROM sync.messages")
            .fetch_one(&world.pool)
            .await
            .unwrap();
        let sent = numbers(&wire);
        assert_eq!(
            u64::try_from(drawn - before).unwrap(),
            sent.len() as u64,
            "one draw per message"
        );
        seen.extend(sent);
    }
    let all: BTreeSet<u64> = seen.iter().map(|(_, n)| *n).collect();
    assert_eq!(all.len(), seen.len(), "no two messages share a number");
    assert!(all.iter().all(|n| i64::try_from(*n).is_ok()));
    for replica in world.replicas {
        let theirs: Vec<u64> = seen
            .iter()
            .filter(|(r, _)| *r == replica)
            .map(|(_, n)| *n)
            .collect();
        assert!(theirs.windows(2).all(|w| w[0] < w[1]), "{theirs:?}");
        assert_eq!(world.last_sent(replica).await, *theirs.last().unwrap());
    }
}

/// A replica owed more, alone, than a pass plans is provisioned again
/// instead: what it is owed stays bounded however long it waits, and what it
/// gets once back is everything it should hold.
#[sqlx::test(migrations = false)]
async fn a_replica_away_long_is_owed_one_provisioning_not_every_change(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, b] = world.replicas;
    let wire = Fake::default();
    let relay = Relay {
        batch: 4,
        ..world.relay()
    };
    wire.failing.lock().unwrap().insert(a, true);
    for n in 0..30 {
        world.item(&format!("n{n}")).await;
        world.pass_relay(&relay, &wire).await;
        sqlx::query("UPDATE sync.replicas SET retry_after = now() + interval '1 hour' WHERE replica_id = $1")
            .bind(a)
            .execute(&world.pool)
            .await
            .unwrap();
        let owed = world
            .count(&format!(
                "SELECT count(*) FROM sync.notes WHERE done_at IS NULL AND scope = '{a}' AND kind <> 'resync'"
            ))
            .await;
        assert!(owed <= 4, "owed {owed} notes alone");
    }
    assert_eq!(
        world
            .count(&format!(
                "SELECT count(*) FROM sync.notes WHERE done_at IS NULL AND kind = 'resync' AND replica_id = '{a}'"
            ))
            .await,
        1
    );
    assert_eq!(world.held(b).await, 30);

    wire.failing.lock().unwrap().clear();
    sqlx::query("UPDATE sync.replicas SET retry_after = now()")
        .execute(&world.pool)
        .await
        .unwrap();
    for _ in 0..10 {
        if world.pass_relay(&relay, &wire).await.done == 0 {
            break;
        }
    }
    let mut model = Modelled::default();
    for (_, message) in wire.sent.lock().unwrap().drain(..).filter(|(r, _)| *r == a) {
        model.apply(message);
    }
    assert_eq!(model.items.len(), 30);
    assert_eq!(world.held(a).await, 30);
}

/// A replica that left its last messages unconfirmed while nothing new went
/// to it — a transport that lost them — is provisioned again, and again only
/// after twice as long while it stays silent.
#[sqlx::test(migrations = false)]
async fn a_replica_silent_on_what_it_was_sent_is_provisioned_again(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, b] = world.replicas;
    let wire = Fake::default();
    world.item("lost").await;
    world.pass(&wire).await;
    wire.delivered();
    world.acknowledge(b, world.last_sent(b).await).await;
    let heal = || async {
        let mut conn = world.pool.acquire().await.unwrap();
        heal(&mut conn, &world.engine, std::time::Duration::from_secs(60))
            .await
            .unwrap()
    };
    let silent_for = |secs: i64| {
        sqlx::query(
            "UPDATE sync.replicas SET last_sent_at = now() - make_interval(secs => $1), \
                    waiting_since = now() - make_interval(secs => $1) WHERE waiting_since IS NOT NULL",
        )
        .bind(secs as f64)
        .execute(&world.pool)
    };

    assert_eq!(heal().await, 0, "not silent long enough");
    silent_for(61).await.unwrap();
    assert_eq!(heal().await, 1, "a, and not b, which confirmed");
    assert_eq!(heal().await, 0, "one is pending");
    world.pass(&wire).await;
    assert!(
        wire.sent
            .lock()
            .unwrap()
            .iter()
            .any(|(r, m)| *r == a && matches!(m, Body::EndSnapshot(_))),
        "provisioned again"
    );

    // Lost again: the next one waits twice as long.
    silent_for(90).await.unwrap();
    sqlx::query(
        "UPDATE sync.notes SET noted_at = now() - interval '80 seconds' WHERE kind = 'resync'",
    )
    .execute(&world.pool)
    .await
    .unwrap();
    assert_eq!(heal().await, 0, "within twice the wait");
    silent_for(121).await.unwrap();
    assert_eq!(heal().await, 1);
}

#[sqlx::test(migrations = false)]
async fn a_row_a_replica_refuses_is_quarantined_and_sent_again_on_retry(pool: crate::DbPool) {
    use crate::envelope::{Key as WireKey, Rejected, Up};
    let world = world(pool).await;
    let [a, b] = world.replicas;
    let wire = Fake::default();
    let id = world.item("refused").await;
    world.pass(&wire).await;
    wire.delivered();

    let rejection = |table: &str, key: &str| Rejected {
        table: table.to_owned(),
        key: Some(WireKey {
            parts: vec![key.to_owned()],
        }),
        reason: "a check the replica's build has\0 and the server's does not".to_owned(),
    };
    let report = Up {
        rejected: vec![
            // Its key as a replica may spell it: the server stores its own.
            rejection("items", &id.to_string().to_uppercase()),
            // Rows it was never sent, a key that is none, a table not sent.
            rejection("items", &Uuid::now_v7().to_string()),
            rejection("items", "not-a-key"),
            rejection("readings", &id.to_string()),
        ],
        ..Up::default()
    };
    let mut conn = world.pool.acquire().await.unwrap();
    crate::filing::file(&mut conn, &world.codecs, &report, a, &())
        .await
        .unwrap();
    assert_eq!(world.held(a).await, 0, "no longer counted as held");
    let set_aside: Vec<(String, String)> =
        sqlx::query_as("SELECT row_key, cause FROM sync.quarantine")
            .fetch_all(&world.pool)
            .await
            .unwrap();
    assert_eq!(
        set_aside,
        [(id.to_string(), "replica".to_owned())],
        "only what it was sent"
    );

    // The replica's build is fixed; the retry sends it to that replica alone.
    retry_quarantine(&mut conn).await.unwrap();
    drop(conn);
    world.pass(&wire).await;

    let delivered = wire.delivered();
    assert_eq!(delivered[&a], names(&["refused"]));
    assert!(!delivered.contains_key(&b));
    assert_eq!(
        world.count("SELECT count(*) FROM sync.quarantine").await,
        1,
        "set aside until it says it took it"
    );
    world.acknowledge(a, world.last_sent(a).await).await;
    assert_eq!(world.count("SELECT count(*) FROM sync.quarantine").await, 0);
    assert_eq!(world.held(a).await, 1);
}

/// An acknowledgement past every message the server records sending is not
/// believed, nor one no message could have, nor one of another epoch.
#[sqlx::test(migrations = false)]
async fn an_acknowledgement_of_more_than_was_sent_changes_nothing(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, _] = world.replicas;
    let wire = Fake::default();
    world.item("one").await;
    world.pass(&wire).await;
    let last = world.last_sent(a).await;
    let applied = || async {
        sqlx::query_scalar::<_, Option<i64>>(
            "SELECT applied_sequence FROM sync.replicas WHERE replica_id = $1",
        )
        .bind(a)
        .fetch_one(&world.pool)
        .await
        .unwrap()
    };
    for sequence in [last + 1, u64::MAX, i64::MAX as u64 + 1] {
        world.acknowledge(a, sequence).await;
        assert_eq!(applied().await, None, "{sequence}");
    }
    world.acknowledge(a, last).await;
    assert_eq!(applied().await, Some(last as i64));
}

#[sqlx::test(migrations = false)]
async fn a_replica_that_holds_other_than_recorded_is_provisioned_again(pool: crate::DbPool) {
    use crate::envelope::{fingerprint, Digest, Up};
    let world = world(pool).await;
    let [a, _] = world.replicas;
    let wire = Fake::default();
    let one = world.item("one").await;
    let two = world.item("two").await;
    world.item("poison").await;
    world.pass(&wire).await;

    let epoch: Uuid = sqlx::query_scalar("SELECT epoch FROM sync.epoch")
        .fetch_one(&world.pool)
        .await
        .unwrap();
    let last = world.last_sent(a).await;
    let digests_of = |tables: &[(&str, &[Uuid])], sequence: u64| Up {
        digests: tables
            .iter()
            .map(|(table, keys)| {
                let mut keys: Vec<String> = keys.iter().map(Uuid::to_string).collect();
                Digest {
                    table: (*table).to_owned(),
                    count: keys.len() as u64,
                    fingerprint: fingerprint(&mut keys),
                }
            })
            .collect(),
        epoch: epoch.to_string(),
        sequence,
        ..Up::default()
    };
    let resyncs = "SELECT count(*) FROM sync.notes WHERE kind = 'resync' AND done_at IS NULL";
    let file = |up: Up| {
        let world = &world;
        async move {
            let mut conn = world.pool.acquire().await.unwrap();
            crate::filing::file(&mut conn, &world.codecs, &up, a, &())
                .await
                .unwrap();
        }
    };

    // The row the server could not send it is not held there either.
    file(digests_of(&[("items", &[one, two])], last)).await;
    assert_eq!(world.count(resyncs).await, 0, "it holds what is recorded");

    file(digests_of(&[("items", &[one])], last - 1)).await;
    assert_eq!(
        world.count(resyncs).await,
        0,
        "messages still on their way: not compared"
    );

    // A digest of a table it is not sent, or of one twice, is no report.
    file(digests_of(&[("readings", &[one])], last)).await;
    file(digests_of(
        &[("items", &[one, two]), ("items", &[one])],
        last,
    ))
    .await;
    assert_eq!(world.count(resyncs).await, 0, "malformed: not compared");

    file(digests_of(&[("items", &[one])], last)).await;
    assert_eq!(world.count(resyncs).await, 1, "provisioned again");
    file(digests_of(&[("items", &[one])], last)).await;
    assert_eq!(world.count(resyncs).await, 1, "once, while one is pending");

    // Provisioned, and differing again the same day: a bug another
    // provisioning would not cure.
    world.pass(&wire).await;
    let last = world.last_sent(a).await;
    file(digests_of(&[("items", &[one])], last)).await;
    assert_eq!(world.count(resyncs).await, 0, "not again within the day");
    sqlx::query("UPDATE sync.replicas SET drifted_at = now() - interval '25 hours'")
        .execute(&world.pool)
        .await
        .unwrap();
    file(digests_of(&[("items", &[one])], last)).await;
    assert_eq!(world.count(resyncs).await, 1, "again the next day");
}

#[sqlx::test(migrations = false)]
async fn an_uploaded_row_the_server_cannot_file_waits_in_the_inbox_until_a_fix(
    pool: crate::DbPool,
) {
    use crate::envelope::{Table as WireTable, Up};
    use crate::testkit::records::{reading_from_wire, reading_to_wire, Reading, ReadingRow};
    let world = world(pool).await;
    let [a, _] = world.replicas;
    sqlx::raw_sql(
        "CREATE TABLE fixture.readings (id UUID PRIMARY KEY, replica_id UUID NOT NULL, value TEXT NOT NULL)",
    )
    .execute(&world.pool)
    .await
    .unwrap();
    let codecs = vec![Codec::<()>::new::<Reading, ReadingRow>(
        reading_to_wire,
        reading_from_wire,
        None,
    )];
    let row = |value: &str| {
        ReadingRow {
            id: Uuid::now_v7().to_string(),
            replica_id: a.to_string(),
            value: value.to_owned(),
        }
        .encode_to_vec()
    };
    let poison = row("poison");
    let upload = Up {
        tables: vec![
            WireTable {
                table: "readings".to_owned(),
                upserts: vec![row("fine"), poison.clone()],
                ..WireTable::default()
            },
            WireTable {
                table: "from_a_newer_build\0".to_owned(),
                upserts: vec![vec![1, 2, 3]],
                ..WireTable::default()
            },
        ],
        ..Up::default()
    };
    for _ in 0..2 {
        let mut tx = world.pool.begin().await.unwrap();
        crate::filing::file(&mut tx, &codecs, &upload, a, &())
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    let values = "SELECT count(*) FROM fixture.readings";
    assert_eq!(world.count(values).await, 1, "the rest is filed");
    assert_eq!(
        world.count("SELECT count(*) FROM sync.inbox").await,
        2,
        "nothing is lost, and what came twice is kept once"
    );

    // A deploy fixes the decoding; the retry files that row. The other waits
    // for a build that mirrors its table.
    BUGGY.set(false);
    let mut tx = world.pool.begin().await.unwrap();
    let newest = crate::filing::newest(&mut tx).await.unwrap();
    for kept in crate::filing::inbox(&mut tx, 0, newest, 10).await.unwrap() {
        crate::filing::refile(&mut tx, &codecs, &kept, &())
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    BUGGY.set(true);
    assert_eq!(world.count(values).await, 2);
    let left: (String, i32) = sqlx::query_as("SELECT relation, attempts FROM sync.inbox")
        .fetch_one(&world.pool)
        .await
        .unwrap();
    assert_eq!(left, ("from_a_newer_build\u{FFFD}".to_owned(), 3));
}

/// A replica as the transport reaches it: the items it holds, by id, the
/// last message it applied — it takes a message only above that — and the
/// snapshot in progress, with the rows it named.
#[derive(Default, Debug)]
struct Modelled {
    items: BTreeMap<Uuid, String>,
    last: u64,
    snapshot: Option<(String, BTreeSet<Uuid>)>,
}

impl Modelled {
    /// Applies a message as a real replica does: only above the last one; a
    /// snapshot's rows remembered, and what it did not name swept at its end.
    fn apply(&mut self, message: Body) {
        let sequence = match &message {
            Body::Down(down) => down.sequence,
            Body::EndSnapshot(end) => end.sequence,
            Body::Filed(_) => return,
        };
        if sequence <= self.last {
            return;
        }
        self.last = sequence;
        match message {
            Body::Down(down) => {
                let snapshot = (!down.snapshot_id.is_empty()).then(|| {
                    if self
                        .snapshot
                        .as_ref()
                        .is_none_or(|(id, _)| *id != down.snapshot_id)
                    {
                        self.snapshot = Some((down.snapshot_id.clone(), BTreeSet::new()));
                    }
                });
                for table in down.tables {
                    for bytes in table.upserts {
                        let row = ItemRow::decode(bytes.as_slice()).expect("an item");
                        let id: Uuid = row.id.parse().unwrap();
                        if let (Some(()), Some((_, named))) = (snapshot, self.snapshot.as_mut()) {
                            named.insert(id);
                        }
                        self.items.insert(id, row.name);
                    }
                    for key in table.deletes {
                        self.items.remove(&key.parts[0].parse().unwrap());
                    }
                }
            }
            Body::EndSnapshot(end) => {
                let named = match self.snapshot.take() {
                    Some((id, named)) if id == end.snapshot_id => named,
                    _ => BTreeSet::new(),
                };
                self.items.retain(|id, _| named.contains(id));
            }
            Body::Filed(_) => {}
        }
    }
}

/// A transport that fails at random — partway through a replica's messages
/// as often as not, now as a network does, now refusing a message for good —
/// and replicas that apply what reaches them, as a real one does.
struct Chaos {
    walk: Mutex<crate::testkit::Walk>,
    /// Out of 100: how often a send fails, while the network is bad.
    failure_rate: Mutex<u64>,
    replicas: Mutex<BTreeMap<Uuid, Modelled>>,
}

impl Transport for Chaos {
    fn send(
        &self,
        replica: Uuid,
        message: Vec<u8>,
        _id: &str,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        let message = body(&message);
        let mut walk = self.walk.lock().unwrap();
        let fails = walk.below(100) < *self.failure_rate.lock().unwrap() as usize;
        let refused = walk.below(3) == 0;
        drop(walk);
        let result = if fails {
            Err(Error::Transport {
                reason: if refused {
                    "refused\0 for good"
                } else {
                    "the network"
                }
                .into(),
                transient: !refused,
            })
        } else {
            self.replicas
                .lock()
                .unwrap()
                .entry(replica)
                .or_default()
                .apply(message);
            Ok(())
        };
        async move { result }
    }
}

/// Whatever fails along the way, once the network is back and the fix is
/// deployed, every replica holds exactly what it should — with nobody
/// touching the data.
#[sqlx::test(migrations = false)]
async fn whatever_fails_on_the_way_every_replica_ends_holding_what_it_should(pool: crate::DbPool) {
    let world = world(pool).await;
    // The replicas live through every storm, as real ones do.
    let chaos = Chaos {
        walk: Mutex::new(crate::testkit::Walk(0)),
        failure_rate: Mutex::new(0),
        replicas: Mutex::new(BTreeMap::new()),
    };
    // Small messages, so a replica's rows span several and a failure can cut
    // them partway; a small batch, so a replica left out long is owed one
    // provisioning rather than every change.
    let relay = Relay {
        limits: Limits {
            rows: 3,
            bytes: 1 << 20,
        },
        batch: 8,
        ..world.relay()
    };
    for seed in [
        0x9E37_79B9_7F4A_7C15_u64,
        0xD1B5_4A32_D192_ED03,
        0x2545_F491_4F6C_DD1D,
    ] {
        *chaos.walk.lock().unwrap() = crate::testkit::Walk(seed);
        *chaos.failure_rate.lock().unwrap() = 30;
        let mut walk = crate::testkit::Walk(seed ^ 0xFFFF);
        let mut names = 0;
        let (mut failed_sends, mut refusals) = (0, 0);
        for step in 0..150 {
            let items: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM fixture.items ORDER BY id")
                .fetch_all(&world.pool)
                .await
                .unwrap();
            let pick = (!items.is_empty()).then(|| items[walk.below(items.len())]);
            names += 1;
            let name = if walk.below(10) == 0 {
                "poison".to_owned()
            } else {
                format!("n{names}")
            };
            match (walk.below(5), pick) {
                (0 | 1, _) | (_, None) => {
                    world.item(&name).await;
                }
                (2, Some(id)) => {
                    world.exec("UPDATE fixture.items SET name = $3 WHERE id = $1 AND partition_id = $2", id, &name).await;
                }
                (3, Some(id)) => {
                    world.exec("UPDATE fixture.items SET retired_at = CASE WHEN retired_at IS NULL THEN now() END WHERE id = $1 AND partition_id = $2 AND $3 <> ''", id, &name).await;
                }
                (_, Some(id)) => {
                    world.exec("DELETE FROM fixture.items WHERE id = $1 AND partition_id = $2 AND $3 <> ''", id, &name).await;
                }
            }
            if walk.below(15) == 0 {
                // Provisioned again mid-storm: a snapshot that may itself
                // be cut short.
                let replica = world.replicas[walk.below(2)];
                sqlx::query("INSERT INTO sync.notes (kind, replica_id) VALUES ('resync', $1)")
                    .bind(replica)
                    .execute(&world.pool)
                    .await
                    .unwrap();
            }
            if walk.below(3) == 0 {
                let failing = world.pass_relay(&relay, &chaos).await.failing;
                failed_sends += failing.len();
                refusals += failing
                    .iter()
                    .filter(|(_, why)| why.contains("for good"))
                    .count();
            }
            if step % 40 == 0 {
                // Some replicas' waits run out early, mid-storm.
                sqlx::query("UPDATE sync.replicas SET retry_after = now()")
                    .execute(&world.pool)
                    .await
                    .unwrap();
            }
        }

        // The storm was one: sends failed, and rows waited in quarantine.
        assert!(failed_sends > 0, "seed {seed:#x}: no send failed");
        assert!(refusals > 0, "seed {seed:#x}: no send refused for good");
        assert!(
            world.count("SELECT count(*) FROM sync.quarantine").await > 0,
            "seed {seed:#x}: nothing was quarantined"
        );

        // The network comes back and the encoding bug is fixed: the relay
        // starts again, as after a deploy, and drains.
        *chaos.failure_rate.lock().unwrap() = 0;
        BUGGY.set(false);
        let mut conn = world.pool.acquire().await.unwrap();
        unpark(&mut conn).await.unwrap();
        retry_quarantine(&mut conn).await.unwrap();
        sqlx::query("UPDATE sync.replicas SET retry_after = now()")
            .execute(&mut *conn)
            .await
            .unwrap();
        drop(conn);
        for _ in 0..500 {
            if world.pass_relay(&relay, &chaos).await.done == 0 {
                break;
            }
        }
        BUGGY.set(true);

        let should: BTreeMap<Uuid, String> =
            sqlx::query_as("SELECT id, name FROM fixture.items WHERE retired_at IS NULL")
                .fetch_all(&world.pool)
                .await
                .unwrap()
                .into_iter()
                .collect();
        for replica in world.replicas {
            let holds = chaos
                .replicas
                .lock()
                .unwrap()
                .get(&replica)
                .map(|m| m.items.clone())
                .unwrap_or_default();
            assert_eq!(
                holds, should,
                "seed {seed:#x}: {replica} holds other than it should"
            );
            let last = chaos
                .replicas
                .lock()
                .unwrap()
                .get(&replica)
                .map_or(0, |m| m.last);
            world.acknowledge(replica, last).await;
        }
        assert_eq!(world.count("SELECT count(*) FROM sync.quarantine").await, 0);
        assert_eq!(
            world
                .count("SELECT count(*) FROM sync.notes WHERE done_at IS NULL")
                .await,
            0
        );
    }
}

/// A transaction that began before a pass read and commits after it: what
/// it changed still reaches every replica, though the pass recorded a later
/// send of the same row.
#[sqlx::test(migrations = false)]
async fn a_change_committed_after_the_pass_read_past_it_still_arrives(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, b] = world.replicas;
    let wire = Fake::default();
    let id = world.item("first").await;
    world.pass(&wire).await;
    wire.delivered();

    let mut late = world.pool.begin().await.unwrap();
    sqlx::query("UPDATE fixture.items SET name = 'late' WHERE id = $1")
        .bind(id)
        .execute(&mut *late)
        .await
        .unwrap();
    world.item("meanwhile").await;
    world.pass(&wire).await;
    late.commit().await.unwrap();
    world.pass(&wire).await;

    let delivered = wire.delivered();
    for replica in [a, b] {
        assert!(delivered[&replica].contains("late"), "{delivered:?}");
    }
}

/// A relay that sent and died before it recorded: the next pass sends the
/// same again, and records it once.
#[sqlx::test(migrations = false)]
async fn a_pass_that_sent_but_never_recorded_is_sent_again(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, _] = world.replicas;
    let wire = Fake::default();
    world.item("once").await;

    let relay = world.relay();
    let mut tx = world.pool.begin_with(SNAPSHOT).await.unwrap();
    let prepared = relay.prepare(&mut tx).await.unwrap().expect("a pass");
    tx.rollback().await.unwrap();
    let first = relay.publish(&wire, prepared).await;
    drop(first);
    assert_eq!(world.held(a).await, 0, "nothing recorded");

    world.pass(&wire).await;

    let downs: Vec<u64> = wire
        .sent
        .lock()
        .unwrap()
        .iter()
        .filter(|(r, _)| *r == a)
        .filter_map(|(_, m)| match m {
            Body::Down(d) => Some(d.sequence),
            _ => None,
        })
        .collect();
    assert_eq!(downs.len(), 2, "sent twice");
    assert!(downs[1] > downs[0], "the second numbered above the first");
    assert_eq!(world.held(a).await, 1);
    assert_eq!(
        world
            .count("SELECT count(*) FROM sync.notes WHERE done_at IS NULL")
            .await,
        0
    );
}

impl World {
    /// The last message sent to `replica`.
    async fn last_sent(&self, replica: Uuid) -> u64 {
        sqlx::query_scalar::<_, i64>(
            "SELECT last_sequence FROM sync.replicas WHERE replica_id = $1",
        )
        .bind(replica)
        .fetch_one(&self.pool)
        .await
        .unwrap() as u64
    }

    /// `replica` says it applied every message up to `sequence`.
    async fn acknowledge(&self, replica: Uuid, sequence: u64) {
        let epoch: Uuid = sqlx::query_scalar("SELECT epoch FROM sync.epoch")
            .fetch_one(&self.pool)
            .await
            .unwrap();
        let ack = envelope::Up {
            replica_id: replica.to_string(),
            epoch: epoch.to_string(),
            sequence,
            ..envelope::Up::default()
        };
        let mut conn = self.pool.acquire().await.unwrap();
        crate::filing::file(&mut conn, &self.codecs, &ack, replica, &())
            .await
            .unwrap();
    }

    async fn overview(&self, scope: Scope<'_>) -> crate::status::Overview {
        let mut conn = self.pool.acquire().await.unwrap();
        self.engine.overview(&mut conn, scope, 10).await.unwrap()
    }
}

/// A change to a row the replica holds that is set aside (it does not
/// encode) stays set aside however often the replica confirms what it holds:
/// the version it confirms is the old one. Only the message that carries the
/// change again, applied, lets it out.
#[sqlx::test(migrations = false)]
async fn a_change_set_aside_stays_set_aside_until_the_change_itself_is_applied(
    pool: crate::DbPool,
) {
    let world = world(pool).await;
    let [a, _] = world.replicas;
    let wire = Fake::default();
    let id = world.item("fine").await;
    world.pass(&wire).await;
    world.acknowledge(a, world.last_sent(a).await).await;

    world
        .exec(
            "UPDATE fixture.items SET name = $3 WHERE id = $1 AND partition_id = $2",
            id,
            "poison",
        )
        .await;
    world.pass(&wire).await;
    let quarantined = "SELECT count(*) FROM sync.quarantine WHERE replica_id = $1";
    let count = |replica: Uuid| {
        let pool = world.pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>(quarantined)
                .bind(replica)
                .fetch_one(&pool)
                .await
                .unwrap()
        }
    };
    assert_eq!(count(a).await, 1);
    world.acknowledge(a, world.last_sent(a).await).await;
    assert_eq!(
        count(a).await,
        1,
        "it confirmed the old version, not this one"
    );
    let overview = world.overview(Scope::Replicas(&[a])).await;
    assert_eq!(overview.replicas.first[0].state, ReplicaState::AwaitingFix);

    // The fix deployed: carried again, and applied.
    BUGGY.set(false);
    retry_quarantine(&mut world.pool.acquire().await.unwrap())
        .await
        .unwrap();
    world.pass(&wire).await;
    BUGGY.set(true);
    assert_eq!(count(a).await, 1, "sent, not yet applied");
    world.acknowledge(a, world.last_sent(a).await).await;
    assert_eq!(count(a).await, 0);
}

/// A second table whose rows share their keys with the items': what settles
/// one table's set-aside row must not read the other's.
#[derive(sqlx::FromRow, Clone, Debug, crate::Row)]
#[row(
    table = "tags",
    schema = "fixture",
    mirror = "catalog",
    sync = "broadcast"
)]
struct Tag {
    id: Uuid,
    partition_id: Uuid,
    name: String,
}

fn tag_to_wire(tag: Tag) -> Result<ItemRow, magic_map::MappingError> {
    to_wire(Item {
        id: tag.id,
        partition_id: tag.partition_id,
        name: tag.name,
    })
}

fn tag_from_wire(row: ItemRow) -> Result<Tag, magic_map::MappingError> {
    let item = from_wire(row)?;
    Ok(Tag {
        id: item.id,
        partition_id: item.partition_id,
        name: item.name,
    })
}

/// A row set aside for a replica that is owed to no one any more leaves the
/// quarantine; a row of another table under the same key, still failing,
/// stays.
#[sqlx::test(migrations = false)]
async fn a_row_set_aside_and_owed_to_no_one_leaves_while_its_namesake_stays(pool: crate::DbPool) {
    let world = world(pool).await;
    sqlx::raw_sql(
        "CREATE TABLE fixture.tags (id UUID PRIMARY KEY, partition_id UUID NOT NULL, name TEXT NOT NULL);",
    )
    .execute(&world.pool)
    .await
    .unwrap();
    let mut tx = world.pool.begin().await.unwrap();
    let engine = Engine::install(
        &mut tx,
        &[&Table::of::<Item>(), &Table::of::<Tag>()],
        REPLICAS,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let codecs = vec![
        Codec::<()>::new::<Item, ItemRow>(to_wire, from_wire, None),
        Codec::<()>::new::<Tag, ItemRow>(tag_to_wire, tag_from_wire, None),
    ];
    let relay = Relay {
        engine: &engine,
        codecs: &codecs,
        ..world.relay()
    };
    let wire = Fake::default();
    let id = world.item("poison").await;
    world
        .exec(
            "INSERT INTO fixture.tags (id, partition_id, name) VALUES ($1, $2, $3)",
            id,
            "poison",
        )
        .await;
    world.pass_relay(&relay, &wire).await;
    let set_aside = |table: &'static str| {
        let pool = world.pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM sync.quarantine WHERE relation = $1")
                .bind(table)
                .fetch_one(&pool)
                .await
                .unwrap()
        }
    };
    assert_eq!((set_aside("items").await, set_aside("tags").await), (2, 2));

    world
        .exec(
            "DELETE FROM fixture.items WHERE id = $1 AND partition_id = $2 AND $3 <> ''",
            id,
            "x",
        )
        .await;
    world.pass_relay(&relay, &wire).await;
    retry_quarantine(&mut world.pool.acquire().await.unwrap())
        .await
        .unwrap();
    world.pass_relay(&relay, &wire).await;

    assert_eq!(set_aside("items").await, 0, "owed to no one");
    assert_eq!(set_aside("tags").await, 2, "still failing");
}

#[path = "status_tests.rs"]
mod status;
