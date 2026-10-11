//! The relay against failures, one at a time: whatever fails is set aside,
//! everything else carries on, and the set-aside part gets through once the
//! cause is gone.

use super::*;
use crate::model::{Model, Rules, Wire};
use crate::plan::Replicas;
use crate::row::Table;
use crate::status::{ReplicaState, RowState, Scope};
use crate::testkit::{from_wire, to_wire, Item, ItemRow, BUGGY};
use crate::wire::MapError;
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

/// How a pass records: the notes, the plan, its rows and what it writes in
/// one snapshot.
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
    /// Each replica as its provisioning left it.
    provisioned: BTreeMap<Uuid, Model>,
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
    let mut world = World {
        pool,
        engine,
        codecs: vec![Codec::new::<Item, ItemRow>(to_wire, from_wire, None)],
        replicas,
        partition,
        provisioned: BTreeMap::new(),
    };
    // Each replica provisioned, of nothing yet: what follows is chained to it.
    for replica in replicas {
        sqlx::query("INSERT INTO sync.notes (kind, replica_id) VALUES ('resync', $1)")
            .bind(replica)
            .execute(&world.pool)
            .await
            .expect("provisioned");
    }
    let wire = Wire::new(0);
    world.pass(&wire).await;
    for replica in replicas {
        let mut model = Model::default();
        while let Some(bytes) = wire.next(replica) {
            model.receive(&bytes, &Rules::ANY);
            wire.acknowledge(replica);
        }
        world.provisioned.insert(replica, model);
        world
            .acknowledge(replica, world.last_sent(replica).await)
            .await;
    }
    world
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
            backoff: crate::run::Backoff::new(
                std::time::Duration::from_secs(2),
                std::time::Duration::from_secs(900),
            ),
            bound: Bound {
                messages: 1_000,
                bytes: 1 << 30,
            },
        }
    }

    /// One pass, as a host runs it.
    async fn pass<T: Transport>(&self, transport: &T) -> Passed {
        self.pass_relay(&self.relay(), transport).await
    }

    async fn pass_relay<T: Transport>(&self, relay: &Relay<'_, ()>, transport: &T) -> Passed {
        let recorded = self.record(relay).await;
        let published = self.publish(relay, transport).await;
        Passed {
            recorded,
            failing: published.failing,
            sent: published.sent,
        }
    }

    /// The first half of a pass: planned and recorded, nothing sent.
    async fn record(&self, relay: &Relay<'_, ()>) -> Outcome {
        let mut tx = self.pool.begin_with(SNAPSHOT).await.expect("a transaction");
        let outcome = relay.record(&mut tx).await.expect("recorded");
        tx.commit().await.expect("commit");
        outcome
    }

    /// The second half: every outbox sent.
    async fn publish<T: Transport>(&self, relay: &Relay<'_, ()>, transport: &T) -> Published {
        let mut conn = self.pool.acquire().await.expect("a connection");
        relay
            .publish(&mut conn, transport)
            .await
            .expect("published")
    }

    /// Messages waiting in `replica`'s outbox.
    async fn outbox(&self, replica: Uuid) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM sync.outbox WHERE replica_id = $1")
            .bind(replica)
            .fetch_one(&self.pool)
            .await
            .expect("outbox")
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

/// What a pass did: recorded, and sent.
#[derive(Debug)]
struct Passed {
    recorded: Outcome,
    failing: Vec<(Uuid, String)>,
    sent: usize,
}

impl std::ops::Deref for Passed {
    type Target = Outcome;
    fn deref(&self) -> &Outcome {
        &self.recorded
    }
}

/// The names of the items a modelled replica holds.
fn items_of(model: &Model) -> BTreeSet<String> {
    model
        .rows
        .values()
        .map(|bytes| ItemRow::decode(bytes.as_slice()).expect("an item").name)
        .collect()
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
        1,
        "one note for the row, whoever it is owed to"
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

    let outcome = world.pass_relay(&relay, &wire).await;

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

/// A replica the transport cannot reach keeps what it is owed in its outbox,
/// in order, recorded as on its way to it, while every other replica is sent
/// to; once it can be reached it is sent the outbox, and nothing is planned
/// twice.
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
    assert_eq!(
        (world.held(a).await, world.held(b).await),
        (1, 1),
        "recorded before it went, as on its way"
    );
    assert_eq!(world.outbox(a).await, 1, "kept for it");
    assert_eq!(
        world
            .count("SELECT count(*) FROM sync.notes WHERE done_at IS NULL")
            .await,
        0,
        "nothing is planned again"
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
        (
            status.owed,
            status.failures,
            status.state,
            status.unconfirmed
        ),
        (1, 1, ReplicaState::Unreachable, true)
    );
    assert!(
        status.retry_at.is_some_and(|at| at > chrono::Utc::now()),
        "{status:?}"
    );

    // While it waits, the others go on getting changes, and its own queue
    // up behind what it was owed first.
    world.item("two").await;
    world.pass(&wire).await;
    assert_eq!(wire.delivered()[&b], names(&["two"]));
    assert_eq!(world.outbox(a).await, 2);

    // Back, and its wait over: it gets its outbox in order, and only it does.
    wire.failing.lock().unwrap().clear();
    sqlx::query("UPDATE sync.replicas SET retry_after = now()")
        .execute(&world.pool)
        .await
        .unwrap();
    let outcome = world.pass(&wire).await;
    assert_eq!(outcome.sent, 2);
    let sent: Vec<(Uuid, String)> = wire
        .sent
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(r, m)| match m {
            Body::Down(d) => Some((
                *r,
                ItemRow::decode(d.tables[0].upserts[0].as_slice())
                    .unwrap()
                    .name,
            )),
            _ => None,
        })
        .collect();
    assert_eq!(sent, [(a, "one".to_owned()), (a, "two".to_owned())]);
    wire.delivered();
    assert_eq!(world.outbox(a).await, 0);
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
    assert!(world.outbox(a).await > 0, "kept for it");

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
    let mut model = world.follower(a);
    model.rows.insert(
        ("items".to_owned(), gone.to_string()),
        ItemRow {
            name: "gone".to_owned(),
            ..ItemRow::default()
        }
        .encode_to_vec(),
    );
    for message in sent {
        model.receive(
            &envelope::ToReplica {
                protocol: envelope::PROTOCOL,
                body: Some(message),
            }
            .encode_to_vec(),
            &Rules::ANY,
        );
    }
    assert_eq!(
        items_of(&model),
        names(&["big"]),
        "the delete and the snapshot arrived too"
    );
}

/// A send that fails stops its replica's outbox there: nothing after it goes
/// before it does, and once it can be reached the rest goes in order.
#[sqlx::test(migrations = false)]
async fn a_send_that_fails_holds_back_what_follows_it(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, b] = world.replicas;
    let wire = Fake::default();
    let relay = Relay {
        limits: Limits {
            rows: 1,
            bytes: 1 << 20,
        },
        ..world.relay()
    };
    world.item("first").await;
    world.item("second").await;
    wire.fail_next.lock().unwrap().insert(a, 1);

    let outcome = world.pass_relay(&relay, &wire).await;

    assert_eq!(outcome.failing.len(), 1);
    let delivered = wire.delivered();
    assert!(!delivered.contains_key(&a), "{delivered:?}");
    assert_eq!(delivered[&b], names(&["first", "second"]));
    assert_eq!(world.outbox(a).await, 2);

    sqlx::query("UPDATE sync.replicas SET retry_after = now()")
        .execute(&world.pool)
        .await
        .unwrap();
    world.pass_relay(&relay, &wire).await;
    let numbers: Vec<u64> = wire
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
    assert_eq!(numbers.len(), 2);
    assert!(numbers[0] < numbers[1], "in order: {numbers:?}");
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
    let outcome = world.pass_relay(&relay, &stuck).await;

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

/// A replica away long enough that its outbox outgrows its bound has its
/// outbox dropped and is provisioned again — an alarm, in its last error —
/// rather than owed every change; an outbox that begins with a snapshot is
/// not dropped for its size. Once back it holds everything it should.
#[sqlx::test(migrations = false)]
async fn a_replica_away_long_is_owed_one_provisioning_not_every_change(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, b] = world.replicas;
    let wire = Fake::default();
    let relay = Relay {
        bound: Bound {
            messages: 4,
            bytes: 1 << 30,
        },
        ..world.relay()
    };
    wire.failing.lock().unwrap().insert(a, true);
    let mut collapsed = 0;
    for n in 0..30 {
        world.item(&format!("n{n}")).await;
        collapsed += world.pass_relay(&relay, &wire).await.collapsed.len();
        assert!(
            world.outbox(a).await <= 7,
            "its outbox stays bounded: a snapshot, and what follows it within the bound"
        );
    }
    assert!(collapsed >= 1, "provisioned again for its outbox");
    let why: String =
        sqlx::query_scalar("SELECT last_error FROM sync.replicas WHERE replica_id = $1")
            .bind(a)
            .fetch_one(&world.pool)
            .await
            .unwrap();
    assert!(!why.is_empty());
    assert_eq!(world.held(b).await, 30);
    assert_eq!(world.outbox(b).await, 0);

    wire.failing.lock().unwrap().clear();
    sqlx::query("UPDATE sync.replicas SET retry_after = now()")
        .execute(&world.pool)
        .await
        .unwrap();
    world.pass_relay(&relay, &wire).await;
    let mut model = world.follower(a);
    for (_, message) in wire.sent.lock().unwrap().drain(..).filter(|(r, _)| *r == a) {
        let bytes = envelope::ToReplica {
            protocol: envelope::PROTOCOL,
            body: Some(message),
        }
        .encode_to_vec();
        model.receive(&bytes, &Rules::ANY);
    }
    assert_eq!(model.rows.len(), 30);
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
    assert_eq!(heal().await, 0, "within twice the wait");
    silent_for(121).await.unwrap();
    assert_eq!(heal().await, 1);

    // Caught up since: silent again, it waits the first wait again.
    world.pass(&wire).await;
    for replica in [a, b] {
        world
            .acknowledge(replica, world.last_sent(replica).await)
            .await;
    }
    world.item("lost again").await;
    world.pass(&wire).await;
    world.acknowledge(b, world.last_sent(b).await).await;
    silent_for(61).await.unwrap();
    assert_eq!(heal().await, 1, "the first wait again");
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

    let epoch: Uuid = sqlx::query_scalar("SELECT epoch FROM sync.epoch")
        .fetch_one(&world.pool)
        .await
        .unwrap();
    let last = world.last_sent(a).await;
    let rejection = |table: &str, key: &str, epoch: Uuid, sequence: u64| Rejected {
        table: table.to_owned(),
        key: Some(WireKey {
            parts: vec![key.to_owned()],
        }),
        reason: "a check the replica's build has\0 and the server's does not".to_owned(),
        epoch: epoch.to_string(),
        sequence,
    };
    let report = Up {
        rejected: vec![
            // Its key as a replica may spell it: the server stores its own.
            rejection("items", &id.to_string().to_uppercase(), epoch, last),
            // A row that message neither carried nor took back, a key that
            // is none, a table not sent, a message of another epoch, one
            // never sent it.
            rejection("items", &Uuid::now_v7().to_string(), epoch, last),
            rejection("items", "not-a-key", epoch, last),
            rejection("readings", &id.to_string(), epoch, last),
            rejection("items", &id.to_string(), Uuid::now_v7(), last),
            rejection("items", &id.to_string(), epoch, last + 1_000),
        ],
        ..Up::default()
    };
    crate::testkit::file(&world.pool, &world.codecs, &report, a, &()).await;
    let refused = "SELECT count(*) FROM sync.held WHERE refused IS NOT NULL";
    assert_eq!(world.count(refused).await, 1, "held, refused");
    let set_aside: Vec<(String, String)> =
        sqlx::query_as("SELECT row_key, cause FROM sync.quarantine")
            .fetch_all(&world.pool)
            .await
            .unwrap();
    assert_eq!(
        set_aside,
        [(id.to_string(), "replica".to_owned())],
        "only what that message carried"
    );

    // The replica's build is fixed; the retry sends it to that replica alone.
    retry_quarantine(&mut world.pool.acquire().await.unwrap())
        .await
        .unwrap();
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
    let still: i64 = sqlx::query_scalar("SELECT count(*) FROM sync.quarantine WHERE row_key = $1")
        .bind(id.to_string())
        .fetch_one(&world.pool)
        .await
        .unwrap();
    assert_eq!(still, 0, "out of the quarantine");
    assert_eq!(world.count(refused).await, 0, "taken");
}

/// An acknowledgement past every message the server records sending a
/// replica is not believed, nor one no message could have, nor one of another
/// epoch.
#[sqlx::test(migrations = false)]
async fn an_acknowledgement_of_more_than_was_sent_changes_nothing(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, b] = world.replicas;
    let wire = Fake::default();
    world.item("one").await;
    world.pass(&wire).await;
    let last = world.last_sent(a).await;
    // Numbers past it drawn for another replica.
    sqlx::query("INSERT INTO sync.notes (kind, replica_id) VALUES ('resync', $1)")
        .bind(b)
        .execute(&world.pool)
        .await
        .unwrap();
    world.pass(&wire).await;
    let applied = || async {
        sqlx::query_scalar::<_, Option<i64>>(
            "SELECT applied_sequence FROM sync.replicas WHERE replica_id = $1",
        )
        .bind(a)
        .fetch_one(&world.pool)
        .await
        .unwrap()
    };
    let before = applied().await;
    for sequence in [world.last_sent(b).await, u64::MAX, i64::MAX as u64 + 1] {
        world.acknowledge(a, sequence).await;
        assert_eq!(applied().await, before, "{sequence}");
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
                let keys: Vec<String> = keys.iter().map(Uuid::to_string).collect();
                Digest {
                    table: (*table).to_owned(),
                    count: keys.len() as u64,
                    fingerprint: fingerprint(keys.clone()),
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
            crate::testkit::file(&world.pool, &world.codecs, &up, a, &()).await;
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

    // Digests of one table twice are no report.
    file(digests_of(
        &[("items", &[one, two]), ("items", &[one])],
        last,
    ))
    .await;
    assert_eq!(world.count(resyncs).await, 0, "malformed: not compared");

    // A digest of a table the server does not send is the replica's own
    // business, and the others are compared all the same.
    file(digests_of(&[("readings", &[one]), ("items", &[one])], last)).await;
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

/// A relay that recorded a pass and died before it sent it: whoever runs the
/// relay next sends what it recorded, once, under the same ids — and plans
/// nothing again.
#[sqlx::test(migrations = false)]
async fn a_pass_recorded_and_never_sent_is_sent_by_the_next_relay(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, b] = world.replicas;
    let wire = Wire::new(1);
    world.item("once").await;

    let relay = world.relay();
    world.record(&relay).await;
    assert!(wire.empty(), "nothing went before the record committed");
    assert_eq!(world.held(a).await, 1, "recorded before it goes");

    // Another process takes the lock: it publishes the outbox first.
    let next = world.relay();
    let outcome = world.pass_relay(&next, &wire).await;
    assert_eq!(outcome.done, 0, "nothing planned again");
    assert_eq!(outcome.sent, 2);
    for replica in [a, b] {
        let ids = wire.ids(replica);
        assert_eq!(ids.len(), 1, "sent once");
        let mut model = world.follower(replica);
        model.receive(&wire.next(replica).unwrap(), &Rules::ANY);
        assert_eq!(items_of(&model), names(&["once"]));
    }
    assert_eq!(world.outbox(a).await + world.outbox(b).await, 0);
}

/// A relay that loses its lock while it records commits nothing: nothing
/// was sent, nothing is recorded, the notes are pending, and the next relay
/// sends it all.
#[sqlx::test(migrations = false)]
async fn a_relay_that_loses_its_lock_mid_pass_sends_and_records_nothing(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, _] = world.replicas;
    let wire = Wire::new(2);
    world.item("kept").await;
    let relay = world.relay();
    let mut tx = world.pool.begin_with(SNAPSHOT).await.unwrap();
    relay.record(&mut tx).await.unwrap();
    // The lock's connection is gone: its transaction never commits.
    drop(tx);
    world.publish(&relay, &wire).await;
    assert!(wire.empty());
    assert_eq!(world.held(a).await, 0);
    assert_eq!(
        world
            .count("SELECT count(*) FROM sync.notes WHERE done_at IS NULL")
            .await,
        1
    );

    world.pass_relay(&relay, &wire).await;
    let mut model = world.follower(a);
    model.receive(&wire.next(a).unwrap(), &Rules::ANY);
    assert_eq!(items_of(&model), names(&["kept"]));
    assert_eq!(world.held(a).await, 1);
}

/// A replica sent part of what it was owed before its transport failed,
/// then a row it took leaves its scope: it is told to let the row go, and
/// ends holding exactly what it should — the part that went was recorded,
/// the part that did not waits, in order.
#[sqlx::test(migrations = false)]
async fn a_partial_send_then_a_row_leaving_scope_leaves_no_orphan(pool: crate::DbPool) {
    struct Partial {
        wire: Wire,
        /// Sends to the first replica that go before the transport fails.
        left: Mutex<usize>,
        replica: Uuid,
    }
    impl Transport for Partial {
        async fn send(&self, replica: Uuid, message: Vec<u8>, id: &str) -> Result<(), Error> {
            if replica == self.replica {
                let mut left = self.left.lock().unwrap();
                if *left == 0 {
                    return Err(Error::Transport {
                        reason: "cut".into(),
                        transient: true,
                    });
                }
                *left -= 1;
            }
            self.wire.send(replica, message, id).await
        }
    }
    let world = world(pool).await;
    let [a, _] = world.replicas;
    let relay = Relay {
        limits: Limits {
            rows: 1,
            bytes: 1 << 20,
        },
        ..world.relay()
    };
    let wire = Partial {
        wire: Wire::new(3),
        left: Mutex::new(1),
        replica: a,
    };
    let first = world.item("first").await;
    world.item("second").await;
    world.pass_relay(&relay, &wire).await;
    assert_eq!(wire.wire.ids(a).len(), 1, "one of two went");

    // The row that went leaves every replica.
    world
        .exec(
            "DELETE FROM fixture.items WHERE id = $1 AND partition_id = $2 AND $3 <> ''",
            first,
            "x",
        )
        .await;
    *wire.left.lock().unwrap() = usize::MAX;
    sqlx::query("UPDATE sync.replicas SET retry_after = now()")
        .execute(&world.pool)
        .await
        .unwrap();
    world.pass_relay(&relay, &wire).await;

    let mut model = world.follower(a);
    while let Some(bytes) = wire.wire.next(a) {
        model.receive(&bytes, &Rules::ANY);
        wire.wire.acknowledge(a);
    }
    assert_eq!(items_of(&model), names(&["second"]), "nothing left behind");
    let held: Vec<String> =
        sqlx::query_scalar("SELECT row_key FROM sync.held WHERE replica_id = $1")
            .bind(a)
            .fetch_all(&world.pool)
            .await
            .unwrap();
    assert_eq!(held.len(), 1, "recorded as it holds: {held:?}");
}

impl World {
    /// A model of `replica` as it stands once provisioned, with nothing:
    /// what each test's replica starts as.
    fn follower(&self, replica: Uuid) -> Model {
        self.provisioned[&replica].clone()
    }

    /// The server's epoch.
    async fn epoch(&self) -> Uuid {
        sqlx::query_scalar("SELECT epoch FROM sync.epoch")
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }

    /// The last message sent to `replica`.
    async fn last_sent(&self, replica: Uuid) -> u64 {
        sqlx::query_scalar::<_, i64>("SELECT last_sequence FROM sync.chains WHERE replica_id = $1")
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
            epoch: epoch.to_string(),
            sequence,
            ..envelope::Up::default()
        };
        crate::testkit::file(&self.pool, &self.codecs, &ack, replica, &()).await;
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

fn tag_to_wire(tag: Tag) -> Result<ItemRow, MapError> {
    to_wire(Item {
        id: tag.id,
        partition_id: tag.partition_id,
        name: tag.name,
    })
}

fn tag_from_wire(row: ItemRow) -> Result<Tag, MapError> {
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

impl World {
    /// Passes, each followed by every replica taking what reached it and
    /// saying what it applied, until nothing moves.
    async fn settle(&self, wire: &Wire, models: &mut BTreeMap<Uuid, Model>) {
        sqlx::query("UPDATE sync.replicas SET retry_after = NULL")
            .execute(&self.pool)
            .await
            .unwrap();
        for _ in 0..50 {
            let passed = self.pass(wire).await;
            let mut moved = passed.done + passed.sent > 0;
            for (replica, model) in models.iter_mut() {
                while let Some(bytes) = wire.next(*replica) {
                    model.receive(&bytes, &REFUSING);
                    wire.acknowledge(*replica);
                    moved = true;
                }
                if let Some(up) = model.up() {
                    crate::testkit::file(&self.pool, &self.codecs, &up, *replica, &()).await;
                    model.delivered(&up);
                    moved = true;
                }
            }
            if !moved {
                return;
            }
        }
        panic!("did not settle");
    }

    /// The modelled replicas, each as its provisioning left it.
    fn models(&self) -> BTreeMap<Uuid, Model> {
        self.replicas
            .iter()
            .map(|r| (*r, self.follower(*r)))
            .collect()
    }

    /// Each replica holds what the engine records — what it took as taken,
    /// what it refused as refused — and what it should, but for what it
    /// refused.
    async fn assert_converged(&self, models: &BTreeMap<Uuid, Model>) {
        let should: BTreeSet<String> =
            sqlx::query_scalar("SELECT name FROM fixture.items WHERE retired_at IS NULL")
                .fetch_all(&self.pool)
                .await
                .unwrap()
                .into_iter()
                .collect();
        for (replica, model) in models {
            let recorded: Vec<(String, bool)> = sqlx::query_as(
                "SELECT row_key, refused IS NOT NULL FROM sync.held WHERE replica_id = $1",
            )
            .bind(replica)
            .fetch_all(&self.pool)
            .await
            .unwrap();
            let as_recorded = |refused: bool| -> BTreeSet<String> {
                recorded
                    .iter()
                    .filter(|(_, r)| *r == refused)
                    .map(|(k, _)| k.clone())
                    .collect()
            };
            let rejected: BTreeSet<String> =
                model.rejections.keys().map(|(_, k)| k.clone()).collect();
            let taken: BTreeSet<String> = model
                .rows
                .keys()
                .map(|(_, k)| k.clone())
                .filter(|k| !rejected.contains(k))
                .collect();
            assert_eq!(
                as_recorded(false),
                taken,
                "{replica}: recorded taken other than it is"
            );
            assert_eq!(
                as_recorded(true),
                rejected,
                "{replica}: recorded refused other than it is"
            );
            if rejected.is_empty() {
                assert_eq!(
                    items_of(model),
                    should,
                    "{replica} holds other than it should"
                );
            }
        }
    }
}

/// What the replicas of these tests refuse: a row named "refused".
const REFUSING: Rules = Rules {
    refuses: |_, bytes| ItemRow::decode(bytes).is_ok_and(|row| row.name == "refused"),
    parents: |_, _| Vec::new(),
};

impl World {
    /// Every message `replica` was sent, taken by its model as `rules` say.
    fn deliver(&self, wire: &Wire, replica: Uuid, model: &mut Model, rules: &Rules) {
        while let Some(bytes) = wire.next(replica) {
            model.receive(&bytes, rules);
            wire.acknowledge(replica);
        }
    }

    /// What `model` of `replica` has to say, filed.
    async fn say(&self, replica: Uuid, model: &mut Model) {
        if let Some(up) = model.up() {
            crate::testkit::file(&self.pool, &self.codecs, &up, replica, &()).await;
            model.delivered(&up);
        }
    }

    async fn rename(&self, id: Uuid, name: &str) {
        self.exec(
            "UPDATE fixture.items SET name = $3 WHERE id = $1 AND partition_id = $2",
            id,
            name,
        )
        .await;
    }

    async fn refused(&self) -> i64 {
        self.count("SELECT count(*) FROM sync.held WHERE refused IS NOT NULL")
            .await
    }
}

/// A rejection that reaches the server after a newer version of its row was
/// recorded is of a version that no longer matters: the row stays held, and
/// the newer one arrives.
#[sqlx::test(migrations = false)]
async fn a_stale_rejection_after_a_newer_delivery_changes_nothing(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, _] = world.replicas;
    let wire = Wire::new(11);
    let mut models = world.models();
    let x = world.item("one").await;
    world.settle(&wire, &mut models).await;
    let mut model = models.remove(&a).unwrap();
    world.rename(x, "refused").await;
    world.pass(&wire).await;
    world.deliver(&wire, a, &mut model, &REFUSING);
    world.rename(x, "two").await;
    world.pass(&wire).await;
    world.say(a, &mut model).await;
    assert_eq!(world.refused().await, 0, "a newer version went since");
    world.deliver(&wire, a, &mut model, &REFUSING);
    world.say(a, &mut model).await;
    models.insert(a, model);
    world.settle(&wire, &mut models).await;
    world.assert_converged(&models).await;
    assert_eq!(items_of(&models[&a]), names(&["two"]));
}

/// A replica that refuses a new version of a row it holds keeps the old
/// one: the server counts it held for what takes rows back, so when the row
/// leaves the replica's scope, the delete still goes.
#[sqlx::test(migrations = false)]
async fn a_refused_version_of_a_held_row_is_still_taken_back(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, _] = world.replicas;
    let wire = Wire::new(12);
    let mut models = world.models();
    let x = world.item("one").await;
    world.settle(&wire, &mut models).await;
    world.rename(x, "refused").await;
    let mut model = models.remove(&a).unwrap();
    world.pass(&wire).await;
    world.deliver(&wire, a, &mut model, &REFUSING);
    world.say(a, &mut model).await;
    assert_eq!(world.refused().await, 1);
    assert_eq!(items_of(&model), names(&["one"]), "it keeps what it had");
    world
        .exec(
            "DELETE FROM fixture.items WHERE id = $1 AND partition_id = $2 AND $3 <> ''",
            x,
            "x",
        )
        .await;
    world.pass(&wire).await;
    world.deliver(&wire, a, &mut model, &REFUSING);
    world.say(a, &mut model).await;
    assert!(
        model.rows.is_empty(),
        "the delete went: {:?}",
        items_of(&model)
    );
    models.insert(a, model);
    world.settle(&wire, &mut models).await;
    world.assert_converged(&models).await;
}

/// A delete the replica cannot carry out — a row of its own points at the
/// row — leaves the row held, refused and quarantined; once the replica can
/// let go, the retry sends the delete again.
#[sqlx::test(migrations = false)]
async fn a_refused_delete_is_sent_again_after_a_fix(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, _] = world.replicas;
    let wire = Wire::new(13);
    let mut models = world.models();
    let x = world.item("one").await;
    world.settle(&wire, &mut models).await;
    let row = ("items".to_owned(), x.to_string());
    models.get_mut(&a).unwrap().pinned.insert(row.clone());
    world
        .exec(
            "DELETE FROM fixture.items WHERE id = $1 AND partition_id = $2 AND $3 <> ''",
            x,
            "x",
        )
        .await;
    world.settle(&wire, &mut models).await;
    assert_eq!(world.refused().await, 1, "held again, refused");
    assert_eq!(items_of(&models[&a]), names(&["one"]));
    world.assert_converged(&models).await;

    models.get_mut(&a).unwrap().pinned.remove(&row);
    retry_quarantine(&mut world.pool.acquire().await.unwrap())
        .await
        .unwrap();
    world.settle(&wire, &mut models).await;
    assert!(models[&a].rows.is_empty());
    assert_eq!(world.refused().await, 0);
    assert_eq!(world.count("SELECT count(*) FROM sync.quarantine").await, 0);
    world.assert_converged(&models).await;
}

/// A snapshot that sends again a row the replica holds, in a version it
/// refuses, names it all the same: its end does not sweep the old copy, the
/// rejection stands, and the server records the row refused.
#[sqlx::test(migrations = false)]
async fn a_snapshot_does_not_sweep_a_row_it_carried_and_the_replica_refused(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, _] = world.replicas;
    let wire = Wire::new(14);
    let mut models = world.models();
    let x = world.item("one").await;
    world.settle(&wire, &mut models).await;
    world.rename(x, "refused").await;
    sqlx::query("INSERT INTO sync.notes (kind, replica_id) VALUES ('resync', $1)")
        .bind(a)
        .execute(&world.pool)
        .await
        .unwrap();
    let mut model = models.remove(&a).unwrap();
    world.pass(&wire).await;
    world.deliver(&wire, a, &mut model, &REFUSING);
    assert_eq!(items_of(&model), names(&["one"]), "named, not swept");
    assert_eq!(model.rejections.len(), 1, "the rejection stands");
    world.say(a, &mut model).await;
    assert_eq!(world.refused().await, 1);
    models.insert(a, model);
    world.settle(&wire, &mut models).await;
    world.assert_converged(&models).await;
}

/// A server database restored from a copy older than what its replicas
/// applied numbers its messages again from where the copy left off: a
/// replica drops them as applied already, and says what it applied, which
/// is past every number the server remembers drawing. The server takes a
/// new epoch and provisions every replica again, and each ends holding
/// what the restored server has.
#[sqlx::test(migrations = false)]
async fn a_server_restored_from_an_older_copy_provisions_its_replicas_again(pool: crate::DbPool) {
    let world = world(pool).await;
    let wire = Wire::new(7);
    let mut models = world.models();
    world.item("one").await;
    world.settle(&wire, &mut models).await;
    crate::testkit::back_up(
        &mut world.pool.acquire().await.unwrap(),
        &["fixture", "sync"],
    )
    .await;
    let before = world.epoch().await;
    for n in 0..5 {
        world.item(&format!("lost {n}")).await;
        world.settle(&wire, &mut models).await;
    }
    crate::testkit::restore(
        &mut world.pool.acquire().await.unwrap(),
        &["fixture", "sync"],
    )
    .await;
    world.item("after").await;
    world.settle(&wire, &mut models).await;

    assert_ne!(before, world.epoch().await, "a new epoch");
    world.assert_converged(&models).await;
    assert_eq!(
        items_of(&models[&world.replicas[0]]),
        names(&["one", "after"])
    );
}

/// A replica restored from a copy older than what it applied misses the
/// messages since: the next one follows one it never applied, so it says it
/// has a gap, is provisioned again, and ends holding what it should.
#[sqlx::test(migrations = false)]
async fn a_replica_restored_from_an_older_copy_is_provisioned_again(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, _] = world.replicas;
    let wire = Wire::new(8);
    let mut models = world.models();
    world.item("one").await;
    world.settle(&wire, &mut models).await;
    let copy = models[&a].clone();
    world.item("two").await;
    world.settle(&wire, &mut models).await;
    models.insert(a, copy);
    world.item("three").await;
    world.settle(&wire, &mut models).await;

    assert!(!models[&a].gap);
    world.assert_converged(&models).await;
    assert_eq!(items_of(&models[&a]), names(&["one", "two", "three"]));
}

/// A message the transport lost is a gap the replica says, and is answered
/// by a provisioning: what the lost message carried arrives.
#[sqlx::test(migrations = false)]
async fn a_message_lost_on_the_way_is_found_by_the_next(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, _] = world.replicas;
    let wire = Wire::new(9);
    let mut models = world.models();
    world.item("lost").await;
    world.pass(&wire).await;
    wire.acknowledge(a);
    world.item("next").await;
    world.settle(&wire, &mut models).await;
    world.assert_converged(&models).await;
    assert_eq!(items_of(&models[&a]), names(&["lost", "next"]));
}

/// A row of a table keyed by its replica is its sender's, whatever it says:
/// one that names another replica is malformed, left out — neither filed
/// nor kept — while the sender's own is filed.
#[sqlx::test(migrations = false)]
async fn an_uploaded_row_of_another_replica_is_left_out(pool: crate::DbPool) {
    use crate::conformance::Reading;
    use crate::envelope::{Table as WireTable, Up};
    let world = world(pool).await;
    let [a, b] = world.replicas;
    sqlx::raw_sql(
        "CREATE TABLE fixture.readings (replica_id UUID NOT NULL, id UUID NOT NULL, \
         value BIGINT NOT NULL, at BIGINT NOT NULL, PRIMARY KEY (replica_id, id))",
    )
    .execute(&world.pool)
    .await
    .unwrap();
    let codecs = vec![Codec::<()>::json::<Reading>(None)];
    let row = |replica: Uuid| {
        let reading = Reading {
            replica_id: replica,
            id: Uuid::now_v7(),
            value: 1,
            at: 1,
        };
        let table = Table::of::<Reading>();
        crate::wire::json_encode("readings", &table.columns, &reading).unwrap()
    };
    let upload = Up {
        tables: vec![WireTable {
            table: "readings".to_owned(),
            upserts: vec![row(a), row(b)],
            ..WireTable::default()
        }],
        protocol: envelope::PROTOCOL,
        ..Up::default()
    };
    crate::testkit::file(&world.pool, &codecs, &upload, a, &()).await;
    let filed: Vec<Uuid> = sqlx::query_scalar("SELECT replica_id FROM fixture.readings")
        .fetch_all(&world.pool)
        .await
        .unwrap();
    assert_eq!(filed, [a]);
    assert_eq!(world.count("SELECT count(*) FROM sync.inbox").await, 0);
}

/// What an upload kept in the inbox said of its moment — what the replica
/// refused, applied, held — was filed when it came: filed again, only its
/// rows are.
#[sqlx::test(migrations = false)]
async fn a_kept_upload_filed_again_files_only_its_rows(pool: crate::DbPool) {
    use crate::envelope::{Key as WireKey, Rejected, Up};
    let world = world(pool).await;
    let [a, _] = world.replicas;
    let id = world.item("one").await;
    world.pass(&Fake::default()).await;
    let said = Up {
        rejected: vec![Rejected {
            table: "items".to_owned(),
            key: Some(WireKey {
                parts: vec![id.to_string()],
            }),
            reason: "then".to_owned(),
            epoch: world.epoch().await.to_string(),
            sequence: world.last_sent(a).await,
        }],
        protocol: envelope::PROTOCOL,
        ..Up::default()
    };
    let mut tx = world.pool.begin().await.unwrap();
    crate::filing::keep_whole(&mut tx, a, &said.encode_to_vec(), "a bug")
        .await
        .unwrap();
    let kept = crate::filing::inbox(&mut tx, 0, i64::MAX, 10)
        .await
        .unwrap();
    crate::filing::refile(&mut tx, &world.codecs, &kept[0], &())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(world.count("SELECT count(*) FROM sync.quarantine").await, 0);
    assert_eq!(world.count("SELECT count(*) FROM sync.inbox").await, 0);
}

#[path = "status_tests.rs"]
mod status;
