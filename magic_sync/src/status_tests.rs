//! What the engine answers about itself, against the relay: every figure
//! counts what replicas confirmed, and a replica called synced holds exactly
//! what it should.

use super::*;

/// The progress a replica shows counts only what it confirmed applying: sent
/// is on its way, a change since makes a row outdated, a row no longer owed is
/// on its way out.
#[sqlx::test(migrations = false)]
async fn progress_counts_only_what_a_replica_confirmed(pool: crate::DbPool) {
    use crate::envelope::Up;

    let world = world(pool).await;
    let [a, b] = world.replicas;
    let wire = Fake::default();
    let one = world.item("one").await;
    let two = world.item("two").await;
    world.item("three").await;
    world.pass(&wire).await;

    let items = |progress: &crate::status::Progress, replica: Uuid| {
        progress
            .replicas
            .iter()
            .find(|p| p.replica == replica)
            .and_then(|p| p.tables.iter().find(|t| t.table == "items"))
            .cloned()
            .unwrap()
    };
    let mut conn = world.pool.acquire().await.unwrap();
    let progress = world.engine.progress(&mut conn, Scope::All).await.unwrap();
    let sent = items(&progress, a);
    assert_eq!(
        (sent.should, sent.holds, sent.in_flight),
        (3, 0, 3),
        "sent is not held"
    );

    // The replica says it applied everything it was sent.
    let (epoch, last): (Uuid, i64) = sqlx::query_as(
        "SELECT e.epoch, r.last_sequence FROM sync.epoch e, sync.replicas r WHERE r.replica_id = $1",
    )
    .bind(a)
    .fetch_one(&mut *conn)
    .await
    .unwrap();
    let ack = Up {
        replica_id: a.to_string(),
        epoch: epoch.to_string(),
        sequence: last as u64,
        ..Up::default()
    };
    crate::filing::file(&mut conn, &world.codecs, &ack, a, &())
        .await
        .unwrap();

    world.exec("UPDATE fixture.items SET name = 'one, renamed' WHERE id = $1 AND partition_id = $2 AND $3 <> ''", one, "x").await;
    world.exec("UPDATE fixture.items SET retired_at = now() WHERE id = $1 AND partition_id = $2 AND $3 <> ''", two, "x").await;
    let progress = world
        .engine
        .progress(&mut conn, Scope::Partition(world.partition))
        .await
        .unwrap();
    let confirmed = items(&progress, a);
    assert_eq!(
        (
            confirmed.should,
            confirmed.holds,
            confirmed.outdated,
            confirmed.in_flight,
            confirmed.to_remove
        ),
        (2, 2, 1, 0, 1),
        "{confirmed:?}"
    );
    assert_eq!(confirmed.up_to_date(), 1, "one of two as it should be");
    let unconfirmed = items(&progress, b);
    assert_eq!(
        (
            unconfirmed.holds,
            unconfirmed.in_flight,
            unconfirmed.to_remove
        ),
        (0, 2, 1),
        "b said nothing; one of what it was sent it should no longer hold"
    );

    let one_where = world
        .engine
        .row_progress(&mut conn, Scope::All, "items", &one.to_string())
        .await
        .unwrap();
    let places = |found: crate::status::RowProgress| -> BTreeMap<Uuid, RowState> {
        states_of(found).into_iter().collect()
    };
    assert_eq!(
        places(one_where),
        BTreeMap::from([(a, RowState::Outdated), (b, RowState::OnItsWay)])
    );
    let two_where = world
        .engine
        .row_progress(&mut conn, Scope::All, "items", &two.to_string())
        .await
        .unwrap();
    assert_eq!(places(two_where).get(&a), Some(&RowState::Leaving));
}

/// A transport that fails at random and queues what it takes, per replica,
/// until the replica takes it: a message can be on its way for a while.
struct Mailbox {
    walk: Mutex<crate::testkit::Walk>,
    failure_rate: Mutex<u64>,
    queues: Mutex<BTreeMap<Uuid, std::collections::VecDeque<Body>>>,
    replicas: Mutex<BTreeMap<Uuid, Modelled>>,
}

impl Transport for Mailbox {
    fn send(
        &self,
        replica: Uuid,
        message: Vec<u8>,
        _id: &str,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        let fails =
            self.walk.lock().unwrap().below(100) < *self.failure_rate.lock().unwrap() as usize;
        let result = if fails {
            Err(Error::Transport {
                reason: "the network".into(),
                transient: true,
            })
        } else {
            self.queues
                .lock()
                .unwrap()
                .entry(replica)
                .or_default()
                .push_back(body(&message));
            Ok(())
        };
        async move { result }
    }
}

impl Mailbox {
    /// `replica` takes up to `count` messages, in order: what it applied
    /// last, and what it holds.
    fn take(&self, replica: Uuid, count: usize) -> (u64, BTreeMap<Uuid, String>) {
        let mut queues = self.queues.lock().unwrap();
        let queue = queues.entry(replica).or_default();
        let mut replicas = self.replicas.lock().unwrap();
        let held = replicas.entry(replica).or_default();
        for _ in 0..count {
            let Some(message) = queue.pop_front() else {
                break;
            };
            held.apply(message);
        }
        (held.last, held.items.clone())
    }
}

/// A replica the dashboard calls synced holds exactly what it should — read
/// from what the engine records, never by counting what it should hold —
/// whatever the network and the encoding do; and once both are fixed and
/// every replica has said what it applied, every one is synced.
#[sqlx::test(migrations = false)]
async fn a_replica_called_synced_holds_exactly_what_it_should(pool: crate::DbPool) {
    let world = world(pool).await;
    let chaos = Mailbox {
        walk: Mutex::new(crate::testkit::Walk(0x5EED)),
        failure_rate: Mutex::new(20),
        queues: Mutex::new(BTreeMap::new()),
        replicas: Mutex::new(BTreeMap::new()),
    };
    let mut walk = crate::testkit::Walk(0xACC0);
    let mut synced_seen = 0;
    let should = || async {
        sqlx::query_as::<_, (Uuid, String)>(
            "SELECT id, name FROM fixture.items WHERE retired_at IS NULL",
        )
        .fetch_all(&world.pool)
        .await
        .unwrap()
        .into_iter()
        .collect::<BTreeMap<Uuid, String>>()
    };
    let modelled = |replica: Uuid| chaos.take(replica, 0).1;
    for step in 0..200 {
        let items: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM fixture.items ORDER BY id")
            .fetch_all(&world.pool)
            .await
            .unwrap();
        let pick = (!items.is_empty()).then(|| items[walk.below(items.len())]);
        let name = if walk.below(12) == 0 {
            "poison".to_owned()
        } else {
            format!("n{step}")
        };
        // Storms, each followed by a calm: no writes, the network back and a
        // fix deployed, so replicas can settle.
        let calm = step % 40 >= 28;
        if step % 40 == 28 {
            *chaos.failure_rate.lock().unwrap() = 0;
            BUGGY.set(false);
            let mut conn = world.pool.acquire().await.unwrap();
            unpark(&mut conn).await.unwrap();
            retry_quarantine(&mut conn).await.unwrap();
            sqlx::query("UPDATE sync.replicas SET retry_after = now()")
                .execute(&mut *conn)
                .await
                .unwrap();
        } else if step % 40 == 0 {
            *chaos.failure_rate.lock().unwrap() = 20;
            BUGGY.set(true);
        }
        if !calm {
            match (walk.below(6), pick) {
                (0 | 1, _) | (_, None) => {
                    world.item(&name).await;
                }
                (2, Some(id)) => {
                    world.exec("UPDATE fixture.items SET name = $3 WHERE id = $1 AND partition_id = $2", id, &name).await;
                }
                (3, Some(id)) => {
                    world.exec("UPDATE fixture.items SET retired_at = CASE WHEN retired_at IS NULL THEN now() END WHERE id = $1 AND partition_id = $2 AND $3 <> ''", id, &name).await;
                }
                (4, Some(id)) => {
                    world.exec("DELETE FROM fixture.items WHERE id = $1 AND partition_id = $2 AND $3 <> ''", id, &name).await;
                }
                _ => {}
            }
        }
        if calm || walk.below(2) == 0 {
            world.pass_with(&chaos).await;
        }
        for replica in world.replicas {
            // It takes some of what waits for it, and says so — or not yet.
            if walk.below(3) == 0 {
                let (last, _) = chaos.take(replica, walk.below(3));
                if last > 0 && walk.below(2) == 0 {
                    world.acknowledge(replica, last).await;
                }
            }
        }
        if step % 30 == 0 {
            sqlx::query("UPDATE sync.replicas SET retry_after = now()")
                .execute(&world.pool)
                .await
                .unwrap();
        }
        let overview = world.overview(Scope::All).await;
        let should = should().await;
        for replica in &overview.replicas.first {
            if replica.state == ReplicaState::Synced {
                synced_seen += 1;
                assert_eq!(
                    modelled(replica.replica),
                    should,
                    "step {step}: called synced, holds other: {replica:?}"
                );
            }
        }
    }
    assert!(
        synced_seen > 0,
        "no step was ever synced: the test proved nothing"
    );

    // The network back and the fix deployed: everything settles.
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
    for _ in 0..50 {
        if world.pass_with(&chaos).await.done == 0 {
            break;
        }
    }
    BUGGY.set(true);
    for replica in world.replicas {
        let (last, _) = chaos.take(replica, usize::MAX);
        world.acknowledge(replica, last).await;
    }
    let overview = world.overview(Scope::All).await;
    assert!(overview.health.healthy, "{:?}", overview.health);
    for replica in &overview.replicas.first {
        assert_eq!(replica.state, ReplicaState::Synced, "{replica:?}");
    }
    assert!(
        overview.tables.iter().all(|t| t.synced),
        "{:?}",
        overview.tables
    );
}

/// What a replica is waiting on reads, step by step, as what it is: a change
/// not yet planned, then a message it has not confirmed, then nothing — and,
/// per table, the rows in flight and the ones a newer change outdates.
#[sqlx::test(migrations = false)]
async fn the_overview_says_what_each_replica_and_table_waits_on(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, b] = world.replicas;
    let wire = Fake::default();
    let one = world.item("one").await;
    world.item("two").await;

    let noted = world.overview(Scope::All).await;
    assert!(noted.health.healthy);
    assert_eq!(noted.health.pending, 2);
    assert!(noted.health.oldest.is_some());
    for replica in &noted.replicas.first {
        assert_eq!(
            (replica.state, replica.pending),
            (ReplicaState::CatchingUp, 2)
        );
    }
    assert_eq!(noted.tables[0].pending, 2);

    world.pass(&wire).await;
    let sent = world.overview(Scope::All).await;
    for replica in &sent.replicas.first {
        assert_eq!(
            (replica.state, replica.pending, replica.unconfirmed),
            (ReplicaState::CatchingUp, 0, true),
            "sent is not held"
        );
        assert!(replica.sent_at.is_some() && replica.applied_at.is_none());
    }
    assert_eq!((sent.tables[0].in_flight, sent.tables[0].replicas), (4, 2));

    let pool = &world.pool;
    let last = |replica: Uuid| async move {
        sqlx::query_scalar::<_, i64>(
            "SELECT last_sequence FROM sync.replicas WHERE replica_id = $1",
        )
        .bind(replica)
        .fetch_one(pool)
        .await
        .unwrap() as u64
    };
    world.acknowledge(a, last(a).await).await;
    let half = world.overview(Scope::All).await;
    let state = |o: &crate::status::Overview, r: Uuid| {
        o.replicas
            .first
            .iter()
            .find(|x| x.replica == r)
            .unwrap()
            .state
    };
    assert_eq!(state(&half, a), ReplicaState::Synced);
    assert_eq!(state(&half, b), ReplicaState::CatchingUp);
    assert_eq!((half.tables[0].in_flight, half.tables[0].replicas), (2, 1));

    world.acknowledge(b, last(b).await).await;
    world
        .exec(
            "UPDATE fixture.items SET name = $3 WHERE id = $1 AND partition_id = $2",
            one,
            "one, renamed",
        )
        .await;
    let changed = world.overview(Scope::All).await;
    assert_eq!(
        (changed.tables[0].pending, changed.tables[0].in_flight),
        (1, 0),
        "{:?}",
        changed.tables
    );

    // What a replica says it wrote and the server has not filed holds it
    // back too, until it says that emptied.
    world.pass(&wire).await;
    for replica in [a, b] {
        world.acknowledge(replica, last(replica).await).await;
    }
    let say = |tables: Vec<envelope::TableBacklog>| async {
        let up = envelope::Up {
            replica_id: a.to_string(),
            backlog: Some(envelope::Backlog { tables }),
            ..envelope::Up::default()
        };
        let mut conn = world.pool.acquire().await.unwrap();
        crate::filing::file(&mut conn, &world.codecs, &up, a, &())
            .await
            .unwrap();
    };
    say(vec![envelope::TableBacklog {
        table: "sales".into(),
        unsent: 3,
        unfiled: 1,
    }])
    .await;
    let writing = world.overview(Scope::Replicas(&[a])).await;
    let backlog = writing.replicas.first[0].backlog.as_ref().expect("said");
    assert_eq!(
        (writing.replicas.first[0].state, backlog.tables[0].unsent),
        (ReplicaState::CatchingUp, 3)
    );
    say(vec![]).await;
    let written = world.overview(Scope::Replicas(&[a])).await;
    assert_eq!(written.replicas.first[0].state, ReplicaState::Synced);
}

/// A partition sees its own replicas, and what waits for them, and nothing of
/// another partition's; a replica that is not live is not one.
#[sqlx::test(migrations = false)]
async fn a_partition_sees_only_its_own(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, b] = world.replicas;
    let other = Uuid::now_v7();
    let theirs = Uuid::now_v7();
    let asleep = Uuid::now_v7();
    for (replica, partition, live) in [(theirs, other, true), (asleep, world.partition, false)] {
        sqlx::query("INSERT INTO fixture.t_replicas VALUES ($1, $2, $3)")
            .bind(replica)
            .bind(partition)
            .bind(live)
            .execute(&world.pool)
            .await
            .unwrap();
    }
    let wire = Fake::default();
    world.item("ours").await;
    sqlx::query("INSERT INTO fixture.items (id, partition_id, name) VALUES ($1, $2, 'poison')")
        .bind(Uuid::now_v7())
        .bind(other)
        .execute(&world.pool)
        .await
        .unwrap();
    world.pass(&wire).await;

    let ours = world.overview(Scope::Partition(world.partition)).await;
    let ids: Vec<Uuid> = ours.replicas.first.iter().map(|r| r.replica).collect();
    assert_eq!(ids, BTreeSet::from([a, b]).into_iter().collect::<Vec<_>>());
    assert!(ours.health.healthy, "{:?}", ours.health);
    assert_eq!(ours.tables[0].quarantined, 0);

    let all = world.overview(Scope::All).await;
    assert_eq!(all.replicas.first.len(), 3, "the live ones");
    assert_eq!(all.health.quarantine.total, 1);
    assert_eq!(all.health.quarantine.first[0].replica, theirs);
    let them = all
        .replicas
        .first
        .iter()
        .find(|r| r.replica == theirs)
        .unwrap();
    assert_eq!(them.state, ReplicaState::AwaitingFix);

    let listed = world.overview(Scope::Replicas(&[a, asleep])).await;
    assert_eq!(
        listed.replicas.first.len(),
        1,
        "a list is narrowed to the live"
    );
}

/// The JSON a dashboard reads is the one `dashboard/src/core/contract/fixtures/`
/// holds: every state a replica or a row can be in and every kind of
/// problem, ids and times made stable. Each fixture is checked, field for
/// field, against the generated types by the dashboard's type-check, and
/// read by its tests: a change on either side shows on the other.
/// `UPDATE_CONTRACT=1` writes them.
#[sqlx::test(migrations = false)]
async fn the_json_a_dashboard_reads_is_the_contract(pool: crate::DbPool) {
    use crate::envelope::{Key as WireKey, Rejected, Table as WireTable, TableBacklog, Up};
    let world = world(pool).await;
    let [synced, refusing] = world.replicas;
    let (silent, away) = (Uuid::now_v7(), Uuid::now_v7());
    for replica in [silent, away] {
        sqlx::query("INSERT INTO fixture.t_replicas VALUES ($1, $2, true)")
            .bind(replica)
            .bind(world.partition)
            .execute(&world.pool)
            .await
            .unwrap();
    }
    let wire = Fake::default();
    wire.failing.lock().unwrap().insert(away, true);
    let one = world.item("one").await;
    let two = world.item("two").await;
    let three = world.item("three").await;
    sqlx::query(
        "INSERT INTO sync.notes (kind, relation, row_key, partition_id, scope) \
         VALUES ('row', 'items', 'not-a-key', $1, $2)",
    )
    .bind(world.partition)
    .bind(refusing)
    .execute(&world.pool)
    .await
    .unwrap();
    // Changes the planner set aside, one of each kind: owed to or about
    // `refusing`, or of a partition no replica in the scope belongs to.
    sqlx::query(
        "INSERT INTO sync.notes (kind, relation, replica_id, partition_id, parked_at, attempts, last_error) \
         VALUES ('members', 'list_members', NULL, $3, now(), 1, 'a bug'), \
                ('assigned', 'replica_lists', $2, $1, now(), 1, 'a bug'), \
                ('resync', NULL, $2, $1, now(), 1, 'a bug')",
    )
    .bind(world.partition)
    .bind(refusing)
    .bind(Uuid::now_v7())
    .execute(&world.pool)
    .await
    .unwrap();
    world.pass(&wire).await;
    for replica in [synced, refusing] {
        world
            .acknowledge(replica, world.last_sent(replica).await)
            .await;
    }
    let say = |from: Uuid, up: Up| {
        let pool = world.pool.clone();
        let codecs = &world.codecs;
        async move {
            let mut conn = pool.acquire().await.unwrap();
            crate::filing::file(&mut conn, codecs, &up, from, &())
                .await
                .unwrap();
        }
    };
    say(
        refusing,
        Up {
            replica_id: refusing.to_string(),
            rejected: vec![Rejected {
                table: "items".into(),
                key: Some(WireKey {
                    parts: vec![two.to_string()],
                }),
                reason: "a check this build has".into(),
            }],
            tables: vec![WireTable {
                table: "from_a_newer_build".into(),
                upserts: vec![vec![1, 2, 3]],
                ..WireTable::default()
            }],
            ..Up::default()
        },
    )
    .await;
    say(
        silent,
        Up {
            replica_id: silent.to_string(),
            backlog: Some(envelope::Backlog {
                tables: vec![TableBacklog {
                    table: "receipts".into(),
                    unsent: 2,
                    unfiled: 1,
                }],
            }),
            ..Up::default()
        },
    )
    .await;

    let mut conn = world.pool.acquire().await.unwrap();
    let overview = world
        .engine
        .overview(&mut conn, Scope::All, 10)
        .await
        .unwrap();
    let states: BTreeSet<ReplicaState> = overview.replicas.first.iter().map(|r| r.state).collect();
    assert_eq!(states.len(), 4, "every state: {:?}", overview.replicas);
    let kinds: BTreeSet<String> = overview
        .health
        .parked
        .first
        .iter()
        .map(|p| format!("{:?}", p.kind))
        .collect();
    assert_eq!(kinds.len(), 4, "every kind of note: {:?}", overview.health);
    let listing = world
        .engine
        .listing(&mut conn, Scope::All, Some(ReplicaState::CatchingUp), 0, 10)
        .await
        .unwrap();
    let progress = world
        .engine
        .progress(&mut conn, Scope::Replicas(&[synced, silent]))
        .await
        .unwrap();

    // Then `one` changes and `two` retires, for every state of a row.
    world
        .exec(
            "UPDATE fixture.items SET name = $3 WHERE id = $1 AND partition_id = $2",
            one,
            "one, renamed",
        )
        .await;
    world
        .exec(
            "UPDATE fixture.items SET retired_at = now() WHERE id = $1 AND partition_id = $2 AND $3 <> ''",
            two,
            "x",
        )
        .await;
    let mut rows = Vec::new();
    for key in [one, two, three] {
        rows.push(
            world
                .engine
                .row_progress(&mut conn, Scope::All, "items", &key.to_string())
                .await
                .unwrap(),
        );
    }
    let places: BTreeSet<RowState> = rows
        .iter()
        .flat_map(|r| r.places.iter().map(|p| p.state))
        .collect();
    assert_eq!(places.len(), 6, "every state of a row: {rows:?}");

    let mut stable = Stable::default();
    for (file, ty, value) in [
        (
            "overview",
            "Overview",
            serde_json::to_value(&overview).unwrap(),
        ),
        (
            "listing",
            "Listing",
            serde_json::to_value(&listing).unwrap(),
        ),
        (
            "progress",
            "Progress",
            serde_json::to_value(&progress).unwrap(),
        ),
        (
            "rows",
            "RowProgress[]",
            serde_json::to_value(&rows).unwrap(),
        ),
    ] {
        let json = serde_json::to_string_pretty(&stable.of(value)).unwrap();
        let import = ty.trim_end_matches("[]");
        let text = format!(
            "// Generated by magic_sync's `the_json_a_dashboard_reads_is_the_contract`: \
             UPDATE_CONTRACT=1 rewrites it.\n\
             import type {{ {import} }} from '../types.js'\n\n\
             export default {json} satisfies {ty}\n"
        );
        crate::status::tests::matches_contract(&format!("fixtures/{file}.ts"), &text);
    }
}

/// Ids and times made stable: each id the same placeholder wherever it
/// appears, in the order it first does, and every time the same moment, in
/// the format the engine writes.
#[derive(Default)]
struct Stable {
    ids: BTreeMap<String, String>,
}

impl Stable {
    fn of(&mut self, value: serde_json::Value) -> serde_json::Value {
        use serde_json::Value;
        match value {
            Value::String(s) => Value::String(self.text(&s)),
            Value::Array(items) => Value::Array(items.into_iter().map(|v| self.of(v)).collect()),
            Value::Object(fields) => {
                Value::Object(fields.into_iter().map(|(k, v)| (k, self.of(v))).collect())
            }
            other => other,
        }
    }

    fn text(&mut self, s: &str) -> String {
        if chrono::DateTime::parse_from_rfc3339(s).is_ok() {
            // One moment, written as the engine writes every time.
            let moment = chrono::DateTime::from_timestamp(1_767_225_600, 123_456_000).unwrap();
            return serde_json::to_value(moment)
                .unwrap()
                .as_str()
                .unwrap()
                .to_owned();
        }
        let mut out = s.to_owned();
        let mut at = 0;
        while let Some(found) = out.get(at..).and_then(|rest| {
            rest.char_indices()
                .find(|(i, _)| {
                    rest.get(*i..*i + 36)
                        .is_some_and(|c| Uuid::parse_str(c).is_ok())
                })
                .map(|(i, _)| at + i)
        }) {
            let id = out[found..found + 36].to_owned();
            let next = self.ids.len() + 1;
            let stable = self
                .ids
                .entry(id.clone())
                .or_insert_with(|| format!("00000000-0000-7000-8000-{next:012}"))
                .clone();
            out.replace_range(found..found + 36, &stable);
            at = found + 36;
        }
        out
    }
}

/// A change owed to one replica alone, after a send to it failed, outdates
/// only that replica's copy: the others, sent it already, hold it as it is —
/// even while an older transaction keeps their horizon behind the change.
#[sqlx::test(migrations = false)]
async fn a_change_owed_to_one_replica_outdates_no_other(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, b] = world.replicas;
    let wire = Fake::default();
    wire.failing.lock().unwrap().insert(a, true);
    let mut older = world.pool.begin().await.unwrap();
    sqlx::query("SELECT pg_current_xact_id()")
        .execute(&mut *older)
        .await
        .unwrap();
    let one = world.item("one").await;
    world.pass(&wire).await;
    let last: i64 =
        sqlx::query_scalar("SELECT last_sequence FROM sync.replicas WHERE replica_id = $1")
            .bind(b)
            .fetch_one(&world.pool)
            .await
            .unwrap();
    world.acknowledge(b, last as u64).await;

    let mut conn = world.pool.acquire().await.unwrap();
    let progress = world
        .engine
        .progress(&mut conn, Scope::Replicas(&[b]))
        .await
        .unwrap();
    assert_eq!(
        (
            progress.replicas[0].tables[0].holds,
            progress.replicas[0].tables[0].outdated
        ),
        (1, 0),
        "{progress:?}"
    );
    let places = world
        .engine
        .row_progress(&mut conn, Scope::Replicas(&[b]), "items", &one.to_string())
        .await
        .unwrap();
    assert_eq!(states_of(places), vec![(b, RowState::Holds)]);
    older.rollback().await.unwrap();
}

/// A replica that is no longer live is no one's to wait for: its failures
/// leave the health, which every read counts over live replicas only.
#[sqlx::test(migrations = false)]
async fn a_replica_retired_while_failing_leaves_the_health(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, _] = world.replicas;
    let wire = Fake::default();
    wire.failing.lock().unwrap().insert(a, true);
    world.item("one").await;
    world.pass(&wire).await;
    assert_eq!(world.overview(Scope::All).await.health.failing.total, 1);

    sqlx::query("UPDATE fixture.t_replicas SET live = false WHERE id = $1")
        .bind(a)
        .execute(&world.pool)
        .await
        .unwrap();
    let health = world.overview(Scope::All).await.health;
    assert!(health.healthy, "{health:?}");
}

/// How long a replica has not answered runs from when it fell behind, or
/// from its last confirmation that left it behind — not from the last thing
/// it confirmed, days ago, before it was sent anything new.
#[sqlx::test(migrations = false)]
async fn waiting_runs_from_when_a_replica_fell_behind(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, _] = world.replicas;
    let wire = Fake::default();
    world.item("one").await;
    world.pass(&wire).await;
    let first = world.last_sent(a).await;
    let waiting = |replica: Uuid| {
        let pool = world.pool.clone();
        async move {
            sqlx::query_scalar::<_, Option<chrono::DateTime<chrono::Utc>>>(
                "SELECT waiting_since FROM sync.replicas WHERE replica_id = $1",
            )
            .bind(replica)
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    assert!(waiting(a).await.is_some(), "sent, not confirmed");
    world.acknowledge(a, first).await;
    assert!(waiting(a).await.is_none(), "caught up");

    // Long quiet, then sent two messages; it confirms the first only.
    sqlx::query("UPDATE sync.replicas SET applied_at = now() - interval '2 days'")
        .execute(&world.pool)
        .await
        .unwrap();
    world.item("two").await;
    world.pass(&wire).await;
    let since = waiting(a).await.expect("behind again");
    assert!(chrono::Utc::now() - since < chrono::Duration::minutes(1));
    let overview = world.overview(Scope::Replicas(&[a])).await;
    assert_eq!(overview.replicas.first[0].waiting_since, Some(since));
    world.item("three").await;
    world.pass(&wire).await;
    world.acknowledge(a, world.last_sent(a).await - 1).await;
    let moved = waiting(a).await.expect("still behind");
    assert!(moved >= since, "its last confirmation restarts the wait");
    world.acknowledge(a, world.last_sent(a).await).await;
    assert!(waiting(a).await.is_none());
}

/// A customer, sent to the replicas its lists are given to.
#[derive(sqlx::FromRow, Clone, Debug, crate::Row)]
#[row(
    table = "customers",
    schema = "fixture",
    mirror = "catalog",
    sync = "grouped(members = list_members, assigned = replica_lists)"
)]
struct Customer {
    id: Uuid,
    partition_id: Uuid,
}

/// A row in two lists given to the same replica is one row it should hold,
/// not two.
#[sqlx::test(migrations = false)]
async fn a_row_reaching_a_replica_through_two_groups_counts_once(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, _] = world.replicas;
    sqlx::raw_sql(
        "CREATE TABLE fixture.customers (id UUID PRIMARY KEY, partition_id UUID NOT NULL);
         CREATE TABLE fixture.lists (id UUID PRIMARY KEY);
         CREATE TABLE fixture.list_members (
             list_id UUID NOT NULL REFERENCES fixture.lists (id),
             customer_id UUID NOT NULL REFERENCES fixture.customers (id),
             partition_id UUID NOT NULL, PRIMARY KEY (list_id, customer_id));
         CREATE TABLE fixture.replica_lists (
             list_id UUID NOT NULL REFERENCES fixture.lists (id),
             replica_id UUID NOT NULL REFERENCES fixture.t_replicas (id),
             partition_id UUID NOT NULL, PRIMARY KEY (list_id, replica_id));
         CREATE VIEW customers AS SELECT * FROM fixture.customers;
         CREATE VIEW list_members AS SELECT * FROM fixture.list_members;
         CREATE VIEW replica_lists AS SELECT * FROM fixture.replica_lists;
         SELECT sync.mirror_table('fixture', 'customers', '{}', 'partition_id');
         SELECT sync.mirror_group('fixture', 'list_members', 'list_id', 'members', 'partition_id');
         SELECT sync.mirror_group('fixture', 'replica_lists', 'replica_id', 'assigned', 'partition_id');",
    )
    .execute(&world.pool)
    .await
    .unwrap();
    let customer = Uuid::now_v7();
    let mut conn = world.pool.acquire().await.unwrap();
    sqlx::query("INSERT INTO fixture.customers VALUES ($1, $2)")
        .bind(customer)
        .bind(world.partition)
        .execute(&mut *conn)
        .await
        .unwrap();
    for list in [Uuid::now_v7(), Uuid::now_v7()] {
        let partition = world.partition;
        sqlx::raw_sql(&format!(
            "INSERT INTO fixture.lists VALUES ('{list}');
             INSERT INTO fixture.list_members VALUES ('{list}', '{customer}', '{partition}');
             INSERT INTO fixture.replica_lists VALUES ('{list}', '{a}', '{partition}');"
        ))
        .execute(&mut *conn)
        .await
        .unwrap();
    }
    let tables = [Table::of::<Item>(), Table::of::<Customer>()];
    let engine = Engine::new(&mut conn, &[&tables[0], &tables[1]], REPLICAS)
        .await
        .unwrap();

    let progress = engine
        .progress(&mut conn, Scope::Replicas(&[a]))
        .await
        .unwrap();
    let customers = progress.replicas[0]
        .tables
        .iter()
        .find(|t| t.table == "customers")
        .unwrap();
    assert_eq!(customers.should, 1, "{customers:?}");
}

/// One replica out of reach is the network's to settle, not a fix to deploy:
/// the health stays healthy, and the changes owed to it alone do not make
/// the backlog look stuck.
#[sqlx::test(migrations = false)]
async fn a_replica_out_of_reach_leaves_the_health_healthy(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, _] = world.replicas;
    let wire = Fake::default();
    wire.failing.lock().unwrap().insert(a, true);
    world.item("one").await;
    world.pass(&wire).await;

    let overview = world.overview(Scope::All).await;
    assert_eq!(overview.health.failing.total, 1);
    assert!(overview.health.healthy, "{:?}", overview.health);
    assert_eq!(
        (overview.health.pending, overview.health.oldest),
        (1, None),
        "owed to the replica out of reach alone: not waiting on the planner"
    );
}

/// A change the planner set aside that may concern every replica is one
/// problem, the engine's: each replica is catching up on it, none of them
/// is waiting for a fix of its own.
#[sqlx::test(migrations = false)]
async fn a_change_set_aside_for_everyone_is_not_each_replicas_fix(pool: crate::DbPool) {
    let world = world(pool).await;
    sqlx::query(
        "INSERT INTO sync.notes (kind, relation, row_key, partition_id) \
         VALUES ('row', 'items', 'not-a-key', $1)",
    )
    .bind(world.partition)
    .execute(&world.pool)
    .await
    .unwrap();
    world.pass(&Fake::default()).await;

    let overview = world.overview(Scope::All).await;
    assert_eq!(overview.health.parked.total, 1);
    assert!(!overview.health.healthy);
    for replica in replicas_of(&overview) {
        assert_eq!(
            (replica.state, replica.parked),
            (ReplicaState::CatchingUp, 1),
            "{replica:?}"
        );
    }
}

/// A row sent and not yet confirmed that a replica should no longer hold is
/// leaving it, as a confirmed one is.
#[sqlx::test(migrations = false)]
async fn a_row_sent_and_no_longer_owed_is_leaving(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, _] = world.replicas;
    let one = world.item("one").await;
    world.pass(&Fake::default()).await;
    world
        .exec(
            "UPDATE fixture.items SET retired_at = now() WHERE id = $1 AND partition_id = $2 AND $3 <> ''",
            one,
            "x",
        )
        .await;

    let mut conn = world.pool.acquire().await.unwrap();
    let found = world
        .engine
        .row_progress(&mut conn, Scope::Replicas(&[a]), "items", &one.to_string())
        .await
        .unwrap();
    assert_eq!(states_of(found), vec![(a, RowState::Leaving)]);
}

/// What the engine stored and cannot read back is an error, never a replica
/// called synced.
#[sqlx::test(migrations = false)]
async fn a_backlog_that_does_not_read_fails_the_read(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, _] = world.replicas;
    sqlx::query(
        "INSERT INTO sync.replicas (replica_id, backlog, backlog_at) \
         VALUES ($1, '{\"not\": \"a backlog\"}', now())",
    )
    .bind(a)
    .execute(&world.pool)
    .await
    .unwrap();

    let mut conn = world.pool.acquire().await.unwrap();
    let read = world
        .engine
        .overview(&mut conn, Scope::Replicas(&[a]), 10)
        .await;
    assert!(read.is_err(), "{read:?}");
}

fn replicas_of(overview: &crate::status::Overview) -> &[crate::status::ReplicaOverview] {
    &overview.replicas.first
}

fn states_of(row: crate::status::RowProgress) -> Vec<(Uuid, RowState)> {
    row.places
        .into_iter()
        .map(|p| (p.replica, p.state))
        .collect()
}

/// An overview's size does not grow with the fleet: every replica is
/// counted in its state, the worst are listed up to the limit, and the
/// listing pages through all of them, each once, worst first.
#[sqlx::test(migrations = false)]
async fn a_fleet_is_counted_whole_and_listed_a_page_at_a_time(pool: crate::DbPool) {
    let world = world(pool).await;
    let [a, _] = world.replicas;
    sqlx::query(
        "INSERT INTO fixture.t_replicas \
         SELECT gen_random_uuid(), $1, true FROM generate_series(1, 300)",
    )
    .bind(world.partition)
    .execute(&world.pool)
    .await
    .unwrap();
    let wire = Fake::default();
    wire.failing.lock().unwrap().insert(a, true);
    world.item("one").await;
    world.pass(&wire).await;

    let overview = world.overview(Scope::All).await;
    let states = overview.states;
    assert_eq!(
        (states.unreachable, states.catching_up, states.synced),
        (1, 301, 0),
        "{states:?}"
    );
    assert_eq!(overview.replicas.total, 302);
    assert_eq!(overview.replicas.first.len(), 10, "cut at the limit");
    assert_eq!(overview.replicas.first[0].replica, a, "the worst first");

    let mut conn = world.pool.acquire().await.unwrap();
    let mut seen = Vec::new();
    for page in 0..4 {
        let listing = world
            .engine
            .listing(&mut conn, Scope::All, None, page * 100, 100)
            .await
            .unwrap();
        assert_eq!(listing.replicas.total, 302);
        seen.extend(
            listing
                .replicas
                .first
                .into_iter()
                .map(|r| (r.state, r.replica)),
        );
    }
    assert_eq!(seen.len(), 302);
    assert!(seen.is_sorted(), "worst first, then by id: {seen:?}");
    let catching_up = world
        .engine
        .listing(
            &mut conn,
            Scope::All,
            Some(ReplicaState::CatchingUp),
            300,
            100,
        )
        .await
        .unwrap();
    assert_eq!(
        (catching_up.replicas.total, catching_up.replicas.first.len()),
        (301, 1)
    );
}

/// Every figure of an answer comes from one snapshot: a read opens its own,
/// and refuses to run in a transaction that would see each statement's own.
#[sqlx::test(migrations = false)]
async fn a_status_read_is_one_snapshot(pool: crate::DbPool) {
    let world = world(pool).await;
    let mut tx = world.pool.begin().await.unwrap();
    let read = world.engine.overview(&mut tx, Scope::All, 10).await;
    assert!(matches!(read, Err(Error::Misuse(_))), "{read:?}");
    tx.rollback().await.unwrap();

    let mut tx = world
        .pool
        .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .unwrap();
    world
        .engine
        .overview(&mut tx, Scope::All, 10)
        .await
        .unwrap();
    // The caller's transaction carries on after the read.
    sqlx::query("SELECT 1").execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();

    let mut conn = world.pool.acquire().await.unwrap();
    world
        .engine
        .overview(&mut conn, Scope::All, 10)
        .await
        .unwrap();
}
