//! The server as a host runs it: written rows reach the replicas without a
//! line of loop in the test, uploads are filed and answered, one process at a
//! time runs the relay, and a restart lets through what was set aside.

use super::*;
use crate::envelope::{self, Body, Table as WireTable, ToReplica};
use crate::testkit::records::{reading_from_wire, reading_to_wire, Reading, ReadingRow};
use crate::testkit::{from_wire, to_wire, Item, ItemRow, BUGGY};
use crate::wire::MapError;
use prost::Message as _;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

const SCHEMA: &str = "
CREATE SCHEMA fixture;
CREATE TABLE fixture.t_replicas (id UUID PRIMARY KEY, partition_id UUID NOT NULL, live BOOLEAN NOT NULL);
CREATE TABLE fixture.items (
    id UUID PRIMARY KEY, partition_id UUID NOT NULL, name TEXT NOT NULL, retired_at TIMESTAMPTZ);
CREATE VIEW items AS SELECT * FROM fixture.items WHERE retired_at IS NULL;
CREATE TABLE fixture.readings (id UUID PRIMARY KEY, replica_id UUID NOT NULL, value TEXT NOT NULL);
CREATE TABLE fixture.logs (replica_id UUID NOT NULL, id UUID NOT NULL, value TEXT NOT NULL,
    PRIMARY KEY (replica_id, id));
CREATE TABLE fixture.log_refs (replica_id UUID NOT NULL, log_id UUID NOT NULL,
    FOREIGN KEY (replica_id, log_id) REFERENCES fixture.logs (replica_id, id));
";

const REPLICAS: Replicas = Replicas {
    relation: "fixture.t_replicas",
    table: "t_replicas",
    live: "live",
    partition: Some("partition_id"),
};

/// What every replica was sent, in order: each message once, by its id, as a
/// transport that deduplicates delivers it — a relay stopped between a send
/// and the trim of its outbox sends that message again.
#[derive(Clone, Default)]
struct Wire(Arc<Mutex<Vec<(Uuid, Body)>>>, Arc<Mutex<BTreeSet<String>>>);

impl Transport for Wire {
    async fn send(&self, replica: Uuid, message: Vec<u8>, id: &str) -> Result<(), Error> {
        let message = ToReplica::decode(message.as_slice()).expect("a message");
        if self.1.lock().unwrap().insert(id.to_owned()) {
            self.0
                .lock()
                .unwrap()
                .push((replica, message.body.expect("a body")));
        }
        Ok(())
    }
}

impl Wire {
    /// The item names `replica` was sent, as often as it was sent each.
    fn items(&self, replica: Uuid) -> Vec<String> {
        let mut names = Vec::new();
        for (to, body) in self.0.lock().unwrap().iter() {
            let Body::Down(down) = body else { continue };
            if *to != replica {
                continue;
            }
            for table in &down.tables {
                for bytes in &table.upserts {
                    names.push(ItemRow::decode(bytes.as_slice()).expect("an item").name);
                }
            }
        }
        names
    }

    fn filed(&self, replica: Uuid) -> Vec<u64> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(to, _)| *to == replica)
            .filter_map(|(_, body)| match body {
                Body::Filed(filed) => Some(filed.uploads.clone()),
                _ => None,
            })
            .flatten()
            .collect()
    }
}

/// A record each replica keeps by its own key, and deletes: the server
/// lets go of it with it, unless a row of the server's still points at it.
#[derive(sqlx::FromRow, Clone, Debug, crate::Row)]
#[row(
    table = "logs",
    schema = "fixture",
    mirror = "record",
    key = "replica_id, id",
    origin = "replica_id"
)]
struct Log {
    replica_id: Uuid,
    id: Uuid,
    value: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct LogRow {
    #[prost(string, tag = "1")]
    replica_id: String,
    #[prost(string, tag = "2")]
    id: String,
    #[prost(string, tag = "3")]
    value: String,
}

fn log_to_wire(log: Log) -> Result<LogRow, MapError> {
    Ok(LogRow {
        replica_id: log.replica_id.to_string(),
        id: log.id.to_string(),
        value: log.value,
    })
}

fn log_from_wire(row: LogRow) -> Result<Log, MapError> {
    Ok(Log {
        replica_id: row.replica_id.parse()?,
        id: row.id.parse()?,
        value: row.value,
    })
}

fn codecs() -> Vec<Codec<()>> {
    vec![
        Codec::new::<Item, ItemRow>(to_wire, from_wire, None),
        Codec::new::<Reading, ReadingRow>(reading_to_wire, reading_from_wire, None),
        Codec::new::<Log, LogRow>(log_to_wire, log_from_wire, None),
    ]
}

fn settings() -> Settings {
    Settings {
        idle: Duration::from_millis(10),
        standby: Duration::from_millis(10),
        ..Settings::default()
    }
}

/// The schema, a partition and two live replicas in it.
async fn world(pool: &PgPool) -> (Uuid, [Uuid; 2]) {
    let mut conn = pool.acquire().await.unwrap();
    crate::plan::tests::install(&mut conn).await;
    sqlx::raw_sql(SCHEMA).execute(&mut *conn).await.unwrap();
    let partition = Uuid::now_v7();
    let replicas = [Uuid::now_v7(), Uuid::now_v7()];
    for replica in replicas {
        sqlx::query("INSERT INTO fixture.t_replicas VALUES ($1, $2, true)")
            .bind(replica)
            .bind(partition)
            .execute(&mut *conn)
            .await
            .unwrap();
    }
    let codecs = codecs();
    let tables: Vec<&Table> = codecs.iter().map(|c| &c.table).collect();
    let mut tx = conn.begin().await.unwrap();
    Engine::install(&mut tx, &tables, REPLICAS).await.unwrap();
    tx.commit().await.unwrap();
    (partition, replicas)
}

async fn item(pool: &PgPool, partition: Uuid, name: &str) {
    sqlx::query("INSERT INTO fixture.items (id, partition_id, name) VALUES ($1, $2, $3)")
        .bind(Uuid::now_v7())
        .bind(partition)
        .bind(name)
        .execute(pool)
        .await
        .unwrap();
}

/// Runs `servers` side by side until `done` holds, or fails after five
/// seconds.
async fn until<S: Senders, T: Transport + Send + 'static, F: Future<Output = bool>>(
    servers: &[&Server<S, T>],
    done: impl Fn() -> F,
) {
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let stopping = || {
        let mut stopped = stopped.clone();
        async move {
            let _ = stopped.wait_for(|stop| *stop).await;
        }
    };
    let runs = async {
        match servers {
            [one] => one.run(stopping()).await,
            [one, two] => {
                tokio::join!(one.run(stopping()), two.run(stopping()));
            }
            _ => unreachable!("one or two servers"),
        }
    };
    let check = async {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !done().await {
            assert!(Instant::now() < deadline, "not done within five seconds");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        stop.send(true).unwrap();
    };
    tokio::join!(runs, check);
}

#[sqlx::test(migrations = false)]
async fn what_is_written_reaches_the_replicas_and_what_comes_up_is_filed_and_answered(
    pool: PgPool,
) {
    let (partition, [a, b]) = world(&pool).await;
    let wire = Wire::default();
    let server = Server::new(
        pool.clone(),
        codecs(),
        REPLICAS,
        wire.clone(),
        Anyone::new(pool.clone()),
        settings(),
    )
    .await
    .expect("the server");

    item(&pool, partition, "one").await;
    until(&[&server], || async {
        wire.items(a) == ["one"] && wire.items(b) == ["one"]
    })
    .await;

    let upload = Up {
        tables: vec![WireTable {
            table: "readings".to_owned(),
            upserts: vec![ReadingRow {
                id: Uuid::now_v7().to_string(),
                replica_id: a.to_string(),
                value: "seven".to_owned(),
            }
            .encode_to_vec()],
            ..WireTable::default()
        }],
        uploads: vec![3],
        protocol: envelope::PROTOCOL,
        ..Up::default()
    };
    let filed = server.receive(a, &upload.encode_to_vec()).await.unwrap();
    assert_eq!(filed.len(), 1);
    assert_eq!(wire.filed(a), vec![3], "answered once filed");
    let stored: i64 = sqlx::query_scalar("SELECT count(*) FROM fixture.readings")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(stored, 1);

    let later = Up {
        protocol: envelope::PROTOCOL + 1,
        ..upload
    };
    let refused = server.receive(a, &later.encode_to_vec()).await;
    assert!(
        matches!(refused, Err(Error::Protocol { .. })),
        "a later protocol comes again once this build speaks it"
    );
}

/// Two processes run the server; one of them runs the relay, so nothing is
/// planned or sent twice.
#[sqlx::test(migrations = false)]
async fn one_process_at_a_time_runs_the_relay(pool: PgPool) {
    let (partition, [a, b]) = world(&pool).await;
    let wire = Wire::default();
    let mut servers = Vec::new();
    for _ in 0..2 {
        servers.push(
            Server::new(
                pool.clone(),
                codecs(),
                REPLICAS,
                wire.clone(),
                Anyone::new(pool.clone()),
                settings(),
            )
            .await
            .unwrap(),
        );
    }
    for n in 0..20 {
        item(&pool, partition, &format!("item {n}")).await;
    }
    until(&[&servers[0], &servers[1]], || async {
        wire.items(a).len() >= 20 && wire.items(b).len() >= 20
    })
    .await;
    assert_eq!(wire.items(a).len(), 20, "each sent once");
    assert_eq!(wire.items(b).len(), 20, "each sent once");
}

/// A row the build cannot encode waits in quarantine; the next process to run
/// the relay — after a deploy — sends it, with nobody touching the data.
#[sqlx::test(migrations = false)]
async fn what_was_set_aside_goes_once_a_fixed_build_runs_the_relay(pool: PgPool) {
    let (partition, [a, _]) = world(&pool).await;
    let wire = Wire::default();
    let server = || {
        Server::new(
            pool.clone(),
            codecs(),
            REPLICAS,
            wire.clone(),
            Anyone::new(pool.clone()),
            settings(),
        )
    };
    item(&pool, partition, "fine").await;
    item(&pool, partition, "poison").await;
    let quarantined = || async {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM sync.quarantine")
            .fetch_one(&pool)
            .await
            .unwrap()
    };

    let before = server().await.unwrap();
    until(&[&before], || async {
        quarantined().await == 2 && wire.items(a) == ["fine"]
    })
    .await;
    assert_eq!(wire.items(a), ["fine"]);
    drop(before);

    BUGGY.set(false);
    let fixed = server().await.unwrap();
    // Recorded and sent, to both replicas.
    let count = |sql: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>(sql)
                .fetch_one(&pool)
                .await
                .unwrap()
        }
    };
    until(&[&fixed], || async {
        count("SELECT count(*) FROM sync.held").await == 4
            && count("SELECT count(*) FROM sync.outbox").await == 0
    })
    .await;
    BUGGY.set(true);
    assert_eq!(wire.items(a), ["fine", "poison"]);
    // Out of the quarantine once each replica says it took it.
    let sent: Vec<(Uuid, i64)> =
        sqlx::query_as("SELECT replica_id, last_sequence FROM sync.chains")
            .fetch_all(&pool)
            .await
            .unwrap();
    for (replica, last) in sent {
        let ack = crate::testkit::confirmation(&pool, replica, last as u64).await;
        fixed.receive(replica, &ack.encode_to_vec()).await.unwrap();
    }
    assert_eq!(quarantined().await, 0);
}

/// A relay whose lock's connection is gone cannot record what it sent: the
/// record runs on that connection, so it dies with the lock, and the notes
/// wait for whoever runs the relay next.
#[sqlx::test(migrations = false)]
async fn a_relay_that_lost_its_lock_records_nothing(pool: PgPool) {
    let (partition, _) = world(&pool).await;
    let server = Server::new(
        pool.clone(),
        codecs(),
        REPLICAS,
        Wire::default(),
        Anyone::new(pool.clone()),
        settings(),
    )
    .await
    .unwrap();
    item(&pool, partition, "one").await;
    let mut lock = Lock::take(&pool, LOCK_KEY)
        .await
        .unwrap()
        .expect("the lock");
    sqlx::query(
        "SELECT pg_terminate_backend(pid) FROM pg_locks \
          WHERE locktype = 'advisory' \
            AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
    )
    .execute(&pool)
    .await
    .unwrap();

    assert!(server.pass(&mut lock).await.is_err());
    let count = |sql: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>(sql)
                .fetch_one(&pool)
                .await
                .unwrap()
        }
    };
    assert_eq!(count("SELECT count(*) FROM sync.held").await, 0);
    assert!(count("SELECT count(*) FROM sync.notes WHERE done_at IS NULL").await > 0);
    assert_eq!(
        server.pass_now().await.unwrap(),
        Some(7),
        "the next relay plans the three notes, provisioning the two replicas it sees for the \
         first time, and sends each its snapshot's two messages"
    );
}

/// A database with none of the engine gets its schema and every mirrored
/// table's triggers; rows written before then reach every replica, by a
/// provisioning.
#[sqlx::test(migrations = false)]
async fn install_sets_up_a_database_that_has_none_of_the_engine(pool: PgPool) {
    sqlx::raw_sql("DROP SCHEMA IF EXISTS sync CASCADE")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::raw_sql(
        "CREATE SCHEMA fixture;
         CREATE TABLE fixture.t_replicas (id UUID PRIMARY KEY, partition_id UUID NOT NULL, live BOOLEAN NOT NULL);
         CREATE TABLE fixture.items (
             id UUID PRIMARY KEY, partition_id UUID NOT NULL, name TEXT NOT NULL, retired_at TIMESTAMPTZ);
         CREATE VIEW items AS SELECT * FROM fixture.items WHERE retired_at IS NULL;
         CREATE TABLE fixture.readings (id UUID PRIMARY KEY, replica_id UUID NOT NULL, value TEXT NOT NULL);
         CREATE TABLE fixture.logs (replica_id UUID NOT NULL, id UUID NOT NULL, value TEXT NOT NULL,
             PRIMARY KEY (replica_id, id));",
    )
    .execute(&pool)
    .await
    .unwrap();
    let partition = Uuid::now_v7();
    let replica = Uuid::now_v7();
    sqlx::query("INSERT INTO fixture.t_replicas VALUES ($1, $2, true)")
        .bind(replica)
        .bind(partition)
        .execute(&pool)
        .await
        .unwrap();
    item(&pool, partition, "before").await;
    let refused = Server::new(
        pool.clone(),
        codecs(),
        REPLICAS,
        Wire::default(),
        Anyone::new(pool.clone()),
        settings(),
    )
    .await;
    assert!(refused.is_err(), "no schema: refused");

    let wire = Wire::default();
    let server = Server::install(
        pool.clone(),
        codecs(),
        REPLICAS,
        wire.clone(),
        Anyone::new(pool.clone()),
        settings(),
    )
    .await
    .expect("installed");
    Server::install(
        pool.clone(),
        codecs(),
        REPLICAS,
        Wire::default(),
        Anyone::new(pool.clone()),
        settings(),
    )
    .await
    .expect("idempotent");
    item(&pool, partition, "after").await;

    until(&[&server], || async {
        let mut names = wire.items(replica);
        names.sort();
        names.dedup();
        names == ["after", "before"]
    })
    .await;
}

/// Files as [`Anyone`] does, except for the replicas told to fail: the
/// host's own bug, which no number of resends cures.
#[derive(Clone)]
struct Broken {
    pool: PgPool,
    failing: Arc<Mutex<std::collections::BTreeSet<Uuid>>>,
}

impl Senders for Broken {
    type Known = ();

    async fn known(&self, replica: Uuid) -> Result<Option<()>, Error> {
        if self.failing.lock().unwrap().contains(&replica) {
            return Err(Error::Host {
                reason: format!("{replica}: a bug in the host"),
                transient: false,
            });
        }
        Ok(Some(()))
    }

    async fn filing(&self, _known: &()) -> Result<Transaction<'static, Postgres>, Error> {
        Ok(self.pool.begin().await?)
    }
}

fn reading(replica: Uuid, value: &str) -> Vec<u8> {
    ReadingRow {
        id: Uuid::now_v7().to_string(),
        replica_id: replica.to_string(),
        value: value.to_owned(),
    }
    .encode_to_vec()
}

/// An upload of `rows` of `table`, readings each, with their keys.
fn upload(number: u64, table: &str, rows: Vec<Vec<u8>>) -> Vec<u8> {
    let keys = rows
        .iter()
        .map(|row| envelope::Key {
            parts: vec![ReadingRow::decode(row.as_slice()).expect("a reading").id],
        })
        .collect();
    envelope::Up {
        tables: vec![WireTable {
            table: table.to_owned(),
            upserts: rows,
            keys,
            ..WireTable::default()
        }],
        uploads: vec![number],
        protocol: envelope::PROTOCOL,
        ..envelope::Up::default()
    }
    .encode_to_vec()
}

/// What the relay does with the inbox once it is due: every page of it
/// filed again. A failure set aside is no error.
async fn refile<S: Senders, T: Transport>(server: &Server<S, T>, lock: &mut Lock) -> usize {
    let mut walk = server.retry(lock).await.unwrap();
    let mut pages = 0;
    while let Some(next) = server.refile_page(walk).await.unwrap() {
        walk = next;
        pages += 1;
    }
    pages
}

async fn count(pool: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(pool).await.unwrap()
}

/// An upload whose filing fails for a reason no resend cures is kept whole,
/// answered so the replica stops sending it, and filed after a fix; an
/// upload that fails again holds back no other. What it says of its sender
/// is filed all the same: what it applied is recorded.
#[sqlx::test(migrations = false)]
async fn an_upload_the_server_cannot_file_at_all_is_kept_whole_and_answered(pool: PgPool) {
    let (partition, [a, b]) = world(&pool).await;
    item(&pool, partition, "sent").await;
    let wire = Wire::default();
    let senders = Broken {
        pool: pool.clone(),
        failing: Arc::new(Mutex::new([a, b].into())),
    };
    let server = Server::new(
        pool.clone(),
        codecs(),
        REPLICAS,
        wire.clone(),
        senders.clone(),
        settings(),
    )
    .await
    .unwrap();
    while server.pass_now().await.unwrap().unwrap_or_default() > 0 {}

    let applied = |replica: Uuid| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, Option<i64>>(
                "SELECT applied_sequence FROM sync.replicas WHERE replica_id = $1",
            )
            .bind(replica)
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    for (replica, number) in [(a, 7), (b, 8)] {
        let last: i64 =
            sqlx::query_scalar("SELECT last_sequence FROM sync.chains WHERE replica_id = $1")
                .bind(replica)
                .fetch_one(&pool)
                .await
                .unwrap();
        let mut up = envelope::Up::decode(
            upload(number, "readings", vec![reading(replica, "kept")]).as_slice(),
        )
        .unwrap();
        let said = crate::testkit::confirmation(&pool, replica, last as u64).await;
        (up.epoch, up.sequence, up.nonce, up.mac) =
            (said.epoch, said.sequence, said.nonce, said.mac);
        let filed = server
            .receive(replica, &up.encode_to_vec())
            .await
            .expect("no reason to send it again");
        assert!(filed.is_empty());
        assert_eq!(wire.filed(replica), [number], "answered");
        assert_eq!(
            applied(replica).await,
            Some(last),
            "what it applied, recorded"
        );
    }
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM sync.inbox WHERE relation = 'readings'"
        )
        .await,
        2
    );
    assert_eq!(
        count(&pool, "SELECT count(*) FROM fixture.readings").await,
        0
    );

    // b's sender is fixed, a's is not: a fails again, and b is filed.
    senders.failing.lock().unwrap().remove(&b);
    let mut lock = Lock::take(&pool, LOCK_KEY).await.unwrap().unwrap();
    refile(&server, &mut lock).await;
    assert_eq!(
        count(&pool, "SELECT count(*) FROM fixture.readings").await,
        1
    );
    let left: (Uuid, i32) = sqlx::query_as("SELECT replica_id, attempts FROM sync.inbox")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(left, (a, 2));

    senders.failing.lock().unwrap().clear();
    refile(&server, &mut lock).await;
    assert_eq!(
        count(&pool, "SELECT count(*) FROM fixture.readings").await,
        2
    );
    assert_eq!(count(&pool, "SELECT count(*) FROM sync.inbox").await, 0);
}

/// A delete the server's database refuses — a row of its own still points at
/// the replica's — is kept for a fix, and the rest of the upload is filed.
#[sqlx::test(migrations = false)]
async fn a_delete_the_server_refuses_is_kept_and_the_rest_filed(pool: PgPool) {
    let (_, [a, _]) = world(&pool).await;
    let wire = Wire::default();
    let server = Server::new(
        pool.clone(),
        codecs(),
        REPLICAS,
        wire.clone(),
        Anyone::new(pool.clone()),
        settings(),
    )
    .await
    .unwrap();
    let (pointed, free) = (Uuid::now_v7(), Uuid::now_v7());
    for id in [pointed, free] {
        sqlx::query("INSERT INTO fixture.logs VALUES ($1, $2, 'x')")
            .bind(a)
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
    }
    sqlx::query("INSERT INTO fixture.log_refs VALUES ($1, $2)")
        .bind(a)
        .bind(pointed)
        .execute(&pool)
        .await
        .unwrap();
    let key = |id: Uuid| envelope::Key {
        parts: vec![a.to_string(), id.to_string()],
    };
    let up = envelope::Up {
        tables: vec![
            WireTable {
                table: "logs".to_owned(),
                deletes: vec![key(pointed), key(free)],
                ..WireTable::default()
            },
            WireTable {
                table: "readings".to_owned(),
                upserts: vec![reading(a, "fine")],
                ..WireTable::default()
            },
        ],
        uploads: vec![3],
        protocol: envelope::PROTOCOL,
        ..envelope::Up::default()
    };

    server.receive(a, &up.encode_to_vec()).await.unwrap();

    assert_eq!(
        count(&pool, "SELECT count(*) FROM fixture.readings").await,
        1
    );
    assert_eq!(count(&pool, "SELECT count(*) FROM fixture.logs").await, 1);
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM sync.inbox WHERE relation = 'logs'"
        )
        .await,
        1
    );
    assert_eq!(wire.filed(a), [3]);
}

/// A replica that leaves what it was sent unconfirmed while nothing new goes
/// to it is provisioned again by the running server: the transport may have
/// lost it.
#[sqlx::test(migrations = false)]
async fn the_running_server_provisions_again_a_replica_silent_on_what_it_was_sent(pool: PgPool) {
    let (partition, [a, _]) = world(&pool).await;
    let wire = Wire::default();
    let server = Server::new(
        pool.clone(),
        codecs(),
        REPLICAS,
        wire.clone(),
        Anyone::new(pool.clone()),
        Settings {
            watch_every: Duration::from_millis(20),
            unconfirmed_after: Duration::from_millis(100),
            ..settings()
        },
    )
    .await
    .unwrap();
    item(&pool, partition, "lost on the way").await;
    until(&[&server], || async {
        wire.0
            .lock()
            .unwrap()
            .iter()
            .any(|(to, body)| *to == a && matches!(body, Body::EndSnapshot(_)))
    })
    .await;
}

/// Whatever a replica sends up — bytes that are no message, keys that are
/// none, sequences no message has, tables nobody mirrors, text with NUL in
/// it, from replicas it holds rows for or one it never heard of, with the
/// relay running in between — the server answers `Ok` or an error worth a
/// resend, answers an upload that names no protocol with nothing, and what
/// it keeps for the replica stays bounded by what it sent it: quarantined
/// rows are rows of a table it sends, no note but one provisioning per
/// replica, an inbox no larger than the rows and deletes that came, nothing
/// kept whole, and no acknowledgement past the last message sent.
#[sqlx::test(migrations = false)]
async fn whatever_a_replica_sends_up_the_server_never_stops_on_it(pool: PgPool) {
    use crate::envelope::{Backlog, Digest, Key, Rejected, TableBacklog, Up};
    let (partition, [a, b]) = world(&pool).await;
    let wire = Wire::default();
    let server = Server::new(
        pool.clone(),
        codecs(),
        REPLICAS,
        wire.clone(),
        Anyone::new(pool.clone()),
        settings(),
    )
    .await
    .unwrap();
    for n in 0..4 {
        item(&pool, partition, &format!("item {n}")).await;
    }
    item(&pool, partition, "poison").await;
    while server.pass_now().await.unwrap().unwrap_or_default() > 0 {}
    let items: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM fixture.items")
        .fetch_all(&pool)
        .await
        .unwrap();
    let epoch: Uuid = sqlx::query_scalar("SELECT epoch FROM sync.epoch")
        .fetch_one(&pool)
        .await
        .unwrap();
    let last: i64 = sqlx::query_scalar("SELECT max(last_sequence) FROM sync.chains")
        .fetch_one(&pool)
        .await
        .unwrap();

    let mut walk = crate::testkit::Walk(0x0BAD_5EED);
    let pick =
        |walk: &mut crate::testkit::Walk, from: &[String]| from[walk.below(from.len())].clone();
    let mut parts: Vec<String> = items.iter().map(Uuid::to_string).collect();
    parts.extend(items.iter().map(|i| i.to_string().to_uppercase()));
    parts.extend(items.iter().map(|i| i.simple().to_string()));
    parts.extend(
        [
            a,
            b,
            Uuid::now_v7(),
            Uuid::now_v7(),
            Uuid::now_v7(),
            Uuid::now_v7(),
        ]
        .iter()
        .map(Uuid::to_string)
        .chain(
            [
                "",
                "not-a-key",
                "a\0b",
                "1",
                "-1",
                "99999999999999999999",
                "x|y",
            ]
            .map(str::to_owned),
        ),
    );
    // Text past what a btree entry takes, that does not compress: what a
    // sender may say is bounded only by the message.
    let long = |walk: &mut crate::testkit::Walk, len: usize| -> String {
        (0..len / 16)
            .map(|_| format!("{:016x}", walk.next()))
            .collect()
    };
    parts.extend([
        long(&mut walk, 3200),
        format!("{}{}", items[0], long(&mut walk, 3200)),
    ]);
    let mut tables: Vec<String> = ["items", "readings", "logs", "nope", "", "it\0ems"]
        .map(str::to_owned)
        .to_vec();
    tables.extend([long(&mut walk, 3200), long(&mut walk, 6400)]);
    let epochs = [
        epoch.to_string(),
        epoch.to_string().to_uppercase(),
        Uuid::now_v7().to_string(),
        "junk\0".to_owned(),
        String::new(),
    ];
    let sequences = [
        0,
        1,
        last as u64,
        last as u64 + 1,
        i64::MAX as u64,
        i64::MAX as u64 + 1,
        u64::MAX,
    ];
    let mut came = 0i64;
    let stranger = Uuid::now_v7();
    for step in 0..300 {
        let sender = [a, b, stranger][walk.below(3)];
        if step % 20 == 19 {
            server.pass_now().await.unwrap();
        }
        let answers = wire.0.lock().unwrap().len();
        let bytes = if step % 10 == 0 {
            (0..walk.below(64)).map(|_| walk.below(256) as u8).collect()
        } else {
            let key = |walk: &mut crate::testkit::Walk| Key {
                parts: (0..[1, 1, 0, 2][walk.below(4)])
                    .map(|_| pick(walk, &parts))
                    .collect(),
            };
            let row = |walk: &mut crate::testkit::Walk| -> Vec<u8> {
                match walk.below(5) {
                    0 => reading(sender, "fine"),
                    1 => reading(sender, "poison"),
                    2 => reading(sender, "nul\0"),
                    3 => ItemRow {
                        id: Uuid::now_v7().to_string(),
                        partition_id: partition.to_string(),
                        name: "from below".into(),
                    }
                    .encode_to_vec(),
                    _ => (0..walk.below(16)).map(|_| walk.below(256) as u8).collect(),
                }
            };
            let up = Up {
                tables: (0..walk.below(3))
                    .map(|_| WireTable {
                        table: pick(&mut walk, &tables),
                        upserts: (0..walk.below(3)).map(|_| row(&mut walk)).collect(),
                        deletes: (0..walk.below(3)).map(|_| key(&mut walk)).collect(),
                        keys: (0..walk.below(3)).map(|_| key(&mut walk)).collect(),
                    })
                    .collect(),
                rejected: (0..walk.below(3))
                    .map(|_| Rejected {
                        table: pick(&mut walk, &tables),
                        key: (walk.below(4) > 0).then(|| key(&mut walk)),
                        reason: "refused\0".into(),
                        epoch: epochs[walk.below(epochs.len())].clone(),
                        sequence: sequences[walk.below(sequences.len())],
                    })
                    .collect(),
                digests: (0..walk.below(3))
                    .map(|_| Digest {
                        table: pick(&mut walk, &tables),
                        count: [0, 1, u64::MAX][walk.below(3)],
                        fingerprint: walk.next(),
                    })
                    .collect(),
                epoch: epochs[walk.below(epochs.len())].clone(),
                sequence: sequences[walk.below(sequences.len())],
                uploads: (0..walk.below(3)).map(|_| walk.next()).collect(),
                protocol: [0, 1, 1, 1, 2, u32::MAX][walk.below(6)],
                backlog: (walk.below(3) == 0).then(|| Backlog {
                    tables: vec![TableBacklog {
                        table: pick(&mut walk, &tables),
                        unsent: [0, 5, u64::MAX][walk.below(3)],
                        unfiled: 1,
                        set_aside: 0,
                    }],
                }),
                nonce: [0, 1, walk.next(), u64::MAX][walk.below(4)],
                mac: (0..walk.below(20)).map(|_| walk.below(256) as u8).collect(),
                gap: [0, 0, 1, walk.next(), u64::MAX][walk.below(5)],
            };
            came += up
                .tables
                .iter()
                .map(|t| (t.upserts.len() + t.deletes.len()) as i64)
                .sum::<i64>();
            up.encode_to_vec()
        };

        match server.receive(sender, &bytes).await {
            Ok(_) => {}
            Err(e) => assert!(
                matches!(e, Error::Protocol { .. }),
                "step {step}: an error that is no reason to resend: {e}"
            ),
        }
        if Up::decode(bytes.as_slice()).is_ok_and(|up| up.protocol == 0) {
            assert_eq!(
                wire.0.lock().unwrap().len(),
                answers,
                "step {step}: an upload that names no protocol is answered"
            );
        }

        let quarantined: Vec<(Uuid, String, String)> =
            sqlx::query_as("SELECT replica_id, relation, row_key FROM sync.quarantine")
                .fetch_all(&pool)
                .await
                .unwrap();
        for (replica, table, key) in quarantined {
            assert!(
                [a, b, stranger].contains(&replica)
                    && table == "items"
                    && Uuid::parse_str(&key).is_ok_and(|k| k.to_string() == key),
                "step {step}: quarantined what is no row it is sent: {replica} {table} {key:?}"
            );
        }
        assert_eq!(
            count(
                &pool,
                "SELECT count(*) FROM sync.notes WHERE done_at IS NULL AND kind <> 'resync'"
            )
            .await,
            0,
            "step {step}"
        );
        assert!(
            count(&pool, "SELECT coalesce(max(n), 0) FROM (SELECT count(*) AS n FROM sync.notes WHERE done_at IS NULL GROUP BY replica_id) c").await <= 1,
            "step {step}: one provisioning per replica at most"
        );
        assert_eq!(
            count(
                &pool,
                "SELECT count(*) FROM sync.inbox WHERE reason LIKE 'filing its upload failed%'"
            )
            .await,
            0,
            "step {step}: an upload kept whole: filing failed on what it should set aside"
        );
        assert!(
            count(&pool, "SELECT count(*) FROM sync.inbox").await <= came,
            "step {step}: the inbox holds more than came"
        );
        assert_eq!(
            count(
                &pool,
                "SELECT count(*) FROM sync.inbox WHERE relation IN ('items', 'readings', 'logs') \
                   AND row_key !~ '^[0-9a-f-]{36}(\\|[0-9a-f-]{36})?$'"
            )
            .await,
            0,
            "step {step}: kept a key that is not its mirrored table's"
        );
        assert_eq!(
            count(
                &pool,
                "SELECT count(*) FROM sync.replicas r LEFT JOIN sync.chains c USING (replica_id) \
                  WHERE r.applied_sequence > c.last_sequence \
                     OR (r.applied_sequence IS NOT NULL AND c.last_sequence IS NULL)"
            )
            .await,
            0,
            "step {step}: acknowledged past what was sent"
        );
    }
}

/// A host spawns what the server runs, and answers from handlers whose
/// futures must be `Send`: every future a host gets from the server is.
#[sqlx::test(migrations = false)]
async fn what_a_host_spawns_can_be_spawned(pool: PgPool) {
    fn spawnable<T: Send>(_: T) {}
    world(&pool).await;
    let server = Server::new(
        pool.clone(),
        codecs(),
        REPLICAS,
        Wire::default(),
        Anyone::new(pool.clone()),
        settings(),
    )
    .await
    .expect("the server");
    spawnable(server.run(std::future::pending()));
    spawnable(server.receive(Uuid::nil(), &[]));
    spawnable(server.pass_now());
    spawnable(server.begin());
    spawnable(Server::new(
        pool.clone(),
        codecs(),
        REPLICAS,
        Wire::default(),
        Anyone::new(pool.clone()),
        settings(),
    ));
    spawnable(Server::install(
        pool.clone(),
        codecs(),
        REPLICAS,
        Wire::default(),
        Anyone::new(pool.clone()),
        settings(),
    ));
    let mut conn = pool.acquire().await.unwrap();
    spawnable(crate::plan::note_resync(&mut conn, None, Uuid::nil()));

    spawnable(Engine::triggers(&mut conn, &codecs(), REPLICAS));
    spawnable(crate::row::upsert::<Item>(&mut conn, Vec::new()));
    spawnable(crate::row::get_by_keys::<Item>(&mut conn, &[]));
    let engine = server.engine();
    let scope = crate::status::Scope::All;
    spawnable(engine.overview(&mut conn, scope, 1));
    spawnable(engine.listing(&mut conn, scope, None, crate::status::Page::new(0, 1)));
    spawnable(engine.health(&mut conn, scope, 1));
    spawnable(engine.progress(&mut conn, scope));
    spawnable(engine.row_progress(&mut conn, scope, "items", ""));
}

/// Settings a server cannot run with are refused when it is built: a wait
/// of nothing, which would spin, or give up on what it waits for at once.
#[sqlx::test(migrations = false)]
async fn a_server_refuses_settings_it_cannot_run_with(pool: PgPool) {
    world(&pool).await;
    /// A setting made zero.
    type Zeroed = (&'static str, fn(&mut Settings));
    let cases: [Zeroed; 8] = [
        ("idle", |s| s.idle = Duration::ZERO),
        ("send_timeout", |s| s.send_timeout = Duration::ZERO),
        ("unconfirmed_after", |s| {
            s.unconfirmed_after = Duration::ZERO
        }),
        ("standby", |s| s.standby = Duration::ZERO),
        ("retry_every", |s| s.retry_every = Duration::ZERO),
        ("prune_after", |s| s.prune_after = Duration::ZERO),
        ("watch_every", |s| s.watch_every = Duration::ZERO),
        ("stuck_after", |s| s.stuck_after = Duration::ZERO),
    ];
    for (what, set) in cases {
        let mut wrong = settings();
        set(&mut wrong);
        let refused = Server::new(
            pool.clone(),
            codecs(),
            REPLICAS,
            Wire::default(),
            Anyone::new(pool.clone()),
            wrong,
        )
        .await;
        match refused {
            Err(Error::Misuse(why)) => assert!(why.contains(what), "{what}: {why}"),
            Err(e) => panic!("{what}: refused for another reason: {e}"),
            Ok(_) => panic!("{what}: not refused"),
        }
    }
}

/// An upload beyond the server's limits — more bytes, or more rows and
/// rejections together than a message carries — or from a sender that is no replica
/// is dropped unanswered: nothing of it filed, nothing recorded of its
/// sender. One within them is filed.
#[sqlx::test(migrations = false)]
async fn an_upload_beyond_the_limits_or_from_no_replica_is_dropped_unanswered(pool: PgPool) {
    let (_, [a, _]) = world(&pool).await;
    let wire = Wire::default();
    let server = Server::new(
        pool.clone(),
        codecs(),
        REPLICAS,
        wire.clone(),
        Anyone::new(pool.clone()),
        Settings {
            limits: Limits::new(2, 8192).unwrap(),
            ..settings()
        },
    )
    .await
    .unwrap();
    let rejected = |n: usize| envelope::Up {
        rejected: (0..n)
            .map(|_| envelope::Rejected {
                table: "items".to_owned(),
                ..envelope::Rejected::default()
            })
            .collect(),
        uploads: vec![9],
        protocol: envelope::PROTOCOL,
        ..envelope::Up::default()
    };
    let stranger = Uuid::now_v7();
    for (sender, bytes) in [
        (
            a,
            upload(1, "readings", vec![reading(a, &"x".repeat(9000))]),
        ),
        (
            a,
            upload(
                2,
                "readings",
                (0..3).map(|_| reading(a, "one of three")).collect(),
            ),
        ),
        (a, rejected(3).encode_to_vec()),
        (a, {
            let mut up = envelope::Up::decode(
                upload(3, "readings", vec![reading(a, "one, and two rejections")]).as_slice(),
            )
            .unwrap();
            up.rejected = rejected(2).rejected;
            up.encode_to_vec()
        }),
        (
            stranger,
            upload(4, "readings", vec![reading(stranger, "a stranger's")]),
        ),
    ] {
        assert!(server.receive(sender, &bytes).await.unwrap().is_empty());
    }
    assert!(wire.filed(a).is_empty() && wire.filed(stranger).is_empty());
    assert_eq!(
        count(&pool, "SELECT count(*) FROM fixture.readings").await,
        0
    );
    assert_eq!(count(&pool, "SELECT count(*) FROM sync.inbox").await, 0);
    let recorded: i64 =
        sqlx::query_scalar("SELECT count(*) FROM sync.replicas WHERE replica_id = $1")
            .bind(stranger)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        recorded, 0,
        "nothing recorded of a sender that is no replica"
    );

    server
        .receive(
            a,
            &upload(5, "readings", (0..2).map(|_| reading(a, "two")).collect()),
        )
        .await
        .unwrap();
    assert_eq!(wire.filed(a), [5]);
    assert_eq!(
        count(&pool, "SELECT count(*) FROM fixture.readings").await,
        2
    );
}

/// A transport that never answers the server's answer to an upload: the
/// upload comes again, rather than holding its handler for ever.
#[sqlx::test(migrations = false)]
async fn an_answer_the_transport_never_takes_has_the_upload_come_again(pool: PgPool) {
    struct Silent;
    impl Transport for Silent {
        async fn send(&self, _: Uuid, _: Vec<u8>, _: &str) -> Result<(), Error> {
            std::future::pending().await
        }
    }
    let (_, [a, _]) = world(&pool).await;
    let server = Server::new(
        pool.clone(),
        codecs(),
        REPLICAS,
        Silent,
        Anyone::new(pool.clone()),
        Settings {
            send_timeout: Duration::from_millis(50),
            ..settings()
        },
    )
    .await
    .unwrap();
    let answered = server
        .receive(a, &upload(1, "readings", vec![reading(a, "kept")]))
        .await;
    assert!(
        matches!(&answered, Err(e) if e.is_transient()),
        "{answered:?}"
    );
    assert_eq!(
        count(&pool, "SELECT count(*) FROM fixture.readings").await,
        1
    );
}

/// A relation of replicas that names one twice — a view joining it to two
/// rows — holds no pass back: the replica is one, provisioned once.
#[sqlx::test(migrations = false)]
async fn a_replica_named_twice_by_its_relation_is_one(pool: PgPool) {
    let (partition, [a, b]) = world(&pool).await;
    sqlx::raw_sql(
        "CREATE VIEW fixture.twice AS SELECT * FROM fixture.t_replicas \
         UNION ALL SELECT * FROM fixture.t_replicas",
    )
    .execute(&pool)
    .await
    .unwrap();
    let wire = Wire::default();
    let server = Server::new(
        pool.clone(),
        codecs(),
        Replicas::relation("fixture.twice")
            .table("t_replicas")
            .live("live")
            .partitioned_by("partition_id"),
        wire.clone(),
        Anyone::new(pool.clone()),
        settings(),
    )
    .await
    .unwrap();
    item(&pool, partition, "one").await;
    while server.pass_now().await.unwrap().unwrap_or_default() > 0 {}
    assert_eq!(wire.items(a), ["one"]);
    assert_eq!(wire.items(b), ["one"]);
}

/// Senders the host knows as the test says: the rest are no one it knows.
#[derive(Clone)]
struct Known {
    pool: PgPool,
    known: Arc<Mutex<std::collections::BTreeSet<Uuid>>>,
}

impl Senders for Known {
    type Known = ();

    async fn known(&self, replica: Uuid) -> Result<Option<()>, Error> {
        Ok(self.known.lock().unwrap().contains(&replica).then_some(()))
    }

    async fn filing(&self, _known: &()) -> Result<Transaction<'static, Postgres>, Error> {
        Ok(self.pool.begin().await?)
    }
}

/// A replica whose uploads the server cannot file keeps at most so many of
/// them in the inbox: past that its uploads come again, waiting on it. The
/// inbox is filed again a page at a time, beside the passes; what a sender
/// the host no longer knows uploaded waits, and is dropped once it waited
/// `prune_after`.
#[sqlx::test(migrations = false)]
async fn the_inbox_is_bounded_per_replica_filed_a_page_at_a_time_and_pruned_of_strangers(
    pool: PgPool,
) {
    let (_, [a, b]) = world(&pool).await;
    let wire = Wire::default();
    let senders = Known {
        pool: pool.clone(),
        known: Arc::new(Mutex::new([a, b].into())),
    };
    let server = Server::new(
        pool.clone(),
        codecs(),
        REPLICAS,
        wire.clone(),
        senders.clone(),
        Settings {
            inbox: NonZeroU32::new(2).unwrap(),
            batch: NonZeroU32::new(1).unwrap(),
            ..settings()
        },
    )
    .await
    .unwrap();
    for number in 1..=2 {
        server
            .receive(a, &upload(number, "readings", vec![reading(a, "poison")]))
            .await
            .unwrap();
    }
    server
        .receive(b, &upload(9, "readings", vec![reading(b, "poison")]))
        .await
        .unwrap();
    let full = server
        .receive(a, &upload(3, "readings", vec![reading(a, "poison")]))
        .await;
    assert!(matches!(&full, Err(e) if e.is_transient()), "{full:?}");
    assert_eq!(wire.filed(a), [1, 2], "the third is not answered");
    let kept = |replica: Uuid| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM sync.inbox WHERE replica_id = $1")
                .bind(replica)
                .fetch_one(&pool)
                .await
                .unwrap()
        }
    };
    assert_eq!(
        (kept(a).await, kept(b).await),
        (2, 1),
        "a's part is full, b's is not"
    );

    BUGGY.set(false);
    senders.known.lock().unwrap().remove(&b);
    let mut lock = Lock::take(&pool, LOCK_KEY).await.unwrap().unwrap();
    assert_eq!(refile(&server, &mut lock).await, 3, "one upload a page");
    assert_eq!(
        (kept(a).await, kept(b).await),
        (0, 1),
        "a's filed, and b's waits for the host to know b again"
    );
    sqlx::query("UPDATE sync.inbox SET first_at = now() - interval '31 days'")
        .execute(&pool)
        .await
        .unwrap();
    refile(&server, &mut lock).await;
    assert_eq!(kept(b).await, 0, "waited too long: dropped");
    BUGGY.set(true);
}

/// A server left at its defaults takes every upload a replica left at its
/// own builds: both take the one default.
#[test]
fn a_servers_default_limits_are_the_replicas() {
    assert_eq!(Settings::default().limits, Limits::default());
}

/// Through a pooler in transaction mode a relay's transaction can run on a
/// server process that does not hold its lock: it is refused, rather than
/// recording beside the process that does.
#[sqlx::test(migrations = false)]
async fn a_transaction_on_a_connection_without_the_lock_is_refused(pool: PgPool) {
    world(&pool).await;
    let held = Lock::take(&pool, LOCK_KEY).await.unwrap().unwrap();
    let mut elsewhere = Lock {
        conn: pool.acquire().await.unwrap().detach(),
    };
    let refused = elsewhere
        .fenced("BEGIN", None)
        .await
        .err()
        .map(|e| e.to_string());
    assert!(
        refused
            .as_deref()
            .is_some_and(|why| why.contains("session pooling")),
        "{refused:?}"
    );
    assert!(!elsewhere.still_held().await, "nor counts as held");
    held.release(LOCK_KEY).await;
}
