//! A replica against what can go wrong with what it is sent: a message from
//! the past, a row it cannot read, a table it does not have. None of it stops
//! the rest, and what it could not take is kept for the server to hear of.

use super::*;
use crate::envelope::{Key as WireKey, Table};
use crate::testkit::{from_wire, to_wire, Item, ItemRow};
use prost::Message as _;

const TABLE: &str = "CREATE TABLE items (
    id BLOB PRIMARY KEY NOT NULL, partition_id BLOB NOT NULL, name TEXT NOT NULL, retired_at TEXT)";

async fn replica(pool: &crate::DbPool) -> sqlx::pool::PoolConnection<crate::Db> {
    let mut conn = pool.acquire().await.expect("a connection");
    sqlx::raw_sql(crate::SQLITE_SCHEMA)
        .execute(&mut *conn)
        .await
        .expect("the engine's tables");
    sqlx::raw_sql(TABLE)
        .execute(&mut *conn)
        .await
        .expect("items");
    conn
}

fn codecs() -> Vec<Codec<()>> {
    vec![Codec::new::<Item, ItemRow>(to_wire, from_wire, None)]
}

/// A row as the server sends it: its key, and its bytes.
fn item(name: &str) -> (Uuid, Vec<u8>) {
    let id = Uuid::now_v7();
    let row = ItemRow {
        id: id.to_string(),
        partition_id: Uuid::nil().to_string(),
        name: name.to_owned(),
    };
    (id, row.encode_to_vec())
}

fn down(table: &str, epoch: &str, sequence: u64, rows: Vec<(Uuid, Vec<u8>)>) -> Down {
    let (keys, upserts): (Vec<WireKey>, Vec<Vec<u8>>) = rows
        .into_iter()
        .map(|(id, bytes)| {
            (
                WireKey {
                    parts: vec![id.to_string()],
                },
                bytes,
            )
        })
        .unzip();
    Down {
        snapshot_id: String::new(),
        tables: vec![Table {
            table: table.to_owned(),
            upserts,
            deletes: Vec::new(),
            keys,
        }],
        sequence,
        epoch: epoch.to_owned(),
    }
}

async fn names(conn: &mut Conn) -> Vec<String> {
    sqlx::query_scalar("SELECT name FROM items ORDER BY name")
        .fetch_all(&mut *conn)
        .await
        .expect("names")
}

#[sqlx::test(migrations = false)]
async fn a_message_at_or_below_the_last_applied_changes_nothing(pool: crate::DbPool) {
    let mut conn = replica(&pool).await;
    let codecs = codecs();

    apply(
        &mut conn,
        &codecs,
        &down("items", "e1", 5, vec![item("five")]),
    )
    .await
    .unwrap();
    apply(
        &mut conn,
        &codecs,
        &down("items", "e1", 4, vec![item("four")]),
    )
    .await
    .unwrap();
    apply(
        &mut conn,
        &codecs,
        &down("items", "e1", 5, vec![item("five again")]),
    )
    .await
    .unwrap();
    assert_eq!(
        names(&mut conn).await,
        vec!["five"],
        "the past changes nothing"
    );

    // A server installed again numbers from scratch, and is listened to.
    apply(
        &mut conn,
        &codecs,
        &down("items", "e2", 1, vec![item("one")]),
    )
    .await
    .unwrap();
    assert_eq!(names(&mut conn).await, vec!["five", "one"]);
}

#[sqlx::test(migrations = false)]
async fn a_row_the_replica_cannot_take_is_reported_until_it_takes_it(pool: crate::DbPool) {
    let mut conn = replica(&pool).await;
    let codecs = codecs();
    let good = item("good");
    let (broken, _) = item("broken");

    let applied = apply(
        &mut conn,
        &codecs,
        &down("items", "e", 1, vec![good, (broken, vec![0xff, 0xff])]),
    )
    .await
    .unwrap();

    assert_eq!(applied[0].1.len(), 1, "the good row is held");
    assert_eq!(names(&mut conn).await, vec!["good"]);
    let reported = rejections(&mut conn, 10).await.unwrap();
    assert_eq!(reported.len(), 1);
    assert_eq!(reported[0].table, "items");
    assert_eq!(
        reported[0].key.as_ref().map(|k| k.parts.clone()),
        Some(vec![broken.to_string()])
    );

    // Sent again as it should be, it is taken, and nothing is left to report.
    let mut fixed = item("fixed");
    fixed.1 = ItemRow {
        id: broken.to_string(),
        partition_id: Uuid::nil().to_string(),
        name: "fixed".to_owned(),
    }
    .encode_to_vec();
    fixed.0 = broken;
    apply(&mut conn, &codecs, &down("items", "e", 2, vec![fixed]))
        .await
        .unwrap();
    assert!(rejections(&mut conn, 10).await.unwrap().is_empty());
    assert_eq!(names(&mut conn).await, vec!["fixed", "good"]);
}

#[sqlx::test(migrations = false)]
async fn a_table_this_build_does_not_mirror_is_rejected_row_by_row(pool: crate::DbPool) {
    let mut conn = replica(&pool).await;
    apply(
        &mut conn,
        &codecs(),
        &down("newer", "e", 1, vec![item("a"), item("b")]),
    )
    .await
    .unwrap();

    let reported = rejections(&mut conn, 10).await.unwrap();
    assert_eq!(reported.len(), 2);
    assert!(reported
        .iter()
        .all(|r| r.reason.contains("does not mirror")));

    reported_up(&mut conn, &reported).await;
    assert!(
        rejections(&mut conn, 10).await.unwrap().is_empty(),
        "forgotten once told"
    );
}

/// A row this build refuses — a trigger, here, as a bug would — is the only
/// one left out: the rows sent with it are taken.
#[sqlx::test(migrations = false)]
async fn a_row_a_trigger_refuses_is_the_only_one_rejected(pool: crate::DbPool) {
    let mut conn = replica(&pool).await;
    sqlx::raw_sql(
        "CREATE TRIGGER a_bug BEFORE INSERT ON items WHEN NEW.name = 'poison' \
         BEGIN SELECT RAISE(ABORT, 'this build cannot take it'); END",
    )
    .execute(&mut *conn)
    .await
    .unwrap();
    let (good, bytes) = item("good");
    apply(
        &mut conn,
        &codecs(),
        &down("items", "e", 1, vec![(good, bytes), item("poison")]),
    )
    .await
    .unwrap();

    let names: Vec<String> = sqlx::query_scalar("SELECT name FROM items")
        .fetch_all(&mut *conn)
        .await
        .unwrap();
    assert_eq!(names, vec!["good".to_owned()]);
    let reported = rejections(&mut conn, 10).await.unwrap();
    assert_eq!(reported.len(), 1, "{reported:?}");
    assert!(reported[0].reason.contains("cannot take it"));
}

async fn reported_up(conn: &mut Conn, rejected: &[envelope::Rejected]) {
    reported(conn, rejected).await.unwrap();
}

#[sqlx::test(migrations = false)]
async fn digests_leave_out_what_was_retired_or_rejected(pool: crate::DbPool) {
    let mut conn = replica(&pool).await;
    let codecs = codecs();
    let (kept, kept_bytes) = item("kept");
    let (retired, retired_bytes) = item("retired");
    let (broken, _) = item("broken");
    apply(
        &mut conn,
        &codecs,
        &down(
            "items",
            "e",
            7,
            vec![
                (kept, kept_bytes),
                (retired, retired_bytes),
                (broken, vec![0xff]),
            ],
        ),
    )
    .await
    .unwrap();
    sqlx::query("UPDATE items SET retired_at = 'now' WHERE name = 'retired'")
        .execute(&mut *conn)
        .await
        .unwrap();

    let up = digests(&mut conn, &codecs, Uuid::nil()).await.unwrap();

    assert_eq!((up.epoch.as_str(), up.sequence), ("e", 7));
    assert_eq!(up.digests.len(), 1);
    assert_eq!(up.digests[0].count, 1);
    assert_eq!(
        up.digests[0].fingerprint,
        envelope::fingerprint(&mut [kept.to_string()])
    );
}

#[sqlx::test(migrations = false)]
async fn what_the_replica_wrote_is_pending_until_the_server_filed_it(pool: crate::DbPool) {
    let mut conn = replica(&pool).await;
    let codecs = codecs();
    let (one, one_bytes) = item("one");
    let (two, two_bytes) = item("two");
    apply(
        &mut conn,
        &codecs,
        &down("items", "e", 1, vec![(one, one_bytes), (two, two_bytes)]),
    )
    .await
    .unwrap();
    for id in [one, two] {
        sqlx::query("INSERT INTO sync_notes (relation, row_key) VALUES ('items', $1)")
            .bind(id.simple().to_string())
            .execute(&mut *conn)
            .await
            .unwrap();
    }
    let progress = |unsent, unfiled| {
        vec![UpProgress {
            table: "items".to_owned(),
            unsent,
            unfiled,
        }]
    };
    assert_eq!(super::progress(&mut conn).await.unwrap(), progress(2, 0));

    let notes = unsent(&mut conn, 10, std::time::Duration::from_secs(3600))
        .await
        .unwrap();
    let up = upload(&mut conn, &codecs, Uuid::nil(), &notes)
        .await
        .unwrap();
    assert_eq!(up.tables[0].upserts.len(), 2);
    assert_eq!(
        up.uploads,
        vec![notes[1].id as u64],
        "numbered by its highest note"
    );
    sent(&mut conn, &notes, &up).await.unwrap();
    assert_eq!(super::progress(&mut conn).await.unwrap(), progress(0, 2));
    assert!(
        unsent(&mut conn, 10, std::time::Duration::from_secs(3600))
            .await
            .unwrap()
            .is_empty(),
        "not sent again while the server may still answer"
    );

    filed(
        &mut conn,
        &envelope::Filed {
            uploads: up.uploads.clone(),
        },
    )
    .await
    .unwrap();
    assert!(
        super::progress(&mut conn).await.unwrap().is_empty(),
        "nothing on its way"
    );
}

/// One message carries whichever thing the server sends; one of a later
/// protocol is not acted on, so it can come again once this build is updated;
/// bytes that are no message at all are dropped rather than stop the queue.
#[sqlx::test(migrations = false)]
async fn a_message_is_acted_on_only_in_a_protocol_this_build_speaks(pool: crate::DbPool) {
    let mut conn = replica(&pool).await;
    let codecs = codecs();

    let applied = receive(
        &mut conn,
        &codecs,
        &Body::Down(down("items", "e", 1, vec![item("one")])).encoded(),
    )
    .await
    .unwrap();
    assert!(
        matches!(applied, Received::Applied(ref t) if t[0].1.len() == 1),
        "{applied:?}"
    );

    let later = ToReplica {
        protocol: envelope::PROTOCOL + 1,
        body: Some(Body::Down(down("items", "e", 2, vec![item("two")]))),
    };
    let refused = receive(&mut conn, &codecs, &later.encode_to_vec()).await;
    assert!(
        matches!(refused, Err(ref e @ Error::Protocol { .. }) if e.is_transient()),
        "{refused:?}"
    );
    assert_eq!(names(&mut conn).await, vec!["one"]);

    let garbage = receive(&mut conn, &codecs, &[0xff, 0xff, 0xff])
        .await
        .unwrap();
    assert!(matches!(garbage, Received::Unreadable(_)), "{garbage:?}");
}

/// One message up carries whatever the replica has to say on its lane: rows it
/// wrote on theirs, and on the main lane what it could not take, what it
/// applied and, when due, what it holds. Once the transport took it, none of
/// it is said again.
#[sqlx::test(migrations = false)]
async fn the_outbox_says_each_thing_once_on_its_lane(pool: crate::DbPool) {
    let mut conn = replica(&pool).await;
    let codecs = codecs();
    let lanes = [
        Lane {
            name: "main",
            tables: &[],
        },
        Lane {
            name: "fast",
            tables: &["items"],
        },
    ];
    let settings = Outbox::default();
    let me = Uuid::now_v7();
    let (written, bytes) = item("written");
    let (_, broken) = item("broken");
    apply(
        &mut conn,
        &codecs,
        &down(
            "items",
            "e",
            7,
            vec![(written, bytes), (Uuid::now_v7(), broken[..3].to_vec())],
        ),
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO sync_notes (relation, row_key) VALUES ('items', $1)")
        .bind(written.simple().to_string())
        .execute(&mut *conn)
        .await
        .unwrap();

    let main = outbox(&mut conn, &codecs, me, &lanes, 0, &settings, false)
        .await
        .unwrap()
        .expect("something to say");
    assert_eq!(main.lane, "main");
    assert!(main.up.tables.is_empty(), "the rows go on their own lane");
    assert_eq!(main.up.rejected.len(), 1);
    assert_eq!((main.up.epoch.as_str(), main.up.sequence), ("e", 7));
    assert_eq!(main.up.protocol, envelope::PROTOCOL);
    delivered(&mut conn, &main).await.unwrap();
    assert!(outbox(&mut conn, &codecs, me, &lanes, 0, &settings, false)
        .await
        .unwrap()
        .is_none());

    let fast = outbox(&mut conn, &codecs, me, &lanes, 1, &settings, false)
        .await
        .unwrap()
        .expect("the row written");
    assert_eq!(fast.up.tables[0].upserts.len(), 1);
    assert_eq!(fast.up.sequence, 0, "the main lane says what was applied");
    assert!(fast.up.rejected.is_empty());
    delivered(&mut conn, &fast).await.unwrap();
    assert!(outbox(&mut conn, &codecs, me, &lanes, 1, &settings, false)
        .await
        .unwrap()
        .is_none());

    let report = outbox(&mut conn, &codecs, me, &lanes, 0, &settings, true)
        .await
        .unwrap()
        .expect("the digests");
    assert_eq!(report.up.digests.len(), 1);
    assert_eq!(report.up.digests[0].count, 1);
    assert_eq!(
        report.up.sequence, 7,
        "digests are compared at what was applied"
    );
}

/// The replica tells the server what it wrote and the server has not filed:
/// with the rows it sends, at once when that starts or empties, and while it
/// grows, no more often than it was told to.
#[sqlx::test(migrations = false)]
async fn the_backlog_is_said_when_it_changes(pool: crate::DbPool) {
    let mut conn = replica(&pool).await;
    let codecs = codecs();
    let lanes = [
        Lane {
            name: "main",
            tables: &[],
        },
        Lane {
            name: "fast",
            tables: &["items"],
        },
    ];
    let mut settings = Outbox::default();
    let me = Uuid::now_v7();
    let note = |key: Uuid| {
        sqlx::query("INSERT INTO sync_notes (relation, row_key) VALUES ('items', $1)")
            .bind(key.simple().to_string())
    };
    let said = |o: &Option<Outgoing>| {
        o.as_ref().and_then(|o| o.up.backlog.clone()).map(|b| {
            b.tables
                .into_iter()
                .map(|t| (t.table, t.unsent, t.unfiled))
                .collect::<Vec<_>>()
        })
    };

    note(Uuid::now_v7()).execute(&mut *conn).await.unwrap();
    let main = outbox(&mut conn, &codecs, me, &lanes, 0, &settings, false)
        .await
        .unwrap();
    assert_eq!(said(&main), Some(vec![("items".into(), 1, 0)]), "it starts");
    delivered(&mut conn, main.as_ref().unwrap()).await.unwrap();

    note(Uuid::now_v7()).execute(&mut *conn).await.unwrap();
    assert!(
        outbox(&mut conn, &codecs, me, &lanes, 0, &settings, false)
            .await
            .unwrap()
            .is_none(),
        "it grows, and was said a moment ago"
    );
    settings.backlog_every = std::time::Duration::ZERO;
    let main = outbox(&mut conn, &codecs, me, &lanes, 0, &settings, false)
        .await
        .unwrap();
    assert_eq!(said(&main), Some(vec![("items".into(), 2, 0)]), "when due");
    delivered(&mut conn, main.as_ref().unwrap()).await.unwrap();

    let fast = outbox(&mut conn, &codecs, me, &lanes, 1, &settings, false)
        .await
        .unwrap()
        .expect("the rows");
    assert!(fast.up.backlog.is_none(), "the main lane says it");
    delivered(&mut conn, &fast).await.unwrap();
    filed(
        &mut conn,
        &envelope::Filed {
            uploads: fast.up.uploads.clone(),
        },
    )
    .await
    .unwrap();
    settings.backlog_every = std::time::Duration::from_secs(3600);
    let main = outbox(&mut conn, &codecs, me, &lanes, 0, &settings, false)
        .await
        .unwrap();
    assert_eq!(said(&main), Some(vec![]), "it empties, said at once");
    assert!(main.as_ref().unwrap().up.tables.is_empty());
    delivered(&mut conn, main.as_ref().unwrap()).await.unwrap();
    assert!(outbox(&mut conn, &codecs, me, &lanes, 0, &settings, false)
        .await
        .unwrap()
        .is_none());
}

/// A replica database with nothing of the engine gets its tables and a
/// trigger on every table that goes up, and every write to one is noted; a
/// table that goes up with nothing noting it is refused.
#[sqlx::test(migrations = false)]
async fn install_notes_every_write_of_what_goes_up(pool: crate::DbPool) {
    use crate::testkit::records::{reading_from_wire, reading_to_wire, Reading, ReadingRow};
    let mut conn = pool.acquire().await.unwrap();
    sqlx::raw_sql(
        "CREATE TABLE readings (id BLOB PRIMARY KEY NOT NULL, replica_id BLOB NOT NULL, value TEXT NOT NULL)",
    )
    .execute(&mut *conn)
    .await
    .unwrap();
    let codecs = vec![Codec::<()>::new::<Reading, ReadingRow>(
        reading_to_wire,
        reading_from_wire,
        None,
    )];
    assert!(
        check(&mut conn, &codecs).await.is_err(),
        "nothing installed yet"
    );

    install(&mut conn, &codecs).await.unwrap();
    install(&mut conn, &codecs).await.expect("idempotent");
    let id = Uuid::now_v7();
    for sql in [
        "INSERT INTO readings VALUES ($1, $1, 'one')",
        "UPDATE readings SET value = 'two' WHERE id = $1",
        "DELETE FROM readings WHERE id = $1",
    ] {
        sqlx::query(sql).bind(id).execute(&mut *conn).await.unwrap();
    }
    let keys: Vec<String> =
        sqlx::query_scalar("SELECT row_key FROM sync_notes WHERE relation = 'readings'")
            .fetch_all(&mut *conn)
            .await
            .unwrap();
    assert_eq!(keys, vec![id.simple().to_string(); 3]);
    assert!(
        codecs[0].key(&[keys[0].clone()]).is_ok(),
        "a key the codec reads"
    );

    sqlx::raw_sql("DROP TRIGGER sync_note_readings_insert; DROP TRIGGER sync_note_readings_update; DROP TRIGGER sync_note_readings_delete")
        .execute(&mut *conn)
        .await
        .unwrap();
    let refused = check(&mut conn, &codecs).await.unwrap_err();
    assert!(refused.to_string().contains("readings"), "{refused}");
}

/// A table that travels as JSON is applied like any other.
#[sqlx::test(migrations = false)]
async fn a_json_table_is_applied_like_any_other(pool: crate::DbPool) {
    let mut conn = replica(&pool).await;
    let codecs = vec![Codec::<()>::json::<Item>(None)];
    let id = Uuid::now_v7();
    let row = serde_json::to_vec(&Item {
        id,
        partition_id: Uuid::nil(),
        name: "as json".to_owned(),
    })
    .unwrap();
    apply(&mut conn, &codecs, &down("items", "e", 1, vec![(id, row)]))
        .await
        .unwrap();
    assert_eq!(names(&mut conn).await, vec!["as json"]);
}

/// A replica does not confirm a message while refusals of it are still to
/// report: the server would take the refused rows for applied.
#[sqlx::test(migrations = false)]
async fn a_replica_confirms_only_once_every_refusal_is_reported(pool: crate::DbPool) {
    let mut conn = replica(&pool).await;
    let codecs = codecs();
    let lanes = [Lane {
        name: "main",
        tables: &[],
    }];
    let settings = Outbox {
        rejections: 1,
        ..Outbox::default()
    };
    let me = Uuid::now_v7();
    let broken = |name: &str| {
        let (id, bytes) = item(name);
        (id, bytes[..3].to_vec())
    };
    apply(
        &mut conn,
        &codecs,
        &down("items", "e", 7, vec![broken("one"), broken("two")]),
    )
    .await
    .unwrap();

    let first = outbox(&mut conn, &codecs, me, &lanes, 0, &settings, false)
        .await
        .unwrap()
        .expect("a refusal to report");
    assert_eq!((first.up.rejected.len(), first.up.sequence), (1, 0));
    delivered(&mut conn, &first).await.unwrap();
    let last = outbox(&mut conn, &codecs, me, &lanes, 0, &settings, false)
        .await
        .unwrap()
        .expect("the other refusal, and the confirmation");
    assert_eq!((last.up.rejected.len(), last.up.sequence), (1, 7));
}

/// A backlog that is not empty is said again now and then, unchanged, and
/// at once to a server installed again: either may have lost it.
#[sqlx::test(migrations = false)]
async fn a_backlog_not_empty_is_said_again(pool: crate::DbPool) {
    let mut conn = replica(&pool).await;
    let codecs = codecs();
    let lanes = [
        Lane {
            name: "main",
            tables: &[],
        },
        Lane {
            name: "fast",
            tables: &["items"],
        },
    ];
    let mut settings = Outbox::default();
    let me = Uuid::now_v7();
    sqlx::query("INSERT INTO sync_notes (relation, row_key) VALUES ('items', 'k')")
        .execute(&mut *conn)
        .await
        .unwrap();
    let main = outbox(&mut conn, &codecs, me, &lanes, 0, &settings, false)
        .await
        .unwrap()
        .expect("it starts");
    delivered(&mut conn, &main).await.unwrap();
    assert!(outbox(&mut conn, &codecs, me, &lanes, 0, &settings, false)
        .await
        .unwrap()
        .is_none());

    settings.backlog_every = std::time::Duration::ZERO;
    let again = outbox(&mut conn, &codecs, me, &lanes, 0, &settings, false)
        .await
        .unwrap()
        .expect("unchanged, said again");
    assert!(again.up.backlog.is_some());
    delivered(&mut conn, &again).await.unwrap();

    settings.backlog_every = std::time::Duration::from_secs(3600);
    apply(&mut conn, &codecs, &down("items", "first", 1, vec![]))
        .await
        .unwrap();
    apply(
        &mut conn,
        &codecs,
        &down("items", "installed again", 1, vec![]),
    )
    .await
    .unwrap();
    let told = outbox(&mut conn, &codecs, me, &lanes, 0, &settings, false)
        .await
        .unwrap()
        .expect("a new server hears it");
    assert!(told.up.backlog.is_some());
}
