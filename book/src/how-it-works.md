# How it works

This chapter explains the engine from the ground up: the idea it rests on,
what a table declares, what happens to a change from the moment it is written
to the moment a replica holds it, how every kind of failure is handled, and
what the host application has to provide. Read it in order; each section
leans on the ones before it.

---

## 1. The idea: compare states, do not replay a log

Most sync engines replay a log. Every change becomes an event, the events are
sent in order, and the replica re-applies them. That design breaks in
familiar ways:

- **A lost or skipped event is a permanent divergence.** If an event fails and
  is skipped, nothing will ever send it again, because the log has moved on.
- **A failing event blocks everything behind it.** Order matters, so one bad
  event stops the queue for everyone (the classic "poison message").
- **Repair is manual.** Someone has to find what was lost and replay it by
  hand.

This engine does not replay anything. For every replica, the server keeps a
record of **which rows that replica holds** (the *held set*). When something
changes, the engine works out **which rows the replica should hold now**
(the *wanted set*) and sends only the difference:

```
send   = wanted − held          (rows to hold, or that changed)
delete = held − wanted          (rows to let go)
```

Then it records the new held set. A change is never "an event that must be
delivered"; it is a reason to compare two sets again. That single choice is
what makes everything else in this document possible:

- A row that could not be sent is simply not recorded as held. The next
  comparison finds it missing and sends it again — **as it is then**, not as
  it was.
- Nothing needs to be replayed in order. If a replica misses messages, the
  next comparison catches it up from the current state.
- Repair is a comparison too. Provisioning a replica from scratch is "compare
  against an empty held set".

Payloads are always rebuilt from the current rows. The engine never stores the
content of a change, only *that* something changed.

---

## 2. Words

| Word | Meaning |
|---|---|
| **server** | the PostgreSQL database that owns the truth |
| **replica** | a SQLite database that holds a copy of some rows, identified by a UUID |
| **partition** | an optional column that splits rows and replicas into groups no row crosses (for example, one per customer of a multi-customer system). Without one, there is a single partition |
| **mirrored table** | a table that exists on both sides with the same columns |
| **note** | a record, written in the same transaction as a change, that a row (or a group, or a replica) changed |
| **held set** | `sync.held`: which rows each replica holds, according to the server |
| **horizon** | the point in time (a PostgreSQL transaction id) a held row was last sent from |
| **retired** | a row the replica let go of but kept, because a row of its own still points at it |
| **relay** | the server process that turns notes into messages, one at a time per server |
| **transport** | whatever carries messages between server and replicas |

---

## 3. What a table declares

A table is declared once, as the Rust struct that is its row, implementing
[`row::Row`]. The declaration answers two questions.

### Which way does it travel? (`Mirror`)

| `Mirror` | Written on | Travels |
|---|---|---|
| `Catalog` | the server | down to the replicas that should hold its rows |
| `Record` | a replica | up to the server |
| `Both` | both, column by column | the columns the replicas own (`Column::replica_owned`) go up; the rest go down |
| `ServerOnly` | the server | nowhere |

### Which replicas hold which rows? (`Sync`, for tables that go down)

| `Sync` | A replica holds a row when |
|---|---|
| `Broadcast` | the row is in the replica's partition |
| `Assigned(column)` | `column` names the replica |
| `Grouped { members, assigned }` | the row is a member of a group (`members` is a `(group, row)` table) that is given to the replica (`assigned` is a `(group, replica)` table) |
| `Follows(column)` | the row `column` points at goes to the replica (a table that extends another one) |
| `Referenced` | no rule of its own: it goes wherever a row that travels points at it |

`Referenced` is what keeps a replica consistent. **A row always travels with
the rows it points at**: if a replica is sent an order that points at a
customer, it is sent the customer too, even though customers have no rule of
their own. And a parent is only taken back once nothing the replica still
holds points at it. The engine reads the foreign keys from the database
catalogue, so this needs no declaration: the schema already says it.

### The rest of a declaration

| `Row` constant | What it is for |
|---|---|
| `TABLE`, `SCHEMA`, `KEY` | where the table lives and its primary key (one or more uuid or integer columns) |
| `VERSION` | a column whose value only moves forward (an `updated_at`): a replica never overwrites a newer copy with an older one |
| `CREATED` | a column written once, never by an update |
| `RETIRED` | the column a replica sets on a row it lets go of but cannot delete, because a row of its own points at it |
| `ORIGIN` | for a record: the key column naming the replica that wrote it, so a replica may delete only its own rows on the server |
| `UNMIRRORED` | columns one side has that the row leaves out |
| `ACTIVE` | a relation (a view) the rows replicas should hold are read through, leaving the others out; the table itself when there is none |

`#[derive(Row)]` writes all of them from the struct and its attributes
([The rows](guide/rows.md)); a host can implement the trait by hand too.

### The codec

A table travels in its own encoding: the row's serde shape as JSON
(`Codec::json`), or a protobuf message of its own with a mapping each way
(`Codec::new`). A [`wire::Codec`] erases the row type so the engine can hold
every table in one list. The engine never names a table; it dispatches on the
table name through the codecs.

---

## 4. The life of a change, server to replica

Here is everything that happens when someone renames a product on the server,
until every replica that should hold it holds the new name.

```
 UPDATE products …                          (the host's transaction)
   └─ trigger: INSERT INTO sync.notes        (same transaction: atomic)

 relay pass
   1. prepare  (a read transaction)
        read pending notes ──▶ plan ──▶ load and encode rows
   2. publish  (no transaction open)
        per replica: pack into messages ──▶ number them ──▶ transport.send
   3. record   (a transaction on the relay lock's connection)
        sync.held ← what was sent;  notes ← done

 replica
   apply the message in one transaction ──▶ ack
```

### 4.1 Noting

`sync.mirror_table('schema', 'table', ignore, 'partition_column')` installs
triggers on a table. Every insert, delete, and update that changes something
writes a row to `sync.notes`: the table, the row's key, the row as it was
before (which says what it pointed at), and the id of the transaction. Because
the note is written by a trigger in the same transaction, **no write path can
forget to note**, and a rolled-back change leaves no note. `TRUNCATE` is
refused on a mirrored table, because it would remove rows without a note.

Group tables get `sync.mirror_group`: a member joining or leaving a group, or a
group given to or taken from a replica, is noted the same way.

### 4.2 Planning

The relay reads up to a batch of pending notes, oldest first. It reads every
note that is not done, instead of "every note after the last one I read",
because PostgreSQL assigns ids before commit: a transaction that started first
can commit last with a lower id. A cursor would skip it; a "not done" query
never does.

For the notes it read, the planner computes, per replica:

1. **The changed rows**: who should hold each one now (by its table's `Sync`
   rule) against who holds it (`sync.held`). New holders get it, holders that
   should not hold it any more lose it.
2. **Parents**: every row being sent brings along each row it points at that
   the replica lacks.
3. **Release**: a row taken back, or one a changed row no longer points at,
   releases what it pointed at, and a parent goes only once nothing the
   replica keeps points at it.
4. **Followers**: rows that `Follow` a parent go wherever the parent goes.

These steps feed each other, so they repeat until nothing changes. The result
is a `Delta`: per replica, rows to send and rows to take back.

**The horizon.** Planning starts by reading the oldest transaction still
running (`pg_snapshot_xmin`). Every transaction older than that has finished,
so anything it changed is in the rows the plan reads. When a replica is
recorded as holding a row, the horizon is recorded with it. Later, a note for
that row is only acted on if its transaction is at or past the recorded
horizon. That is how a change the replica already received is not sent twice,
while a change committed *after* the plan read (by a transaction that started
earlier) is never missed.

### 4.3 Loading

Every row the delta sends is read once and encoded once by its codec, however
many replicas get it. A row that is gone by now (deleted since the plan read
it) is taken back instead of sent.

### 4.4 Publishing

Each replica's rows are packed into messages ([`envelope::Down`]) under the
transport's size limits: parents' rows first, then deletes, children first.
Every message goes out as one [`envelope::ToReplica`], whatever it carries, so
a transport moves bytes and never looks inside. A row too large for any
message is set aside at loading, like one that does not encode (section 6).
Each row travels with its key, so a replica that cannot even decode a row can
still say which one it was.

**Every message is numbered.** The relay draws a number from a PostgreSQL
sequence at the start of each pass and numbers the pass's messages from it. A
sequence never goes backwards, whichever relay draws from it, and is not
rolled back with a transaction. A replica applies nothing at or below the last
number it applied. That protects against a message delivered twice, and
against a relay that lost its lock and sends a stale message late: its numbers
are lower than the new relay's, so the replica drops them. Messages also carry
the *epoch*, a random id made when the server's schema was installed: a server
installed again numbers from scratch, and a replica that sees a new epoch
starts counting again.

A provisioning (a *snapshot*) sends everything the replica should hold, then an
[`envelope::EndSnapshot`]. On the end, the replica deletes every row of those
tables the snapshot did not name.

### 4.5 Recording

Only after the messages are out does the relay record what each replica now
holds and mark the notes done. If the relay dies between publishing and
recording, the next pass sends the same rows again, numbered higher, and the
replica applies them again with the same result. **A row is never recorded as
held that was not sent**; at worst, it is sent twice.

The record transaction runs on the connection that holds the relay's lock. If
that connection is lost, PostgreSQL releases the lock and rolls back the
record together. So a relay that lost its lock can never commit over the one
that took it.

### 4.6 Applying, on the replica, and saying so

The replica applies a message in one transaction ([`replica::apply`]):

- Each row overwrites its copy, except the columns the replica owns, unless the
  replica holds a newer version (`VERSION`).
- A row it had retired is offered again and becomes live.
- A row it is told to let go of is deleted or, if a row of its own still points
  at it, retired (its `RETIRED` column set). A sale keeps its customer.

Then it tells the server the number of the last message it applied. Until it
does, what the server recorded in 4.5 is only *on its way*: every held row
remembers the message that carried it, and counts as held once the replica
has acknowledged that message. This is what keeps the progress figures
(section 6) honest: they count what the replica confirmed, never what was
merely sent.

---

## 5. The life of a change, replica to server

A replica writes its own rows (records, and its columns of two-way tables).
A trigger notes each write in the writing transaction (`sync_notes`: the table
and the key), so no write path can forget. The replica's upward loop gathers
the noted rows **as they are now** — a row edited twice goes up once, with its
last version — into an [`envelope::Up`], and hands it to the host's uplink. A
row that is gone by then goes as its key, but only for a table keyed by its
replica (`ORIGIN`): history a replica pruned before it went up is not the
server's to delete.

Each upload is numbered by the highest note it carries. The server answers
every upload it filed with a `Filed` message, after its transaction commits,
and the replica lets go of those notes. An upload the server does not answer
goes again after a while. So the replica always knows what it wrote that has
not reached the server yet.

The same `Up` carries everything else a replica has to tell the server: the
rows it could not take, the last message it applied, and, hourly, its
digests. A replica may split its writes into **lanes** — a table written often
on a stream of its own — and the first lane carries what the engine itself
says.

The server files an upload under **the replica the message came from, and
what the server knows about it**, never what the rows claim:

- a record is kept from its first delivery, or the newest report wins when it
  has a `VERSION`;
- a two-way table takes only the columns the replica owns;
- a catalog is refused, because replicas do not write the server's tables;
- a replica deletes only its own rows.

The host passes its own knowledge of the sender as a value of its choosing
(the `P` in `Codec<P>`), and each codec may stamp rows with it.

---

## 6. When something fails

This is the contract the engine is built around:

> **An operator fixes the infrastructure or deploys a fix. Nobody repairs data
> by hand.**

Every failure is one of two kinds ([`Error::is_transient`]):

- **Transient**: the infrastructure failed. The database is unreachable, the
  transport is down, a deadlock happened. Retrying the same work unchanged may
  succeed.
- **Permanent**: the data or the code failed. A row that does not encode, a
  value a constraint refuses, a bug. Retrying unchanged fails the same way.

The transient side is the listed one: an error nobody listed is treated as
permanent, so it is set aside and shows, instead of being retried forever.

**A transient failure stops the pass, and the same pass runs again.** Nothing
is lost: the notes are still pending and nothing was recorded. Order is
preserved because everything waits.

**A permanent failure is narrowed down to what it is about and set aside.**
Nothing else waits for it, and it gets through by itself once the cause is
fixed:

| What fails | What the engine does | How it heals |
|---|---|---|
| Planning, on one note | halves the batch until the failing note is alone, parks it (`parked_at`), and plans the rest | parked notes are unparked when the relay starts (usually after a deploy) and on a timer |
| Encoding a row, on the server | leaves the row out of what is sent and recorded, and **quarantines** it for every replica that was owed it. A replica keeps whatever copy it has | quarantined rows are noted again, for their replica alone, when the relay starts and on a timer; a row leaves the quarantine once its replica holds it, or once it is owed to no one |
| The transport, for one replica | that replica waits out a backoff (`sync.replicas`); what it is owed is noted again **for it alone**, as of the transactions that first noted it; every other replica carries on | sent when its backoff ends |
| A row larger than any message may be | left out at loading and **quarantined**, as one that does not encode | as above, once a fix makes it smaller or the limit larger |
| The transport, refusing a message for good | what the message carried is quarantined for that replica | as above |
| A replica, taking a row | the replica notes it in the same transaction (`sync_rejections`) and reports it in an `Up`; the server stops counting it as held and quarantines it | a fix deployed on either side lets the retry through |
| The server, filing an uploaded row | the row is kept **as it came**, bytes and all, in `sync.inbox` (the replica may have let go of it since); the rest of the upload is filed | filed again when the relay starts and on a timer |
| Anything nobody foresaw | see below | |

### The backstop: digests

Every so often, each replica reports a **digest** of what it holds of every
table: how many rows, and a fingerprint of their keys (FNV-1a over the sorted
keys; rows it retired or could not take are left out). The server compares it
with what it records the replica holds (leaving out the rows it quarantined
for it). It only compares once the replica has applied every message recorded
for it, so a message on its way is never mistaken for drift. If they differ,
the replica is provisioned again from scratch.

So even a failure nobody predicted, or one misclassified as harmless, ends with
the replica holding exactly what it should, without anyone noticing it first.

### Progress: what each replica holds, honestly

Every read below takes a scope — some replicas, a partition, or all of them —
and counts only live replicas, so a host shows each audience its own.

[`plan::Engine::overview`] answers first whether everything is where it
should be. Per replica, one state:

| State | Means |
|---|---|
| `synced` | it holds what it should, and the server filed everything it last said it wrote |
| `catching_up` | changes are on their way to it (noted and not yet planned, or in a message it has not confirmed) or from it (its backlog) |
| `unreachable` | sends to it fail; it is tried again after a backoff |
| `awaiting_fix` | something is set aside for it until a fix: a row in quarantine, a parked note, an upload in the inbox |

It never counts the rows a replica should hold: it reads only what the engine
records, so it costs the same for a fleet as for one replica, and it is exact
all the same. A change a replica lacks is in one of three places — a note not
yet planned, a message it has not confirmed, or set aside — and each is
recorded. Per table, the same figures over the scope's replicas say which
tables are not where they should be yet.

[`plan::Engine::progress`] answers how far along: for a few replicas, per
table:

| Figure | Meaning |
|---|---|
| `should` | rows its rules say it should hold |
| `holds` | rows it should hold and **confirmed** it holds |
| `outdated` | of those, rows changed since (a newer version is on its way or owed) |
| `in_flight` | rows sent and not confirmed yet |
| `to_remove` | rows it holds and should no longer |
| `quarantined` | rows set aside for it until a fix |

`holds - outdated` of `should` is the replica's progress bar, the way a music
library shows "40 of 500 songs on this tablet". It cannot run ahead of the
replica, because nothing counts until the replica says it applied it.
[`plan::Engine::row_progress`] answers the other question, "where is this
row?": per replica, whether it holds it, holds an older version, has it on
its way, lacks it, is letting it go, or has it set aside.

The replica answers the opposite direction with [`replica::progress`]: per
table, rows it wrote that are not sent yet, and rows sent the server has not
filed yet. It tells the server the same whenever it changes, and the server
shows it as the replica's backlog.

### What an operator sees

[`plan::Engine::health`] answers what the engine waits on over a scope: how
many notes are pending and since when, the parked notes, the rows in
quarantine and why, the uploads in the inbox and why, and the replicas a send
keeps failing to. Each of these means one of two things: wait (the
infrastructure is down), or deploy a fix (a bug). Never "go fix the data".
When there are none, the engine is healthy — the normal state.

---

## 7. Guarantees, and what they rest on

The engine guarantees that:

1. **No committed change is lost.** It is noted in the same transaction, and a
   note is only done once what it asked for was sent and recorded.
2. **A replica converges to what it should hold.** At-least-once delivery,
   idempotent application, and the digest backstop.
3. **A replica never holds a row without the rows it points at.**
4. **A replica never goes back in time.** Numbered messages, and the `VERSION`
   guard on every row.
5. **One failure holds back only what it is about.**

They rest on:

- **One relay at a time.** The server's runner holds a PostgreSQL advisory
  lock while it relays, and records each pass on that lock's connection.
- **One message in flight per replica.** The transport delivers a replica's
  messages in order and does not deliver the next one until the previous one
  is acknowledged. With NATS JetStream, that is a durable pull consumer with
  `max_ack_pending = 1`. Without this, a failed message could be retried after
  the ones behind it.
- **Transactions.** `prepare` and `file` run in a transaction (they use
  savepoints); `apply` runs in one so a message applies whole or not at all.
- **A monotonic version column**, if tables have one: the server must stamp it
  with the clock at the write, never behind the value it replaces (not
  `now()`, which is when the transaction began).

---

## 8. What is the host's, and what is the engine's

The host writes its tables, a struct per table, the codec list, where the
replicas are, how bytes travel each way (a [`relay::Transport`] and a
[`run::replica::Uplink`]), and, when it needs to, what it knows about a
sender ([`run::server::Senders`]). [Build your own](guide/tables.md) walks
through each.

Everything else is the runners' ([`run`]): on the server, the lock, the
passes and their backoff, retrying what was set aside, pruning, the alarms,
filing uploads and answering them; on a replica, applying each message in one
transaction, and the upward loop. The step functions they are built on —
`Relay::prepare`/`publish`/`record`, `replica::apply`, `replica::outbox` and
the rest — stay public, for a host that needs to drive the engine itself.

---

## 9. The parts

| Module | What it holds |
|---|---|
| [`row`] | a table's declaration (`Row`, `Mirror`, `Sync`, `Column`), and the writes both sides make: upsert with the version guard, insert-first-wins, retire |
| [`wire`] | the codec: a table's rows encoded as its own message, and applied by the rules its row declares |
| [`envelope`] | the messages between server and replica, the packer, the fingerprint (the wire is `proto/sync.proto`, and [the protocol](protocol.md) says what a replica in any language must do) |
| [`plan`] | the planner and the notes (server) |
| [`relay`] | one pass (prepare, publish, record), the transport trait, parking, quarantine, the inbox (server) |
| [`status`] | what the engine answers about itself: the overview, health, progress, where a row is (server) |
| [`replica`] | applying, sweeping a snapshot, uploading, rejecting, digests (replica), and filing uploads (server) |
| [`dialect`] | the few things PostgreSQL and SQLite spell differently |
| [`run`] | the runners: [`Server`] (the relay under its lock, retries, filing and answering uploads) and [`Replica`] (receiving, and the way up) |

| File | What it is |
|---|---|
| `sql/postgres.sql` | the server's schema |
| `sql/sqlite.sql` | a replica's schema |
| `proto/sync.proto` | the engine's messages, for a replica in another language |

The derive lives in its own crate, `magic_sync_derive`, re-exported as
`magic_sync::Row` and `magic_sync::Columns`.

The crate is compiled for one backend at a time: `postgres` for the server,
`sqlite` for a replica.

---

## 10. How it is tested

- **The planner** is driven by a seeded random walk of writes over a schema
  with every strategy. After each step, every modelled replica must hold
  exactly what its strategies say, and never a row without its parent.
- **Each failure** in section 6 has its own test, against a fake transport and
  a codec with a deliberate bug that a "deploy" fixes.
- **A chaos test** runs storms of random writes (inserts, renames, rows retired
  and deleted, a tenth of them rows the encoding refuses) while the transport
  fails a third of the sends at random. Replicas are modelled as real ones
  behave: they only apply messages numbered above the last one. Then the
  network comes back and the fix is deployed. Every replica must end up holding
  exactly the live rows, with nothing pending and nothing in quarantine.
- **Races**: a transaction that commits after a plan read past it, and a relay
  that sent but died before recording.
- **The replica side** runs on SQLite: messages from the past, rows it cannot
  decode, tables it does not have, digests.

Run them with `cargo test -p magic_sync --features postgres` (PostgreSQL
through `DATABASE_URL`; each test gets a database of its own) and
`cargo test -p magic_sync --features sqlite`.

[`row::Row`]: https://docs.rs/magic_sync/latest/magic_sync/row/trait.Row.html
[`wire::Codec`]: https://docs.rs/magic_sync/latest/magic_sync/wire/struct.Codec.html
[`envelope::Down`]: https://docs.rs/magic_sync/latest/magic_sync/envelope/struct.Down.html
[`envelope::EndSnapshot`]: https://docs.rs/magic_sync/latest/magic_sync/envelope/struct.EndSnapshot.html
[`envelope::ToReplica`]: https://docs.rs/magic_sync/latest/magic_sync/envelope/struct.ToReplica.html
[`envelope::Up`]: https://docs.rs/magic_sync/latest/magic_sync/envelope/struct.Up.html
[`replica::apply`]: https://docs.rs/magic_sync/latest/magic_sync/replica/fn.apply.html
[`replica::file`]: https://docs.rs/magic_sync/latest/magic_sync/replica/fn.file.html
[`Error::is_transient`]: https://docs.rs/magic_sync/latest/magic_sync/enum.Error.html#method.is_transient
[`plan::Engine::overview`]: https://docs.rs/magic_sync/latest/magic_sync/plan/struct.Engine.html#method.overview
[`plan::Engine::health`]: https://docs.rs/magic_sync/latest/magic_sync/plan/struct.Engine.html#method.health
[`plan::Engine::progress`]: https://docs.rs/magic_sync/latest/magic_sync/plan/struct.Engine.html#method.progress
[`plan::Engine::row_progress`]: https://docs.rs/magic_sync/latest/magic_sync/plan/struct.Engine.html#method.row_progress
[`plan::Engine::new`]: https://docs.rs/magic_sync/latest/magic_sync/plan/struct.Engine.html#method.new
[`plan::Replicas`]: https://docs.rs/magic_sync/latest/magic_sync/plan/struct.Replicas.html
[`replica::progress`]: https://docs.rs/magic_sync/latest/magic_sync/replica/fn.progress.html
[`relay::Transport`]: https://docs.rs/magic_sync/latest/magic_sync/relay/trait.Transport.html
[`run::replica::Uplink`]: https://docs.rs/magic_sync/latest/magic_sync/run/replica/trait.Uplink.html
[`run::server::Senders`]: https://docs.rs/magic_sync/latest/magic_sync/run/server/trait.Senders.html
[`run`]: https://docs.rs/magic_sync/latest/magic_sync/run/index.html
[`row`]: https://docs.rs/magic_sync/latest/magic_sync/row/index.html
[`wire`]: https://docs.rs/magic_sync/latest/magic_sync/wire/index.html
[`envelope`]: https://docs.rs/magic_sync/latest/magic_sync/envelope/index.html
[`plan`]: https://docs.rs/magic_sync/latest/magic_sync/plan/index.html
[`relay`]: https://docs.rs/magic_sync/latest/magic_sync/relay/index.html
[`status`]: https://docs.rs/magic_sync/latest/magic_sync/status/index.html
[`replica`]: https://docs.rs/magic_sync/latest/magic_sync/replica/index.html
[`dialect`]: https://docs.rs/magic_sync/latest/magic_sync/dialect/index.html
[`Server`]: https://docs.rs/magic_sync/latest/magic_sync/run/server/struct.Server.html
[`Replica`]: https://docs.rs/magic_sync/latest/magic_sync/run/replica/struct.Replica.html
