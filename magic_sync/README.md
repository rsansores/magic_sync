# magic_sync

A sync engine that keeps copies of PostgreSQL tables on SQLite replicas, and
brings rows the replicas write back to the server. It is built on `sqlx`, and
carries its messages on any transport you give it (the first host uses NATS
JetStream).

This document explains the engine from the ground up: the idea it rests on,
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

### The codec

A table travels as its own protobuf message. A [`wire::Codec`] erases the row
type so the engine can hold every table in one list: it is built from the row
type, its message type, and the two mapping functions between them. The engine
never names a table; it dispatches on the table name through the codecs.

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
Its own triggers note them locally; the host's upload loop gathers the noted
rows as they are now ([`replica::collect`]) and sends them in an
[`envelope::Up`]. A row that is gone by then goes as its key, but only for a
table keyed by its replica (`ORIGIN`): history a replica pruned before it went
up is not the server's to delete.

Each upload is numbered by the highest note it carries. The server answers
every upload it filed with a `Filed` message, after its transaction commits,
and the replica lets go of those notes. An upload the server does not answer
goes again after a while. So the replica always knows what it wrote that has
not reached the server yet.

The server files an upload ([`replica::file`]) under **the replica the message
came from, and what the server knows about it**, never what the rows claim:

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

[`plan::Engine::progress`] answers, for one replica, a list of them, a whole
partition or every replica, and per table:

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
filed yet.

### What an operator sees

[`relay::health`] answers how the engine is doing: how many notes are pending
and how old the oldest is, the parked notes, how many rows are in quarantine
and in the inbox, and the replicas a send keeps failing to.
[`relay::replica_status`] answers the same for one replica. Each of these
means one of two things: wait (the infrastructure is down), or deploy a fix (a
bug). Never "go fix the data".

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

They rest on what the host provides:

- **One relay at a time.** The host runs the relay under a lock (a PostgreSQL
  advisory lock in the first host), and records each pass on that lock's
  connection.
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

## 8. What a host does

Most of what follows is done for the host by the runners: [`Server`] on the
server and [`Replica`] on a replica ([`run`]). A host builds one, runs it in a
task of its own, and hands it every message the transport delivers. The
steps below are what the runners do, for a host that drives the engine
itself.

### On the server

1. **Install the schema.** Run `sql/postgres.sql` in a migration, before any
   table calls `sync.mirror_table`. It creates the `sync` schema: `notes`,
   `held`, `replicas`, `quarantine`, `inbox`, the message sequence, the epoch,
   and the trigger functions.
2. **Mirror the tables.** After each table, `SELECT sync.mirror_table(...)`;
   after each group table, `SELECT sync.mirror_group(...)`.
3. **Declare the rows and codecs**, and **declare the replicas**
   ([`plan::Replicas`]): the relation listing them, which of them are live, and
   the partition column, if any.
4. **Build the engine** once ([`plan::Engine::new`]). It reads the catalogue
   and refuses declarations the schema cannot honour: a cycle, a `Follows`
   column that is not a foreign key, a table with no notes.
5. **Implement the transport** ([`relay::Transport`]): send one message's
   bytes to one replica, returning a transient or permanent `Error::Transport`
   on failure.
6. **Run the relay loop**, holding the lock:
   - each pass: `prepare` in a read transaction, `publish`, then `record` in a
     transaction on the lock's connection;
   - on a transient error, back off and run the pass again;
   - when the lock is taken, and on a timer: `unpark`, `retry_quarantine`, and
     re-file the inbox (`inbox` + `refile`);
   - on a timer: `prune` done notes, and check `health`.
7. **File uploads** with `replica::file` in a transaction, and run the effects
   the host wants once it commits.
8. **Provision a replica** with `note_resync` in the transaction that links it.

### On a replica

1. **Install the schema.** Run `sql/sqlite.sql` in a migration.
2. **Receive** each message with `replica::receive`, in a transaction;
   acknowledge it to the transport only after the commit, and not at all when
   `receive` fails.
3. **Send up** with `replica::outbox`, then `replica::delivered` once the
   transport took the message: one message per lane says everything the
   replica has to tell the server — the rows it wrote, the rows it could not
   take, the last message it applied, and, on a timer, its digests.

---

## 9. The parts

| Module | What it holds |
|---|---|
| [`row`] | a table's declaration (`Row`, `Mirror`, `Sync`, `Column`), and the writes both sides make: upsert with the version guard, insert-first-wins, retire |
| [`wire`] | the codec: a table's rows encoded as its own message, and applied by the rules its row declares |
| [`envelope`] | the messages between server and replica, the packer, the fingerprint (the wire is `proto/sync.proto`, and [PROTOCOL.md](PROTOCOL.md) says what a replica in any language must do) |
| [`plan`] | the planner and the notes (server) |
| [`relay`] | one pass (prepare, publish, record), the transport trait, parking, quarantine, the inbox, health (server) |
| [`replica`] | applying, sweeping a snapshot, uploading, rejecting, digests (replica), and filing uploads (server) |
| [`dialect`] | the few things PostgreSQL and SQLite spell differently |
| [`run`] | the runners: [`Server`] (the relay under its lock, retries, filing and answering uploads) and [`Replica`] (receiving, and the way up) |

| File | What it is |
|---|---|
| `sql/postgres.sql` | the server's schema |
| `sql/sqlite.sql` | a replica's schema |

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

Run them with `just test-sync-engine` (PostgreSQL through `DATABASE_URL`, SQLite
in a temporary file).
