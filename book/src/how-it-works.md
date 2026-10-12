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
| **noted** | the newest note of a row when the version a replica holds was read: which of its changes that version holds |
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
| `Both` | both, column by column | the columns the replicas own (`#[row(replica_owned)]`) go up; the rest go down |

### Which replicas hold which rows? (`sync`, for tables that go down)

| `sync` | A replica holds a row when |
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

| Declaration | What it is for |
|---|---|
| `table`, `schema`, `key` | where the table lives and its primary key (one or more uuid or integer columns) |
| `#[row(version)]` | for a record a register edits, a column whose value only moves forward (an `updated_at`): the server never files a replica's older report over a newer one. A table the server sends down has none: a replica takes what the server sends, whatever its version |
| `#[row(created)]` | a column written once, never by an update |
| `retired` | the column a replica sets on a row it lets go of but cannot delete, because a row of its own points at it |
| `origin` | for a record: the key column naming the replica that wrote it, so a replica may delete only its own rows on the server |
| `unmirrored` | columns one side has that the row leaves out |
| `active` | a relation (a view) the rows replicas should hold are read through, leaving the others out; the table itself when there is none |

`#[derive(Row)]` turns them into the row's implementation of [`row::Row`]
([The rows](guide/rows.md)), and is the only way to implement it: what it
writes is checked where it is written, and again against the database when
either side starts.

### The codec

A table travels in its own encoding: JSON, an object of its columns
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
   0. observe  (what became of the replicas: live, moved, gone)
   1. record   (one REPEATABLE READ transaction on the relay lock's connection)
        read pending notes ──▶ plan ──▶ load and encode rows, a chunk at a time
        ──▶ per replica: pack into messages, number them
        ──▶ sync.outbox ← the messages;  sync.held ← what they carry;  notes ← done
   2. publish  (no transaction open)
        per replica: its outbox, in order ──▶ transport.send ──▶ out of the outbox

 replica
   apply the message in one transaction ──▶ ack
```

### 4.1 Noting

Every mirrored table has the engine's triggers, as `Engine::triggers` writes
them ([The tables](guide/tables.md)). Every insert, delete, and update that
changes a column the replicas do not own writes a row to `sync.notes`: the
table as the engine knows it (a partitioned table by its own name, not its
partition's), the row's key in the order the row declares it, the columns it
pointed at other mirrored rows with before the change, and the id of the
transaction. A key that changed notes the old key too, in the partition it
was in, and a row that moved to another partition is noted in the one it
left as well. Because
the note is written by a trigger in the same transaction, **no write path can
forget to note**, and a rolled-back change leaves no note. `TRUNCATE` is
refused on a mirrored table, because it would remove rows without a note.

Group tables get a trigger too: a member joining or leaving a group, or a
group given to or taken from a replica, is noted the same way. The server
refuses to start while a trigger is missing, disabled, or not as it would
write it.

### 4.2 Planning

Before each pass the relay compares the replicas (`Replicas::new` and
`live`) with what it saw last (`sync.replicas.live`, `partition_id`): a
replica that became live, or moved to another partition, is provisioned again
from scratch; one that stopped being live, or is gone, is forgotten — nothing
recorded as held by it, nothing sent it.

The relay reads up to a batch of pending notes, oldest first, in a
`REPEATABLE READ` transaction: the notes, the plan, the rows it loads and
what it records are one snapshot, so a row is never sent as it is after a write the plan did not
see — a parent it now points at left behind. A note this build cannot read (a
later build's) is skipped by the query, and holds back nothing. It reads every
note that is not done, instead of "every note after the last one I read",
because PostgreSQL assigns ids before commit: a transaction that started first
can commit last with a lower id. A cursor would skip it; a "not done" query
never does.

What a replica should hold is the least set of rows closed under three
rules: the rows its tables' rules send it; every row one of those points at
(a parent goes with its child); every row that follows one of those. For the
notes it read, the planner works out how that set moved, around what changed
— never the whole of it but for a replica provisioned from scratch:

1. **Rows in question.** Of the rows a replica holds, those a change may
   have taken out of its set: a changed row its rules no longer send it (a
   `Referenced` or following row: any that changed), the parents a changed
   row pointed at before, rows that cannot go. Then, closed over what they
   point at and what follows them, the rows of its that leaned on those.
   Every row it holds outside them stays as recorded.
2. **What it keeps.** The rows its rules send it; the rows in question that
   a row it holds outside them points at, or that follow one it holds; then,
   a round at a time from the rows the last round added, what those need:
   the parents of a row it lacks or that is in question (a follower points
   at its parent too), and the followers of one it lacks or that is in
   question.
3. **The difference.** What it keeps and lacks — or holds as a version
   older than a change of the batch, or refused — is sent; the rows in
   question it does not keep are taken back.

What a row a replica holds points at is what the version it holds points
at: the row as it is, unless it changed since — then what the first change
after that version recorded it pointed at before, whether that change waits
beyond the batch, is parked, or was planned while the replica kept the older
version for something that still needed it. That version's parents stay as
long as it does. Rows that cannot go (section 4.3) are never kept, nor any
row that points at one; a plan that would send such a row runs again with it
among them, and a row set aside for pointing at one is tried again once that
row changes.

The plan is worked out in the database, in temporary tables of the relay's
connection, set by set, and read a page at a time by what records it: a
plan, however many rows it sends — a snapshot of a large replica, a group of
thousands given to many replicas — is never in the relay's memory whole.
Every note of the batch is planned together, in one plan: a part planned
apart could undo what another sends — take back a parent a child planned
elsewhere still needs.

**What a replica's version holds.** Every plan reads one snapshot of the
database. When a replica is recorded as holding a row, the newest note of
that row the snapshot sees is recorded with it. Writers of one row take
turns on its lock, so a row's notes are numbered in the order its changes
committed: a later note of it is a change the replica lacks, and sends the
row again; an earlier one is a change it holds, and sends nothing. A change
committed *after* the plan read — by a transaction that started earlier,
or took its transaction id first — is never missed, and a change the
replica already received is never sent twice. The numbers are a sequence's,
so a database dumped and restored, or moved to another cluster, keeps
them: its transaction ids start over, and nothing depends on them. (Logical
replication carries no sequences: a database moved by it has them set, as
for every serial column, before its relay runs.)

### 4.3 Loading

Every row the delta sends is read once and encoded once by its codec, however
many replicas get it, in the plan's snapshot, a chunk of rows at a time: what
the relay holds in memory is a chunk, and one message being filled per
replica, however much it sends. Filling many replicas' messages at once, it
ends them early past a bound — never between a table's deletes and its
upserts, which a unique value moving from one row to another needs in one
message. A row that cannot go — it does
not encode, or no message could carry it — is planned around: the plan runs
again with it blocked, so it is not sent, what points at it is not sent
either, and a replica holding an older version of it has that version taken
back with whatever points at it there. A replica never keeps a version the
planner can no longer reason about; each of those rows is quarantined and
comes back once a fix lets it through.

### 4.4 Recording, then publishing

Each replica's rows are packed into messages ([`envelope::Down`]) under the
transport's size limits: parents' rows first, then deletes, children first.
Every message goes out as one [`envelope::ToReplica`], whatever it carries, so
a transport moves bytes and never looks inside. A row too large for any
message is set aside at loading, like one that does not encode (section 6).
Each row travels with its key, so a replica that cannot even decode a row can
still say which one it was.

**Every message is numbered, and chained.** The relay draws one number per
message from a PostgreSQL sequence, and gives them out in the order the
messages go. A sequence never goes backwards and is not rolled back with a
transaction, so no two messages ever share a number. A replica applies
nothing at or below the last number it applied: a message delivered twice
changes nothing. Each message also carries a random *nonce*, and names the
one sent the replica before it, by number and nonce (`previous`,
`previous_nonce`); a replica applies a message only when it follows the last
one it applied, so one that follows a message it never got — lost on the
way, or the replica's database restored from an older copy — is a *gap*: it
applies nothing more until a snapshot begins, tells the server the newest
message it could not follow, and is provisioned again, unless a snapshot
that began after that message is already on its way. Messages carry the
*epoch* too, a random id of the server's installation: a new epoch always
begins with a snapshot.

Every message is sealed for its replica: an HMAC of its replica, epoch,
number and nonce under a secret of the epoch (`sync.epoch`). The replica
gives the seal back, as it came, when it says what it applied, so the
server knows it sent that message. A server database restored from an
older copy numbers again from where the copy left off, but draws other
nonces: its replicas apply none of it, and say what they applied — a
message the server sealed and has no record of sending. The server then
takes a new epoch and provisions every replica again; one restored from
before its last new epoch hears of an epoch it never had, from replicas it
last heard follow its own, and provisions each again. A replica that says
it applied less than it said before was restored itself, and is
provisioned again.

A provisioning (a *snapshot*) sends everything the replica should hold, then an
[`envelope::EndSnapshot`]. On the end, the replica deletes every row of those
tables the snapshot did not name. A snapshot replaces what the replica was
owed: its outbox is dropped when the snapshot is written.

**Recorded before it is sent.** Each message is written to its replica's
outbox (`sync.outbox`) as it is filled, and what it carries is recorded in
`sync.held` with its number, in the same transaction that marks the notes
done. Only once that commits does the relay publish: each replica's outbox,
in order, a message leaving it once the transport took it — up to
`Settings::concurrency` replicas at once, so one slow to take its messages
holds back no other. So the server
decides what a replica will hold before anything goes, and nothing goes that
is not recorded: a relay that dies after the commit loses nothing — whoever
runs the relay next sends the outbox — and one that dies before it sent
nothing. A message keeps its number, and its transport id, however often it
goes; a database restored from an older copy numbers its next messages again,
and each of those has an id of its own (`{epoch}-{sequence}-{nonce}`). What a
message carries is stored once, in `sync.bodies`, however many replicas'
messages carry it — the rows of a change every replica gets are one body —
under each replica's own head and tail: a protobuf message is its fields one
after the other, so the bytes the transport is given are the message's own.

The record transaction runs on the connection that holds the relay's lock. If
that connection is lost, PostgreSQL releases the lock and rolls back the
record together. So a relay that lost its lock can never commit over the one
that took it.

A replica the transport cannot reach keeps its messages in its outbox, in
order, while every other one is sent to: nothing is planned again for it.
Should its outbox outgrow `outbox_messages` or `outbox_bytes` behind any
snapshot it begins with, it is dropped and the replica provisioned again
instead — an alarm, in its `last_error`.

### 4.5 Applying, on the replica, and saying so

The replica applies a message in one transaction ([`replica::apply`]):

- Each row overwrites its copy, except the columns the replica owns, whatever
  its version: the chain orders the server's messages, so the last applied is
  the server's latest, even after a restore or a clock set back.
- A row it had retired is offered again and becomes live.
- A row it is told to let go of is deleted or, if a row of its own still points
  at it, retired (its `RETIRED` column set). A sale keeps its customer.

Then it tells the server the last message it applied — its number, its nonce
and the seal it came with. Until it does, what the server recorded in 4.4 is
only *on its way*: every held row remembers the message that carried it, and
counts as held once the replica has acknowledged that message. This is what keeps the progress figures
(section 6) honest: they count what the replica confirmed, never what was
merely sent.

---

## 5. The life of a change, replica to server

A replica writes its own rows (records, and its columns of two-way tables).
A trigger notes each write in the writing transaction (`sync_notes`: one
note per row — the table and the key — counting its writes), so no write path
can forget, and a row written a thousand times is one note. The replica's upward loop gathers
the noted rows **as they are now** — a row edited twice goes up once, with its
last version — into an [`envelope::Up`], and hands it to the host's uplink. A
row that is gone by then goes as its key, but only for a table keyed by its
replica (`ORIGIN`): history a replica pruned before it went up is not the
server's to delete.

Every send of an upload draws a random number of its own, a resend included,
stamped on its notes in the transaction that makes it. The server answers
every upload it filed with a `Filed` message, after its transaction commits,
and the replica lets go of the notes that send carried — a note sent again
since waits for the answer to that send, and a row written since goes up
again, so a late answer never forgets a write it did not carry. An answer
that arrives before the replica recorded the send counts all the same; one
meant for another replica, or for this one before it was restored, names
nothing of its. An upload the server does not answer
goes again after a while. An upload stays within the transport's size; a row
that can never fit, or that the transport refuses for good, is set aside and
counted in the backlog until the replica runs again — found by sending half of
what was refused, and half again, and blamed only when the transport took some
other message meanwhile; a transport that refuses everything sets nothing aside
and is asked again after a backoff. So the replica always
knows what it wrote that has not reached the server yet.

The same `Up` carries everything else a replica has to tell the server: the
rows it could not take, the last message it applied, and, hourly, its
digests. A replica may split its writes into **lanes** — a table written often
on a stream of its own — and the first lane carries what the engine itself
says.

The server files an upload under **the replica the message came from, and
what the server knows about it**, never what the rows claim:

- a record is kept from its first delivery, or the newest report wins when it
  has a `VERSION`;
- a two-way table takes only the columns the replica owns, and only of a row
  the server has and records the replica holding: an edit of one it forgot
  the replica holds while it provisions it again waits; one of a row it
  deleted or is taking back is dropped, the server's change winning; one of
  a row the replica was never sent is refused — a replica never creates a
  two-way row;
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
| Encoding a row, on the server | leaves the row out of what is sent and recorded, with every row that points at it there, and **quarantines** them for every replica that was owed them. A replica holding an older copy has it taken back, with what points at it | quarantined rows are noted again when the relay starts and on a timer, as of no transaction — so each goes only where it is missing; a row leaves the quarantine once its replica confirms it holds it, or once it is owed to no one |
| The transport, for one replica — unreachable, refusing a message for good, or not answering within `send_timeout` | that replica waits out a backoff (`sync.replicas`, the refusal in `last_error`); its outbox keeps the message and every one after it, in order; every other replica carries on. An outbox past its bound, or a message refused for good three times in a row, is dropped and the replica provisioned again, with an alarm | sent when its backoff ends |
| A row larger than any message may be | left out at loading and **quarantined**, as one that does not encode | as above, once a fix makes it smaller or the limit larger |
| A replica, taking a row or letting one go | rows refused together are tried one by one, so only the refused row is left out; the replica notes it in the same transaction (`sync_rejections`), with the message that refused it, and reports it in an `Up` before it says it applied that message; the server records the row *refused* — still held for what takes rows back, so a delete still goes, not for progress or digests — and quarantines it. A rejection of a message other than the last that carried the row is ignored | a fix deployed on either side lets the retry through |
| The server, filing an uploaded row or delete | the row, or the delete, is kept **as it came**, bytes and all, in `sync.inbox` (the replica may have let go of it since); the rest of the upload is filed. The inbox keeps one part per row, the newest the replica sent: a later upload of the row takes its place, or drops it once filed, so what is filed again never rolls a row back | filed again when the relay starts and on a timer, a page at a time beside the passes |
| The server, filing an upload as a whole | every row and delete of it is kept in `sync.inbox`, and it is answered, so the replica does not send it again for nothing | as above |
| The server, with a replica's part of the inbox full (`Settings::inbox`) | that replica's uploads that would add to it fail as transient: they wait on the replica, unanswered, with an alarm | filed again, a page at a time beside the passes, once a fix files what is kept; what a sender the host no longer knows uploaded waits `prune_after`, then is dropped with an alarm |
| The transport, losing the last messages it was sent | a replica that leaves them unconfirmed while nothing is sent to it and nothing confirmed by it is provisioned again after `unconfirmed_after`, then after twice as long while it stays silent | by itself |
| Anything nobody foresaw | see below | |

### The backstop: digests

Every so often, each replica reports a **digest** of what it holds of every
table: how many rows, and a fingerprint of their keys (FNV-1a over the sorted
keys; rows it retired or could not take are left out). The server compares it
with what it records the replica holds (leaving out the rows the replica
refused, as it does). It only compares once the replica has applied every message recorded
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
| `catching_up` | changes are on their way to it (noted and not yet planned, held back by a parked note that may concern it, or in a message it has not confirmed) or from it (its backlog) |
| `unreachable` | sends to it fail; it is tried again after a backoff |
| `awaiting_fix` | something of its own is set aside until a fix: a row in quarantine, a parked note owed to it alone, an upload of its in the inbox |

It never counts the rows a replica should hold: it reads only what the engine
records, so it costs little for a fleet — a few milliseconds for 200 replicas
holding 3 million rows, while they are caught up; the rows still on their way
are counted one by one, about a fifth of a second when every replica has all
of them unconfirmed (`bench_the_dashboard_over_200_units_holding_3m_rows`, on
a 32-core workstation) — and it is exact all the same. A change a replica lacks is in one of three places — a note not
yet planned, a message it has not confirmed, or set aside — and each is
recorded. It counts every replica per state and lists the worst up to a
limit; [`plan::Engine::listing`] pages through the rest. Per table, the same
figures over the scope's replicas say which tables are not where they should
be yet. Every read is one snapshot, and carries the server's `now`.

[`plan::Engine::progress`] answers how far along: for a few replicas, per
table:

| Figure | Meaning |
|---|---|
| `should` | rows its rules say it should hold |
| `holds` | rows it should hold and **confirmed** it holds |
| `outdated` | of those, rows changed since (a newer version is on its way or owed) |
| `in_flight` | rows it should hold, sent and not confirmed yet |
| `to_remove` | rows it was sent and should no longer hold |
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
When nothing waits for a fix, the engine is healthy — the normal state; a
replica out of reach is listed, and waited for.

---

## 7. Guarantees, and what they rest on

The engine guarantees that:

1. **No committed change is lost.** It is noted in the same transaction, and a
   note is only done in the transaction that records what it asked for and
   writes the messages that carry it, which are sent until the transport
   takes them.
2. **A replica converges to what it should hold.** At-least-once delivery,
   idempotent application, and the digest backstop.
3. **A replica never holds a row without the rows it points at.**
4. **A replica never goes back in time.** The messages of an epoch are
   chained: the replica applies each once, in order, so the one applied last
   is the server's latest.
5. **One failure holds back only what it is about.**

They rest on:

- **One relay at a time.** The server's runner holds a PostgreSQL advisory
  lock while it relays, and records each pass on that lock's connection.
- **A transport that keeps its contract**
  ([the protocol, section 2](protocol.md#2-what-the-transport-must-provide)).
  Without it, a failed message could be retried after the ones behind it, or
  lost until the safety net of section 6 catches it.
- **Transactions.** `record` and `file` run in a transaction (they use
  savepoints); `apply` runs in one so a message applies whole or not at all.
- **A monotonic version column**, on a record a replica edits: the replica
  must stamp it with a clock that never goes behind the value it replaces,
  or its newer report is filed as older and dropped.

---

## 8. What is the host's, and what is the engine's

The host writes its tables, a struct per table, the codec list, where the
replicas are, how bytes travel each way (a [`run::server::Transport`] and a
[`run::replica::Uplink`]), and, when it needs to, what it knows about a
sender ([`run::server::Senders`]). [Build your own](guide/tables.md) walks
through each.

Everything else is the runners' ([`run`]): on the server, the lock, the
passes and their backoff, retrying what was set aside, pruning, the alarms,
filing uploads and answering them; on a replica, applying each message in one
transaction, and the upward loop. The step functions they are built on —
`Relay::record`/`publish`, `replica::apply`, `replica::outbox` and
the rest — are the engine's own: a host drives the engine through the
runners.

---

## 9. The parts

| Module | What it holds |
|---|---|
| [`row`] | a table's declaration (`Row`, `Mirror`, the key and version types), the check of a table against its row, and the writes both sides make: upsert (a record with a version: newest wins), insert-first-wins, retire |
| [`wire`] | the codec: a table's rows encoded as its own message, and applied by the rules its row declares |
| [`envelope`] | the messages between server and replica, their limits, the packer, the fingerprint (the wire is `proto/sync.proto`, and [the protocol](protocol.md) says what a replica in any language must do) |
| [`plan`] | the planner and the notes (server) |
| [`relay`] | one pass (record, then publish), the outbox, parking, quarantine, provisioning a silent replica again (server) |
| [`filing`] | what a replica sends up, filed: its rows, its rejections, its acknowledgement, its digests, its backlog; the inbox (server) |
| [`status`] | what the engine answers about itself: the overview, health, progress, where a row is (server) |
| [`replica`] | applying, sweeping a snapshot, uploading, rejecting, digests (replica) |
| [`dialect`] | the few things PostgreSQL and SQLite spell differently |
| [`run`] | the runners: [`Server`] (the relay under its lock, retries, filing and answering uploads, and the transport it sends through) and [`Replica`] (receiving, and the way up) |

| File | What it is |
|---|---|
| `sql/postgres/` | the server's schema, one step per version |
| `sql/sqlite/` | a replica's schema, one step per version |
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
- **A storm** runs the same walk through the real relay while the transport
  fails a fifth of the sends — some for good — and loses messages; the relay
  dies between its record and its sends, or loses its lock mid-pass; replicas
  take part of what waits for them, die before acknowledging, or are not
  heard. Replicas are modelled from the protocol (`src/model.rs`): epochs,
  chaining and gaps, snapshots, rejections. Each time the system settles,
  every replica holds exactly what the engine records; once the faults stop
  and the fix is deployed, exactly what it should, with provisionings rare.
- **Races and restores**: a transaction that commits after a plan read past
  it, a pass recorded and never sent, a lock lost mid-pass, a send cut short
  before a row leaves the replica's scope, a message lost, a server or a
  replica restored from an older copy.
- **The replica side** runs on SQLite: messages from the past, rows it cannot
  decode, tables it does not have, digests.

Run them with `cargo test -p magic_sync --features postgres` (PostgreSQL
through `DATABASE_URL`; each test gets a database of its own) and
`cargo test -p magic_sync --features sqlite`.

[`row::Row`]: api/server/magic_sync/row/trait.Row.html
[`wire::Codec`]: api/server/magic_sync/wire/struct.Codec.html
[`envelope::Down`]: https://github.com/rsansores/magic_sync/blob/main/magic_sync/proto/sync.proto
[`envelope::EndSnapshot`]: https://github.com/rsansores/magic_sync/blob/main/magic_sync/proto/sync.proto
[`envelope::ToReplica`]: https://github.com/rsansores/magic_sync/blob/main/magic_sync/proto/sync.proto
[`envelope::Up`]: https://github.com/rsansores/magic_sync/blob/main/magic_sync/proto/sync.proto
[`replica::apply`]: https://github.com/rsansores/magic_sync/blob/main/magic_sync/src/replica.rs
[`filing::file`]: https://github.com/rsansores/magic_sync/blob/main/magic_sync/src/filing.rs
[`Error::is_transient`]: api/server/magic_sync/enum.Error.html#method.is_transient
[`plan::Engine::overview`]: api/server/magic_sync/plan/struct.Engine.html#method.overview
[`plan::Engine::health`]: api/server/magic_sync/plan/struct.Engine.html#method.health
[`plan::Engine::progress`]: api/server/magic_sync/plan/struct.Engine.html#method.progress
[`plan::Engine::listing`]: api/server/magic_sync/plan/struct.Engine.html#method.listing
[`plan::Engine::row_progress`]: api/server/magic_sync/plan/struct.Engine.html#method.row_progress
[`plan::Replicas`]: api/server/magic_sync/plan/struct.Replicas.html
[`replica::progress`]: api/replica/magic_sync/replica/fn.progress.html
[`run::server::Transport`]: api/server/magic_sync/run/server/trait.Transport.html
[`run::replica::Uplink`]: api/replica/magic_sync/run/replica/trait.Uplink.html
[`run::server::Senders`]: api/server/magic_sync/run/server/trait.Senders.html
[`run`]: api/server/magic_sync/run/index.html
[`row`]: api/server/magic_sync/row/index.html
[`wire`]: api/server/magic_sync/wire/index.html
[`envelope`]: api/server/magic_sync/envelope/index.html
[`plan`]: api/server/magic_sync/plan/index.html
[`relay`]: https://github.com/rsansores/magic_sync/blob/main/magic_sync/src/relay.rs
[`filing`]: https://github.com/rsansores/magic_sync/blob/main/magic_sync/src/filing.rs
[`status`]: api/server/magic_sync/status/index.html
[`replica`]: api/replica/magic_sync/replica/index.html
[`dialect`]: https://github.com/rsansores/magic_sync/blob/main/magic_sync/src/dialect.rs
[`Server`]: api/server/magic_sync/run/server/struct.Server.html
[`Replica`]: api/replica/magic_sync/run/replica/struct.Replica.html
