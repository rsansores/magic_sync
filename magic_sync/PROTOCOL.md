# The replica protocol

What a replica must do to be kept in sync by this engine, whatever language it
is written in and whatever database it keeps its rows in. The engine's own
replica (`src/replica.rs`, on SQLite) is one implementation of it; this
document is the contract that one follows, and any other must.

The messages are in [`proto/sync.proto`](https://github.com/rsansores/magic_sync/blob/main/magic_sync/proto/sync.proto). Generate code for
your language from it; the engine's own types are generated from it too, and
a test fails while they differ.

The words **must**, **must not** and **should** mean what they mean in RFCs.

---

## 1. The parties

- The **server** owns the truth. It decides which rows each replica should
  hold, sends the difference, and records what each replica holds.
- A **replica** holds a copy of some of the server's tables, and may write
  tables of its own that go up to the server. It is identified by a UUID the
  server knows.
- The **transport** carries messages between them. The engine does not depend
  on which one, but it requires what section 2 lists.

A replica never decides what it should hold. It applies what it is sent,
reports what it could not apply, and reports what it holds.

---

## 2. What the transport must provide

The server sends one kind of message, `ToReplica`, which carries a `Down`, an
`EndSnapshot` or a `Filed`. A replica sends one kind, `Up`, on one or more
lanes (section 7). A transport carries bytes and nothing else: it never needs
to know what is inside. Whatever it is — a broker, HTTP over a queue of your
own, files on a USB stick — it **must** provide, in both directions:

1. **Nothing it took is lost.** Once the transport accepted a message, it
   delivers it: while the other end is unreachable, and across a restart of
   either end or of the transport itself. It stores, then forwards.
2. **In order.** A replica's messages are delivered in the order the server
   sent them, and a replica's `Up`s on one lane reach the server in the order
   it sent them.
3. **One at a time, down.** The next message to a replica is not delivered
   until the replica acknowledged the previous one (5.1).
4. **At least once.** A message that was not acknowledged is delivered
   again. Each side therefore treats every message as one it may have seen
   before (4.1, 7).
5. **An acknowledgement names its message.** It releases that message and no
   other: one that arrives twice, or late, never releases a message the
   replica has not applied.

With NATS JetStream, that is a stream on file storage, and per replica a
durable pull consumer with explicit acknowledgements and
`max_ack_pending = 1`; up, a stream the server consumes per lane in order.

A message the transport carries has a size limit of its own; the server keeps
every message within it, and a replica must keep every `Up` within it
(section 7).

A **transport acknowledgement** (section 5.1) is what makes the transport
deliver the next message. It is not the same as telling the server what was
applied (section 5.2).

A transport that loses a message anyway costs time, not data: the next
message names the one before it, so the replica knows it missed one and is
provisioned again (4.1); a replica that leaves messages unconfirmed while
nothing newer goes to it is provisioned again (section 9), an upload the server did not answer is sent again
(section 7), and a digest that disagrees provisions the replica again
(section 8). That is the engine's safety net, not a licence: until it acts,
the replica holds less than it should.

---

## 3. Rows and keys

Each table travels in **its own encoding**, agreed between the server and the
replica for that table: either a protobuf message with one field per column,
or a JSON object of its columns (the column names as keys). The engine's
messages carry those rows as opaque bytes, so this protocol never changes when
a table does.

A row's **key** is its primary key, as text, in key order:

- a UUID part: lowercase, hyphenated (`0196…-…`);
- an integer part: decimal;
- the parts joined by `|` where a single text is needed (`a1b2…|3`).

This is the one form keys are compared in, and every key a side writes into
a message **must** be in it. A side reading a key parses each part as its
column types it and writes it back in this form before using it (a UUID that
came uppercase or without hyphens is the same key); a key that does not
parse is malformed.

A table that travels as **JSON** is a JSON object with one member per column,
named as the column, each value as the column's type writes it in JSON —
except a 64-bit integer, which is its decimal text (`"9007199254740993"`): a
JSON number would be rounded by a reader that holds numbers as doubles. A
decimal is its text, a UUID its canonical form, an instant RFC 3339.

---

## 4. Applying what the server sends

### 4.0 The protocol version

Every `ToReplica` and every `Up` carries `protocol`, the version of this
document its sender speaks (now `1`).

- A replica **must** set `protocol` on every `Up` it sends.
- A replica that receives a `ToReplica` of a **later** protocol than its own
  **must not** act on it and **must not** acknowledge it to the transport: it
  waits to be updated, and the message is delivered again then. Every message
  behind it waits too, which is right, because they speak the same version.
- The server does the same with an `Up` of a later protocol than its own,
  and drops an `Up` of protocol 0 as malformed.

A message that is **malformed** — bytes that do not decode as a `ToReplica`;
one of protocol 0 (unset), or of the replica's own protocol or earlier whose
body it does not know, or that has none; numbers no server sends (4.1) —
carries nothing a replica can act on: it **must** apply none of it, **should** log it, and
**must** acknowledge it, so it does not hold back the queue. The digests
(section 8) find whatever it carried, if it mattered.

What changes the protocol's number: a new kind of body, or a field whose
meaning changes. What does not: a new field. A side **must** ignore fields it
does not know, so a message with a field added means to a side that predates
it what it meant before.

### 4.1 `Down`: rows to hold, rows to let go

A replica **must** apply a `Down` in one transaction: either all of it, or
none of it.

**The number comes first.** Every `Down` carries a `sequence` and an
`epoch`, and `previous`: the sequence of the message the server sent this
replica before this one, in this epoch — 0 on a message that begins a
snapshot (4.2), which follows nothing. The replica keeps the
`(epoch, sequence)` of the last message it applied, and checks, in order:

- `sequence` 0 or above 2^63 − 1, an `epoch` that is not a UUID, a
  `previous` at or past `sequence`, or a `previous` of 0 on a message that
  begins no snapshot: no server numbers a message so. The message is
  malformed (4.0).
- the same `epoch` and a `sequence` **at or below** the last one: the message
  changes nothing. The replica **must not** apply it, and **must**
  acknowledge it to the transport: it is a redelivery, or the server's
  database was restored from an older copy and is numbering again. Either
  way the replica **should** tell the server the last message it applied
  again (5.2), so a restored server hears of it.
- `previous` 0: the message begins a snapshot, and the replica applies it,
  whatever it applied before — of this epoch, or of another, which is a
  server installed again or restored: it keeps that epoch's numbers from
  now on. A new epoch always begins with a snapshot.
- otherwise `previous` **must** be the last message applied, of the same
  epoch. If it is not, the replica missed one — lost on the way, or the
  replica's own database restored from an older copy — and has a **gap**: it
  **must not** apply the message, **must** acknowledge it to the transport,
  and from then on applies nothing but a message that begins a snapshot,
  saying it has a gap to the server (5.2) until one does. The server
  answers with one.

Then the replica applies `tables` **in the order given**. The server puts
parents' rows before their children's, and children's deletes before their
parents', so in that order no row ever points at a row the replica lacks.

For each `Table`:

A table the replica holds from the server is one the server writes: a
catalog, or a table written on both sides. Rows of any other table — a
record of the replica's own, a table it does not know — are rejected, and
their deletes ignored.

1. **Upserts.** Each entry of `upserts` is one row, encoded as the table's
   message; `keys[i]` is the key of `upserts[i]`, and there **must** be one
   key per upsert. For each row:
   - insert it, or overwrite the stored row with the same key;
   - **unless** the table has a version column (an `updated_at`) and the
     stored row's version is **newer** than the incoming one: then keep the
     stored row (an equal version is applied). Versions compare as what they
     are — numbers as numbers, instants as instants, whatever text form each
     was stored in;
   - columns the replica owns in a table written by both sides are **not**
     overwritten;
   - a row the replica had retired (4.3) becomes live again.
2. **Deletes.** For each key in `deletes`: delete the row. If a row of the
   replica's own points at it — through a foreign key that would refuse the
   delete, whether checked at once or at commit — retire it instead (4.3);
   where the table has nothing to retire it by, or the database refuses to
   let it go, keep it and record a rejection (section 6).

**A row the replica cannot take, or let go of, does not fail the message.**
If a row does not decode, if the replica's database refuses it for its data
(a constraint, a trigger), or if the replica does not know the table at all,
the replica **must** leave that row as it was, apply everything else, and
record a **rejection** (section 6) in the same transaction. Nothing a message
carries fails it. Only a failure of the replica's own storage (the
database is unavailable, the disk is full) fails the message: then the replica
**must not** acknowledge it, and it will be delivered again.

### 4.2 Snapshots

When the server provisions a replica from scratch, it sends everything the
replica should hold in `Down` messages that carry a `snapshot_id`, then an
`EndSnapshot` naming the tables the snapshot covered. A snapshot of nothing
is its `EndSnapshot` alone.

- A snapshot **begins with its first message**, the one whose `previous` is
  0 — the `EndSnapshot` itself, for a snapshot of nothing. It supersedes the
  snapshot in progress, if any: the replica forgets what that one named. The
  messages after it follow it, as any do (4.1).
- While applying a `Down` of the snapshot in progress, the replica **must**
  remember the key of every row it carried, per table — the ones it refused
  as well: a refused row keeps whatever the replica had of it.
- On `EndSnapshot`, the replica **must** let go of every row of the named
  tables that the snapshot did not name (delete, or retire as in 4.3),
  children before parents: **in reverse** of the order the tables are
  listed. A row it refused was named, so it is never swept. The rejections
  it recorded for those tables before the snapshot began are forgotten — the
  snapshot sent again what it should hold — and the ones it recorded since
  stand. Then the snapshot is no longer in progress.
- A message that follows the last applied and names a snapshot other than
  the one in progress — one that ended, or was superseded — is one no
  server sends: malformed (4.0).
- `EndSnapshot` carries `sequence`, `epoch` and `previous`, and follows the
  same rules as `Down` (4.1). It needs no count of what the snapshot sent:
  its messages are chained, and one lost is a gap.

### 4.3 Retired rows

A replica that must let go of a row that one of its own rows still points at
(a receipt that names an order) keeps the row and marks it retired: a column
the replica owns, set to when it was retired. Retired rows are out of
everything the replica offers its users, but stay for what points at them. A
retired row the server sends again becomes live.

A row of a table written on both sides that the replica edited, and whose
edit the server has not filed yet (section 7), is retired too when it is let
go of, rather than deleted: the edit still goes up, as the row is. Once the
server files it (4.4), the replica lets go of the row. Where the table has
nothing to retire it by, the replica refuses to let it go (section 6), and
the server sends the delete again later.

### 4.4 `Filed`

The server answers each upload (section 7) once it has filed it. On `Filed`,
the replica **must** forget the notes those uploads carried — the ones whose
latest send carried that number, and no other: a note sent again since is
forgotten only on the answer to that send, and a row written since the send
goes up again. A number none of its sends carries — meant for another
replica, or for this one before it was restored — names nothing. The answer
may arrive before the replica recorded that the transport took the upload:
it counts all the same.

---

## 5. Acknowledging

There are two acknowledgements, and a replica gives both.

### 5.1 To the transport

After the transaction that applied a message **commits**, the replica
acknowledges the message to the transport, which then delivers the next one.
Never before the commit: a replica that acknowledged and then failed to commit
would never be sent that message again.

### 5.2 To the server: what was applied

The server counts a row as held by a replica **only once the replica says it
applied the message that carried it**. Until then, the row is "on its way".
That is what makes the server's progress figures true.

The replica **must** tell the server the `(epoch, sequence)` of the last
message it applied, in an `Up` that carries them in `epoch` and `sequence`.
It may do so after every message, or once for several (a replica that
applied messages 5, 6 and 7 reports 7). It should do so within seconds of
applying, and again whenever it applied something new, or was delivered a
message at or below the last it applied. `sequence` 0 says it has applied
nothing yet. The server ignores a report of another epoch, one behind the
last it heard, and one past the last message it records sending the
replica — except one past every number it ever drew in its epoch: that says
its own database was restored from a copy older than what the replica
applied. The server then takes a new epoch, drops what it was about to
send, and provisions every replica again.

A replica with a gap (4.1) **must** say so, in an `Up` with `gap` set and
its last applied `(epoch, sequence)`, and set `gap` on every `Up` it sends
until a snapshot begins. The server provisions it again, unless a snapshot
written for it since that message is on its way.

---

## 6. Rejections

A row the replica could not take, or could not let go of (4.1), is a
**rejection**: the table, the key — **required**, in the form of section 3:
read off the row, or from `keys[i]` if the row did not even decode — why,
and the `(epoch, sequence)` of the message that carried the row, or its
delete. A row whose key cannot be told is logged and not reported.

- The replica **must** record a rejection in the same transaction that left
  the row out, so it cannot be lost.
- It **must** report each rejection to the server in an `Up` (`rejected`),
  **no later than** the `Up` that says it applied the message that refused
  it (5.2): the server counts the rows a message carried as held once it
  hears the message was applied.
- It **must** keep the rejection until it takes the row (a later message
  carries the row and it applies) or lets go of it (a delete, or a snapshot
  that does not name it): a digest (section 8) leaves rejected rows out, as
  the server leaves out the rows recorded refused. It reports only the
  rejections of the epoch it follows; a snapshot of a new epoch clears the
  others.

On a rejection of the last message that carried the row, the server
records the row **refused**: still held for every decision that takes rows
back — a delete still goes when the row leaves the replica's scope, and a
refused delete is sent again — and not held for progress, or for digests.
The row is set aside for that replica, and sent again, or its delete, when
the relay restarts or on a timer, which is after a deploy on either side
has had the chance to fix the cause; it is taken once the replica applies a
later message that carries it. A rejection of a delete re-records the row
the server took back. A rejection of a message older than the last one that
carried the row, or of another epoch, names a version that no longer
matters, and is left out. The key is read as its table's key (section 3)
whatever its spelling. A rejection of anything else — a table the server
does not send, no key or one that is not the table's, a message never sent
the replica — is malformed and left out. Nobody repairs the row by hand.

---

## 7. Sending rows up

For a table the replica writes and the server mirrors:

1. **Note** every insert, update and delete in the writing transaction: the
   table and the key — on an update that changes the key, the old key too,
   which is gone. (A trigger is the only way no write path can forget.)
   What the replica writes while applying what the server sent is not
   noted: it is the server's already. A row of a table written on both sides
   begins on the server; the replica edits only its own columns of it, and
   only those edits go up: it **must** refuse an insert of one, an update
   of a column it does not own, and a delete unless the table is keyed by
   the replica. A record without a version is history, written once.
2. **Upload** noted rows, as they are **at upload time**, in an `Up`:
   - each row in `tables[].upserts`, encoded as its table's message;
   - a row that is gone by now, as its key in `tables[].deletes`, but only
     for a table keyed by the replica that wrote it: history a replica pruned
     is not the server's to delete;
   - one key per row in `tables[].keys`;
   - upserts parents first, in the order the tables point at one another,
     then deletes children first;
   - in `uploads`, the number of this upload: a **fresh** one on every send,
     resends included, drawn **at random** from 1 to 2^63 − 1, so an answer
     meant for another replica, or for this replica before it was restored
     from an older copy, never names its notes;
   - within the transport's size limit: what does not fit waits for the next
     upload. A row that can never fit, or that the transport refuses for
     good, is set aside — kept, counted in the backlog, never sent until the
     replica runs again with a build that may send it.
3. **Wait for `Filed`** (4.4) naming that upload, then forget its notes. An
   upload not answered after a while (minutes) should be sent again; the
   server files the same rows idempotently.

The server files an upload under **the replica it came from** (the transport
says which), whatever the rows themselves claim: a row of a table keyed by
its replica that names another replica there is malformed, and left out. A
row the server cannot file, or a delete its database refuses, is kept on the
server, not lost, and filed later; so are an upload's rows whose filing fails
as a whole for a reason a resend cannot cure. Either still counts as filed
for `Filed`. What the upload says of the replica — rejections, the last
applied message, a gap, digests, the backlog — is filed apart from its rows,
so neither holds back the other. Deletes of a table not keyed by the replica,
and keys that are not the table's, are malformed and left out.

A replica **may** say what it wrote and the server has not filed yet, in
`Up.backlog`: per table, rows not sent yet (`unsent`), rows sent and not
yet `Filed` (`unfiled`), and rows set aside (`set_aside`). The server keeps the last one it was told, with the
time, and shows the replica as catching up while it is not empty. Say it
when it changes — at once when it empties — and not on every write; say it
again now and then while it is not empty, and to a server whose epoch
changed, either of which may have lost it. An `Up` without `backlog` leaves
the last one standing. A replica that never says leaves the server with
nothing to show, and nothing breaks.

A replica may send its writes on several **lanes** — streams of `Up`s the
transport keeps apart, each carrying the writes of some of its tables, so a
table that writes often never waits behind the rest. Every table that goes up
is on exactly one lane. Rejections, the last applied message, digests and
the backlog go on the first. The transport keeps each lane in order
(section 2): the backlog an earlier `Up` carried must not arrive after a
later one's.

One `Up` may carry several of these at once: rows, rejections, digests, the
backlog, and the last applied message.

---

## 8. Digests

From time to time (hourly is reasonable), a replica **should** report what it
holds of every table the server sends it, in `Up.digests`, together with the
`(epoch, sequence)` of the last message it applied:

- `table`: the table;
- `count`: how many rows it holds, **leaving out** rows it retired (4.3) and
  rows it rejected (6);
- `fingerprint`: FNV-1a, 64 bits, over the keys of those rows as text (section
  3), **sorted** (byte order), each followed by a newline (`\n`):

```
hash = 0xcbf29ce484222325
for each key, sorted:
    for each byte of key + "\n":
        hash = hash XOR byte
        hash = hash × 0x100000001b3   (mod 2^64)
```

Test vectors: no keys → `0xcbf29ce484222325`; the single key `a` →
`0x089bdc07b544e7b2`.

The server compares a digest only when the replica has applied every message
recorded for it (the same epoch and last sequence). If they differ, it
provisions the replica again from scratch (4.2). Digests that differ again
within a day of the provisioning they caused are a bug no provisioning cures:
the server logs them and does not act on them. A digest of a table the server
does not send the replica — a later build's, or one it no longer sends — is
skipped. Two digests of one table are malformed: none of that `Up`'s digests
is compared. A replica that skips digests loses only
this safety net.

---

## 9. What the server guarantees in return

- Every change committed on the server reaches every replica that should hold
  the row, as the row is at sending time.
- A row is sent with every row it points at that the replica lacks, and is
  taken back only once nothing the replica keeps points at it.
- Every message carries a number of its own, and the numbers a replica is
  sent rise within an epoch in the order it is sent them; each names the
  one before it, so a replica can tell it missed one. What a message
  carries is recorded before it is sent, and a message that the transport
  did not take is sent again, under the same number, before anything after
  it.
- A replica that leaves the last messages it was sent unconfirmed, while
  nothing newer goes to it, is provisioned again after a while: the transport
  may have lost them.
  One that says it missed a message is provisioned again at once.
- A server whose database was restored from an older copy finds out from
  the first replica that says it applied more than that copy remembers, and
  provisions every replica again, under a new epoch. A replica restored from
  an older copy finds out from the next message it is sent, and says so.
- A row the server cannot send to a replica, or that a replica rejected, is
  set aside and sent again after a fix: it is never silently dropped.
- An upload is answered with `Filed` only after the transaction that filed it
  committed.

---

## 10. A session, start to finish

```
server                                         replica
  │  (every arrow down is a ToReplica carrying the message named)
  │  Down {snapshot s1, seq 101, previous 0, products…}
  │                                          ──▶  begins s1, applies, remembers its keys
  │  Down {snapshot s1, seq 102, previous 101, orders…}
  │                                          ──▶  applies
  │  EndSnapshot {s1, seq 103, previous 102, [products,orders]}
  │                                          ──▶  sweeps what s1 did not name
  │  ◀── Up {epoch E, sequence 103}               "applied up to 103"
  │      progress: holds 3,200 of 3,200
  │
  │  (a product is renamed)
  │  Down {seq 104, previous 103, products: 1 row}
  │                                          ──▶  applies
  │  ◀── Up {epoch E, sequence 104}
  │
  │  (a row the replica refuses: a check it has, the server lacks)
  │  Down {seq 105, previous 104, orders: 2 rows}
  │                                          ──▶  applies one, rejects one
  │  ◀── Up {sequence 105, rejected [orders 9f…: "check failed"]}
  │      the row is set aside for this replica
  │
  │                                               (writes two receipts)
  │  ◀── Up {tables [receipts: 2 rows], uploads [57]}
  │      filed
  │  Filed {uploads [57]}                    ──▶  forgets the notes upload 57 carried
  │
  │  ◀── Up {digests […], epoch E, sequence 105}   (hourly)
  │      matches what the server records: nothing to do
```

---

## 11. Conformance checklist

The engine's repository holds test vectors, each line a name and a message
in hex: `tests/golden/to_replica.txt`, what the server sends one replica
step by step — a provisioning of several messages, updates, a row the
replica's own check refuses, a delete of a row a row of its own points at,
a snapshot cut short and the one that supersedes it, a message lost on the
way and the gap after it, a new epoch, the answer to an upload; the
replica's suite (`src/replica_tests.rs`) says what a replica holds after
each step. `tests/golden/malformed.txt`, messages a replica takes as
malformed; and `tests/golden/to_server.txt`, an upload the server files.

A replica implementation is complete when:

- [ ] it reads every message as a `ToReplica`, leaves one of a later protocol
      unacknowledged, and sets `protocol` on every `Up`;
- [ ] it applies a `Down` in one transaction, tables in the order given;
- [ ] it ignores a message at or below the last applied in the same epoch,
      and says again what it applied; acknowledges without applying a
      malformed message (4.0, 4.1);
- [ ] it applies a message only when it follows the last applied, or begins
      a snapshot; one that follows another is a gap, which it says with
      every `Up` until a snapshot begins (4.1, 5.2);
- [ ] it keeps a newer stored version over an older incoming one, comparing
      versions as what they are;
- [ ] it never overwrites the columns it owns;
- [ ] it retires a row it cannot delete, and revives a retired row sent again;
- [ ] it begins a snapshot with the message that follows nothing — an
      `EndSnapshot` included — and sweeps on its end, in reverse table order;
- [ ] it leaves as it was and records, in the same transaction, a row it
      cannot take or let go of;
- [ ] it reports rejections, each with the message that refused it, before
      it says it applied that message, and keeps them until it takes the
      row or lets it go;
- [ ] it acknowledges the transport only after committing;
- [ ] it reports the last applied `(epoch, sequence)` to the server;
- [ ] it notes its own writes in the writing transaction, uploads them as they
      are within the transport's limit, gives every send a fresh random
      number, and forgets on `Filed` only the notes that send carried, as it
      carried them;
- [ ] it reports digests with the fingerprint above (optional, recommended);
- [ ] it says its backlog when it changes (optional, recommended).
