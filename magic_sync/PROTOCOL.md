# The replica protocol

What a replica must do to be kept in sync by this engine, whatever language it
is written in and whatever database it keeps its rows in. The engine's own
replica (`src/replica.rs`, on SQLite) is one implementation of it; this
document is the contract that one follows, and any other must.

The messages are in [`proto/sync.proto`](proto/sync.proto). Generate code for
your language from it; a test keeps it identical to the engine's types.

The words **must**, **must not** and **should** mean what they mean in RFCs.

---

## 1. The parties

- The **server** owns the truth. It decides which rows each replica should
  hold, sends the difference, and records what each replica holds.
- A **replica** holds a copy of some of the server's tables, and may write
  tables of its own that go up to the server. It is identified by a UUID the
  server knows.
- The **transport** carries messages between them. The engine does not depend
  on which one, but it requires the three properties in section 2.

A replica never decides what it should hold. It applies what it is sent,
reports what it could not apply, and reports what it holds.

---

## 2. What the transport must provide

1. **Per-replica order, one message at a time.** Messages to a replica are
   delivered in the order they were sent, and the next one is not delivered
   until the replica acknowledged the previous one. (With NATS JetStream: a
   durable pull consumer per replica with `max_ack_pending = 1`.)
2. **At-least-once delivery.** A message the replica did not acknowledge is
   delivered again. A replica must therefore treat every message as one it
   may have seen before (section 4).
3. **Store and forward.** A message sent while the replica is unreachable is
   delivered when it is reachable again.

The server sends one kind of message, `ToReplica`, which carries a `Down`, an
`EndSnapshot` or a `Filed`. A replica sends one kind, `Up`. So a transport
carries bytes and nothing else: it never needs to know what is inside.

A **transport acknowledgement** (section 5.1) is what makes the transport
deliver the next message. It is not the same as telling the server what was
applied (section 5.2).

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

A replica should send keys in the same form. The server also accepts a UUID
part without hyphens.

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
- The server does the same with an `Up` of a later protocol than its own.

Bytes that do not decode as a `ToReplica` at all, or one with no body, carry
nothing a replica can act on: it **should** log them and acknowledge them, so
they do not hold back the queue. The digests (section 8) find whatever they
carried, if it mattered.

### 4.1 `Down`: rows to hold, rows to let go

A replica **must** apply a `Down` in one transaction: either all of it, or
none of it.

**The number comes first.** Every `Down` carries a `sequence` and an `epoch`.
The replica keeps the `(epoch, sequence)` of the last message it applied:

- the same `epoch` and a `sequence` **at or below** the last one: the message
  changes nothing. The replica **must not** apply it, and **must** acknowledge
  it to the transport (it is a redelivery, or a message from a server process
  that lost its leadership);
- a different `epoch`: the server was installed again. The replica applies
  the message and keeps its numbers from now on;
- `sequence` 0: an unnumbered message, always applied.

Then the replica applies `tables` **in the order given**. The server puts
parents' rows before their children's, and children's deletes before their
parents', so in that order no row ever points at a row the replica lacks.

For each `Table`:

1. **Upserts.** Each entry of `upserts` is one row, encoded as the table's
   message; `keys[i]` is the key of `upserts[i]`. For each row:
   - insert it, or overwrite the stored row with the same key;
   - **unless** the table has a version column (an `updated_at`) and the
     stored row's version is **newer** than the incoming one: then keep the
     stored row (an equal version is applied);
   - columns the replica owns in a table written by both sides are **not**
     overwritten;
   - a row the replica had retired (4.3) becomes live again.
2. **Deletes.** For each key in `deletes`: delete the row. If a row of the
   replica's own points at it (the delete would break a foreign key), retire
   it instead (4.3).

**A row the replica cannot take does not fail the message.** If a row does
not decode, if the replica's database refuses it for its data (a constraint),
or if the replica does not know the table at all, the replica **must** leave
that row out, apply everything else, and record a **rejection** (section 6)
in the same transaction. Only a failure of the replica's own storage (the
database is unavailable, the disk is full) fails the message: then the replica
**must not** acknowledge it, and it will be delivered again.

### 4.2 Snapshots

When the server provisions a replica from scratch, it sends everything the
replica should hold in `Down` messages that carry a `snapshot_id`, then an
`EndSnapshot` naming the tables the snapshot covered.

- While applying a `Down` with a `snapshot_id`, the replica **must** remember
  the key of every row it took, per table.
- A `snapshot_id` is a UUID v7: a larger one began later. A `Down` of a newer
  snapshot supersedes an older one in progress: the replica forgets what the
  older one named.
- On `EndSnapshot` for the snapshot in progress, the replica **must** let go
  of every row of the named tables that the snapshot did not name (delete, or
  retire as in 4.3), children before parents: **in reverse** of the order the
  tables are listed.
- An `EndSnapshot` for a snapshot that already ended, or that a newer one
  superseded, changes nothing.
- `EndSnapshot` carries a `sequence` and `epoch` and follows the same rule as
  `Down` (4.1).

### 4.3 Retired rows

A replica that must let go of a row that one of its own rows still points at
(a receipt that names an order) keeps the row and marks it retired: a column
the replica owns, set to when it was retired. Retired rows are out of
everything the replica offers its users, but stay for what points at them. A
retired row the server sends again becomes live.

### 4.4 `Filed`

The server answers each upload (section 7) once it has filed it. On `Filed`,
the replica **must** forget the notes those uploads carried.

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
applying, and again whenever it applied something new.

---

## 6. Rejections

A row the replica could not take (4.1) is a **rejection**: the table, the key
(from `keys[i]`, if the row did not even decode), and why.

- The replica **must** record a rejection in the same transaction that left
  the row out, so it cannot be lost.
- It **must** report each rejection to the server in an `Up` (`rejected`), and
  should do so promptly.
- It **must** keep the rejection until it takes the row (a later message
  carries the row and it applies): a digest (section 8) leaves rejected rows
  out, as the server leaves out the rows it set aside for the replica.

On a rejection, the server stops counting the row as held and sets it aside
for that replica. It sends it again when the relay restarts or on a timer,
which is after a deploy on either side has had the chance to fix the cause.
Nobody repairs the row by hand.

---

## 7. Sending rows up

For a table the replica writes and the server mirrors:

1. **Note** every insert, update and delete in the writing transaction: the
   table and the key. (A trigger is the only way no write path can forget.)
2. **Upload** noted rows, as they are **at upload time**, in an `Up`:
   - each row in `tables[].upserts`, encoded as its table's message;
   - a row that is gone by now, as its key in `tables[].deletes`, but only
     for a table keyed by the replica that wrote it: history a replica pruned
     is not the server's to delete;
   - in `uploads`, the number of this upload: the highest note it carries.
     The same notes sent again carry the same number.
3. **Wait for `Filed`** (4.4) naming that upload, then forget its notes. An
   upload not answered after a while (minutes) should be sent again; the
   server files the same rows idempotently.

The server files an upload under **the replica it came from** (the transport
says which), whatever the rows themselves claim. A row the server cannot file
is kept on the server, not lost, and filed later; it still counts as filed for
`Filed`.

A replica **may** say what it wrote and the server has not filed yet, in
`Up.backlog`: per table, rows not sent yet (`unsent`) and rows sent and not
yet `Filed` (`unfiled`). The server keeps the last one it was told, with the
time, and shows the replica as catching up while it is not empty. Say it
when it changes — at once when it empties — and not on every write; say it
again now and then while it is not empty, and to a server whose epoch
changed, either of which may have lost it. An `Up` without `backlog` leaves
the last one standing. A replica that never says leaves the server with
nothing to show, and nothing breaks.

A replica's `Up`s on one lane reach the server in the order it sent them: the
backlog an earlier one carried must not arrive after a later one's.

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
provisions the replica again from scratch (4.2). A replica that skips digests
loses only this safety net.

---

## 9. What the server guarantees in return

- Every change committed on the server reaches every replica that should hold
  the row, as the row is at sending time.
- A row is sent with every row it points at that the replica lacks, and is
  taken back only once nothing the replica keeps points at it.
- Messages are numbered and only ever rise within an epoch.
- A row the server cannot send to a replica, or that a replica rejected, is
  set aside and sent again after a fix: it is never silently dropped.
- An upload is answered with `Filed` only after the transaction that filed it
  committed.

---

## 10. A session, start to finish

```
server                                         replica
  │  (every arrow down is a ToReplica carrying the message named)
  │  Down {snapshot s1, seq 101, products…}  ──▶  applies, remembers s1's keys
  │  Down {snapshot s1, seq 102, orders…}    ──▶  applies
  │  EndSnapshot {s1, seq 103, [products,orders]}
  │                                          ──▶  sweeps what s1 did not name
  │  ◀── Up {epoch E, sequence 103}               "applied up to 103"
  │      progress: holds 3,200 of 3,200
  │
  │  (a product is renamed)
  │  Down {seq 104, products: 1 row}         ──▶  applies
  │  ◀── Up {epoch E, sequence 104}
  │
  │  (a row the replica refuses: a check it has, the server lacks)
  │  Down {seq 105, orders: 2 rows}          ──▶  applies one, rejects one
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

A replica implementation is complete when:

- [ ] it reads every message as a `ToReplica`, leaves one of a later protocol
      unacknowledged, and sets `protocol` on every `Up`;
- [ ] it applies a `Down` in one transaction, tables in the order given;
- [ ] it ignores a message at or below the last applied in the same epoch, and
      starts over on a new epoch;
- [ ] it keeps a newer stored version over an older incoming one;
- [ ] it never overwrites the columns it owns;
- [ ] it retires a row it cannot delete, and revives a retired row sent again;
- [ ] it follows snapshots and sweeps on their end, in reverse table order;
- [ ] it leaves out and records, in the same transaction, a row it cannot take;
- [ ] it reports rejections, and keeps them until it takes the row;
- [ ] it acknowledges the transport only after committing;
- [ ] it reports the last applied `(epoch, sequence)` to the server;
- [ ] it notes its own writes in the writing transaction, uploads them as they
      are, numbers each upload, and forgets the notes on `Filed`;
- [ ] it reports digests with the fingerprint above (optional, recommended);
- [ ] it says its backlog when it changes (optional, recommended).
