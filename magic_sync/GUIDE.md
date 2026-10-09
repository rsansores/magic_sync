# magic_sync, by example

This is the how-to. It builds one small system step by step and shows the
code you write at each step. For *why* the engine works the way it does, read
the [README](README.md) afterwards.

## The example

A chain of shops. The head office runs PostgreSQL. Every cash register in
every shop runs SQLite and must keep working when the network is down.

- **Products** are the same for every register of a company → sent to all of
  them.
- **Orders** are taken by one register → sent to that register only.
- **Receipts** are written by a register → sent up to the head office.

The company is the *partition*: a product of one company never reaches
another company's registers. Each register is a *replica*.

---

## Step 1. Install the schemas

On the server, run the engine's schema in a migration, before your tables:

```sql
-- migrations/0001_sync.sql: the contents of magic_sync/sql/postgres.sql
```

On each register, the same with `sql/sqlite.sql`.

That gives the server a `sync` schema (notes, what each register holds, the
quarantine, and so on) and the register a few `sync_*` tables. You never write
to them yourself.

---

## Step 2. Create your tables, and mirror them

On the server, create the tables as usual, then tell the engine to watch them:

```sql
CREATE TABLE shop.registers (
    id         UUID PRIMARY KEY,
    company_id UUID NOT NULL,
    active     BOOLEAN NOT NULL
);

CREATE TABLE shop.products (
    id         UUID PRIMARY KEY,
    company_id UUID NOT NULL,
    name       TEXT NOT NULL,
    price      BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- watch it, partitioned by company
SELECT sync.mirror_table('shop', 'products', '{}', 'company_id');

CREATE TABLE shop.orders (
    id          UUID PRIMARY KEY,
    company_id  UUID NOT NULL,
    register_id UUID REFERENCES shop.registers (id),
    product_id  UUID NOT NULL REFERENCES shop.products (id),
    quantity    INTEGER NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT sync.mirror_table('shop', 'orders', '{}', 'company_id');

CREATE TABLE shop.receipts (
    id          UUID PRIMARY KEY,
    register_id UUID NOT NULL,
    order_id    UUID,
    total       BIGINT NOT NULL
);
-- receipts go up, not down: nothing to watch on the server
```

From now on, every insert, update and delete on `products` and `orders` is
noted in the same transaction. You do not call the engine when you write; you
just write.

On the register, create the same tables with the same columns, plus a
`retired_at` column on the tables that come down (the register sets it on a
row it must keep because one of its own rows still points at it).

> **Tip.** If your tables have an `updated_at`, stamp it with the clock at the
> write and never behind the value it replaces. `now()` is when the transaction
> *began*, and a transaction that began first and committed last would stamp an
> older version than the row it replaces.

---

## Step 3. Declare the rows

Each table is a Rust struct whose fields are exactly its columns, and which
says how it travels.

```rust
#[derive(sqlx::FromRow, Clone, Row)]
#[row(table = "products", schema = "shop", mirror = "catalog", sync = "broadcast")]
pub struct Product {
    pub id: Uuid,
    pub company_id: Uuid,
    pub name: String,
    pub price: i64,
    pub updated_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow, Clone, Row)]
#[row(table = "orders", schema = "shop", mirror = "catalog", sync = "assigned(register_id)")]
pub struct Order {
    pub id: Uuid,
    pub company_id: Uuid,
    pub register_id: Option<Uuid>,
    pub product_id: Uuid,
    pub quantity: i32,
    pub updated_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow, Clone, Row)]
#[row(table = "receipts", schema = "shop", mirror = "record", key = "id")]
pub struct Receipt {
    pub id: Uuid,
    pub register_id: Uuid,
    pub order_id: Option<Uuid>,
    pub total: i64,
}
```

> **Note.** The `Row` derive still lives in the first host's macro crate, and
> moves into this one when the engine leaves it; a few attribute names there
> are still the host's. What it generates is the [`Row`](src/row.rs) trait: you
> can implement it by hand, as the engine's own tests do.

What the attributes say:

- `mirror = "catalog"`: written on the server, held by registers.
  `"record"`: written on a register, held by the server. `"both"`: split by
  column.
- `sync = "broadcast"`: every register of the row's company.
  `sync = "assigned(register_id)"`: only the register the column names.

You did not declare that an order needs its product. The engine reads the
foreign key from the schema: a register that is sent an order is sent its
product too, and keeps the product while it keeps an order pointing at it.

Each row also gets a protobuf message of the same shape, and a pair of mapping
functions between the two. Together they make a **codec**:

```rust
pub fn codecs() -> Vec<Codec<Shop>> {
    vec![
        Codec::new::<Product, ProductRow>(product_to_wire, product_from_wire, None),
        Codec::new::<Order, OrderRow>(order_to_wire, order_from_wire, None),
        Codec::new::<Receipt, ReceiptRow>(receipt_to_wire, receipt_from_wire, Some(place_receipt)),
    ]
}
```

`Shop` is whatever your server knows about a register that sends something up
(here, its company). `place_receipt` stamps it on every receipt, so a receipt is
filed under the register that **sent** it, whatever the row claims:

```rust
pub struct Shop { pub company_id: Uuid }

fn place_receipt(receipt: &mut Receipt, register: Uuid, _shop: &Shop) {
    receipt.register_id = register;
}
```

---

## Step 4. Say who the replicas are

```rust
const REGISTERS: Replicas = Replicas {
    relation: "shop.registers",   // a table or view with an `id` column
    table: "registers",           // as foreign keys name it
    live: "active",               // which of them receive anything
    partition: Some("company_id"),
};

let engine = Engine::new(&mut conn, &tables, REGISTERS).await?;
```

`Engine::new` reads the schema once and refuses anything it cannot honour: two
tables pointing at each other in a cycle, an `assigned` column that does not
exist, a table you declared but forgot to `mirror_table`.

---

## Step 5. Send things: the transport

The engine does not know how bytes travel. You tell it, by implementing one
method:

```rust
struct Nats { js: async_nats::jetstream::Context }

impl Transport for Nats {
    fn send(&self, register: Uuid, message: Message, id: &str)
        -> impl Future<Output = Result<(), magic_sync::Error>> + Send
    {
        let subject = format!("registers.{register}.down");
        let bytes = match message {
            Message::Down(m) => m.encode_to_vec(),
            Message::EndSnapshot(m) => m.encode_to_vec(),
        };
        async move {
            self.js.publish_with_headers(subject, dedupe_header(id), bytes.into())
                .await.map_err(away)?
                .await.map_err(away)?;      // wait until the broker stored it
            Ok(())
        }
    }
}

fn away(e: impl std::fmt::Display) -> magic_sync::Error {
    magic_sync::Error::Transport { reason: e.to_string(), transient: true }
}
```

Say `transient: false` only for a message the transport will **never** take
(for example, one larger than it allows). The engine quarantines that one
instead of retrying it.

The one rule for the transport: a register gets its messages **in order, one
at a time** (the next one only after the previous one is acknowledged). With
JetStream that is a durable pull consumer with `max_ack_pending = 1`.

---

## Step 6. Run the relay

One process at a time runs the relay. Hold a lock while you do (a PostgreSQL
advisory lock is enough), and record on the lock's own connection:

```rust
let relay = Relay { engine: &engine, codecs: &codecs, limits: Limits { rows: 500, bytes: 512 * 1024 }, batch: 500 };

// Once when you take the lock (after a deploy, what failed may now work), then hourly:
relay::unpark(&mut lock_conn).await?;
relay::retry_quarantine(&mut lock_conn).await?;

loop {
    // 1. plan, in a read transaction
    let mut tx = pool.begin().await?;
    let prepared = relay.prepare(&mut tx).await?;
    tx.rollback().await?;
    let Some(prepared) = prepared else { sleep(1s).await; continue };

    // 2. send, with no transaction open
    let sent = relay.publish(&nats, prepared).await;

    // 3. record, on the lock's connection
    let mut tx = lock_conn.begin().await?;
    let outcome = relay.record(&mut tx, sent).await?;
    tx.commit().await?;
}
```

If any step returns an error, wait a moment and run the pass again: an error
here always means the database or the network is away. Everything else (a bad
row, a register that cannot be reached) the engine has already set aside, and
`outcome` tells you what.

That is all it takes for products and orders to reach the registers. Insert a
product, and a second later every active register of that company has it.

---

## Step 7. Apply on the register

When a message arrives, apply it in a transaction, and acknowledge it only
after the commit:

```rust
async fn on_message(db: &SqlitePool, codecs: &[Codec<Shop>], bytes: &[u8]) -> Result<()> {
    let down = Down::decode(bytes)?;
    let mut tx = db.begin().await?;
    let applied = replica::apply(&mut tx, codecs, &down).await?;
    tx.commit().await?;
    // now ack the message to the transport
    for (table, keys) in applied { /* react, if you need to: refresh a screen */ }
    Ok(())
}
```

`EndSnapshot` messages go to `replica::end_snapshot` the same way.

If `apply` returns an error, do **not** acknowledge: it means the register's
own database failed, and the message must come again. A row the register
cannot take (it does not decode, a constraint refuses it) is **not** an error:
`apply` leaves it out, notes it, and carries on.

---

## Step 8. Provision a new register

When a register is set up, or you suspect it is wrong, ask for a snapshot in
the same transaction that links it:

```rust
sync::plan::note_resync(&mut tx, Some(company_id), register_id).await?;
```

Its next message is everything it should hold, followed by an end marker. On
the end marker, it deletes every row of those tables the snapshot did not name.

---

## Step 9. Send records up

A register writes receipts. Note them as it writes, with a trigger per table
that goes up, into the engine's `sync_notes` (the table and the key):

```sql
CREATE TRIGGER sync_note_receipts AFTER INSERT ON receipts BEGIN
    INSERT INTO sync_notes (relation, row_key) VALUES ('receipts', lower(hex(NEW.id)));
END;
```

From time to time, upload what was noted, and say it went:

```rust
let notes = replica::unsent(&mut conn, 500, Duration::from_secs(600)).await?;
if !notes.is_empty() {
    let up = replica::upload(&mut conn, &codecs, register_id, &notes).await?;
    publish_up(up.encode_to_vec()).await?;      // your transport, upward
    replica::sent(&mut conn, &notes, &up).await?;
}
```

`upload` reads each row as it is **now**, so a receipt edited twice goes up
once, with its last version. When the server answers with `Filed`, hand it to
`replica::filed`, and those notes are done; an upload it never answers goes
again after the ten minutes you passed to `unsent`.

On the server, file what arrives in a transaction:

```rust
let up = Up::decode(bytes)?;
let register = register_of(&message)?;      // from the subject, not from the payload
let shop = Shop { company_id: company_of(register).await? };
let mut tx = pool.begin().await?;
let filed = replica::file(&mut tx, &codecs, &up, register, &shop).await?;
tx.commit().await?;
```

A receipt the server cannot file (it does not decode, a constraint refuses it)
is kept as it came in `sync.inbox`, and the rest is filed. File the inbox again
when the relay starts and hourly:

```rust
for row in relay::inbox(&mut tx, 500).await? {
    let shop = Shop { company_id: company_of(row.replica_id).await? };
    relay::refile(&mut tx, &codecs, &row, &shop).await?;
}
```

---

## Step 10. Let registers tell you what went wrong

Two small loops on the register close the circle.

**Rejections.** Rows it could not take go up, so the server quarantines them
and sends them again once a fix is deployed:

```rust
let rejected = replica::rejections(&mut conn, 500).await?;
if !rejected.is_empty() {
    publish_up(Up { replica_id: register_id.to_string(), rejected: rejected.clone(), ..Up::default() }
        .encode_to_vec()).await?;
    replica::reported(&mut conn, &rejected).await?;
}
```

**Acknowledgements.** Tell the server what the register applied, whenever it
applied something new. Until it does, the server counts those rows as on
their way, not held:

```rust
if let Some(ack) = replica::acknowledgement(&mut conn, register_id).await? {
    publish_up(ack.encode_to_vec()).await?;
    replica::acknowledged(&mut conn, ack.sequence).await?;
}
```

**Digests**, hourly. A fingerprint of what the register holds. If it does not
match what the server thinks it holds, the server provisions it again, by
itself:

```rust
let up = replica::digests(&mut conn, &codecs, register_id).await?;
publish_up(up.encode_to_vec()).await?;
```

Both arrive at the server through the same `replica::file` as receipts.

---

## Step 11. Show progress

"Register 7 has 480 of the 500 products it should have" — and the number does
not lie, because it only counts what the register confirmed:

```rust
let shop = engine.progress(&mut conn, Scope::Partition(company_id)).await?;
for register in shop {
    for t in register.tables {
        println!("{}: {} of {} ({} on their way, {} set aside)",
            t.table, t.up_to_date(), t.should, t.in_flight, t.quarantined);
    }
}
```

`Scope::Replicas(&[a, b])` for some registers, `Scope::All` for every one.
Add up the figures for a group; show `t.table` with your own words for it.

Where is one product?

```rust
for (register, state) in engine.row_progress(&mut conn, "products", &id.to_string()).await? {
    // RowState::Holds, Outdated, OnItsWay, Missing, Leaving, Quarantined
}
```

On the register, the other direction: "12 receipts not at the head office yet":

```rust
for t in replica::progress(&mut conn).await? {
    println!("{}: {} not sent, {} not filed yet", t.table, t.unsent, t.unfiled);
}
```

For that, the register notes what it writes in `sync_notes` (one trigger per
table), uploads with `replica::unsent` + `replica::upload` + `replica::sent`,
and hands every `Filed` message from the server to `replica::filed`.

## Step 12. Watch it

```rust
let h = relay::health(&mut conn, 5).await?;
// h.pending, h.oldest     notes waiting to be sent, and how long the oldest has waited
// h.parked                notes the planner set aside (a bug: deploy a fix)
// h.quarantined           rows set aside for some register (a bug: deploy a fix)
// h.unfiled               uploaded rows the server could not file (a bug: deploy a fix)
// h.failing               registers the transport keeps failing to reach (the network)

let s = relay::replica_status(&mut conn, register_id).await?;
// s.owed, s.quarantined, s.failures, s.retry_in, s.sent_ago
```

Every number means "wait" (the network) or "deploy a fix" (a bug). None of them
ever means "go fix the data by hand".

---

## Recipes

### A table that extends another one

A product's nutrition facts, keyed by the product. It should go wherever the
product goes:

```rust
#[row(table = "nutrition", schema = "shop", mirror = "catalog",
      key = "product_id", sync = "follows(product_id)")]
pub struct Nutrition { pub product_id: Uuid, /* … */ }
```

### Give a register a whole list at once

Customers belong to lists, and a register is given lists. A list of 10,000
customers given to a register is one row in `register_lists`:

```sql
CREATE TABLE shop.lists (id UUID PRIMARY KEY, company_id UUID NOT NULL);
CREATE TABLE shop.list_members (
    list_id UUID REFERENCES shop.lists (id), customer_id UUID REFERENCES shop.customers (id),
    company_id UUID NOT NULL, PRIMARY KEY (list_id, customer_id));
CREATE TABLE shop.register_lists (
    list_id UUID REFERENCES shop.lists (id), register_id UUID REFERENCES shop.registers (id),
    company_id UUID NOT NULL, PRIMARY KEY (list_id, register_id));
SELECT sync.mirror_group('shop', 'list_members',   'list_id',     'members',  'company_id');
SELECT sync.mirror_group('shop', 'register_lists', 'register_id', 'assigned', 'company_id');
```

```rust
#[row(table = "customers", schema = "shop", mirror = "catalog",
      sync = "grouped(members = list_members, assigned = register_lists)")]
pub struct Customer { /* … */ }
```

Taking a list away from a register is deleting one row; the engine works out
which customers to take back (and keeps those another list still gives it).

### A table both sides write

A register reports its own status on a row the server owns. Mark the columns
the register owns; they go up, the rest come down, and an update that only
touches the register's columns is not sent back down:

```rust
#[row(table = "register_state", schema = "shop", mirror = "both", sync = "assigned(id)")]
pub struct RegisterState {
    pub id: Uuid,
    pub label: String,              // the server's
    #[row(replica_owned)]
    pub battery: Option<i32>,       // the register's
    pub updated_at: DateTime<Utc>,
}
```

```sql
SELECT sync.mirror_table('shop', 'register_state', '{battery}', 'company_id');
```

### A row the register must keep after it is gone from the server

If a receipt on the register points at an order, and the server takes the order
away, the register cannot delete it without breaking its receipt. It sets the
order's `retired_at` instead: the order stays for the receipt, and your
register's screens leave retired rows out. If the order comes back, it is live
again.
