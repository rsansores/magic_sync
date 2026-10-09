# magic_sync

Keeps partial copies of PostgreSQL tables on SQLite replicas, and brings back
what the replicas write. Offline-first: a replica keeps working while it is
unreachable, and catches up when it is back — with nobody repairing data by
hand.

![The shop example: a head office and two registers, one of them unplugged](https://raw.githubusercontent.com/rsansores/magic_sync/main/book/src/images/shop.png)

*The [shop example](https://rsansores.github.io/magic_sync/getting-started.html): register 2 is unplugged. It keeps selling
from the products it holds; the head office knows it holds 5 of 7, and that
two receipts are on their way. Plug it back in and both catch up.*

```rust
// One struct is one table, and says how it travels. Both sides share it.
#[derive(sqlx::FromRow, Clone, serde::Serialize, serde::Deserialize, magic_sync::Row)]
#[row(table = "products", schema = "shop", mirror = "catalog", sync = "broadcast")]
pub struct Product {
    pub id: Uuid,
    pub name: String,
    pub price_cents: i64,
    #[row(version)]
    pub updated_at: DateTime<Utc>,
}
```

```rust
// The server (feature `postgres`): the engine runs itself. You say how bytes
// reach a replica.
let server = magic_sync::Server::install(pool.clone(), codecs(), REGISTERS, my_transport, Anyone(pool), Settings::default()).await?;
tokio::spawn(async move { server.run(shutdown).await });
```

```rust
// A replica (feature `sqlite`): hand it what arrives; acknowledge it to your
// transport once `receive` returned Ok.
let replica = magic_sync::Replica::new(sqlite, codecs(), my_id, my_uplink, Settings::default());
replica.receive(&bytes).await?;
```

You write your tables, a struct per table, and how bytes travel between your
server and your replicas. You do not write the sync.

## Why another sync engine?

Most sync engines replay a log: every change becomes an event, sent in order,
re-applied on the other side. A lost event is a divergence nothing heals; a
failing event blocks everything behind it; repair is someone's afternoon.

`magic_sync` replays nothing. For every replica, the server keeps **what that
replica holds**. When something changes, it works out **what the replica
should hold now**, and sends the difference:

```
send   = should hold − holds
delete = holds − should hold
```

A change is never "an event that must be delivered" — it is a reason to
compare two sets again. So a row that could not be sent is simply not
recorded as held, and the next comparison sends it, as it is *then*. A
replica that missed a week catches up from the current state. Provisioning a
new replica is a comparison against an empty set.

Built on that:

- **Partial replication by rules.** A row goes to every replica of its
  partition (`broadcast`), to the replica a column names (`assigned(col)`), to
  the replicas a group of rows is given to (`grouped`), or wherever its parent
  goes (`follows(col)`).
- **Referential closure, read from your schema.** A replica sent an order is
  sent its customer too, and keeps the customer while anything of its own
  points at it. The engine reads the foreign keys; you declare nothing.
- **Both directions.** Replicas write rows of their own (receipts, readings)
  that go up and are filed under the replica that sent them — whatever the
  rows claim.
- **A failure contract.** Every failure is either *transient* (the network,
  the database — it waits and retries) or *permanent* (a bug, data no
  declaration allows — it is narrowed down to the one row, set aside, and
  retried after your next deploy). Nothing else waits for it. An hourly
  fingerprint of what each replica holds catches whatever nobody foresaw.
- **Progress that does not lie.** "Register 7 holds 480 of 500 products"
  counts only what the register confirmed it applied.
- **Any transport.** You implement one method that sends bytes to a replica
  (NATS JetStream, HTTP, a queue, a USB stick). The engine needs one property
  from it: a replica's messages arrive in order, one at a time.
- **Any language on the replica side.** The [protocol](https://rsansores.github.io/magic_sync/protocol.html)
  is written down, its messages are a [`.proto`](https://github.com/rsansores/magic_sync/blob/main/magic_sync/proto/sync.proto),
  and the engine's own replica is just one implementation of it.

## Where it fits, and where it does not

It fits a system with one PostgreSQL server that owns the truth, and many
replicas — devices, stores, vehicles, field laptops — that each hold *part*
of it in SQLite, must keep working offline, and write records of their own.

It does not try to be a collaborative-editing engine: two writers on the same
column of the same row is not a case it resolves (a table either goes down,
goes up, or splits its columns between the two sides). If you need
multi-writer merges, look at CRDT-based tools ([Automerge], [Yjs]). If your
clients are browsers or mobile apps talking to a hosted service, look at
[PowerSync] or [ElectricSQL]. `magic_sync` is a library inside your own Rust
server — no extra service to run.

[Automerge]: https://automerge.org
[Yjs]: https://yjs.dev
[PowerSync]: https://www.powersync.com
[ElectricSQL]: https://electric-sql.com

## Learn it

- **[The book](https://rsansores.github.io/magic_sync/)** — the shop, built step by step: what you
  write, then what happens.
- **[How it works](https://rsansores.github.io/magic_sync/how-it-works.html)** — the engine from the ground up:
  the idea, the life of a change, every failure and how it heals.
- **[The replica protocol](https://rsansores.github.io/magic_sync/protocol.html)** — what a replica in any
  language must do.
- **[The shop example](https://github.com/rsansores/magic_sync/tree/main/examples/shop)** — [run it](https://rsansores.github.io/magic_sync/getting-started.html) in five minutes.

## Status

`0.1`. Extracted from a system that keeps fleets of edge devices in sync; the
API is young and will change before `1.0`. PostgreSQL 14 or later on the
server, SQLite on the replicas, [sqlx] underneath.

A build of the crate is one side: the `postgres` feature for the server, the
`sqlite` feature for a replica.

[sqlx]: https://crates.io/crates/sqlx

## License

Licensed under either of [Apache License, Version 2.0](https://github.com/rsansores/magic_sync/blob/main/LICENSE-APACHE) or
[MIT license](https://github.com/rsansores/magic_sync/blob/main/LICENSE-MIT) at your option.
