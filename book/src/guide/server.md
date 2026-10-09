# The server

The server's side is one value, a [`Server`], that you build once and run in a
task of its own. It needs four things from you: the codecs, where the replicas
are, how bytes reach a replica, and what you know about a replica that sends
something up.

## Where the replicas are

> **You write:**

```rust
pub const REGISTERS: magic_sync::plan::Replicas = magic_sync::plan::Replicas {
    relation: "shop.registers",   // a table or view with an `id` column
    table: "registers",           // its name, as foreign keys name it
    live: "active",               // which of them receive anything
    partition: None,
};
```

`partition` names a column that splits rows and replicas into groups no row
crosses — one per customer of a multi-customer system, say: a `broadcast` row
reaches every live replica *of its partition*. The shop has one company, so
none.

## How bytes reach a replica: the transport

> **You write** one method: send these bytes to that replica.

```rust
use magic_sync::{relay::Transport, Error};

#[derive(Clone, Default)]
struct Queues(Arc<Mutex<HashMap<Uuid, VecDeque<Vec<u8>>>>>);

impl Transport for Queues {
    async fn send(&self, register: Uuid, message: Vec<u8>, _id: &str) -> Result<(), Error> {
        self.0.lock().unwrap().entry(register).or_default().push_back(message);
        Ok(())
    }
}
```

The shop's transport is a queue per register, read over HTTP: a register asks
for the message at the head of its queue, applies it, and only then
acknowledges it, which removes it. That keeps the **one rule** the engine
needs from a transport:

> A replica gets its messages **in order, one at a time**: the next one only
> after it acknowledged the previous one.

With NATS JetStream, that is a durable pull consumer per replica with
`max_ack_pending = 1`. `id` is the same for the same message sent again, for a
transport that deduplicates.

When sending fails, say whether a retry may help:

```rust
Err(Error::Transport { reason: e.to_string(), transient: true })   // the network
Err(Error::Transport { reason: e.to_string(), transient: false })  // this message, never
```

> **What happens:** a transient failure leaves that replica waiting out a
> backoff while every other replica carries on; what it is owed is noted
> again for it alone. A permanent one sets the message's rows aside for that
> replica until a fix is deployed. The engine never hands a transport a
> message larger than its limit: a row too big for any message is set aside
> instead.

## Building and running it

> **You write:**

```rust
use magic_sync::run::server::{Anyone, Settings};
use magic_sync::Server;

let server = Arc::new(
    Server::install(pool.clone(), codecs(), REGISTERS, queues.clone(), Anyone(pool.clone()), Settings::default())
        .await?,
);
let relay = server.clone();
tokio::spawn(async move { relay.run(std::future::pending()).await });
```

`Server::install` sets up what the database lacks ([The tables](tables.md));
with migrations of your own, use `Server::new` with the same arguments, which
refuses to start on a database that is not set up.

> **What happens** in `run`, for as long as your process lives:
>
> - It takes a PostgreSQL advisory lock. Run as many server processes as you
>   like: one of them runs the relay, the others stand by and take over if it
>   goes away.
> - While it holds the lock, it reads the noted changes, plans what each
>   replica should hold against what it holds, sends the difference through
>   your transport, and records what each replica now holds — on the lock's own
>   connection, so a process that lost the lock cannot record over the one
>   that took it.
> - When it takes the lock — usually after a deploy — and every hour, it tries
>   again whatever it set aside: notes it could not plan, rows it could not
>   send, uploads it could not file. A deploy that fixed the cause lets them
>   through.
> - Every few minutes it logs what needs someone: a backlog that is not
>   draining, rows in quarantine. Each of those means *wait* (the network) or
>   *deploy a fix* (a bug), never *go fix the data*.

`Settings` holds the knobs: message size limits, notes per pass, the lock's
key, how often it retries and prunes, and `role` — a role every transaction of
the engine's own assumes, when your connecting role has less than it needs.

## What replicas send up

> **You write** a route that hands every message from a replica to the server,
> saying which replica it came from — from your transport, never from the
> message itself:

```rust
async fn up(State(app): State<App>, Path(register): Path<Uuid>, body: Bytes) -> Result<StatusCode, …> {
    app.server.receive(register, &body).await?;
    Ok(StatusCode::NO_CONTENT)
}
```

An error means the message must come again (the database is away, or it
speaks a later protocol than this build); filing it again is harmless.

> **What happens:** the rows are filed in a transaction, each by its codec's
> rules — a record kept from its first delivery, the replica's columns of a
> `both` table taken, a catalog refused — under the replica the message came
> from. After the commit, the replica is told its upload was filed, through
> your transport. A row the server cannot file is kept as it came, and filed
> again after a deploy. The same message also carries what the replica could
> not take, what it applied, and what it holds.

## What you know about a sender: `Senders`

`Anyone` files every replica's uploads as they are, in a plain transaction.

> **You write**, when your database cares who is writing — a role, a tenant
> setting your row policies read — or you want to stamp uploads with what you
> know of the sender:

```rust
use magic_sync::run::server::Senders;

struct Companies(PgPool);

impl Senders for Companies {
    type Known = Company;   // the `P` of `Codec<P>`

    async fn known(&self, register: Uuid) -> Result<Option<Company>, magic_sync::Error> {
        // the register's company, or None for a register you do not know
    }

    async fn filing(&self, company: &Company) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, magic_sync::Error> {
        // a transaction set up to write as that company
    }
}
```

[`Server`]: https://docs.rs/magic_sync/latest/magic_sync/run/server/struct.Server.html
