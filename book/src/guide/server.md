# The server

The server's side is one value, a [`Server`], that you build once and run in a
task of its own. It needs four things from you: the codecs, where the replicas
are, how bytes reach a replica, and what you know about a replica that sends
something up.

## Where the replicas are

> **You write** (`examples/shop/rows/src/lib.rs`):

```rust,ignore
{{#include ../../../examples/shop/rows/src/lib.rs:registers}}
```

`shop.registers` is a table or a view with an `id` column; `registers` is its
name as foreign keys name it; `active` is the condition a register that
should receive anything meets. A register that becomes active is provisioned
([Provisioning](provisioning.md)); one that stops being active is forgotten.

`Replicas::partitioned_by("company_id")` names a column that splits rows and
replicas into groups no row crosses — one per customer of a multi-customer
system, say: a `broadcast` row reaches every live replica *of its partition*
([Recipes](recipes.md#several-companies-on-one-server)). The shop has one
company, so none.

## How bytes reach a replica: the transport

The engine hands you each message as bytes, for one replica, and you deliver
it. How is yours — a broker, HTTP, a queue of your own — but a transport must
provide what [the replica protocol requires of it](../protocol.md#2-what-the-transport-must-provide):
nothing it took is lost, a replica's messages arrive in order and one at a
time, a message comes again until it is acknowledged, and an acknowledgement
names its message. The engine's guarantees rest on it.

> **You write** one method: send these bytes to that replica. The shop's
> transport is a queue per register in the head office's own database,
> which a register reads over HTTP (`examples/shop/server/src/main.rs`):

```rust,ignore
{{#include ../../../examples/shop/server/src/main.rs:transport}}
```

`id` names the message: the shop gives it to the register with the message,
and the register acknowledges it by that name, which takes that message —
and no other — out of the queue:

```rust,ignore
{{#include ../../../examples/shop/server/src/main.rs:next}}
```

With NATS JetStream, the stream is the queue, and a durable pull consumer per
replica with `max_ack_pending = 1` hands out one message at a time.

When sending fails, say whether a retry may help:

```rust,ignore
{{#include ../../snippets/src/server.rs:transport_errors}}
```

> **What happens:** either way, that replica waits out a backoff while every
> other replica carries on; the message stays first in its outbox, with
> everything after it, and goes again, in order, when the backoff ends. A
> refusal shows in its `last_error`; a message refused for good again and
> again, or an outbox grown past its bound, is dropped and the replica
> provisioned again — an alarm. The same message always goes under the same
> `id`. A send that does not answer
> within `send_timeout` is a transient failure. The engine never hands a
> transport a message larger than its limit: a row too big for any message is
> set aside instead.

## Building and running it

> **You write:**

```rust,ignore
{{#include ../../../examples/shop/server/src/main.rs:build}}
```

`Server::install` sets up what the database lacks ([The tables](tables.md));
with migrations of your own, use `Server::new` with the same arguments, which
refuses to start on a database that is not set up. `run` returns as soon as
the future it is given resolves — the shop's, on Ctrl-C. A pass it cuts short
is planned and sent again by the next relay, as after a crash:

```rust,ignore
{{#include ../../../examples/shop/server/src/main.rs:shutdown}}
```

> **What happens** in `run`, for as long as your process lives:
>
> - It takes a PostgreSQL advisory lock. Run as many server processes as you
>   like: one of them runs the relay, the others stand by and take over if it
>   goes away.
> - While it holds the lock, it reads the noted changes, plans what each
>   replica should hold against what it holds, and — in one transaction on the
>   lock's own connection, so a process that lost the lock cannot record over
>   the one that took it — records what each replica will hold and writes the
>   messages that carry the difference to the replica's outbox. Then it sends
>   every outbox through your transport.
> - When it takes the lock — usually after a deploy — and every hour, it tries
>   again whatever it set aside: notes it could not plan, rows it could not
>   send, uploads it could not file. A deploy that fixed the cause lets them
>   through.
> - Every few minutes it logs what needs someone: a backlog that is not
>   draining, rows in quarantine. Each of those means *wait* (the network) or
>   *deploy a fix* (a bug), never *go fix the data*.

`Settings` holds the knobs: message size limits, notes per pass, the lock's
key, how often it retries and prunes, how long a send may take and how long
a replica it failed to waits, how long a replica may leave what it was sent
unconfirmed before it is provisioned again, and `role` — a role every
transaction of the engine's own assumes, when your connecting role has less
than it needs. Change what you need of the defaults:

```rust,ignore
{{#include ../../snippets/src/server.rs:settings}}
```

## What replicas send up

> **You write** a route that hands every message from a replica to the server,
> saying which replica it came from — from your transport, never from the
> message itself:

```rust,ignore
{{#include ../../../examples/shop/server/src/main.rs:up}}
```

An error means the message must come again (the database is away, or it
speaks a later protocol than this build); filing it again is harmless. Any
other failure is no reason to send it again: the upload is kept whole, it is
answered, and it is filed again after a deploy. The shop answers an error
with the status that says whether to try again, which its registers read the
same way:

```rust,ignore
{{#include ../../../examples/shop/server/src/main.rs:failure}}
```

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

```rust,ignore
{{#include ../../snippets/src/server.rs:senders}}
```

A register for which `known` answers `None` is one the server does not know:
its messages are dropped, with a log.

[`Server`]: ../api/server/magic_sync/run/server/struct.Server.html
