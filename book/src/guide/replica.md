# A replica

A replica's side is one value too, a [`Replica`]: you hand it every message
that arrives, and run its upward loop in a task of its own.

## Building it

> **You write** (`examples/shop/register/src/main.rs`):

```rust,ignore
{{#include ../../../examples/shop/register/src/main.rs:build}}
```

`me` says which replica this is: a `Uuid` the server knows (a row of
`shop.registers`). When a replica does not know its id until it is set up,
implement [`Identity`] instead, answering `None` until it does: nothing goes
up until then. `run` sends up until the future it is given resolves,
finishing what it is sending.

`Replica::new` refuses a connection the engine cannot keep its word on, and
so does every message it applies, on whichever connection of the pool:
commits that are not durable (`synchronous` below `FULL`, which sqlx's pools
keep by default), since a message the replica acknowledged must still be
applied after a power cut; a journal that cannot roll back (`journal_mode`
`OFF` or `MEMORY`), since a message is applied whole or not at all; and
triggers that miss the rows a conflict replaces (`recursive_triggers`, off
by default), since a row an `INSERT OR REPLACE` deletes must go up as gone.
Set them in the pool's options, so every connection has them:

```rust,ignore
{{#include ../../../examples/shop/register/src/main.rs:pool}}
```

## Taking what the server sends

> **You write** a loop over your transport: hand each message to `receive`,
> and acknowledge it to the transport **only after `receive` returned `Ok`**.
> The shop's asks the head office for the next message, which answers as
> soon as there is one, and acknowledges it by its id:

```rust,ignore
{{#include ../../../examples/shop/register/src/main.rs:receive}}
```

> **What happens:** the message is applied in one transaction, and committed
> before `receive` returns. A message at or below the last one applied is a
> redelivery and changes nothing. A row this replica cannot take — it does not
> decode, a constraint refuses it — is left out and reported, and the rest
> applies: one bad row never blocks the queue. An error means the replica's own
> storage failed, or the message speaks a later protocol than this build:
> don't acknowledge, and it comes again.

`receive` answers what it did — `Received::Applied` with the keys of every
row it applied, per table — so you can react: the shop's loop logs it; a
screen would refresh when products arrived.

## Sending things up: the uplink

> **You write** one method: send these bytes to the server. It is a
> transport too, and provides [the same](../protocol.md#2-what-the-transport-must-provide);
> the shop's is a POST to the head office, which answers once it filed what
> came:

```rust,ignore
{{#include ../../../examples/shop/register/src/main.rs:uplink}}
```

> **What happens** in `run`: whenever there is something to say — a receipt
> written, a message applied, an hourly fingerprint due — it gathers it into
> one message and sends it through your uplink: the rows written since, as
> they are *now* (a receipt edited twice goes up once), the rows it could not
> take, the last message it applied, and its fingerprints. Once the uplink took
> it, that is recorded; a receipt is forgotten only when the server says it
> filed it, and sent again if the server never answers. When the uplink fails
> for a while, it backs off and tries again; nothing is lost. When it refuses a
> message for good, the rows it carried are set aside, counted in the backlog,
> until the replica runs again with a build that may send them.

After writing something you want to see go up at once, call
`replica.nudge()`; otherwise it goes on the loop's next turn.

### Lanes

A replica that writes a table often — positions, readings — can give it a lane
of its own, so it never waits behind the rest, nor the rest behind it:

```rust,ignore
{{#include ../../snippets/src/replica.rs:lanes}}
```

Your uplink gets the lane's name and sends each on its own stream, each kept
in order. Each lane runs on its own: one whose uplink fails waits out its own
backoff, and one whose send hangs is given up on after `send_timeout` (30
seconds unless you set it), while the others go on.

[`Replica`]: ../api/replica/magic_sync/run/replica/struct.Replica.html
[`Identity`]: ../api/replica/magic_sync/run/replica/trait.Identity.html
