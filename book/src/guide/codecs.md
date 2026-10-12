# How a row travels: codecs

A **codec** is a table's row type, and how its rows are turned into bytes and
back. The engine holds every table's codec in one list, and never names a
table itself.

> **You write** the list, parents before children
> (`examples/shop/rows/src/lib.rs`):

```rust,ignore
{{#include ../../../examples/shop/rows/src/lib.rs:codecs}}
```

## JSON or protobuf

`Codec::json` sends a row as a JSON object of its columns, each value as its
field's serde writes it — a 64-bit integer as its decimal text, which a
reader holding numbers as doubles would otherwise round: nothing else to
declare, as long as serde writes each field under its own name. A field with
`#[serde(rename = "…")]`, a `rename_all` on the row, or a part brought in with
`#[sqlx(flatten)]` that serde nests instead of flattening (give the field
`#[serde(flatten)]` too) makes every row of the table fail to encode, the first
time one travels. That is the simple way, and the one to start with.

`Codec::new` sends it as a protobuf message of your own, with a mapping each
way — smaller on the wire, and a schema a replica in another language can
generate code from:

```rust,ignore
{{#include ../../snippets/src/codecs.rs:codec}}
```

where `ProductRow` is a [`prost`] message:

```rust,ignore
{{#include ../../snippets/src/codecs.rs:message}}
```

and the two functions map the row to it and back, failing with a
`magic_sync::wire::MapError`, which any error converts into with `?`:

```rust,ignore
{{#include ../../snippets/src/codecs.rs:mapping}}
```

A row that does not encode is set aside on the server, for every replica
that was to get it; one that does not decode on a replica is refused there,
and reported. Either comes back once a fix lets it through.

## What the server knows about a sender: `P` and `place`

`Codec<P>` carries a type `P` of yours: what the server knows about a replica
that sends something up (here `()`: nothing). Every row a replica uploads goes
through its codec's `place` function with the replica's id and that knowledge,
so a receipt is filed under the register that **sent** it — the transport says
which — whatever the row itself claims.

A multi-company shop would make `P` the register's company, read when the
upload arrives ([`Senders`](server.md#what-you-know-about-a-sender-senders)),
and stamp it on every receipt.

[`prost`]: https://crates.io/crates/prost
