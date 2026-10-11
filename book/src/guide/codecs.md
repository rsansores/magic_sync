# How a row travels: codecs

A **codec** is a table's row type, and how its rows are turned into bytes and
back. The engine holds every table's codec in one list, and never names a
table itself.

> **You write** the list, parents before children:

```rust
use magic_sync::wire::Codec;

pub fn codecs() -> Vec<Codec<()>> {
    vec![
        Codec::json::<Product>(None),
        Codec::json::<Receipt>(Some(place_receipt)),
    ]
}

/// A receipt is filed under the register that sent it, whatever it says.
fn place_receipt(receipt: &mut Receipt, register: Uuid, _: &()) {
    receipt.register_id = register;
}
```

## JSON or protobuf

`Codec::json` sends a row as a JSON object of its columns, each value as its
field's serde writes it — a 64-bit integer as its decimal text, which a
reader holding numbers as doubles would otherwise round: nothing else to
declare. That is the simple way, and the one to start with.

`Codec::new` sends it as a protobuf message of your own, with a mapping each
way — smaller on the wire, and a schema a replica in another language can
generate code from:

```rust
Codec::new::<Product, ProductRow>(product_to_wire, product_from_wire, None)
```

where `ProductRow` is a [`prost`] message and the two functions map the row to
it and back, failing with a `magic_sync::wire::MapError` — which any error
converts into with `?`, so a [`magic_map`] mapping, say, is one line:

```rust
fn product_to_wire(p: Product) -> Result<ProductRow, MapError> {
    Ok(p.try_map_into()?)
}
```

## What the server knows about a sender: `P` and `place`

`Codec<P>` carries a type `P` of yours: what the server knows about a replica
that sends something up (here `()`: nothing). Every row a replica uploads goes
through its codec's `place` function with the replica's id and that knowledge,
so a receipt is filed under the register that **sent** it — the transport says
which — whatever the row itself claims.

A multi-company shop would make `P` the register's company, read when the
upload arrives, and stamp it on every receipt.

[`prost`]: https://crates.io/crates/prost
[`magic_map`]: https://crates.io/crates/magic_map
