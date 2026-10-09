# Progress and health

## How far each replica is

> **You write:**

```rust
use magic_sync::plan::Scope;

let mut tx = server.begin().await?;
for register in server.engine().progress(&mut tx, Scope::All).await? {
    for t in register.tables {
        println!("{}: {} of {} ({} on their way, {} set aside)",
            t.table, t.up_to_date(), t.should, t.in_flight, t.quarantined);
    }
}
```

`Scope::Replicas(&[a, b])` for some replicas, `Scope::Partition(id)` for one
partition, `Scope::All` for every one. Show `t.table` with your own words for
it.

> **What happens:** a row counts as held only once the replica *confirmed* it
> applied the message that carried it. Until then it is `in_flight`. So the
> numbers never run ahead of the replica: "holds 480 of 500" means 480 are on
> it, now.

Where is one row?

```rust
for (register, state) in server.engine().row_progress(&mut tx, "products", &id.to_string()).await? {
    // RowState::Holds, Outdated, OnItsWay, Missing, Leaving, Quarantined
}
```

## The other direction, on a replica

```rust
for t in magic_sync::replica::progress(&mut conn).await? {
    println!("{}: {} not sent, {} not filed yet", t.table, t.unsent, t.unfiled);
}
```

That is the shop register's "2 receipts not at the head office yet".

## The engine's health

```rust
let h = magic_sync::relay::health(&mut tx, 5).await?;
// h.pending, h.oldest  notes waiting to be sent, and how long the oldest has waited
// h.parked             notes the planner set aside      (a bug: deploy a fix)
// h.quarantined        rows set aside for some replica  (a bug: deploy a fix)
// h.unfiled            uploads the server could not file (a bug: deploy a fix)
// h.failing            replicas the transport keeps failing to reach (the network)

let s = magic_sync::relay::replica_status(&mut tx, register).await?;
// s.owed, s.quarantined, s.failures, s.retry_in, s.sent_ago
```

The server's runner logs the same every few minutes. Every number means one of
two things — *wait* (the network) or *deploy a fix* (a bug) — and none of them
ever means *go fix the data by hand*.
