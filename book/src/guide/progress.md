# Progress and health

Every read here takes a `Scope` — `Scope::Replicas(&[a, b])`, a whole
`Scope::Partition(id)`, or `Scope::All` — and counts only live replicas. That
is how a host shows each audience its own: a customer of a multi-customer
system gets its partition, an operator gets everything, and a user who may see
some replicas gets the list of those. The engine knows replicas only by id;
put your own names on them.

Every answer derives `serde::Serialize`: return it from a handler as it is, and
a dashboard reads the JSON.

## The overview: is everything where it should be?

> **You write:**

```rust
use magic_sync::status::{ReplicaState, Scope};

let mut tx = server.begin().await?;
let overview = server.engine().overview(&mut tx, Scope::Partition(customer), 20).await?;
if overview.health.healthy() {
    // the green banner: nothing waits for a fix, every replica can be reached
}
for r in &overview.replicas {
    match r.state {
        ReplicaState::Synced => {}       // holds what it should; everything it wrote is filed
        ReplicaState::CatchingUp => {}   // r.pending, r.unconfirmed, r.backlog
        ReplicaState::Unreachable => {}  // r.failures, r.last_error, r.retry_at
        ReplicaState::AwaitingFix => {}  // r.quarantined, r.parked, r.unfiled
    }
}
for t in &overview.tables {
    // t.pending, t.in_flight, t.quarantined, t.unfiled: all zero
    // means every replica of the scope holds this table as it should
}
```

> **What happens:** the overview never counts the rows a replica should hold.
> It reads what the engine records — the changes not yet planned, the messages
> a replica has not confirmed, what is set aside — so it costs the same for 200
> replicas as for one. Whether a replica is synced is exact all the same: a
> change it lacks is either noted and not yet planned, or in a message it has
> not said it applied, or set aside. A test holds the engine to it under a
> failing network: a replica called synced holds exactly what it should.

`overview.health` lists what the engine waits on over the scope, cut at the
limit you pass (`total` is uncut): parked notes, rows in quarantine and why,
uploads in the inbox and why, replicas a send keeps failing to. Each means one
of two things — *wait* (the network) or *deploy a fix* (a bug) — and none of
them ever means *go fix the data by hand*. `engine().health(&mut tx, scope,
limit)` answers it alone; the server's runner logs it every few minutes.

## How far along one replica is

The overview says *whether*; progress says *how far*: "holds 480 of 500".

```rust
for register in server.engine().progress(&mut tx, Scope::Replicas(&[register])).await? {
    for t in register.tables {
        println!("{}: {} of {} ({} on their way, {} set aside)",
            t.table, t.up_to_date(), t.should, t.in_flight, t.quarantined);
    }
}
```

> **What happens:** a row counts as held only once the replica *confirmed* it
> applied the message that carried it. Until then it is `in_flight`. So the
> numbers never run ahead of the replica: "holds 480 of 500" means 480 are on
> it, now. Counting what a replica should hold reads its rules over the
> tables: ask it of a few replicas at a time, not of a fleet.

Where is one row?

```rust
for (register, state) in server.engine()
    .row_progress(&mut tx, Scope::All, "products", &id.to_string()).await?
{
    // RowState::Holds, Outdated, OnItsWay, Missing, Leaving, Quarantined
}
```

## The other direction

On a replica:

```rust
for t in magic_sync::replica::progress(&mut conn).await? {
    println!("{}: {} not sent, {} not filed yet", t.table, t.unsent, t.unfiled);
}
```

That is the shop register's "2 receipts not at the head office yet". The
replica tells the server the same, on its main lane, whenever it changes —
with the rows it sends, at once when it starts or empties, and at most every
`Outbox::backlog_every` while it grows. The server shows it as the replica's
`backlog`, as of when it was said; a replica with one is still catching up.
