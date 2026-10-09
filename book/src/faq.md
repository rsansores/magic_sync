# Questions

## Why one backend per build?

The server plans in PostgreSQL and a replica applies in SQLite: the two halves
share the row types and the codecs, and almost nothing else. Building the
crate for one side keeps every query in it checked against the database it
runs on. Your server binary enables `postgres`, your replica binary `sqlite`;
crates shared by both (your row types) enable neither and get the feature from
the binary that uses them.

## Can two sides write the same row?

A table goes down, goes up, or splits its *columns* between the two (`mirror =
"both"`, `#[row(replica_owned)]`). Two writers on the same column of the same
row is not a case the engine resolves. That is deliberate: in the systems it
was built for, every fact has one owner, and merging two owners' edits is a
business decision, not a sync one.

## What if a replica is offline for a month?

Nothing special. What it should hold is compared with what it holds when it is
back, and the difference is sent, as it is then — not a month of events. What
the replica wrote meanwhile goes up as it is then.

## What if my transport loses a message?

The replica's progress counts nothing it did not confirm, so the server keeps
counting those rows as on their way. A replica that acknowledged to the
transport but lost what it applied (it should not — it acknowledges after the
commit) is caught by the hourly fingerprint, and provisioned again by itself.

## Does the replica have to be written in Rust?

No. [The replica protocol](protocol.md) is what a replica must do, and the
messages are a `.proto` file. The engine's own replica is one implementation
of it.

## What does "nobody repairs data by hand" mean?

Every failure is either the infrastructure (it waits, and retries) or a bug or
data no declaration allows (it is narrowed down to the row it is about, set
aside, and retried after a deploy). Neither one ever needs someone to edit
rows. Every number the engine reports means *wait* or *deploy a fix*.

## How big can it get?

The planner works on sets in SQL, not row by row: giving a list of 10,000
customers to a register is one row written, and one plan. The engine's
benchmarks (`cargo test -p magic_sync --features postgres bench_ -- --ignored`)
measure the operations that move many rows at once.
