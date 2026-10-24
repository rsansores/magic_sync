# Progress and health

Every read here takes a `Scope` — `Scope::Replicas(&[a, b])`, a whole
`Scope::Partition(id)`, every replica of one kind (`Scope::Audience(name)`,
[Several kinds of replica](audiences.md)), or `Scope::All` — and counts only
live replicas. That is how a host shows each viewer their own: a customer of
a multi-customer system gets its partition, an operator gets everything, and
a user who may see some replicas gets the list of those. The engine knows
replicas only by id, and by the audience each is of; put your own names on
them.

Each read is one snapshot: every figure in an answer was true at the same
moment. Called on a connection outside a transaction, a read opens a
`REPEATABLE READ` one of its own; called inside yours, yours must be
`REPEATABLE READ` (or `SERIALIZABLE`), and a read refuses a `READ COMMITTED`
one, which would see each statement's own moment.

Every answer derives `serde::Serialize`: return it from a handler as it is, and
a dashboard reads the JSON. Each carries `contract`, the version of that JSON
(`status::CONTRACT`), and a dashboard refuses an answer of a contract it does
not read.

## The overview: is everything where it should be?

> **You write** (`server.engine()` hands out the engine):

```rust,ignore
{{#include ../../snippets/src/server.rs:overview}}
```

> **What happens:** the overview never counts the rows a replica should hold.
> It reads what the engine records — the changes not yet planned, the messages
> a replica has not confirmed, what is set aside — so it costs a few
> milliseconds for 200 replicas holding 3 million rows while they are caught
> up, and about a fifth of a second at worst, when every row is on its way
> (the engine's `bench_the_dashboard_over_200_units_holding_3m_rows`, on a
> 32-core workstation).
> Whether a replica is synced is exact all the same: a change it lacks is
> either noted and not yet planned, or in a message it has not said it
> applied, or set aside. A test holds the engine to it under a failing
> network: a replica called synced holds exactly what it should.

Its size does not grow with the fleet. Every replica is counted in
`overview.states`, and `overview.replicas` lists the worst of them up to the
limit you pass — those waiting for a fix, then those out of reach, then those
catching up, the longest silent first — with `total` uncut. The rest are a
page away: `engine().listing(&mut conn, scope, Some(ReplicaState::Synced),
Page::new(offset, limit))` lists them in the same order, in one state or in all.
`overview.now` is the server's time when it read: every time in the answer is
the server's, and how long ago one was is measured from it, never from the
reader's clock.

A replica is waiting for a fix (`AwaitingFix`) only for what is its own: a
row set aside for it, an upload of its that waits in the inbox, a change of it
alone (its provisioning, say) that the planner set aside. A change the planner set aside that
may concern every replica is one problem, listed once in the health; each
replica it may concern is catching up on it.

`overview.health` lists what the engine waits on over the scope, cut at the
limit you pass (`total` is uncut): parked notes, rows in quarantine and why,
uploads in the inbox and why, replicas a send keeps failing to. Each means one
of two things — *wait* (the network) or *deploy a fix* (a bug) — and none of
them ever means *go fix the data by hand*. `healthy` is whether nothing waits
for a fix: a replica out of reach is the network's to settle, listed in
`failing` but not unhealthy; what waits for it waits in its outbox (`owed`
on its overview), not on the planner, so `oldest`, the age of the oldest
change not yet planned, does not grow while it is away.
`engine().health(&mut conn, scope, limit)` answers it alone; the server's
runner logs it every few minutes.

## How far along one replica is

The overview says *whether*; progress says *how far*: "holds 480 of 500".

```rust,ignore
{{#include ../../snippets/src/server.rs:progress}}
```

> **What happens:** a row counts as held only once the replica *confirmed* it
> applied the message that carried it. Until then it is `in_flight`; a row it
> was sent and should no longer hold is `to_remove`, confirmed or not. So the
> numbers never run ahead of the replica: "holds 480 of 500" means 480 are on
> it, now. Counting what a replica should hold reads its rules over the
> tables: ask it of a few replicas at a time, not of a fleet.

Where is one row?

```rust,ignore
{{#include ../../snippets/src/server.rs:row}}
```

## The other direction

On a replica:

```rust,ignore
{{#include ../../../examples/shop/register/src/main.rs:progress}}
```

That is the shop register's "2 receipts not at the head office yet". The
replica tells the server the same, on its main lane, whenever it changes —
with the rows it sends, at once when it starts or empties, and at most every
`Outbox::keepalive_every` while it grows — and how long ago each table's
oldest row was written. The server shows it as the replica's `backlog`, as
of when it was said, each table's `oldest_at` reckoned from that age; a
replica is catching up while a row of it has waited longer than the
server's `Settings::behind_after` (two minutes), or cannot go as it is:
rows that flow are not waiting. Each table says whether its rows wait
(`waits`); of a replica of an earlier build, which says no age, they wait
while any has not gone. Keep `keepalive_every` well under `behind_after`:
a replica that writes nothing else says what waits only that often.

## A replica that says nothing

A replica that has sent nothing on any lane for `Outbox::keepalive_every`
(a minute) says it is there, on its main lane, and every message it sends
says how long that is. The server keeps when it last heard from each
replica (`last_heard_at`) and when it last filed a row of each table from
it (`filed`); one that has said nothing for
`Settings::silent_after_keepalives` (three) of its keepalives is `silent` —
off, or cut off, which nothing on the server's side tells apart — and is
listed in the health's `silent`, which leaves it healthy. A replica of an
earlier build says no keepalive, and is never silent. A replica refuses a
keepalive over a day, which the server would take for none, and one its
`idle` would sleep past.

The server keeps `behind_after` and `silent_after_keepalives` in its
database (`sync.settings`) when it starts, and every status reads them
there: a process that only reads a status judges as the server does,
whatever settings it was built with.

`engine().states(&mut conn, scope)` answers every replica's state at once,
and nothing else of them: what a map of the fleet colours its tiles by —
some 15 ms over 4000 replicas.
