# Provisioning a replica

A new replica holds nothing. **Provisioning** sends it everything it should
hold, then tells it to let go of anything else it has.

> **You write** nothing for a new replica: before every pass the relay looks
> at the replicas, and provisions each one it sees live for the first time —
> or live again, or in another partition than before. One that stops being
> live, or whose row is gone, is forgotten and sent nothing until it is live
> again. The shop's head office only writes the register's row when it
> introduces itself.

To provision one again on demand, in any transaction of yours:

```rust,ignore
{{#include ../../snippets/src/server.rs:resync}}
```

The second argument of `note_resync` is the replica's partition, when you use
partitions. A "send it everything again" button for an operator is that
function.

> **What happens:** the next pass forgets what the server thought the replica
> held, and sends everything its rules say it should hold, numbered as one
> *snapshot*, followed by an end marker. On the end marker, the replica deletes
> (or retires) every row of those tables the snapshot did not name. A newer
> snapshot supersedes one still in progress.
>
> You rarely need it to repair anything. The server provisions a replica
> again by itself when what it holds may be wrong: when, every hour, the fingerprint it
> reports of what it holds does not match what the server recorded, and when
> it leaves the last messages it was sent unconfirmed for long
> (`Settings::unconfirmed_after`) while nothing newer goes to it — the
> transport may have lost them.
