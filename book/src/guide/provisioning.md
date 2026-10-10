# Provisioning a replica

A new replica holds nothing. **Provisioning** sends it everything it should
hold, then tells it to let go of anything else it has.

> **You write** nothing for a new replica: before every pass the relay looks
> at the replicas, and provisions each one it sees live for the first time —
> or live again, or in another partition than before. One that stops being
> live, or whose row is gone, is forgotten and sent nothing until it is live
> again.

To provision one again on demand, in any transaction of yours:

```rust
magic_sync::plan::note_resync(&mut tx, None, register_id).await?;
```

The second argument is the replica's partition, when you use partitions.

Do it whenever you suspect a replica is wrong — a "send it everything
again" button for an operator is one line.

> **What happens:** the next pass forgets what the server thought the replica
> held, and sends everything its rules say it should hold, numbered as one
> *snapshot*, followed by an end marker. On the end marker, the replica deletes
> (or retires) every row of those tables the snapshot did not name. A newer
> snapshot supersedes one still in progress.
>
> You rarely need to call it to repair anything: every hour a replica reports
> a fingerprint of what it holds, and when it does not match what the server
> recorded, the server provisions it again by itself.
