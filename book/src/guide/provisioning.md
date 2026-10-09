# Provisioning a replica

A new replica holds nothing. **Provisioning** sends it everything it should
hold, then tells it to let go of anything else it has.

> **You write**, in the transaction that sets the replica up:

```rust
magic_sync::plan::note_resync(&mut tx, None, register_id).await?;
```

The second argument is the replica's partition, when you use partitions.

The shop does it when a register introduces itself for the first time:

```rust
let mut tx = pool.begin().await?;
sqlx::query("INSERT INTO shop.registers (id, name, url) VALUES ($1, $2, $3)")
    .bind(id).bind(&name).bind(&url)
    .execute(&mut *tx).await?;
magic_sync::plan::note_resync(&mut tx, None, id).await?;
tx.commit().await?;
```

Do the same whenever you suspect a replica is wrong — a "send it everything
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
