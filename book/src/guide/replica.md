# A replica

A replica's side is one value too, a [`Replica`]: you hand it every message
that arrives, and run its upward loop in a task of its own.

## Building it

> **You write:**

```rust
use magic_sync::run::replica::Settings;
use magic_sync::{replica, Replica};

replica::install(&mut *pool.acquire().await?, codecs()).await?;   // no migrations of your own

let replica = Arc::new(Replica::new(pool.clone(), codecs(), me, Http { … }, Settings::default()));
let up = replica.clone();
tokio::spawn(async move { up.run(std::future::pending()).await });
```

`me` says which replica this is: a `Uuid` the server knows (a row of
`shop.registers`). When a replica does not know its id until it is set up,
implement [`Identity`] instead, answering `None` until it does: nothing goes
up until then.

## Taking what the server sends

> **You write** a loop over your transport: hand each message to `receive`,
> and acknowledge it to the transport **only after `receive` returned `Ok`**.

```rust
loop {
    let Some(message) = next_message().await else { continue };
    match replica.receive(&message).await {
        Ok(_) => acknowledge().await,          // the transport sends the next one
        Err(e) => sleep(Duration::from_secs(1)).await,   // not acknowledged: it comes again
    }
}
```

> **What happens:** the message is applied in one transaction, and committed
> before `receive` returns. A message at or below the last one applied is a
> redelivery and changes nothing. A row this replica cannot take — it does not
> decode, a constraint refuses it — is left out and reported, and the rest
> applies: one bad row never blocks the queue. An error means the replica's own
> storage failed, or the message speaks a later protocol than this build:
> don't acknowledge, and it comes again.

`receive` answers what it did, so you can react — refresh a screen when
products arrived:

```rust
if let Received::Applied(tables) = replica.receive(&message).await? {
    for (table, keys) in tables { /* … */ }
}
```

## Sending things up: the uplink

> **You write** one method: send these bytes to the server.

```rust
use magic_sync::run::replica::Uplink;

impl Uplink for Http {
    async fn send(&self, register: Uuid, _lane: &'static str, message: Vec<u8>) -> Result<(), Error> {
        self.client.post(format!("{}/sync/{register}/up", self.server))
            .body(message).send().await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| Error::Transport { reason: e.to_string(), transient: true })?;
        Ok(())
    }
}
```

> **What happens** in `run`: whenever there is something to say — a receipt
> written, a message applied, an hourly fingerprint due — it gathers it into
> one message and sends it through your uplink: the rows written since, as
> they are *now* (a receipt edited twice goes up once), the rows it could not
> take, the last message it applied, and its fingerprints. Once the uplink took
> it, that is recorded; a receipt is forgotten only when the server says it
> filed it, and sent again if the server never answers. When the uplink fails,
> it backs off and tries again; nothing is lost.

After writing something you want to see go up at once, call
`replica.nudge()`; otherwise it goes on the loop's next turn.

### Lanes

A replica that writes a table often — positions, readings — can give it a lane
of its own, so it never waits behind the rest, nor the rest behind it:

```rust
Settings {
    lanes: vec![
        Lane { name: "main", tables: &[] },                   // everything else, and what the engine says
        Lane { name: "telemetry", tables: &["positions"] },
    ],
    ..Settings::default()
}
```

Your uplink gets the lane's name and sends each on its own stream.

[`Replica`]: https://docs.rs/magic_sync/latest/magic_sync/run/replica/struct.Replica.html
[`Identity`]: https://docs.rs/magic_sync/latest/magic_sync/run/replica/trait.Identity.html
