# The API

A build of `magic_sync` is one side, and so is its API reference: each lists
what a host of that side can name, built from the same sources as this book.

- [**The server's**](api/server/magic_sync/index.html) — feature `postgres`:
  `Server`, the planner's `Engine` and its status reads, the transport.
- [**A replica's**](api/replica/magic_sync/index.html) — feature `sqlite`:
  `Replica`, `replica::install` and `check`, the uplink.

What both sides share — the rows, the codecs, the derive, the errors — is in
both. [docs.rs](https://docs.rs/magic_sync) has the server's, for every
published version.
