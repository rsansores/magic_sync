# Changelog

## 0.1.0 — unreleased

The first release: the engine extracted from the system it was built in.

- `#[derive(Row)]` / `#[derive(Columns)]` (`magic_sync_derive`): one struct is
  one table; every convention is a declaration.
- Codecs: `Codec::json` (the row's serde shape) and `Codec::new` (a protobuf
  message of your own).
- `Server` and `Replica` runners, built on public step functions.
- `Server::install`, `replica::install` and `replica::check` for the engine's
  schema and triggers; a schema version both sides check.
- The replica protocol (`PROTOCOL.md`, `proto/sync.proto`), version 1.
- The shop example: a head office and its registers, each with a page.
