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
- `status`: what the engine answers about itself, for a dashboard — the
  overview (each replica's state and each table's, without counting what any
  replica should hold), health over a scope, progress, where one row is. Every
  read takes a scope and counts live replicas only; every answer serializes.
- `Up.backlog`: a replica says what it wrote and the server has not filed,
  and the server shows it.
- `magic_sync_dashboard` (npm): the dashboard for Vue 3 — a ready-made
  component styled through CSS variables, and the same without its looks
  (`useSyncDashboard`) for a host with its own design system. English and
  Spanish; it fetches nothing.
