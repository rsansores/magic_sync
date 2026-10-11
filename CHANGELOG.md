# Changelog

## 0.1.0 — unreleased

The first release: the engine extracted from the system it was built in.

- `#[derive(Row)]` / `#[derive(Columns)]` (`magic_sync_derive`): one struct is
  one table; every convention is a declaration. A misuse is a compile error
  pointing at what says it; a table that is not its row is refused when the
  server or a replica starts.
- Codecs: `Codec::json` (an object of the row's columns, 64-bit integers as
  text) and `Codec::new` (a protobuf message of your own).
- `Server` and `Replica` runners: what a host builds, runs and hands what
  arrives. The steps they are built on are the engine's own.
- `Server::install`, `replica::install` and `replica::check` for the engine's
  schema and triggers; a schema version both sides check. On the server the
  schema is installed or upgraded in one transaction, one process at a time,
  and `Engine::triggers` prints the triggers a host's migrations write; the
  server refuses to start while one is missing, disabled or different. On a
  replica, `replica::install` migrates and installs in one transaction,
  `replica::triggers` writes the triggers for a host's migrations, and
  `check` holds the database to exactly those.
- Replicas are provisioned when they become live or move partition, and
  forgotten when they stop being live; no host call needed.
- The replica protocol (`PROTOCOL.md`, `proto/sync.proto`), version 1; the
  engine's messages are generated from `sync.proto`, and a replica
  conformance suite applies the server's own messages (`tests/golden`).
- The shop example: a head office and its registers, each with a page.
- `status`: what the engine answers about itself, for a dashboard — the
  overview (replicas counted per state and the worst listed, each table's
  state, without counting what any replica should hold), a paged listing of
  the replicas, health over a scope, progress, where one row is. Every read
  takes a scope, counts live replicas only and is one snapshot; every answer
  serializes as JSON of a versioned contract, with the server's time.
- `Up.backlog`: a replica says what it wrote and the server has not filed,
  and the server shows it.
- `magic_sync_dashboard` (npm): the dashboard for Vue 3 — a ready-made
  component styled through CSS variables, and the same without its looks
  (`useSyncDashboard`) for a host with its own design system. English and
  Spanish; it fetches nothing. Its types are generated from the engine's.
