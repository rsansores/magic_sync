# Changelog

## 0.2.0 — unreleased

One server, several kinds of replica. Schemas: server 3, replica 2.
Protocol 1 (unchanged). Dashboard contract 1 (fields added).

- **Audiences.** A server serves one kind of replica or several from one
  stream of changes: `Audience::new(name, replicas, codecs)`. Each audience
  holds the tables it declares, each encoded as its own codec says; one may
  hold a part of what another holds, or a table in a shape of its own. A
  table two audiences declare has one trigger, which notes what either
  needs of it, and is planned for each against its own replicas. A replica
  the relations of two audiences name is the first's. The wire does not
  change: a replica is the same whatever audience it is of.
- **A row read from a view of its own.** `#[row(from = "schema.view")]`
  reads a catalog row's columns from a view over its table — parent columns
  joined in, rows left out — while the table stays what the row is noted,
  keyed and placed by. The server holds the row to the view when it starts.
- **Copied columns: `depends`.** `#[row(depends = "fk_column")]`: a change of
  the row a foreign key points at is a change of every row pointing at it
  so, sent again by the audience whose row copies the parent's columns,
  and by no other. The parent need not be mirrored: the engine notes it all
  the same.
- **The status by audience.** `Scope::Audience(name)`; every replica in an
  answer, and every table, says its audience (`audience`), and a table two
  audiences hold is listed once for each. The dashboard says the audience
  of each row when there are several.
- **The protocol** describes a replica that applies through another
  system's services (Appendix B): a ledger, apply then record, skip what it
  holds, adopt. `tests/golden/audiences.txt` is what one server sends a
  replica of each of two audiences.

### Breaking

- `Server::new` and `Server::install` take `Vec<Audience<P>>` in place of the
  codecs and the `Replicas`: `vec![Audience::new("registers", REGISTERS,
  codecs())]` is the server of 0.1.
- `Engine::triggers` and `Engine::indexes` take `&[Audience<P>]`.
- `Transport::send` is told the audience: `send(&self, audience, replica,
  message, id)`. `Senders::known` too: `known(&self, audience, replica)`.
- `RowPlace` is no longer `Copy` (it carries its audience's name).
- **Server schema 3.** `Server::install` takes the database there; with
  migrations of your own, run `plan::migration(3)`. Triggers are printed as
  before for a server of one audience: nothing to print again. The replicas
  the engine recorded are taken over by the audience that sees them, and
  what 0.1 last ran with is that audience's: nothing is provisioned again.

## 0.1.1 — 2026-10-13

- **A foreign key may carry the partition.** With `partitioned_by(column)`,
  the columns of a foreign key that are that column on both sides say the
  two rows share a partition, not which parent it is: `(company_id,
  region_id)` into `regions (company_id, id)` — how PostgreSQL keeps a row
  from pointing into another partition — points at a region by `region_id`,
  and `follows(region_id)` takes it. A key made only of the partition column
  points at no parent. A parent whose primary key holds the partition column
  — a table partitioned by it — is pointed at by all of them, as before.
  Until now, the server refused to start on either.
  Nothing on the wire or in either schema changes.
- **A replica sets itself up before it knows which it is.** Its triggers
  refuse a write of yours — to the server's tables, or an edit of history —
  only once it runs knowing its id: built with it, or sending or applying a
  message once `Identity` answers. Before that, a placeholder of its own row
  may be written and renamed, with the rows that point at it; a write to the
  server's tables is not noted, and the server's provisioning sets those
  rows straight. Print the replica's triggers again (`replica::triggers`):
  `replica::check` refuses the earlier ones.

## 0.1.0 — 2026-10-13

The first release. `magic_sync` keeps partial copies of PostgreSQL tables on
SQLite replicas and brings back what the replicas write, offline-first: for
every replica the server records what it holds, works out what it should hold,
and sends the difference. Schemas: server 2, replica 2. Protocol 1. Dashboard
contract 1. PostgreSQL 14 or later on the server; Rust 1.85 or later.

### What it is

- **The crates.** `magic_sync` is built for one side: the `postgres` feature
  is the server, the `sqlite` feature a replica, and a build with both (or
  neither) does not compile. `magic_sync_derive` is the derive, re-exported as
  `magic_sync::Row` and `magic_sync::Columns`. `magic_sync_dashboard` (npm) is
  the dashboard for Vue 3, released at the same version: `SyncDashboard`,
  `RowPlaces` for where one row is, `WishList` for the wishes of the
  replicas' settings, and each headless (`use…`) for a host's own components.
- **Tables are declared once**, as the struct that is the row
  (`#[derive(Row)]`): where it lives, which way it travels (`catalog`,
  `record`, `both`), which replicas hold which rows (`broadcast`,
  `assigned`, `grouped`, `follows`, or none: a row goes where a row that
  travels points at it, read from the foreign keys). A misuse is a compile
  error; a table that is not its row, a missing or different trigger, or a
  missing index is refused when the server or a replica starts.
- **Settings and wishes.** A `setting` is a row a replica owns and acts on;
  the server may wish its `#[row(desired)]` columns other values
  (`Server::desire`, `withdraw`, `wishes`). The replica carries each wish out
  once through the host's handler (`run::replica::CarryOut`, `Settings::wishes`) —
  outside any transaction, so it may reach hardware — and answers `applied`
  (the engine writes the row and the answer together), `refused`, `waiting`,
  or `superseded` when `apply = "if_unchanged"` and the setting changed on
  the replica since the wish was based on it. Wishes travel as rows of the
  engine's own two-way table `sync_wishes` (schema step 2 on each side), so
  every guarantee below holds for them. The server files a setting as a
  record, the newer version winning; a replica says its settings and answers
  again whenever a snapshot begins, which the server takes whatever their
  version, and its digests fingerprint them; a snapshot names the settings
  the server records it holding, and the replica deletes those it no longer
  holds — so once the system settles, both sides show the same value, after
  a restore of either too. A handler has `Settings::wishes_timeout` to
  answer; `Wish::new` lets a host test its own. Table names beginning
  `sync_` are the engine's.
- **Codecs.** `Codec::json` (an object of the row's columns, 64-bit integers
  as text) and `Codec::new` (a protobuf message of your own).
- **Runners.** `Server` and `Replica` run what a host builds; the host
  provides the transport (`Transport`, `Uplink`) and what it knows of a
  sender (`Senders`). The steps they are built on are the engine's own.
- **Installation.** The engine's schema is installed by `Server::install` and
  `replica::install`, or written into the host's migrations
  (`plan::migration`, `replica::migration`, `Engine::triggers`,
  `Engine::indexes`, `replica::triggers`); `check` holds a database to exactly
  those.
- **Status.** `status` answers for a dashboard, over a scope, in one
  snapshot: the overview, a paged listing, health, progress, where one row
  is, and the wishes of the replicas' settings (`Engine::wishes`; each
  replica counts its own, and one with a wish it has not answered is
  catching up). Every answer serializes as JSON of a versioned contract.
- **The replica protocol** (`PROTOCOL.md`, `proto/sync.proto`) is written for
  a replica in any language, with test vectors (`tests/golden`) that the
  engine's own replica is held to.
- **The shop** example, a head office and its registers with a page each — a
  footer of each register's the head office may wish another among them — and
  **the book**, which builds it step by step, explains the engine and carries
  the protocol; both sides' API reference is published with it.

### What it guarantees

- No committed change is lost: it is noted in the transaction that makes it,
  recorded with the messages that carry it, and sent until the transport takes
  it.
- A replica converges to what it should hold, never holds a row without the
  rows it points at, and never goes back in time: messages are numbered,
  chained and sealed per epoch, so a gap, a restored replica and a restored
  server are each found out and provisioned again.
- A failure holds back only what it is about. A transient failure (the network,
  the database) waits and retries; a permanent one (a row that does not encode,
  a value a replica refuses) is narrowed to the row, set aside, and retried
  after a deploy. Nobody repairs data by hand.
- What a replica writes is filed under the replica that sent it, whatever the
  rows claim; an upload is answered only once filed, and a replica forgets a
  write only on that answer.
- Progress counts only what a replica confirmed it applied. A row a replica
  did not take because it deleted it and the delete is not filed yet is
  reported held back (`Rejected.held_back`): no fault, no alarm, and sent
  again should the server keep it.
- An hourly fingerprint of what each replica holds catches what nothing
  foresaw.

### The API

`Error::Transport` and `Error::Host` are `#[non_exhaustive]`: build them with
`Error::transport(transient, reason)` and `Error::host(transient, reason)`.
What a host can name on each side is listed in `magic_sync/public-api/`, and
CI holds the crate to it. The API is young and may change before 1.0.
