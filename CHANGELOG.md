# Changelog

## 0.4.0 — unreleased

Where a replica is stuck. Schemas: server 5, replica 3. Protocol 1
(unchanged: fields added). Dashboard contract 2.

- **A backlog is what is owed once its upload is filed.** An upload counted
  the rows it carried as unsent, and the server stored, once it had filed
  them, a backlog that still held them. A backlog now leaves them out, and
  rows only on their way start nothing: the upload that carries them said
  what was owed besides.
- **How long a row has waited.** A replica stamps each note as it is made
  (`sync_notes.noted_at`, by an engine trigger: the host's triggers do not
  change), and its backlog says how long ago each table's oldest row was
  written, as an age (`TableBacklog.oldest_ms`), so the two clocks need not
  agree. A status reads it as `UpProgress::oldest_at`.
- **Behind is what waits.** A replica is catching up from its side while a
  row it wrote has waited longer than `Settings::behind_after` (two
  minutes), or cannot go as it is: rows that flow are younger. A replica of
  an earlier build, which says no age, is judged by when it said its
  backlog.
- **Keepalive.** `Outbox::keepalive_every` (a minute) takes the place of
  `Outbox::backlog_every`: once nothing has gone up on any lane for so long,
  the replica sends a message on its first lane, its backlog in it, with
  nothing else to say if need be, and every message says the cadence
  (`Up.keepalive_ms`). The server keeps when it last heard from each
  replica, on any lane (`ReplicaOverview::last_heard_at`), and when it last
  filed a row of each table from it (`ReplicaOverview::filed`).
- **Silent.** `ReplicaState::Silent`: a replica that said its keepalive and
  has said nothing for `Settings::silent_after_keepalives` (three) times
  that — off, or cut off. After a fix and an unreachable one, before
  catching up; counted (`ReplicaCounts::silent`), listed, and named in
  `Health::silent`, which leaves the health healthy. A replica that says no
  keepalive, as one of an earlier build, never is: a fleet upgrades over
  days.
- **Every replica's state at once.** `Engine::states(scope)`: each replica
  of a scope with its audience and its state, worst first, for a map of the
  fleet — a few milliseconds over 200 replicas.
- **The dashboard shows only what waits.** The ready-made `SyncDashboard`
  has counters that filter, a tile per replica worst first (from
  `Engine::states`, passed as `states`), and the replicas that need
  attention, a sentence each; a replica's page says when it was last heard
  and whether what it writes goes up live, its three numbers (to send, to
  confirm, set aside), its checks, and only the tables that wait, both ways,
  the settled folded into one line ("2 more tables up to date"). A host
  places its replicas (`levels`, `placeOf`: the tiles gather in a cluster
  per place, a cluster's header leads a level down, `v-model:level` and
  `v-model:within`), groups its tables (`group`), adds checks of its own
  (`checks`) and more under them (the slot `replica-extra`). The Tables tab
  is called Data.
- **The dashboard's core, for a host's own design system.** Pure functions
  of the answers: a `Tone` per state; `syncCheck` and `checksOf`, the
  engine's check after the host's; `standingOf` with a host's `Combine`,
  for states of its own made from the engine's and its checks; `tallyOf`;
  `attentionOf` and `lineOf`; `tilesOf`, `gridOf`, `into` and `levelUnder`
  for the tiles in clusters; `downOf`, `upOf`, `fleetOf` and `totalsOf` for
  the tables both ways, live or stalled, in the host's groups; `downChips`,
  `upChips`, `describeFlow`, `describeSettled`, `describeHeard` and
  `lasting` to say them. `useSyncDashboard` takes `states`, `levels`,
  `placeOf`, `level`, `within`, `group`, `checks`, `combine` and
  `behindAfter`, and answers `tiles`, `grid`, `tally`, `attention`,
  `tables` and `detail`. Every new sentence is in English and Spanish, and
  every new look a variable (`--msd-quiet`, `--msd-off`, their `-soft`
  tints, `--msd-accent-soft`, `--msd-ok-tile`, `--msd-tile`).

Upgrading: the replica first or the server first, either way. An earlier
replica says no age and no keepalive, and is judged as before but for
silence; an earlier server ignores both.

### Breaking

- **Server schema 5.** `Server::install` takes the database there; with
  migrations of your own, run `plan::migration(5)`, which adds
  `sync.replicas.last_heard_at`, `keepalive` and `filed`. Stop the server's
  processes of the earlier build before the upgrade (*Upgrading*, in the
  book).
- **Replica schema 3.** `replica::install` takes the database there; with
  migrations of your own, run `replica::migration(3)`, which adds
  `sync_notes.noted_at`, the trigger `sync_notes_noted` that stamps it, and
  `sync_spoken`. The triggers `replica::triggers` prints are printed alike.
- **`Outbox::backlog_every` is `Outbox::keepalive_every`.**
- **Dashboard contract 2.** `catching_up` means what waits, not what is on
  its way; `silent` is a state; `ReplicaOverview` says `last_heard_at` and
  `filed`, `UpProgress` `oldest_at`, `Health` `silent`, and `States` is the
  answer of `Engine::states`. The dashboard reads contract 2, and refuses 1.
- **The ready-made dashboard's filters are counters,** drawn by
  `.msd-counter` where they were `.msd-chip`, and a state's colour is its
  tone (`data-tone`) where it was its state (`data-state`): a stylesheet of
  a host's that styled those is restyled.
- The public API adds `Engine::states`, `States`, `StateEntry`, `Silent`,
  `TableFiled`, `ReplicaState::Silent`, `ReplicaCounts::silent`,
  `Health::silent`, `ReplicaOverview::{last_heard_at, filed}`,
  `UpProgress::oldest_at` and server `Settings::{behind_after,
  silent_after_keepalives}`; it renames `Outbox::backlog_every` to
  `Outbox::keepalive_every`.

## 0.3.0 — 2026-10-23

Rows a view places. Schemas: server 4, replica 2 (unchanged). Protocol 1
(unchanged). Dashboard contract 1 (a note kind added).

- **Routed tables.** `sync = "routed(view = v, row = col, replica = col)"`:
  a replica holds a row where a view of the host's pairs the two — in its
  partition, when the replicas are partitioned, which the view says too,
  and which a row with the partition column is of as well. The view decides
  who holds a row, and `active` still which rows travel at all; a pair
  answered twice is owed once. `view` named alone is the one the search
  path finds. `row = (a, b)` names the view's columns of a key of two, in
  key order. `assigned` and `grouped` are such views, and the engine's tests
  hold the two ways to sending alike.
- **What the view reads, read off the catalog.** When the server starts, it
  reads what the view reads where PostgreSQL keeps it (`pg_depend`), column
  by column, through the views it reads and the functions with a
  SQL-standard body (`BEGIN ATOMIC … END`, `RETURN …`) it calls, an
  operator's too. What PostgreSQL ships, and what an extension installed,
  reads no table of the host's, and has to be immutable: `now()`,
  `current_date` or `current_setting()` answer otherwise with no write. A
  view that calls any other function, or one of those that is not
  immutable, or reads a materialized view, a foreign table, a partitioned
  table or a table of an inheritance tree, is refused, naming it; so is one
  that reads a `replica_owned` column of the routed table, whose writes note
  no change of the row, lacks a column the route names, or holds the key in
  another type.
- **Re-routes.** Every table the view reads, but the routed table itself,
  gets four statement-level triggers (`sync_reroute_insert`, `_update`,
  `_delete` and `_truncate`), each noting every partition its statement
  touched once — an update only where what the views read changed, a
  truncate every partition — as a note of the kind `rerouted`. The planner
  asks the view again for every replica of the partition, and sends the
  difference. A change of the routed row is planned by its key, as any
  row's. A view written otherwise provisions every replica of its audience
  again, as a declaration changed. The status lists a parked re-route as
  `NoteKind::Rerouted`.

### Breaking

- **Server schema 4.** `Server::install` takes the database there; with
  migrations of your own, run `plan::migration(4)`, which admits the new
  kind of note and adds `sync.note_reroute()`. The triggers an earlier build
  printed are printed alike: print them again once a table is routed by a
  view.

## 0.2.0 — 2026-10-19

One server, several kinds of replica. Schemas: server 3, replica 2.
Protocol 1 (unchanged). Dashboard contract 1 (fields added).

- **Audiences.** A server serves one kind of replica or several from one
  stream of changes: `Audience::new(name, replicas, codecs)`. Each audience
  holds the tables it declares, each encoded as its own codec says; one may
  hold a part of what another holds, or a table in a shape of its own. A
  table two audiences declare has one trigger, which notes what either
  needs of it, and is planned for each against its own replicas. A replica
  two audiences count live is the first's, unless it is live in the other
  already; an audience renamed takes its replicas over, provisioning them
  again, and one no longer declared lets them go. The wire does not change:
  a replica is the same whatever audience it is of.
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
  the engine recorded are taken over by the audience that counts them live,
  which compares its declarations with what 0.1 last ran with, wherever it
  is declared among the audiences: unchanged, nothing is provisioned again.

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
