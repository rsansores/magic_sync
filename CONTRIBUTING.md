# Contributing

Issues and pull requests are welcome.

A build of `magic_sync` is one side — `postgres` for the server, `sqlite` for
a replica — so most of what follows runs once per side.

## The tests

The server's need a PostgreSQL (14 or later) whose user may create databases:
each test gets one of its own.

```sh
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
cargo test -p magic_sync --features postgres
cargo test -p magic_sync --features sqlite
```

Each run includes the derive's suite (`magic_sync/tests/derive.rs`), which
builds every case under `magic_sync/tests/derive/` as a host would, on that
side: what it accepts compiles, and each misuse fails with the message checked
in beside it. The messages are the compiler's words, so they are held to one
release of rustc, named in the test (`PINNED`); on any other the suite builds
what is accepted and says it left the messages alone. CI's `diagnostics` job
runs them on the pinned one. A change to a message is written anew with that
toolchain and `TRYBUILD=overwrite`, and read before it is committed. A
misuse the derive refuses has a case under `tests/derive/fail/`; a new
refusal comes with its case.

Some tests compare what the engine makes with a file checked in — the
messages generated from `proto/sync.proto`, the replica's golden messages,
the triggers the book shows for the shop — and `MAGIC_SYNC_BLESS=1` writes
them anew; the dashboard's types and fixtures are written by the status
tests, with `UPDATE_CONTRACT=1`. Read the difference before committing it.

The planner's walks (`every_replica_holds_exactly_what_its_strategies_say`
and `whatever_fails_on_the_way…` in `plan_tests.rs`) take the seeds in
`SEEDS`; `MAGIC_SYNC_WALK_SEEDS` gives them others — seeds, comma-separated,
or `random` — and each walk prints the seeds it took. A nightly workflow runs
them with `random`: a walk that fails there is taken again with
`MAGIC_SYNC_WALK_SEEDS` set to the seeds it printed, in their order — how
much a pass plans follows a seed's place — and they join `SEEDS` with the
fix.

## The benchmarks

The operations that move many rows at once, the dashboard's reads over
200 replicas holding 3 million rows, and every state of 4000 replicas. They seed with `uuidv7()`, so they need
PostgreSQL 18; the suite itself runs on 14 and later:

```sh
cargo test -p magic_sync --features postgres bench_ -- --ignored --nocapture --test-threads 1
```

## The public API

What a host can name on each side is listed in `magic_sync/public-api/`, and
CI fails when the crate's API is not that list. A change to the API is a
change to the list, made on purpose:

```sh
rustup toolchain install nightly-2026-10-10 --profile minimal
cargo install cargo-public-api --version 0.52.0 --locked
for side in postgres sqlite; do
  cargo +nightly-2026-10-10 public-api -p magic_sync --features $side -s > magic_sync/public-api/$side.txt
done
```

## The book, the README and the example

The book (`book/`) shows code that compiles. What comes from the shop
(`examples/shop`) is quoted from it through mdbook's
`{{#include file:anchor}}`, between `ANCHOR:` and `ANCHOR_END:` comments in the
example's files; the rest is in `book/snippets`, a crate of its own built once
per side, and quoted the same way. Every Rust block of the README is code of
`book/snippets/src/readme.rs`, line for line, which a test of that crate
holds it to.

`scripts/site.sh` builds what GitHub Pages serves — the book, and the API
reference of each side under `api/` — into `target/site`, and
`scripts/check-links.sh` checks every link the README, the book, the
protocol, the other documents and the rustdoc comments make into the
repository, anchors included, against those files: no network. It needs
[mdbook](https://rust-lang.github.io/mdBook/) and
[lychee](https://lychee.cli.rs).

The example's two programs, the rows they share and the book's snippets are
each a workspace of their own (a workspace builds its members together, and a
build is one side): run `cargo fmt` and `cargo clippy` with
`--manifest-path`, as CI does.

## What CI checks

`.github/workflows/ci.yml`, on every pull request and every push to main:

- **lint** — `cargo fmt --check` of the workspace, the example's crates and
  the book's snippets; `cargo clippy -D warnings` of the engine on each side
  (the derive with the replica's), of the example's two programs, and of the
  snippets on each side; the derive's docs with `-D warnings`.
- **dependencies** — `cargo deny check` (advisories, licenses, sources and
  unmaintained crates anywhere in the graph; `deny.toml`) of the workspace, the
  example's two programs and the book's snippets; **unused** — `cargo machete`
  over every workspace. A scheduled workflow (`advisories.yml`) runs the
  advisories check daily, on days nothing is pushed, and Dependabot proposes
  the new versions of the actions, the crates and the dashboard's and the
  example page's npm packages.
- **workflows** — zizmor over `.github/`: every action pinned to a commit,
  no token left behind by a checkout, no expression injected into a script.
- **test-server** — the server's suite against PostgreSQL 14 and 18, and the
  book's snippets built on the server's side.
- **moved-database** — the server's database moved between two clusters
  mid-walk (`pg_dump` into `pg_restore`, the first one's transaction ids far
  ahead of the second's) keeps sending what changes
  (`scripts/moved-database.sh`; it needs PostgreSQL's server programs).
- **diagnostics** — the derive's refusals, message by message, on the pinned
  rustc.
- **test-replica** — a replica's suite, and the snippets' test of the README.
- **public-api** — each side's API against `magic_sync/public-api/`.
- **msrv** — each side builds on Rust 1.85.
- **example-page** — the dashboard builds from source, and the shop's page
  (formatting, lint, types) over it.
- **dashboard** — its formatting, lint, types, unused code (knip), tests,
  and the packed tarball as npm and TypeScript read it.
- **site** — the book and both sides' API docs build, the docs with
  `-D warnings`, and every link into the repository resolves.
- **package** — one version everywhere (`scripts/check-versions.sh`), and
  `cargo package` of both crates, each verified by building the tarball on
  each side.

## A change to the protocol

`magic_sync/PROTOCOL.md` is a contract other implementations follow. A change
that a replica of an earlier version cannot read raises `PROTOCOL` in
`src/envelope.rs`, and says so in the document and the changelog. A test keeps
`proto/sync.proto` and the engine's messages identical, field for field.

## A change to the engine's tables

The engine's schema on each side is a list of steps (`MIGRATIONS` in
`src/plan.rs` and `src/replica.rs`), one per `SERVER_SCHEMA_VERSION` or `REPLICA_SCHEMA_VERSION`, each side counting
its own. A released step is never edited: a change is a new step, that side's
version raised by one,
and a test that upgrades a database of the version before it. The changelog
says so, and names the step for hosts with migrations of their own.
