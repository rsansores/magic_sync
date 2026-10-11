# Contributing

Issues and pull requests are welcome.

## Running the tests

A build of `magic_sync` is one side, so the tests run once per side. The
server's need a PostgreSQL (14 or later) whose user may create databases —
each test gets one of its own:

```sh
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
cargo test -p magic_sync --features postgres
cargo test -p magic_sync --features sqlite
```

The benchmarks of the operations that move many rows at once:

```sh
cargo test -p magic_sync --features postgres bench_ -- --ignored --nocapture --test-threads 1
```

The derive's suite (`tests/derive.rs`) builds every case under
`tests/derive/` as a host would: what it accepts compiles, and each misuse
fails with the message checked in beside it. A change to a message is
written anew with `TRYBUILD=overwrite`, and read before it is committed.

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

## What CI checks

Formatting, clippy and docs on each side, the tests against PostgreSQL 14 and
18, the public API, the minimum Rust version (1.85), the example's two
programs, and the book. See `.github/workflows/ci.yml`.

## A change to the protocol

`magic_sync/PROTOCOL.md` is a contract other implementations follow. A change
that a replica of an earlier version cannot read raises `PROTOCOL` in
`src/envelope.rs`, and says so in the document and the changelog. A test keeps
`proto/sync.proto` and the engine's messages identical, field for field.
