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

## What CI checks

Formatting, clippy and docs on each side, the tests against PostgreSQL 14 and
18, the minimum Rust version (1.85), the example's two programs, and the book.
See `.github/workflows/ci.yml`.

## A change to the protocol

`magic_sync/PROTOCOL.md` is a contract other implementations follow. A change
that a replica of an earlier version cannot read raises `PROTOCOL` in
`src/envelope.rs`, and says so in the document and the changelog. A test keeps
`proto/sync.proto` and the engine's messages identical, field for field.
