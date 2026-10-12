# Set up a project

A system kept in sync by `magic_sync` is at least two programs: the
**server**, on PostgreSQL, and a **replica**, on SQLite, that runs on every
device. They share one thing — the tables that travel, as Rust structs — and
almost nothing else. The shop is laid out so:

```text
shop/
├── rows/       the tables, as both sides hold them: a library
├── server/     the head office: a program, and a workspace of its own
└── register/   a register: a program, and a workspace of its own
```

A build of `magic_sync` is **one side**: the `postgres` feature builds the
server's, the `sqlite` feature a replica's. A build with both is refused —
the crate does not compile — and so is a build with neither. Everything in
this chapter follows from that.

## The rows: one crate, no side of its own

> **You write** a library for the rows and their codecs, which depends on
> `magic_sync` without picking a side, and lets each program pick one through
> a feature of its own:

```toml
{{#include ../../../examples/shop/rows/Cargo.toml}}
```

What only one side has is behind its feature: where the server finds the
replicas (`magic_sync::plan::Replicas`) exists only on the server, so the
shop's `REGISTERS` is `#[cfg(feature = "server")]`. The rows themselves and
the codec list compile on either side ([The rows](rows.md),
[codecs](codecs.md)).

## The server

> **You write** a program with the `postgres` side, and the rows built for
> it:

```toml
{{#include ../../../examples/shop/server/Cargo.toml}}
```

sqlx is the engine's database layer: the pool you hand it is sqlx's, and so
are the transactions and connections it takes. Enable its `postgres` driver
and a runtime; the engine runs on tokio.

## A replica

> **You write** a program with the `sqlite` side:

```toml
{{#include ../../../examples/shop/register/Cargo.toml}}
```

## One workspace per side

Cargo builds the members of a workspace together and turns on, for a crate
they share, every feature any of them asks for. Put the server and a replica
in one workspace and the first `cargo build`, `cargo test` or editor check
that builds both builds `magic_sync` with both sides, and the crate refuses
to compile, saying so.

So each program is a workspace of its own: an empty `[workspace]` table in its
manifest, as above, or one workspace for everything on the server and another
for everything on the replica. The rows are a path dependency of both, and
belong to neither. Each side's tests run in its own workspace, and a test
never needs the other side's build: the server's tests drive the server,
a replica's tests drive the replica.

A replica that is not written in Rust needs none of this: it follows
[the replica protocol](../protocol.md), and only the server uses the crate.

## Outside this repository

The shop reaches the engine by path, because it lives next to it; a project
of yours depends on the published crates, and leaves `path` out:

```toml
magic_sync = { version = "0.1.0", features = ["postgres"] }
```

The rest of this part writes what goes in these crates: the tables, the rows,
the codecs, then each side.
