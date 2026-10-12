# The shop

A head office (PostgreSQL) and its registers (SQLite), each with a page in the
browser. See [Run the shop](https://rsansores.github.io/magic_sync/getting-started.html) for how to start
it, and the rest of [the book](https://rsansores.github.io/magic_sync/) for how it is built.

- `rows/` — the tables, as both sides hold them, and their codecs
- `server/` — the head office: the catalog editor, the sync server, the
  engine's answers for the dashboard, and the transport — a queue per
  register in the head office's database, read over HTTP, each message
  acknowledged by its id
- `web/` — the head office's page: a Vue app with the sync dashboard
  (`magic_sync_dashboard`); built with `pnpm install && pnpm build`, served by
  `server/`. It takes the dashboard as a copy of `../../../dashboard` made at
  install: after changing the dashboard, build it and install again
  (`rm -rf node_modules && pnpm install`)
- `register/` — a register: sells, works unplugged, sends its receipts up

`server/` and `register/` are a workspace each, because a build of
`magic_sync` is one side ([Set up a project](https://rsansores.github.io/magic_sync/guide/setup.html)).
They reach the engine and the dashboard in this repository by path; a copy
outside it leaves `path` out of each `Cargo.toml`, and depends on
`magic_sync_dashboard` from npm.
