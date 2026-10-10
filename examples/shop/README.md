# The shop

A head office (PostgreSQL) and its registers (SQLite), each with a page in the
browser. See [Run the shop](https://rsansores.github.io/magic_sync/getting-started.html) for how to start
it, and the rest of [the book](https://rsansores.github.io/magic_sync/) for how it is built.

- `rows/` — the tables, as both sides hold them
- `server/` — the head office: the catalog editor, the sync server, and the
  engine's answers for the dashboard
- `web/` — the head office's page: a Vue app with the sync dashboard
  (`magic_sync_dashboard`); built with `pnpm install && pnpm build`, served by
  `server/`. Outside this repository, depend on `magic_sync_dashboard` from
  npm instead of `file:../../../dashboard`
- `register/` — a register: sells, works unplugged, sends its receipts up
