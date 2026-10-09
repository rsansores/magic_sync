# The shop

A head office (PostgreSQL) and its registers (SQLite), each with a page in the
browser. See [Run the shop](../../book/src/getting-started.md) for how to start
it, and the rest of [the book](../../book/src/SUMMARY.md) for how it is built.

- `rows/` — the tables, as both sides hold them
- `server/` — the head office: the catalog editor, and the sync server
- `register/` — a register: sells, works unplugged, sends its receipts up
