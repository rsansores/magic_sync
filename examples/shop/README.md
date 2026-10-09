# The shop

A head office (PostgreSQL) and its registers (SQLite), each with a page in the
browser. See [Run the shop](https://rsansores.github.io/magic_sync/getting-started.html) for how to start
it, and the rest of [the book](https://rsansores.github.io/magic_sync/) for how it is built.

- `rows/` — the tables, as both sides hold them
- `server/` — the head office: the catalog editor, and the sync server
- `register/` — a register: sells, works unplugged, sends its receipts up
