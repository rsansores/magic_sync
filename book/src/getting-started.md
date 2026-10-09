# Run the shop

The example is two small programs: the **head office** (PostgreSQL) and a
**register** (SQLite), each with a page in the browser. You will start one
head office and two registers, edit the catalog, unplug a register, sell
while it is unplugged, and plug it back in.

## 1. A database for the head office

Any PostgreSQL 14 or later will do. With Docker:

```sh
cd examples/shop
docker compose up -d
```

That starts one on port 5433 with the user, password and database `shop`,
which is what the head office connects to unless you set `DATABASE_URL`.

## 2. The head office

```sh
cd examples/shop/server
cargo run
```

Open <http://127.0.0.1:8080>. The first time, it installs everything it needs
in the database: its own tables, and the engine's.

## 3. Two registers

In two more terminals:

```sh
cd examples/shop/register
cargo run -- --name "Register 1" --port 8081
cargo run -- --name "Register 2" --port 8082
```

Each introduces itself to the head office, which provisions it, and appears on
the head office's page with its own page inside.

## 4. Things to try

- **Add a product** at the head office. Both registers have it within a
  second; the bar over each says how many products it has *confirmed* it
  holds.
- **Unplug Register 2** (its *Unplug* button). Sell a few things on it: it
  keeps selling, and says how many receipts are not at the head office yet.
  Change a price and add a product at the head office: Register 2's bar falls
  behind, and a message waits for it.
- **Plug it back in.** Its receipts reach the head office, and it catches up
  with the catalog, in order.
- **Delete a product a register has sold.** That register keeps the product
  for its receipts — it *retires* it: out of the list, still there for what
  points at it — and the other deletes it.

The rest of the book builds this from scratch.
