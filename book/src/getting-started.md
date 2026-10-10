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
Without Docker Compose, the same with plain Docker:

```sh
docker run -d --name shop-postgres -p 5433:5432 \
  -e POSTGRES_USER=shop -e POSTGRES_PASSWORD=shop -e POSTGRES_DB=shop postgres:17
```

## 2. The head office

Its page is a Vue app (`examples/shop/web`) over the engine's dashboard, so it
is built once, with Node 20 or later and [pnpm](https://pnpm.io):

```sh
(cd dashboard && pnpm install && pnpm build)
(cd examples/shop/web && pnpm install && pnpm build)
```

Then the head office itself:

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

Both listen on `127.0.0.1` only: the example has no login. To open it from
another machine (over a VPN, say), bind them to every interface:

```sh
cargo run -- --bind 0.0.0.0:8080                                  # the head office
cargo run -- --name "Register 1" --port 8081 --bind 0.0.0.0       # a register
```

## 4. Things to try

- **Add a product** at the head office. Both registers have it within a
  second, and the dashboard stays green: *everything is in sync*.
- **Unplug Register 2** (its *Unplug* button). Sell a few things on it: it
  keeps selling, and says how many receipts are not at the head office yet.
  Change a price and add a product at the head office: the dashboard turns
  amber, *catching up*, and after half a minute says Register 2 is not
  answering. Click it to see how far along it is, table by table.
- **Plug it back in.** Its receipts reach the head office, it catches up
  with the catalog, in order, and the dashboard turns green again.
- **Delete a product a register has sold.** That register keeps the product
  for its receipts — it *retires* it: out of the list, still there for what
  points at it — and the other deletes it.

The rest of the book builds this from scratch.
