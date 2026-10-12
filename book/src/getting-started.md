# Run the shop

The example is two small programs: the **head office** (PostgreSQL) and a
**register** (SQLite), each with a page in the browser. You will start one
head office and two registers, edit the catalog, unplug a register, sell
while it is unplugged, and plug it back in.

You need [Rust](https://rustup.rs) 1.85 or later, Node 22.13 or later with
[pnpm](https://pnpm.io), and PostgreSQL 14 or later (Docker brings one).
Every command below runs from the root of a clone:

```sh
git clone https://github.com/rsansores/magic_sync
cd magic_sync
```

## 1. A database for the head office

With Docker:

```sh
docker compose -f examples/shop/docker-compose.yml up -d
```

That starts PostgreSQL on port 5433 with the user, password and database
`shop`, which is what the head office connects to unless you set
`DATABASE_URL`. Without Docker Compose, the same with plain Docker:

```sh
docker run -d --name shop-postgres -p 5433:5432 \
  -e POSTGRES_USER=shop -e POSTGRES_PASSWORD=shop -e POSTGRES_DB=shop postgres:17
```

## 2. The head office

Its page is a Vue app (`examples/shop/web`) over the engine's dashboard, which
is built from this repository first:

```sh
pnpm --dir dashboard install
pnpm --dir dashboard build
pnpm --dir examples/shop/web install
pnpm --dir examples/shop/web build
```

Then the head office itself:

```sh
cargo run --manifest-path examples/shop/server/Cargo.toml
```

Open <http://127.0.0.1:8080>. The first time, it installs everything it needs
in the database: its own tables, and the engine's.

## 3. Two registers

Each in a terminal of its own, from the root of the clone:

```sh
cargo run --manifest-path examples/shop/register/Cargo.toml -- --name "Register 1" --port 8081
```

```sh
cargo run --manifest-path examples/shop/register/Cargo.toml -- --name "Register 2" --port 8082
```

Each introduces itself to the head office, which provisions it, and appears on
the head office's page with its own page inside. A register keeps its
database in the directory it was started from (`register-8081.db`): started
there again, it is the same register.

Both programs listen on `127.0.0.1` only: the example has no login. To open it
from another machine (over a VPN, say), bind them to every interface:

```sh
cargo run --manifest-path examples/shop/server/Cargo.toml -- --bind 0.0.0.0:8080
cargo run --manifest-path examples/shop/register/Cargo.toml -- --name "Register 1" --port 8081 --bind 0.0.0.0
```

Ctrl-C stops either one, and loses nothing it was doing.

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
- **Restart the head office** while a register is unplugged and a product
  has changed. What the head office queued for the register is in its
  database: plugged back in, the register gets it.
- **Ask where a product is.** *who holds it?* next to a product lists each
  register and where that product stands on it: holds it, on its way, set
  aside. That is the question to answer next to a row, in your own listings.
- **Break something.** Start a register with a bug that refuses one product:

  ```sh
  cargo run --manifest-path examples/shop/register/Cargo.toml -- --name "Register 3" --port 8083 --refuse "Tea"
  ```

  Add *Tea* at the head office: Register 3 turns red, *needs a fix*, the
  Problems tab says why, and every other product still reaches it. Stop it
  and start it again without `--refuse` — the fix deployed — and within
  seconds the head office's retry gets *Tea* through and the page is green.
- **Lose a register.** *cut the line* under a register makes every send to it
  fail, as a network would: it turns *unreachable*, and the head office tries
  it again, waiting longer each time. *mend the line*, and it catches up.
- **Delete a product a register has sold.** That register keeps the product
  for its receipts — it *retires* it: out of the list, still there for what
  points at it — and the other deletes it.
- **Wish a register another footer.** Each register prints a footer of its
  own under its receipts, and the head office may wish it another (*wish
  another*, under the register): it changes on the register within a second,
  and *Footers wished* says *applied*. Turn the register's printer off first
  and the wish *waits* — *the printer is off* — until it is back on. Wish one
  longer than the printer's 40 characters and the register *refuses* it.
  Change the footer on the register itself while the head office's form is
  open, then wish: the wish was based on the footer the page showed, so it is
  *changed there first* — superseded — and the register's own stands. Unplug
  a register and wish: it is *on its way* until the register is back. Every
  time, both pages end showing the same footer
  ([Settings and wishes](guide/settings.md)).

The rest of the book builds this from scratch.
