# The tables

The shop has three tables on the head office's PostgreSQL:

- **registers** — the replicas. The engine reads which registers exist, and
  which of them should receive anything, from here.
- **products** — written at the head office, held by every register.
- **receipts** — written by a register, filed at the head office.

And the same tables, minus `registers`, on every register's SQLite.

## On the server

> **You write** your tables, as you always do:

```sql
CREATE SCHEMA shop;

CREATE TABLE shop.registers (
    id     UUID PRIMARY KEY,
    name   TEXT NOT NULL,
    url    TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE shop.products (
    id          UUID PRIMARY KEY,
    name        TEXT NOT NULL,
    price_cents BIGINT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);

CREATE TABLE shop.receipts (
    id          UUID PRIMARY KEY,
    register_id UUID NOT NULL,
    product_id  UUID NOT NULL,
    quantity    INTEGER NOT NULL,
    total_cents BIGINT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL
);
```

`products.updated_at` is the row's **version**: a register never overwrites a
newer copy with an older one. Stamp it with the clock *at the write*, and never
behind the value it replaces:

```sql
UPDATE shop.products SET name = $2, updated_at = clock_timestamp() WHERE id = $1;
```

Not `now()`: that is when the transaction *began*, and a transaction that
began first and committed last would stamp an older version than the row it
replaces.

### The engine's part: installed for you, or in your migrations

The engine keeps its own tables in a `sync` schema, and puts a trigger on every
table that travels down, noting each change in the writing transaction. There
are two ways to get them there.

> **You write**, if you have no migrations of your own — the example's way —
> nothing: build the server with `Server::install` instead of `Server::new`
> ([next chapters](server.md)). Every time it starts, in one transaction and
> one process at a time, it brings the `sync` schema to the version this build
> needs and writes every trigger that is missing or not as the engine needs it.

Run it before anything writes a mirrored table: a write before its trigger
exists is never noted.

> **You write**, if you do have migrations, the engine's schema in one of them
> — it is [`magic_sync::POSTGRES_SCHEMA`], one SQL script — and the triggers in
> the migration that creates the tables, as you would their indexes. You do not
> write them by hand: `Engine::triggers` prints them, from the same
> declarations the server starts from, one `CREATE TRIGGER` per line:

```sql
-- after CREATE TABLE shop.products …
CREATE TRIGGER sync_note_row AFTER INSERT OR UPDATE OR DELETE ON "shop"."products" FOR EACH ROW EXECUTE FUNCTION sync.note_row('products', '', '1', 'id', '0');
CREATE TRIGGER sync_refuse_truncate BEFORE TRUNCATE ON "shop"."products" FOR EACH STATEMENT EXECUTE FUNCTION sync.refuse_truncate();
```

The arguments say how the table is noted: its name, its partition column
(see [The server](server.md); `''` for none), its key's columns in the order
the row declares them, the columns that point at other mirrored tables (kept
of the row as it was, so a parent it no longer points at can be let go), and
the columns replicas own (in a table written on both sides; see
[Recipes](recipes.md)). Print them again when a declaration changes.

> **What happens:** forget one, write one with other arguments, or disable it,
> and the server refuses to start, naming the table and printing the trigger
> it needs. Leave a trigger on a table no row declares, and it refuses too: its
> changes would be noted and never sent.

## On a register

> **You write** the same tables, with one more column on each table that comes
> down and that a register's own rows point at:

```sql
CREATE TABLE products (
    id          BLOB PRIMARY KEY NOT NULL,
    name        TEXT NOT NULL,
    price_cents INTEGER NOT NULL,
    updated_at  TEXT NOT NULL,
    retired_at  TEXT
);

CREATE TABLE receipts (
    id          BLOB PRIMARY KEY NOT NULL,
    register_id BLOB NOT NULL,
    product_id  BLOB NOT NULL REFERENCES products (id),
    quantity    INTEGER NOT NULL,
    total_cents INTEGER NOT NULL,
    created_at  TEXT NOT NULL
);
```

> **What happens:** when the head office deletes a product a receipt here
> points at, the register cannot delete it without breaking the receipt. It
> sets `retired_at` instead: the product stays for the receipt, and your
> screens leave retired rows out. Offered again, it is live again.

Same two ways for the engine's part: [`magic_sync::replica::install`] creates
the engine's tables and a trigger noting every write to each table that goes
up; or your migrations run [`magic_sync::SQLITE_SCHEMA`] and declare the
triggers themselves:

```sql
CREATE TRIGGER sync_note_receipts AFTER INSERT ON receipts BEGIN
    INSERT INTO sync_notes (relation, row_key) VALUES ('receipts', lower(hex(NEW.id)));
END;
```

…and a test runs [`magic_sync::replica::check`] over the database your
migrations build, so a table that goes up and notes nothing fails it.

[`magic_sync::POSTGRES_SCHEMA`]: https://docs.rs/magic_sync/latest/magic_sync/constant.POSTGRES_SCHEMA.html
[`magic_sync::SQLITE_SCHEMA`]: https://docs.rs/magic_sync/latest/magic_sync/constant.SQLITE_SCHEMA.html
[`magic_sync::replica::install`]: https://docs.rs/magic_sync/latest/magic_sync/replica/fn.install.html
[`magic_sync::replica::check`]: https://docs.rs/magic_sync/latest/magic_sync/replica/fn.check.html
