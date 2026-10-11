# The tables

The shop has three tables on the head office's PostgreSQL:

- **registers** — the replicas. The engine reads which registers exist, and
  which of them should receive anything, from here.
- **products** — written at the head office, held by every register.
- **receipts** — written by a register, filed at the head office.

And the same tables, minus `registers`, on every register's SQLite.

## On the server

> **You write** your tables, as you always do
> (`examples/shop/server/schema.sql`):

```sql
{{#include ../../../examples/shop/server/schema.sql:tables}}
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
table that travels down, noting each change in the writing transaction — so a
schema of yours by that name needs another: the server refuses to install
into it, or to read it as its own. There are two ways to get them there.

> **You write**, if you have migrations — the way a system in production
> should — the engine's schema in one of them: it is
> [`magic_sync::POSTGRES_SCHEMA`], one SQL script. Then the triggers, in the
> migration that creates the tables, as you would their indexes. You do not
> write them by hand: [`Engine::triggers`] prints them, from the same
> declarations the server starts from, one `CREATE TRIGGER` per line:

```rust,ignore
{{#include ../../snippets/src/server.rs:triggers}}
```

For the shop, it prints:

```sql
{{#include shop-triggers.sql}}
```

The arguments say how the table is noted: its name, its partition column
(see [The server](server.md); `''` for none), its key's columns in the order
the row declares them, the columns that point at other mirrored tables (kept
of the row as it was, so a parent it no longer points at can be let go), and
the columns replicas own (in a table written on both sides; see
[Recipes](recipes.md)) — each list after its length. Print them again when a
declaration changes.

> **What happens:** forget one, write one with other arguments, or disable it,
> and the server refuses to start, naming the table and printing the trigger
> it needs. Leave a trigger on a table no row declares, and it refuses too: its
> changes would be noted and never sent.

> **Mind:** a trigger as printed does not fire in a session whose
> `session_replication_role` is `replica` — a logical replication
> subscription applying changes, a restore told to disable triggers — so
> what such a session writes reaches no replica. If one writes your mirrored
> tables, enable their triggers always
> (`ALTER TABLE … ENABLE ALWAYS TRIGGER sync_note_row`, and the same for
> `sync_refuse_truncate` and `sync_note_group`): the server takes a trigger
> enabled always, and refuses one enabled for replication sessions only.

> **You write**, if you have no migrations — the shop's way — nothing: build
> the server with `Server::install` instead of `Server::new`
> ([The server](server.md)). Every time it starts, in one transaction and one
> process at a time, it brings the `sync` schema to the version this build
> needs and writes every trigger that is missing or not as the engine needs
> it. Run it before anything writes a mirrored table: a write before its
> trigger exists is never noted.

## On a register

> **You write** the same tables, with one more column on each table that comes
> down and that a register's own rows point at
> (`examples/shop/register/schema.sql`):

```sql
{{#include ../../../examples/shop/register/schema.sql:tables}}
```

> **What happens:** when the head office deletes a product a receipt here
> points at, the register cannot delete it without breaking the receipt. It
> sets `retired_at` instead: the product stays for the receipt, and your
> screens leave retired rows out. Offered again, it is live again.

Same two ways for the engine's part: your migrations run the engine's
first step, [`magic_sync::replica::migration`]`(1)`, and the script
[`magic_sync::replica::triggers`] writes for your codecs — print them once
and paste them into the migration that creates the tables, and the triggers
again when a table is added or changes how it travels:

```rust,ignore
{{#include ../../snippets/src/replica.rs:triggers}}
```

or, without migrations, [`magic_sync::replica::install`] applies the engine's
tables and its triggers in one transaction, every time the replica starts.

The triggers note every insert, update and delete of a table that goes up
(an update that changes the key notes the old key too), refuse a write the
engine did not make to a table the server sends, let a row of a two-way table
begin only on the server, and refuse an update of history (a record without
a version). `Replica::new` runs [`magic_sync::replica::check`], which refuses
a database whose triggers are not exactly these; a test of yours runs it too,
over the database your migrations build.

[`magic_sync::POSTGRES_SCHEMA`]: ../api/server/magic_sync/constant.POSTGRES_SCHEMA.html
[`Engine::triggers`]: ../api/server/magic_sync/plan/struct.Engine.html#method.triggers
[`magic_sync::replica::migration`]: ../api/replica/magic_sync/replica/fn.migration.html
[`magic_sync::replica::install`]: ../api/replica/magic_sync/replica/fn.install.html
[`magic_sync::replica::check`]: ../api/replica/magic_sync/replica/fn.check.html
[`magic_sync::replica::triggers`]: ../api/replica/magic_sync/replica/fn.triggers.html
