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

`products.updated_at` is a column like any other: a register takes what the
head office sends, whatever its version, because the order of the messages —
not a column — says which is the latest. A column that orders versions
(`#[row(version)]`) belongs to a table a register edits and the head office
files, such as a record a register corrects.

### The engine's part: installed for you, or in your migrations

The engine keeps its own tables in a `sync` schema, and puts a trigger on every
table that travels down, noting each change in the writing transaction — so a
schema of yours by that name needs another: the server refuses to install
into it, or to read it as its own. There are two ways to get them there.

> **You write**, if you have migrations — the way a system in production
> should — the engine's schema in one of them: its first step,
> [`magic_sync::plan::migration`]`(1)`, one SQL script. Then the triggers, in the
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

The planner looks rows up by more than their keys: by the columns that point
at another mirrored table, by a row's replica or partition column, by the
column a table follows, and a group's members by their row and their group,
its assignments by their group and their replica. On a large table, a lookup
no index serves reads the whole table on every pass, so the server needs an
index for each: one of yours serves when it leads with those columns, in any
order. [`Engine::indexes`] prints, one `CREATE INDEX` per line, those your
database does not have — write them in the migration beside the triggers.

> **What happens:** forget one, and the server refuses to start, printing the
> indexes it needs.

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
> needs, writes every trigger that is missing or not as the engine needs
> it, and creates every index it needs that no index serves. Run it before anything writes a mirrored table: a write before its
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
> screens leave retired rows out. Offered again, it is live again. Once
> nothing points at it any more, it goes, with the next message the register
> applies.

A retired row keeps its values only for what points at it: a unique index on
such a table leaves the retired rows out, or a product deleted and made again
with the same code would be refused here while the old one stays. `check`
refuses one that does not:

```sql
CREATE UNIQUE INDEX products_code ON products (code) WHERE retired_at IS NULL;
```

After every message it applies, in a transaction of its own, the replica
looks for the retired rows that nothing keeps any more — no row of any
table, live or retired, points at them — and lets them go, children first:
on a table of many rows, an index on its retired column keeps that cheap.

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

[`magic_sync::plan::migration`]: ../api/server/magic_sync/plan/fn.migration.html
[`Engine::triggers`]: ../api/server/magic_sync/plan/struct.Engine.html#method.triggers
[`Engine::indexes`]: ../api/server/magic_sync/plan/struct.Engine.html#method.indexes
[`magic_sync::replica::migration`]: ../api/replica/magic_sync/replica/fn.migration.html
[`magic_sync::replica::install`]: ../api/replica/magic_sync/replica/fn.install.html
[`magic_sync::replica::check`]: ../api/replica/magic_sync/replica/fn.check.html
[`magic_sync::replica::triggers`]: ../api/replica/magic_sync/replica/fn.triggers.html
