# Rows a view places

`assigned` and `grouped` are two shapes of one question — which replicas
should hold this row — that the engine knows how to ask. Some rules have no
such shape: a notice granted or kept from regions and stores, the rule
nearest the store winning; a document every device at a site may read, unless
the site is closed. A rule like that is a query, and you already write
queries: write it as a view, and route the table by it.

## Routing by a view

The shop sends notices — a recall, a new price list — to the registers of
the stores a rule grants them. A rule names a region or one store, and grants
the notice or keeps it from there; a store's own rule wins over its
region's:

```sql
CREATE TABLE shop.notices (id UUID PRIMARY KEY, title TEXT NOT NULL, body TEXT NOT NULL);
CREATE TABLE shop.notice_rules (
    notice_id UUID NOT NULL REFERENCES shop.notices (id),
    region_id UUID REFERENCES shop.regions (id),
    store_id UUID REFERENCES shop.stores (id),
    grants BOOLEAN NOT NULL,
    note TEXT);

-- Whether a notice reaches a store: its own rule, else its region's.
CREATE FUNCTION shop.granted(p_notice UUID, p_store UUID) RETURNS BOOLEAN
    LANGUAGE sql STABLE
BEGIN ATOMIC
    SELECT coalesce(
        (SELECT r.grants FROM shop.notice_rules r
          WHERE r.notice_id = p_notice AND r.store_id = p_store),
        (SELECT r.grants FROM shop.notice_rules r JOIN shop.stores s ON s.region_id = r.region_id
          WHERE r.notice_id = p_notice AND s.id = p_store AND r.store_id IS NULL),
        false);
END;

-- Who holds a notice: every register of a store it reaches.
CREATE VIEW shop.notice_registers AS
    SELECT n.id AS notice_id, g.id AS register_id
      FROM shop.notices n CROSS JOIN shop.registers g
     WHERE shop.granted(n.id, g.store_id);
```

> **You write** the row, routed by the view
> (`book/snippets/src/rows.rs`):

```rust,ignore
{{#include ../../snippets/src/rows.rs:routed}}
```

`view` is the relation, `view` or `schema.view` — named alone, the one the
server's search path finds; `row` its columns that
hold the row's key — `row = (shelf_id, number)` for a key of two, in key
order; `replica` its uuid column naming the replica. With replicas
partitioned ([Recipes](recipes.md#several-companies-on-one-server)), the view
has the partition column too, by the same name, and says which partition
each pair is of.

> **What happens:** a replica should hold a row when the view pairs the two,
> in the replica's partition — and in the row's, when the row has the
> partition column, as a broadcast row is: a view that pairs one company's
> notice with another's register sends that register nothing. A row without
> the column, a table of every company's, goes where the view's partition
> column says. The view decides who holds a row; `active`, if
> the row has it, still decides which rows travel at all: an archived notice
> the view pairs with a register is not sent. A view may answer a pair more
> than once, and the row is owed once. What the row points at goes with it,
> and what follows it, as with any rule. The server holds the view to the row
> when it starts: a view that is not there, lacks a column the route names,
> or holds the key in a column of another type, is refused, naming it.

## What the view reads

A notice goes to other registers when a rule changes, when a store moves to
another region, when a register moves to another store — when anything the
view reads changes. The engine does not ask you for that list. One forgotten
table would be a replica that keeps rows it should no longer hold, and
nothing would say so.

> **What happens:** when the server starts, it reads what the view reads
> where PostgreSQL keeps it: a view depends, column by column, on every
> column it reads (`pg_depend`). A view it reads is read in turn, whole —
> every column that view reads. A function with a **SQL-standard body**
> (`BEGIN ATOMIC … END`, or `RETURN …`, PostgreSQL 14 and later) depends on
> what its body reads, and is read the same way, as is the function behind
> an operator. For the shop: the notices' rules, by `notice_id`, `region_id`,
> `store_id` and `grants` — not `note`, which nothing reads — the stores by
> `id` and `region_id`, and the registers by `id` and `store_id`.

What PostgreSQL ships reads no table of yours: the catalog records no
dependency on it at all. A function an extension installed is taken to read
none either. Neither is read through, so each has to be **immutable** — to
answer the same for the same arguments: `now()`, `current_date`,
`current_setting()` and `random()` answer otherwise with no write, and no
trigger would see the rows they place move. What compares, casts and tests
uuids, integers, text and booleans is immutable. Any other function's body
is text the catalog does not look into — PL/pgSQL, or SQL written as a
string (`AS $$ … $$`) — so what it reads cannot be known, nor noted.

> **What happens:** the server refuses to start on a view that calls such a
> function, behind an operator too, naming the function and the view that
> calls it: write the function's body SQL-standard, or read its tables in
> the view. It refuses one that calls, anywhere it reads through, a function
> of PostgreSQL's or an extension's that is not immutable, naming it: keep
> what it answers in a column a write sets — a flag a job raises when a
> notice expires — and read that. And it refuses a view that reads a
> materialized view or a foreign table, whose rows change where no trigger
> sees, or a partitioned table, a partition, or a table that inherits or is
> inherited: a statement fires the triggers of the table it names, and
> changes the rows of another.

> **Mind:** the routed table itself is not something the view reads, for this
> purpose: a change of a notice is planned by its key, as a change of any
> row is — sent again to whoever holds it, and the view asked about that
> notice alone. A view in which a change of one notice moves *another* notice
> is not one this route follows. Nor is a replica's write of a column it owns
> a change of the row ([`mirror = "both"`](rows.md#which-way-it-travels-mirror)):
> the server refuses to start on a view that reads a `replica_owned` column
> of the routed table, naming it. A whole row read anywhere — a row passed to
> a function, `r IS NULL` — names no column of its own: every column of the
> tables read there counts.

## The triggers

Every table the view reads gets four triggers of the engine's, which
[`Engine::triggers`] prints with the others, one for each statement that can
change it: `sync_reroute_insert`, `sync_reroute_update`,
`sync_reroute_delete` and `sync_reroute_truncate`. Each fires once a
statement, with the statement's rows, and notes each partition they hold —
once, however many rows: a script that rebuilds a table of a thousand rows
notes one change a partition. An update notes a partition only where what the
views read of its rows changed; a busy column no view reads — a register's
last sale, a counter — moves nothing. A truncate notes every partition.

A table with the replicas' partition column names the partition of each of
its rows; one without — a table of every company's, a catalogue — notes
every partition. The view is read as keeping partitions apart: a rule of one
company re-routes that company's registers, and no other.

> **What happens:** the server refuses to start while a table the view reads
> lacks its triggers, or has them for other columns than the view reads now,
> and while a table it no longer reads keeps them: edit the view, and print
> the triggers again in the same migration. `Server::install` writes them
> itself.

PostgreSQL gives a trigger its statement's rows only for one event, and
with no column list (`UPDATE OF …`): so four triggers, not one, and the
update's compares the columns the views read — as text — before and after.

## What a change costs

- **A change of the row itself** is planned by its key: the view is asked
  for that row.
- **A change of what the view reads** re-routes its partition: the view is
  asked again for every replica of the partition, and what it answers is
  compared with everything each holds of the table. Only the difference goes
  — the registers whose notices did not move are sent nothing — but the
  asking is the whole partition's. That suits what such tables usually are:
  rules, assignments, a tree of regions — written rarely, by people. A table
  written often, in the columns the view reads, re-routes as often; keep
  what moves often out of the view.
- **A view written otherwise** — another rule, another column read — routes
  rows otherwise: the first pass after the deploy provisions every replica of
  the audience again, as a declaration that changed does ([Upgrading](upgrading.md#your-declarations)).

## The other rules, as views

`assigned` and `grouped` are views too:

```sql
-- assigned(register_id)
CREATE VIEW shop.order_registers AS
    SELECT o.id AS order_id, o.register_id FROM shop.orders o;

-- grouped(members = list_members, assigned = register_lists)
CREATE VIEW shop.customer_registers AS
    SELECT m.customer_id, a.register_id
      FROM shop.list_members m JOIN shop.register_lists a ON a.list_id = m.list_id;
```

Routed by them, the registers are sent what they are sent without — the
engine's own tests hold it to that, write after write. Keep the engine's
rules where they fit: `assigned` reads the one column, and `grouped` plans a
member that moves by its row, where a view re-routes the partition.

[`Engine::triggers`]: ../api/server/magic_sync/plan/struct.Engine.html#method.triggers
