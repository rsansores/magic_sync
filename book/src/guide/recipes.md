# Recipes

## A table that extends another one

A product's nutrition facts, keyed by the product, should go wherever the
product goes:

```rust,ignore
{{#include ../../snippets/src/rows.rs:follows}}
```

## Orders for one register

An order is taken by one register and goes to that register only:

```rust,ignore
{{#include ../../snippets/src/rows.rs:assigned}}
```

The order's product goes with it — no declaration: the foreign key says so.

## Give a register a whole list at once

Customers belong to lists, and a register is given lists. A list of 10,000
customers given to a register is one row in `register_lists`:

```sql
CREATE TABLE shop.lists (id UUID PRIMARY KEY);
CREATE TABLE shop.list_members (
    list_id UUID REFERENCES shop.lists (id), customer_id UUID REFERENCES shop.customers (id),
    PRIMARY KEY (list_id, customer_id));
CREATE TABLE shop.register_lists (
    list_id UUID REFERENCES shop.lists (id), register_id UUID REFERENCES shop.registers (id),
    PRIMARY KEY (list_id, register_id));
```

```rust,ignore
{{#include ../../snippets/src/rows.rs:grouped}}
```

Taking a list away from a register is deleting one row; the engine works out
which customers to take back, and keeps those another list still gives it.
The members table points at the grouped table by its key — one column or
several — and both group tables point at the table of groups. Each group
table gets a trigger of its own (`sync_note_group`), which `Engine::triggers`
prints with the others.

## A table both sides write

A register reports its own status on a row the head office owns. Mark the
columns the register owns: they go up, the rest come down, and a change to
only the register's columns is not sent back down:

```rust,ignore
{{#include ../../snippets/src/rows.rs:both}}
```

The row begins on the server: a register edits only its own columns of a row
it holds.

## A setting both sides want to change

The head office wants a register's screen brightness at 70; the register's
user turned it to 40. Two writers, one setting. The register is right about
what the setting *is*, and the head office about what it *wants*: declare the
register's row a `setting`, and let the head office wish it other values —
[Settings and wishes](settings.md) is the whole of it.

## Archived rows replicas should not hold

Keep a view of the rows that should travel, and name it:

```sql
CREATE VIEW shop.live_products AS SELECT * FROM shop.products WHERE archived_at IS NULL;
```

```rust,ignore
{{#include ../../snippets/src/rows.rs:active}}
```

Archiving a product takes it back from every register — retired where a
receipt still points at it. The view decides what is sent for its own sake,
not what may travel: an archived product a row the register holds points at
still goes there, as that row's parent, and leaves once nothing points at it.
Leave `active` out and archived products travel
like any other, with their `archived_at` as data, for the register to show as
archived.

## Several companies on one server

Give rows and replicas a partition column, and name it:

```rust,ignore
{{#include ../../snippets/src/server/partitioned.rs:partitioned}}
```

Every table that goes down carries `company_id`; a `broadcast` product reaches
only its company's registers. The triggers `Engine::triggers` prints note it.

[`Engine::triggers`]: ../api/server/magic_sync/plan/struct.Engine.html#method.triggers
