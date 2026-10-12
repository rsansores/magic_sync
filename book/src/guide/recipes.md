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
user turned it to 40. Two writers, one setting. Do not make it one row both
write: split it by who is right about what. The head office is right about
what it *wants*; the register is right about what the setting *is*.

The head office writes a **desired** table, which goes down to the register it
is for:

```rust,ignore
{{#include ../../snippets/src/rows.rs:desired}}
```

The register writes a **reported** table, a record that goes up. Its
`version` column orders the register's own reports, so the newest one wins at
the head office, whatever order they arrive in:

```rust,ignore
{{#include ../../snippets/src/rows.rs:reported}}
```

On the server, with the triggers [`Engine::triggers`] prints beside them
([Tables](tables.md)):

```sql
CREATE TABLE shop.desired_settings (
    register_id UUID NOT NULL REFERENCES shop.registers (id),
    id          UUID NOT NULL,
    value       TEXT NOT NULL,
    revision    BIGINT NOT NULL,
    PRIMARY KEY (register_id, id)
);

CREATE TABLE shop.reported_settings (
    register_id UUID NOT NULL REFERENCES shop.registers (id),
    id          UUID NOT NULL,
    value       TEXT NOT NULL,
    reported_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (register_id, id)
);
```

On the register, the same two tables as SQLite stores them:

```sql
CREATE TABLE desired_settings (
    register_id BLOB NOT NULL, id BLOB NOT NULL,
    value TEXT NOT NULL, revision INTEGER NOT NULL,
    PRIMARY KEY (register_id, id)
);
CREATE TABLE reported_settings (
    register_id BLOB NOT NULL, id BLOB NOT NULL,
    value TEXT NOT NULL, reported_at TEXT NOT NULL,
    PRIMARY KEY (register_id, id)
);
```

Both are keyed by the register and the setting's id, so a wish and its report
share their key. The wish carries a `revision` the head office raises on
every change. Nothing in the engine compares it: a table the server sends
down is taken as it comes, whatever its columns say, so the register does the
comparing. It keeps, in a table of its own that never travels, the last
revision it carried out of each setting, and carries out only the wishes
newer than that — each once — reporting what the setting is afterwards in the
same transaction that records it:

```rust,ignore
{{#include ../../snippets/src/replica.rs:apply_desired}}
```

Its bookkeeping table is the register's alone:

```sql
CREATE TABLE applied_settings (id BLOB PRIMARY KEY NOT NULL, revision INTEGER NOT NULL);
```

Run it after every message the register applied (`Received::Applied` names
the tables, so you can run it only when `desired_settings` is among them, and
a snapshot ends with `Received::SnapshotEnded`) and at start-up.
The head office reads `reported_settings` to see what each register is
actually set to, and compares it with what it wants. A user who turns the
brightness down by hand writes the `reported` row again, with a newer
`reported_at`; the wish stands until the head office changes it, and is not
carried out a second time.

> **Mind:** `apply` runs before the revision is recorded, so a register that
> loses power between the two carries the wish out again. Make it
> idempotent.

> **Note:** the engine has no primitive for this pair: the register's half,
> carrying a wish out once per revision, is the host's to write, as above.

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
