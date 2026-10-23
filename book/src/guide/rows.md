# The rows

Each table that travels is a Rust struct whose fields are exactly its columns,
and which says how it travels. The same struct serves both sides.

> **You write** (`examples/shop/rows/src/lib.rs`):

```rust,ignore
{{#include ../../../examples/shop/rows/src/lib.rs:rows}}
```

## Which way it travels: `mirror`

| `mirror` | Written on | Travels |
|---|---|---|
| `catalog` | the server | down, to the replicas that should hold it |
| `record` | a replica | up, to the server |
| `both` | both, column by column | the columns replicas own go up, the rest come down |
| `setting` | a replica | up; the server may wish its `#[row(desired)]` columns other values, and the replica carries the wish out |

## Which replicas hold it: `sync`

For a table that goes down:

| `sync` | A replica holds a row when |
|---|---|
| `broadcast` | the row is in the replica's partition (every replica, without partitions) |
| `assigned(col)` | `col` names the replica |
| `follows(col)` | the row `col` points at goes to the replica |
| `grouped(members = t, assigned = t)` | the row is in a group the replica is given ([Recipes](recipes.md)) |
| `routed(view = v, row = col, replica = col)` | a view of yours pairs the row with the replica ([Rows a view places](routed.md)) |
| left out | no rule of its own: it goes wherever a row that travels points at it |

> **What happens:** you never declared that a receipt needs its product. The
> engine reads the foreign keys from the database: a replica sent a row is sent
> every row it points at, and keeps those while anything of its own points at
> them. A table with no `sync` rule still reaches every replica that needs it.

## The rest

The shop's rows use a few more: `schema`, the table's PostgreSQL schema;
`retired`, the column a register sets on a product it lets go of but must
keep; `#[row(version)]`, the column of a record a register edits whose value
only moves forward; and `#[row(created)]`, a column written once. Every key and flag the derive takes
— a key of several columns, a view rows are read through, the columns one
side leaves out, the columns replicas own — is in
[the derive's reference](../api/server/magic_sync/derive.Row.html), and
[Recipes](recipes.md) puts most of them to work.

Fields follow `sqlx::FromRow`: `#[sqlx(rename = "…")]` names the column,
`#[sqlx(skip)]` is no column, `#[sqlx(flatten)]` brings in a part's columns
(the part derives `magic_sync::Columns`).

## Keeping the struct and the table together

> **What happens:** every time the server or a replica starts, the engine
> holds each table against its row on that database — column for column,
> type for type, nullability for nullability, and the primary key — and
> refuses to start on one that differs, naming every difference. A column
> added on one side only, or to the struct only, never runs. The columns a
> row leaves out on purpose (`retired`, `unmirrored`) are not compared.

A test of yours that builds the server, or a replica, over the database your
migrations build is all it takes to see it before a deploy.

The derive checks the rest where you write it: a key that is not a field, a
column named twice, an attribute a table of its kind cannot have (a `sync`
rule on a record, a version on a table the server sends down), a key or version of a type
the engine cannot read — each is a compile error pointing at what says it.
