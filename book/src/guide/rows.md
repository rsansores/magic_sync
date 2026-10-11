# The rows

Each table that travels is a Rust struct whose fields are exactly its columns,
and which says how it travels. The same struct serves both sides.

> **You write:**

```rust
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// A product: written at the head office, held by every register.
#[derive(sqlx::FromRow, Clone, serde::Serialize, serde::Deserialize, magic_sync::Row)]
#[row(
    table = "products",
    schema = "shop",
    mirror = "catalog",
    sync = "broadcast",
    retired = "retired_at"
)]
pub struct Product {
    pub id: Uuid,
    pub name: String,
    pub price_cents: i64,
    #[row(version)]
    pub updated_at: DateTime<Utc>,
}

/// A receipt: written by a register, filed at the head office.
#[derive(sqlx::FromRow, Clone, serde::Serialize, serde::Deserialize, magic_sync::Row)]
#[row(table = "receipts", schema = "shop", mirror = "record")]
pub struct Receipt {
    pub id: Uuid,
    pub register_id: Uuid,
    pub product_id: Uuid,
    pub quantity: i32,
    pub total_cents: i64,
    #[row(created)]
    pub created_at: DateTime<Utc>,
}
```

## Which way it travels: `mirror`

| `mirror` | Written on | Travels |
|---|---|---|
| `catalog` | the server | down, to the replicas that should hold it |
| `record` | a replica | up, to the server |
| `both` | both, column by column | the columns replicas own go up, the rest come down |
| `server` | the server | nowhere: a struct the engine's writes use, nothing more |

## Which replicas hold it: `sync`

For a table that goes down:

| `sync` | A replica holds a row when |
|---|---|
| `broadcast` | the row is in the replica's partition (every replica, without partitions) |
| `assigned(col)` | `col` names the replica |
| `follows(col)` | the row `col` points at goes to the replica |
| `grouped(members = t, assigned = t)` | the row is in a group the replica is given ([Recipes](recipes.md)) |
| left out | no rule of its own: it goes wherever a row that travels points at it |

> **What happens:** you never declared that a receipt needs its product. The
> engine reads the foreign keys from the database: a replica sent a row is sent
> every row it points at, and keeps those while anything of its own points at
> them. A table with no `sync` rule still reaches every replica that needs it.

## The rest

On the struct:

| key | means |
|---|---|
| `schema` | its PostgreSQL schema; `public` when left out |
| `key` | the primary key's columns, when it is not `id` (`key = "list_id, customer_id"`) |
| `retired` | the column a replica sets on a row it lets go of but must keep |
| `active` | a view the rows replicas hold are read through, leaving rows out — archived ones, say |
| `origin` | for a record keyed by its replica, that key column: a replica may delete only its own rows |
| `unmirrored` | columns one side's table has that the struct leaves out |

On a field:

| key | means |
|---|---|
| `version` | a column whose value only moves forward: an older copy never overwrites a newer one |
| `created` | written once, never by an update |
| `replica_owned` | in a `both` table, a column only replicas write |
| `generated` | computed by the database: read, never written |

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
rule on a record, a version owned by a replica), a key or version of a type
the engine cannot read — each is a compile error pointing at what says it.
