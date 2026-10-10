# Recipes

## A table that extends another one

A product's nutrition facts, keyed by the product, should go wherever the
product goes:

```rust
#[row(table = "nutrition", schema = "shop", mirror = "catalog",
      key = "product_id", sync = "follows(product_id)")]
pub struct Nutrition { pub product_id: Uuid, /* … */ }
```

## Orders for one register

An order is taken by one register and goes to that register only:

```rust
#[row(table = "orders", schema = "shop", mirror = "catalog", sync = "assigned(register_id)")]
pub struct Order { pub id: Uuid, pub register_id: Option<Uuid>, pub product_id: Uuid, /* … */ }
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

```rust
#[row(table = "customers", schema = "shop", mirror = "catalog",
      sync = "grouped(members = list_members, assigned = register_lists)")]
pub struct Customer { /* … */ }
```

Taking a list away from a register is deleting one row; the engine works out
which customers to take back, and keeps those another list still gives it.
With migrations of your own, each group table gets its trigger, which
`Engine::triggers` prints with the others:

```sql
CREATE TRIGGER sync_note_group AFTER INSERT OR UPDATE OR DELETE ON "shop"."list_members" FOR EACH ROW EXECUTE FUNCTION sync.note_group('list_members', 'list_id', 'members', '');
CREATE TRIGGER sync_note_group AFTER INSERT OR UPDATE OR DELETE ON "shop"."register_lists" FOR EACH ROW EXECUTE FUNCTION sync.note_group('register_lists', 'register_id', 'assigned', '');
```

The members table points at the grouped table by its key — one column or
several — and both group tables point at the table of groups.

## A table both sides write

A register reports its own status on a row the head office owns. Mark the
columns the register owns: they go up, the rest come down, and a change to
only the register's columns is not sent back down:

```rust
#[row(table = "register_state", schema = "shop", mirror = "both", sync = "assigned(id)")]
pub struct RegisterState {
    pub id: Uuid,
    pub label: String,              // the head office's
    #[row(replica_owned)]
    pub battery: Option<i32>,       // the register's
    #[row(version)]
    pub updated_at: DateTime<Utc>,
}
```

## Archived rows replicas should not hold

Keep a view of the rows that should travel, and name it:

```sql
CREATE VIEW shop.live_products AS SELECT * FROM shop.products WHERE archived_at IS NULL;
```

```rust
#[row(table = "products", schema = "shop", mirror = "catalog", sync = "broadcast",
      active = "shop.live_products", unmirrored = "archived_at")]
```

Archiving a product takes it back from every register — retired where a
receipt still points at it. Leave `active` out and archived products travel
like any other, with their `archived_at` as data, for the register to show as
archived.

## Several companies on one server

Give rows and replicas a partition column, and name it:

```rust
pub const REGISTERS: Replicas = Replicas {
    relation: "shop.registers", table: "registers", live: "active",
    partition: Some("company_id"),
};
```

Every table that goes down carries `company_id`; a `broadcast` product reaches
only its company's registers. The triggers `Engine::triggers` prints note it.
