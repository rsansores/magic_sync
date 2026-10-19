# Several kinds of replica

The registers are one kind of replica. A server may feed others from the
same tables: an accounting system that wants the products and nothing else,
in a shape of its own; a fleet of a second product that holds a part of what
the first holds. Each kind is an **audience**: where its replicas are, and
the tables they hold, each with its codec.

## Declaring them

> **You write** one `Audience` per kind of replica, and hand the server the
> list (`book/snippets/src/server/audiences.rs`):

```rust,ignore
{{#include ../../snippets/src/server/audiences.rs:audiences}}
```

> **What happens:** one stream of changes, one relay, one dashboard. A table
> two audiences declare has one trigger, which notes what either needs of it;
> each audience plans the change against its own replicas, and sends it
> encoded as its own codec says. A table only one audience declares is never
> planned, let alone sent, for the other. A change one audience cannot plan
> is set aside, and the others plan it all the same; tried again, the ones
> that planned it find nothing new to send.

Each audience has a name — what the transport is told, what the status
says, and what the engine records each of its replicas under. A replica two
audiences count live is the first's, unless it was live in the other already:
it stays there until that audience lets it go, and then comes to this one,
provisioned again. Renaming an audience provisions its replicas again under
the new name; an audience no longer declared lets its replicas go.

An audience is the server's alone. A replica is the same whatever audience it
is of: nothing on the wire names one, and [the protocol](../protocol.md) does
not change for it. It holds the tables it is sent, encoded as its audience's
codecs encode them.

## A table in a shape of its own

A codec of its own already gives an audience another encoding of the same
row. A row of another *shape* — a parent's column joined in, some rows left
out — is a row read `from` a view of its own:

```rust,ignore
{{#include ../../snippets/src/server/audiences.rs:shape}}
```

```sql
CREATE VIEW books.products AS
    SELECT p.id, p.name, p.price_cents, c.tax_code
      FROM shop.products p JOIN shop.categories c ON c.id = p.category_id
     WHERE p.on_sale;
```

> **What happens:** the row is still the table's — noted by its trigger, keyed
> by its primary key, placed by its rule and its foreign keys — but its
> columns are read from the view, and the view decides which rows are sent:
> a product that goes off sale leaves the view, and the books let go of it,
> as a row its rule no longer sends. The server holds the row to the view
> when it starts, column for column and type for type, and the key to the
> table's. A view that leaves out a row another row the audience is sent
> points at keeps that row from going too: it is set aside until a fix.

## Columns copied from a parent: `depends`

The view copies the category's tax code into every product. A change of the
category changes none of its products' rows, so nothing would send them
again — unless the row says it `depends` on the column that points at the
category. Then a change of the category is a change of every product that
points at it, for the audience whose row copies it: the books get their
products again with the new code, and the registers, whose products copy
nothing, get nothing.

Each column `depends` names is the whole of a foreign key of the table (the
partition column aside). The parent need not be one of anyone's tables: the
engine notes it all the same, with a trigger of its own. One hop: a column
copied from a grandparent is the parent's change to make.

## The transport and the senders

The transport is told the audience of each message, so one transport can
route each kind its own way:

```rust,ignore
{{#include ../../snippets/src/server/audiences.rs:routes}}
```

`Senders::known` is told the audience of a replica that uploads, so the host
can look it up where that kind of replica lives. An upload is filed by the
tables of its sender's audience: a table its audience does not hold is not
filed, but kept until a fix.

## What the status says

Every replica in an answer says its audience, and each table is listed once
for each audience that holds it, over that audience's replicas.
`Scope::Audience("books")` narrows a read to one audience
([Progress and health](progress.md)); the dashboard says each row's audience
when there is more than one.

## A replica that is another system

A replica need not keep its rows in tables of its own. One that hands them to
another system's services — an accounting system whose tables only its own
services may write — keeps a ledger of what it applied, applies a message
before it records it, and skips a row it already holds as it is: [the
protocol, Appendix B](../protocol.md#appendix-b-a-replica-that-applies-through-another-systems-services).
