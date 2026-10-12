# Upgrading

Two things outlive a build of `magic_sync`: the engine's tables in each
database, and the protocol the server and its replicas speak. A release says
in its changelog when it changes either, and what follows is how each
changes without losing anything.

## The engine's schema

Each side's database holds the engine's own tables at a version of that
side's own: [`SERVER_SCHEMA_VERSION`] for the server's, and
[`REPLICA_SCHEMA_VERSION`] for a replica's. They count different steps and
move independently: a release that changes only the server's tables leaves the
replicas' version where it was, and the other way round. A build runs against
a database at its own version only: `Server::new` and `Replica::new` refuse
any other, naming both.

A release that changes one side's tables raises that side's version and
brings the step from each earlier version with it: one script per version, applied in
order, in one transaction — never edited once released. So the engine's
tables move forward one step at a time, and never back: a database a later
build upgraded is refused by an earlier one rather than misread.

- **Without migrations of your own**, starting the new build is the upgrade.
  `Server::install` brings the server's database from whatever version it is
  at to this build's, in one transaction and one process at a time (an
  advisory lock), before the server starts; `replica::install` does the same
  on a replica, in one transaction.
- **With migrations**, the upgrade is a migration of yours that runs the
  release's step, as your first one ran [`POSTGRES_SCHEMA`] (on a replica,
  the step itself, [`replica::migration`]`(n)` for version `n`, which also
  records the version): the changelog names it. Print the triggers again
  ([The tables](tables.md)) in the same migration: a release may write them
  differently, and the server refuses to start while one differs.

On the server, stop every process of the earlier build before the database
is upgraded, then start the new one: a process keeps running against the
tables it started with, and does not know what the step changed. The
replicas keep working meanwhile — they are built to be offline — and catch up
when the server is back.

Each replica upgrades its own database when its new build starts, whenever
that is: a fleet is never upgraded at once, and does not need to be.

## Your declarations

What decides which rows each replica holds is yours: the replicas'
relation, and per table its key, its `sync` rule, the tables it points at,
its groups and its columns. The relay remembers a digest of them
(`sync.version.declared`); the first pass of a build whose declarations
differ — a table routed otherwise, mirrored for the first time or no
more, a column added — provisions every live replica again, and lets go of
what it kept of a table it mirrors no more. That is an alarm in the log,
and a deploy's worth of traffic: change declarations on purpose. A change
to how a codec encodes a row that leaves its columns alone is not seen:
provision the replicas yourself ([Provisioning](provisioning.md)).

## The protocol

Every message carries the protocol it speaks, [`PROTOCOL`]: a new kind of
message, or a field whose meaning changes, raises it; a new field does not
([the protocol, section 4.0](../protocol.md#40-the-protocol-version)).

A side that receives a message of a later protocol than its own does not act
on it: a replica leaves it unacknowledged, and everything behind it waits
until the replica is updated; the server answers a replica's upload with an
error, and the replica sends it again until the server is updated. Nothing is
lost either way, and the order of the upgrade only decides which direction
waits. A release that raises `PROTOCOL` says in its changelog which side to
upgrade first.

## The dashboard

`magic_sync_dashboard` reads the JSON the engine answers, of the version it
carries (`status::CONTRACT`), and refuses another rather than draw it wrong.
The crate and the package are released together, at the same version:
upgrade them together.

[`SERVER_SCHEMA_VERSION`]: ../api/server/magic_sync/constant.SERVER_SCHEMA_VERSION.html
[`REPLICA_SCHEMA_VERSION`]: ../api/replica/magic_sync/constant.REPLICA_SCHEMA_VERSION.html
[`POSTGRES_SCHEMA`]: ../api/server/magic_sync/constant.POSTGRES_SCHEMA.html
[`replica::migration`]: ../api/replica/magic_sync/replica/fn.migration.html
[`PROTOCOL`]: ../api/server/magic_sync/envelope/constant.PROTOCOL.html
