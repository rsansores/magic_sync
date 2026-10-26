-- The sync engine: a replica's side, version 4. When the server was last
-- heard from.
--
-- Unix milliseconds by the replica's clock, each NULL until it first
-- happens: `heard_at`, the last message from the server this build could
-- read, whatever it said; `applied_at`, the last that carried rows or ended
-- a snapshot and was applied; `filed_at`, the last that said the server
-- filed an upload of this replica's. `replica::status` reads them, so whoever
-- stands at the replica can tell a server that answers from one that does not.
CREATE TABLE sync_heard (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    heard_at   INTEGER,
    applied_at INTEGER,
    filed_at   INTEGER
);
