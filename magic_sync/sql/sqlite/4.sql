-- The sync engine: a replica's side, version 4. When the server was last
-- heard from, and since when it has kept an upload waiting.
--
-- Unix milliseconds by the replica's clock, each NULL until it first
-- happens: `heard_at`, the last message from the server this build could
-- read, whatever it said; `applied_at`, the last that carried rows or ended
-- a snapshot and was applied; `filed_at`, the last that said the server
-- filed an upload of this replica's. `replica::status` reads them for
-- whoever stands at the replica.
CREATE TABLE sync_heard (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    heard_at   INTEGER,
    applied_at INTEGER,
    filed_at   INTEGER
);

-- When the transport first took a note's upload (unix milliseconds): a
-- resend, which stamps `sent_at` again, keeps it, so it says how long the
-- server has kept the note waiting. A note written again once its upload was
-- filed goes again, and waits from its next send. A note this step finds
-- sent waits since its last send, the earliest this step can know.
ALTER TABLE sync_notes ADD COLUMN first_sent_at INTEGER;

UPDATE sync_notes SET first_sent_at = CAST((julianday(sent_at) - 2440587.5) * 86400000 AS INTEGER)
 WHERE sent_at IS NOT NULL;
