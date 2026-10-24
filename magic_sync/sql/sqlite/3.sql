-- The sync engine: a replica's side, version 3. How long a write has waited,
-- and how long the replica has said nothing.
--
-- When each note was made (unix milliseconds, by the replica's clock): the
-- backlog says how long ago its oldest was, so the server tells rows that
-- flow from rows that wait. A note written again keeps its time — a row
-- written over and over has waited since the first — until the upload that
-- carried it is filed: what is left of it then dates from that upload. The
-- engine stamps a note as it is made, so the triggers that write notes stay
-- as they were; a note this step finds is stamped now.
ALTER TABLE sync_notes ADD COLUMN noted_at INTEGER;

UPDATE sync_notes SET noted_at = CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER);

CREATE TRIGGER sync_notes_noted AFTER INSERT ON sync_notes WHEN NEW.noted_at IS NULL
BEGIN
    UPDATE sync_notes SET noted_at = CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
     WHERE id = NEW.id;
END;

-- When the transport last took a message of this replica's, on any lane
-- (unix milliseconds): once it has said nothing for its keepalive, it says
-- so on its first lane, with nothing else to say, and the server knows it is
-- there.
CREATE TABLE sync_spoken (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    at INTEGER NOT NULL
);
