-- The sync engine: a replica's side, version 3. How long a write has waited.
--
-- When each note was made (unix milliseconds, by the replica's clock): the
-- backlog says how long ago its oldest was, so the server tells rows that
-- flow from rows that wait. A note written again keeps its time — a row
-- written over and over has waited since the first. The engine stamps a note
-- as it is made, so the triggers that write notes stay as they were; a note
-- this step finds is stamped now.
ALTER TABLE sync_notes ADD COLUMN noted_at INTEGER;

UPDATE sync_notes SET noted_at = CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER);

CREATE TRIGGER sync_notes_noted AFTER INSERT ON sync_notes WHEN NEW.noted_at IS NULL
BEGIN
    UPDATE sync_notes SET noted_at = CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
     WHERE id = NEW.id;
END;
