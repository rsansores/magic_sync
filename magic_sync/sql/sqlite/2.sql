-- The sync engine: a replica's side, version 2. Wishes.
--
-- What the server wants one of this replica's settings to hold, as the server
-- sent it; the answer is the replica's, and goes up. A wish whose answer is not
-- `answered` = `version` is carried out once its table's handler is offered it.
CREATE TABLE sync_wishes (
    id           BLOB PRIMARY KEY NOT NULL,
    relation     TEXT NOT NULL,
    row_key      TEXT NOT NULL,
    wanted       BLOB NOT NULL,
    version      INTEGER NOT NULL,
    if_unchanged INTEGER NOT NULL,
    note         TEXT,
    answered     INTEGER,
    outcome      TEXT,
    reason       TEXT,
    answered_at  TEXT,
    retired_at   TEXT
);

-- A note of a row re-stated when a snapshot began (a setting, or an answer to
-- a wish), until the server files it: the server takes it whatever its
-- version, as what the replica holds.
ALTER TABLE sync_notes ADD COLUMN restated INTEGER NOT NULL DEFAULT 0;
