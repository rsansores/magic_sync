-- The sync engine: a replica's side.

-- The version of this schema: the engine refuses to run against another.
CREATE TABLE sync_version (
    id      INTEGER PRIMARY KEY CHECK (id = 1),
    version INTEGER NOT NULL
);
INSERT INTO sync_version (id, version) VALUES (1, 1);

-- The snapshots the server has provisioned this replica with. Only the latest
-- is in progress; one that has ended, or that a newer one superseded, sweeps
-- nothing when its end arrives again.
CREATE TABLE sync_snapshots (
    snapshot_id TEXT PRIMARY KEY NOT NULL,
    started_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    ended_at    TEXT
);

-- The rows the snapshot in progress has named, per table: at its end every
-- row of those tables it did not name goes. Emptied when the snapshot ends.
CREATE TABLE sync_snapshot_keys (
    snapshot_id TEXT NOT NULL,
    table_name  TEXT NOT NULL,
    row_key     TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, table_name, row_key)
);

-- The last message this replica applied, from which server: one at or below
-- it is a redelivery, or comes from a relay that lost its lock, and changes
-- nothing. A server whose schema was installed again starts a new epoch. The
-- server counts a row as held only once it has been told it was applied.
CREATE TABLE sync_applied (
    id           INTEGER PRIMARY KEY CHECK (id = 1),
    epoch        TEXT    NOT NULL,
    sequence     INTEGER NOT NULL,
    -- The last one the server has been told of.
    acknowledged INTEGER NOT NULL DEFAULT 0
);

-- Rows the server sent that this replica could not take, until it takes
-- them: the server is told once (reported_at), quarantines them, and sends
-- them again once a fix is deployed on either side.
CREATE TABLE sync_rejections (
    table_name  TEXT NOT NULL,
    row_key     TEXT NOT NULL,
    reason      TEXT NOT NULL,
    rejected_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    reported_at TEXT,
    PRIMARY KEY (table_name, row_key)
);

-- What this replica wrote, noted in the writing transaction by a trigger on
-- each table that goes up: the table and the row's key (its parts as text,
-- joined by |). An upload carries noted rows as they are by then, and is
-- numbered by the highest note it carries; a note leaves once the server says
-- it filed that upload.
CREATE TABLE sync_notes (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    relation TEXT    NOT NULL,
    row_key  TEXT    NOT NULL,
    noted_at TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    upload   INTEGER,
    sent_at  TEXT
);

CREATE INDEX sync_notes_unsent ON sync_notes (id) WHERE upload IS NULL;
