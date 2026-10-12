-- The sync engine on a replica: the step to version 1, as released. Never
-- edited: a later version is a step of its own. `replica::migration(1)` is
-- this step and the version it brings the database to.

-- The version of the engine's tables: the engine refuses to run against
-- another.
CREATE TABLE sync_version (
    id      INTEGER PRIMARY KEY CHECK (id = 1),
    version INTEGER NOT NULL
);

-- Which replica this is, once it knows: a row of a table written on both
-- sides and keyed by its replica is this one's to delete only when keyed by
-- it.
CREATE TABLE sync_replica (
    id      INTEGER PRIMARY KEY CHECK (id = 1),
    replica BLOB    NOT NULL
);

-- Present only while the engine applies what the server sent, inside that
-- transaction: the triggers note no write it makes, and refuse none.
CREATE TABLE sync_applying (
    id INTEGER PRIMARY KEY CHECK (id = 1)
);

-- The rows the snapshot in progress has named, per table: at its end every
-- row of those tables it did not name goes. Emptied when a snapshot begins.
CREATE TABLE sync_snapshot_keys (
    table_name TEXT NOT NULL,
    row_key    TEXT NOT NULL,
    PRIMARY KEY (table_name, row_key)
);

-- The last message this replica applied, from which server: one at or below
-- it is a redelivery, or comes from a relay that lost its lock, and changes
-- nothing; the next names it — its sequence and its nonce — as the one it
-- follows, or the replica missed one (`gap`) and applies nothing more until
-- a snapshot begins. A server whose schema was installed again starts a new
-- epoch, with a snapshot. The server counts a row as held only once it has
-- been told it was applied, with the message's own nonce and mac, which it
-- checks against what it sent.
CREATE TABLE sync_applied (
    id           INTEGER PRIMARY KEY CHECK (id = 1),
    epoch        TEXT    NOT NULL,
    sequence     INTEGER NOT NULL,
    nonce        INTEGER NOT NULL DEFAULT 0,
    mac          BLOB    NOT NULL DEFAULT x'',
    -- The last one the server of this epoch has been told of.
    acknowledged INTEGER NOT NULL DEFAULT 0,
    -- The snapshot in progress, and the message that began it.
    snapshot_id    TEXT,
    snapshot_began INTEGER,
    -- The newest message it could not follow, while it missed one (0: it
    -- missed none); `gap_told`, the one the server was last told of.
    gap          INTEGER NOT NULL DEFAULT 0,
    gap_told     INTEGER NOT NULL DEFAULT 0
);

-- What this replica last told the server it wrote and the server has not
-- filed ([{table, unsent, unfiled, set_aside}]), and when (unix milliseconds): it
-- says again when that changes, at most so often while it grows.
CREATE TABLE sync_backlog (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    backlog     TEXT    NOT NULL,
    reported_at INTEGER NOT NULL
);

-- When the replica next tells the server what it holds (unix milliseconds):
-- kept here, so a replica that restarts often still says it.
CREATE TABLE sync_digests (
    id  INTEGER PRIMARY KEY CHECK (id = 1),
    due INTEGER NOT NULL
);

-- Rows the server sent that this replica could not take, or could not let
-- go of, until it does: the server is told once (reported_at), quarantines
-- them, and sends them again once a fix is deployed on either side. `epoch`
-- and `sequence` are the message that refused it, which the server reads
-- the rejection against: a snapshot that ends clears what was refused before
-- it began, which it sent again. `held_back`: a row of its own it deleted,
-- not taken while the delete is on its way — nothing to fix.
CREATE TABLE sync_rejections (
    table_name  TEXT    NOT NULL,
    row_key     TEXT    NOT NULL,
    reason      TEXT    NOT NULL,
    epoch       TEXT    NOT NULL,
    sequence    INTEGER NOT NULL,
    held_back   INTEGER NOT NULL DEFAULT 0,
    rejected_at TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    reported_at TEXT,
    PRIMARY KEY (table_name, row_key)
);

-- Rows of tables written on both sides the server let go of while what this
-- replica wrote of them had not reached it: kept retired, and let go of once
-- the server files it. `epoch` and `sequence` are the message that let go
-- of it, which a rejection names should the replica not manage to then.
CREATE TABLE sync_leaving (
    relation TEXT    NOT NULL,
    row_key  TEXT    NOT NULL,
    epoch    TEXT    NOT NULL,
    sequence INTEGER NOT NULL,
    PRIMARY KEY (relation, row_key)
);

-- What this replica wrote, noted in the writing transaction by a trigger on
-- each table that goes up: one note per row — the table and its key (its
-- parts as text, joined by |) — and how many writes it noted (`writes`). An
-- upload carries noted rows as they are by then, under a random number of
-- its own (`upload`, stamped when it is made) and the writes it carried
-- (`sent`); `sent_at` once the transport took it. A note leaves once the
-- server says it filed that upload, unless the row was written since. A
-- note that cannot go as it is waits, `set_aside` with why, until the
-- replica runs again or the row is written again.
CREATE TABLE sync_notes (
    id        INTEGER PRIMARY KEY,
    relation  TEXT    NOT NULL,
    row_key   TEXT    NOT NULL,
    writes    INTEGER NOT NULL DEFAULT 1,
    upload    INTEGER,
    sent      INTEGER,
    sent_at   TEXT,
    set_aside TEXT,
    UNIQUE (relation, row_key)
);

CREATE INDEX sync_notes_upload ON sync_notes (upload) WHERE upload IS NOT NULL;
