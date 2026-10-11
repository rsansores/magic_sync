-- The sync engine: a replica's side, version 1. The engine applies it in one
-- transaction with the triggers its tables need (replica::install); a host
-- with migrations runs it in one, with replica::triggers' script.

-- The version of this schema: the engine refuses to run against another.
CREATE TABLE sync_version (
    id      INTEGER PRIMARY KEY CHECK (id = 1),
    version INTEGER NOT NULL
);
INSERT INTO sync_version (id, version) VALUES (1, 1);

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
-- nothing; the next names it as the one it follows, or the replica missed
-- one (`gap`) and applies nothing more until a snapshot begins. A server
-- whose schema was installed again starts a new epoch, with a snapshot. The
-- server counts a row as held only once it has been told it was applied.
CREATE TABLE sync_applied (
    id           INTEGER PRIMARY KEY CHECK (id = 1),
    epoch        TEXT    NOT NULL,
    sequence     INTEGER NOT NULL,
    -- The last one the server of this epoch has been told of.
    acknowledged INTEGER NOT NULL DEFAULT 0,
    -- The snapshot in progress, and the message that began it.
    snapshot_id    TEXT,
    snapshot_began INTEGER,
    -- 1 while it missed a message; `gap_told` once the server was told.
    gap          INTEGER NOT NULL DEFAULT 0,
    gap_told     INTEGER NOT NULL DEFAULT 0
);

-- What this replica last told the server it wrote and the server has not
-- filed ([{table, unsent, unfiled, set_aside}]), and when (unix seconds): it
-- says again when that changes, at most so often while it grows.
CREATE TABLE sync_backlog (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    backlog     TEXT    NOT NULL,
    reported_at INTEGER NOT NULL
);

-- Rows the server sent that this replica could not take, or could not let
-- go of, until it does: the server is told once (reported_at), quarantines
-- them, and sends them again once a fix is deployed on either side. `sequence`
-- is the message that refused it: a snapshot that ends clears what was
-- refused before it began, which it sent again.
CREATE TABLE sync_rejections (
    table_name  TEXT    NOT NULL,
    row_key     TEXT    NOT NULL,
    reason      TEXT    NOT NULL,
    sequence    INTEGER NOT NULL,
    rejected_at TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    reported_at TEXT,
    PRIMARY KEY (table_name, row_key)
);

-- What this replica wrote, noted in the writing transaction by a trigger on
-- each table that goes up: the table and the row's key (its parts as text,
-- joined by |). An upload carries noted rows as they are by then, under a
-- number of its own (`upload`), and a note leaves once the server says it
-- filed that upload. A note that cannot go as it is waits, `set_aside` with
-- why, until the replica runs again.
CREATE TABLE sync_notes (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    relation  TEXT    NOT NULL,
    row_key   TEXT    NOT NULL,
    noted_at  TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    upload    INTEGER,
    sent_at   TEXT,
    set_aside TEXT
);

CREATE INDEX sync_notes_unsent ON sync_notes (id) WHERE upload IS NULL;
CREATE INDEX sync_notes_upload ON sync_notes (upload) WHERE upload IS NOT NULL;

-- The last upload number drawn: every send draws a fresh one, so the
-- server's answer to an upload never names notes another one carried.
CREATE TABLE sync_uploads (
    id   INTEGER PRIMARY KEY CHECK (id = 1),
    last INTEGER NOT NULL
);
INSERT INTO sync_uploads (id, last) VALUES (1, 0);
