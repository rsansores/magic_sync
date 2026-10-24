-- The sync engine: the server's side, version 5. When each replica was last
-- heard.
--
-- A replica that says how long it stays quiet at most is known to have
-- stopped once it says nothing for several times that; what it uploads of
-- each table, and when, says which of its writes flow.

ALTER TABLE sync.replicas
    ADD COLUMN last_heard_at TIMESTAMPTZ,
    ADD COLUMN keepalive     INTERVAL,
    ADD COLUMN filed         JSONB;

COMMENT ON COLUMN sync.replicas.last_heard_at IS
    'When the server last filed an upload from the replica, on any lane. NULL until it does.';
COMMENT ON COLUMN sync.replicas.keepalive IS
    'How long the replica said, in its last upload, it stays quiet at most: one that says '
    'nothing for several times that has stopped. NULL when it does not say, and is never '
    'taken for one that stopped.';
COMMENT ON COLUMN sync.replicas.filed IS
    'When the server last filed a row of each table from the replica: {table: time}. NULL '
    'until it does.';
COMMENT ON COLUMN sync.replicas.backlog IS
    'What the replica last said it wrote and the server has not filed, per table '
    '([{table, unsent, unfiled, set_aside, oldest_ms}]: oldest_ms how long its oldest row had '
    'waited, as backlog_at), as of backlog_at. NULL until it says.';

-- What a status judges a replica by, kept here so that every process reading
-- a status judges alike: the server writes its settings when it starts.
CREATE TABLE sync.settings (
    id                      INTEGER  PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    behind_after            INTERVAL NOT NULL DEFAULT '2 minutes' CHECK (behind_after > '0'),
    silent_after_keepalives INTEGER  NOT NULL DEFAULT 3 CHECK (silent_after_keepalives > 0)
);
INSERT INTO sync.settings DEFAULT VALUES;

COMMENT ON TABLE sync.settings IS
    'What a status judges a replica by, as the server last started with: one row.';
COMMENT ON COLUMN sync.settings.behind_after IS
    'How long a row a replica wrote may wait to be filed before the replica is behind.';
COMMENT ON COLUMN sync.settings.silent_after_keepalives IS
    'How many times the keepalive a replica says it keeps it may say nothing for before it '
    'is silent.';
