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
