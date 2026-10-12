-- The sync engine: the server's side, version 2. Wishes.
--
-- A wish is what the server wants one setting of a replica to hold: a row of
-- a table declared `mirror = "setting"`, which the replica owns. One per
-- setting row, replaced by the next. It travels as a row of this table, which
-- is written on both sides: the server owns what is wished, the replica owns
-- its answer. `Server::desire` writes it, `Server::withdraw` deletes it.
CREATE TABLE sync.sync_wishes (
    -- Derived from the setting's table and key: wishing a row again replaces
    -- its wish.
    id           UUID PRIMARY KEY,
    -- The replica the setting belongs to (its `origin`), which the wish goes to.
    replica_id   UUID NOT NULL,
    relation     TEXT NOT NULL,
    row_key      TEXT NOT NULL,
    -- The row as wished, in the setting's own encoding: the replica takes its
    -- desired columns, and, when `if_unchanged`, compares its version.
    wanted       BYTEA NOT NULL,
    -- Drawn at random for every wish, never 0: only whether an answer is of
    -- this wish matters, and a server restored from an older copy draws
    -- others.
    version      BIGINT NOT NULL CHECK (version > 0),
    if_unchanged BOOLEAN NOT NULL,
    note         TEXT,
    wished_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- The replica's answer, and the wish it answers.
    answered     BIGINT,
    outcome      TEXT CHECK (outcome IN ('applied', 'refused', 'superseded', 'waiting')),
    reason       TEXT,
    answered_at  TIMESTAMPTZ
);

CREATE INDEX sync_wishes_replica ON sync.sync_wishes (replica_id);
CREATE INDEX sync_wishes_row ON sync.sync_wishes (relation, row_key);
