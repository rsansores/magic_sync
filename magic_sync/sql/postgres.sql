-- The sync engine: the server's side, in its own schema. Version 1.
--
-- A trigger on every mirrored table notes each change in the writing
-- transaction, so no write path can forget one; the planner reads the notes,
-- compares what each replica should hold with what it is known to hold, sends
-- the difference and records it. The triggers are the engine's to write:
-- `Engine::triggers` prints them for a host's migrations, `Engine::install`
-- creates them, and `Engine::new` refuses to start while one differs.

CREATE SCHEMA sync;

-- The version of this schema: the engine refuses to run against another. A
-- later version is a script of its own, run after this one, that ends by
-- writing its number here.
CREATE TABLE sync.version (
    id       INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    version  INTEGER NOT NULL,
    declared TEXT
);

COMMENT ON COLUMN sync.version.declared IS
    'A digest of what decided which rows each replica holds, and what a row carries, when the '
    'relay last ran: the replicas, and per table its key, its rule, what it points at, its '
    'groups and its columns. A relay that finds it different provisions every replica again.';

-- What changed, in the writing transaction. Read and marked done by the one
-- relay that holds the engine's lock.
CREATE TABLE sync.notes (
    id           BIGSERIAL   PRIMARY KEY,
    kind         TEXT        NOT NULL CHECK (kind IN ('row', 'members', 'assigned', 'resync')),
    relation     TEXT,
    row_key      TEXT,
    replica_id   UUID,
    partition_id UUID,
    previous     JSONB,
    xid          XID8        NOT NULL DEFAULT pg_current_xact_id(),
    noted_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    done_at      TIMESTAMPTZ,
    parked_at    TIMESTAMPTZ,
    attempts     INTEGER     NOT NULL DEFAULT 0,
    last_error   TEXT
);

-- Every read of what is not done yet — the relay's, and a dashboard's, parked
-- notes included — stays among those, however many done ones wait to be pruned.
CREATE INDEX notes_undone ON sync.notes (id) WHERE done_at IS NULL;
-- The relay's read: what is pending, however many parked notes lead it.
CREATE INDEX notes_pending ON sync.notes (id) WHERE done_at IS NULL AND parked_at IS NULL;
-- The prune: what is done, oldest first.
CREATE INDEX notes_done ON sync.notes (done_at) WHERE done_at IS NOT NULL;
-- A replica's provisioning is noted once, however many reasons ask for it.
CREATE INDEX notes_resync ON sync.notes (replica_id) WHERE kind = 'resync' AND done_at IS NULL;
-- Whether a row has a change pending, by the row: what a retry notes once,
-- where a row stands, and the pending changes of a table the planner reads.
CREATE INDEX notes_rows ON sync.notes (relation, row_key) WHERE kind = 'row' AND done_at IS NULL;

COMMENT ON TABLE sync.notes IS
    'Every change to a mirrored table or a group table, and every replica to provision '
    'again, noted in the transaction that made it. It says what changed, never who receives '
    'it: the planner works that out when it reads the note.';
COMMENT ON COLUMN sync.notes.kind IS
    'row: a row of relation changed. members: a row joined or left a group. assigned: a '
    'group was given to or taken from replica_id. resync: replica_id is provisioned again.';
COMMENT ON COLUMN sync.notes.row_key IS
    'The changed row''s primary key, its parts joined by |, in key order.';
COMMENT ON COLUMN sync.notes.previous IS
    'For a row: the columns it points at other mirrored rows with, as they were before an '
    'update or a delete, so a parent it no longer points at can be let go — and nothing else '
    'of the row. For a member: the member row.';
COMMENT ON COLUMN sync.notes.xid IS
    'The transaction that noted it. A replica sent a row from a snapshot that sees it already '
    'holds this change.';
COMMENT ON COLUMN sync.notes.done_at IS
    'When the relay recorded what the note asked for and wrote its messages to the outbox. '
    'NULL: pending.';
COMMENT ON COLUMN sync.notes.parked_at IS
    'When the planner set the note aside because planning it fails for a reason a retry '
    'cannot change (a bug, data no declaration allows). Parked notes are tried again when '
    'the relay starts — after a deploy — and on a timer.';
COMMENT ON COLUMN sync.notes.attempts IS
    'Passes that failed on this note.';

-- What each replica holds, as the engine sent it: the other half of every
-- decision the planner takes.
CREATE TABLE sync.held (
    replica_id   UUID        NOT NULL,
    relation     TEXT        NOT NULL,
    row_key      TEXT        NOT NULL,
    sent_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    snapshot     PG_SNAPSHOT NOT NULL,
    sequence     BIGINT      NOT NULL,
    refused      BIGINT,
    PRIMARY KEY (replica_id, relation, row_key)
);

CREATE INDEX held_row ON sync.held (relation, row_key);

COMMENT ON TABLE sync.held IS
    'Every mirrored row each replica holds, as the engine sent it — recorded with the message '
    'that carries it before that message goes. What a replica should hold, less what it holds, '
    'is sent; what it holds and should no longer is taken back.';
COMMENT ON COLUMN sync.held.sent_at IS
    'When the version the replica holds was sent.';
COMMENT ON COLUMN sync.held.sequence IS
    'The message that carried the row: the replica holds it once it has acknowledged a '
    'message at or past this one. Until then the row is on its way.';
COMMENT ON COLUMN sync.held.refused IS
    'The message whose version of the row — or whose delete of it — the replica refused: it '
    'keeps what it had, or nothing. Held for every decision that takes rows back, so a delete '
    'still goes, and a refused delete is sent again; not held for progress, closure shown to '
    'users, or digests. Cleared once the replica applies a later message that carries the row. '
    'NULL: taken.';
COMMENT ON COLUMN sync.held.snapshot IS
    'The snapshot the row was last sent from: a change noted by a transaction it sees is in '
    'what the replica holds, and one by any other is not.';

-- Rows taken back from a replica, until it says it applied the message that
-- took them: a refusal of that message is a delete it could not carry out,
-- and the row is held again, refused; a refusal of an older one is of a
-- version it no longer matters it did not take.
CREATE TABLE sync.gone (
    replica_id UUID   NOT NULL,
    relation   TEXT   NOT NULL,
    row_key    TEXT   NOT NULL,
    sequence   BIGINT NOT NULL,
    PRIMARY KEY (replica_id, relation, row_key)
);

-- Every message the engine sends carries a number from this sequence: it
-- only rises, whichever relay draws it, so a replica can drop a message older
-- than one it applied.
CREATE SEQUENCE sync.messages;

-- Which installation of this schema the numbers come from: a database
-- installed again starts its sequence over, and a replica that sees a new
-- epoch takes its numbers from scratch.
CREATE TABLE sync.epoch (
    id    INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    epoch UUID    NOT NULL DEFAULT gen_random_uuid()
);
INSERT INTO sync.epoch DEFAULT VALUES;

-- The messages each replica is owed, written in the transaction that
-- records what they carry, and sent after it commits, in order: a message
-- leaves once the transport took it. A replica the transport cannot reach
-- keeps its own here, while every other is sent to.
CREATE TABLE sync.outbox (
    replica_id UUID        NOT NULL,
    sequence   BIGINT      NOT NULL,
    message    BYTEA       NOT NULL,
    snapshot   BOOLEAN     NOT NULL,
    written_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (replica_id, sequence)
);

COMMENT ON TABLE sync.outbox IS
    'Messages written for a replica and not yet taken by the transport, as the bytes it '
    'carries. Sent in sequence order once the pass that wrote them commits, by whichever '
    'process runs the relay: a crash or a lost lock loses none of them.';
COMMENT ON COLUMN sync.outbox.snapshot IS
    'Part of a snapshot: what the replica should hold, whole. A snapshot written for a replica '
    'replaces its outbox, so these lead it; what follows them counts against its bound.';

-- The messages written for each replica, as the relay alone writes them: a
-- pass records in one REPEATABLE READ snapshot, and nothing else updates
-- these rows under it.
CREATE TABLE sync.chains (
    replica_id        UUID        PRIMARY KEY,
    last_sequence     BIGINT,
    snapshot_sequence BIGINT,
    snapshot_end      BIGINT,
    dropped           TEXT,
    dropped_at        TIMESTAMPTZ
);

COMMENT ON COLUMN sync.chains.last_sequence IS
    'The last message written to its outbox in this epoch. NULL: none since it was last '
    'provisioned or the epoch began.';
COMMENT ON COLUMN sync.chains.snapshot_sequence IS
    'The first message of the last snapshot written to its outbox, in this epoch.';
COMMENT ON COLUMN sync.chains.snapshot_end IS
    'The message that ended that snapshot: a row it refused to let go of at the sweep is held '
    'again, refused.';
COMMENT ON COLUMN sync.chains.dropped IS
    'Why its outbox was last dropped and it provisioned again, an alarm: it outgrew its bound, '
    'or the transport refused its next message for good again and again.';

-- How sending to each replica is going, and what the engine last saw of it.
CREATE TABLE sync.replicas (
    replica_id   UUID        PRIMARY KEY,
    live         BOOLEAN     NOT NULL DEFAULT false,
    partition_id UUID,
    failures     INTEGER     NOT NULL DEFAULT 0,
    retry_after  TIMESTAMPTZ,
    last_error       TEXT,
    last_sent_at     TIMESTAMPTZ,
    refusals         INTEGER     NOT NULL DEFAULT 0,
    applied_epoch    UUID,
    applied_sequence BIGINT,
    applied_at       TIMESTAMPTZ,
    waiting_since    TIMESTAMPTZ,
    heals            INTEGER     NOT NULL DEFAULT 0,
    backlog          JSONB,
    backlog_at       TIMESTAMPTZ,
    drifted_at       TIMESTAMPTZ
);

COMMENT ON TABLE sync.replicas IS
    'How sending to each replica is going: consecutive failures, when it is tried again, '
    'when it was last sent to, and the last message it acknowledged applying.';
COMMENT ON COLUMN sync.replicas.live IS
    'Whether the engine last saw it live, in partition_id: a replica that became live or moved '
    'is provisioned again, one that stopped being live is forgotten.';
COMMENT ON COLUMN sync.replicas.waiting_since IS
    'While it has messages it has not said it applied: since when it has not moved — the '
    'first send it did not confirm, or the last confirmation that left it behind. NULL: caught up.';
COMMENT ON COLUMN sync.replicas.heals IS
    'Times it was provisioned again for staying silent on what it was sent, since it last '
    'caught up: each waits twice as long as the one before, up to 32 times the first.';
COMMENT ON COLUMN sync.replicas.backlog IS
    'What the replica last said it wrote and the server has not filed, per table '
    '([{table, unsent, unfiled}]), as of backlog_at. NULL until it says.';
COMMENT ON COLUMN sync.replicas.last_error IS
    'Why the last send to it failed — the network, or a message the transport refuses for '
    'good — while failures counts on, or why it was provisioned again. NULL once a send gets '
    'through.';
COMMENT ON COLUMN sync.replicas.refusals IS
    'Times in a row the transport refused its next message for good: past a few it is '
    'provisioned again, and that is an alarm.';
COMMENT ON COLUMN sync.replicas.drifted_at IS
    'When its digests last differed from what the server records and it was provisioned '
    'again for it. Digests that differ again within a day are a bug no provisioning cures: '
    'they are logged, and it is not provisioned again before the day is out.';

-- Rows a replica should hold that the engine could not get to it for a reason
-- a retry cannot change: a row that does not encode or fits no message, a row
-- the replica refused. Each is tried again on a timer and when the relay
-- starts, and leaves when the replica holds it.
CREATE TABLE sync.quarantine (
    replica_id UUID        NOT NULL,
    relation   TEXT        NOT NULL,
    row_key    TEXT        NOT NULL,
    cause      TEXT        NOT NULL CHECK (cause IN ('server', 'replica')),
    reason     TEXT        NOT NULL,
    first_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts   INTEGER     NOT NULL DEFAULT 1,
    carried    BIGINT,
    PRIMARY KEY (replica_id, relation, row_key)
);

-- Where one row stands reads its quarantine by the row, over every replica.
CREATE INDEX quarantine_row ON sync.quarantine (relation, row_key);

COMMENT ON TABLE sync.quarantine IS
    'Rows a replica should hold and does not, for a reason a retry cannot change. Never '
    'repaired by hand: a deploy that fixes the cause lets the next retry through.';
COMMENT ON COLUMN sync.quarantine.cause IS
    'server: the server could not send the row — or a row it points at — and the replica '
    'does not hold it: an older version it had is taken back. replica: the replica refused it, and leaves it out of its digests, as the server '
    'does when it compares them.';
COMMENT ON COLUMN sync.quarantine.carried IS
    'The message that carried the row again since it was set aside: once the replica says it '
    'applied it, the row leaves the quarantine. NULL: not carried since.';

-- What replicas uploaded that the server could not file, kept as it came: the
-- replica may have let go of it since. Filed again on a timer and when the
-- relay starts, and gone once filed.
CREATE TABLE sync.inbox (
    id         BIGSERIAL   PRIMARY KEY,
    replica_id UUID        NOT NULL,
    relation   TEXT,
    upload     BYTEA       NOT NULL,
    reason     TEXT        NOT NULL,
    first_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts   INTEGER     NOT NULL DEFAULT 1
);

CREATE INDEX inbox_replica ON sync.inbox (replica_id);
-- The same part of an upload kept twice — the replica sent it again — is one.
CREATE UNIQUE INDEX inbox_once ON sync.inbox (replica_id, md5(upload));

COMMENT ON TABLE sync.inbox IS
    'What replicas uploaded that the server could not file for a reason a retry cannot '
    'change: a table it does not mirror, a row that does not decode, a row or a delete its '
    'database refuses — or a whole upload, when filing it failed as a whole. Never repaired '
    'by hand: a deploy that fixes the cause lets the next retry file it.';
COMMENT ON COLUMN sync.inbox.relation IS
    'The table, for one row or one delete. NULL: a whole upload.';
COMMENT ON COLUMN sync.inbox.upload IS
    'An Up, as protobuf: the upload as it came, or one table''s row or delete of it.';

-- A truncate would take rows away without a note, and every replica would
-- keep them.
CREATE FUNCTION sync.refuse_truncate() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '%.% is mirrored: delete its rows, so each is noted', TG_TABLE_SCHEMA, TG_TABLE_NAME;
END;
$$;

-- Arguments: the table's name as the engine knows it, its partition column
-- ('' for none), the number of key columns and the key's columns in key
-- order, the number of columns kept of the row as it was and those columns
-- (what it points at), then the columns the replicas own. An update that
-- changes only those is no change here.
CREATE FUNCTION sync.note_row() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    v_keys    INTEGER := TG_ARGV[2]::INTEGER;
    v_key     TEXT[]  := TG_ARGV[3:2 + v_keys];
    v_kept    INTEGER := TG_ARGV[3 + v_keys]::INTEGER;
    v_pointed TEXT[]  := TG_ARGV[4 + v_keys:3 + v_keys + v_kept];
    v_owned   TEXT[]  := coalesce(TG_ARGV[4 + v_keys + v_kept:], '{}');
    v_new     JSONB;
    v_old     JSONB;
    v_new_key TEXT;
    v_old_key TEXT;
    v_previous JSONB;
BEGIN
    IF TG_OP <> 'DELETE' THEN
        v_new := to_jsonb(NEW);
        SELECT string_agg(v_new ->> k, '|' ORDER BY n) INTO v_new_key
          FROM unnest(v_key) WITH ORDINALITY AS a(k, n);
    END IF;
    IF TG_OP <> 'INSERT' THEN
        v_old := to_jsonb(OLD);
        -- As text: a number written at another scale (1.0, 1.00) is equal
        -- as jsonb, and reads otherwise on a replica.
        IF TG_OP = 'UPDATE' AND (v_old - v_owned)::text = (v_new - v_owned)::text THEN
            RETURN NULL;
        END IF;
        SELECT string_agg(v_old ->> k, '|' ORDER BY n) INTO v_old_key
          FROM unnest(v_key) WITH ORDINALITY AS a(k, n);
        SELECT coalesce(jsonb_object_agg(k, v_old -> k), '{}') INTO v_previous
          FROM unnest(v_pointed) AS a(k);
    END IF;
    IF v_new_key IS NOT NULL THEN
        INSERT INTO sync.notes (kind, relation, row_key, partition_id, previous)
        VALUES ('row', TG_ARGV[0], v_new_key,
                CASE WHEN TG_ARGV[1] = '' THEN NULL ELSE (v_new ->> TG_ARGV[1])::UUID END,
                v_previous);
    END IF;
    -- A row deleted, or whose key changed: the old key is gone.
    IF v_old_key IS DISTINCT FROM v_new_key AND v_old_key IS NOT NULL THEN
        INSERT INTO sync.notes (kind, relation, row_key, partition_id, previous)
        VALUES ('row', TG_ARGV[0], v_old_key,
                CASE WHEN TG_ARGV[1] = '' THEN NULL ELSE (v_old ->> TG_ARGV[1])::UUID END,
                v_previous);
    END IF;
    RETURN NULL;
END;
$$;

-- Arguments: the table's name as the engine knows it, the column to note,
-- members or assigned, then the partition column ('' for none). Each version
-- of the row is noted: a member moving between groups leaves one and joins
-- the other.
CREATE FUNCTION sync.note_group() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO sync.notes (kind, relation, row_key, replica_id, partition_id, previous)
    SELECT TG_ARGV[2], TG_ARGV[0], r ->> TG_ARGV[1],
           CASE WHEN TG_ARGV[2] = 'assigned' THEN (r ->> TG_ARGV[1])::UUID END,
           CASE WHEN TG_ARGV[3] = '' THEN NULL ELSE (r ->> TG_ARGV[3])::UUID END,
           CASE WHEN TG_ARGV[2] = 'members' THEN r END
      FROM (SELECT DISTINCT r
              FROM unnest(ARRAY[
                       CASE WHEN TG_OP = 'DELETE' THEN NULL ELSE to_jsonb(NEW) END,
                       CASE WHEN TG_OP = 'INSERT' THEN NULL ELSE to_jsonb(OLD) END]) AS r
             WHERE r IS NOT NULL) AS versions;
    RETURN NULL;
END;
$$;

INSERT INTO sync.version (version) VALUES (1);
