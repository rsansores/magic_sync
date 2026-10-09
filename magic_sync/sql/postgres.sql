-- The sync engine: the server's side, in its own schema.
--
-- A trigger on every mirrored table notes each change in the writing
-- transaction, so no write path can forget one; the planner reads the notes,
-- compares what each replica should hold with what it is known to hold, sends
-- the difference and records it.

CREATE SCHEMA sync;

-- The version of this schema: the engine refuses to run against another.
CREATE TABLE sync.version (
    id      INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    version INTEGER NOT NULL
);
INSERT INTO sync.version (version) VALUES (1);

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
    scope        UUID,
    xid          XID8        NOT NULL DEFAULT pg_current_xact_id(),
    noted_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    done_at      TIMESTAMPTZ,
    parked_at    TIMESTAMPTZ,
    attempts     INTEGER     NOT NULL DEFAULT 0,
    last_error   TEXT
);

CREATE INDEX notes_pending ON sync.notes (id) WHERE done_at IS NULL AND parked_at IS NULL;

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
    'For a row: the row before an update or a delete, which says what it pointed at, so a '
    'parent it no longer points at can be let go. For a member: the member row.';
COMMENT ON COLUMN sync.notes.xid IS
    'The transaction that noted it. A replica sent a row from a snapshot whose horizon is past '
    'it already holds this change.';
COMMENT ON COLUMN sync.notes.scope IS
    'The one replica the note is still owed to, after a send to it failed: what it asks is '
    'planned for that replica alone. NULL: every replica.';
COMMENT ON COLUMN sync.notes.done_at IS
    'When the relay sent what the note asked for and recorded it. NULL: pending.';
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
    partition_id UUID,
    held_since   TIMESTAMPTZ NOT NULL DEFAULT now(),
    horizon      XID8        NOT NULL,
    sequence     BIGINT,
    PRIMARY KEY (replica_id, relation, row_key)
);

CREATE INDEX held_row ON sync.held (relation, row_key);

COMMENT ON TABLE sync.held IS
    'Every mirrored row each replica holds, as the engine sent it. What a replica should '
    'hold, less what it holds, is sent; what it holds and should no longer is taken back.';
COMMENT ON COLUMN sync.held.sequence IS
    'The message that carried the row: the replica holds it once it has acknowledged a '
    'message at or past this one. Until then the row is on its way.';
COMMENT ON COLUMN sync.held.horizon IS
    'The xmin of the snapshot the row was last sent from: every transaction before it had '
    'finished, so a change it noted is in what the replica holds.';

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

-- A replica the relay could not send to: it is left out of passes until
-- retry_after, while every other replica carries on.
CREATE TABLE sync.replicas (
    replica_id   UUID        PRIMARY KEY,
    failures     INTEGER     NOT NULL DEFAULT 0,
    retry_after  TIMESTAMPTZ,
    last_error       TEXT,
    last_sent_at     TIMESTAMPTZ,
    last_sequence    BIGINT,
    applied_epoch    UUID,
    applied_sequence BIGINT,
    applied_at       TIMESTAMPTZ
);

COMMENT ON TABLE sync.replicas IS
    'How sending to each replica is going: consecutive failures, when it is tried again, '
    'when it was last sent to, and the last message it acknowledged applying.';

-- Rows a replica should hold that the engine could not get to it for a reason
-- a retry cannot change: a row that does not encode, a message the transport
-- refuses, a row the replica refused. Each is tried again on a timer and when
-- the relay starts, and leaves when the replica holds it.
CREATE TABLE sync.quarantine (
    replica_id UUID        NOT NULL,
    relation   TEXT        NOT NULL,
    row_key    TEXT        NOT NULL,
    reason     TEXT        NOT NULL,
    first_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts   INTEGER     NOT NULL DEFAULT 1,
    PRIMARY KEY (replica_id, relation, row_key)
);

COMMENT ON TABLE sync.quarantine IS
    'Rows a replica should hold and does not, for a reason a retry cannot change. Never '
    'repaired by hand: a deploy that fixes the cause lets the next retry through.';

-- Rows a replica uploaded that the server could not file, kept as they came:
-- the replica may have let go of them since. Filed again on a timer and when
-- the relay starts, and gone once filed.
CREATE TABLE sync.inbox (
    id         BIGSERIAL   PRIMARY KEY,
    replica_id UUID        NOT NULL,
    relation   TEXT        NOT NULL,
    row_bytes  BYTEA       NOT NULL,
    reason     TEXT        NOT NULL,
    first_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts   INTEGER     NOT NULL DEFAULT 1
);

COMMENT ON TABLE sync.inbox IS
    'Rows replicas uploaded that the server could not file for a reason a retry cannot '
    'change: a table it does not mirror, a row that does not decode, one its database '
    'refuses. Never repaired by hand: a deploy that fixes the cause lets the next retry file it.';

-- A truncate would take rows away without a note, and every replica would
-- keep them.
CREATE FUNCTION sync.refuse_truncate() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '%.% is mirrored: delete its rows, so each is noted', TG_TABLE_SCHEMA, TG_TABLE_NAME;
END;
$$;

-- Arguments: the partition column ('' for none), then the primary key's
-- columns in order.
CREATE FUNCTION sync.note_row() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    v_row       JSONB := to_jsonb(CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END);
    v_partition UUID  := CASE WHEN TG_ARGV[0] = '' THEN NULL ELSE (v_row ->> TG_ARGV[0])::UUID END;
    v_key       TEXT;
    v_old_key   TEXT;
BEGIN
    SELECT string_agg(v_row ->> k, '|' ORDER BY n) INTO v_key
      FROM unnest(TG_ARGV[1:]) WITH ORDINALITY AS a(k, n);
    INSERT INTO sync.notes (kind, relation, row_key, partition_id, previous)
    VALUES ('row', TG_TABLE_NAME, v_key, v_partition,
            CASE WHEN TG_OP = 'INSERT' THEN NULL ELSE to_jsonb(OLD) END);
    IF TG_OP = 'UPDATE' THEN
        SELECT string_agg(to_jsonb(OLD) ->> k, '|' ORDER BY n) INTO v_old_key
          FROM unnest(TG_ARGV[1:]) WITH ORDINALITY AS a(k, n);
        -- A key that changed: the old one is gone.
        IF v_old_key IS DISTINCT FROM v_key THEN
            INSERT INTO sync.notes (kind, relation, row_key, partition_id, previous)
            VALUES ('row', TG_TABLE_NAME, v_old_key, v_partition, to_jsonb(OLD));
        END IF;
    END IF;
    RETURN NULL;
END;
$$;

-- Arguments: the column to note, members or assigned, then the partition
-- column ('' for none). Each version of the row is noted: a member moving
-- between groups leaves one and joins the other.
CREATE FUNCTION sync.note_group() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO sync.notes (kind, relation, row_key, replica_id, partition_id, previous)
    SELECT TG_ARGV[1], TG_TABLE_NAME, r ->> TG_ARGV[0],
           CASE WHEN TG_ARGV[1] = 'assigned' THEN (r ->> TG_ARGV[0])::UUID END,
           CASE WHEN TG_ARGV[2] = '' THEN NULL ELSE (r ->> TG_ARGV[2])::UUID END,
           CASE WHEN TG_ARGV[1] = 'members' THEN r END
      FROM (SELECT DISTINCT r
              FROM unnest(ARRAY[
                       CASE WHEN TG_OP = 'DELETE' THEN NULL ELSE to_jsonb(NEW) END,
                       CASE WHEN TG_OP = 'INSERT' THEN NULL ELSE to_jsonb(OLD) END]) AS r
             WHERE r IS NOT NULL) AS versions;
    RETURN NULL;
END;
$$;

-- Mirrors p_schema.p_table: every insert, delete, and update that changes the
-- row is noted, keyed by its primary key; an update that changes only the
-- p_ignore columns (the ones its replicas own) is not. p_partition names the
-- column that partitions it, when the engine runs with partitions.
CREATE FUNCTION sync.mirror_table(
    p_schema TEXT, p_table TEXT, p_ignore TEXT[] DEFAULT '{}', p_partition TEXT DEFAULT ''
)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    v_args TEXT;
BEGIN
    SELECT string_agg(quote_literal(a.attname), ', ' ORDER BY k.n) INTO v_args
      FROM pg_index i
      CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, n)
      JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
     WHERE i.indrelid = format('%I.%I', p_schema, p_table)::regclass AND i.indisprimary;
    IF v_args IS NULL THEN
        RAISE EXCEPTION 'mirror_table: %.% has no primary key to note its rows by', p_schema, p_table;
    END IF;
    v_args := quote_literal(p_partition) || ', ' || v_args;
    EXECUTE format(
        'CREATE TRIGGER sync_note_written AFTER INSERT OR DELETE ON %I.%I '
        'FOR EACH ROW EXECUTE FUNCTION sync.note_row(%s)', p_schema, p_table, v_args);
    EXECUTE format(
        'CREATE TRIGGER sync_note_changed AFTER UPDATE ON %I.%I '
        'FOR EACH ROW WHEN ((to_jsonb(OLD) - %L::text[]) IS DISTINCT FROM (to_jsonb(NEW) - %L::text[])) '
        'EXECUTE FUNCTION sync.note_row(%s)',
        p_schema, p_table, p_ignore, p_ignore, v_args);
    EXECUTE format(
        'CREATE TRIGGER sync_refuse_truncate BEFORE TRUNCATE ON %I.%I '
        'FOR EACH STATEMENT EXECUTE FUNCTION sync.refuse_truncate()', p_schema, p_table);
END;
$$;

-- A table that assigns mirrored rows by group: p_role is members (a row joins
-- or leaves a group; p_column is the group) or assigned (a group is given to
-- or taken from a replica; p_column is the replica).
CREATE FUNCTION sync.mirror_group(
    p_schema TEXT, p_table TEXT, p_column TEXT, p_role TEXT, p_partition TEXT DEFAULT ''
)
RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    IF p_role NOT IN ('members', 'assigned') THEN
        RAISE EXCEPTION 'mirror_group: role is members or assigned, not %', p_role;
    END IF;
    EXECUTE format(
        'CREATE TRIGGER sync_note_group AFTER INSERT OR UPDATE OR DELETE ON %I.%I '
        'FOR EACH ROW EXECUTE FUNCTION sync.note_group(%L, %L, %L)',
        p_schema, p_table, p_column, p_role, p_partition);
END;
$$;
