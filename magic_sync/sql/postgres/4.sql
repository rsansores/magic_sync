-- The sync engine: the server's side, version 4. Routed tables.
--
-- A table routed by a view of the host's (`routed(view = …)`) is held where
-- the view pairs its rows with replicas. The engine reads what the view
-- reads off the catalog — tables, through other views and SQL-standard
-- functions — and each of those tables notes a change of what the view
-- reads of it: once a statement, once each partition it touched. The
-- planner asks the view again for every replica of that partition.

ALTER TABLE sync.notes DROP CONSTRAINT notes_kind_check;
ALTER TABLE sync.notes ADD CONSTRAINT notes_kind_check
    CHECK (kind IN ('row', 'retry', 'members', 'assigned', 'resync', 'rerouted'));

COMMENT ON COLUMN sync.notes.kind IS
    'row: a row of relation changed. retry: a row set aside is tried again where it was set '
    'aside, and nothing else. members: a row joined or left a group. assigned: a group was '
    'given to or taken from replica_id. rerouted: what a routed table''s view reads of '
    'relation changed in partition_id — in every partition, when NULL. resync: replica_id is '
    'provisioned again.';

-- Arguments: the table as SQL names it, its partition column ('' for none),
-- then, for an update, the columns the views read of it. Fires once a
-- statement, with the statement's rows as the transition tables sync_old
-- and sync_new, and notes each partition they hold once: an insert's, a
-- delete's, and of an update only those whose rows differ in what the views
-- read — compared as text, the partition as it is — before and after. A
-- truncate notes every partition.
CREATE FUNCTION sync.note_reroute() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    v_read TEXT;
    v_rows TEXT;
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        INSERT INTO sync.notes (kind, relation) VALUES ('rerouted', TG_ARGV[0]);
        RETURN NULL;
    END IF;
    SELECT CASE WHEN TG_ARGV[1] = '' THEN 'NULL::uuid' ELSE format('%I', TG_ARGV[1]) END
           || ' AS sync_partition'
           || coalesce(string_agg(format(', %I::text', c), '' ORDER BY n), '')
      INTO v_read
      FROM unnest(TG_ARGV[2:]) WITH ORDINALITY AS a(c, n);
    v_rows := CASE TG_OP
        WHEN 'INSERT' THEN format('SELECT %s FROM sync_new', v_read)
        WHEN 'DELETE' THEN format('SELECT %s FROM sync_old', v_read)
        ELSE format('(SELECT %1$s FROM sync_old EXCEPT ALL SELECT %1$s FROM sync_new) '
                    'UNION ALL (SELECT %1$s FROM sync_new EXCEPT ALL SELECT %1$s FROM sync_old)',
                    v_read)
    END;
    EXECUTE format('INSERT INTO sync.notes (kind, relation, partition_id) '
                   'SELECT DISTINCT ''rerouted'', $1, r.sync_partition FROM (%s) AS r', v_rows)
        USING TG_ARGV[0];
    RETURN NULL;
END;
$$;
