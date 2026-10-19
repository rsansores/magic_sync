-- The sync engine: the server's side, version 3. Audiences.
--
-- A server serves one kind of replica or several — an audience each: where
-- it finds them, and the tables they hold, each in the shape it sends them.
-- One note stream, one relay; each audience planned against its own
-- replicas.

-- Which audience a replica is of, as the engine last saw it live. NULL: seen
-- before the engine knew of audiences, and claimed by the first audience
-- that sees it, with no provisioning.
ALTER TABLE sync.replicas ADD COLUMN audience TEXT;

COMMENT ON COLUMN sync.replicas.audience IS
    'The audience the replica is of, by its name: what plans what it holds, and what it is '
    'sent and uploads in. A replica two audiences name is the first''s.';

-- What decided which rows each audience's replicas hold, and what a row
-- carries, when the relay last ran: a deploy that changes one audience's
-- provisions that audience's replicas again, and no other's.
CREATE TABLE sync.audiences (
    name     TEXT PRIMARY KEY,
    declared TEXT NOT NULL
);

COMMENT ON TABLE sync.audiences IS
    'A digest of what decided which rows each audience''s replicas hold, and what a row carries, '
    'when the relay last ran: its replicas, and per table its key, its rule, what it points at, '
    'its groups and its columns. A relay that finds it different provisions every replica of '
    'the audience again. sync.version.declared was the one audience''s before there were '
    'several: the first audience to run takes it, and it is cleared.';
