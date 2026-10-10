// Generated from magic_sync::status by its test `the_dashboards_types_are_generated_from_these`: UPDATE_CONTRACT=1 rewrites it.

/** The contract these types describe: an answer of another is refused. */
export const CONTRACT = 1

/**
 * Where a replica stands, in one word: the first of these that applies.
 * Ordered worst first. It reads back from its JSON too, for a host that
 * takes the state to list from a request.
 */
export type ReplicaState = "awaiting_fix" | "unreachable" | "catching_up" | "synced";

/**
 * What one row is to one replica.
 */
export type RowState = "holds" | "outdated" | "on_its_way" | "missing" | "leaving" | "quarantined";

/**
 * What a note asks for.
 */
export type NoteKind = "row" | "members" | "assigned" | "resync";

/**
 * How far this replica is from having sent the server what it wrote, per
 * table: rows noted and not sent yet, rows sent the server has not said it
 * filed, and rows that cannot go as they are.
 */
export type UpProgress = { table: string, unsent: number, unfiled: number, set_aside: number, };

/**
 * What a replica wrote and the server has not filed, as of when it said so.
 */
export type Backlog = { tables: Array<UpProgress>, at: string, };

/**
 * One replica, from what the engine records about it: nothing here counts
 * the rows it should hold, so it costs the same for a fleet as for one.
 * Whether it is synced is exact all the same: a change it lacks is either a
 * note not yet planned, a message it has not confirmed, or set aside.
 */
export type ReplicaOverview = { replica: string, state: ReplicaState, 
/**
 * Changes noted and not yet planned that may concern it.
 */
pending: number, 
/**
 * Of those, the ones owed to it alone after a send to it failed.
 */
owed: number, 
/**
 * Changes the planner set aside that may concern it.
 */
parked: number, 
/**
 * Whether it has messages it has not said it applied.
 */
unconfirmed: boolean, 
/**
 * Rows set aside for it until a fix.
 */
quarantined: number, 
/**
 * Rows it sent that wait in the inbox until a fix.
 */
unfiled: number, 
/**
 * Sends to it that failed in a row, and the last one's error.
 */
failures: number, last_error: string | null, 
/**
 * When it is tried again, while it waits out a failure.
 */
retry_at: string | null, 
/**
 * When it was last sent a message, and when it last said it applied one.
 */
sent_at: string | null, applied_at: string | null, 
/**
 * While it has messages it has not said it applied: since when it has
 * not moved — the first one it did not confirm, or its last confirmation
 * that left it behind. How long a replica has not answered.
 */
waiting_since: string | null, 
/**
 * What it last said it wrote and the server has not filed, per table:
 * `None` until it says.
 */
backlog: Backlog | null, };

/**
 * How many replicas are in each state.
 */
export type ReplicaCounts = { awaiting_fix: number, unreachable: number, catching_up: number, synced: number, };

/**
 * One table over the replicas of a scope: every figure zero means each of
 * them holds it as it should. It is read without counting what any replica
 * should hold, or which of them a pending change will reach.
 */
export type TableOverview = { table: string, 
/**
 * Every figure below is zero.
 */
synced: boolean, 
/**
 * Changes to its rows not yet planned.
 */
pending: number, 
/**
 * Changes to its rows the planner set aside until a fix.
 */
parked: number, 
/**
 * Rows sent and not confirmed yet.
 */
in_flight: number, 
/**
 * Rows set aside until a fix.
 */
quarantined: number, 
/**
 * Uploaded rows waiting in the inbox until a fix.
 */
unfiled: number, 
/**
 * Replicas with a row of it in flight, set aside or unfiled. A change
 * not yet planned is counted in `pending`, not here: who it goes to is
 * what planning decides.
 */
replicas: number, };

/**
 * The entries of a list a read asked for, and how many there are in all.
 */
export type Listed<T> = { total: number, first: Array<T>, };

/**
 * A change the planner set aside.
 */
export type Parked = { id: number, kind: NoteKind, table: string | null, key: string | null, replica: string | null, attempts: number, last_error: string | null, parked_at: string | null, };

/**
 * A row set aside for one replica.
 */
export type Quarantined = { replica: string, table: string, key: string, reason: string, first_at: string, last_at: string, attempts: number, };

/**
 * What a replica sent that the server could not file: a row, a delete, or a
 * whole upload.
 */
export type Unfiled = { id: number, replica: string, 
/**
 * Empty for a whole upload kept as it came.
 */
table: string, reason: string, first_at: string, last_at: string, attempts: number, };

/**
 * A replica sends to keep failing to.
 */
export type Failing = { replica: string, failures: number, last_error: string | null, retry_at: string | null, };

/**
 * What the engine is waiting on, over a scope: each entry is either the
 * infrastructure (wait) or a bug (deploy a fix) — never data to repair by
 * hand. Lists are cut at the limit the read was given; `total` is uncut.
 */
export type Health = { 
/**
 * Nothing waits for a fix. A replica out of reach is not unhealthy — it
 * is the network's to settle, and listed in `failing`; nor is a
 * backlog, unless it stops draining, which the host says when.
 */
healthy: boolean, 
/**
 * Changes noted and not yet planned, and when the oldest the planner is
 * due to get to was noted: one owed to a replica out of reach waits for
 * it, and is not counted there.
 */
pending: number, oldest: string | null, parked: Listed<Parked>, quarantine: Listed<Quarantined>, inbox: Listed<Unfiled>, failing: Listed<Failing>, };

/**
 * [`Engine::overview`]: everything a dashboard shows first, in one read.
 * Its size does not grow with the fleet: the replicas are counted per
 * state, and listed worst first up to the limit; [`Engine::listing`] pages
 * through the rest.
 */
export type Overview = { contract: number, 
/**
 * The server's time when the answer was read: every time in it is the
 * server's, and how long ago one was is measured from this.
 */
now: string, health: Health, states: ReplicaCounts, replicas: Listed<ReplicaOverview>, tables: Array<TableOverview>, };

/**
 * [`Engine::listing`]: one page of the replicas, worst first.
 */
export type Listing = { contract: number, 
/**
 * The server's time when the answer was read.
 */
now: string, 
/**
 * The state the page was asked for (`None`: every state), and where in
 * the list it begins.
 */
state: ReplicaState | null, offset: number, replicas: Listed<ReplicaOverview>, };

/**
 * One table on one replica, counted from what the replica acknowledged
 * applying. `holds - outdated` of `should` is how far along it is; the
 * numbers never count a row as held that the replica has not confirmed.
 * Every row the engine sent it is in exactly one of `holds`, `in_flight`
 * and `to_remove`.
 */
export type TableProgress = { table: string, 
/**
 * Rows its rules say it should hold. For a table that only travels with
 * others, what the engine decided to send it.
 */
should: number, 
/**
 * Rows it should hold and has confirmed it holds.
 */
holds: number, 
/**
 * Of those, rows changed since: a newer version is on its way or owed.
 */
outdated: number, 
/**
 * Rows it should hold, sent and not confirmed yet.
 */
in_flight: number, 
/**
 * Rows it was sent and should no longer hold: on their way out.
 */
to_remove: number, 
/**
 * Rows set aside for it until a fix.
 */
quarantined: number, };

/**
 * How far one replica is from holding what it should.
 */
export type ReplicaProgress = { replica: string, tables: Array<TableProgress>, };

/**
 * [`Engine::progress`]: how far each replica of a scope is from holding
 * what it should.
 */
export type Progress = { contract: number, replicas: Array<ReplicaProgress>, };

/**
 * Where one row stands on one replica.
 */
export type RowPlace = { replica: string, state: RowState, };

/**
 * [`Engine::row_progress`]: where one row stands on each replica of a scope
 * that holds it, should, or is owed it.
 */
export type RowProgress = { contract: number, places: Array<RowPlace>, };
