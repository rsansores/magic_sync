// Generated from magic_sync::status by its test `the_dashboards_types_are_generated_from_these`: UPDATE_CONTRACT=1 rewrites it.

/** The contract these types describe: an answer of another is refused. */
export const CONTRACT = 2

/**
 * Where a replica stands, in one word: the first of these that applies.
 * Ordered worst first.
 */
export type ReplicaState = "awaiting_fix" | "unreachable" | "silent" | "catching_up" | "synced";

/**
 * What one row is to one replica.
 */
export type RowState = "holds" | "outdated" | "on_its_way" | "missing" | "leaving" | "quarantined";

/**
 * What a note asks for.
 */
export type NoteKind = "row" | "members" | "assigned" | "rerouted" | "resync";

/**
 * How far this replica is from having sent the server what it wrote, per
 * table: rows noted and not sent yet, rows sent the server has not said it
 * filed, and rows that cannot go as they are.
 */
export type UpProgress = { 
/**
 * The table.
 */
table: string, 
/**
 * Rows written and not sent yet.
 */
unsent: number, 
/**
 * Rows sent that the server has not said it filed.
 */
unfiled: number, 
/**
 * Rows that cannot go up as they are, until a build that can send them.
 */
set_aside: number, 
/**
 * When the oldest of these rows was written: by the replica's clock on
 * the replica, and on the server as it reckons it from the age the
 * replica said. `None` from a replica that does not say.
 */
oldest_at: string | null, };

/**
 * One table of a replica's backlog, and whether its rows wait.
 */
export type UpTable = { 
/**
 * Its rows wait, and hold the replica back: some cannot go as they
 * are, or the oldest was written longer than the server's
 * `Settings::behind_after` ago — or, of a replica that says no age,
 * some have not gone. Rows written a moment ago flow.
 */
waits: boolean, 
/**
 * The table.
 */
table: string, 
/**
 * Rows written and not sent yet.
 */
unsent: number, 
/**
 * Rows sent that the server has not said it filed.
 */
unfiled: number, 
/**
 * Rows that cannot go up as they are, until a build that can send them.
 */
set_aside: number, 
/**
 * When the oldest of these rows was written: by the replica's clock on
 * the replica, and on the server as it reckons it from the age the
 * replica said. `None` from a replica that does not say.
 */
oldest_at: string | null, };

/**
 * What a replica wrote and the server has not filed, as of when it said so.
 */
export type Backlog = { 
/**
 * Each table with something on its way.
 */
tables: Array<UpTable>, 
/**
 * When the replica said so.
 */
at: string, };

/**
 * One replica, from what the engine records about it: nothing here counts
 * the rows it should hold, so it costs the same for a fleet as for one.
 * Whether it is synced is exact all the same: a change it lacks is either a
 * note not yet planned, a message it has not confirmed, or set aside.
 */
export type ReplicaOverview = { 
/**
 * The replica.
 */
replica: string, 
/**
 * The audience it is of.
 */
audience: string, 
/**
 * Where it stands, in one word.
 */
state: ReplicaState, 
/**
 * Changes noted and not yet planned that may concern it.
 */
pending: number, 
/**
 * Messages written for it that the transport has not taken yet: what
 * waits for it, in order, while it cannot be reached.
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
 * Sends to it that failed in a row.
 */
failures: number, 
/**
 * The last failed send's error — or, failing none, why what waited for
 * it was last dropped and it was provisioned again.
 */
last_error: string | null, 
/**
 * When it is tried again, while it waits out a failure.
 */
retry_at: string | null, 
/**
 * When it was last sent a message.
 */
sent_at: string | null, 
/**
 * When it last said it applied one.
 */
applied_at: string | null, 
/**
 * While it has messages it has not said it applied: since it last
 * moved — its last send or its last confirmation, whichever came later.
 * How long a replica has not answered.
 */
waiting_since: string | null, 
/**
 * When the server last heard from it: an upload of any kind.
 */
last_heard_at: string | null, 
/**
 * What it last said it wrote and the server has not filed, per table:
 * `None` until it says.
 */
backlog: Backlog | null, 
/**
 * When the server last filed a row of each table from it, by table.
 */
filed: Array<TableFiled>, 
/**
 * Its settings' wishes, by where they stand.
 */
wishes: WishCounts, };

/**
 * How many replicas are in each state.
 */
export type ReplicaCounts = { 
/**
 * Replicas whose state is `awaiting_fix`.
 */
awaiting_fix: number, 
/**
 * Replicas whose state is `unreachable`.
 */
unreachable: number, 
/**
 * Replicas whose state is `silent`.
 */
silent: number, 
/**
 * Replicas whose state is `catching_up`.
 */
catching_up: number, 
/**
 * Replicas whose state is `synced`.
 */
synced: number, };

/**
 * One table over the replicas of a scope: every figure zero means each of
 * them holds it as it should. It is read without counting what any replica
 * should hold, or which of them a pending change will reach.
 */
export type TableOverview = { 
/**
 * The table.
 */
table: string, 
/**
 * The audience it is the table of: a table two audiences hold is
 * listed once for each, over its replicas.
 */
audience: string, 
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
export type Listed<T> = { 
/**
 * How many there are in all.
 */
total: number, 
/**
 * The first of them, up to the limit the read was given.
 */
first: Array<T>, };

/**
 * A change the planner set aside.
 */
export type Parked = { 
/**
 * The note's number.
 */
id: number, 
/**
 * What it asks for.
 */
kind: NoteKind, 
/**
 * The table it is of, for a row or a group — for a re-route, as SQL
 * names it.
 */
table: string | null, 
/**
 * The row's key, for a row.
 */
key: string | null, 
/**
 * The replica it is about.
 */
replica: string | null, 
/**
 * How many times it was planned and failed.
 */
attempts: number, 
/**
 * Why it failed last.
 */
last_error: string | null, 
/**
 * When it was set aside.
 */
parked_at: string | null, };

/**
 * A row set aside for one replica.
 */
export type Quarantined = { 
/**
 * The replica it is set aside for.
 */
replica: string, 
/**
 * Its table.
 */
table: string, 
/**
 * Its key.
 */
key: string, 
/**
 * Why it could not go, or could not be taken.
 */
reason: string, 
/**
 * When it was first set aside.
 */
first_at: string, 
/**
 * When it last failed.
 */
last_at: string, 
/**
 * How many times it failed.
 */
attempts: number, };

/**
 * What a replica sent that the server could not file: a row, or a delete.
 */
export type Unfiled = { 
/**
 * Its number in the inbox.
 */
id: number, 
/**
 * The replica that sent it.
 */
replica: string, 
/**
 * The table of the row, or of the delete.
 */
table: string, 
/**
 * Why it could not be filed.
 */
reason: string, 
/**
 * When it first could not be filed.
 */
first_at: string, 
/**
 * When filing it last failed.
 */
last_at: string, 
/**
 * How many times filing it failed.
 */
attempts: number, };

/**
 * A replica that sends keep failing to.
 */
export type Failing = { 
/**
 * The replica.
 */
replica: string, 
/**
 * Sends to it that failed in a row.
 */
failures: number, 
/**
 * The last failed send's error.
 */
last_error: string | null, 
/**
 * When it is tried again.
 */
retry_at: string | null, };

/**
 * A replica that has said nothing for longer than it said it would.
 */
export type Silent = { 
/**
 * The replica.
 */
replica: string, 
/**
 * When the server last heard from it.
 */
last_heard_at: string, };

/**
 * When the server last filed a row of one table from a replica: a table
 * whose rows flow was filed a moment ago.
 */
export type TableFiled = { 
/**
 * The table.
 */
table: string, 
/**
 * When a row of it was last filed.
 */
last_filed_at: string, };

/**
 * What the engine is waiting on, over a scope: each entry is either the
 * infrastructure (wait) or a bug (deploy a fix) — never data to repair by
 * hand. Lists are cut at the limit the read was given; `total` is uncut.
 */
export type Health = { 
/**
 * Nothing waits for a fix. A replica out of reach is not unhealthy — it
 * is the network's to settle, and listed in `failing`; nor is a
 * backlog, unless it stops draining, which `oldest` shows.
 */
healthy: boolean, 
/**
 * Changes noted and not yet planned.
 */
pending: number, 
/**
 * When the oldest change not yet planned was noted.
 */
oldest: string | null, 
/**
 * Changes the planner set aside until a fix.
 */
parked: Listed<Parked>, 
/**
 * Rows set aside for a replica until a fix.
 */
quarantine: Listed<Quarantined>, 
/**
 * What replicas sent that waits in the inbox until a fix.
 */
inbox: Listed<Unfiled>, 
/**
 * Replicas that sends keep failing to.
 */
failing: Listed<Failing>, 
/**
 * Replicas whose state is `silent` — they have said nothing for longer
 * than they said they would: off, or cut off — the longest quiet first.
 * Not unhealthy, as one out of reach is not.
 */
silent: Listed<Silent>, };

/**
 * `Engine::overview`: everything a dashboard shows first, in one read.
 * Its size does not grow with the fleet: the replicas are counted per
 * state, and listed worst first up to the limit; `Engine::listing` pages
 * through the rest.
 */
export type Overview = { 
/**
 * The contract this answer serializes as: `CONTRACT`.
 */
contract: number, 
/**
 * The server's time when the answer was read: every time in it is the
 * server's, and how long ago one was is measured from this.
 */
now: string, 
/**
 * What the engine waits on.
 */
health: Health, 
/**
 * How many replicas are in each state.
 */
states: ReplicaCounts, 
/**
 * The replicas that need attention most, worst first.
 */
replicas: Listed<ReplicaOverview>, 
/**
 * Each table the server sends down, parents first.
 */
tables: Array<TableOverview>, };

/**
 * `Engine::listing`: one page of the replicas, worst first.
 */
export type Listing = { 
/**
 * The contract this answer serializes as: `CONTRACT`.
 */
contract: number, 
/**
 * The server's time when the answer was read.
 */
now: string, 
/**
 * The state the page was asked for (`None`: every state).
 */
state: ReplicaState | null, 
/**
 * Where in the list the page begins.
 */
offset: number, 
/**
 * The page.
 */
replicas: Listed<ReplicaOverview>, };

/**
 * Where one replica stands, in one word.
 */
export type StateEntry = { 
/**
 * The replica.
 */
replica: string, 
/**
 * The audience it is of.
 */
audience: string, 
/**
 * Where it stands.
 */
state: ReplicaState, };

/**
 * `Engine::states`: where every replica of a scope stands, in one word
 * each, worst first — what a map of the fleet colours it by, with no
 * figure beside.
 */
export type States = { 
/**
 * The contract this answer serializes as: `CONTRACT`.
 */
contract: number, 
/**
 * The server's time when the answer was read.
 */
now: string, 
/**
 * Every replica of the scope.
 */
replicas: Array<StateEntry>, };

/**
 * One table on one replica, counted from what the replica acknowledged
 * applying. `holds - outdated` of `should` is how far along it is; the
 * numbers never count a row as held that the replica has not confirmed.
 * Every row the engine sent it is in exactly one of `holds`, `in_flight`
 * and `to_remove`.
 */
export type TableProgress = { 
/**
 * The table.
 */
table: string, 
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
export type ReplicaProgress = { 
/**
 * The replica.
 */
replica: string, 
/**
 * The audience it is of.
 */
audience: string, 
/**
 * Each table the server sends down, parents first.
 */
tables: Array<TableProgress>, };

/**
 * `Engine::progress`: how far each replica of a scope is from holding
 * what it should.
 */
export type Progress = { 
/**
 * The contract this answer serializes as: `CONTRACT`.
 */
contract: number, 
/**
 * Each replica of the scope.
 */
replicas: Array<ReplicaProgress>, };

/**
 * Where one row stands on one replica.
 */
export type RowPlace = { 
/**
 * The replica.
 */
replica: string, 
/**
 * The audience it is of.
 */
audience: string, 
/**
 * What the row is to it.
 */
state: RowState, };

/**
 * `Engine::row_progress`: where one row stands on each replica of a scope
 * that holds it, should, or is owed it.
 */
export type RowProgress = { 
/**
 * The contract this answer serializes as: `CONTRACT`.
 */
contract: number, 
/**
 * Each replica of the scope the row is something to.
 */
places: Array<RowPlace>, };

/**
 * Where a wish stands: what its replica answered to it, or that it has
 * not yet.
 */
export type WishState = "pending" | "waiting" | "applied" | "refused" | "superseded";

/**
 * How many of a replica's wishes stand where: those it carried out are
 * the rest.
 */
export type WishCounts = { 
/**
 * Not answered yet.
 */
pending: number, 
/**
 * Waiting until the replica can carry them out.
 */
waiting: number, 
/**
 * Refused by the replica.
 */
refused: number, 
/**
 * Superseded: the setting changed on the replica first.
 */
superseded: number, };

/**
 * One wish, as `Engine::wishes` lists it.
 */
export type WishEntry = { 
/**
 * The replica it is for.
 */
replica: string, 
/**
 * The setting's table.
 */
table: string, 
/**
 * The setting row's key.
 */
key: string, 
/**
 * Where it stands.
 */
state: WishState, 
/**
 * Why, for an answer that says: refused, superseded or waiting.
 */
reason: string | null, 
/**
 * What the host said of it when it wished it.
 */
note: string | null, 
/**
 * When it was wished.
 */
wished_at: string, 
/**
 * When the replica answered it, by its clock.
 */
answered_at: string | null, };

/**
 * `Engine::wishes`: one page of the wishes of a scope's replicas, those
 * that need attention first.
 */
export type WishListing = { 
/**
 * The contract this answer serializes as: `CONTRACT`.
 */
contract: number, 
/**
 * The server's time when the answer was read.
 */
now: string, 
/**
 * The state the page was asked for (`None`: every state).
 */
state: WishState | null, 
/**
 * Where in the list the page begins.
 */
offset: number, 
/**
 * The page.
 */
wishes: Listed<WishEntry>, };
