// What the dashboard shows, worked out from what the engine answers: pure
// functions of the answers, the host's names, and the server's time.

import { CONTRACT } from './contract/types.js'
import type {
  Listing,
  Overview,
  ReplicaCounts,
  ReplicaOverview,
  ReplicaProgress,
  ReplicaState,
  RowState,
  TableOverview,
  WishCounts,
  WishState,
} from './contract/types.js'
import { timeOf } from './time.js'

/** The page in one word: the banner's colour. */
export type Mood = 'healthy' | 'catching_up' | 'attention' | 'empty'

export interface SummaryOptions {
  /** The host's name for a replica; the end of its id otherwise. */
  name?: (replica: string) => string | undefined
  /** The host's name for a table; the table's own otherwise. */
  label?: (table: string) => string | undefined
  /** The server's time now, in milliseconds (see `serverNow`). */
  now: number
  /** A replica that has not moved on what it was sent for this long is not
   * answering (it is off, or out of reach). Default 15 minutes. */
  silentAfter?: number
  /** A change waiting this long to be sent means the engine is stuck.
   * Default 15 minutes: the server's own alarm (`Settings::stuck_after`). */
  stuckAfter?: number
}

export interface ReplicaRow {
  id: string
  name: string
  /** The audience it is of: the kind of replica, as the server names it. */
  audience: string
  state: ReplicaState
  /** How long it has not answered, when that is longer than `silentAfter`. */
  silentFor: number | null
  /** When it last said it applied a message. */
  heardAt: number | null
  pending: number
  /** Messages written for it that the transport has not taken yet. */
  owed: number
  unconfirmed: boolean
  quarantined: number
  parked: number
  unfiled: number
  failures: number
  lastError: string | null
  retryAt: number | null
  /** What it last said it wrote and the server has not filed, summed. */
  backlog: { unsent: number; unfiled: number; at: number | null } | null
  /** Its settings' wishes, by where they stand. */
  wishes: WishCounts
  source: ReplicaOverview
}

/** The replicas a list shows: a page the host fetched, or the overview's
 * worst when it fetched none. */
export interface ReplicaPage {
  rows: ReplicaRow[]
  /** Where in the list the rows begin, and how many it holds in all. */
  offset: number
  total: number
  /** The overview's worst, not a page: the rows are what needs attention most. */
  worst: boolean
}

export interface TableRow {
  table: string
  label: string
  /** The audience it is the table of: a table two audiences hold is a row
   * for each. */
  audience: string
  synced: boolean
  pending: number
  parked: number
  inFlight: number
  quarantined: number
  unfiled: number
  replicas: number
  source: TableOverview
}

export type ProblemKind = 'parked' | 'quarantined' | 'unfiled' | 'unreachable'

/** One thing waiting for a fix (or, for `unreachable`, for the network). */
export interface Problem {
  kind: ProblemKind
  replica: string | null
  replicaName: string | null
  table: string | null
  tableLabel: string | null
  key: string | null
  reason: string | null
  since: number | null
  attempts: number
  retryAt: number | null
}

export interface Summary {
  mood: Mood
  /** Every replica, and how many are in each state. */
  total: number
  counts: ReplicaCounts
  /** Things set aside until a fix is deployed. */
  fixes: number
  /** Replicas not answering, among the overview's worst. */
  silent: number
  /** Changes noted and not yet sent, and since when the oldest waits. */
  pending: number
  waitingSince: number | null
  /** The oldest has waited longer than `stuckAfter`. */
  stuck: boolean
  /** The replicas that need attention most, worst first. */
  worst: ReplicaRow[]
  tables: TableRow[]
  /** The audiences the tables are of, in the engine's order: a page of one
   * need not say which. */
  audiences: string[]
  problems: Problem[]
  /** Problems the engine counted and did not list. */
  more: number
}

const MINUTE = 60_000

// Every replica, including those in a state an engine newer than this
// dashboard added: they are counted, and are not settled.
const everyone = (states: ReplicaCounts) => Object.values(states).reduce((n, count) => n + count, 0)

/** Where a row is on a replica, in one word: settled, on its way, set aside,
 * or `unknown` for a state this dashboard was not written for. */
export type PlaceMood = 'holds' | 'moving' | 'set_aside' | 'unknown'

const placeMoods: Record<RowState, PlaceMood> = {
  holds: 'holds',
  outdated: 'moving',
  on_its_way: 'moving',
  missing: 'moving',
  leaving: 'moving',
  quarantined: 'set_aside',
}

const wishMoods: Record<WishState, PlaceMood> = {
  applied: 'holds',
  pending: 'moving',
  waiting: 'moving',
  refused: 'set_aside',
  superseded: 'set_aside',
}

/** The mood of a wish's state, whatever the engine answered. */
export function wishMoodOf(state: string): PlaceMood {
  return Object.hasOwn(wishMoods, state) ? wishMoods[state as WishState] : 'unknown'
}

/** The mood of a row's state, whatever the engine answered. */
export function placeMoodOf(state: string): PlaceMood {
  return Object.hasOwn(placeMoods, state) ? placeMoods[state as RowState] : 'unknown'
}

const replicaStates: Record<ReplicaState, true> = {
  awaiting_fix: true,
  unreachable: true,
  catching_up: true,
  synced: true,
}

/** Whether this dashboard has a word for a replica's state. */
export function knowsReplicaState(state: string): state is ReplicaState {
  return Object.hasOwn(replicaStates, state)
}

/** Whether an answer is of the contract this dashboard reads. */
export function readsContract(answer: { contract: number }): boolean {
  return answer.contract === CONTRACT
}

/** A replica's name: the host's, or the end of its id — an id's start is
 * its creation time, the same for a fleet made in one go. */
export function nameOf(options: Pick<SummaryOptions, 'name'>, replica: string): string {
  return options.name?.(replica) ?? replica.slice(-12)
}

function labelOf(options: Pick<SummaryOptions, 'label'>, table: string): string {
  return options.label?.(table) ?? table
}

function replicaRow(r: ReplicaOverview, options: SummaryOptions): ReplicaRow {
  const waiting = timeOf(r.waiting_since)
  const quiet = waiting == null ? null : options.now - waiting
  const silentFor = quiet != null && quiet > (options.silentAfter ?? 15 * MINUTE) ? quiet : null
  const unsent = r.backlog?.tables.reduce((n, t) => n + t.unsent, 0) ?? 0
  const unfiled = r.backlog?.tables.reduce((n, t) => n + t.unfiled, 0) ?? 0
  return {
    id: r.replica,
    name: nameOf(options, r.replica),
    // An engine that predates audiences serves one, and names none.
    audience: r.audience ?? '',
    state: r.state,
    silentFor,
    heardAt: timeOf(r.applied_at),
    pending: r.pending,
    owed: r.owed,
    unconfirmed: r.unconfirmed,
    quarantined: r.quarantined,
    parked: r.parked,
    unfiled: r.unfiled,
    failures: r.failures,
    lastError: r.last_error,
    retryAt: timeOf(r.retry_at),
    backlog: unsent + unfiled > 0 ? { unsent, unfiled, at: timeOf(r.backlog?.at ?? null) } : null,
    // An engine that predates wishes says nothing of them.
    wishes: r.wishes ?? { pending: 0, waiting: 0, refused: 0, superseded: 0 },
    source: r,
  }
}

function problems(overview: Overview, options: SummaryOptions): Problem[] {
  const { health } = overview
  const name = (r: string | null) => (r == null ? null : nameOf(options, r))
  const label = (t: string | null) => (t == null ? null : labelOf(options, t))
  const problem = (p: Omit<Problem, 'replicaName' | 'tableLabel'>): Problem => ({
    ...p,
    replicaName: name(p.replica),
    tableLabel: label(p.table),
  })
  return [
    ...health.quarantine.first.map((q) =>
      problem({
        kind: 'quarantined',
        replica: q.replica,
        table: q.table,
        key: q.key,
        reason: q.reason,
        since: timeOf(q.first_at),
        attempts: q.attempts,
        retryAt: null,
      }),
    ),
    ...health.inbox.first.map((u) =>
      problem({
        kind: 'unfiled',
        replica: u.replica,
        table: u.table,
        key: null,
        reason: u.reason,
        since: timeOf(u.first_at),
        attempts: u.attempts,
        retryAt: null,
      }),
    ),
    ...health.parked.first.map((p) =>
      problem({
        kind: 'parked',
        replica: p.replica,
        table: p.table,
        key: p.key,
        reason: p.last_error,
        since: timeOf(p.parked_at),
        attempts: p.attempts,
        retryAt: null,
      }),
    ),
    ...health.failing.first.map((f) =>
      problem({
        kind: 'unreachable',
        replica: f.replica,
        table: null,
        key: null,
        reason: f.last_error,
        since: null,
        attempts: f.failures,
        retryAt: timeOf(f.retry_at),
      }),
    ),
  ]
}

/** Everything the dashboard's first screen shows. */
export function summarize(overview: Overview, options: SummaryOptions): Summary {
  const { health, states } = overview
  const total = everyone(states)
  const worst = overview.replicas.first.map((r) => replicaRow(r, options))
  const waitingSince = timeOf(health.oldest)
  const stuck = waitingSince != null && options.now - waitingSince > (options.stuckAfter ?? 15 * MINUTE)
  const tables = overview.tables
    .map((t): TableRow => ({
      table: t.table,
      label: labelOf(options, t.table),
      audience: t.audience ?? '',
      synced: t.synced,
      pending: t.pending,
      parked: t.parked,
      inFlight: t.in_flight,
      quarantined: t.quarantined,
      unfiled: t.unfiled,
      replicas: t.replicas,
      source: t,
    }))
    // Not synced first; otherwise in the engine's order, parents first.
    .sort((a, b) => Number(a.synced) - Number(b.synced))
  const listed = problems(overview, options)
  const fixes = health.parked.total + health.quarantine.total + health.inbox.total
  const settled = states.synced === total && health.pending === 0
  return {
    mood:
      !health.healthy || stuck ? 'attention' : total === 0 ? 'empty' : settled ? 'healthy' : 'catching_up',
    total,
    counts: states,
    fixes,
    silent: worst.filter((r) => r.silentFor != null).length,
    pending: health.pending,
    waitingSince,
    stuck,
    worst,
    tables,
    audiences: [...new Set(overview.tables.map((t) => t.audience ?? ''))],
    problems: listed,
    more: fixes + health.failing.total - listed.length,
  }
}

/** The replicas to list for `filter` (every state, when `null`): the host's
 * page when it fetched one for that filter, else the overview's worst in it. */
export function pageOf(
  overview: Overview,
  listing: Listing | null | undefined,
  filter: ReplicaState | null,
  options: SummaryOptions,
): ReplicaPage {
  if (listing && readsContract(listing) && listing.state === filter)
    return {
      rows: listing.replicas.first.map((r) => replicaRow(r, options)),
      offset: listing.offset,
      total: listing.replicas.total,
      worst: false,
    }
  const { states } = overview
  return {
    rows: overview.replicas.first
      .filter((r) => filter == null || r.state === filter)
      .map((r) => replicaRow(r, options)),
    offset: 0,
    total: filter == null ? everyone(states) : states[filter],
    worst: true,
  }
}

export interface TableShare {
  table: string
  label: string
  should: number
  upToDate: number
  outdated: number
  inFlight: number
  toRemove: number
  quarantined: number
  /** `upToDate` of `should`, from 0 to 1; 1 when it should hold nothing. */
  share: number
}

export interface ProgressSummary {
  tables: TableShare[]
  should: number
  upToDate: number
  share: number
}

/** How far along one replica is, per table and over all of them. */
export function progressOf(
  progress: ReplicaProgress,
  options: Pick<SummaryOptions, 'label'> = {},
): ProgressSummary {
  const share = (done: number, of: number) => (of === 0 ? 1 : Math.min(1, done / of))
  const tables = progress.tables.map((t): TableShare => {
    const upToDate = t.holds - t.outdated
    return {
      table: t.table,
      label: labelOf(options, t.table),
      should: t.should,
      upToDate,
      outdated: t.outdated,
      inFlight: t.in_flight,
      toRemove: t.to_remove,
      quarantined: t.quarantined,
      share: share(upToDate, t.should),
    }
  })
  const should = tables.reduce((n, t) => n + t.should, 0)
  const upToDate = tables.reduce((n, t) => n + t.upToDate, 0)
  return { tables, should, upToDate, share: share(upToDate, should) }
}
