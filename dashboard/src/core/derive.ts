// What the dashboard shows, worked out from what the engine answers: pure
// functions of the answers, the host's names, and the time.

import type {
  Overview,
  ReplicaId,
  ReplicaOverview,
  ReplicaProgress,
  ReplicaState,
  TableOverview,
  Timestamp,
} from './types'

/** The page in one word: the banner's colour. */
export type Mood = 'healthy' | 'catching_up' | 'attention'

export interface SummaryOptions {
  /** The host's name for a replica; its id's first characters otherwise. */
  name?: (replica: ReplicaId) => string | undefined
  /** The host's name for a table; the table's own otherwise. */
  label?: (table: string) => string | undefined
  /** Now, in milliseconds. */
  now: number
  /** A replica that has not confirmed what it was sent for this long is not
   * answering (it is off, or out of reach). Default 15 minutes. */
  silentAfter?: number
  /** A change waiting this long to be sent means the engine is stuck.
   * Default 10 minutes. */
  stuckAfter?: number
}

export interface ReplicaRow {
  id: ReplicaId
  name: string
  state: ReplicaState
  /** How long it has not answered, when that is longer than `silentAfter`. */
  silentFor: number | null
  /** When it last said it applied a message. */
  heardAt: number | null
  pending: number
  unconfirmed: boolean
  quarantined: number
  parked: number
  unfiled: number
  failures: number
  lastError: string | null
  retryAt: number | null
  /** What it last said it wrote and the server has not filed, summed. */
  backlog: { unsent: number; unfiled: number; at: number } | null
  source: ReplicaOverview
}

export interface TableRow {
  table: string
  label: string
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
  replica: ReplicaId | null
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
  total: number
  counts: Record<ReplicaState, number>
  /** Replicas not answering, among the ones catching up. */
  silent: number
  /** Changes noted and not yet sent, and since when the oldest waits. */
  pending: number
  waitingSince: number | null
  /** The oldest has waited longer than `stuckAfter`. */
  stuck: boolean
  replicas: ReplicaRow[]
  tables: TableRow[]
  problems: Problem[]
  /** Problems the engine counted and did not list. */
  more: number
}

const MINUTE = 60_000

const severity: Record<ReplicaState, number> = {
  awaiting_fix: 0,
  unreachable: 1,
  catching_up: 2,
  synced: 3,
}

const at = (t: Timestamp | null): number | null => (t == null ? null : Date.parse(t))

/** A replica's name: the host's, or the first characters of its id. */
export function nameOf(options: Pick<SummaryOptions, 'name'>, replica: ReplicaId): string {
  return options.name?.(replica) ?? replica.slice(0, 8)
}

function labelOf(options: Pick<SummaryOptions, 'label'>, table: string): string {
  return options.label?.(table) ?? table
}

function replicaRow(r: ReplicaOverview, options: SummaryOptions): ReplicaRow {
  const heardAt = at(r.applied_at)
  const since = heardAt ?? at(r.sent_at)
  const quiet = r.unconfirmed && since != null ? options.now - since : null
  const silentFor = quiet != null && quiet > (options.silentAfter ?? 15 * MINUTE) ? quiet : null
  const backlog = r.backlog && {
    unsent: r.backlog.tables.reduce((n, t) => n + t.unsent, 0),
    unfiled: r.backlog.tables.reduce((n, t) => n + t.unfiled, 0),
    at: Date.parse(r.backlog.at),
  }
  return {
    id: r.replica,
    name: nameOf(options, r.replica),
    state: r.state,
    silentFor,
    heardAt,
    pending: r.pending,
    unconfirmed: r.unconfirmed,
    quarantined: r.quarantined,
    parked: r.parked,
    unfiled: r.unfiled,
    failures: r.failures,
    lastError: r.last_error,
    retryAt: at(r.retry_at),
    backlog: backlog && (backlog.unsent > 0 || backlog.unfiled > 0) ? backlog : null,
    source: r,
  }
}

/** Worse first; among the ones catching up, the silent first; then by name. */
function byConcern(a: ReplicaRow, b: ReplicaRow): number {
  return (
    severity[a.state] - severity[b.state] ||
    Number(b.silentFor != null) - Number(a.silentFor != null) ||
    a.name.localeCompare(b.name, undefined, { numeric: true })
  )
}

function problems(overview: Overview, options: SummaryOptions): Problem[] {
  const { health } = overview
  const name = (r: ReplicaId | null) => (r == null ? null : nameOf(options, r))
  const label = (t: string | null) => (t == null ? null : labelOf(options, t))
  return [
    ...health.quarantine.first.map((q): Problem => ({
      kind: 'quarantined',
      replica: q.replica,
      replicaName: name(q.replica),
      table: q.table,
      tableLabel: label(q.table),
      key: q.key,
      reason: q.reason,
      since: Date.parse(q.first_at),
      attempts: q.attempts,
      retryAt: null,
    })),
    ...health.inbox.first.map((u): Problem => ({
      kind: 'unfiled',
      replica: u.replica,
      replicaName: name(u.replica),
      table: u.table,
      tableLabel: label(u.table),
      key: null,
      reason: u.reason,
      since: Date.parse(u.first_at),
      attempts: u.attempts,
      retryAt: null,
    })),
    ...health.parked.first.map((p): Problem => ({
      kind: 'parked',
      replica: p.replica,
      replicaName: name(p.replica),
      table: p.table,
      tableLabel: label(p.table),
      key: p.key,
      reason: p.last_error,
      since: at(p.parked_at),
      attempts: p.attempts,
      retryAt: null,
    })),
    ...health.failing.first.map((f): Problem => ({
      kind: 'unreachable',
      replica: f.replica,
      replicaName: name(f.replica),
      table: null,
      tableLabel: null,
      key: null,
      reason: f.last_error,
      since: null,
      attempts: f.failures,
      retryAt: at(f.retry_at),
    })),
  ]
}

/** Everything the dashboard's first screen shows. */
export function summarize(overview: Overview, options: SummaryOptions): Summary {
  const { health } = overview
  const replicas = overview.replicas.map((r) => replicaRow(r, options)).sort(byConcern)
  const counts: Record<ReplicaState, number> = { awaiting_fix: 0, unreachable: 0, catching_up: 0, synced: 0 }
  for (const r of replicas) counts[r.state] += 1
  const waitingSince = at(health.oldest)
  const stuck = waitingSince != null && options.now - waitingSince > (options.stuckAfter ?? 10 * MINUTE)
  const tables = overview.tables
    .map((t): TableRow => ({
      table: t.table,
      label: labelOf(options, t.table),
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
  const total = health.parked.total + health.quarantine.total + health.inbox.total + health.failing.total
  const settled = counts.synced === replicas.length && health.pending === 0
  return {
    mood: !health.healthy || stuck ? 'attention' : settled ? 'healthy' : 'catching_up',
    total: replicas.length,
    counts,
    silent: replicas.filter((r) => r.silentFor != null).length,
    pending: health.pending,
    waitingSince,
    stuck,
    replicas,
    tables,
    problems: listed,
    more: total - listed.length,
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
