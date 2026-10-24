// What one replica's tables wait on, both ways: down, what the server sends
// it (how far along it is, from `Engine::progress`); up, what it sends the
// server (what it said waits, and when the server last filed a row of each
// table, from the overview). A table that waits on nothing is settled, and
// settled ones are folded into one line: the page shows only what waits.
//
// A host may gather the tables in groups of its own (`group`); without
// one, each table is a group of one.

import type { ReplicaRow, TableRow, TableShare } from './derive.js'
import { rankOf, type Tone } from './standing.js'
import { timeOf } from './time.js'

/** A group a host gathers tables in: `order` sorts the groups. */
export interface TableGroupKey {
  key: string
  label: string
  order?: number
}

/** The group a table is in; `null` for a group of its own. */
export type GroupOf = (table: string) => TableGroupKey | null | undefined

/** One table the server sends a replica. */
export interface DownTable extends TableShare {
  /** Rows it should hold that are neither held, on their way, nor set
   * aside: what is still to be sent. */
  toSend: number
  tone: Tone
}

/** One table a replica sends the server. */
export interface UpTable {
  table: string
  label: string
  unsent: number
  unfiled: number
  setAside: number
  /** When the oldest row waiting was written; `null` when none waits, or
   * the replica does not say. */
  oldestAt: number | null
  /** When the server last filed a row of it from this replica. */
  lastFiledAt: number | null
  /** Rows of it reach the server as they are written (`live`), or the
   * oldest waiting has waited too long, or the replica says nothing
   * (`stalled`). */
  flow: 'live' | 'stalled'
  tone: Tone
}

/** Tables in a group, rolled up. */
export interface TableGroup<T> {
  key: string
  label: string
  order: number
  /** The tables that wait on something, and the settled ones. */
  lagging: T[]
  settled: T[]
  /** Rows on their way over its tables, summed. */
  waiting: number
  tone: Tone
}

/** One way of a replica's tables: the groups that wait on something, worst
 * first, and how many groups and tables are settled. */
export interface Direction<T> {
  lagging: TableGroup<T>[]
  settled: TableGroup<T>[]
  /** Tables settled in every group, the lagging ones included. */
  settledTables: number
}

function grouped<T extends { table: string; label: string; tone: Tone }>(
  tables: T[],
  waiting: (t: T) => number,
  group?: GroupOf,
  own: (t: T) => string = (t) => t.table,
): Direction<T> {
  const groups = new Map<string, TableGroup<T>>()
  tables.forEach((t, i) => {
    const g = group?.(t.table) ?? { key: `table:${own(t)}`, label: t.label, order: i }
    let into = groups.get(g.key)
    if (!into) {
      into = {
        key: g.key,
        label: g.label,
        order: g.order ?? i,
        lagging: [],
        settled: [],
        waiting: 0,
        tone: 'ok',
      }
      groups.set(g.key, into)
    }
    ;(t.tone === 'ok' ? into.settled : into.lagging).push(t)
    into.waiting += waiting(t)
    if (rankOf(t.tone) < rankOf(into.tone)) into.tone = t.tone
  })
  const all = [...groups.values()]
  const byOrder = (a: TableGroup<T>, b: TableGroup<T>) => a.order - b.order
  return {
    lagging: all
      .filter((g) => g.tone !== 'ok')
      .sort((a, b) => rankOf(a.tone) - rankOf(b.tone) || byOrder(a, b)),
    settled: all.filter((g) => g.tone === 'ok').sort(byOrder),
    settledTables: all.reduce((n, g) => n + g.settled.length, 0),
  }
}

/** The tables the server sends a replica, from `progressOf`'s shares. */
export function downOf(shares: TableShare[], group?: GroupOf): Direction<DownTable> {
  const tables = shares.map((s): DownTable => {
    const toSend = Math.max(0, s.should - s.upToDate - s.inFlight - s.quarantined)
    const tone: Tone =
      s.quarantined > 0 ? 'attention' : toSend + s.inFlight + s.toRemove + s.outdated > 0 ? 'moving' : 'ok'
    return { ...s, toSend, tone }
  })
  return grouped(tables, (t) => t.toSend + t.inFlight + t.toRemove, group)
}

export interface UpOptions {
  /** The host's name for a table; the table's own otherwise. */
  label?: (table: string) => string | undefined
  /** The server's time now, in milliseconds. */
  now: number
  /** How long the oldest row may wait before its table is stalled, in ms:
   * the server's `Settings::behind_after`. Default 2 minutes. */
  behindAfter?: number
}

/** The tables a replica sends the server: each it said waits, and each the
 * server has filed rows of. */
export function upOf(r: ReplicaRow, options: UpOptions, group?: GroupOf): Direction<UpTable> {
  const behindAfter = options.behindAfter ?? 2 * 60_000
  // An engine that predates filing says nothing of it.
  const filed = new Map((r.source.filed ?? []).map((f) => [f.table, timeOf(f.last_filed_at)]))
  const said = new Map((r.source.backlog?.tables ?? []).map((b) => [b.table, b]))
  const quiet = r.state === 'silent' || r.state === 'unreachable'
  const tables = [...new Set([...said.keys(), ...filed.keys()])].map((table): UpTable => {
    const b = said.get(table)
    const unsent = b?.unsent ?? 0
    const unfiled = b?.unfiled ?? 0
    const setAside = b?.set_aside ?? 0
    const oldestAt = unsent + unfiled > 0 ? timeOf(b?.oldest_at ?? null) : null
    // A replica that does not say how old a row is: as old as what it said.
    const waitedSince = oldestAt ?? (unsent + unfiled > 0 ? (r.backlog?.at ?? null) : null)
    const stalled = quiet || (waitedSince != null && options.now - waitedSince > behindAfter)
    const tone: Tone = setAside > 0 ? 'attention' : stalled ? (quiet ? 'silent' : 'moving') : 'ok'
    return {
      table,
      label: options.label?.(table) ?? table,
      unsent,
      unfiled,
      setAside,
      oldestAt,
      lastFiledAt: filed.get(table) ?? null,
      flow: stalled ? 'stalled' : 'live',
      tone,
    }
  })
  return grouped(tables, (t) => t.unsent + t.unfiled, group)
}

/** What one replica has on its way, both ways, in three numbers. */
export interface Totals {
  /** Rows still to be sent: to it, and by it. */
  toSend: number
  /** Rows sent and not confirmed yet: to it, and by it. */
  toConfirm: number
  /** Rows and changes set aside until a fix. */
  setAside: number
}

/** The three numbers of one replica, from its row and, once fetched, its
 * tables down. */
export function totalsOf(r: ReplicaRow, down?: Direction<DownTable> | null): Totals {
  const tables = down ? [...down.lagging, ...down.settled].flatMap((g) => [...g.lagging, ...g.settled]) : []
  const sum = (f: (t: DownTable) => number) => tables.reduce((n, t) => n + f(t), 0)
  const backlog = r.source.backlog?.tables ?? []
  const up = (f: (t: (typeof backlog)[number]) => number) => backlog.reduce((n, t) => n + f(t), 0)
  return {
    toSend: (down ? sum((t) => t.toSend) : r.owed) + up((t) => t.unsent),
    toConfirm: sum((t) => t.inFlight) + up((t) => t.unfiled),
    setAside: r.quarantined + r.unfiled + r.parked + up((t) => t.set_aside),
  }
}

/** One table over the fleet, with its tone. */
export interface FleetTable extends TableRow {
  tone: Tone
}

/** The tables over every replica of the scope (the summary's), grouped: a
 * table two audiences hold is a table for each. */
export function fleetOf(tables: TableRow[], group?: GroupOf): Direction<FleetTable> {
  const toned = tables.map((t): FleetTable => ({
    ...t,
    tone: t.synced ? 'ok' : t.quarantined + t.unfiled + t.parked > 0 ? 'attention' : 'moving',
  }))
  return grouped(
    toned,
    (t) => t.pending + t.inFlight,
    group,
    (t) => `${t.audience}/${t.table}`,
  )
}
