import { describe, expect, it } from 'vitest'
import { knowsReplicaState, pageOf, placeMoodOf, progressOf, summarize } from './derive.js'
import type { Overview } from './contract/types.js'
import overview from './contract/fixtures/overview.js'
import listing from './contract/fixtures/listing.js'
import progress from './contract/fixtures/progress.js'
import { parseTimestamp } from './time.js'

// What the engine answers, as its own test wrote it: every state and every
// kind of problem, all times at the same moment.
const AT = parseTimestamp(overview.now)!
const MINUTE = 60_000

const id = (n: number) => `00000000-0000-7000-8000-${String(n).padStart(12, '0')}`
const names: Record<string, string> = {
  [id(1)]: 'Away',
  [id(2)]: 'Refusing',
  [id(4)]: 'Quiet',
  [id(5)]: 'Writing',
  [id(6)]: 'Synced',
}
const options = { name: (r: string) => names[r], now: AT }

const settled: Overview = {
  ...overview,
  health: {
    ...overview.health,
    healthy: true,
    pending: 0,
    oldest: null,
    parked: { total: 0, first: [] },
    quarantine: { total: 0, first: [] },
    inbox: { total: 0, first: [] },
    failing: { total: 0, first: [] },
  },
  states: { awaiting_fix: 0, unreachable: 0, silent: 0, catching_up: 0, synced: 4 },
  replicas: {
    total: 4,
    first: overview.replicas.first.map((r) => ({ ...r, state: 'synced' as const })),
  },
  tables: overview.tables.map((t) => ({ ...t, synced: true })),
}

describe('summarize', () => {
  it('keeps the engine’s order, worst first, and its counts of every replica', () => {
    const summary = summarize(overview, options)
    expect(summary.worst.map((r) => [r.name, r.state])).toEqual([
      ['Refusing', 'awaiting_fix'],
      ['Away', 'unreachable'],
      ['Quiet', 'silent'],
      ['Writing', 'catching_up'],
      ['Synced', 'synced'],
    ])
    expect(summary.counts).toEqual({ awaiting_fix: 1, unreachable: 1, silent: 1, catching_up: 1, synced: 1 })
    expect(summary.total).toBe(5)
  })

  it('calls the page for attention while anything waits for a fix', () => {
    expect(summarize(overview, options).mood).toBe('attention')
  })

  it('does not call the page for attention for a replica out of reach', () => {
    const away: Overview = {
      ...settled,
      health: { ...settled.health, failing: overview.health.failing, pending: 3 },
      states: { ...settled.states, synced: 3, unreachable: 1 },
    }
    const summary = summarize(away, options)
    expect([summary.mood, summary.fixes, summary.problems.map((p) => p.kind)]).toEqual([
      'catching_up',
      0,
      ['unreachable'],
    ])
  })

  it('is healthy only when every replica is synced and nothing is pending', () => {
    expect(summarize(settled, options).mood).toBe('healthy')
    const busy = { ...settled, health: { ...settled.health, pending: 3, oldest: overview.now } }
    expect(summarize(busy, options).mood).toBe('catching_up')
    // The same backlog, waiting longer than the server's own alarm allows:
    // stuck, at the moment the server says so in its log.
    const waiting = summarize(busy, { ...options, now: AT + 14 * MINUTE })
    expect([waiting.mood, waiting.stuck]).toEqual(['catching_up', false])
    const stuck = summarize(busy, { ...options, now: AT + 16 * MINUTE })
    expect([stuck.mood, stuck.stuck]).toEqual(['attention', true])
  })

  it('says there is no replica rather than that every one is in sync', () => {
    const empty: Overview = {
      ...settled,
      states: { awaiting_fix: 0, unreachable: 0, silent: 0, catching_up: 0, synced: 0 },
      replicas: { total: 0, first: [] },
    }
    expect(summarize(empty, options).mood).toBe('empty')
  })

  it('says a replica is not answering once it has not moved for long, by the server’s clock', () => {
    const silent = (now: number) =>
      summarize(overview, { ...options, now }).worst.find((r) => r.name === 'Writing')!
    expect(silent(AT + 5 * MINUTE).silentFor).toBeNull()
    expect(silent(AT + 20 * MINUTE).silentFor).toBe(20 * MINUTE)
    expect(summarize(overview, { ...options, now: AT + 20 * MINUTE, silentAfter: 30 * MINUTE }).silent).toBe(
      0,
    )
  })

  it('sums what a replica said it wrote and the server has not filed', () => {
    const silent = summarize(overview, options).worst.find((r) => r.name === 'Writing')!
    expect(silent.backlog).toEqual({ unsent: 2, unfiled: 1, at: AT })
  })

  it('lists every problem with the names of what it is about, and counts the rest', () => {
    const summary = summarize(overview, {
      ...options,
      label: (t) => (t === 'items' ? 'Products' : undefined),
    })
    expect(summary.problems.map((p) => [p.kind, p.replicaName, p.tableLabel])).toEqual([
      ['quarantined', 'Refusing', 'Products'],
      ['unfiled', 'Refusing', 'from_a_newer_build'],
      ['parked', null, 'Products'],
      ['parked', null, 'list_members'],
      ['parked', 'Refusing', 'replica_lists'],
      ['parked', 'Refusing', null],
      ['parked', null, '"fixture"."grants"'],
      ['unreachable', 'Away', null],
    ])
    expect([summary.fixes, summary.more]).toEqual([7, 0])
    const cut = {
      ...overview,
      health: { ...overview.health, quarantine: { ...overview.health.quarantine, total: 40 } },
    }
    expect(summarize(cut, options).more).toBe(39)
  })

  it('puts the tables that are not synced first, and names them the host’s way', () => {
    const summary = summarize(overview, {
      ...options,
      label: (t) => (t === 'items' ? 'Products' : undefined),
    })
    expect(summary.tables.map((t) => [t.label, t.synced])).toEqual([
      ['Products', false],
      ['"fixture"."grants"', false],
      ['from_a_newer_build', false],
      ['list_members', false],
      ['replica_lists', false],
    ])
    const kinds = { ...overview.tables[0]!, table: 'kinds', synced: true }
    const reordered = summarize({ ...overview, tables: [kinds, ...overview.tables] }, options)
    expect(reordered.tables.at(-1)!.table).toBe('kinds')
  })

  it('names a replica the host does not know by the end of its id, not its creation time', () => {
    const summary = summarize(overview, { now: AT })
    expect(summary.worst[0]!.name).toBe('000000000002')
  })
})

describe('pageOf', () => {
  it('lists the host’s page when it is for the filter shown', () => {
    const page = pageOf(overview, listing, 'catching_up', options)
    expect([page.rows.map((r) => r.name), page.offset, page.total, page.worst]).toEqual([
      ['Writing'],
      0,
      1,
      false,
    ])
  })

  it('lists the overview’s worst in the filter while the page is for another', () => {
    const page = pageOf(overview, listing, 'synced', options)
    expect([page.rows.map((r) => r.name), page.total, page.worst]).toEqual([['Synced'], 1, true])
    const all = pageOf(overview, null, null, options)
    expect([all.rows.length, all.total]).toEqual([5, 5])
  })
})

describe('progressOf', () => {
  it('counts a replica as far along as it confirmed, never further', () => {
    const synced = progressOf(progress.replicas.find((p) => p.replica === id(6))!)
    expect([synced.upToDate, synced.should, synced.share]).toEqual([3, 3, 1])
    const silent = progressOf(progress.replicas.find((p) => p.replica === id(5))!)
    expect([silent.upToDate, silent.should, silent.share]).toEqual([0, 3, 0])
    expect(silent.tables[0]!.inFlight).toBe(3)
  })

  it('is complete when a replica should hold nothing', () => {
    expect(progressOf({ replica: id(9), audience: 'registers', tables: [] }).share).toBe(1)
  })
})

describe('a value an engine newer than the dashboard added', () => {
  it('counts a replica in a state it does not know, and does not call everything in sync', () => {
    const newer = {
      ...settled,
      states: { ...settled.states, synced: 3, resting: 1 },
    } as Overview
    const summary = summarize(newer, options)
    expect([summary.total, summary.mood]).toEqual([4, 'catching_up'])
  })

  it('knows the states it was written for, and no other word', () => {
    expect(['awaiting_fix', 'unreachable', 'catching_up', 'synced'].every(knowsReplicaState)).toBe(true)
    expect(['resting', 'constructor', '__proto__', ''].some(knowsReplicaState)).toBe(false)
  })

  it('puts a row state it does not know in no mood of the ones it does', () => {
    expect(placeMoodOf('holds')).toBe('holds')
    expect(placeMoodOf('quarantined')).toBe('set_aside')
    expect(['teleporting', 'toString'].map(placeMoodOf)).toEqual(['unknown', 'unknown'])
  })
})

describe('an engine of several audiences', () => {
  it('lists a table once for each audience that holds it, and says which', () => {
    const table = settled.tables[0]!
    const two: Overview = {
      ...settled,
      tables: [
        { ...table, audience: 'devices' },
        { ...table, audience: 'erp' },
      ],
    }
    const summary = summarize(two, options)
    expect(summary.audiences).toEqual(['devices', 'erp'])
    expect(summary.tables.map((t) => [t.table, t.audience])).toEqual([
      [table.table, 'devices'],
      [table.table, 'erp'],
    ])
  })

  it('names one when the engine serves one', () => {
    expect(summarize(settled, options).audiences).toHaveLength(1)
  })
})
