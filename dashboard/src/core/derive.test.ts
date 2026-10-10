import { describe, expect, it } from 'vitest'
import { progressOf, summarize } from './derive'
import type { Overview, ReplicaProgress } from './types'
import overviewJson from './contract/overview.json'
import progressJson from './contract/progress.json'

// What the engine answers, as its own test wrote it: every state and every
// kind of problem, all times at the same moment.
const overview = overviewJson as Overview
const progress = progressJson as ReplicaProgress[]
const AT = Date.parse('2026-01-01T00:00:00Z')
const MINUTE = 60_000

const id = (n: number) => `00000000-0000-7000-8000-${String(n).padStart(12, '0')}`
const names: Record<string, string> = {
  [id(1)]: 'Away',
  [id(2)]: 'Refusing',
  [id(4)]: 'Synced',
  [id(5)]: 'Silent',
}
const options = { name: (r: string) => names[r], now: AT }

describe('summarize', () => {
  it('puts the replicas that need someone first, then by name', () => {
    const summary = summarize(overview, options)
    expect(summary.replicas.map((r) => [r.name, r.state])).toEqual([
      ['Refusing', 'awaiting_fix'],
      ['Away', 'unreachable'],
      ['Silent', 'catching_up'],
      ['Synced', 'synced'],
    ])
    expect(summary.counts).toEqual({ awaiting_fix: 1, unreachable: 1, catching_up: 1, synced: 1 })
  })

  it('calls the page for attention while anything waits for a fix', () => {
    expect(summarize(overview, options).mood).toBe('attention')
  })

  it('is healthy only when every replica is synced and nothing is pending', () => {
    const settled: Overview = {
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
      replicas: overview.replicas.map((r) => ({ ...r, state: 'synced' })),
      tables: overview.tables.map((t) => ({ ...t, synced: true })),
    }
    expect(summarize(settled, options).mood).toBe('healthy')
    const busy = { ...settled, health: { ...settled.health, pending: 3, oldest: '2026-01-01T00:00:00Z' } }
    expect(summarize(busy, options).mood).toBe('catching_up')
    // The same backlog, waiting longer than it should: stuck.
    const stuck = summarize(busy, { ...options, now: AT + 11 * MINUTE })
    expect([stuck.mood, stuck.stuck]).toEqual(['attention', true])
  })

  it('says a replica is not answering once it has not confirmed for long', () => {
    const silent = (now: number) =>
      summarize(overview, { ...options, now }).replicas.find((r) => r.name === 'Silent')!
    expect(silent(AT + 5 * MINUTE).silentFor).toBeNull()
    expect(silent(AT + 20 * MINUTE).silentFor).toBe(20 * MINUTE)
    expect(summarize(overview, { ...options, now: AT + 20 * MINUTE, silentAfter: 30 * MINUTE }).silent).toBe(
      0,
    )
  })

  it('sums what a replica said it wrote and the server has not filed', () => {
    const silent = summarize(overview, options).replicas.find((r) => r.name === 'Silent')!
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
      ['unreachable', 'Away', null],
    ])
    expect(summary.more).toBe(0)
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
      ['from_a_newer_build', false],
    ])
    expect(summary.tables[0]).toMatchObject({ inFlight: 2, quarantined: 1, replicas: 2 })
    const kinds = { ...overview.tables[0]!, table: 'kinds', synced: true }
    const reordered = summarize({ ...overview, tables: [kinds, ...overview.tables] }, options)
    expect(reordered.tables.map((t) => t.table)).toEqual(['items', 'from_a_newer_build', 'kinds'])
  })

  it('names a replica the host does not know by its id', () => {
    const summary = summarize(overview, { now: AT })
    expect(summary.replicas[0]!.name).toBe(id(2).slice(0, 8))
  })
})

describe('progressOf', () => {
  it('counts a replica as far along as it confirmed, never further', () => {
    const synced = progressOf(progress.find((p) => p.replica === id(4))!)
    expect([synced.upToDate, synced.should, synced.share]).toEqual([2, 2, 1])
    const silent = progressOf(progress.find((p) => p.replica === id(5))!)
    expect([silent.upToDate, silent.should, silent.share]).toEqual([0, 2, 0])
    expect(silent.tables[0]!.inFlight).toBe(2)
  })

  it('is complete when a replica should hold nothing', () => {
    expect(progressOf({ replica: id(9), tables: [] }).share).toBe(1)
  })
})
