import { describe, expect, it } from 'vitest'
import type { ReplicaOverview, TableProgress } from './contract/types.js'
import overview from './contract/fixtures/overview.js'
import { progressOf, summarize } from './derive.js'
import { describeFlow, describeHeard, describeSettled, downChips, upChips, type Speech } from './describe.js'
import { downOf, fleetOf, totalsOf, upOf, type GroupOf } from './flow.js'
import { translator } from './i18n.js'
import { parseTimestamp } from './time.js'

const AT = parseTimestamp(overview.now)!
const SECOND = 1_000
const MINUTE = 60_000
const at = (ms: number) => new Date(AT - ms).toISOString()
const speech = (locale = 'en'): Speech => ({ t: translator(locale), locale, now: AT })

const synced = overview.replicas.first.find((r) => r.state === 'synced')!
const rowOf = (r: Partial<ReplicaOverview>) =>
  summarize({ ...overview, replicas: { total: 1, first: [{ ...synced, ...r }] } }, { now: AT }).worst[0]!

const table = (name: string, t: Partial<TableProgress> = {}): TableProgress => ({
  table: name,
  should: 100,
  holds: 100,
  outdated: 0,
  in_flight: 0,
  to_remove: 0,
  quarantined: 0,
  ...t,
})
const shares = (...tables: TableProgress[]) =>
  progressOf({ replica: synced.replica, audience: 'test', tables }).tables

// The host's groups: customers and addresses are people, the rest catalogue.
const group: GroupOf = (t) =>
  ['customers', 'addresses'].includes(t)
    ? { key: 'people', label: 'People', order: 1 }
    : { key: 'catalogue', label: 'Catalogue', order: 2 }

describe('downOf', () => {
  const down = shares(
    table('products'),
    table('prices', { should: 9407, holds: 8211, in_flight: 10, quarantined: 2 }),
    table('customers', { should: 9410, holds: 9310, in_flight: 100 }),
    table('addresses'),
  )

  it('splits the tables that wait from the settled, each a group of its own without the host’s', () => {
    const d = downOf(down)
    expect(d.lagging.map((g) => [g.label, g.tone, g.waiting])).toEqual([
      ['prices', 'attention', 1194],
      ['customers', 'moving', 100],
    ])
    expect([d.settled.map((g) => g.label), d.settledTables]).toEqual([['products', 'addresses'], 2])
    expect(d.lagging[0]!.lagging[0]).toMatchObject({ toSend: 1184, inFlight: 10, quarantined: 2 })
  })

  it('rolls the tables up in the host’s groups, a lagging group keeping its settled tables apart', () => {
    const d = downOf(down, group)
    expect(d.lagging.map((g) => [g.label, g.tone, g.lagging.length, g.settled.length])).toEqual([
      ['Catalogue', 'attention', 1, 1],
      ['People', 'moving', 1, 1],
    ])
    expect([d.settled.length, d.settledTables]).toEqual([0, 2])
  })

  it('says a table’s counts as chips', () => {
    const prices = downOf(down).lagging[0]!.lagging[0]!
    expect(downChips(prices, speech())).toEqual([
      { kind: 'to_send', text: '1,184 to send' },
      { kind: 'to_confirm', text: '10 to confirm' },
      { kind: 'set_aside', text: '2 set aside' },
    ])
    expect(downChips(prices, speech('es')).map((c) => c.text)).toEqual([
      '1184 por enviar',
      '10 por confirmar',
      '2 apartados',
    ])
  })
})

describe('upOf', () => {
  const writing = rowOf({
    state: 'catching_up',
    last_heard_at: at(9 * SECOND),
    backlog: {
      at: at(SECOND),
      tables: [
        { table: 'receipts', unsent: 0, unfiled: 1, set_aside: 0, oldest_at: at(4 * SECOND) },
        { table: 'cuts', unsent: 3, unfiled: 0, set_aside: 0, oldest_at: at(5 * MINUTE) },
      ],
    },
    filed: [
      { table: 'receipts', last_filed_at: at(4 * SECOND) },
      { table: 'path_points', last_filed_at: at(2 * SECOND) },
    ],
  })

  it('is live while what waits is young, and stalled once the oldest has waited too long', () => {
    const up = upOf(writing, { now: AT })
    const all = [...up.lagging, ...up.settled].flatMap((g) => [...g.lagging, ...g.settled])
    expect(all.map((x) => [x.table, x.flow, x.tone])).toEqual([
      ['cuts', 'stalled', 'moving'],
      ['receipts', 'live', 'ok'],
      ['path_points', 'live', 'ok'],
    ])
    expect(up.lagging.map((g) => g.label)).toEqual(['cuts'])
    expect(upOf(writing, { now: AT, behindAfter: 10 * MINUTE }).lagging).toEqual([])
  })

  it('stalls every table of a replica that has said nothing', () => {
    const quiet = rowOf({ state: 'silent', filed: [{ table: 'receipts', last_filed_at: at(41 * MINUTE) }] })
    const up = upOf(quiet, { now: AT })
    expect(up.lagging[0]!.lagging[0]).toMatchObject({ flow: 'stalled', tone: 'silent' })
    expect(describeFlow(up.lagging[0]!.lagging[0]!, speech('es'))).toBe('Último hace 41 min')
  })

  it('takes a replica that does not say a row’s age to have waited since it said so', () => {
    const old = rowOf({
      backlog: {
        at: at(3 * MINUTE),
        tables: [{ table: 'receipts', unsent: 2, unfiled: 0, set_aside: 0, oldest_at: null }],
      },
    })
    expect(upOf(old, { now: AT }).lagging[0]!.lagging[0]).toMatchObject({ flow: 'stalled', oldestAt: null })
  })

  it('says how a table flows, its chips, and when the replica was last heard', () => {
    const up = upOf(writing, { now: AT })
    const receipts = up.settled[0]!.settled[0]!
    expect(describeFlow(receipts, speech())).toBe('Live · last 4 sec. ago')
    expect(upChips(receipts, speech())).toEqual([
      { kind: 'to_confirm', text: '1 to confirm' },
      { kind: 'oldest', text: 'oldest 4 sec' },
    ])
    expect(describeHeard(writing, up, speech())).toEqual(['Last heard 9 sec. ago', 'uploading stalled'])
    expect(
      describeHeard(writing, upOf(writing, { now: AT, behindAfter: 10 * MINUTE }), speech('es')),
    ).toEqual(['Última señal hace 9 s', 'subiendo en vivo'])
  })
})

describe('totalsOf', () => {
  it('sums what is to send, to confirm and set aside, both ways', () => {
    const r = rowOf({
      owed: 7,
      unfiled: 1,
      backlog: {
        at: at(SECOND),
        tables: [{ table: 'receipts', unsent: 2, unfiled: 3, set_aside: 1, oldest_at: null }],
      },
    })
    expect(totalsOf(r)).toEqual({ toSend: 9, toConfirm: 3, setAside: 2 })
    const down = downOf(shares(table('products', { should: 20, holds: 10, in_flight: 4 })))
    expect(totalsOf(r, down)).toEqual({ toSend: 8, toConfirm: 7, setAside: 2 })
  })
})

describe('fleetOf', () => {
  it('folds the settled tables over the fleet, and keeps one row per audience', () => {
    const tables = summarize(overview, { now: AT }).tables
    const fleet = fleetOf([...tables, ...tables.map((x) => ({ ...x, audience: 'other' }))])
    expect(fleet.lagging.length + fleet.settled.length).toBe(tables.length * 2)
    expect(fleet.lagging.every((g) => g.tone !== 'ok')).toBe(true)
  })

  it('says the settled in one line', () => {
    expect(describeSettled(1, speech())).toBe('1 more table up to date')
    expect(describeSettled(4, speech('es'), true)).toBe('4 grupos más al día')
  })
})
