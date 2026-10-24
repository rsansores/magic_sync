import { describe, expect, it } from 'vitest'
import type { States } from './contract/types.js'
import states from './contract/fixtures/states.js'
import { gridOf, into, levelUnder, tilesOf, type Level, type Place } from './grid.js'
import { translator } from './i18n.js'

const t = translator('en')
const id = (n: number) => `00000000-0000-7000-8000-${String(n).padStart(12, '0')}`

// A fleet of eight in two regions and three stores; register 8 is placed
// nowhere.
const fleet: States = {
  ...states,
  replicas: [
    [1, 'synced'],
    [2, 'catching_up'],
    [3, 'synced'],
    [4, 'awaiting_fix'],
    [5, 'synced'],
    [6, 'silent'],
    [7, 'synced'],
    [8, 'synced'],
  ].map(([n, state]) => ({ replica: id(n as number), audience: 'registers', state: state as 'synced' })),
}
const levels: Level[] = [
  { key: 'region', label: 'Region' },
  { key: 'store', label: 'Store' },
]
const where: Record<number, [Place, Place]> = {
  1: [
    { key: 'n', label: 'North' },
    { key: 'n1', label: 'Harbour' },
  ],
  2: [
    { key: 'n', label: 'North' },
    { key: 'n1', label: 'Harbour' },
  ],
  3: [
    { key: 'n', label: 'North' },
    { key: 'n2', label: 'Airport' },
  ],
  4: [
    { key: 'n', label: 'North' },
    { key: 'n2', label: 'Airport' },
  ],
  5: [
    { key: 's', label: 'South' },
    { key: 's1', label: 'Market' },
  ],
  6: [
    { key: 's', label: 'South' },
    { key: 's1', label: 'Market' },
  ],
  7: [
    { key: 's', label: 'South' },
    { key: 's1', label: 'Market' },
  ],
}
const placeOf = (replica: string, level: string) =>
  where[Number(replica.slice(-2))]?.[level === 'region' ? 0 : 1] ?? null
const tiles = tilesOf(fleet, t, { name: (r) => `Register ${Number(r.slice(-2))}` })
const names = (tiles: { name: string }[]) => tiles.map((x) => x.name)

describe('tilesOf', () => {
  it('makes a tile of every replica, in the engine’s words unless the host combines its own', () => {
    expect(tiles.map((x) => x.standing.tone)).toEqual([
      'ok',
      'moving',
      'ok',
      'attention',
      'ok',
      'silent',
      'ok',
      'ok',
    ])
    const offline = { state: 'offline', tone: 'off' as const, label: 'Offline' }
    const combined = tilesOf(fleet, t, { standing: (r) => (r === id(1) ? offline : null) })
    expect(combined[0]!.standing).toEqual(offline)
    expect(combined[0]!.name).toBe(id(1).slice(-12))
  })
})

describe('gridOf', () => {
  it('is one cluster of every tile, worst first, with no levels', () => {
    const grid = gridOf(tiles)
    expect([grid.level, grid.deeper, grid.clusters.length, grid.total]).toEqual([null, null, 1, 8])
    expect(names(grid.clusters[0]!.tiles).slice(0, 3)).toEqual(['Register 4', 'Register 6', 'Register 2'])
    expect(grid.tones).toMatchObject({ attention: 1, silent: 1, moving: 1, ok: 5 })
  })

  it('clusters by the first level, the worst cluster first and the replicas placed nowhere last', () => {
    const grid = gridOf(tiles, { levels, placeOf, nowhere: 'No place' })
    expect([grid.level, grid.deeper]).toEqual(['region', 'store'])
    expect(grid.clusters.map((c) => [c.label, c.trouble, c.tiles.length, c.worst])).toEqual([
      ['North', 2, 4, 'attention'],
      ['South', 1, 3, 'silent'],
      ['No place', 0, 1, 'ok'],
    ])
  })

  it('clusters by the level asked, and narrows to the places of its trail', () => {
    const byStore = gridOf(tiles, { levels, placeOf, level: 'store' })
    expect(byStore.clusters.map((c) => c.label)).toEqual(['Airport', 'Market', 'Harbour', ''])
    const north = gridOf(tiles, {
      levels,
      placeOf,
      level: 'store',
      within: [{ level: 'region', key: 'n', label: 'North' }],
    })
    expect([north.total, north.deeper, north.clusters.map((c) => c.label)]).toEqual([
      4,
      null,
      ['Airport', 'Harbour'],
    ])
  })

  it('shows only the tiles in the state asked', () => {
    const grid = gridOf(tiles, { levels, placeOf, state: 'synced' })
    expect([grid.total, grid.clusters.map((c) => c.trouble)]).toEqual([5, [0, 0, 0]])
  })
})

describe('into and levelUnder', () => {
  it('go one level down into a cluster, and the breadcrumb leads back up', () => {
    const top = gridOf(tiles, { levels, placeOf })
    const trail = into(top, top.clusters[0]!)
    expect(trail).toEqual([{ level: 'region', key: 'n', label: 'North' }])
    expect(levelUnder(levels, trail)).toBe('store')
    expect(levelUnder(levels, [])).toBe('region')
    // The last level has none under it: the trail narrows, the level stays.
    expect(levelUnder(levels, [...trail, { level: 'store', key: 'n2', label: 'Airport' }])).toBe('store')
  })

  it('do not go into the tiles placed nowhere, nor anywhere without levels', () => {
    const top = gridOf(tiles, { levels, placeOf })
    expect(into(top, top.clusters.at(-1)!)).toEqual([])
    const flat = gridOf(tiles)
    expect(into(flat, flat.clusters[0]!)).toEqual([])
  })
})
