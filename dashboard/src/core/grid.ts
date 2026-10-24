// The fleet as tiles, one per replica, coloured by where each stands. A
// host that knows where its replicas are (a region, a store, a route) gives
// the levels it places them at, and the tiles gather in clusters, one per
// place; a host that knows nothing of the kind has one cluster of them all.

import type { States } from './contract/types.js'
import { nameOf } from './derive.js'
import { collator } from './format.js'
import type { Translate } from './i18n.js'
import { engineStanding, rankOf, tones, type Standing, type Tone } from './standing.js'

/** One replica, as a tile. */
export interface Tile {
  replica: string
  name: string
  audience: string
  standing: Standing
}

export interface TileOptions {
  /** The host's name for a replica; the end of its id otherwise. */
  name?: (replica: string) => string | undefined
  /** Where a replica stands, when the host combines states of its own; the
   * engine's state otherwise. */
  standing?: (replica: string, state: string) => Standing | null | undefined
}

/** Every replica of `Engine::states`, as a tile. */
export function tilesOf(states: States, t: Translate, options: TileOptions = {}): Tile[] {
  return states.replicas.map((e) => ({
    replica: e.replica,
    name: nameOf(options, e.replica),
    audience: e.audience,
    standing: options.standing?.(e.replica, e.state) ?? engineStanding(e.state, t),
  }))
}

/** A way the host places its replicas: `{ key: 'region', label: 'Region' }`. */
export interface Level {
  key: string
  label: string
}

/** Where a replica is, at one level. */
export interface Place {
  key: string
  label: string
}

/** One step of the way down: the place the grid is narrowed to, at a level. */
export interface Crumb {
  level: string
  key: string
  label: string
}

/** The tiles of one place, worst first. */
export interface Cluster {
  /** The place's key; `''` for the tiles the host placed nowhere. */
  key: string
  label: string
  tiles: Tile[]
  /** Tiles not settled. */
  trouble: number
  /** The worst tone of its tiles. */
  worst: Tone
}

export interface Grid {
  /** The level the clusters are of; `null` with no levels. */
  level: string | null
  /** The level a cluster's header leads down to; `null` at the last. */
  deeper: string | null
  /** The places the grid is narrowed to, from the top. */
  trail: Crumb[]
  clusters: Cluster[]
  /** The tiles shown, and how many stand in each tone. */
  total: number
  tones: Record<Tone, number>
}

export interface GridOptions {
  /** The levels the host places replicas at, the widest first. */
  levels?: Level[]
  /** Where a replica is at a level; `null` when nowhere. */
  placeOf?: (replica: string, level: string) => Place | null | undefined
  /** The level to cluster by; the first level otherwise. */
  level?: string | null
  /** Only the tiles under these places. */
  within?: Crumb[]
  /** Only the tiles in this state. */
  state?: string | null
  /** The label of the cluster of tiles placed nowhere. */
  nowhere?: string
  locale?: string
}

const zeros = () => Object.fromEntries(tones.map((tone) => [tone, 0])) as Record<Tone, number>

const worstFirst = (order: Intl.Collator) => (a: Tile, b: Tile) =>
  rankOf(a.standing.tone) - rankOf(b.standing.tone) || order.compare(a.name, b.name)

/** The tiles in clusters, one per place at the level asked: the clusters
 * with the worst tiles first, and each cluster's tiles worst first. */
export function gridOf(tiles: Tile[], options: GridOptions = {}): Grid {
  const levels = options.levels ?? []
  const at = levels.findIndex((l) => l.key === options.level)
  const level = levels[at === -1 ? 0 : at]?.key ?? null
  const deeper = levels[(at === -1 ? 0 : at) + 1]?.key ?? null
  const trail = options.within ?? []
  const place = (replica: string, level: string) => options.placeOf?.(replica, level) ?? null
  const shown = tiles.filter(
    (tile) =>
      (options.state == null || tile.standing.state === options.state) &&
      trail.every((c) => place(tile.replica, c.level)?.key === c.key),
  )

  const counts = zeros()
  const clusters = new Map<string, Cluster>()
  for (const tile of shown) {
    counts[tile.standing.tone] += 1
    const where = level == null ? null : place(tile.replica, level)
    const key = level == null ? '*' : (where?.key ?? '')
    let cluster = clusters.get(key)
    if (!cluster) {
      cluster = {
        key,
        label: where?.label ?? (level == null ? '' : (options.nowhere ?? '')),
        tiles: [],
        trouble: 0,
        worst: 'ok',
      }
      clusters.set(key, cluster)
    }
    cluster.tiles.push(tile)
    if (tile.standing.tone !== 'ok') cluster.trouble += 1
    if (rankOf(tile.standing.tone) < rankOf(cluster.worst)) cluster.worst = tile.standing.tone
  }

  const order = collator(options.locale ?? 'en')
  for (const cluster of clusters.values()) cluster.tiles.sort(worstFirst(order))
  const sorted = [...clusters.values()].sort(
    (a, b) =>
      // The replicas placed nowhere last, whatever they hold.
      Number(a.key === '') - Number(b.key === '') ||
      rankOf(a.worst) - rankOf(b.worst) ||
      b.trouble - a.trouble ||
      order.compare(a.label, b.label),
  )
  return { level, deeper, trail, clusters: sorted, total: shown.length, tones: counts }
}

/** The trail one step down: into a cluster of the grid. */
export function into(grid: Grid, cluster: Cluster): Crumb[] {
  return grid.level == null || cluster.key === ''
    ? grid.trail
    : [...grid.trail, { level: grid.level, key: cluster.key, label: cluster.label }]
}

/** The level to cluster by once the trail is `trail`: the one under its
 * last place, or the first. */
export function levelUnder(levels: Level[], trail: Crumb[]): string | null {
  const last = trail.at(-1)
  if (!last) return levels[0]?.key ?? null
  const at = levels.findIndex((l) => l.key === last.level)
  return levels[at + 1]?.key ?? last.level
}
