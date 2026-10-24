// The dashboard without its looks: what to show, and how to say it. The
// ready-made `SyncDashboard` is one layout over it; a host with its own
// design system calls it in its own component and draws the same answers
// with the sentence functions of `magic_sync_dashboard/core`, given `speech`.
//
// It reads nothing itself: the host fetches the engine's answers (from its
// own handlers, behind its own auth) and passes them in.

import { computed, toValue, type MaybeRefOrGetter } from 'vue'
import type { Listing, Overview, Progress, ReplicaState, States } from '../core/contract/types.js'
import { bannerOf, describeHeard, engineTableLabel, mismatchOf, type Speech } from '../core/describe.js'
import { pageOf, progressOf, readsContract, selectedRow, summarize, type ReplicaRow } from '../core/derive.js'
import { downOf, fleetOf, totalsOf, upOf, type GroupOf } from '../core/flow.js'
import { gridOf, tilesOf, type Crumb, type GridOptions, type Level, type Tile } from '../core/grid.js'
import type { Messages } from '../core/i18n.js'
import {
  attentionOf,
  checksOf,
  engineStanding,
  rankOf,
  standingOf,
  tallyOf,
  type Combine,
  type HostChecks,
  type Tally,
} from '../core/standing.js'
import { useServerClock } from './clock.js'
import { useDashboardI18n } from './i18n.js'

/** A name for each id: a map, or a function the host answers. */
export type Names = Record<string, string> | ((key: string) => string | undefined)

export interface SyncDashboardOptions {
  /** `Engine::overview`, as the host's handler answered it; `null` while loading. */
  overview: MaybeRefOrGetter<Overview | null | undefined>
  /** `Engine::listing` for `filter`, when the host pages through the replicas. */
  listing?: MaybeRefOrGetter<Listing | null | undefined>
  /** The engine's state the replicas are listed in; every state when `null`. */
  filter?: MaybeRefOrGetter<ReplicaState | null | undefined>
  /** The state the tiles and the replicas that need attention are narrowed
   * to, the engine's word or one of `combine`'s; `filter` when not given. */
  state?: MaybeRefOrGetter<string | null | undefined>
  /** The replica being looked at, and `Engine::progress` for it. */
  selected?: MaybeRefOrGetter<string | null | undefined>
  progress?: MaybeRefOrGetter<Progress | null | undefined>
  /** `Engine::listing` over the selected replica alone
   * (`Scope::Replicas(&[selected])`): its row, wherever it is in the fleet.
   * Without it, the row is looked for in the page and the overview's worst. */
  replica?: MaybeRefOrGetter<Listing | null | undefined>
  /** The host's names for its replicas. */
  names?: MaybeRefOrGetter<Names | undefined>
  /** The host's names for the tables. */
  labels?: MaybeRefOrGetter<Names | undefined>
  /** Force a locale ('en', 'es'); the host's vue-i18n locale otherwise. */
  locale?: MaybeRefOrGetter<string | undefined>
  /** Messages to add or override, per locale. */
  messages?: MaybeRefOrGetter<Messages | undefined>
  /** How long a replica may leave what it was sent unconfirmed before it is
   * "not answering", in ms. Default 15 minutes. */
  unansweringAfter?: MaybeRefOrGetter<number | undefined>
  /** How long a change may wait to be sent before the page asks for
   * attention, in ms. Default 15 minutes, the server's own alarm. */
  stuckAfter?: MaybeRefOrGetter<number | undefined>
  /** `Engine::states`: every replica's state, for the tiles. Without it the
   * tiles are the overview's worst. */
  states?: MaybeRefOrGetter<States | null | undefined>
  /** The levels the host places its replicas at, the widest first, and
   * where each replica is at one: the tiles gather in a cluster per place.
   * Without them, one cluster of every tile. */
  levels?: MaybeRefOrGetter<Level[] | undefined>
  placeOf?: MaybeRefOrGetter<GridOptions['placeOf']>
  /** The level the tiles are clustered by, and the places they are narrowed to. */
  level?: MaybeRefOrGetter<string | null | undefined>
  within?: MaybeRefOrGetter<Crumb[] | undefined>
  /** The group the host gathers each table in; a group per table otherwise. */
  group?: MaybeRefOrGetter<GroupOf | undefined>
  /** The checks the host knows of a replica, read before the engine's own. */
  checks?: MaybeRefOrGetter<HostChecks | undefined>
  /** A state of the host's own from the engine's and the host's checks. */
  combine?: MaybeRefOrGetter<Combine | undefined>
}

export const lookup = (names: Names | undefined, key: string) =>
  typeof names === 'function' ? names(key) : names?.[key]

export function useSyncDashboard(options: SyncDashboardOptions) {
  const { t, locale } = useDashboardI18n(
    () => toValue(options.locale),
    () => toValue(options.messages),
  )
  const answer = () => toValue(options.overview)
  const now = useServerClock(answer)

  /** Why the answer cannot be shown, when it cannot. */
  const error = computed(() => {
    const overview = answer()
    if (!overview) return null
    return mismatchOf(overview, t) ?? (now.value == null ? t('contract.unreadable') : null)
  })
  const derive = computed(() =>
    now.value == null
      ? null
      : {
          name: (replica: string) => lookup(toValue(options.names), replica),
          label: (table: string) => lookup(toValue(options.labels), table) ?? engineTableLabel(table, t),
          now: now.value,
          unansweringAfter: toValue(options.unansweringAfter),
          stuckAfter: toValue(options.stuckAfter),
        },
  )
  const speech = computed<Speech | null>(() =>
    now.value == null ? null : { t, locale: locale.value, now: now.value },
  )

  const summary = computed(() => {
    const overview = answer()
    return overview && derive.value && !error.value ? summarize(overview, derive.value) : null
  })
  const page = computed(() => {
    const overview = answer()
    return overview && derive.value && !error.value
      ? pageOf(overview, toValue(options.listing), toValue(options.filter) ?? null, derive.value)
      : null
  })
  const progress = computed(() => {
    const answer = toValue(options.progress)
    const selected = toValue(options.selected)
    const mine = answer && readsContract(answer) ? answer.replicas.find((p) => p.replica === selected) : null
    return mine ? progressOf(mine, { label: derive.value?.label }) : null
  })
  const banner = computed(() =>
    summary.value && speech.value ? bannerOf(summary.value, speech.value) : null,
  )

  const hostChecks = (replica: string) => toValue(options.checks)?.(replica) ?? []
  const standing = (replica: string, state: string) =>
    standingOf(state, hostChecks(replica), t, toValue(options.combine))

  /** Every replica as a tile: of `Engine::states`, else of the overview's worst. */
  const tiles = computed((): Tile[] => {
    const states = toValue(options.states)
    if (!summary.value || !derive.value) return []
    if (states && readsContract(states)) return tilesOf(states, t, { name: derive.value.name, standing })
    return summary.value.worst.map((r) => ({
      replica: r.id,
      name: r.name,
      audience: r.audience,
      standing: standing(r.id, r.state),
    }))
  })
  /** The state the tiles and the replicas that need attention are narrowed to. */
  const shown = () => toValue(options.state) ?? toValue(options.filter) ?? null
  /** The tiles in clusters, narrowed to the state shown. */
  const grid = computed(() =>
    gridOf(tiles.value, {
      levels: toValue(options.levels),
      placeOf: toValue(options.placeOf),
      level: toValue(options.level),
      within: toValue(options.within),
      state: shown(),
      nowhere: t('grid.nowhere'),
      locale: locale.value,
    }),
  )
  /** How many stand in each state, worst first: of every tile when the host
   * passed `states`, else the engine's counts. */
  const tally = computed((): Tally[] => {
    const s = summary.value
    if (!s) return []
    const states = toValue(options.states)
    if ((states && readsContract(states)) || toValue(options.combine))
      return tallyOf(tiles.value.map((x) => x.standing))
    return (Object.keys(s.counts) as ReplicaState[])
      .filter((state) => s.counts[state] > 0)
      .map((state) => ({ ...engineStanding(state, t), n: s.counts[state] }))
      .sort((a, b) => rankOf(a.tone) - rankOf(b.tone))
  })
  const rowStanding = (r: ReplicaRow) => standing(r.id, r.state)
  /** The replicas listed that are not settled, worst first, a sentence each:
   * of the state shown, when one is. */
  const attention = computed(() => {
    if (!page.value || !speech.value) return []
    return attentionOf(page.value.rows, speech.value, { standing: rowStanding, state: shown() })
  })
  /** The tables over the fleet, the settled folded. */
  const tables = computed(() =>
    summary.value ? fleetOf(summary.value.tables, toValue(options.group)) : null,
  )

  /** The replica looked at: where it stands, its checks, its tables both ways. */
  const detail = computed(() => {
    const id = toValue(options.selected)
    const sp = speech.value
    if (id == null || !sp || !derive.value) return null
    const row = selectedRow(
      id,
      toValue(options.replica),
      [...(page.value?.rows ?? []), ...(summary.value?.worst ?? [])],
      derive.value,
    )
    if (!row) return null
    const group = toValue(options.group)
    const down = progress.value ? downOf(progress.value.tables, group) : null
    const up = upOf(row, { label: derive.value.label }, group)
    return {
      row,
      standing: rowStanding(row),
      checks: checksOf(row, sp, toValue(options.checks)),
      heard: describeHeard(row, up, sp),
      totals: totalsOf(row, down),
      down,
      up,
      problems: summary.value?.problems.filter((p) => p.replica === id) ?? [],
    }
  })

  return {
    summary,
    page,
    progress,
    banner,
    error,
    speech,
    t,
    locale,
    now,
    tiles,
    grid,
    tally,
    attention,
    tables,
    detail,
  }
}

export type SyncDashboard = ReturnType<typeof useSyncDashboard>
