// The dashboard without its looks: what to show, and how to say it. The
// ready-made `SyncDashboard` is one layout over it; a host with its own
// design system calls it in its own component and draws the same answers
// with the sentence functions of `magic_sync_dashboard/core`, given `speech`.
//
// It reads nothing itself: the host fetches the engine's answers (from its
// own handlers, behind its own auth) and passes them in.

import { computed, toValue, type MaybeRefOrGetter } from 'vue'
import type { Listing, Overview, Progress, ReplicaState } from '../core/contract/types.js'
import { bannerOf, mismatchOf, type Speech } from '../core/describe.js'
import { pageOf, progressOf, readsContract, summarize } from '../core/derive.js'
import type { Messages } from '../core/i18n.js'
import { useServerClock } from './clock.js'
import { useDashboardI18n } from './i18n.js'

/** A name for each id: a map, or a function the host answers. */
export type Names = Record<string, string> | ((key: string) => string | undefined)

export interface SyncDashboardOptions {
  /** `Engine::overview`, as the host's handler answered it; `null` while loading. */
  overview: MaybeRefOrGetter<Overview | null | undefined>
  /** `Engine::listing` for `filter`, when the host pages through the replicas. */
  listing?: MaybeRefOrGetter<Listing | null | undefined>
  /** The state the replicas are listed in; every state when `null`. */
  filter?: MaybeRefOrGetter<ReplicaState | null | undefined>
  /** The replica being looked at, and `Engine::progress` for it. */
  selected?: MaybeRefOrGetter<string | null | undefined>
  progress?: MaybeRefOrGetter<Progress | null | undefined>
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
  silentAfter?: MaybeRefOrGetter<number | undefined>
  /** How long a change may wait to be sent before the page asks for
   * attention, in ms. Default 15 minutes, the server's own alarm. */
  stuckAfter?: MaybeRefOrGetter<number | undefined>
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
          label: (table: string) => lookup(toValue(options.labels), table),
          now: now.value,
          silentAfter: toValue(options.silentAfter),
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

  return { summary, page, progress, banner, error, speech, t, locale, now }
}

export type SyncDashboard = ReturnType<typeof useSyncDashboard>
