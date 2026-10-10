// Where one row is, per replica, without looks: the answer to "which
// registers hold this product?", asked next to the product in the host's own
// listing. `RowPlaces` is the ready-made list over it.

import { computed, toValue, type MaybeRefOrGetter } from 'vue'
import type { RowProgress, RowState } from '../core/contract/types.js'
import { nameOf } from '../core/derive.js'
import { mismatchOf } from '../core/describe.js'
import { collator } from '../core/format.js'
import type { Messages } from '../core/i18n.js'
import { useDashboardI18n } from './i18n.js'
import { lookup, type Names } from './useSyncDashboard.js'

export interface RowPlacesOptions {
  /** `Engine::row_progress`'s answer for the row; `null` while loading. */
  places: MaybeRefOrGetter<RowProgress | null | undefined>
  /** The host's names for its replicas. */
  names?: MaybeRefOrGetter<Names | undefined>
  locale?: MaybeRefOrGetter<string | undefined>
  messages?: MaybeRefOrGetter<Messages | undefined>
}

/** Where a row is on a replica, in one word: settled, on its way, or set aside. */
export type PlaceMood = 'holds' | 'moving' | 'set_aside'

const moods: Record<RowState, PlaceMood> = {
  holds: 'holds',
  outdated: 'moving',
  on_its_way: 'moving',
  missing: 'moving',
  leaving: 'moving',
  quarantined: 'set_aside',
}

export function useRowPlaces(options: RowPlacesOptions) {
  const { t, locale } = useDashboardI18n(
    () => toValue(options.locale),
    () => toValue(options.messages),
  )
  const answer = computed(() => toValue(options.places))
  const error = computed(() => (answer.value ? mismatchOf(answer.value, t) : null))
  const rows = computed(() => {
    if (!answer.value || error.value) return []
    const compare = collator(locale.value).compare
    return answer.value.places
      .map((p) => ({
        replica: p.replica,
        name: nameOf({ name: (r) => lookup(toValue(options.names), r) }, p.replica),
        state: p.state,
        mood: moods[p.state],
        label: t(`row.${p.state}`),
      }))
      .sort((a, b) => compare(a.name, b.name))
  })
  return {
    rows,
    error,
    /** The answer has not come yet. */
    loading: computed(() => answer.value == null),
    t,
    locale,
  }
}
