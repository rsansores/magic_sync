// Where one row is, per replica, without looks: the answer to "which
// registers hold this product?", asked next to the product in the host's own
// listing. `RowPlaces` is the ready-made list over it.

import { computed, toValue, type MaybeRefOrGetter } from 'vue'
import type { RowPlace, RowState } from '../core/types'
import { createDashboardI18n, type Messages } from './i18n'
import { lookup, type Names } from './useSyncDashboard'

export interface RowPlacesOptions {
  /** `Engine::row_progress`'s answer for the row. */
  places: MaybeRefOrGetter<RowPlace[] | null | undefined>
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
  const { t } = createDashboardI18n(
    () => toValue(options.locale),
    () => toValue(options.messages),
  )
  const rows = computed(() =>
    (toValue(options.places) ?? [])
      .map((p) => ({
        replica: p.replica,
        name: lookup(toValue(options.names), p.replica) ?? p.replica.slice(0, 8),
        state: p.state,
        mood: moods[p.state],
        label: t(`row.${p.state}`),
      }))
      .sort((a, b) => a.name.localeCompare(b.name, undefined, { numeric: true })),
  )
  return { rows, nowhere: computed(() => t('row.nowhere')), t }
}
