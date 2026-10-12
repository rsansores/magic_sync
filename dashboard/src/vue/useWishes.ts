// The wishes of the replicas' settings, without looks: what the head office
// asked of each setting and how each replica answered. `WishList` is the
// ready-made list over it.

import { computed, toValue, type MaybeRefOrGetter } from 'vue'
import type { WishEntry, WishListing } from '../core/contract/types.js'
import { nameOf } from '../core/derive.js'
import { describeWish, mismatchOf } from '../core/describe.js'
import type { Messages } from '../core/i18n.js'
import { useDashboardI18n } from './i18n.js'
import { lookup, type Names } from './useSyncDashboard.js'

export interface WishesOptions {
  /** `Engine::wishes`' answer; `null` while loading. */
  wishes: MaybeRefOrGetter<WishListing | null | undefined>
  /** The host's names for its replicas. */
  names?: MaybeRefOrGetter<Names | undefined>
  /** The host's name for a wish's setting: its table and key are the
   * engine's, which a person does not read. */
  setting?: (wish: WishEntry) => string | undefined
  locale?: MaybeRefOrGetter<string | undefined>
  messages?: MaybeRefOrGetter<Messages | undefined>
}

export function useWishes(options: WishesOptions) {
  const { t, locale } = useDashboardI18n(
    () => toValue(options.locale),
    () => toValue(options.messages),
  )
  const answer = computed(() => toValue(options.wishes))
  const error = computed(() => (answer.value ? mismatchOf(answer.value, t) : null))
  const rows = computed(() => {
    if (!answer.value || error.value) return []
    return answer.value.wishes.first.map((w) => ({
      id: `${w.replica} ${w.table} ${w.key}`,
      replica: nameOf({ name: (r) => lookup(toValue(options.names), r) }, w.replica),
      setting: options.setting?.(w) ?? `${w.table} ${w.key}`,
      state: w.state,
      reason: w.reason,
      note: w.note,
      ...describeWish(w.state, t),
    }))
  })
  return {
    rows,
    /** How many there are in all, beyond the page. */
    total: computed(() => answer.value?.wishes.total ?? 0),
    error,
    /** The answer has not come yet. */
    loading: computed(() => answer.value == null),
    t,
    locale,
  }
}
