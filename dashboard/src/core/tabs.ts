// The keyboard over a row of tabs, as WAI-ARIA's tabs pattern has it: the
// arrows move to the next or previous tab, wrapping; Home and End to the
// first and last.

/** The tab a key moves to from `current`; `null` for a key that does not. */
export function tabAfter<T>(tabs: readonly T[], current: T, key: string): T | null {
  const at = tabs.indexOf(current)
  const last = tabs.length - 1
  switch (key) {
    case 'ArrowRight':
      return tabs[at === last ? 0 : at + 1] ?? null
    case 'ArrowLeft':
      return tabs[at <= 0 ? last : at - 1] ?? null
    case 'Home':
      return tabs[0] ?? null
    case 'End':
      return tabs[last] ?? null
    default:
      return null
  }
}
