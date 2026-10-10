// Times and counts as a person reads them, in their locale.

const units: [Intl.RelativeTimeFormatUnit, number][] = [
  ['day', 86_400_000],
  ['hour', 3_600_000],
  ['minute', 60_000],
  ['second', 1_000],
]

function relative(ms: number, locale: string): string {
  const format = new Intl.RelativeTimeFormat(locale, { numeric: 'auto', style: 'short' })
  const size = Math.abs(ms)
  const [unit, length] = units.find(([, length]) => size >= length) ?? units[units.length - 1]!
  return format.format(Math.round(ms / length), unit)
}

/** "3 hr. ago", for a moment in the past. */
export function since(at: number, now: number, locale: string): string {
  return relative(Math.min(0, at - now), locale)
}

/** "in 2 min.", for a moment to come. */
export function until(at: number, now: number, locale: string): string {
  return relative(Math.max(0, at - now), locale)
}

/** 12,345 */
export function count(n: number, locale: string): string {
  return new Intl.NumberFormat(locale).format(n)
}
