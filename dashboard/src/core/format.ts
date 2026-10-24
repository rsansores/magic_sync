// Times and counts as a person reads them, in their locale. Every Intl
// object is made once per locale and kept.

/** A locale as Intl takes it: `es_MX` and `es-mx` read as `es-MX`; one that
 * does not read at all is English. */
export function normalizeLocale(locale: string | undefined): string {
  try {
    return Intl.getCanonicalLocales(locale?.replaceAll('_', '-') || 'en')[0] ?? 'en'
  } catch {
    return 'en'
  }
}

function cached<T>(make: (locale: string) => T): (locale: string) => T {
  const made = new Map<string, T>()
  return (locale) => {
    let it = made.get(locale)
    if (it === undefined) {
      it = make(locale)
      made.set(locale, it)
    }
    return it
  }
}

const relativeFormat = cached(
  (locale) => new Intl.RelativeTimeFormat(locale, { numeric: 'auto', style: 'short' }),
)
const numberFormat = cached((locale) => new Intl.NumberFormat(locale))
export const pluralRules = cached((locale) => new Intl.PluralRules(locale))
export const collator = cached((locale) => new Intl.Collator(locale, { numeric: true }))

const units: [Intl.RelativeTimeFormatUnit, number][] = [
  ['day', 86_400_000],
  ['hour', 3_600_000],
  ['minute', 60_000],
  ['second', 1_000],
]

/** The largest unit the span reaches once rounded in the unit below it:
 * 59.6 minutes is an hour, not "60 minutes". */
function relative(ms: number, locale: string): string {
  const size = Math.abs(ms)
  const at = units.findIndex(([, length], i) => size >= length - (units[i + 1]?.[1] ?? 0) / 2)
  const [unit, length] = units[at === -1 ? units.length - 1 : at]!
  return relativeFormat(locale).format(Math.round(ms / length), unit)
}

/** "3 hr. ago", for a moment in the past. */
export function since(at: number, now: number, locale: string): string {
  return relative(Math.min(0, at - now), locale)
}

/** "in 2 min.", for a moment to come. */
export function until(at: number, now: number, locale: string): string {
  return relative(Math.max(0, at - now), locale)
}

const spanFormat = cached(
  (locale) =>
    new Map(
      units.map(([unit]) => [
        unit,
        new Intl.NumberFormat(locale, { style: 'unit', unit, unitDisplay: 'short' }),
      ]),
    ),
)

/** "6 min", for how long something has lasted: in the largest unit it
 * reaches, as `since` picks it. */
export function lasting(ms: number, locale: string): string {
  const size = Math.max(0, ms)
  const at = units.findIndex(([, length], i) => size >= length - (units[i + 1]?.[1] ?? 0) / 2)
  const [unit, length] = units[at === -1 ? units.length - 1 : at]!
  return spanFormat(locale)
    .get(unit)!
    .format(Math.round(size / length))
}

/** 12,345 */
export function count(n: number, locale: string): string {
  return numberFormat(locale).format(n)
}
