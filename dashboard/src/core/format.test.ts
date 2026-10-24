import { describe, expect, it } from 'vitest'
import { count, lasting, normalizeLocale, since, until } from './format.js'

const MINUTE = 60_000

describe('normalizeLocale', () => {
  it('reads a locale written the POSIX way or in any case', () => {
    expect(normalizeLocale('es_MX')).toBe('es-MX')
    expect(normalizeLocale('es-mx')).toBe('es-MX')
    expect(normalizeLocale('EN')).toBe('en')
  })

  it('falls back to English for no locale or one that does not read', () => {
    expect(normalizeLocale(undefined)).toBe('en')
    expect(normalizeLocale('')).toBe('en')
    expect(normalizeLocale('not a locale!')).toBe('en')
  })
})

describe('since and until', () => {
  it('rounds to the unit a person would say', () => {
    expect(since(0, 59.6 * MINUTE, 'en')).toBe(since(0, 60 * MINUTE, 'en'))
    expect(since(0, 59.4 * MINUTE, 'en')).toBe(since(0, 59 * MINUTE, 'en'))
    expect(since(0, 30 * MINUTE, 'en')).toMatch(/30/)
    expect(since(0, 23.6 * 60 * MINUTE, 'en')).toBe(since(0, 24 * 60 * MINUTE, 'en'))
  })

  it('never says a past moment is to come, nor the reverse', () => {
    expect(since(10 * MINUTE, 0, 'en')).toBe(since(0, 0, 'en'))
    expect(until(0, 10 * MINUTE, 'en')).toBe(until(0, 0, 'en'))
    expect(until(2 * MINUTE, 0, 'en')).toMatch(/2/)
  })

  it('writes in the locale asked for', () => {
    expect(since(0, 3 * 60 * MINUTE, 'es')).not.toBe(since(0, 3 * 60 * MINUTE, 'en'))
    expect(count(12345, 'en')).toBe('12,345')
    expect(count(12345, 'de')).toBe('12.345')
  })
})

describe('lasting', () => {
  it('says how long in the largest unit the span reaches, never less than nothing', () => {
    expect(lasting(6 * MINUTE, 'en')).toBe('6 min')
    expect(lasting(59.6 * MINUTE, 'en')).toBe('1 hr')
    expect(lasting(3 * 1_000, 'es')).toBe('3 s')
    expect(lasting(-5 * MINUTE, 'en')).toBe('0 sec')
  })
})
