import { describe, expect, it } from 'vitest'
import { builtinMessages, translator } from './i18n.js'

describe('translator', () => {
  it('picks a plural’s form by the locale’s rules', () => {
    const en = translator('en')
    expect(en('replica.pending', { n: 1 })).toBe('1 change on its way.')
    expect(en('replica.pending', { n: 0 })).toBe('0 changes on their way.')
    expect(en('replica.pending', { n: 2000 })).toBe('2,000 changes on their way.')
    const es = translator('es')
    expect(es('replica.pending', { n: 1 })).toBe('1 cambio en camino.')
    expect(es('replica.pending', { n: 0 })).toBe('0 cambios en camino.')
  })

  it('falls back to the other form for a category a message does not have', () => {
    // French says 0 in the singular; Spanish has a form for millions the
    // messages do not write out, so `other` stands in for it.
    const t = translator('fr', { fr: { k: { one: 'un {n}', other: 'des {n}' } } })
    expect(t('k', { n: 0 })).toBe('un 0')
    expect(translator('es')('replica.pending', { n: 1_000_000 })).toBe('1.000.000 cambios en camino.')
  })

  it('takes a regional locale’s language, and English for what it lacks', () => {
    expect(translator('es_MX')('tab.tables')).toBe('Datos')
    expect(translator('de')('tab.tables')).toBe('Data')
    expect(translator('es', { es: {}, en: { only: 'English' } })('only')).toBe('English')
  })

  it('leaves a placeholder it was not given as it is, and never writes nothing in its place', () => {
    expect(translator('en')('replica.retry')).toBe('Tried again {until}.')
  })

  it('has every English message in Spanish, with the same placeholders and plural forms', () => {
    const { en, es } = builtinMessages
    const shape = (m: unknown) =>
      JSON.stringify(
        typeof m === 'string' ? (m.match(/\{\w+\}/g) ?? []).sort() : Object.keys(m as object).sort(),
      )
    for (const [key, message] of Object.entries(en!)) {
      expect(es![key], key).toBeDefined()
      expect([key, shape(es![key])]).toEqual([key, shape(message)])
    }
    expect(Object.keys(es!).sort()).toEqual(Object.keys(en!).sort())
  })
})
