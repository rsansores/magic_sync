import { describe, expect, it } from 'vitest'
import { tabAfter } from './tabs.js'

const tabs = ['replicas', 'tables', 'problems'] as const

describe('tabAfter', () => {
  it('moves along the row with the arrows, wrapping at both ends', () => {
    expect(tabAfter(tabs, 'replicas', 'ArrowRight')).toBe('tables')
    expect(tabAfter(tabs, 'problems', 'ArrowRight')).toBe('replicas')
    expect(tabAfter(tabs, 'tables', 'ArrowLeft')).toBe('replicas')
    expect(tabAfter(tabs, 'replicas', 'ArrowLeft')).toBe('problems')
  })

  it('goes to the first and the last with Home and End, and nowhere on another key', () => {
    expect(tabAfter(tabs, 'tables', 'Home')).toBe('replicas')
    expect(tabAfter(tabs, 'tables', 'End')).toBe('problems')
    expect(tabAfter(tabs, 'tables', 'Enter')).toBeNull()
  })
})
