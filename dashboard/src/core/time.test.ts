import { describe, expect, it } from 'vitest'
import { parseTimestamp, serverNow } from './time.js'

describe('parseTimestamp', () => {
  it('reads every fraction the engine may write, to the millisecond', () => {
    const at = Date.UTC(2026, 0, 1, 12, 30, 15)
    expect(parseTimestamp('2026-01-01T12:30:15Z')).toBe(at)
    expect(parseTimestamp('2026-01-01T12:30:15.1Z')).toBe(at + 100)
    expect(parseTimestamp('2026-01-01T12:30:15.123Z')).toBe(at + 123)
    expect(parseTimestamp('2026-01-01T12:30:15.123456Z')).toBe(at + 123)
    expect(parseTimestamp('2026-01-01T12:30:15.123456789Z')).toBe(at + 123)
  })

  it('reads an offset as the moment it names', () => {
    expect(parseTimestamp('2026-01-01T06:30:15-06:00')).toBe(Date.UTC(2026, 0, 1, 12, 30, 15))
    expect(parseTimestamp('2026-01-01T14:00:15.5+01:30')).toBe(Date.UTC(2026, 0, 1, 12, 30, 15, 500))
  })

  it('refuses what is not a time instead of reading it as nothing', () => {
    for (const text of [
      '',
      'yesterday',
      '2026-01-01',
      '2026-01-01T12:30:15',
      '2026-13-01T00:00:00Z',
      '2026-04-31T00:00:00Z',
      '2026-01-01T24:00:00Z',
    ])
      expect([text, parseTimestamp(text)]).toEqual([text, null])
  })
})

describe('serverNow', () => {
  it('moves the server’s time on by what passed in the page, whatever the page’s clock says', () => {
    const answered = '2026-01-01T00:00:00Z'
    const server = Date.UTC(2026, 0, 1)
    // The page's clock is a day off: only what passed since the answer counts.
    const page = server + 86_400_000
    expect(serverNow(answered, page, page)).toBe(server)
    expect(serverNow(answered, page, page + 30_000)).toBe(server + 30_000)
    expect(serverNow('not a time', page, page)).toBeNull()
  })
})
