import { describe, expect, it } from 'vitest'
import {
  bannerOf,
  describeProblem,
  describePlace,
  describeReplica,
  describeState,
  describeTable,
  describeWish,
  mismatchOf,
  type Speech,
} from './describe.js'
import { summarize } from './derive.js'
import overview from './contract/fixtures/overview.js'
import { translator } from './i18n.js'
import { parseTimestamp } from './time.js'

const AT = parseTimestamp(overview.now)!
const MINUTE = 60_000
const speech = (now = AT, locale = 'en'): Speech => ({ t: translator(locale), locale, now })
const summary = (now = AT) => summarize(overview, { now })

describe('bannerOf', () => {
  it('says each thing the page waits on in a sentence of its own', () => {
    const banner = bannerOf(summary(), speech())
    expect(banner.title).toBe('Needs attention')
    expect(banner.lines).toEqual([
      '1 of 4 replicas has changes on their way.',
      '1 replica cannot be reached; it gets what it is owed once it can.',
      '6 problems wait for a fix.',
    ])
  })

  it('says so in the locale asked for', () => {
    expect(bannerOf(summary(), speech(AT, 'es')).lines[2]).toBe('6 problemas esperan una corrección.')
  })
})

describe('describeReplica', () => {
  const row = (name: string, now = AT) =>
    summarize(overview, { now, name: (r) => r }).worst.find((r) => r.id.endsWith(name))!

  it('says what an unreachable replica waits for, and when it is tried again, only when it knows', () => {
    const away = row('1')
    expect(describeReplica(away, speech())).toEqual([
      'Cannot be reached.',
      'Tried again now.',
      '2 messages wait to be sent to it.',
    ])
    expect(describeReplica({ ...away, retryAt: null }, speech())).toEqual([
      'Cannot be reached.',
      '2 messages wait to be sent to it.',
    ])
  })

  it('says how long a replica has not answered, by the server’s clock', () => {
    const later = AT + 20 * MINUTE
    expect(describeReplica(row('4', later), speech(later))).toEqual([
      'Last answered 20 min. ago.',
      '3 rows of its own are not at the server yet.',
    ])
  })

  it('says when a replica with nothing to do last confirmed, or that it never has', () => {
    const synced = row('5')
    expect(describeReplica(synced, speech(AT + 3 * 60 * MINUTE))).toEqual(['Last confirmed 3 hr. ago.'])
    expect(describeReplica({ ...synced, heardAt: null }, speech())).toEqual(['Nothing confirmed yet.'])
  })
})

describe('describeTable', () => {
  it('says each figure of a table that is behind in a sentence of its own', () => {
    const items = summary().tables.find((t) => t.table === 'items')!
    expect(describeTable(items, speech())).toEqual([
      '6 rows on their way.',
      '1 row set aside.',
      '1 change held back.',
      '3 replicas are behind on it.',
    ])
    expect(describeTable({ ...items, synced: true }, speech())).toEqual(['In sync everywhere.'])
  })
})

describe('describeProblem', () => {
  it('says what a problem is about part by part, and leaves out what it does not know', () => {
    const [quarantined, , parked] = summary().problems
    expect(describeProblem(quarantined!, speech(AT + 42 * MINUTE))).toMatchObject({
      title: 'Row set aside',
      about: ['000000000002', 'items', '00000000-0000-7000-8000-000000000003'],
      facts: ['First seen 42 min. ago.', '1 attempt.'],
    })
    expect(describeProblem(parked!, speech()).about).toEqual(['items', 'not-a-key'])
  })
})

describe('mismatchOf', () => {
  it('refuses an answer of another contract, naming both', () => {
    expect(mismatchOf(overview, translator('en'))).toBeNull()
    expect(mismatchOf({ contract: 2 }, translator('en'))).toMatch(/version 1.*version 2/)
  })
})

describe('a value an engine newer than the dashboard added', () => {
  it('is named as it came, never as a message key', () => {
    const t = translator('en')
    expect(describeState('synced', t)).toBe('Synced')
    expect(describeState('resting', t)).toBe('Unknown (resting)')
    expect(describePlace('holds', t)).toEqual({ mood: 'holds', label: 'Holds it' })
    expect(describePlace('teleporting', t)).toEqual({ mood: 'unknown', label: 'Unknown (teleporting)' })
    expect(describePlace('teleporting', translator('es')).label).toBe('Desconocido (teleporting)')
  })
})

describe('wishes', () => {
  const synced = summarize(overview, { now: AT, name: (r) => r }).worst.find((r) => r.state === 'synced')!

  it('says what a replica has not carried out, and what it answered otherwise', () => {
    const asked = { ...synced, wishes: { pending: 2, waiting: 1, refused: 1, superseded: 0 } }
    expect(describeReplica(asked, speech())).toEqual([
      '1 setting waits until it can be carried out.',
      '2 settings to carry out.',
      'It refused 1 setting.',
    ])
    expect(describeReplica(asked, speech(AT, 'es'))[1]).toBe('2 ajustes por aplicar.')
  })

  it('names where a wish stands, and one a later engine answers', () => {
    const t = translator('en')
    expect(describeWish('superseded', t)).toEqual({ mood: 'set_aside', label: 'Changed there first' })
    expect(describeWish('applied', t)).toEqual({ mood: 'holds', label: 'Applied' })
    expect(describeWish('waiting', t).mood).toBe('moving')
    expect(describeWish('postponed', t)).toEqual({ mood: 'unknown', label: 'Unknown (postponed)' })
  })
})
