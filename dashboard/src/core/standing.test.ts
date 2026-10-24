import { describe, expect, it } from 'vitest'
import type { ReplicaOverview } from './contract/types.js'
import overview from './contract/fixtures/overview.js'
import { summarize } from './derive.js'
import type { Speech } from './describe.js'
import { translator } from './i18n.js'
import {
  attentionOf,
  checksOf,
  engineStanding,
  lineOf,
  standingOf,
  syncCheck,
  tallyOf,
  toneOf,
  type Check,
  type Standing,
} from './standing.js'
import { parseTimestamp } from './time.js'

const AT = parseTimestamp(overview.now)!
const MINUTE = 60_000
const speech = (locale = 'en', now = AT): Speech => ({ t: translator(locale), locale, now })
const t = translator('en')
const at = (ms: number) => new Date(AT - ms).toISOString()

// One replica as the overview lists it: synced, heard a moment ago,
// unless a test says otherwise.
const synced = overview.replicas.first.find((r) => r.state === 'synced')!
const rowOf = (r: Partial<ReplicaOverview>, now = AT) =>
  summarize(
    { ...overview, replicas: { total: 1, first: [{ ...synced, ...r }] } },
    { now, name: () => 'Register 7' },
  ).worst[0]!

describe('toneOf', () => {
  it('colours each of the engine’s states, and one it does not know as unknown', () => {
    expect(['awaiting_fix', 'unreachable', 'silent', 'catching_up', 'synced', 'asleep'].map(toneOf)).toEqual([
      'attention',
      'off',
      'silent',
      'moving',
      'ok',
      'unknown',
    ])
  })
})

describe('syncCheck', () => {
  it('is quiet for a replica that holds what it should, and says when it was last heard', () => {
    expect(syncCheck(rowOf({ last_heard_at: at(12_000) }), speech())).toEqual({
      key: 'sync',
      label: 'Sync',
      status: 'ok',
      line: 'Last heard 12 sec. ago',
    })
  })

  it('is working while the replica applies what it was sent', () => {
    const check = syncCheck(rowOf({ state: 'catching_up', last_heard_at: at(2_000) }), speech('es'))
    expect([check.status, check.label, check.line]).toEqual([
      'working',
      'Sincronización',
      'Aplicando cambios · última señal hace 2 s',
    ])
  })

  it('says what a replica catching up of its own accord waits on: its rows going up, or set aside', () => {
    const backlog = (unsent: number, set_aside: number) => ({
      at: at(1_000),
      tables: [{ table: 'receipts', unsent, unfiled: 0, set_aside, oldest_at: at(5 * MINUTE), waits: true }],
    })
    const uploading = rowOf({ state: 'catching_up', last_heard_at: at(2_000), backlog: backlog(4, 0) })
    expect(syncCheck(uploading, speech()).line).toBe('Rows of its own wait to go up · last heard 2 sec. ago')
    expect(syncCheck(uploading, speech('es')).line).toBe(
      'Tiene registros propios por subir · última señal hace 2 s',
    )
    const stuck = rowOf({ state: 'catching_up', last_heard_at: at(2_000), backlog: backlog(0, 2) })
    expect([syncCheck(stuck, speech()).status, syncCheck(stuck, speech()).line]).toEqual([
      'working',
      '2 rows of its own cannot go up as they are',
    ])
    const both = rowOf({ ...uploading.source, unconfirmed: true })
    expect(syncCheck(both, speech()).line).toBe('Applying changes · last heard 2 sec. ago')
  })

  it('fails for a replica that has said nothing, saying for how long', () => {
    const check = syncCheck(rowOf({ state: 'silent', last_heard_at: at(6 * MINUTE) }), speech())
    expect([check.status, check.tone, check.line]).toEqual([
      'failing',
      'silent',
      'Has said nothing for 6 min',
    ])
    expect(syncCheck(rowOf({ state: 'silent', last_heard_at: at(6 * MINUTE) }), speech('es')).line).toBe(
      'Sin señales desde hace 6 min',
    )
  })

  it('fails for what waits for a fix, and for a replica out of reach', () => {
    expect(syncCheck(rowOf({ state: 'awaiting_fix', quarantined: 2, unfiled: 1 }), speech()).line).toBe(
      '3 changes wait for a fix',
    )
    const away = syncCheck(
      rowOf({ state: 'unreachable', retry_at: new Date(AT + MINUTE).toISOString() }),
      speech(),
    )
    expect([away.status, away.tone, away.line]).toEqual([
      'failing',
      'off',
      'Cannot be reached · tried again in 1 min.',
    ])
  })

  it('knows nothing of a replica never heard from', () => {
    expect(syncCheck(rowOf({ last_heard_at: null, applied_at: null }), speech()).status).toBe('unknown')
  })
})

describe('checksOf', () => {
  it('reads the host’s checks first, in its order, and the engine’s last', () => {
    const link: Check = { key: 'link', label: 'Link', status: 'ok', line: 'Online' }
    const checks = checksOf(rowOf({}), speech(), (replica) => (replica === synced.replica ? [link] : []))
    expect(checks.map((c) => c.key)).toEqual(['link', 'sync'])
    expect(checksOf(rowOf({}), speech()).map((c) => c.key)).toEqual(['sync'])
  })
})

describe('standingOf', () => {
  const offline: Standing = { state: 'offline', tone: 'off', label: 'Offline' }
  const link = (status: Check['status']): Check => ({ key: 'link', label: 'Link', status, line: '' })
  const combine = (state: string, checks: Check[]) =>
    state !== 'awaiting_fix' && checks.some((c) => c.key === 'link' && c.status === 'failing')
      ? offline
      : null

  it('takes the host’s combination when it has one, and the engine’s state when not', () => {
    expect(standingOf('synced', [link('failing')], t, combine)).toEqual(offline)
    expect(standingOf('awaiting_fix', [link('failing')], t, combine)).toEqual(
      engineStanding('awaiting_fix', t),
    )
    expect(standingOf('synced', [link('ok')], t, combine)).toEqual({
      state: 'synced',
      tone: 'ok',
      label: 'Synced',
    })
    expect(standingOf('silent', [], t)).toEqual({ state: 'silent', tone: 'silent', label: 'Silent' })
  })
})

describe('tallyOf', () => {
  it('counts each state once, worst first, the host’s states among the engine’s', () => {
    const s = (state: string) => engineStanding(state, t)
    const tally = tallyOf([
      s('synced'),
      s('synced'),
      { state: 'offline', tone: 'off', label: 'Offline' },
      s('awaiting_fix'),
      s('catching_up'),
    ])
    expect(tally.map((x) => [x.state, x.n])).toEqual([
      ['awaiting_fix', 1],
      ['offline', 1],
      ['catching_up', 1],
      ['synced', 2],
    ])
  })
})

describe('lineOf', () => {
  it('says the one thing a replica waits on most, in a short sentence', () => {
    expect(lineOf(rowOf({ state: 'awaiting_fix', unfiled: 1, quarantined: 2 }), speech())).toBe(
      '1 row it sent could not be filed',
    )
    expect(lineOf(rowOf({ state: 'silent', last_heard_at: at(6 * MINUTE) }), speech())).toBe(
      'Last heard 6 min. ago',
    )
    expect(lineOf(rowOf({ state: 'catching_up', pending: 10, owed: 4 }), speech())).toBe(
      '14 changes on their way',
    )
    expect(lineOf(rowOf({ state: 'catching_up', unconfirmed: true }), speech('es'))).toBe(
      'Aplicando lo que se le envió',
    )
  })

  it('says rows of its own that wait to go up, and rows that cannot go as they are', () => {
    const backlog = (unsent: number, set_aside: number, waits: boolean) => ({
      at: at(1_000),
      tables: [{ table: 'receipts', unsent, unfiled: 1, set_aside, oldest_at: at(5 * MINUTE), waits }],
    })
    expect(lineOf(rowOf({ state: 'catching_up', backlog: backlog(4, 0, true) }), speech())).toBe(
      '5 rows of its own wait to go up',
    )
    expect(lineOf(rowOf({ state: 'catching_up', backlog: backlog(0, 1, true) }), speech('es'))).toBe(
      '1 registro propio no puede subir como está',
    )
    expect(lineOf(rowOf({ backlog: backlog(4, 0, false), last_heard_at: at(3_000) }), speech())).toBe(
      'Last heard 3 sec. ago',
    )
  })
})

describe('attentionOf', () => {
  const rows = summarize(overview, { now: AT, name: (r) => r.slice(-1) }).worst

  it('lists every replica not settled, worst first, each with one sentence', () => {
    const listed = attentionOf(rows, speech())
    expect(listed.map((a) => a.standing.state)).toEqual([
      'awaiting_fix',
      'unreachable',
      'silent',
      'catching_up',
    ])
    expect(listed.every((a) => a.line.length > 0)).toBe(true)
  })

  it('takes the host’s standing and sentence for a replica when it gives them', () => {
    const listed = attentionOf(rows, speech(), {
      standing: (r) =>
        r.state === 'synced'
          ? { state: 'offline', tone: 'off', label: 'Offline' }
          : engineStanding(r.state, t),
      line: (r) => (r.state === 'synced' ? 'Offline for 3 hr.' : null),
    })
    expect(listed.map((a) => [a.standing.state, a.name])).toContainEqual(['offline', '6'])
    expect(listed.find((a) => a.standing.state === 'offline')!.line).toBe('Offline for 3 hr.')
    expect(listed.map((a) => a.standing.tone)).toEqual(['attention', 'off', 'off', 'silent', 'moving'])
  })

  it('narrows to one state, the host’s as well as the engine’s', () => {
    const offline = (r: { state: string }) =>
      r.state === 'synced'
        ? { state: 'offline', tone: 'off' as const, label: 'Offline' }
        : engineStanding(r.state, t)
    expect(attentionOf(rows, speech(), { standing: offline, state: 'offline' }).map((a) => a.name)).toEqual([
      '6',
    ])
    expect(attentionOf(rows, speech(), { state: 'silent' }).map((a) => a.standing.state)).toEqual(['silent'])
  })
})
