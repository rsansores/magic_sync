// Where a replica stands, as a person reads it: a word, a tone a design
// system colours it by, and the checks behind it. The engine's state is one
// check — whether the replica applies what it is sent and says it is there;
// a host adds what only it knows (a link, a transport, anything) and may
// combine them into states of its own.

import type { ReplicaCounts, ReplicaState } from './contract/types.js'
import { knowsReplicaState, type ReplicaRow } from './derive.js'
import { lasting, since, until } from './format.js'
import type { Speech } from './describe.js'
import type { Translate } from './i18n.js'

/** The colour of a state, in one word, worst first: waits for a fix, cut
 * off, saying nothing, a state this dashboard has no word for, on its way,
 * settled. */
export type Tone = 'attention' | 'off' | 'silent' | 'unknown' | 'moving' | 'ok'

/** Every tone, worst first. */
export const tones: readonly Tone[] = ['attention', 'off', 'silent', 'unknown', 'moving', 'ok']

/** How bad a tone is: 0 is the worst. */
export const rankOf = (tone: Tone) => tones.indexOf(tone)

const engineTones: Record<ReplicaState, Tone> = {
  awaiting_fix: 'attention',
  unreachable: 'off',
  silent: 'silent',
  catching_up: 'moving',
  synced: 'ok',
}

/** The tone of one of the engine's states; `unknown` for one an engine
 * newer than this dashboard added. */
export function toneOf(state: string): Tone {
  return knowsReplicaState(state) ? engineTones[state] : 'unknown'
}

/** Where a replica stands: the engine's state, or one the host combined. */
export interface Standing {
  /** The state's key: the engine's word, or the host's. */
  state: string
  tone: Tone
  label: string
}

/** Where one of the engine's states stands, in the dashboard's words. */
export function engineStanding(state: string, t: Translate): Standing {
  return {
    state,
    tone: toneOf(state),
    label: knowsReplicaState(state) ? t(`state.${state}`) : t('state.unknown', { state }),
  }
}

/** One thing checked about a replica. A passing check stays quiet; only one
 * that fails is drawn in a colour — its `tone`, else the attention one. */
export interface Check {
  key: string
  label: string
  status: 'ok' | 'working' | 'failing' | 'unknown'
  /** One sentence: what was found. */
  line: string
  tone?: Tone
}

/** The checks a host knows of a replica, in the order they are read: the
 * first that fails explains the ones after it. */
export type HostChecks = (replica: string) => Check[] | undefined

/** A state of the host's own from the engine's and the host's checks, or
 * `null` to keep the engine's. */
export type Combine = (state: string, checks: Check[]) => Standing | null | undefined

/** Where a replica stands, all checks considered: the host's combination,
 * or the engine's state. */
export function standingOf(state: string, checks: Check[], t: Translate, combine?: Combine): Standing {
  return combine?.(state, checks) ?? engineStanding(state, t)
}

/** Whether the server has something on its way to a replica: what a replica
 * catching up for no reason of its own waits on. */
const owedDown = (r: ReplicaRow) =>
  r.pending + r.owed + r.parked + r.quarantined + r.wishes.pending > 0 || r.unconfirmed

/** The engine's own check: whether the replica applies what it is sent and
 * says it is there. */
export function syncCheck(r: ReplicaRow, speech: Speech): Check {
  const { t, locale, now } = speech
  const base = { key: 'sync', label: t('check.sync') }
  const heard = r.lastHeardAt ?? r.heardAt
  if (r.state === 'awaiting_fix')
    return { ...base, status: 'failing', tone: 'attention', line: t('check.sync.fix', { n: r.awaitingFix }) }
  if (r.state === 'unreachable')
    return {
      ...base,
      status: 'failing',
      tone: 'off',
      line:
        r.retryAt == null
          ? t('check.sync.unreachable')
          : t('check.sync.retry', { until: until(r.retryAt, now, locale) }),
    }
  if (r.state === 'silent' && heard != null)
    return {
      ...base,
      status: 'failing',
      tone: 'silent',
      line: t('check.sync.silent', { for: lasting(now - heard, locale) }),
    }
  if (heard == null) return { ...base, status: 'unknown', line: t('check.sync.never') }
  const ago = since(heard, now, locale)
  if (r.state === 'catching_up') {
    const working = { ...base, status: 'working' as const }
    if (r.backlog && r.backlog.setAside > 0)
      return { ...working, line: t('check.sync.set_aside', { n: r.backlog.setAside }) }
    if (r.backlog?.waits && !owedDown(r))
      return { ...working, line: t('check.sync.uploading', { since: ago }) }
    return { ...working, line: t('check.sync.applying', { since: ago }) }
  }
  return {
    ...base,
    status: knowsReplicaState(r.state) ? 'ok' : 'unknown',
    line: t('check.sync.heard', { since: ago }),
  }
}

/** Every check of a replica: the host's, in its order, then the engine's. */
export function checksOf(r: ReplicaRow, speech: Speech, host?: HostChecks): Check[] {
  return [...(host?.(r.id) ?? []), syncCheck(r, speech)]
}

/** How many replicas stand in one state. */
export interface Tally extends Standing {
  n: number
}

/** How many stand in each state, worst first: the counters a page filters by. */
export function tallyOf(standings: Iterable<Standing>): Tally[] {
  const by = new Map<string, Tally>()
  for (const s of standings) {
    const known = by.get(s.state)
    if (known) known.n += 1
    else by.set(s.state, { ...s, n: 1 })
  }
  return [...by.values()].sort((a, b) => rankOf(a.tone) - rankOf(b.tone))
}

/** The counters of a page: of where every replica stands, when the page has
 * every replica (`Engine::states`) — the host's combined states included —
 * else the engine's own counts. A page that knows only some replicas counts
 * none of them by hand: the host's states need every replica. */
export function countersOf(counts: ReplicaCounts, fleet: Standing[] | null, t: Translate): Tally[] {
  if (fleet) return tallyOf(fleet)
  return (Object.keys(counts) as ReplicaState[])
    .filter((state) => counts[state] > 0)
    .map((state) => ({ ...engineStanding(state, t), n: counts[state] }))
    .sort((a, b) => rankOf(a.tone) - rankOf(b.tone))
}

/** One replica that needs attention: where it stands, and one sentence —
 * `null` when the page holds no row of it to say it from. */
export interface Attention {
  id: string
  name: string
  standing: Standing
  line: string | null
}

/** What a replica waits on, in one short sentence: its state decides, in
 * the engine's order, and a cause is said only within the state it
 * explains. */
export function lineOf(r: ReplicaRow, speech: Speech): string {
  const { t, locale, now } = speech
  const heard = r.lastHeardAt ?? r.heardAt
  const lastHeard = () =>
    heard == null ? t('line.never') : t('line.heard', { since: since(heard, now, locale) })
  switch (r.state) {
    case 'awaiting_fix':
      if (r.unfiled > 0) return t('line.unfiled', { n: r.unfiled })
      if (r.quarantined > 0) return t('line.quarantined', { n: r.quarantined })
      if (r.parked > 0) return t('line.parked', { n: r.parked })
      return t('check.sync.fix', { n: r.awaitingFix })
    case 'unreachable':
      return r.retryAt == null
        ? t('line.unreachable')
        : t('line.retry', { until: until(r.retryAt, now, locale) })
    case 'silent':
      return heard == null ? t('line.never') : t('line.silent', { since: since(heard, now, locale) })
    case 'catching_up': {
      if (r.backlog && r.backlog.setAside > 0) return t('line.set_aside', { n: r.backlog.setAside })
      const moving = r.pending + r.owed
      if (moving > 0) return t('line.moving', { n: moving })
      if (r.unconfirmed) return t('line.applying')
      if (r.backlog?.waits) return t('line.uploading', { n: r.backlog.unsent + r.backlog.unfiled })
      return lastHeard()
    }
    default:
      return lastHeard()
  }
}

export interface AttentionOptions {
  /** Where each stands; the engine's state otherwise. */
  standing?: (r: ReplicaRow) => Standing
  /** The host's sentence for one, when it has a better one. */
  line?: (r: ReplicaRow) => string | null | undefined
  /** Only those in this state — the engine's word, or the host's. */
  state?: string | null
  /** Every replica, where each stands (the tiles of `Engine::states`): the
   * list is of them, and `rows` only lends the sentences. Without it, the
   * list is of `rows`. */
  fleet?: { replica: string; name: string; standing: Standing }[] | null
}

/** The replicas that need attention, worst first, each with one sentence:
 * every one not settled, of the state asked for when one is — of the whole
 * fleet when it is given, else of the rows. */
export function attentionOf(rows: ReplicaRow[], speech: Speech, options: AttentionOptions = {}): Attention[] {
  const said = (r: ReplicaRow) => options.line?.(r) ?? lineOf(r, speech)
  const fleet = options.fleet
  let listed: Attention[]
  if (fleet) {
    const byId = new Map(rows.map((r) => [r.id, r]))
    listed = fleet.map((x) => {
      const r = byId.get(x.replica)
      return { id: x.replica, name: x.name, standing: x.standing, line: r ? said(r) : null }
    })
  } else {
    listed = rows.map((r) => ({
      id: r.id,
      name: r.name,
      standing: options.standing?.(r) ?? engineStanding(r.state, speech.t),
      line: said(r),
    }))
  }
  return listed
    .filter((a) => a.standing.tone !== 'ok' && (options.state == null || a.standing.state === options.state))
    .sort((a, b) => rankOf(a.standing.tone) - rankOf(b.standing.tone))
}
