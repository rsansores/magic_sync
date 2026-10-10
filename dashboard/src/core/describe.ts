// The dashboard's sentences: pure functions of what `derive` worked out, the
// messages, and the server's time. Each line is one whole message.

import { CONTRACT } from './contract/types.js'
import {
  readsContract,
  type Mood,
  type Problem,
  type ProblemKind,
  type ReplicaRow,
  type Summary,
  type TableRow,
  type TableShare,
} from './derive.js'
import { since, until } from './format.js'
import type { Translate } from './i18n.js'

/** How to say things: the messages, the locale, and the server's time now. */
export interface Speech {
  t: Translate
  locale: string
  now: number
}

const ago = (at: number, speech: Speech) => since(at, speech.now, speech.locale)
const ahead = (at: number, speech: Speech) => until(at, speech.now, speech.locale)

/** Why this dashboard cannot read an answer, when it cannot. */
export function mismatchOf(answer: { contract: number }, t: Translate): string | null {
  return readsContract(answer) ? null : t('contract.mismatch', { ours: CONTRACT, theirs: answer.contract })
}

export interface Banner {
  mood: Mood
  title: string
  lines: string[]
}

/** The banner: its title and the sentences under it. */
export function bannerOf(s: Summary, speech: Speech): Banner {
  const { t } = speech
  const lines: string[] = []
  if (s.mood === 'empty') lines.push(t('mood.empty.detail'))
  if (s.mood === 'healthy') lines.push(t('mood.healthy.detail', { n: s.total }))
  if (s.counts.catching_up > 0)
    lines.push(t('mood.catching_up.detail', { n: s.counts.catching_up, total: s.total }))
  if (s.counts.unreachable > 0) lines.push(t('mood.unreachable', { n: s.counts.unreachable }))
  if (s.fixes > 0) lines.push(t('mood.attention.detail', { n: s.fixes }))
  if (s.stuck && s.waitingSince != null) lines.push(t('mood.stuck', { since: ago(s.waitingSince, speech) }))
  if (s.silent > 0) lines.push(t('mood.silent', { n: s.silent }))
  return { mood: s.mood, title: t(`mood.${s.mood}`), lines }
}

/** What a replica is waiting on, a sentence a line. */
export function describeReplica(r: ReplicaRow, speech: Speech): string[] {
  const { t } = speech
  const out: string[] = []
  if (r.state === 'unreachable') {
    out.push(t('replica.unreachable'))
    if (r.retryAt != null) out.push(t('replica.retry', { until: ahead(r.retryAt, speech) }))
  }
  if (r.quarantined > 0) out.push(t('replica.quarantined', { n: r.quarantined }))
  if (r.unfiled > 0) out.push(t('replica.unfiled', { n: r.unfiled }))
  if (r.parked > 0) out.push(t('replica.parked', { n: r.parked }))
  if (r.silentFor != null) out.push(t('replica.silent', { since: ago(speech.now - r.silentFor, speech) }))
  else if (r.pending > 0) out.push(t('replica.pending', { n: r.pending }))
  else if (r.unconfirmed) out.push(t('replica.applying'))
  if (r.backlog) out.push(t('replica.backlog', { n: r.backlog.unsent + r.backlog.unfiled }))
  if (out.length === 0)
    out.push(r.heardAt == null ? t('replica.never') : t('replica.heard', { since: ago(r.heardAt, speech) }))
  return out
}

/** What a table is waiting on, a sentence a line. */
export function describeTable(table: TableRow, speech: Speech): string[] {
  const { t } = speech
  if (table.synced) return [t('table.synced')]
  const out: string[] = []
  if (table.pending > 0) out.push(t('table.pending', { n: table.pending }))
  if (table.inFlight > 0) out.push(t('table.in_flight', { n: table.inFlight }))
  if (table.quarantined > 0) out.push(t('table.quarantined', { n: table.quarantined }))
  if (table.unfiled > 0) out.push(t('table.unfiled', { n: table.unfiled }))
  if (table.parked > 0) out.push(t('table.parked', { n: table.parked }))
  if (table.replicas > 0) out.push(t('table.replicas', { n: table.replicas }))
  return out
}

/** Whether a table waits on a fix, on the network, or on nothing. */
export function tableMood(table: TableRow): Mood {
  if (table.synced) return 'healthy'
  return table.quarantined + table.unfiled + table.parked > 0 ? 'attention' : 'catching_up'
}

export interface ProblemText {
  kind: ProblemKind
  title: string
  /** What it is about: the replica's name, the table's, the row's key. */
  about: string[]
  reason: string | null
  facts: string[]
}

/** A problem: what it is, what it is about, and what is known of it. */
export function describeProblem(p: Problem, speech: Speech): ProblemText {
  const { t } = speech
  const facts: string[] = []
  if (p.since != null) facts.push(t('problem.since', { since: ago(p.since, speech) }))
  if (p.attempts > 0) facts.push(t('problem.attempts', { n: p.attempts }))
  if (p.retryAt != null) facts.push(t('problem.retry', { until: ahead(p.retryAt, speech) }))
  return {
    kind: p.kind,
    title: t(`problem.${p.kind}`),
    about: [p.replicaName, p.tableLabel, p.key].filter((x) => x != null),
    reason: p.reason,
    facts,
  }
}

/** What else is true of a table on a replica, beyond how far along it is. */
export function describeShare(share: TableShare, speech: Speech): string[] {
  const { t } = speech
  const out: string[] = []
  if (share.inFlight > 0) out.push(t('detail.in_flight', { n: share.inFlight }))
  if (share.outdated > 0) out.push(t('detail.outdated', { n: share.outdated }))
  if (share.toRemove > 0) out.push(t('detail.to_remove', { n: share.toRemove }))
  if (share.quarantined > 0) out.push(t('detail.quarantined', { n: share.quarantined }))
  return out
}

/** A share as a whole percentage, never rounded up to 100 before it is. */
export function percent(share: number): number {
  return Math.floor(share * 100)
}
