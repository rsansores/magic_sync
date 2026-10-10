// The dashboard without its looks: what to show, and how to say it. The
// ready-made `SyncDashboard` is one layout over it; a host with its own
// design system calls it in its own component and draws the same answers.
//
// It reads nothing itself: the host fetches the engine's answers (from its
// own handlers, behind its own auth) and passes them in.

import { computed, onScopeDispose, ref, toValue, type MaybeRefOrGetter } from 'vue'
import { progressOf, summarize, type Problem, type ReplicaRow, type TableRow } from '../core/derive'
import { count, since, until } from '../core/format'
import type { Overview, ReplicaId, ReplicaProgress, ReplicaState } from '../core/types'
import { createDashboardI18n, type Messages } from './i18n'

/** A name for each id: a map, or a function the host answers. */
export type Names = Record<string, string> | ((key: string) => string | undefined)

export interface SyncDashboardOptions {
  /** `Engine::overview`, as the host's handler answered it; `null` while loading. */
  overview: MaybeRefOrGetter<Overview | null | undefined>
  /** `Engine::progress` for the replica being looked at, when there is one. */
  progress?: MaybeRefOrGetter<ReplicaProgress | null | undefined>
  /** The host's names for its replicas. */
  names?: MaybeRefOrGetter<Names | undefined>
  /** The host's names for the tables. */
  labels?: MaybeRefOrGetter<Names | undefined>
  /** Force a locale ('en', 'es'); the host's vue-i18n locale otherwise. */
  locale?: MaybeRefOrGetter<string | undefined>
  /** Strings to add or override, per locale. */
  messages?: MaybeRefOrGetter<Messages | undefined>
  /** How long a replica may leave what it was sent unconfirmed before it is
   * "not answering", in ms. Default 15 minutes. */
  silentAfter?: MaybeRefOrGetter<number | undefined>
  /** How long a change may wait to be sent before the page asks for
   * attention, in ms. Default 10 minutes. */
  stuckAfter?: MaybeRefOrGetter<number | undefined>
}

export const lookup = (names: Names | undefined, key: string) =>
  typeof names === 'function' ? names(key) : names?.[key]

export function useSyncDashboard(options: SyncDashboardOptions) {
  const { t, locale } = createDashboardI18n(
    () => toValue(options.locale),
    () => toValue(options.messages),
  )
  // Relative times move on by themselves.
  const now = ref(Date.now())
  const timer = setInterval(() => (now.value = Date.now()), 15_000)
  onScopeDispose(() => clearInterval(timer))

  const name = (replica: ReplicaId) => lookup(toValue(options.names), replica)
  const label = (table: string) => lookup(toValue(options.labels), table)

  const summary = computed(() => {
    const overview = toValue(options.overview)
    return overview
      ? summarize(overview, {
          name,
          label,
          now: now.value,
          silentAfter: toValue(options.silentAfter),
          stuckAfter: toValue(options.stuckAfter),
        })
      : null
  })
  const progress = computed(() => {
    const p = toValue(options.progress)
    return p ? progressOf(p, { label }) : null
  })

  const ago = (at: number) => since(at, now.value, locale())
  const ahead = (at: number) => until(at, now.value, locale())
  const n = (value: number) => count(value, locale())
  const plural = (key: string, value: number) => t(key, { n: value })

  /** The banner: its title and the sentences under it. */
  const banner = computed(() => {
    const s = summary.value
    if (!s) return null
    const lines: string[] = []
    if (s.mood === 'healthy') lines.push(plural('mood.healthy.detail', s.total))
    if (s.counts.catching_up > 0)
      lines.push(t('mood.catching_up.detail', { n: s.counts.catching_up, total: s.total }))
    const problems = s.problems.length + s.more
    if (problems > 0) lines.push(plural('mood.attention.detail', problems))
    if (s.stuck && s.waitingSince != null) lines.push(t('mood.stuck', { since: ago(s.waitingSince) }))
    if (s.silent > 0) lines.push(plural('mood.silent', s.silent))
    return { mood: s.mood, title: t(`mood.${s.mood}`), lines }
  })

  const stateLabel = (state: ReplicaState) => t(`state.${state}`)

  /** What a replica is waiting on, in a sentence or a few. */
  function describeReplica(r: ReplicaRow): string[] {
    const out: string[] = []
    if (r.state === 'unreachable')
      out.push(
        t('replica.unreachable', {
          error: r.lastError ?? '',
          until: r.retryAt == null ? '' : ahead(r.retryAt),
        }),
      )
    if (r.quarantined > 0) out.push(plural('replica.quarantined', r.quarantined))
    if (r.unfiled > 0) out.push(plural('replica.unfiled', r.unfiled))
    if (r.parked > 0) out.push(plural('replica.parked', r.parked))
    if (r.silentFor != null) out.push(t('replica.silent', { since: ago(now.value - r.silentFor) }))
    else if (r.pending > 0) out.push(plural('replica.pending', r.pending))
    else if (r.unconfirmed) out.push(t('replica.applying'))
    if (r.backlog) out.push(plural('replica.backlog', r.backlog.unsent + r.backlog.unfiled))
    if (out.length === 0)
      out.push(r.heardAt == null ? t('replica.never') : t('replica.heard', { since: ago(r.heardAt) }))
    return out
  }

  /** What a table is waiting on, in one sentence. */
  function describeTable(table: TableRow): string {
    if (table.synced) return t('table.synced')
    const parts: string[] = []
    if (table.pending > 0) parts.push(plural('table.pending', table.pending))
    if (table.inFlight > 0)
      parts.push(`${plural('table.in_flight', table.inFlight)} ${plural('table.replicas', table.replicas)}`)
    if (table.quarantined > 0) parts.push(plural('table.quarantined', table.quarantined))
    if (table.unfiled > 0) parts.push(plural('table.unfiled', table.unfiled))
    if (table.parked > 0) parts.push(plural('table.parked', table.parked))
    return parts.join(' · ')
  }

  /** A problem: what it is, what it is about, and what is known of it. */
  function describeProblem(p: Problem) {
    const about = [p.replicaName, p.tableLabel, p.key].filter((x) => x != null).join(' · ')
    const facts: string[] = []
    if (p.since != null) facts.push(t('problem.since', { since: ago(p.since) }))
    if (p.attempts > 0) facts.push(plural('problem.attempts', p.attempts))
    if (p.retryAt != null) facts.push(t('problem.retry', { until: ahead(p.retryAt) }))
    return { title: t(`problem.${p.kind}`), about, reason: p.reason ?? '', facts: facts.join(' · ') }
  }

  return {
    summary,
    progress,
    banner,
    t,
    locale,
    now,
    name: (replica: ReplicaId) => name(replica) ?? replica.slice(0, 8),
    count: n,
    ago,
    ahead,
    plural,
    stateLabel,
    describeReplica,
    describeTable,
    describeProblem,
  }
}

export type SyncDashboard = ReturnType<typeof useSyncDashboard>
