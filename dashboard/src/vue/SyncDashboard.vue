<script setup lang="ts">
// The ready-made dashboard: a banner that says in one line whether
// everything is where it should be, then the replicas, the tables and the
// problems; and, for the replica the host selects, how far along it is.
//
// It fetches nothing. The host passes the engine's answers in, and answers
// `update:selected` (a replica picked; pass its progress back). Where one row
// is belongs next to the row, in the host's own listing: `RowPlaces`.

import { computed, ref } from 'vue'
import '../styles/dashboard.css'
import type { Overview, ReplicaProgress, ReplicaState } from '../core/types'
import type { Messages } from './i18n'
import { useSyncDashboard, type Names } from './useSyncDashboard'

const props = defineProps<{
  overview: Overview | null | undefined
  progress?: ReplicaProgress | null
  names?: Names
  labels?: Names
  locale?: string
  messages?: Messages
  silentAfter?: number
  stuckAfter?: number
}>()

const selected = defineModel<string | null>('selected', { default: null })

const dash = useSyncDashboard({
  overview: () => props.overview,
  progress: () => (selected.value && props.progress?.replica === selected.value ? props.progress : null),
  names: () => props.names,
  labels: () => props.labels,
  locale: () => props.locale,
  messages: () => props.messages,
  silentAfter: () => props.silentAfter,
  stuckAfter: () => props.stuckAfter,
})
const { t, summary, banner } = dash
const shares = dash.progress

type Tab = 'replicas' | 'tables' | 'problems'
const tab = ref<Tab>('replicas')
const filter = ref<ReplicaState | 'all'>('all')
const search = ref('')
const states: ReplicaState[] = ['awaiting_fix', 'unreachable', 'catching_up', 'synced']

const shown = computed(() => {
  const s = summary.value
  if (!s) return []
  const words = search.value.trim().toLocaleLowerCase()
  return s.replicas.filter(
    (r) => (filter.value === 'all' || r.state === filter.value) && r.name.toLocaleLowerCase().includes(words),
  )
})
const unsynced = computed(() => summary.value?.tables.filter((tb) => !tb.synced).length ?? 0)
const problemCount = computed(() => (summary.value ? summary.value.problems.length + summary.value.more : 0))
const current = computed(() => summary.value?.replicas.find((r) => r.id === selected.value) ?? null)
const ownProblems = computed(() => summary.value?.problems.filter((p) => p.replica === selected.value) ?? [])

const percent = (share: number) => `${Math.floor(share * 100)}%`
</script>

<template>
  <div class="msd">
    <p v-if="!summary || !banner" class="msd-muted">{{ t('loading') }}</p>
    <template v-else>
      <section class="msd-banner" :data-mood="banner.mood" role="status">
        <span class="msd-dot" aria-hidden="true" />
        <div>
          <h2 class="msd-title">{{ banner.title }}</h2>
          <p v-for="line in banner.lines" :key="line" class="msd-line">{{ line }}</p>
        </div>
      </section>

      <section v-if="current" class="msd-detail">
        <button type="button" class="msd-link" @click="selected = null">← {{ t('detail.back') }}</button>
        <header class="msd-row-head">
          <span class="msd-state" :data-state="current.state">{{ dash.stateLabel(current.state) }}</span>
          <h3 class="msd-name">{{ current.name }}</h3>
        </header>
        <p v-for="line in dash.describeReplica(current)" :key="line" class="msd-muted">{{ line }}</p>
        <p v-if="!shares" class="msd-muted">{{ t('detail.loading') }}</p>
        <template v-else>
          <div class="msd-progress">
            <div class="msd-bar"><div :style="{ width: percent(shares.share) }" /></div>
            <span>{{ t('detail.rows', { done: shares.upToDate, of: shares.should }) }}</span>
          </div>
          <ul class="msd-list">
            <li v-for="tb in shares.tables" :key="tb.table" class="msd-item">
              <div class="msd-row-head">
                <span class="msd-name">{{ tb.label }}</span>
                <span class="msd-muted">{{ t('detail.rows', { done: tb.upToDate, of: tb.should }) }}</span>
              </div>
              <div class="msd-bar"><div :style="{ width: percent(tb.share) }" /></div>
              <p class="msd-muted">
                <template v-if="tb.inFlight">{{ dash.plural('detail.in_flight', tb.inFlight) }} </template>
                <template v-if="tb.outdated">{{ dash.plural('detail.outdated', tb.outdated) }} </template>
                <template v-if="tb.toRemove">{{ dash.plural('detail.to_remove', tb.toRemove) }} </template>
                <template v-if="tb.quarantined">{{
                  dash.plural('detail.quarantined', tb.quarantined)
                }}</template>
              </p>
            </li>
          </ul>
        </template>
        <ul v-if="ownProblems.length" class="msd-list">
          <li v-for="(p, i) in ownProblems" :key="i" class="msd-item msd-problem">
            <strong>{{ dash.describeProblem(p).title }}</strong>
            <span class="msd-muted">{{ dash.describeProblem(p).about }}</span>
            <code v-if="p.reason">{{ p.reason }}</code>
            <span class="msd-muted">{{ dash.describeProblem(p).facts }}</span>
          </li>
        </ul>
      </section>

      <template v-else>
        <nav class="msd-tabs" role="tablist">
          <button
            v-for="[key, n] in [
              ['replicas', summary.total],
              ['tables', unsynced],
              ['problems', problemCount],
            ] as const"
            :key="key"
            type="button"
            role="tab"
            class="msd-tab"
            :aria-selected="tab === key"
            @click="tab = key"
          >
            {{ t(`tab.${key}`) }}
            <span v-if="key === 'replicas' || n > 0" class="msd-count">{{ dash.count(n) }}</span>
          </button>
        </nav>

        <section v-if="tab === 'replicas'" role="tabpanel">
          <div class="msd-filters">
            <button type="button" class="msd-chip" :aria-pressed="filter === 'all'" @click="filter = 'all'">
              {{ t('filter.all') }} {{ dash.count(summary.total) }}
            </button>
            <button
              v-for="state in states"
              :key="state"
              type="button"
              class="msd-chip"
              :data-state="state"
              :aria-pressed="filter === state"
              :disabled="summary.counts[state] === 0"
              @click="filter = state"
            >
              {{ dash.stateLabel(state) }} {{ dash.count(summary.counts[state]) }}
            </button>
            <input v-model="search" class="msd-input" type="search" :placeholder="t('search')" />
          </div>
          <p v-if="shown.length === 0" class="msd-muted">{{ t('replica.none') }}</p>
          <ul class="msd-list">
            <li v-for="r in shown" :key="r.id">
              <button type="button" class="msd-item msd-replica" @click="selected = r.id">
                <span class="msd-row-head">
                  <span class="msd-state" :data-state="r.state">{{ dash.stateLabel(r.state) }}</span>
                  <span class="msd-name">{{ r.name }}</span>
                </span>
                <span class="msd-muted">{{ dash.describeReplica(r).join(' · ') }}</span>
              </button>
            </li>
          </ul>
        </section>

        <section v-else-if="tab === 'tables'" role="tabpanel">
          <ul class="msd-list">
            <li v-for="tb in summary.tables" :key="tb.table" class="msd-item">
              <span class="msd-row-head">
                <span
                  class="msd-dot msd-dot-small"
                  :data-mood="
                    tb.synced
                      ? 'healthy'
                      : tb.quarantined + tb.unfiled + tb.parked > 0
                        ? 'attention'
                        : 'catching_up'
                  "
                  aria-hidden="true"
                />
                <span class="msd-name">{{ tb.label }}</span>
              </span>
              <span class="msd-muted">{{ dash.describeTable(tb) }}</span>
            </li>
          </ul>
        </section>

        <section v-else role="tabpanel">
          <p v-if="problemCount === 0" class="msd-muted">{{ t('problem.none') }}</p>
          <ul class="msd-list">
            <li v-for="(p, i) in summary.problems" :key="i" class="msd-item msd-problem">
              <strong>{{ dash.describeProblem(p).title }}</strong>
              <span class="msd-muted">{{ dash.describeProblem(p).about }}</span>
              <code v-if="p.reason">{{ p.reason }}</code>
              <span class="msd-muted">{{ dash.describeProblem(p).facts }}</span>
            </li>
          </ul>
          <p v-if="summary.more > 0" class="msd-muted">{{ dash.plural('problem.more', summary.more) }}</p>
          <p v-if="problemCount > 0" class="msd-note">{{ t('problem.contract') }}</p>
        </section>
      </template>
    </template>
  </div>
</template>
