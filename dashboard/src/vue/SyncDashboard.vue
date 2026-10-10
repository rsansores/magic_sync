<script setup lang="ts">
// The ready-made dashboard: a banner that says in one line whether
// everything is where it should be, then the replicas, the tables and the
// problems; and, for the replica the host selects, how far along it is.
//
// It fetches nothing. The host passes the engine's answers in, and answers
// `update:selected` (a replica picked: pass its progress back) and, when it
// pages through the replicas, `update:filter` and `update:offset` (pass the
// listing back). Where one row is belongs next to the row, in the host's own
// listing: `RowPlaces`. Its look is in `magic_sync_dashboard/styles.css`.

import { computed, nextTick, ref, useId, watch } from 'vue'
import type { Listing, Overview, Progress, ReplicaState } from '../core/contract/types.js'
import {
  describeProblem,
  describeReplica,
  describeShare,
  describeTable,
  percent,
  tableMood,
} from '../core/describe.js'
import { nameOf } from '../core/derive.js'
import { count } from '../core/format.js'
import type { Messages } from '../core/i18n.js'
import { tabAfter } from '../core/tabs.js'
import { lookup, useSyncDashboard, type Names } from './useSyncDashboard.js'

const props = defineProps<{
  overview: Overview | null | undefined
  listing?: Listing | null
  progress?: Progress | null
  names?: Names
  labels?: Names
  locale?: string
  messages?: Messages
  silentAfter?: number
  stuckAfter?: number
  /** The replicas a page holds: the `limit` the host asks `Engine::listing`
   * for. Default 50. */
  pageSize?: number
}>()

const selected = defineModel<string | null>('selected', { default: null })
const filter = defineModel<ReplicaState | null>('filter', { default: null })
const offset = defineModel<number>('offset', { default: 0 })

const {
  summary,
  page,
  progress: shares,
  banner,
  error,
  speech,
  t,
  locale,
} = useSyncDashboard({
  overview: () => props.overview,
  listing: () => props.listing,
  filter,
  selected,
  progress: () => props.progress,
  names: () => props.names,
  labels: () => props.labels,
  locale: () => props.locale,
  messages: () => props.messages,
  silentAfter: () => props.silentAfter,
  stuckAfter: () => props.stuckAfter,
})

const n = (value: number) => count(value, locale.value)
const states: ReplicaState[] = ['awaiting_fix', 'unreachable', 'catching_up', 'synced']

type Tab = 'replicas' | 'tables' | 'problems'
const tabs: Tab[] = ['replicas', 'tables', 'problems']
const tab = ref<Tab>('replicas')
const id = useId()
const tabButtons = ref<Partial<Record<Tab, HTMLElement>>>({})
function onTabKey(event: KeyboardEvent) {
  const next = tabAfter(tabs, tab.value, event.key)
  if (next == null) return
  event.preventDefault()
  tab.value = next
  tabButtons.value[next]?.focus()
}
const tabCount = computed(() => {
  const s = summary.value
  return {
    replicas: s?.total ?? 0,
    tables: s?.tables.filter((tb) => !tb.synced).length ?? 0,
    problems: s ? s.problems.length + s.more : 0,
  }
})

function show(state: ReplicaState | null) {
  filter.value = state
  offset.value = 0
}
const pageEnd = computed(() => (page.value ? page.value.offset + page.value.rows.length : 0))
const hasPrevious = computed(() => (page.value?.offset ?? 0) > 0)
const hasNext = computed(() => (page.value ? pageEnd.value < page.value.total : false))
function turn(forward: boolean) {
  if (forward ? !hasNext.value : !hasPrevious.value) return
  offset.value = forward ? pageEnd.value : Math.max(0, (page.value?.offset ?? 0) - (props.pageSize ?? 50))
}

const current = computed(() => {
  const id = selected.value
  if (id == null) return null
  const known = page.value?.rows.find((r) => r.id === id) ?? summary.value?.worst.find((r) => r.id === id)
  return { id, name: known?.name ?? nameOf({ name: (r) => lookup(props.names, r) }, id), row: known ?? null }
})
const ownProblems = computed(() => summary.value?.problems.filter((p) => p.replica === selected.value) ?? [])

// Focus follows the view: into the replica opened, and back to the button
// that opened it.
const detailHeading = ref<HTMLElement | null>(null)
const replicaButtons = new Map<string, HTMLElement>()
watch(selected, async (now, before) => {
  await nextTick()
  if (now != null) detailHeading.value?.focus()
  else if (before != null) (replicaButtons.get(before) ?? tabButtons.value.replicas)?.focus()
})
</script>

<template>
  <div class="msd" :lang="locale">
    <p v-if="error" class="msd-error" role="alert">{{ error }}</p>
    <p v-else-if="!summary || !banner || !speech || !page" class="msd-muted">{{ t('loading') }}</p>
    <template v-else>
      <section class="msd-banner" :data-mood="banner.mood">
        <span class="msd-dot" aria-hidden="true" />
        <div>
          <!-- Announced when it changes; the lines under it tick every few seconds. -->
          <div role="status">
            <h2 class="msd-title">{{ banner.title }}</h2>
          </div>
          <p v-for="line in banner.lines" :key="line" class="msd-line">{{ line }}</p>
        </div>
      </section>

      <section v-if="current" class="msd-detail" :aria-labelledby="`${id}-detail`">
        <button type="button" class="msd-link" @click="selected = null">
          <span aria-hidden="true">← </span>{{ t('detail.back') }}
        </button>
        <header class="msd-row-head">
          <span v-if="current.row" class="msd-state" :data-state="current.row.state">
            {{ t(`state.${current.row.state}`) }}
          </span>
          <h3 :id="`${id}-detail`" ref="detailHeading" class="msd-name" tabindex="-1">{{ current.name }}</h3>
        </header>
        <template v-if="current.row">
          <p v-for="line in describeReplica(current.row, speech)" :key="line" class="msd-muted">{{ line }}</p>
          <code v-if="current.row.lastError">{{ current.row.lastError }}</code>
        </template>
        <p v-if="!shares" class="msd-muted">{{ t('detail.loading') }}</p>
        <template v-else>
          <div class="msd-progress">
            <div
              class="msd-bar"
              role="progressbar"
              aria-valuemin="0"
              aria-valuemax="100"
              :aria-valuenow="percent(shares.share)"
              :aria-valuetext="t('detail.rows', { done: shares.upToDate, of: shares.should })"
              :aria-labelledby="`${id}-detail`"
            >
              <div :style="{ width: `${percent(shares.share)}%` }" />
            </div>
            <span aria-hidden="true">{{
              t('detail.rows', { done: shares.upToDate, of: shares.should })
            }}</span>
          </div>
          <ul class="msd-list">
            <li v-for="tb in shares.tables" :key="tb.table" class="msd-item">
              <div class="msd-row-head">
                <span :id="`${id}-table-${tb.table}`" class="msd-name">{{ tb.label }}</span>
                <span class="msd-muted" aria-hidden="true">
                  {{ t('detail.rows', { done: tb.upToDate, of: tb.should }) }}
                </span>
              </div>
              <div
                class="msd-bar"
                role="progressbar"
                aria-valuemin="0"
                aria-valuemax="100"
                :aria-valuenow="percent(tb.share)"
                :aria-valuetext="t('detail.rows', { done: tb.upToDate, of: tb.should })"
                :aria-labelledby="`${id}-table-${tb.table}`"
              >
                <div :style="{ width: `${percent(tb.share)}%` }" />
              </div>
              <p v-for="line in describeShare(tb, speech)" :key="line" class="msd-muted">{{ line }}</p>
            </li>
          </ul>
        </template>
        <ul v-if="ownProblems.length" class="msd-list">
          <li
            v-for="(p, i) in ownProblems.map((p) => describeProblem(p, speech!))"
            :key="i"
            class="msd-item msd-problem"
          >
            <strong>{{ p.title }}</strong>
            <span v-for="part in p.about" :key="part" class="msd-muted">{{ part }}</span>
            <code v-if="p.reason">{{ p.reason }}</code>
            <span v-for="fact in p.facts" :key="fact" class="msd-muted">{{ fact }}</span>
          </li>
        </ul>
      </section>

      <template v-else>
        <div class="msd-tabs" role="tablist" :aria-label="t('tabs')" @keydown="onTabKey">
          <button
            v-for="key in tabs"
            :id="`${id}-tab-${key}`"
            :key="key"
            :ref="(el) => (tabButtons[key] = (el as HTMLElement | null) ?? undefined)"
            type="button"
            role="tab"
            class="msd-tab"
            :aria-selected="tab === key"
            :aria-controls="`${id}-panel-${key}`"
            :tabindex="tab === key ? 0 : -1"
            @click="tab = key"
          >
            <span>{{ t(`tab.${key}`) }}</span>
            <span v-if="key === 'replicas' || tabCount[key] > 0" class="msd-count">{{
              n(tabCount[key])
            }}</span>
          </button>
        </div>

        <section
          v-if="tab === 'replicas'"
          :id="`${id}-panel-replicas`"
          role="tabpanel"
          :aria-labelledby="`${id}-tab-replicas`"
          tabindex="0"
        >
          <div class="msd-filters" role="group" :aria-label="t('filter.label')">
            <button type="button" class="msd-chip" :aria-pressed="filter == null" @click="show(null)">
              <span>{{ t('filter.all') }}</span>
              <span class="msd-count">{{ n(summary.total) }}</span>
            </button>
            <button
              v-for="state in states"
              :key="state"
              type="button"
              class="msd-chip"
              :data-state="state"
              :aria-pressed="filter === state"
              @click="show(state)"
            >
              <span>{{ t(`state.${state}`) }}</span>
              <span class="msd-count">{{ n(summary.counts[state]) }}</span>
            </button>
          </div>
          <p v-if="page.rows.length === 0" class="msd-muted">{{ t('replica.none') }}</p>
          <ul class="msd-list">
            <li v-for="r in page.rows" :key="r.id">
              <button
                :ref="
                  (el) => (el ? replicaButtons.set(r.id, el as HTMLElement) : replicaButtons.delete(r.id))
                "
                type="button"
                class="msd-item msd-replica"
                @click="selected = r.id"
              >
                <span class="msd-row-head">
                  <span class="msd-state" :data-state="r.state">{{ t(`state.${r.state}`) }}</span>
                  <span class="msd-name">{{ r.name }}</span>
                </span>
                <span v-for="line in describeReplica(r, speech)" :key="line" class="msd-muted">{{
                  line
                }}</span>
              </button>
            </li>
          </ul>
          <p v-if="page.worst && page.total > page.rows.length" class="msd-muted">
            {{ t('replicas.worst', { n: page.rows.length, total: page.total }) }}
          </p>
          <nav v-else-if="!page.worst && page.total > page.rows.length" class="msd-pager">
            <button type="button" class="msd-chip" :aria-disabled="!hasPrevious" @click="turn(false)">
              {{ t('replicas.previous') }}
            </button>
            <span class="msd-muted">
              {{ t('replicas.page', { from: page.offset + 1, to: pageEnd, total: page.total }) }}
            </span>
            <button type="button" class="msd-chip" :aria-disabled="!hasNext" @click="turn(true)">
              {{ t('replicas.next') }}
            </button>
          </nav>
        </section>

        <section
          v-else-if="tab === 'tables'"
          :id="`${id}-panel-tables`"
          role="tabpanel"
          :aria-labelledby="`${id}-tab-tables`"
          tabindex="0"
        >
          <ul class="msd-list">
            <li v-for="tb in summary.tables" :key="tb.table" class="msd-item">
              <span class="msd-row-head">
                <span class="msd-dot msd-dot-small" :data-mood="tableMood(tb)" aria-hidden="true" />
                <span class="msd-name">{{ tb.label }}</span>
              </span>
              <span v-for="line in describeTable(tb, speech)" :key="line" class="msd-muted">{{ line }}</span>
            </li>
          </ul>
        </section>

        <section
          v-else
          :id="`${id}-panel-problems`"
          role="tabpanel"
          :aria-labelledby="`${id}-tab-problems`"
          tabindex="0"
        >
          <p v-if="tabCount.problems === 0" class="msd-muted">{{ t('problem.none') }}</p>
          <ul class="msd-list">
            <li
              v-for="(p, i) in summary.problems.map((p) => describeProblem(p, speech!))"
              :key="i"
              class="msd-item msd-problem"
            >
              <strong>{{ p.title }}</strong>
              <span v-for="part in p.about" :key="part" class="msd-muted">{{ part }}</span>
              <code v-if="p.reason">{{ p.reason }}</code>
              <span v-for="fact in p.facts" :key="fact" class="msd-muted">{{ fact }}</span>
            </li>
          </ul>
          <p v-if="summary.more > 0" class="msd-muted">{{ t('problem.more', { n: summary.more }) }}</p>
          <p v-if="tabCount.problems > 0" class="msd-note">{{ t('problem.contract') }}</p>
        </section>
      </template>
    </template>
  </div>
</template>
