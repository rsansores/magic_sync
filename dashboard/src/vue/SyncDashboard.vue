<script setup lang="ts">
// The ready-made dashboard: a banner that says in one line whether
// everything is where it should be; counters that filter; every replica as
// a tile, worst first; the replicas that need attention, a sentence each;
// the tables and the problems. For the replica the host selects: where it
// stands, its checks, and only the tables that wait, both ways, every other
// one folded into one line.
//
// It fetches nothing. The host passes the engine's answers in, and answers
// `update:selected` (a replica picked: pass its progress and its own
// listing back) and, when it pages through the replicas, `update:filter` and
// `update:offset` (pass the listing back). Where one row is belongs next to
// the row, in the host's own listing: `RowPlaces`. Its look is in
// `magic_sync_dashboard/styles.css`.

import { computed, nextTick, ref, useId, watch } from 'vue'
import type { Listing, Overview, Progress, ReplicaState, States } from '../core/contract/types.js'
import {
  describeFlow,
  describeProblem,
  describeSettled,
  describeTable,
  downChips,
  percent,
  upChips,
} from '../core/describe.js'
import { knowsReplicaState, nameOf } from '../core/derive.js'
import { drawnOf, type GroupOf } from '../core/flow.js'
import { count } from '../core/format.js'
import { into, levelUnder, type Cluster, type Crumb, type Level, type Place } from '../core/grid.js'
import type { Messages } from '../core/i18n.js'
import type { Combine, HostChecks } from '../core/standing.js'
import { tabAfter } from '../core/tabs.js'
import { lookup, useSyncDashboard, type Names } from './useSyncDashboard.js'

const props = defineProps<{
  overview: Overview | null | undefined
  listing?: Listing | null
  progress?: Progress | null
  /** `Engine::listing` over the selected replica alone: its row, wherever
   * it is in the fleet. */
  replica?: Listing | null
  /** `Engine::states`, for a tile of every replica; the overview's worst otherwise. */
  states?: States | null
  names?: Names
  labels?: Names
  locale?: string
  messages?: Messages
  unansweringAfter?: number
  stuckAfter?: number
  /** The replicas a page holds: the `limit` the host asks `Engine::listing`
   * for. Default 50. */
  pageSize?: number
  /** The levels the host places its replicas at, the widest first, and
   * where each is at one: the tiles gather in a cluster per place. */
  levels?: Level[]
  placeOf?: (replica: string, level: string) => Place | null | undefined
  /** The group the host gathers each table in. */
  group?: GroupOf
  /** The checks the host knows of a replica, shown before the engine's own. */
  checks?: HostChecks
  /** A state of the host's own from the engine's and the host's checks:
   * counted with `states`, which has every replica. */
  combine?: Combine
}>()

defineSlots<{
  /** Under a replica's checks: what else the host knows of it. */
  'replica-extra'(props: { replica: string }): unknown
}>()

const selected = defineModel<string | null>('selected', { default: null })
const filter = defineModel<ReplicaState | null>('filter', { default: null })
const offset = defineModel<number>('offset', { default: 0 })
/** The level the tiles are clustered by, and the places they are narrowed to. */
const level = defineModel<string | null>('level', { default: null })
const within = defineModel<Crumb[]>('within', { default: () => [] })
/** The counter picked: the state the tiles and the replicas that need
 * attention are narrowed to — the engine's word, which `filter` lists too,
 * or one of `combine`'s, the listing then of every state. */
const state = defineModel<string | null>('state', { default: null })
watch(filter, (engine) => {
  if (engine != null && state.value !== engine) state.value = null
})
const shown = computed(() => state.value ?? filter.value)

const {
  summary,
  page,
  banner,
  error,
  speech,
  t,
  locale,
  grid,
  tally,
  attention,
  tables,
  detail,
  fleet,
  progress: shares,
} = useSyncDashboard({
  overview: () => props.overview,
  listing: () => props.listing,
  states: () => props.states,
  filter,
  state: shown,
  selected,
  progress: () => props.progress,
  replica: () => props.replica,
  names: () => props.names,
  labels: () => props.labels,
  locale: () => props.locale,
  messages: () => props.messages,
  unansweringAfter: () => props.unansweringAfter,
  stuckAfter: () => props.stuckAfter,
  levels: () => props.levels,
  placeOf: () => props.placeOf,
  level,
  within,
  group: () => props.group,
  checks: () => props.checks,
  combine: () => props.combine,
})

const n = (value: number) => count(value, locale.value)
// The banner's sentences on one line, each without its full stop.
const oneLine = (lines: string[]) => lines.map((line) => line.replace(/\.$/, '')).join(' · ')
const all = computed(() => tally.value.reduce((sum, x) => sum + x.n, 0))

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
    tables: tables.value?.lagging.reduce((sum, g) => sum + g.lagging.length, 0) ?? 0,
    problems: s ? s.problems.length + s.more : 0,
  }
})

function show(picked: string | null) {
  filter.value = picked != null && knowsReplicaState(picked) ? picked : null
  state.value = picked
  offset.value = 0
}
const pageEnd = computed(() => (page.value ? page.value.offset + page.value.rows.length : 0))
const hasPrevious = computed(() => (page.value?.offset ?? 0) > 0)
const hasNext = computed(() => (page.value ? pageEnd.value < page.value.total : false))
function turn(forward: boolean) {
  if (forward ? !hasNext.value : !hasPrevious.value) return
  offset.value = forward ? pageEnd.value : Math.max(0, (page.value?.offset ?? 0) - (props.pageSize ?? 50))
}

// Down into a cluster, and back up the trail.
function drill(cluster: Cluster) {
  within.value = into(grid.value, cluster)
  level.value = levelUnder(props.levels ?? [], within.value)
}
function climb(to: number) {
  within.value = within.value.slice(0, to)
  level.value = levelUnder(props.levels ?? [], within.value)
}
const canDrill = (cluster: Cluster) => grid.value.deeper != null && cluster.key !== ''
const legend = computed(() => tally.value.filter((x) => x.n > 0))

const current = computed(() => {
  const id = selected.value
  if (id == null) return null
  return { id, name: detail.value?.row.name ?? nameOf({ name: (r) => lookup(props.names, r) }, id) }
})
/** What the selected replica's tables wait on, each way: only the tables
 * that wait, under their group's label when the host groups them. */
const down = computed(() => (detail.value?.down ? drawnOf(detail.value.down) : null))
const up = computed(() => {
  const sp = speech.value
  if (!detail.value || !sp) return null
  const drawn = drawnOf(detail.value.up)
  return {
    ...drawn,
    groups: drawn.groups.map((g) => ({
      ...g,
      tables: g.tables.map((tb) => ({ ...tb, chips: upChips(tb, sp) })),
    })),
  }
})
const total = (value: number | null) => (value == null ? '–' : n(value))

// With `states`, the replicas that need attention are of the whole fleet:
// a page of them at first, and a page more at a time.
const step = computed(() => props.pageSize ?? 50)
const attentionLimit = ref(step.value)
watch([shown, step], () => (attentionLimit.value = step.value))
const attentionShown = computed(() =>
  fleet.value ? attention.value.slice(0, attentionLimit.value) : attention.value,
)
const attentionRest = computed(() => attention.value.length - attentionShown.value.length)

// A tile or a row opens its replica (`data-replica`), through one handler;
// focus follows the view: into the replica opened, and back to what opened
// it (`data-opens`), drawn anew when the view comes back.
const root = ref<HTMLElement | null>(null)
const detailHeading = ref<HTMLElement | null>(null)
let openedBy: string | null = null
function open(event: MouseEvent) {
  const el = (event.target as HTMLElement | null)?.closest<HTMLElement>('[data-replica]')
  if (!el?.dataset.replica) return
  openedBy = el.dataset.opens ?? null
  selected.value = el.dataset.replica
}
watch(selected, async (now, before) => {
  await nextTick()
  if (now != null) detailHeading.value?.focus()
  else if (before != null) {
    const by = [openedBy, `tile:${before}`].find((key) => key?.endsWith(`:${before}`))
    const back = by ? root.value?.querySelector<HTMLElement>(`[data-opens="${CSS.escape(by)}"]`) : null
    ;(back ?? tabButtons.value.replicas)?.focus()
  }
})
</script>

<template>
  <div ref="root" class="msd" :lang="locale">
    <p v-if="error" class="msd-error" role="alert">{{ error }}</p>
    <p v-else-if="!summary || !banner || !speech || !page" class="msd-muted">{{ t('loading') }}</p>
    <template v-else>
      <section class="msd-banner" :data-mood="banner.mood">
        <span class="msd-dot" aria-hidden="true" />
        <div>
          <!-- Announced when it changes; the line under it ticks every few seconds. -->
          <div role="status">
            <h2 class="msd-title">{{ banner.title }}</h2>
          </div>
          <p v-if="banner.lines.length" class="msd-line">{{ oneLine(banner.lines) }}</p>
        </div>
      </section>

      <section v-if="current" class="msd-detail" :aria-labelledby="`${id}-detail`">
        <button type="button" class="msd-link" @click="selected = null">
          <span aria-hidden="true">← </span>{{ t('detail.back') }}
        </button>
        <header class="msd-row-head">
          <span v-if="detail" class="msd-state" :data-tone="detail.standing.tone">
            {{ detail.standing.label }}
          </span>
          <h3 :id="`${id}-detail`" ref="detailHeading" class="msd-name msd-big" tabindex="-1">
            {{ current.name }}
          </h3>
        </header>
        <template v-if="detail">
          <p class="msd-muted">{{ detail.heard.join(' · ') }}</p>

          <div class="msd-totals">
            <div class="msd-total">
              <strong>{{ total(detail.totals.toSend) }}</strong>
              <span>{{ t('detail.to_send') }}</span>
            </div>
            <div class="msd-total">
              <strong>{{ total(detail.totals.toConfirm) }}</strong>
              <span>{{ t('detail.to_confirm') }}</span>
            </div>
            <div class="msd-total" :data-tone="detail.totals.setAside > 0 ? 'attention' : undefined">
              <strong>{{ n(detail.totals.setAside) }}</strong>
              <span>{{ t('detail.set_aside') }}</span>
            </div>
          </div>

          <h4 class="msd-section">{{ t('detail.checks') }}</h4>
          <ul class="msd-checks">
            <li
              v-for="check in detail.checks"
              :key="check.key"
              class="msd-check"
              :data-status="check.status"
              :data-tone="check.status === 'failing' ? (check.tone ?? 'attention') : undefined"
            >
              <span class="msd-check-mark" aria-hidden="true">{{
                check.status === 'ok'
                  ? '✓'
                  : check.status === 'working'
                    ? '↻'
                    : check.status === 'failing'
                      ? '!'
                      : '?'
              }}</span>
              <span>
                <span class="msd-check-label">{{ check.label }}</span>
                <span class="msd-check-line">{{ check.line }}</span>
              </span>
            </li>
          </ul>
          <slot name="replica-extra" :replica="current.id" />

          <template v-if="!shares">
            <h4 class="msd-section">{{ t('detail.down') }}</h4>
            <p class="msd-muted">{{ t('detail.loading') }}</p>
          </template>
          <template v-else-if="down">
            <h4 class="msd-section">{{ t('detail.down') }}</h4>
            <template v-for="g in down.groups" :key="g.key">
              <h5 v-if="group" class="msd-group">{{ g.label }}</h5>
              <ul class="msd-tables">
                <li v-for="tb in g.tables" :key="tb.table" class="msd-table">
                  <div class="msd-row-head msd-spread">
                    <span :id="`${id}-down-${tb.table}`" class="msd-name">{{ tb.label }}</span>
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
                    :aria-labelledby="`${id}-down-${tb.table}`"
                  >
                    <div :style="{ width: `${percent(tb.share)}%` }" />
                    <div
                      class="msd-bar-moving"
                      :style="{
                        width: `${tb.should === 0 ? 0 : Math.min(100 - percent(tb.share), (tb.inFlight / tb.should) * 100)}%`,
                      }"
                    />
                  </div>
                  <span class="msd-chips">
                    <span
                      v-for="chip in downChips(tb, speech)"
                      :key="chip.kind"
                      class="msd-chip-count"
                      :data-kind="chip.kind"
                    >
                      {{ chip.text }}
                    </span>
                  </span>
                </li>
              </ul>
            </template>
            <p v-if="down.folded > 0" class="msd-settled">
              {{ describeSettled(down.folded, speech) }}
            </p>
          </template>

          <template v-if="up && (up.groups.length > 0 || up.folded > 0)">
            <h4 class="msd-section">{{ t('detail.up') }}</h4>
            <template v-for="g in up.groups" :key="g.key">
              <h5 v-if="group" class="msd-group">{{ g.label }}</h5>
              <ul class="msd-tables">
                <li v-for="tb in g.tables" :key="tb.table" class="msd-table">
                  <div class="msd-row-head msd-spread">
                    <span class="msd-row-head">
                      <span class="msd-dot msd-dot-small" :data-tone="tb.tone" aria-hidden="true" />
                      <span class="msd-name">{{ tb.label }}</span>
                    </span>
                    <span class="msd-muted">{{ describeFlow(tb, speech) }}</span>
                  </div>
                  <span v-if="tb.chips.length" class="msd-chips">
                    <span
                      v-for="chip in tb.chips"
                      :key="chip.kind"
                      class="msd-chip-count"
                      :data-kind="chip.kind"
                    >
                      {{ chip.text }}
                    </span>
                  </span>
                </li>
              </ul>
            </template>
            <p v-if="up.folded > 0" class="msd-settled">
              {{ describeSettled(up.folded, speech) }}
            </p>
          </template>

          <template v-if="detail.problems.length">
            <h4 class="msd-section">{{ t('detail.problem') }}</h4>
            <ul class="msd-list">
              <li
                v-for="(p, i) in detail.problems.map((p) => describeProblem(p, speech!))"
                :key="i"
                class="msd-problem"
              >
                <strong>{{ p.title }}</strong>
                <span
                  v-for="part in p.about.filter((x) => x !== current!.name)"
                  :key="part"
                  class="msd-muted"
                  >{{ part }}</span
                >
                <code v-if="p.reason">{{ p.reason }}</code>
                <span v-for="fact in p.facts" :key="fact" class="msd-muted">{{ fact }}</span>
              </li>
            </ul>
          </template>
          <code v-else-if="detail.row.lastError">{{ detail.row.lastError }}</code>
        </template>
        <slot v-else name="replica-extra" :replica="current.id" />
      </section>

      <template v-else>
        <div class="msd-counters" role="group" :aria-label="t('filter.label')">
          <button type="button" class="msd-counter" :aria-pressed="shown == null" @click="show(null)">
            <strong>{{ n(all) }}</strong>
            <span>{{ t('filter.all') }}</span>
          </button>
          <button
            v-for="x in tally"
            :key="x.state"
            type="button"
            class="msd-counter"
            :data-tone="x.tone"
            :aria-pressed="shown === x.state"
            @click="show(x.state)"
          >
            <strong>{{ n(x.n) }}</strong>
            <span>{{ x.label }}</span>
          </button>
        </div>

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

        <div
          v-if="tab === 'replicas'"
          :id="`${id}-panel-replicas`"
          class="msd-panel"
          role="tabpanel"
          :aria-labelledby="`${id}-tab-replicas`"
          tabindex="0"
          @click="open"
        >
          <div class="msd-card">
            <div class="msd-row-head msd-spread">
              <h3 class="msd-name">{{ t('grid.title') }}</h3>
              <div v-if="levels?.length" class="msd-row-head" role="group" :aria-label="t('grid.by')">
                <span class="msd-muted">{{ t('grid.by') }}</span>
                <button
                  v-for="l in levels"
                  :key="l.key"
                  type="button"
                  class="msd-chip"
                  :aria-pressed="grid.level === l.key"
                  @click="level = l.key"
                >
                  {{ l.label }}
                </button>
              </div>
              <span v-else class="msd-muted">{{ t('grid.order') }}</span>
            </div>
            <nav v-if="levels?.length && within.length" class="msd-trail" :aria-label="t('grid.trail')">
              <button type="button" class="msd-link" @click="climb(0)">{{ t('grid.all') }}</button>
              <template v-for="(crumb, i) in within" :key="`${crumb.level}/${crumb.key}`">
                <span aria-hidden="true">›</span>
                <button v-if="i < within.length - 1" type="button" class="msd-link" @click="climb(i + 1)">
                  {{ crumb.label }}
                </button>
                <span v-else aria-current="location">{{ crumb.label }}</span>
              </template>
            </nav>
            <div class="msd-clusters" :data-flat="grid.level == null || undefined">
              <div v-for="cluster in grid.clusters" :key="cluster.key" class="msd-cluster">
                <div v-if="grid.level != null" class="msd-row-head msd-spread msd-cluster-head">
                  <button
                    v-if="canDrill(cluster)"
                    type="button"
                    class="msd-link msd-name"
                    @click="drill(cluster)"
                  >
                    {{ cluster.label }}
                  </button>
                  <span v-else class="msd-name">{{ cluster.label }}</span>
                  <span class="msd-muted">{{
                    cluster.trouble > 0
                      ? t('grid.trouble', { n: cluster.trouble, total: cluster.tiles.length })
                      : n(cluster.tiles.length)
                  }}</span>
                </div>
                <div class="msd-tiles">
                  <button
                    v-for="tile in cluster.tiles"
                    :key="tile.replica"
                    type="button"
                    class="msd-tile"
                    :data-replica="tile.replica"
                    :data-opens="`tile:${tile.replica}`"
                    :data-tone="tile.standing.tone"
                    :title="t('grid.tile', { name: tile.name, state: tile.standing.label })"
                    :aria-label="t('grid.tile', { name: tile.name, state: tile.standing.label })"
                  />
                </div>
              </div>
            </div>
            <ul class="msd-legend" :aria-label="t('grid.legend')">
              <li v-for="x in legend" :key="x.state" :data-tone="x.tone">{{ x.label }}</li>
            </ul>
          </div>

          <div class="msd-card">
            <div class="msd-row-head msd-spread">
              <h3 class="msd-name">{{ t('attention.title') }}</h3>
              <span class="msd-muted">{{ t('grid.order') }}</span>
            </div>
            <p v-if="attention.length === 0" class="msd-muted">
              {{
                !fleet && page.offset > 0
                  ? t('attention.none_here')
                  : shown == null
                    ? t('attention.none')
                    : t('replica.none')
              }}
            </p>
            <ul class="msd-attention">
              <li v-for="a in attentionShown" :key="a.id">
                <button
                  type="button"
                  class="msd-attention-row"
                  :data-replica="a.id"
                  :data-opens="`row:${a.id}`"
                >
                  <span class="msd-state" :data-tone="a.standing.tone">{{ a.standing.label }}</span>
                  <span class="msd-name">{{ a.name }}</span>
                  <span v-if="a.line" class="msd-muted">{{ a.line }}</span>
                </button>
              </li>
            </ul>
            <nav v-if="attentionRest > 0" class="msd-pager">
              <span class="msd-muted">
                {{ t('replicas.page', { from: 1, to: attentionShown.length, total: attention.length }) }}
              </span>
              <button type="button" class="msd-chip" @click="attentionLimit += step">
                {{ t('attention.more', { n: Math.min(step, attentionRest) }) }}
              </button>
            </nav>
            <p v-else-if="!fleet && page.worst && page.total > page.rows.length" class="msd-muted">
              {{ t('replicas.worst', { n: page.rows.length, total: page.total }) }}
            </p>
            <nav v-else-if="!fleet && !page.worst && page.total > page.rows.length" class="msd-pager">
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
          </div>
        </div>

        <div
          v-else-if="tab === 'tables' && tables"
          :id="`${id}-panel-tables`"
          class="msd-panel"
          role="tabpanel"
          :aria-labelledby="`${id}-tab-tables`"
          tabindex="0"
        >
          <ul class="msd-list">
            <li v-for="g in tables.lagging" :key="g.key" class="msd-item">
              <span class="msd-row-head">
                <span class="msd-dot msd-dot-small" :data-tone="g.tone" aria-hidden="true" />
                <span class="msd-name">{{ g.label }}</span>
                <span v-if="g.waiting > 0" class="msd-muted">{{ t('group.waiting', { n: g.waiting }) }}</span>
              </span>
              <template v-for="tb in g.lagging" :key="`${tb.audience}/${tb.table}`">
                <span v-if="group" class="msd-name">{{ tb.label }}</span>
                <span v-for="line in describeTable(tb, speech)" :key="line" class="msd-muted">{{
                  line
                }}</span>
              </template>
              <span v-if="g.settled.length" class="msd-settled">{{
                describeSettled(g.settled.length, speech)
              }}</span>
            </li>
          </ul>
          <p v-if="tables.settled.length" class="msd-settled">
            {{ describeSettled(tables.settled.length, speech, !!group) }}
          </p>
        </div>

        <div
          v-else
          :id="`${id}-panel-problems`"
          class="msd-panel"
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
        </div>
      </template>
    </template>
  </div>
</template>
