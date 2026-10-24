<script setup lang="ts">
// The dashboard over made-up fleets, one per situation. A host fetches these
// answers from its own handlers; here they are made on the spot.
import { computed, ref } from 'vue'
import { RowPlaces, SyncDashboard } from '../src/index.js'
import {
  CONTRACT,
  type Overview,
  type Progress,
  type ReplicaOverview,
  type Level,
  type ReplicaState,
  type RowProgress,
  type States,
  type TableOverview,
} from '../src/index.js'

type Scenario = 'healthy' | 'catching_up' | 'attention'
// ?scenario=attention&locale=es&selected=<id> opens the demo on a situation.
const query = new URLSearchParams(location.search)
const scenario = ref<Scenario>((query.get('scenario') as Scenario | null) ?? 'catching_up')
const locale = ref(query.get('locale') ?? 'en')
const selected = ref<string | null>(query.get('selected'))
const theme = query.get('theme') ?? 'light'
// ?places=1 gathers the tiles by where the registers are.
const placed = ref(query.get('places') === '1')

const now = Date.now()
const ago = (minutes: number) => new Date(now - minutes * 60_000).toISOString()
const id = (n: number) => `0190a1b2-0000-7000-8000-${String(n).padStart(12, '0')}`
const FLEET = 24
const names = Object.fromEntries(Array.from({ length: FLEET }, (_, i) => [id(i), `Register ${i + 1}`]))
// Where one row is, as a host shows it next to the row in its own listing.
const places: RowProgress = {
  contract: CONTRACT,
  places: [
    { replica: id(0), audience: 'registers', state: 'holds' },
    { replica: id(1), audience: 'registers', state: 'on_its_way' },
    { replica: id(2), audience: 'registers', state: 'quarantined' },
    { replica: id(3), audience: 'registers', state: 'outdated' },
  ],
}
const labels = { products: 'Products', prices: 'Prices', customers: 'Customers', receipts: 'Receipts' }

function replica(i: number, scenario: Scenario): ReplicaOverview {
  const base: ReplicaOverview = {
    replica: id(i),
    audience: 'registers',
    state: 'synced',
    pending: 0,
    owed: 0,
    parked: 0,
    unconfirmed: false,
    quarantined: 0,
    unfiled: 0,
    awaiting_fix: 0,
    failures: 0,
    last_error: null,
    retry_at: null,
    sent_at: ago(3 + i),
    applied_at: ago(3 + i),
    waiting_since: null,
    last_heard_at: ago(i % 4),
    backlog: null,
    filed: [{ table: 'receipts', last_filed_at: ago(2 + i) }],
    wishes: { pending: 0, waiting: 0, refused: 0, superseded: 0 },
  }
  if (scenario === 'healthy') return base
  if (i % 5 === 1) return { ...base, state: 'catching_up', unconfirmed: true, waiting_since: ago(1) }
  if (i === 4) return { ...base, state: 'catching_up', unconfirmed: true, waiting_since: ago(190) }
  if (i === 7)
    return {
      ...base,
      state: 'catching_up',
      backlog: {
        at: ago(1),
        tables: [
          { table: 'receipts', unsent: 3, unfiled: 1, set_aside: 0, oldest_at: ago(200), waits: true },
        ],
      },
    }
  if (i === 13)
    return {
      ...base,
      state: 'silent',
      last_heard_at: ago(6),
      filed: [{ table: 'receipts', last_filed_at: ago(6) }],
    }
  if (i === 11)
    return { ...base, state: 'catching_up', wishes: { pending: 1, waiting: 1, refused: 1, superseded: 0 } }
  if (scenario === 'attention' && i === 2)
    return { ...base, state: 'awaiting_fix', quarantined: 2, awaiting_fix: 2 }
  if (scenario === 'attention' && i === 9)
    return {
      ...base,
      state: 'unreachable',
      failures: 4,
      last_error: 'connection refused',
      retry_at: new Date(now + 90_000).toISOString(),
      owed: 6,
      pending: 6,
    }
  return base
}

function table(name: string, scenario: Scenario): TableOverview {
  const busy = scenario !== 'healthy' && name === 'prices'
  const broken = scenario === 'attention' && name === 'products'
  return {
    table: name,
    audience: 'registers',
    synced: !busy && !broken,
    pending: busy ? 120 : 0,
    parked: 0,
    in_flight: busy ? 840 : 0,
    quarantined: broken ? 2 : 0,
    unfiled: 0,
    replicas: busy ? 5 : broken ? 1 : 0,
  }
}

const overview = computed<Overview>(() => {
  const s = scenario.value
  const attention = s === 'attention'
  const severity: ReplicaState[] = ['awaiting_fix', 'unreachable', 'silent', 'catching_up', 'synced']
  const replicas = Array.from({ length: FLEET }, (_, i) => replica(i, s)).sort(
    (a, b) => severity.indexOf(a.state) - severity.indexOf(b.state),
  )
  const states = { awaiting_fix: 0, unreachable: 0, silent: 0, catching_up: 0, synced: 0 }
  for (const r of replicas) states[r.state] += 1
  return {
    contract: CONTRACT,
    now: new Date(now).toISOString(),
    states,
    replicas: { total: replicas.length, first: replicas.slice(0, 20) },
    health: {
      healthy: !attention,
      pending: s === 'healthy' ? 0 : 120,
      oldest: s === 'healthy' ? null : ago(1),
      parked: { total: 0, first: [] },
      quarantine: {
        total: attention ? 2 : 0,
        first: attention
          ? [1, 2].map((n) => ({
              replica: id(2),
              table: 'products',
              key: `0190a1b2-aaaa-7000-8000-00000000000${n}`,
              reason: 'value too long for type character varying(40)',
              first_at: ago(42),
              last_at: ago(2),
              attempts: 5,
            }))
          : [],
      },
      inbox: { total: 0, first: [] },
      failing: {
        total: attention ? 1 : 0,
        first: attention
          ? [
              {
                replica: id(9),
                failures: 4,
                last_error: 'connection refused',
                retry_at: new Date(now + 90_000).toISOString(),
              },
            ]
          : [],
      },
      silent: {
        total: states.silent,
        first: replicas
          .filter((r) => r.state === 'silent')
          .map((r) => ({ replica: r.replica, last_heard_at: r.last_heard_at! })),
      },
    },
    tables: Object.keys(labels).map((name) => table(name, s)),
  }
})

// Every register's state, for a tile of each.
const states = computed<States>(() => ({
  contract: CONTRACT,
  now: overview.value.now,
  replicas: Array.from({ length: FLEET }, (_, i) => replica(i, scenario.value)).map((r) => ({
    replica: r.replica,
    audience: r.audience,
    state: r.state,
  })),
}))

// Where the registers are: a region, then a store.
const levels: Level[] = [
  { key: 'region', label: 'Region' },
  { key: 'store', label: 'Store' },
]
const stores = ['Harbour', 'Airport', 'Market', 'Station']
function placeOf(replica: string, level: string) {
  const store = Number(replica.slice(-2)) % stores.length
  return level === 'region'
    ? { key: store < 2 ? 'north' : 'south', label: store < 2 ? 'North' : 'South' }
    : { key: String(store), label: stores[store]! }
}

const progress = computed<Progress | null>(() => {
  if (!selected.value) return null
  const behind = overview.value.replicas.first.find((r) => r.replica === selected.value)?.state !== 'synced'
  return {
    contract: CONTRACT,
    replicas: [
      {
        replica: selected.value,
        audience: 'registers',
        tables: [
          {
            table: 'products',
            should: 1_200,
            holds: 1_200,
            outdated: 0,
            in_flight: 0,
            to_remove: 0,
            quarantined: 0,
          },
          {
            table: 'prices',
            should: 3_400,
            holds: behind ? 2_560 : 3_400,
            outdated: 0,
            in_flight: behind ? 840 : 0,
            to_remove: 0,
            quarantined: 0,
          },
          {
            table: 'customers',
            should: 5_000,
            holds: 5_000,
            outdated: behind ? 12 : 0,
            in_flight: 0,
            to_remove: 0,
            quarantined: 0,
          },
        ],
      },
    ],
  }
})
</script>

<template>
  <main class="page" :data-theme="theme">
    <header class="controls">
      <strong>magic_sync_dashboard</strong>
      <label v-for="s in ['healthy', 'catching_up', 'attention'] as const" :key="s">
        <input v-model="scenario" type="radio" :value="s" /> {{ s }}
      </label>
      <label><input v-model="placed" type="checkbox" /> places</label>
      <select v-model="locale">
        <option value="en">English</option>
        <option value="es">Español</option>
      </select>
    </header>
    <SyncDashboard
      v-model:selected="selected"
      :overview="overview"
      :states="states"
      :levels="placed ? levels : undefined"
      :place-of="placeOf"
      :progress="progress"
      :names="names"
      :labels="labels"
      :locale="locale"
    />
    <h3>Who holds “Coffee”?</h3>
    <RowPlaces :places="places" :names="names" :locale="locale" />
  </main>
</template>

<style>
body {
  margin: 0;
  font:
    15px/1.45 system-ui,
    sans-serif;
  background: #f6f7fb;
}
/* The dashboard takes a host's tokens from any ancestor. */
.page[data-theme='dark'] {
  --msd-fg: #e6e9f0;
  --msd-muted: #9aa3b5;
  --msd-card: #1f2430;
  --msd-border: #343b4a;
  --msd-accent: #7c9cff;
  --msd-ok-soft: #16261c;
  --msd-warn-soft: #2b2112;
  --msd-danger-soft: #2d1719;
  --msd-quiet: #aab2c2;
  --msd-quiet-soft: #262b36;
  --msd-off: #b197fc;
  --msd-off-soft: #251f3a;
  --msd-unknown: #66d9e8;
  --msd-unknown-soft: #142a2e;
  --msd-accent-soft: #1c2440;
  --msd-ok-tile: #2f6b3d;
  background: #151821;
  color: #e6e9f0;
}
.page {
  min-height: 100vh;
  max-width: 820px;
  margin: 0 auto;
  padding: 24px;
}
.controls {
  display: flex;
  gap: 14px;
  align-items: center;
  margin-bottom: 16px;
}
</style>
