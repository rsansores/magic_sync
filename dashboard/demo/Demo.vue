<script setup lang="ts">
// The dashboard over made-up fleets, one per situation. A host fetches these
// answers from its own handlers; here they are made on the spot.
import { computed, ref } from 'vue'
import { RowPlaces, SyncDashboard } from '../src/index'
import type { Overview, ReplicaOverview, ReplicaProgress, RowPlace, TableOverview } from '../src/index'

type Scenario = 'healthy' | 'catching_up' | 'attention'
// ?scenario=attention&locale=es&selected=<id> opens the demo on a situation.
const query = new URLSearchParams(location.search)
const scenario = ref<Scenario>((query.get('scenario') as Scenario | null) ?? 'catching_up')
const locale = ref(query.get('locale') ?? 'en')
const selected = ref<string | null>(query.get('selected'))
const theme = query.get('theme') ?? 'light'

const now = Date.now()
const ago = (minutes: number) => new Date(now - minutes * 60_000).toISOString()
const id = (n: number) => `0190a1b2-0000-7000-8000-${String(n).padStart(12, '0')}`
const FLEET = 24
const names = Object.fromEntries(Array.from({ length: FLEET }, (_, i) => [id(i), `Register ${i + 1}`]))
// Where one row is, as a host shows it next to the row in its own listing.
const places: RowPlace[] = [
  { replica: id(0), state: 'holds' },
  { replica: id(1), state: 'on_its_way' },
  { replica: id(2), state: 'quarantined' },
  { replica: id(3), state: 'outdated' },
]
const labels = { products: 'Products', prices: 'Prices', customers: 'Customers', receipts: 'Receipts' }

function replica(i: number, scenario: Scenario): ReplicaOverview {
  const base: ReplicaOverview = {
    replica: id(i),
    state: 'synced',
    pending: 0,
    owed: 0,
    parked: 0,
    unconfirmed: false,
    quarantined: 0,
    unfiled: 0,
    failures: 0,
    last_error: null,
    retry_at: null,
    sent_at: ago(3 + i),
    applied_at: ago(3 + i),
    backlog: null,
  }
  if (scenario === 'healthy') return base
  if (i % 5 === 1) return { ...base, state: 'catching_up', unconfirmed: true, applied_at: ago(2) }
  if (i === 4) return { ...base, state: 'catching_up', unconfirmed: true, applied_at: ago(190) }
  if (i === 7)
    return {
      ...base,
      state: 'catching_up',
      backlog: { at: ago(1), tables: [{ table: 'receipts', unsent: 3, unfiled: 1 }] },
    }
  if (scenario === 'attention' && i === 2) return { ...base, state: 'awaiting_fix', quarantined: 2 }
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
  return {
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
    },
    replicas: Array.from({ length: FLEET }, (_, i) => replica(i, s)),
    tables: Object.keys(labels).map((name) => table(name, s)),
  }
})

const progress = computed<ReplicaProgress | null>(() => {
  if (!selected.value) return null
  const behind = overview.value.replicas.find((r) => r.replica === selected.value)?.state !== 'synced'
  return {
    replica: selected.value,
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
      <select v-model="locale">
        <option value="en">English</option>
        <option value="es">Español</option>
      </select>
    </header>
    <SyncDashboard
      v-model:selected="selected"
      :overview="overview"
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
