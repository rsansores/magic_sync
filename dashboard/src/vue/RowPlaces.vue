<script setup lang="ts">
// Where one row is, replica by replica: put it next to the row, in the
// host's own listing ("which registers hold this product?").
import '../styles/dashboard.css'
import type { RowPlace } from '../core/types'
import type { Messages } from './i18n'
import { useRowPlaces } from './useRowPlaces'
import type { Names } from './useSyncDashboard'

const props = defineProps<{
  places: RowPlace[] | null | undefined
  names?: Names
  locale?: string
  messages?: Messages
}>()

const { rows, nowhere } = useRowPlaces({
  places: () => props.places,
  names: () => props.names,
  locale: () => props.locale,
  messages: () => props.messages,
})
</script>

<template>
  <div class="msd">
    <p v-if="rows.length === 0" class="msd-muted">{{ nowhere }}</p>
    <ul v-else class="msd-list">
      <li v-for="r in rows" :key="r.replica" class="msd-place">
        <span class="msd-name">{{ r.name }}</span>
        <span class="msd-state" :data-mood="r.mood">{{ r.label }}</span>
      </li>
    </ul>
  </div>
</template>
