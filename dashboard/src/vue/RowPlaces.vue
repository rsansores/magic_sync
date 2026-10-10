<script setup lang="ts">
// Where one row is, replica by replica: put it next to the row, in the
// host's own listing ("which registers hold this product?"). Its look is in
// `magic_sync_dashboard/styles.css`.
import type { RowProgress } from '../core/contract/types.js'
import type { Messages } from '../core/i18n.js'
import { useRowPlaces } from './useRowPlaces.js'
import type { Names } from './useSyncDashboard.js'

const props = defineProps<{
  places: RowProgress | null | undefined
  names?: Names
  locale?: string
  messages?: Messages
}>()

const { rows, error, loading, t, locale } = useRowPlaces({
  places: () => props.places,
  names: () => props.names,
  locale: () => props.locale,
  messages: () => props.messages,
})
</script>

<template>
  <div class="msd" :lang="locale">
    <p v-if="error" class="msd-error" role="alert">{{ error }}</p>
    <p v-else-if="loading" class="msd-muted">{{ t('row.loading') }}</p>
    <p v-else-if="rows.length === 0" class="msd-muted">{{ t('row.nowhere') }}</p>
    <ul v-else class="msd-list">
      <li v-for="r in rows" :key="r.replica" class="msd-place">
        <span class="msd-name">{{ r.name }}</span>
        <span class="msd-state" :data-mood="r.mood">{{ r.label }}</span>
      </li>
    </ul>
  </div>
</template>
