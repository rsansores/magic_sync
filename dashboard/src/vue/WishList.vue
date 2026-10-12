<script setup lang="ts">
// The wishes of the replicas' settings, each with how its replica answered:
// the head office's "did the registers take the new brightness?". Its look
// is in `magic_sync_dashboard/styles.css`.
import type { WishEntry, WishListing } from '../core/contract/types.js'
import type { Messages } from '../core/i18n.js'
import { useWishes } from './useWishes.js'
import type { Names } from './useSyncDashboard.js'

const props = defineProps<{
  wishes: WishListing | null | undefined
  names?: Names
  setting?: (wish: WishEntry) => string | undefined
  locale?: string
  messages?: Messages
}>()

const { rows, error, loading, t, locale } = useWishes({
  wishes: () => props.wishes,
  names: () => props.names,
  setting: (wish) => props.setting?.(wish),
  locale: () => props.locale,
  messages: () => props.messages,
})
</script>

<template>
  <div class="msd" :lang="locale">
    <p v-if="error" class="msd-error" role="alert">{{ error }}</p>
    <p v-else-if="loading" class="msd-muted">{{ t('wish.loading') }}</p>
    <p v-else-if="rows.length === 0" class="msd-muted">{{ t('wish.none') }}</p>
    <ul v-else class="msd-list">
      <li v-for="w in rows" :key="w.id" class="msd-place">
        <span class="msd-name">{{ w.replica }} · {{ w.setting }}</span>
        <span class="msd-state" :data-mood="w.mood">{{ w.label }}</span>
        <span v-if="w.reason" class="msd-muted">{{ w.reason }}</span>
      </li>
    </ul>
  </div>
</template>
