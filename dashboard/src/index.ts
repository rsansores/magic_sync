// magic_sync_dashboard: the dashboard of the magic_sync engine, for Vue 3.

// The ready-made dashboard; its look is in `magic_sync_dashboard/styles.css`.
export { default as SyncDashboard } from './vue/SyncDashboard.vue'

// The same without its looks: a host with its own design system calls it in
// its own component and draws the answers its own way, with the sentence
// functions of the core.
export { useSyncDashboard } from './vue/useSyncDashboard.js'
export type {
  SyncDashboard as SyncDashboardState,
  SyncDashboardOptions,
  Names,
} from './vue/useSyncDashboard.js'

// Where one row is: next to the row, in the host's own listing.
export { default as RowPlaces } from './vue/RowPlaces.vue'
export { useRowPlaces } from './vue/useRowPlaces.js'
export type { RowPlacesOptions, PlaceMood } from './vue/useRowPlaces.js'

// Everything framework-free, also as `magic_sync_dashboard/core`.
export * from './core/index.js'
