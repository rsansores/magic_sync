// magic_sync_dashboard: the dashboard of the magic_sync engine, for Vue 3.

// The ready-made dashboard; its look is in `magic_sync_dashboard/styles.css`.
export { default as SyncDashboard } from './vue/SyncDashboard.vue'

// The same without its looks: a host with its own design system calls it in
// its own component and draws the answers its own way.
export { useSyncDashboard } from './vue/useSyncDashboard'
export type { SyncDashboard as SyncDashboardState, SyncDashboardOptions, Names } from './vue/useSyncDashboard'
export { builtinMessages } from './vue/i18n'
export type { Messages, MessageTable, Locale } from './vue/i18n'

// Everything framework-free, also as `magic_sync_dashboard/core`.
export * from './core/index'
