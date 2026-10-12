// magic_sync_dashboard/core: the engine's answers, typed, and what a
// dashboard works out from them and says — no framework.

export * from './contract/types.js'
export {
  summarize,
  pageOf,
  progressOf,
  nameOf,
  readsContract,
  knowsReplicaState,
  wishMoodOf,
} from './derive.js'
export type {
  Mood,
  PlaceMood,
  SummaryOptions,
  Summary,
  ReplicaRow,
  ReplicaPage,
  TableRow,
  Problem,
  ProblemKind,
  TableShare,
  ProgressSummary,
} from './derive.js'
export {
  mismatchOf,
  bannerOf,
  describeReplica,
  describeTable,
  describeProblem,
  describeShare,
  describeState,
  describePlace,
  describeWish,
  tableMood,
  percent,
} from './describe.js'
export type { Speech, Banner, ProblemText } from './describe.js'
export { parseTimestamp, serverNow } from './time.js'
export { since, until, count, normalizeLocale } from './format.js'
export { builtinMessages, translator } from './i18n.js'
export type { Messages, MessageTable, Message, Plural, Locale, Translate } from './i18n.js'
export { tabAfter } from './tabs.js'
