// magic_sync_dashboard/core: the engine's answers, typed, and what a
// dashboard works out from them and says — no framework.

export * from './contract/types.js'
export {
  summarize,
  pageOf,
  selectedRow,
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
  engineTableLabel,
  tableMood,
  percent,
  downChips,
  upChips,
  describeFlow,
  describeSettled,
  describeHeard,
} from './describe.js'
export type { Speech, Banner, ProblemText, Chip } from './describe.js'
export {
  tones,
  rankOf,
  toneOf,
  engineStanding,
  standingOf,
  syncCheck,
  checksOf,
  tallyOf,
  countersOf,
  lineOf,
  attentionOf,
} from './standing.js'
export type {
  Tone,
  Standing,
  Check,
  HostChecks,
  Combine,
  Tally,
  Attention,
  AttentionOptions,
} from './standing.js'
export { tilesOf, gridOf, into, levelUnder } from './grid.js'
export type { Tile, TileOptions, Level, Place, Crumb, Cluster, Grid, GridOptions } from './grid.js'
export { downOf, upOf, fleetOf, totalsOf, drawnOf } from './flow.js'
export type {
  TableGroupKey,
  GroupOf,
  DownFlow,
  UpFlow,
  FleetFlow,
  TableGroup,
  Direction,
  Drawn,
  UpOptions,
  Totals,
} from './flow.js'
export { parseTimestamp, serverNow } from './time.js'
export { since, until, lasting, count, normalizeLocale } from './format.js'
export { builtinMessages, translator } from './i18n.js'
export type { Messages, MessageTable, Message, Plural, Locale, Translate } from './i18n.js'
export { tabAfter } from './tabs.js'
