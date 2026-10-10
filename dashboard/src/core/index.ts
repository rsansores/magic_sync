// magic_sync_dashboard/core: the engine's answers, typed, and what a
// dashboard works out from them — no framework.

export type * from './types'
export { summarize, progressOf, nameOf } from './derive'
export type {
  Mood,
  SummaryOptions,
  Summary,
  ReplicaRow,
  TableRow,
  Problem,
  ProblemKind,
  TableShare,
  ProgressSummary,
} from './derive'
export { since, until, count } from './format'
