// The JSON the engine's `magic_sync::status` answers, field for field. A
// host returns those answers from its own handlers, as they are; a test on
// the Rust side keeps `contract/overview.json` equal to what it serializes,
// and the tests here read that file.

/** A replica's id, as the engine knows it; the host puts a name on it. */
export type ReplicaId = string

/** An RFC 3339 timestamp. */
export type Timestamp = string

/** Where a replica stands, in one word: the first of these that applies. */
export type ReplicaState = 'awaiting_fix' | 'unreachable' | 'catching_up' | 'synced'

/** What a replica wrote and the server has not filed, one table's part. */
export interface UpProgress {
  table: string
  unsent: number
  unfiled: number
}

/** What a replica last said it wrote and the server has not filed. */
export interface Backlog {
  tables: UpProgress[]
  at: Timestamp
}

export interface ReplicaOverview {
  replica: ReplicaId
  state: ReplicaState
  /** Changes noted and not yet planned that may concern it. */
  pending: number
  /** Of those, the ones owed to it alone after a send to it failed. */
  owed: number
  /** Changes the planner set aside that may concern it. */
  parked: number
  /** Whether it has messages it has not said it applied. */
  unconfirmed: boolean
  quarantined: number
  unfiled: number
  failures: number
  last_error: string | null
  retry_at: Timestamp | null
  sent_at: Timestamp | null
  applied_at: Timestamp | null
  backlog: Backlog | null
}

export interface TableOverview {
  table: string
  /** Every figure below is zero. */
  synced: boolean
  pending: number
  parked: number
  in_flight: number
  quarantined: number
  unfiled: number
  /** Replicas with a row of it in flight, set aside or unfiled. */
  replicas: number
}

export interface Listed<T> {
  total: number
  first: T[]
}

export interface Parked {
  id: number
  kind: string
  table: string | null
  key: string | null
  replica: ReplicaId | null
  attempts: number
  last_error: string | null
  parked_at: Timestamp | null
}

export interface Quarantined {
  replica: ReplicaId
  table: string
  key: string
  reason: string
  first_at: Timestamp
  last_at: Timestamp
  attempts: number
}

export interface Unfiled {
  id: number
  replica: ReplicaId
  table: string
  reason: string
  first_at: Timestamp
  last_at: Timestamp
  attempts: number
}

export interface Failing {
  replica: ReplicaId
  failures: number
  last_error: string | null
  retry_at: Timestamp | null
}

export interface Health {
  /** Nothing waits for a fix and every replica can be reached. */
  healthy: boolean
  pending: number
  oldest: Timestamp | null
  parked: Listed<Parked>
  quarantine: Listed<Quarantined>
  inbox: Listed<Unfiled>
  failing: Listed<Failing>
}

/** `Engine::overview`: everything the dashboard shows first. */
export interface Overview {
  health: Health
  replicas: ReplicaOverview[]
  tables: TableOverview[]
}

/** One table on one replica, counted from what it confirmed applying. */
export interface TableProgress {
  table: string
  should: number
  holds: number
  outdated: number
  in_flight: number
  to_remove: number
  quarantined: number
}

/** `Engine::progress`, one replica's part. */
export interface ReplicaProgress {
  replica: ReplicaId
  tables: TableProgress[]
}

export type RowState = 'holds' | 'outdated' | 'on_its_way' | 'missing' | 'leaving' | 'quarantined'

/** `Engine::row_progress`, one replica's part. */
export interface RowPlace {
  replica: ReplicaId
  state: RowState
}
