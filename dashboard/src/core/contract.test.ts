import { describe, expect, it } from 'vitest'
import type { NoteKind, ReplicaState, RowState, WishState } from './contract/types.js'
import overview from './contract/fixtures/overview.js'
import listing from './contract/fixtures/listing.js'
import progress from './contract/fixtures/progress.js'
import rows from './contract/fixtures/rows.js'
import wishes from './contract/fixtures/wishes.js'
import { parseTimestamp } from './time.js'

// The fixtures are checked field for field against the generated types by
// the type-check (`satisfies`); here, what a type cannot say.

// Exhaustive by type: a variant added on the engine's side fails to compile
// here until it is named.
const replicaStates = { awaiting_fix: 1, unreachable: 1, catching_up: 1, synced: 1 } satisfies Record<
  ReplicaState,
  1
>
const rowStates = {
  holds: 1,
  outdated: 1,
  on_its_way: 1,
  missing: 1,
  leaving: 1,
  quarantined: 1,
} satisfies Record<RowState, 1>
const noteKinds = { row: 1, members: 1, assigned: 1, rerouted: 1, resync: 1 } satisfies Record<NoteKind, 1>
const wishStates = { pending: 1, waiting: 1, applied: 1, refused: 1, superseded: 1 } satisfies Record<
  WishState,
  1
>

const TIME_FIELDS = /(_at|^now|^oldest|^waiting_since|^at)$/

function times(value: unknown, key = ''): [string, unknown][] {
  if (Array.isArray(value)) return value.flatMap((v) => times(v, key))
  if (value && typeof value === 'object') return Object.entries(value).flatMap(([k, v]) => times(v, k))
  return TIME_FIELDS.test(key) && value != null ? [[key, value]] : []
}

describe('the contract fixtures', () => {
  it('hold every state of a replica, every state of a row and every kind of note', () => {
    expect(new Set(overview.replicas.first.map((r) => r.state))).toEqual(new Set(Object.keys(replicaStates)))
    expect(new Set(rows.flatMap((r) => r.places.map((p) => p.state)))).toEqual(
      new Set(Object.keys(rowStates)),
    )
    expect(new Set(overview.health.parked.first.map((p) => p.kind))).toEqual(new Set(Object.keys(noteKinds)))
  })

  it('hold every state of a wish', () => {
    expect(new Set(wishes.wishes.first.map((w) => w.state))).toEqual(new Set(Object.keys(wishStates)))
  })

  it('write every time as the engine does, and every one reads', () => {
    const all = [overview, listing, progress, rows, wishes].flatMap((answer) => times(answer))
    expect(all.length).toBeGreaterThan(10)
    for (const [field, time] of all)
      expect([field, parseTimestamp(String(time))]).toEqual([field, expect.any(Number)])
    expect(overview.now).toMatch(/\.\d{6}Z$/)
  })
})
