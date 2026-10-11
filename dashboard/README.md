# magic_sync_dashboard

The dashboard of [magic_sync](https://github.com/rsansores/magic_sync), for
Vue 3: whether every replica holds what it should, which tables are on their
way, and what waits for a fix. One green banner when everything is in sync,
which is most of the time.

![The dashboard, with something to fix](https://rsansores.github.io/magic_sync/images/dashboard.png)

It fetches nothing. Your server asks the engine (`Engine::overview`,
`Engine::listing`, `Engine::progress`, `Engine::row_progress`) from its own
handlers, behind its own authentication, over the replicas the caller may
see, and returns the answers as they are. The dashboard draws them.

```sh
pnpm add magic_sync_dashboard
```

It needs Vue 3.5 or later.

## The ready-made dashboard

```vue
<script setup lang="ts">
import { ref } from 'vue'
import {
  SyncDashboard,
  type Listing,
  type Overview,
  type Progress,
  type ReplicaState,
} from 'magic_sync_dashboard'
import 'magic_sync_dashboard/styles.css'

const overview = ref<Overview | null>(null)
const listing = ref<Listing | null>(null)
const progress = ref<Progress | null>(null)
const selected = ref<string | null>(null)
const filter = ref<ReplicaState | null>(null)
const offset = ref(0)
// Fetch `overview` every few seconds, `listing` for `filter` and `offset`,
// and `progress` for `selected`, from your own endpoints.
</script>

<template>
  <SyncDashboard
    v-model:selected="selected"
    v-model:filter="filter"
    v-model:offset="offset"
    :overview="overview"
    :listing="listing"
    :progress="progress"
    :names="{ '0190a1b2-…': 'Register 1' }"
    :labels="{ products: 'Products' }"
  />
</template>
```

| Prop                         | What it is                                                                                         |
| ---------------------------- | -------------------------------------------------------------------------------------------------- |
| `overview`                   | `Engine::overview`'s answer; `null` while loading                                                  |
| `listing`                    | `Engine::listing`'s answer for `filter` and `offset`; without it, the overview's worst are listed  |
| `progress`                   | `Engine::progress`'s answer for the selected replica                                               |
| `names`, `labels`            | your names for replicas and tables: a map, or a function                                           |
| `v-model:selected`           | the replica being looked at; keep it in your URL if you like                                       |
| `v-model:filter`             | the state the replicas are listed in (`null`: all of them)                                         |
| `v-model:offset`, `pageSize` | where the page of the listing begins, and the `limit` you ask it for (50)                          |
| `locale`, `messages`         | see below                                                                                          |
| `silentAfter`, `stuckAfter`  | when a replica is "not answering" (15 min) and when waiting changes need attention (15 min), in ms |

An answer carries the version of its JSON (`contract`): one this dashboard
does not read is refused, with a message that says so, rather than drawn
wrong. Every time is measured against the server's `now` in the answer, moved
on by the time passed in the page — never against the browser's clock.

## Where one row is

"Which replicas hold this product?" belongs next to the product, in your own
listing: ask `Engine::row_progress` with its key, and show the answer with
`RowPlaces` (or `useRowPlaces`, without looks):

```vue
<RowPlaces :places="rowProgress" :names="registerNames" />
```

### Its look

Every colour and size is a CSS variable; set them on `.msd` or any ancestor:
`--msd-font`, `--msd-fg`, `--msd-muted`, `--msd-bg`, `--msd-card`,
`--msd-border`, `--msd-accent`, `--msd-ok`, `--msd-warn`,
`--msd-danger`, `--msd-ok-soft`, `--msd-warn-soft`, `--msd-danger-soft`,
`--msd-radius`, `--msd-gap`. The defaults keep every text at a contrast of
4.5:1 or more against what it is drawn on.

## The same, with your own components

A host with a design system of its own calls `useSyncDashboard` in its own
component and draws the same answers with its own parts, saying them with the
sentence functions of the core:

```ts
import { describeReplica, describeTable, describeProblem, useSyncDashboard } from 'magic_sync_dashboard'

const dash = useSyncDashboard({ overview, listing, filter, selected, progress, names, labels })
dash.error.value // why the answer cannot be shown (another contract), or null
dash.banner.value // { mood: 'healthy' | 'catching_up' | 'attention' | 'empty', title, lines }
dash.summary.value // counts per state, the worst replicas, tables, problems
dash.page.value // the replicas to list: rows, offset, total
dash.progress.value // the selected replica, per table: upToDate of should
describeReplica(row, dash.speech.value) // ["Last answered 3 hr. ago.", "2 rows set aside."]
describeTable(table, dash.speech.value) // ["840 rows on their way.", "5 replicas are behind on it."]
describeProblem(problem, dash.speech.value) // { title, about, reason, facts }
```

Everything that does not need Vue — the types, `summarize`, `pageOf`,
`progressOf`, the sentence functions and `translator` — is also in
`magic_sync_dashboard/core`.

## Languages

English and Spanish are built in. With `vue-i18n` in the host, the dashboard
follows its global locale on its own (`vue-i18n` is not a dependency); pin
one with `locale` (`es-MX` and `es_MX` both read as `es-MX`, and fall back to
`es`), and override or add messages with `messages`. A message with a count
has a form per plural category of the locale (`Intl.PluralRules`), `other`
always:

```ts
{
  en: { 'mood.healthy': 'All registers are up to date' },
  fr: { 'replica.pending': { one: '{n} modification en route.', other: '{n} modifications en route.' } },
}
```

## Versions

The dashboard reads the JSON of the engine of the same version: release and
upgrade them together. Its types are generated from the engine's
(`src/core/contract/types.ts`), and the engine's tests write what it answers
into `src/core/contract/fixtures/`, which the dashboard's type-check holds
field for field to those types and its tests read.

## License

MIT OR Apache-2.0.
