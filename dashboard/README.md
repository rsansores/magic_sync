# magic_sync_dashboard

The dashboard of [magic_sync](https://github.com/rsansores/magic_sync), for
Vue 3: whether every replica holds what it should, which tables are on their
way, and what waits for a fix. One green banner when everything is in sync,
which is most of the time.

![The dashboard, with something to fix](https://rsansores.github.io/magic_sync/images/dashboard.png)

It fetches nothing. Your server asks the engine (`Engine::overview`,
`Engine::progress`, `Engine::row_progress`) from its own handlers, behind its
own authentication, over the replicas the caller may see, and returns the
answers as they are. The dashboard draws them.

```sh
pnpm add magic_sync_dashboard
```

## The ready-made dashboard

```vue
<script setup lang="ts">
import { ref } from 'vue'
import { SyncDashboard, type Overview, type ReplicaProgress } from 'magic_sync_dashboard'
import 'magic_sync_dashboard/styles.css'

const overview = ref<Overview | null>(null)
const progress = ref<ReplicaProgress | null>(null)
const selected = ref<string | null>(null)
// Fetch `overview` every few seconds, and `progress` for `selected` when it
// changes, from your own endpoints.
</script>

<template>
  <SyncDashboard
    v-model:selected="selected"
    :overview="overview"
    :progress="progress"
    :names="{ '0190a1b2-…': 'Register 1' }"
    :labels="{ products: 'Products' }"
  />
</template>
```

| Prop                        | What it is                                                                                         |
| --------------------------- | -------------------------------------------------------------------------------------------------- |
| `overview`                  | `Engine::overview`'s answer; `null` while loading                                                  |
| `progress`                  | `Engine::progress`'s answer for the selected replica                                               |
| `names`, `labels`           | your names for replicas and tables: a map, or a function                                           |
| `v-model:selected`          | the replica being looked at; keep it in your URL if you like                                       |
| `locale`, `messages`        | see below                                                                                          |
| `silentAfter`, `stuckAfter` | when a replica is "not answering" (15 min) and when waiting changes need attention (10 min), in ms |

## Where one row is

"Which replicas hold this product?" belongs next to the product, in your own
listing: ask `Engine::row_progress` with its key, and show the answer with
`RowPlaces` (or `useRowPlaces`, without looks):

```vue
<RowPlaces :places="places" :names="registerNames" />
```

### Its look

Every colour and size is a CSS variable; set them on `.msd` or any ancestor:
`--msd-font`, `--msd-fg`, `--msd-muted`, `--msd-bg`, `--msd-card`,
`--msd-border`, `--msd-accent`, `--msd-accent-fg`, `--msd-ok`, `--msd-warn`,
`--msd-danger`, `--msd-ok-soft`, `--msd-warn-soft`, `--msd-danger-soft`,
`--msd-radius`, `--msd-gap`.

## The same, with your own components

A host with a design system of its own calls `useSyncDashboard` in its own
component and draws the same answers with its own parts:

```ts
import { useSyncDashboard } from 'magic_sync_dashboard'

const dash = useSyncDashboard({ overview, progress, names, labels })
dash.banner.value // { mood: 'healthy' | 'catching_up' | 'attention', title, lines }
dash.summary.value // replicas (worst first), tables, problems, counts per state
dash.progress.value // the selected replica, per table: upToDate of should
dash.describeReplica(r) // "Last answered 3 hr. ago", "2 rows set aside", …
dash.describeTable(t) // "840 rows on their way to 5 replicas"
dash.describeProblem(p) // { title, about, reason, facts }
```

Everything that does not need Vue — the types and `summarize` /
`progressOf` — is also in `magic_sync_dashboard/core`.

## Languages

English and Spanish are built in. With `vue-i18n` in the host, the dashboard
follows its global locale on its own (`vue-i18n` is not a dependency); pin
one with `locale`, and override or add strings with `messages`:

```ts
{ en: { 'mood.healthy': 'All registers are up to date' }, fr: { … } }
```

## Versions

The dashboard reads the JSON of the engine of the same version: release and
upgrade them together. The engine's tests keep a copy of that JSON in
`src/core/contract/`, and the dashboard's tests read it.

## License

MIT OR Apache-2.0.
