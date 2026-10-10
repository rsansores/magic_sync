# The dashboard

The engine answers what a dashboard asks ([Progress and
health](progress.md)); `magic_sync_dashboard`, on npm, draws it in Vue 3. In
between are your handlers: the dashboard fetches nothing, and the engine
serves no HTTP. That is on purpose — *who may see which replicas* is your
system's question, asked behind your login, and the engine only takes the
answer as a scope.

![The shop's head office: one register refusing a product, another out of reach](../images/dashboard.png)

## The handlers

> **You write** three reads, in whatever your server is built with. The
> shop's, in axum (`examples/shop/server`):

```rust
use magic_sync::status::{Overview, ReplicaProgress, RowPlace, Scope};

async fn overview(State(app): State<App>) -> Result<Json<Overview>, Failure> {
    let mut tx = app.server.begin().await?;
    Ok(Json(app.server.engine().overview(&mut tx, Scope::All, 20).await?))
}

async fn progress(State(app): State<App>, Path(register): Path<Uuid>)
    -> Result<Json<ReplicaProgress>, Failure>
{
    let mut tx = app.server.begin().await?;
    let mut all = app.server.engine().progress(&mut tx, Scope::Replicas(&[register])).await?;
    all.pop().map(Json).ok_or(Failure::NotFound)
}

async fn row(State(app): State<App>, Path((table, key)): Path<(String, String)>)
    -> Result<Json<Vec<RowPlace>>, Failure>
{
    let mut tx = app.server.begin().await?;
    Ok(Json(app.server.engine().row_progress(&mut tx, Scope::All, &table, &key).await?))
}
```

The shop has no login, so it asks over every register (`Scope::All`). A
system with one answers for the caller instead: a customer's partition
(`Scope::Partition`), or the replicas this user may see
(`Scope::Replicas(&ids)`). The answers serialize as they are; return them
untouched, and the dashboard of the same version reads them.

> **What happens:** the overview reads only what the engine records, so it
> answers in a few milliseconds for hundreds of replicas, and the page can ask
> every second or two. Progress counts what one replica should hold: ask it
> for the one being looked at, not for the fleet.

## The page

```sh
pnpm add magic_sync_dashboard
```

> **You write** the fetching, and your names for what the engine knows by
> id. The shop's (`examples/shop/web/src/App.vue`), cut down:

```vue
<script setup lang="ts">
import { SyncDashboard, type Overview, type ReplicaProgress } from 'magic_sync_dashboard'
import 'magic_sync_dashboard/styles.css'

const overview = ref<Overview | null>(null)
const progress = ref<ReplicaProgress | null>(null)
const selected = ref<string | null>(null)
setInterval(async () => {
  overview.value = await (await fetch('/api/sync/overview')).json()
  progress.value = selected.value
    ? await (await fetch(`/api/sync/progress/${selected.value}`)).json()
    : null
}, 1000)
</script>

<template>
  <SyncDashboard
    v-model:selected="selected"
    :overview="overview"
    :progress="progress"
    :names="registerNames"
    :labels="{ products: 'Products' }"
  />
</template>
```

> **What happens:** a banner says in one line where things stand — green,
> *everything is in sync*, most of the time; amber while changes are on their
> way; red when something waits for a fix — then the replicas, worst first,
> the tables, and the problems, each with its reason. Clicking a replica sets
> `selected`; you fetch its progress, and it shows table by table.

## Where one row is

"Which registers hold this product?" is asked next to the product, in your own
listing — not on the dashboard, which knows rows only by key. The shop puts a
*who holds it?* link on each product: it asks the third handler with the
product's id, and shows the answer with `RowPlaces`:

```vue
<RowPlaces :places="places" :names="registerNames" />
```

> **What happens:** each register, and where the row stands on it — holds it,
> holds an older version, on its way, letting it go, or set aside. In a system
> with many listings, the same link on each listing of a table that travels
> answers "which units hold this operator", "which hold this order".

## Its look and its words

Its look is CSS variables (`--msd-accent`, `--msd-ok`, …) you may set to your
own tokens on any ancestor, and its words are English or Spanish, following
your `vue-i18n` locale. A system with a design system of its own skips the
ready-made component and draws the same answers with its own parts, through
`useSyncDashboard`: the package's README says how.
