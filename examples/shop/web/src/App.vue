<script setup lang="ts">
// The head office: the catalog, the receipts the registers sent, and the
// sync dashboard over the registers, each register's own page beside it.
// ANCHOR: script
import { computed, onUnmounted, ref, watch } from 'vue'
import {
  RowPlaces,
  SyncDashboard,
  type Listing,
  type Overview,
  type Progress,
  type ReplicaState,
  type RowProgress,
  type States,
} from 'magic_sync_dashboard'
import { api, type FooterTwin, type Product, type ShopState } from './api'

const state = ref<ShopState | null>(null)
const overview = ref<Overview | null>(null)
// Every register's state, for a tile of each.
const states = ref<States | null>(null)
const selected = ref<string | null>(null)
// The register looked at: where it is on each table, and its own row.
const progress = ref<Progress | null>(null)
const replica = ref<Listing | null>(null)
// The replicas listed, a page at a time, in the state picked.
const filter = ref<ReplicaState | null>(null)
const offset = ref(0)
const listing = ref<Listing | null>(null)
// The product whose registers are shown, and where it is on each.
const holding = ref<string | null>(null)
const places = ref<RowProgress | null>(null)
const error = ref('')

// The dashboard's names for what the engine knows by id.
const names = computed(() => Object.fromEntries((state.value?.registers ?? []).map((r) => [r.id, r.name])))
const labels = { products: 'Products', receipts: 'Receipts' }

async function refresh() {
  try {
    ;[state.value, overview.value, states.value, listing.value] = await Promise.all([
      api.state(),
      api.overview(),
      api.states(),
      api.listing(filter.value, offset.value),
    ])
    ;[progress.value, replica.value] = selected.value
      ? await Promise.all([api.progress(selected.value), api.replica(selected.value)])
      : [null, null]
    places.value = holding.value ? await api.row('products', holding.value) : null
    error.value = ''
  } catch (e) {
    error.value = String(e)
  }
}
refresh()
const timer = setInterval(refresh, 1000)
onUnmounted(() => clearInterval(timer))
watch([selected, holding, filter, offset], refresh)
// ANCHOR_END: script

// Every action says what went wrong, rather than nothing.
async function act(action: () => Promise<void>) {
  try {
    await action()
    await refresh()
  } catch (e) {
    error.value = String(e)
  }
}
function cutLine(register: string) {
  return act(() => api.line(register))
}
function whoHolds(p: Product) {
  holding.value = holding.value === p.id ? null : p.id
}

const money = (cents: number) => (cents / 100).toFixed(2)
const newName = ref('')
const newPrice = ref('')
function add() {
  return act(async () => {
    await api.addProduct(newName.value, Math.round(Number(newPrice.value) * 100))
    newName.value = ''
    newPrice.value = ''
  })
}
const editing = ref<{ id: string; name: string; price: string } | null>(null)
function edit(p: Product) {
  editing.value = { id: p.id, name: p.name, price: money(p.price_cents) }
}
function save() {
  const e = editing.value!
  return act(async () => {
    await api.editProduct(e.id, e.name, Math.round(Number(e.price) * 100))
    editing.value = null
  })
}
function remove(p: Product) {
  return act(() => api.deleteProduct(p.id))
}

// ANCHOR: footer
const footerOf = (register: string) => state.value?.footers.find((f) => f.register_id === register)
// The footer each register's form was opened on: the wish is based on it.
const asking = ref<{ seen: FooterTwin; text: string } | null>(null)
function ask(seen: FooterTwin) {
  asking.value = { seen, text: seen.wish && open(seen) ? seen.wish.text : seen.text }
}
function wish() {
  const { seen, text } = asking.value!
  return act(async () => {
    await api.wishFooter(
      { register_id: seen.register_id, text: seen.text, changed_at: seen.changed_at },
      text,
    )
    asking.value = null
  })
}
function dismiss(f: FooterTwin) {
  return act(() => api.withdrawFooter(f.register_id))
}
// Each register beside its footer, once it said it.
const twins = computed(() => (state.value?.registers ?? []).map((r) => ({ r, f: footerOf(r.id) })))
// A wish not carried out yet: on its way, or waiting for the register.
const open = (f: FooterTwin) => f.wish?.state === 'pending' || f.wish?.state === 'waiting'
// A wish the register did not carry out, and said why — unless it prints
// that footer anyway.
const declined = (f: FooterTwin) =>
  (f.wish?.state === 'refused' || f.wish?.state === 'superseded') && f.wish.text !== f.text
// ANCHOR_END: footer

// A register's page, on the host this page was opened with.
function pageOf(url: string) {
  const page = new URL(url)
  page.hostname = location.hostname
  return page.toString()
}
</script>

<template>
  <header>
    <h1>The head office</h1>
    <p>Edit the catalog here. Every register holds it, and keeps working when it is offline.</p>
    <p v-if="error" class="error">{{ error }}</p>
  </header>
  <main>
    <div class="column">
      <section>
        <h2>Products</h2>
        <p class="legend">
          Add, edit and delete products here. The catalog is written at the head office and
          <b>broadcast to every register</b> (<code>mirror = "catalog"</code>,
          <code>sync = "broadcast"</code>): each register holds all of it, and keeps selling from it while it
          is offline.
        </p>
        <form class="add" @submit.prevent="add">
          <input v-model="newName" placeholder="Name" required />
          <input v-model="newPrice" placeholder="Price" type="number" step="0.01" required />
          <button>Add</button>
        </form>
        <table>
          <tr>
            <th>Name</th>
            <th>Price</th>
            <th></th>
          </tr>
          <template v-for="p in state?.products" :key="p.id">
            <tr>
              <template v-if="editing?.id === p.id">
                <td><input v-model="editing.name" /></td>
                <td><input v-model="editing.price" type="number" step="0.01" /></td>
                <td><button class="link" @click="save">save</button></td>
              </template>
              <template v-else>
                <td>{{ p.name }}</td>
                <td>{{ money(p.price_cents) }}</td>
                <td>
                  <button class="link" @click="edit(p)">edit</button>
                  <button class="link" @click="remove(p)">delete</button>
                  <button class="link" @click="whoHolds(p)">who holds it?</button>
                </td>
              </template>
            </tr>
            <!-- Where this product is, register by register: asked next to it. -->
            <!-- ANCHOR: row -->
            <tr v-if="holding === p.id">
              <td colspan="3"><RowPlaces :places="places" :names="names" /></td>
            </tr>
            <!-- ANCHOR_END: row -->
          </template>
        </table>
      </section>
      <section>
        <h2>Receipts filed here</h2>
        <table>
          <tr>
            <th>Register</th>
            <th>Qty</th>
            <th>Total</th>
          </tr>
          <tr v-for="r in state?.receipts" :key="r.id">
            <td>{{ names[r.register_id] ?? r.register_id.slice(-12) }}</td>
            <td>{{ r.quantity }}</td>
            <td>{{ money(r.total_cents) }}</td>
          </tr>
        </table>
      </section>
    </div>
    <div class="column">
      <section>
        <h2>Sync</h2>
        <!-- ANCHOR: dashboard -->
        <!-- It is a demo: a register is "not answering" after 30 s, not 15 min. -->
        <SyncDashboard
          v-model:selected="selected"
          v-model:filter="filter"
          v-model:offset="offset"
          :overview="overview"
          :states="states"
          :listing="listing"
          :progress="progress"
          :replica="replica"
          :page-size="20"
          :names="names"
          :labels="labels"
          :unanswering-after="30_000"
        />
        <!-- ANCHOR_END: dashboard -->
      </section>
      <section>
        <h2>Remote update of footers through wishes</h2>
        <p class="legend">
          Each register owns the footer it prints under its receipts (<code>mirror = "setting"</code>): it may
          change it itself, and it tells the head office what it prints. The head office
          <b>wishes</b> another; the register carries the wish out through its printer and answers. The
          strategy is <code>apply = "if_unchanged"</code>: a wish based on a footer the register has changed
          since is not applied — the register's own change stands. See
          <a
            href="https://rsansores.github.io/magic_sync/guide/settings.html#strategies-who-wins"
            target="_blank"
          >
            wish strategies</a
          >
          in the book.
        </p>
        <ul class="twins">
          <li
            v-for="{ r, f } in twins"
            :key="r.id"
            :class="['twin', { wished: f && open(f), declined: f && declined(f) }]"
          >
            <span class="who">{{ r.name }}</span>
            <span v-if="!f" class="state">has not said its footer yet</span>
            <form v-else-if="asking?.seen.register_id === r.id" class="wish" @submit.prevent="wish">
              <input v-model="asking.text" />
              <button>Wish</button>
              <button type="button" class="link" @click="asking = null">cancel</button>
            </form>
            <template v-else>
              <span class="value">
                <template v-if="open(f)">
                  <s>“{{ f.text }}”</s> → <b>“{{ f.wish!.text }}”</b>
                </template>
                <template v-else>“{{ f.text }}”</template>
              </span>
              <span class="state">
                <template v-if="f.wish?.state === 'pending'">wished · on its way to the register</template>
                <template v-else-if="f.wish?.state === 'waiting'"
                  >wished · waiting: {{ f.wish.reason }}</template
                >
                <template v-else-if="declined(f) && f.wish?.state === 'refused'">
                  wish refused: {{ f.wish.reason }}
                </template>
                <template v-else-if="declined(f) && f.wish?.state === 'superseded'">
                  “{{ f.wish.text }}” not applied: the register changed its footer first
                </template>
                <template v-else>in sync · what the register prints</template>
              </span>
              <span class="actions">
                <button class="link" @click="ask(f)">change</button>
                <button v-if="open(f) || declined(f)" class="link" @click="dismiss(f)">
                  {{ open(f) ? 'withdraw' : 'dismiss' }}
                </button>
              </span>
            </template>
          </li>
        </ul>
      </section>
      <section>
        <h2>Registers</h2>
        <p class="legend">
          Each register's own page, as its cashier sees it. To see the engine at work, break things:
          <b>Go offline</b> (on a register) — it stops talking to the head office, keeps selling, and its
          receipts wait in its outbox until it is back online, then go up in order; the head office stops
          hearing from it, and after three of its 5 s keepalives the dashboard shows it <b>silent</b> — as it
          does a register you stop; <b>break the network</b> (here) — every send from the head office to it
          fails, the dashboard shows it unreachable, and the head office retries, waiting longer each time;
          nothing it sends is lost, because it is recorded before it is sent; <b>printer off</b> (on a
          register) — not sync at all: the hardware a footer wish is carried out on, so a wish waits, and
          applies once the printer is back on.
        </p>
        <p v-if="state && state.registers.length === 0" class="note">
          No register yet. Start one: <code>cargo run -- --name "Register 1" --port 8081</code> in
          <code>examples/shop/register</code>.
        </p>
        <div class="registers">
          <figure v-for="r in state?.registers" :key="r.id">
            <figcaption>
              {{ r.name }}
              <!-- A demo fault: the head office cannot reach it. -->
              <button class="link" @click="cutLine(r.id)">
                {{ r.cut ? 'restore the network' : 'break the network' }}
              </button>
            </figcaption>
            <iframe :src="pageOf(r.url)" :title="r.name" />
          </figure>
        </div>
      </section>
    </div>
  </main>
</template>

<style>
body {
  margin: 0;
  font:
    15px/1.45 system-ui,
    sans-serif;
  color: #1d2433;
  background: #f6f7fb;
}
header {
  padding: 18px 28px;
  background: white;
  border-bottom: 1px solid #e3e6ee;
}
h1 {
  margin: 0;
  font-size: 20px;
}
header p {
  margin: 4px 0 0;
  color: #6b7385;
}
.error {
  color: #e03131 !important;
}
main {
  display: grid;
  grid-template-columns: 380px 1fr;
  gap: 20px;
  padding: 20px 28px;
}
.column {
  display: flex;
  flex-direction: column;
  gap: 20px;
  min-width: 0;
}
section {
  background: white;
  border: 1px solid #e3e6ee;
  border-radius: 10px;
  padding: 16px;
}
h2 {
  font-size: 15px;
  margin: 0 0 10px;
}
table {
  width: 100%;
  border-collapse: collapse;
}
td,
th {
  text-align: left;
  padding: 6px 4px;
  border-bottom: 1px solid #e3e6ee;
  font-size: 14px;
}
th {
  color: #6b7385;
  font-weight: 500;
}
input {
  font: inherit;
  padding: 6px 8px;
  border: 1px solid #e3e6ee;
  border-radius: 6px;
  width: 100%;
}
button {
  font: inherit;
  padding: 6px 12px;
  border: 0;
  border-radius: 6px;
  background: #3b5bdb;
  color: white;
  cursor: pointer;
}
button.link {
  background: none;
  color: #3b5bdb;
  padding: 0 4px;
}
.add {
  display: grid;
  grid-template-columns: 1fr 90px auto;
  gap: 6px;
  margin-bottom: 10px;
}
.registers {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(330px, 1fr));
  gap: 16px;
}
figure {
  margin: 0;
}
figcaption {
  font-weight: 600;
  font-size: 14px;
}
iframe {
  width: 100%;
  height: 360px;
  border: 1px solid #e3e6ee;
  border-radius: 8px;
  margin-top: 8px;
}
.note {
  color: #6b7385;
  font-size: 13px;
}
.wish {
  display: grid;
  grid-template-columns: 1fr auto auto;
  gap: 6px;
  grid-column: 2 / 5;
}
.legend {
  color: #6b7385;
  font-size: 13px;
  margin: 0 0 12px;
}
.twins {
  list-style: none;
  margin: 0;
  padding: 0;
}
.twin {
  display: grid;
  grid-template-columns: 110px 1fr auto auto;
  gap: 12px;
  align-items: center;
  padding: 8px 10px;
  margin-bottom: 6px;
  border-left: 4px solid #2f9e44;
  background: #ebfbee;
  border-radius: 6px;
  font-size: 14px;
}
.twin.wished {
  border-left-color: #f08c00;
  background: #fff4e6;
}
.twin.declined {
  border-left-color: #e03131;
  background: #fff5f5;
}
.twin .who {
  font-weight: 600;
}
.twin .state {
  color: #6b7385;
  font-size: 13px;
}
.twin.wished b {
  color: #d9480f;
}
.twin s {
  color: #868e96;
}
</style>
