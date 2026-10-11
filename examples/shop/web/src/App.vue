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
} from 'magic_sync_dashboard'
import { api, type Product, type ShopState } from './api'

const state = ref<ShopState | null>(null)
const overview = ref<Overview | null>(null)
const selected = ref<string | null>(null)
const progress = ref<Progress | null>(null)
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
    ;[state.value, overview.value, listing.value] = await Promise.all([
      api.state(),
      api.overview(),
      api.listing(filter.value, offset.value),
    ])
    progress.value = selected.value ? await api.progress(selected.value) : null
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
    <p>Edit the catalog here. Every register holds it, and keeps working when it is unplugged.</p>
    <p v-if="error" class="error">{{ error }}</p>
  </header>
  <main>
    <div class="column">
      <section>
        <h2>Products</h2>
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
        <!-- A register unplugged is "not answering" after 30 s, not 15 min: it is a demo. -->
        <SyncDashboard
          v-model:selected="selected"
          v-model:filter="filter"
          v-model:offset="offset"
          :overview="overview"
          :listing="listing"
          :progress="progress"
          :page-size="20"
          :names="names"
          :labels="labels"
          :silent-after="30_000"
        />
        <!-- ANCHOR_END: dashboard -->
      </section>
      <section>
        <h2>Registers</h2>
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
                {{ r.cut ? 'mend the line' : 'cut the line' }}
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
</style>
