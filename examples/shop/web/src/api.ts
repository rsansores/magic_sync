// What the page asks the head office. The sync reads answer the engine's
// JSON as it is, so their types are the dashboard's.

import type { Listing, Overview, Progress, ReplicaState, RowProgress } from 'magic_sync_dashboard'

export interface Product {
  id: string
  name: string
  price_cents: number
}

export interface Receipt {
  id: string
  register_id: string
  quantity: number
  total_cents: number
}

export interface Register {
  id: string
  name: string
  url: string
  /** Its line is cut: the head office cannot reach it. */
  cut: boolean
}

export interface ShopState {
  products: Product[]
  receipts: Receipt[]
  registers: Register[]
}

async function ask<T>(method: string, url: string, body?: unknown): Promise<T> {
  const response = await fetch(url, {
    method,
    headers: body === undefined ? undefined : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  if (!response.ok) throw new Error(await response.text())
  return response.status === 204 || response.status === 201 ? (undefined as T) : response.json()
}

export const api = {
  state: () => ask<ShopState>('GET', '/api/state'),
  addProduct: (name: string, price_cents: number) =>
    ask<void>('POST', '/api/products', { name, price_cents }),
  editProduct: (id: string, name: string, price_cents: number) =>
    ask<void>('PUT', `/api/products/${id}`, { name, price_cents }),
  deleteProduct: (id: string) => ask<void>('DELETE', `/api/products/${id}`),
  line: (register: string) => ask<void>('POST', `/api/registers/${register}/line`),
  overview: () => ask<Overview>('GET', '/api/sync/overview'),
  listing: (state: ReplicaState | null, offset: number) =>
    ask<Listing>('GET', `/api/sync/replicas?offset=${offset}${state ? `&state=${state}` : ''}`),
  progress: (register: string) => ask<Progress>('GET', `/api/sync/progress/${register}`),
  row: (table: string, key: string) =>
    ask<RowProgress>('GET', `/api/sync/rows/${encodeURIComponent(table)}/${encodeURIComponent(key)}`),
}
