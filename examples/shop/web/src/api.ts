// What the page asks the head office. The sync reads answer the engine's
// JSON as it is, so their types are the dashboard's.

import type { Listing, Overview, Progress, ReplicaState, RowProgress, WishState } from 'magic_sync_dashboard'

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

/** What a register prints under its receipts, as it last said. */
export interface Footer {
  register_id: string
  text: string
  changed_at: string
}

/** A register's footer as the head office knows it: what the register last
 * said, and the head office's wish of it, if any, with where it stands. */
export interface FooterTwin extends Footer {
  wish: { text: string; state: WishState; reason: string | null } | null
}

export interface ShopState {
  products: Product[]
  receipts: Receipt[]
  registers: Register[]
  footers: FooterTwin[]
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
  wishFooter: (seen: Footer, text: string) =>
    ask<void>('POST', `/api/registers/${seen.register_id}/footer`, { seen, text }),
  withdrawFooter: (register: string) => ask<void>('DELETE', `/api/registers/${register}/footer/wish`),
  row: (table: string, key: string) =>
    ask<RowProgress>('GET', `/api/sync/rows/${encodeURIComponent(table)}/${encodeURIComponent(key)}`),
}
