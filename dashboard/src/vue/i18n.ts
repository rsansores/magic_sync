// The dashboard's own strings, in English and Spanish.
//
// When the host app uses vue-i18n, the dashboard follows its locale with no
// wiring: it reads the global `$i18n.locale`, so vue-i18n is not a
// dependency. A host can pin a `locale`, or override any string through
// `messages`. A key with a `|` is a plural: one form, then the other.

import { computed, getCurrentInstance } from 'vue'

export type Locale = 'en' | 'es' | (string & {})
export type MessageTable = Record<string, string>
export type Messages = Partial<Record<Locale, MessageTable>>

const en: MessageTable = {
  'mood.healthy': 'Everything is in sync',
  'mood.healthy.detail': '{n} replica holds what it should. | All {n} replicas hold what they should.',
  'mood.catching_up': 'Catching up',
  'mood.catching_up.detail': '{n} of {total} replicas have changes on their way.',
  'mood.attention': 'Needs attention',
  'mood.attention.detail': '{n} problem waits for a fix. | {n} problems wait for a fix.',
  'mood.stuck': 'The oldest change waiting to be sent was made {since}.',
  'mood.silent': '{n} not answering. | {n} not answering.',
  loading: 'Loading…',
  'tab.replicas': 'Replicas',
  'tab.tables': 'Tables',
  'tab.problems': 'Problems',
  'filter.all': 'All',
  search: 'Search',
  'state.awaiting_fix': 'Needs a fix',
  'state.unreachable': 'Unreachable',
  'state.catching_up': 'Catching up',
  'state.synced': 'Synced',
  'replica.silent': 'Last answered {since}',
  'replica.pending': '{n} change on its way | {n} changes on their way',
  'replica.applying': 'Applying what it was sent',
  'replica.backlog': '{n} row of its own not at the server yet | {n} rows of its own not at the server yet',
  'replica.heard': 'Last confirmed {since}',
  'replica.never': 'Nothing confirmed yet',
  'replica.unreachable': 'Cannot be reached ({error}), tried again {until}',
  'replica.quarantined': '{n} row set aside | {n} rows set aside',
  'replica.unfiled': '{n} upload not filed | {n} uploads not filed',
  'replica.parked': '{n} change held back | {n} changes held back',
  'replica.none': 'No replica matches.',
  'table.synced': 'In sync everywhere',
  'table.pending': '{n} change waiting to be sent | {n} changes waiting to be sent',
  'table.in_flight': '{n} row on its way | {n} rows on their way',
  'table.replicas': 'to {n} replica | to {n} replicas',
  'table.quarantined': '{n} row set aside | {n} rows set aside',
  'table.unfiled': '{n} upload not filed | {n} uploads not filed',
  'table.parked': '{n} change held back | {n} changes held back',
  'problem.quarantined': 'Row set aside',
  'problem.unfiled': 'Upload not filed',
  'problem.parked': 'Change held back',
  'problem.unreachable': 'Replica unreachable',
  'problem.since': 'first seen {since}',
  'problem.attempts': '{n} attempt | {n} attempts',
  'problem.retry': 'tried again {until}',
  'problem.more': 'And {n} more.',
  'problem.none': 'Nothing waits for a fix.',
  'problem.contract':
    'Each of these waits for the network to come back or for a fix to be deployed; it then gets through by itself. No data is repaired by hand.',
  'detail.back': 'All replicas',
  'detail.rows': '{done} of {of} rows',
  'detail.loading': 'Counting…',
  'detail.in_flight': '{n} on its way | {n} on their way',
  'detail.to_remove': '{n} leaving | {n} leaving',
  'detail.quarantined': '{n} set aside | {n} set aside',
  'detail.outdated': '{n} outdated | {n} outdated',
  'row.title': 'Where is a row?',
  'row.table': 'Table',
  'row.key': 'Key',
  'row.find': 'Find',
  'row.nowhere': 'No replica holds it or should.',
  'row.holds': 'Holds it',
  'row.outdated': 'Holds an older version',
  'row.on_its_way': 'On its way',
  'row.missing': 'Should hold it, does not yet',
  'row.leaving': 'Letting it go',
  'row.quarantined': 'Set aside',
}

const es: MessageTable = {
  'mood.healthy': 'Todo está sincronizado',
  'mood.healthy.detail': '{n} réplica tiene lo que debe. | Las {n} réplicas tienen lo que deben.',
  'mood.catching_up': 'Poniéndose al día',
  'mood.catching_up.detail': '{n} de {total} réplicas tienen cambios en camino.',
  'mood.attention': 'Requiere atención',
  'mood.attention.detail': '{n} problema espera una corrección. | {n} problemas esperan una corrección.',
  'mood.stuck': 'El cambio más antiguo por enviar se hizo {since}.',
  'mood.silent': '{n} no responde. | {n} no responden.',
  loading: 'Cargando…',
  'tab.replicas': 'Réplicas',
  'tab.tables': 'Tablas',
  'tab.problems': 'Problemas',
  'filter.all': 'Todas',
  search: 'Buscar',
  'state.awaiting_fix': 'Requiere corrección',
  'state.unreachable': 'Inalcanzable',
  'state.catching_up': 'Poniéndose al día',
  'state.synced': 'Sincronizada',
  'replica.silent': 'Última respuesta {since}',
  'replica.pending': '{n} cambio en camino | {n} cambios en camino',
  'replica.applying': 'Aplicando lo que se le envió',
  'replica.backlog':
    '{n} registro propio aún no llega al servidor | {n} registros propios aún no llegan al servidor',
  'replica.heard': 'Última confirmación {since}',
  'replica.never': 'Aún no confirma nada',
  'replica.unreachable': 'No se puede alcanzar ({error}); se reintenta {until}',
  'replica.quarantined': '{n} registro apartado | {n} registros apartados',
  'replica.unfiled': '{n} envío sin archivar | {n} envíos sin archivar',
  'replica.parked': '{n} cambio detenido | {n} cambios detenidos',
  'replica.none': 'Ninguna réplica coincide.',
  'table.synced': 'Sincronizada en todas',
  'table.pending': '{n} cambio por enviar | {n} cambios por enviar',
  'table.in_flight': '{n} registro en camino | {n} registros en camino',
  'table.replicas': 'a {n} réplica | a {n} réplicas',
  'table.quarantined': '{n} registro apartado | {n} registros apartados',
  'table.unfiled': '{n} envío sin archivar | {n} envíos sin archivar',
  'table.parked': '{n} cambio detenido | {n} cambios detenidos',
  'problem.quarantined': 'Registro apartado',
  'problem.unfiled': 'Envío sin archivar',
  'problem.parked': 'Cambio detenido',
  'problem.unreachable': 'Réplica inalcanzable',
  'problem.since': 'desde {since}',
  'problem.attempts': '{n} intento | {n} intentos',
  'problem.retry': 'se reintenta {until}',
  'problem.more': 'Y {n} más.',
  'problem.none': 'Nada espera una corrección.',
  'problem.contract':
    'Cada uno espera a que vuelva la red o a que se despliegue una corrección; después pasa solo. Ningún dato se repara a mano.',
  'detail.back': 'Todas las réplicas',
  'detail.rows': '{done} de {of} registros',
  'detail.loading': 'Contando…',
  'detail.in_flight': '{n} en camino | {n} en camino',
  'detail.to_remove': '{n} por quitar | {n} por quitar',
  'detail.quarantined': '{n} apartado | {n} apartados',
  'detail.outdated': '{n} desactualizado | {n} desactualizados',
  'row.title': '¿Dónde está un registro?',
  'row.table': 'Tabla',
  'row.key': 'Clave',
  'row.find': 'Buscar',
  'row.nowhere': 'Ninguna réplica lo tiene ni debe tenerlo.',
  'row.holds': 'Lo tiene',
  'row.outdated': 'Tiene una versión anterior',
  'row.on_its_way': 'En camino',
  'row.missing': 'Debe tenerlo, aún no',
  'row.leaving': 'Lo está soltando',
  'row.quarantined': 'Apartado',
}

export const builtinMessages: Messages = { en, es }

export type TFn = (key: string, params?: Record<string, unknown>) => string

function interpolate(s: string, locale: string, params?: Record<string, unknown>): string {
  if (!params) return s
  const show = (v: unknown) => (typeof v === 'number' ? new Intl.NumberFormat(locale).format(v) : String(v))
  return s.replace(/\{(\w+)\}/g, (_, k: string) => (k in params ? show(params[k]) : `{${k}}`))
}

function mergeMessages(over?: Messages): Messages {
  if (!over) return builtinMessages
  const merged: Messages = { ...builtinMessages }
  for (const locale of Object.keys(over))
    merged[locale] = { ...(builtinMessages[locale] ?? {}), ...over[locale] }
  return merged
}

/** "es-MX" → "es" when only the language has a table. */
function tableFor(messages: Messages, locale: string): MessageTable | undefined {
  return messages[locale] ?? messages[locale.split('-')[0]!]
}

/** Translation for one dashboard: `params.n` picks a plural's form, and
 * every number is written in the locale's way. */
export function createDashboardI18n(
  getLocale: () => string | undefined,
  getMessages: () => Messages | undefined,
) {
  const instance = getCurrentInstance()
  // vue-i18n (any mode) exposes the global composer as `$i18n`, with a
  // `locale` that is a ref or a string.
  const hostI18n = instance?.appContext.config.globalProperties.$i18n as { locale?: unknown } | undefined
  const hostLocale = () => {
    const l = hostI18n?.locale as { value?: unknown } | string | undefined
    return typeof l === 'string' ? l : typeof l?.value === 'string' ? l.value : undefined
  }
  const active = computed(() => getLocale() ?? hostLocale() ?? 'en')
  const messages = computed(() => mergeMessages(getMessages()))
  const t: TFn = (key, params) => {
    const table = messages.value
    const s = tableFor(table, active.value)?.[key] ?? table.en?.[key] ?? key
    const forms = s.split(' | ')
    const n = params?.n
    const form = forms.length > 1 && typeof n === 'number' && n !== 1 ? forms[1]! : forms[0]!
    return interpolate(form, active.value, params)
  }
  return { t, locale: () => active.value }
}
