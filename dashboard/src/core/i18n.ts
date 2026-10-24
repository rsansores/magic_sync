// The dashboard's own strings, in English and Spanish, and how one is said:
// every message is a whole sentence (or a whole label), and a count picks
// its form by the locale's plural rules.

import { count, normalizeLocale, pluralRules } from './format.js'

/** A message that depends on a count `n`: one form per plural category the
 * locale uses (`Intl.PluralRules`), `other` always. */
export type Plural = { [K in Intl.LDMLPluralRule]?: string } & { other: string }
export type Message = string | Plural
export type MessageTable = Record<string, Message>
export type Locale = 'en' | 'es' | (string & {})
export type Messages = Partial<Record<Locale, MessageTable>>
/** A message by key: `params.n` picks a plural's form, and every number is
 * written the locale's way. */
export type Translate = (key: string, params?: Record<string, string | number>) => string

const en: MessageTable = {
  'mood.healthy': 'Everything is in sync',
  'mood.healthy.detail': {
    one: 'The replica holds what it should.',
    other: 'All {n} replicas hold what they should.',
  },
  'mood.catching_up': 'Catching up',
  'mood.catching_up.detail': {
    one: '{n} of {total} replicas has changes on their way.',
    other: '{n} of {total} replicas have changes on their way.',
  },
  'mood.unreachable': {
    one: '{n} replica cannot be reached; it gets what it is owed once it can.',
    other: '{n} replicas cannot be reached; each gets what it is owed once it can.',
  },
  'mood.silent': {
    one: '{n} replica has said nothing for longer than it said it would.',
    other: '{n} replicas have said nothing for longer than they said they would.',
  },
  'mood.attention': 'Needs attention',
  'mood.attention.detail': {
    one: '{n} problem waits for a fix.',
    other: '{n} problems wait for a fix.',
  },
  'mood.stuck': 'The oldest change waiting to be sent was made {since}.',
  'mood.unanswering': {
    one: '{n} replica listed is not answering.',
    other: '{n} replicas listed are not answering.',
  },
  'mood.empty': 'No replicas yet',
  'mood.empty.detail': 'A replica shows here once it is live.',
  loading: 'Loading…',
  'contract.mismatch':
    'This dashboard reads the engine’s answers of version {ours}, and the server answered version {theirs}. Upgrade the dashboard and the engine together.',
  'contract.unreadable': 'The server’s answer does not read: its time is not a time.',
  tabs: 'Sync status',
  'tab.replicas': 'Replicas',
  'tab.tables': 'Data',
  'tab.problems': 'Problems',
  'filter.label': 'Show replicas',
  'filter.all': 'All',
  'state.awaiting_fix': 'Needs a fix',
  'state.unreachable': 'Unreachable',
  'state.silent': 'Silent',
  'state.catching_up': 'Catching up',
  'state.synced': 'Synced',
  'state.unknown': 'Unknown ({state})',
  'replicas.worst': {
    one: 'The replica that needs attention most, of {total}.',
    other: 'The {n} replicas that need attention most, of {total}.',
  },
  'replicas.page': '{from}–{to} of {total}',
  'replicas.previous': 'Previous',
  'replicas.next': 'Next',
  'replica.unanswering': 'Last answered {since}.',
  'replica.pending': {
    one: '{n} change on its way.',
    other: '{n} changes on their way.',
  },
  'replica.owed': {
    one: '{n} message waits to be sent to it.',
    other: '{n} messages wait to be sent to it.',
  },
  'replica.applying': 'Applying what it was sent.',
  'replica.backlog': {
    one: '{n} row of its own is not at the server yet.',
    other: '{n} rows of its own are not at the server yet.',
  },
  'replica.heard': 'Last confirmed {since}.',
  'replica.never': 'Nothing confirmed yet.',
  'replica.unreachable': 'Cannot be reached.',
  'replica.silent': 'Last heard {since}.',
  'replica.retry': 'Tried again {until}.',
  'replica.quarantined': {
    one: '{n} row set aside.',
    other: '{n} rows set aside.',
  },
  'replica.unfiled': {
    one: '{n} row it sent is not filed.',
    other: '{n} rows it sent are not filed.',
  },
  'replica.parked': {
    one: '{n} change held back.',
    other: '{n} changes held back.',
  },
  'replica.none': 'No replica matches.',
  'table.synced': 'In sync everywhere.',
  'table.pending': {
    one: '{n} change waiting to be sent.',
    other: '{n} changes waiting to be sent.',
  },
  'table.in_flight': {
    one: '{n} row on its way.',
    other: '{n} rows on their way.',
  },
  'table.replicas': {
    one: '{n} replica is behind on it.',
    other: '{n} replicas are behind on it.',
  },
  'table.quarantined': {
    one: '{n} row set aside.',
    other: '{n} rows set aside.',
  },
  'table.unfiled': {
    one: '{n} row sent up is not filed.',
    other: '{n} rows sent up are not filed.',
  },
  'table.parked': {
    one: '{n} change held back.',
    other: '{n} changes held back.',
  },
  'problem.quarantined': 'Row set aside',
  'problem.unfiled': 'Row sent up, not filed',
  'problem.parked': 'Change held back',
  'problem.unreachable': 'Replica unreachable',
  'problem.since': 'First seen {since}.',
  'problem.attempts': {
    one: '{n} attempt.',
    other: '{n} attempts.',
  },
  'problem.retry': 'Tried again {until}.',
  'problem.more': {
    one: 'And {n} more.',
    other: 'And {n} more.',
  },
  'problem.none': 'Nothing waits for a fix.',
  'problem.contract':
    'Each of these waits for the network to come back or for a fix to be deployed; it then gets through by itself. No data is repaired by hand.',
  'detail.back': 'All replicas',
  'detail.rows': '{done} of {of} rows',
  'detail.loading': 'Counting…',
  'detail.in_flight': {
    one: '{n} on its way.',
    other: '{n} on their way.',
  },
  'detail.to_remove': {
    one: '{n} leaving.',
    other: '{n} leaving.',
  },
  'detail.quarantined': {
    one: '{n} set aside.',
    other: '{n} set aside.',
  },
  'detail.outdated': {
    one: '{n} outdated.',
    other: '{n} outdated.',
  },
  'replica.wishes.pending': {
    one: '{n} setting to carry out.',
    other: '{n} settings to carry out.',
  },
  'replica.wishes.waiting': {
    one: '{n} setting waits until it can be carried out.',
    other: '{n} settings wait until they can be carried out.',
  },
  'replica.wishes.refused': {
    one: 'It refused {n} setting.',
    other: 'It refused {n} settings.',
  },
  'replica.wishes.superseded': {
    one: 'It changed {n} setting before the wish arrived.',
    other: 'It changed {n} settings before the wish arrived.',
  },
  'wish.loading': 'Looking…',
  'wish.none': 'Nothing wished.',
  'wish.pending': 'On its way',
  'wish.waiting': 'Waiting',
  'wish.applied': 'Applied',
  'wish.refused': 'Refused',
  'wish.superseded': 'Changed there first',
  'wish.unknown': 'Unknown ({state})',
  'table.sync_wishes': 'Wishes',
  'row.loading': 'Looking…',
  'row.nowhere': 'No replica holds it.',
  'row.holds': 'Holds it',
  'row.outdated': 'Holds an older version',
  'row.on_its_way': 'On its way',
  'row.missing': 'Should hold it, does not yet',
  'row.leaving': 'Letting it go',
  'row.quarantined': 'Set aside',
  'row.unknown': 'Unknown ({state})',
  'check.sync': 'Sync',
  'check.sync.heard': 'Last heard {since}',
  'check.sync.applying': 'Applying changes · last heard {since}',
  'check.sync.uploading': 'Rows of its own wait to go up · last heard {since}',
  'check.sync.set_aside': {
    one: '{n} row of its own cannot go up as it is',
    other: '{n} rows of its own cannot go up as they are',
  },
  'check.sync.silent': 'Has said nothing for {for}',
  'check.sync.unreachable': 'Cannot be reached',
  'check.sync.retry': 'Cannot be reached · tried again {until}',
  'check.sync.fix': {
    one: '{n} change waits for a fix',
    other: '{n} changes wait for a fix',
  },
  'check.sync.never': 'Nothing heard yet',
  'line.unfiled': {
    one: '{n} row it sent could not be filed',
    other: '{n} rows it sent could not be filed',
  },
  'line.quarantined': {
    one: '{n} row set aside',
    other: '{n} rows set aside',
  },
  'line.parked': {
    one: '{n} change held back',
    other: '{n} changes held back',
  },
  'line.unreachable': 'Cannot be reached',
  'line.retry': 'Cannot be reached · tried again {until}',
  'line.silent': 'Last heard {since}',
  'line.moving': {
    one: '{n} change on its way',
    other: '{n} changes on their way',
  },
  'line.applying': 'Applying what it was sent',
  'line.uploading': {
    one: '{n} row of its own waits to go up',
    other: '{n} rows of its own wait to go up',
  },
  'line.set_aside': {
    one: '{n} row of its own cannot go up as it is',
    other: '{n} rows of its own cannot go up as they are',
  },
  'line.heard': 'Last heard {since}',
  'line.never': 'Nothing heard yet',
  'flow.live': 'Live · last {since}',
  'flow.stalled': 'Last {since}',
  'flow.never': 'Nothing filed yet',
  'settled.tables': {
    one: '{n} more table up to date',
    other: '{n} more tables up to date',
  },
  'settled.groups': {
    one: '{n} more group up to date',
    other: '{n} more groups up to date',
  },
  'group.waiting': {
    one: '{n} change on its way',
    other: '{n} changes on their way',
  },
  'chip.to_send': '{n} to send',
  'chip.to_confirm': '{n} to confirm',
  'chip.set_aside': {
    one: '{n} set aside',
    other: '{n} set aside',
  },
  'chip.oldest': 'oldest {for}',
  'detail.heard': 'Last heard {since}',
  'detail.never_heard': 'Nothing heard yet',
  'detail.uploading': 'uploading live',
  'detail.not_uploading': 'uploading stalled',
  'detail.to_send': 'to send',
  'detail.to_confirm': 'to confirm',
  'detail.set_aside': 'set aside',
  'detail.down': 'To the replica',
  'detail.up': 'From the replica',
  'detail.checks': 'Checks',
  'detail.problem': 'Problem',
  'grid.title': 'Replicas',
  'grid.order': 'worst first',
  'grid.by': 'Group by',
  'grid.all': 'All',
  'grid.trail': 'Where',
  'grid.nowhere': 'No place',
  'grid.trouble': '{n} of {total}',
  'grid.tile': '{name}: {state}',
  'grid.legend': 'What each colour means',
  'attention.title': 'Need attention',
  'attention.none': 'Nothing needs attention.',
}

const es: MessageTable = {
  'mood.healthy': 'Todo está sincronizado',
  'mood.healthy.detail': {
    one: 'La réplica tiene lo que debe.',
    other: 'Las {n} réplicas tienen lo que deben.',
  },
  'mood.catching_up': 'Poniéndose al día',
  'mood.catching_up.detail': {
    one: '{n} de {total} réplicas tiene cambios en camino.',
    other: '{n} de {total} réplicas tienen cambios en camino.',
  },
  'mood.unreachable': {
    one: '{n} réplica no se puede alcanzar; recibe lo que se le debe en cuanto se pueda.',
    other: '{n} réplicas no se pueden alcanzar; cada una recibe lo que se le debe en cuanto se pueda.',
  },
  'mood.silent': {
    one: '{n} réplica lleva más tiempo sin señales del que dijo.',
    other: '{n} réplicas llevan más tiempo sin señales del que dijeron.',
  },
  'mood.attention': 'Requiere atención',
  'mood.attention.detail': {
    one: '{n} problema espera una corrección.',
    other: '{n} problemas esperan una corrección.',
  },
  'mood.stuck': 'El cambio más antiguo por enviar se hizo {since}.',
  'mood.unanswering': {
    one: '{n} réplica de la lista no responde.',
    other: '{n} réplicas de la lista no responden.',
  },
  'mood.empty': 'Aún no hay réplicas',
  'mood.empty.detail': 'Una réplica aparece aquí en cuanto está activa.',
  loading: 'Cargando…',
  'contract.mismatch':
    'Este tablero lee las respuestas del motor de la versión {ours}, y el servidor respondió la versión {theirs}. Actualicen el tablero y el motor juntos.',
  'contract.unreadable': 'La respuesta del servidor no se puede leer: su hora no es una hora.',
  tabs: 'Estado de la sincronización',
  'tab.replicas': 'Réplicas',
  'tab.tables': 'Datos',
  'tab.problems': 'Problemas',
  'filter.label': 'Mostrar réplicas',
  'filter.all': 'Todas',
  'state.awaiting_fix': 'Requiere corrección',
  'state.unreachable': 'Inalcanzable',
  'state.silent': 'Sin señales',
  'state.catching_up': 'Poniéndose al día',
  'state.synced': 'Sincronizada',
  'state.unknown': 'Desconocido ({state})',
  'replicas.worst': {
    one: 'La réplica que más requiere atención, de {total}.',
    other: 'Las {n} réplicas que más requieren atención, de {total}.',
  },
  'replicas.page': '{from}–{to} de {total}',
  'replicas.previous': 'Anterior',
  'replicas.next': 'Siguiente',
  'replica.unanswering': 'Última respuesta {since}.',
  'replica.pending': {
    one: '{n} cambio en camino.',
    other: '{n} cambios en camino.',
  },
  'replica.owed': {
    one: '{n} mensaje espera enviársele.',
    other: '{n} mensajes esperan enviársele.',
  },
  'replica.applying': 'Aplicando lo que se le envió.',
  'replica.backlog': {
    one: '{n} registro propio aún no llega al servidor.',
    other: '{n} registros propios aún no llegan al servidor.',
  },
  'replica.heard': 'Última confirmación {since}.',
  'replica.never': 'Aún no confirma nada.',
  'replica.unreachable': 'No se puede alcanzar.',
  'replica.silent': 'Última señal {since}.',
  'replica.retry': 'Se reintenta {until}.',
  'replica.quarantined': {
    one: '{n} registro apartado.',
    other: '{n} registros apartados.',
  },
  'replica.unfiled': {
    one: '{n} registro que envió no está archivado.',
    other: '{n} registros que envió no están archivados.',
  },
  'replica.parked': {
    one: '{n} cambio detenido.',
    other: '{n} cambios detenidos.',
  },
  'replica.none': 'Ninguna réplica coincide.',
  'table.synced': 'Sincronizada en todas.',
  'table.pending': {
    one: '{n} cambio por enviar.',
    other: '{n} cambios por enviar.',
  },
  'table.in_flight': {
    one: '{n} registro en camino.',
    other: '{n} registros en camino.',
  },
  'table.replicas': {
    one: '{n} réplica va atrasada en ella.',
    other: '{n} réplicas van atrasadas en ella.',
  },
  'table.quarantined': {
    one: '{n} registro apartado.',
    other: '{n} registros apartados.',
  },
  'table.unfiled': {
    one: '{n} registro recibido no está archivado.',
    other: '{n} registros recibidos no están archivados.',
  },
  'table.parked': {
    one: '{n} cambio detenido.',
    other: '{n} cambios detenidos.',
  },
  'problem.quarantined': 'Registro apartado',
  'problem.unfiled': 'Registro recibido sin archivar',
  'problem.parked': 'Cambio detenido',
  'problem.unreachable': 'Réplica inalcanzable',
  'problem.since': 'Visto por primera vez {since}.',
  'problem.attempts': {
    one: '{n} intento.',
    other: '{n} intentos.',
  },
  'problem.retry': 'Se reintenta {until}.',
  'problem.more': {
    one: 'Y {n} más.',
    other: 'Y {n} más.',
  },
  'problem.none': 'Nada espera una corrección.',
  'problem.contract':
    'Cada uno espera a que vuelva la red o a que se despliegue una corrección; después pasa solo. Ningún dato se repara a mano.',
  'detail.back': 'Todas las réplicas',
  'detail.rows': '{done} de {of} registros',
  'detail.loading': 'Contando…',
  'detail.in_flight': {
    one: '{n} en camino.',
    other: '{n} en camino.',
  },
  'detail.to_remove': {
    one: '{n} por quitar.',
    other: '{n} por quitar.',
  },
  'detail.quarantined': {
    one: '{n} apartado.',
    other: '{n} apartados.',
  },
  'detail.outdated': {
    one: '{n} desactualizado.',
    other: '{n} desactualizados.',
  },
  'replica.wishes.pending': {
    one: '{n} ajuste por aplicar.',
    other: '{n} ajustes por aplicar.',
  },
  'replica.wishes.waiting': {
    one: '{n} ajuste espera poder aplicarse.',
    other: '{n} ajustes esperan poder aplicarse.',
  },
  'replica.wishes.refused': {
    one: 'Rechazó {n} ajuste.',
    other: 'Rechazó {n} ajustes.',
  },
  'replica.wishes.superseded': {
    one: 'Cambió {n} ajuste antes de que llegara lo pedido.',
    other: 'Cambió {n} ajustes antes de que llegara lo pedido.',
  },
  'wish.loading': 'Buscando…',
  'wish.none': 'Nada pedido.',
  'wish.pending': 'En camino',
  'wish.waiting': 'En espera',
  'wish.applied': 'Aplicado',
  'wish.refused': 'Rechazado',
  'wish.superseded': 'Cambiado allá primero',
  'wish.unknown': 'Desconocido ({state})',
  'table.sync_wishes': 'Ajustes pedidos',
  'row.loading': 'Buscando…',
  'row.nowhere': 'Ninguna réplica lo tiene.',
  'row.holds': 'Lo tiene',
  'row.outdated': 'Tiene una versión anterior',
  'row.on_its_way': 'En camino',
  'row.missing': 'Debe tenerlo, aún no',
  'row.leaving': 'Lo está soltando',
  'row.quarantined': 'Apartado',
  'row.unknown': 'Desconocido ({state})',
  'check.sync': 'Sincronización',
  'check.sync.heard': 'Última señal {since}',
  'check.sync.applying': 'Aplicando cambios · última señal {since}',
  'check.sync.uploading': 'Tiene registros propios por subir · última señal {since}',
  'check.sync.set_aside': {
    one: '{n} registro propio no puede subir como está',
    other: '{n} registros propios no pueden subir como están',
  },
  'check.sync.silent': 'Sin señales desde hace {for}',
  'check.sync.unreachable': 'No se puede alcanzar',
  'check.sync.retry': 'No se puede alcanzar · se reintenta {until}',
  'check.sync.fix': {
    one: '{n} cambio espera una corrección',
    other: '{n} cambios esperan una corrección',
  },
  'check.sync.never': 'Aún sin señales',
  'line.unfiled': {
    one: '{n} registro que envió no se pudo archivar',
    other: '{n} registros que envió no se pudieron archivar',
  },
  'line.quarantined': {
    one: '{n} registro apartado',
    other: '{n} registros apartados',
  },
  'line.parked': {
    one: '{n} cambio detenido',
    other: '{n} cambios detenidos',
  },
  'line.unreachable': 'No se puede alcanzar',
  'line.retry': 'No se puede alcanzar · se reintenta {until}',
  'line.silent': 'Última señal {since}',
  'line.moving': {
    one: '{n} cambio en camino',
    other: '{n} cambios en camino',
  },
  'line.applying': 'Aplicando lo que se le envió',
  'line.uploading': {
    one: '{n} registro propio espera subir',
    other: '{n} registros propios esperan subir',
  },
  'line.set_aside': {
    one: '{n} registro propio no puede subir como está',
    other: '{n} registros propios no pueden subir como están',
  },
  'line.heard': 'Última señal {since}',
  'line.never': 'Aún sin señales',
  'flow.live': 'En vivo · último {since}',
  'flow.stalled': 'Último {since}',
  'flow.never': 'Aún no se recibe nada',
  'settled.tables': {
    one: '{n} tabla más al día',
    other: '{n} tablas más al día',
  },
  'settled.groups': {
    one: '{n} grupo más al día',
    other: '{n} grupos más al día',
  },
  'group.waiting': {
    one: '{n} cambio en camino',
    other: '{n} cambios en camino',
  },
  'chip.to_send': '{n} por enviar',
  'chip.to_confirm': '{n} por confirmar',
  'chip.set_aside': {
    one: '{n} apartado',
    other: '{n} apartados',
  },
  'chip.oldest': 'el más antiguo, {for}',
  'detail.heard': 'Última señal {since}',
  'detail.never_heard': 'Aún sin señales',
  'detail.uploading': 'subiendo en vivo',
  'detail.not_uploading': 'subida detenida',
  'detail.to_send': 'por enviar',
  'detail.to_confirm': 'por confirmar',
  'detail.set_aside': 'apartados',
  'detail.down': 'Hacia la réplica',
  'detail.up': 'Desde la réplica',
  'detail.checks': 'Revisiones',
  'detail.problem': 'Problema',
  'grid.title': 'Réplicas',
  'grid.order': 'el más grave primero',
  'grid.by': 'Agrupar por',
  'grid.all': 'Todas',
  'grid.trail': 'Dónde',
  'grid.nowhere': 'Sin ubicación',
  'grid.trouble': '{n} de {total}',
  'grid.tile': '{name}: {state}',
  'grid.legend': 'Qué significa cada color',
  'attention.title': 'Piden atención',
  'attention.none': 'Nada pide atención.',
}

export const builtinMessages: Messages = { en, es }

/** The table for a locale: its own, else its language's (`es-MX` → `es`). */
function tableFor(messages: Messages, locale: string): MessageTable | undefined {
  return messages[locale] ?? messages[locale.split('-')[0]!]
}

/** The built-in messages with the host's laid over them, key by key. */
export function mergeMessages(over: Messages | undefined): Messages {
  if (!over) return builtinMessages
  const merged: Messages = { ...builtinMessages }
  for (const locale of Object.keys(over)) merged[locale] = { ...builtinMessages[locale], ...over[locale] }
  return merged
}

/** How the dashboard says things in `locale` (normalized first), from
 * `messages` (the built-in ones by default); English stands in for a key
 * the locale lacks. */
export function translator(locale: string, messages: Messages = builtinMessages): Translate {
  const tag = normalizeLocale(locale)
  const table = tableFor(messages, tag)
  return (key, params) => {
    const message = table?.[key] ?? messages.en?.[key] ?? key
    const n = params?.n
    const form =
      typeof message === 'string'
        ? message
        : ((typeof n === 'number' ? message[pluralRules(tag).select(n)] : undefined) ?? message.other)
    if (!params) return form
    return form.replace(/\{(\w+)\}/g, (whole, name: string) => {
      const value = params[name]
      return value === undefined ? whole : typeof value === 'number' ? count(value, tag) : value
    })
  }
}
