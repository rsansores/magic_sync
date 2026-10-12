// The dashboard's locale in Vue. When the host app uses vue-i18n, the
// dashboard follows its locale with no wiring: it reads the global
// `$i18n.locale`, so vue-i18n is not a dependency. A host can pin a
// `locale`, or override any message through `messages`.

import { computed, getCurrentInstance } from 'vue'
import { normalizeLocale } from '../core/format.js'
import { mergeMessages, translator, type Messages, type Translate } from '../core/i18n.js'

export function useDashboardI18n(
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
  const locale = computed(() => normalizeLocale(getLocale() ?? hostLocale()))
  const messages = computed(() => mergeMessages(getMessages()))
  const translate = computed(() => translator(locale.value, messages.value))
  const t: Translate = (key, params) => translate.value(key, params)
  return { t, locale }
}
