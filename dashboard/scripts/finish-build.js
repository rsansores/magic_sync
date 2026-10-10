// What `vite build` and `vue-tsc` leave for the published package to finish.
import { copyFileSync, readdirSync, renameSync } from 'node:fs'

// The stylesheet ships as written: no component imports it, a host does.
copyFileSync('src/styles/dashboard.css', 'dist/dashboard.css')

// vue-tsc writes a component's declarations as `X.vue.d.ts`, which Node's
// own resolution (TypeScript's `node16`) never finds for `./X.vue`; it does
// find `X.d.vue.ts`.
for (const file of readdirSync('dist/vue'))
  if (file.endsWith('.vue.d.ts'))
    renameSync(`dist/vue/${file}`, `dist/vue/${file.replace(/\.vue\.d\.ts$/, '.d.vue.ts')}`)
