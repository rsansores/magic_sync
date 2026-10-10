/// <reference types="vitest/config" />
import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { resolve } from 'node:path'

// `vite` serves the demo (index.html, made-up data); `vite build` builds the
// library a host embeds.
export default defineConfig(({ command }) => ({
  plugins: [vue()],
  build:
    command === 'build'
      ? {
          lib: {
            entry: {
              dashboard: resolve(import.meta.dirname, 'src/index.ts'),
              core: resolve(import.meta.dirname, 'src/core/index.ts'),
            },
            formats: ['es'],
            cssFileName: 'dashboard',
          },
          rollupOptions: { external: ['vue'] },
        }
      : undefined,
  test: {
    include: ['src/**/*.test.ts'],
    environment: 'node',
  },
}))
