import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// `pnpm build` writes dist/, which the head office serves at
// http://127.0.0.1:8080. `pnpm dev` serves the page on :5173 with hot reload,
// and passes what it asks of the head office through.
export default defineConfig({
  plugins: [vue()],
  server: {
    proxy: { '/api': 'http://127.0.0.1:8080' },
  },
})
