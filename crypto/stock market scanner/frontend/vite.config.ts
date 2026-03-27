import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      '/scan': 'http://api:8000',
      '/health': 'http://api:8000',
      '/ws': { target: 'ws://api:8000', ws: true },
    },
  },
})
