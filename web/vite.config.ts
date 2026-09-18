import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      // En desarrollo, redirigimos al gateway para evitar CORS.
      // En producción el proxy lo haría nginx o un ingress.
      '/subscribe': {
        target: 'http://localhost:8081',
        changeOrigin: true,
      },
      // Las publicaciones van al publisher, no al gateway.
      '/publish': {
        target: 'http://localhost:8080',
        changeOrigin: true,
      },
      '/health': {
        target: 'http://localhost:8081',
        changeOrigin: true,
      },
    },
  },
})
