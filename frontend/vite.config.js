import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// In dev the SPA is served by Vite (5173) and the API by uvicorn (8000). Rather than
// hard-code the backend origin (and fight CORS), we proxy:
//   /api/*  -> backend root   (the `/api` prefix is stripped)
//   /ws/*   -> backend WebSocket routes (path preserved)
// so the app only ever talks to its own origin. Override the backend target with
// VITE_BACKEND when it lives elsewhere.
const BACKEND = process.env.VITE_BACKEND || "http://localhost:8000";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: BACKEND,
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ""),
      },
      "/ws": {
        target: BACKEND.replace(/^http/, "ws"),
        ws: true,
      },
    },
  },
});
