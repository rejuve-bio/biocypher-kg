import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev proxies /api to the backend so SPA and API share an origin; override via VITE_API_TARGET / VITE_PORT.
const API_TARGET = process.env.VITE_API_TARGET || "http://localhost:9000";
const DEV_PORT = Number(process.env.VITE_PORT || 5173);

export default defineConfig({
  base: "/console/",
  plugins: [react()],
  server: {
    port: DEV_PORT,
    proxy: {
      "/api": {
        target: API_TARGET,
        changeOrigin: true,
      },
    },
  },
});
