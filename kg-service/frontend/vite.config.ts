import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Served under /console in production (StaticFiles mount in the FastAPI app).
// In dev, /api is proxied to the backend so the SPA and API share an origin.
export default defineConfig({
  base: "/console/",
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
    },
  },
});
