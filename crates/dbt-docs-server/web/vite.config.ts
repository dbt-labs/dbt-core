import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Use relative base so the embedded SPA works regardless of mount point.
export default defineConfig({
  plugins: [react()],
  base: "./",
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
  server: {
    proxy: {
      "/api": "http://127.0.0.1:8580",
    },
  },
});
