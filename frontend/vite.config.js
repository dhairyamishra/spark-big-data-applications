import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: "dist",
    sourcemap: false,
    chunkSizeWarningLimit: 600,
    rollupOptions: {
      output: {
        manualChunks: {
          maplibre: ["maplibre-gl", "react-map-gl"],
          deckgl: [
            "@deck.gl/core",
            "@deck.gl/layers",
            "@deck.gl/react",
            "@deck.gl/aggregation-layers",
            "@deck.gl/geo-layers",
          ],
          recharts: ["recharts"],
        },
      },
    },
  },
});
