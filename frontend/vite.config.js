import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// In development the page runs on Vite's server and API calls are passed
// through to the Python API. In production the API serves the built files
// from frontend/dist itself, so there is one process and one origin.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": "http://127.0.0.1:8000",
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
  test: {
    environment: "jsdom",
    setupFiles: ["./src/test-setup.js"],
  },
});
