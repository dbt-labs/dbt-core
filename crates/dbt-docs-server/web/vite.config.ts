import path from 'path';
import { fileURLToPath } from 'url';
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export default defineConfig(({ mode }) => ({
  // Absolute base. The SPA uses `BrowserRouter` with real paths (e.g.
  // `/details/:dbtUniqueId/`), and `dbt-docs-server` serves any unknown path by
  // falling back to `index.html`. Relative asset URLs would resolve against the
  // current route and 404 on a deep-link reload, so this must stay `/`.
  base: '/',
  plugins: [
    react({
      babel: {
        plugins: [['babel-plugin-react-compiler']],
      },
    }),
  ],
  resolve: {
    dedupe: ['@dbt-labs/sourdough', '@tanstack/react-query', 'react', 'react-dom'],
  },
  build: {
    target: 'esnext',
    // Sourcemaps are never shipped: the bundle is committed to this repo and
    // embedded in the binary. `web/.gitignore` also excludes `dist/**/*.map`.
    sourcemap: mode === 'production' ? false : true,
    rollupOptions: {
      input: {
        index: path.resolve(__dirname, 'index.html'),
      },
      output: {
        dir: 'dist',
      },
    },
  },
  server: {
    port: 3002,
    proxy: {
      '/api': 'http://127.0.0.1:8580',
    },
  },
}));
