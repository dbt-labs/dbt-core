import react from '@vitejs/plugin-react';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  plugins: [react()],
  test: {
    globals: true,
    environment: 'happy-dom',
    setupFiles: ['./vitest.setup.ts'],
    include: ['src/**/*.test.{ts,tsx}'],
    css: false,
    server: {
      deps: {
        // Let vite transform these rather than loading them through node's ESM
        // resolver. `@dbt-labs/sourdough` pulls CJS-only deps (react-use), whose
        // named exports node refuses to bind; vite's interop handles them.
        inline: [/@dbt-labs\//, 'react-use'],
      },
    },
  },
});
