// Vendored copy of @dbt-labs/sourdough's tailwind.config.cjs -- see
// src/styles/sourdough-preset.cjs.
const sourdoughConfig = require('./src/styles/sourdough-preset.cjs');
const dbtDagPreset = require('@dbt-labs/dbt-dag/tailwind.config');
// Vendored copy of @dbt-labs/biga's tokens.js — see src/styles/tokens.js.
const { tokens: bigaTokens } = require('./src/styles/tokens.js');

/** @type {import('tailwindcss').Config} */
module.exports = {
  presets: [sourdoughConfig, dbtDagPreset],
  content: [
    './index.html',
    // Covers both the app and the shared component/data layer under src/shared/.
    './src/**/*.{js,ts,jsx,tsx,css}',
    ...sourdoughConfig.content,
  ],
  theme: {
    extend: {
      colors: {
        ...bigaTokens,
      },
    },
  },
};
