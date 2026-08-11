const sourdoughConfig = require('@dbt-labs/sourdough/tailwind.config');
const dbtDagPreset = require('@dbt-labs/dbt-dag/tailwind.config');
const { tokens: bigaTokens } = require('@dbt-labs/biga');

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
