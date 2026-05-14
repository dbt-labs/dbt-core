/** @type {import('tailwindcss').Config} */
module.exports = {
  presets: [
    require("@dbt-labs/sourdough/tailwind.config"),
    require("@dbt-labs/dbt-dag/tailwind.config"),
  ],
  content: [
    "./index.html",
    "./src/**/*.{ts,tsx}",
    "./node_modules/@dbt-labs/sourdough/dist/**/*.{js,cjs}",
    "./node_modules/@dbt-labs/dbt-dag/dist/**/*.{js,cjs}",
  ],
};
