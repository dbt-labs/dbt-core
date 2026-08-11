/**
 * UI visibility flags. Hardcoded today; the natural next step is to derive
 * each one from a wire-format capability or a build-time env var.
 *
 *  - `hasAnalysis` — analyses are surfaced in the Asset list, File tree, and
 *    Filter pane. Off until `/api/v1/search`'s `ResourceType` enum learns
 *    about `Analysis` (handlers/search.rs). Analyses already live in
 *    `dbt.nodes` (post PR 10186) so individual detail pages work — only the
 *    discovery surfaces are gated.
 */
export const FEATURE_FLAGS = {
  hasAnalysis: false,
} as const;
