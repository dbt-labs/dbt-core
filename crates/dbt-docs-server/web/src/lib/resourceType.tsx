import { Icon } from "@dbt-labs/sourdough";
import {
  RyeconCamera,
  RyeconClipboardSuccess,
  RyeconDatabase,
  RyeconFile,
  RyeconGraphNodes,
  RyeconGroup,
  RyeconMeter,
  RyeconMetrics,
  RyeconModel,
  RyeconSave,
  RyeconSeed,
} from "@dbt-labs/sourdough";
import type { Ryecon } from "@dbt-labs/sourdough";

export const RESOURCE_TYPE_ORDER = [
  "model",
  "source",
  "test",
  "exposure",
  "group",
  "metric",
  "semantic_model",
  "seed",
  "macro",
  "snapshot",
  "saved_query",
  "analysis",
] as const;

export const RESOURCE_TYPE_LABEL: Record<string, string> = {
  model: "Models",
  source: "Sources",
  test: "Tests",
  exposure: "Exposures",
  group: "Groups",
  metric: "Metrics",
  semantic_model: "Semantic models",
  seed: "Seeds",
  macro: "Macros",
  snapshot: "Snapshots",
  saved_query: "Saved queries",
  analysis: "Analyses",
};

/** Singular display name — used in breadcrumbs and detail headers. */
export const RESOURCE_TYPE_SINGULAR: Record<string, string> = {
  model: "Model",
  source: "Source",
  test: "Test",
  exposure: "Exposure",
  group: "Group",
  metric: "Metric",
  semantic_model: "Semantic model",
  seed: "Seed",
  macro: "Macro",
  snapshot: "Snapshot",
  saved_query: "Saved query",
  analysis: "Analysis",
};

export const RESOURCE_TYPE_RYECON: Record<string, Ryecon> = {
  model: RyeconModel,
  source: RyeconDatabase,
  test: RyeconClipboardSuccess,
  exposure: RyeconMeter,
  group: RyeconGroup,
  metric: RyeconMetrics,
  semantic_model: RyeconGraphNodes,
  seed: RyeconSeed,
  macro: RyeconFile,
  snapshot: RyeconCamera,
  saved_query: RyeconSave,
  analysis: RyeconFile,
};

export function ryeconForType(type: string): Ryecon {
  return RESOURCE_TYPE_RYECON[type] ?? RyeconFile;
}

/** Canonical color key per resource type — mirrors the Pumpernickel
 *  storybook "Resource badge" palette. We hand-roll the visual instead of
 *  sourdough Badge because sourdough's saturated `type` colors are too
 *  loud on the dark canvas; we want the Pumpernickel pastel-soft look
 *  driven by biga's `--brand*Tinted` tokens. When sourdough ships its
 *  own `ResourceBadge`, this whole component swaps to one import line. */
export const RESOURCE_TYPE_BADGE_COLOR: Record<string, string> = {
  model: "blue",
  source: "green",
  test: "green",
  exposure: "orange",
  group: "neutral",
  metric: "yellow",
  semantic_model: "pink",
  seed: "green",
  macro: "pink",
  snapshot: "purple",
  saved_query: "neutral",
  analysis: "purple",
};

interface ResourceBadgeProps {
  type: string;
  size?: "xs" | "sm";
  /** Show the resource ryecon left of the label. Default true. */
  withRyecon?: boolean;
}

export function ResourceBadge({
  type,
  size = "xs",
  withRyecon = true,
}: ResourceBadgeProps) {
  const color = RESOURCE_TYPE_BADGE_COLOR[type] ?? "neutral";
  return (
    <span
      className={`resource-badge resource-badge--${color} resource-badge--${size}`}
    >
      {withRyecon && <Icon ryecon={ryeconForType(type)} size="xs" alt="" />}
      <span>{RESOURCE_TYPE_SINGULAR[type] ?? type}</span>
    </span>
  );
}

/** Infer the dbt modeling layer from the file path. Matches the standard
 *  staging/intermediate/marts convention; returns null when nothing matches
 *  so callers can render "—". */
export function inferModelingLayer(path?: string | null): string | null {
  if (!path) return null;
  const p = path.toLowerCase();
  if (p.includes("/staging/") || p.includes("/stg_") || p.startsWith("staging/")) {
    return "Staging";
  }
  if (p.includes("/intermediate/") || p.includes("/int_") || p.startsWith("intermediate/")) {
    return "Intermediate";
  }
  if (p.includes("/marts/") || p.includes("/dim_") || p.includes("/fct_") || p.startsWith("marts/")) {
    return "Marts";
  }
  return null;
}
