import { createElement } from 'react';
import {
  Box,
  Camera,
  ChartColumn,
  CircleGauge,
  ClipboardCheck,
  Database,
  FileText,
  type LucideIcon,
  Save,
  Sprout,
  Users,
  Waypoints,
} from 'lucide-react';

export const RESOURCE_TYPE_ORDER = [
  'model',
  'source',
  'test',
  'exposure',
  'group',
  'metric',
  'semantic_model',
  'seed',
  'macro',
  'snapshot',
  'saved_query',
  'analysis',
] as const;

/** Every real dbt resource type -- mirrors `@dbt-labs/dbt-dag`'s `ResourceType`
 *  union so callers moving off that package aren't narrowing what they can
 *  represent. */
export type ResourceType =
  | 'analysis'
  | 'exposure'
  | 'macro'
  | 'metric'
  | 'model'
  | 'seed'
  | 'snapshot'
  | 'source'
  | 'test'
  | 'unit_test'
  | 'semantic_model'
  | 'group'
  | 'saved_query'
  | 'function';

/** `ResourceType` plus the two pseudo-types the file/asset explorer tree
 *  also has to represent: the project root itself, and a column node.
 *  Mirrors dbt-dag's `ResourceTypeExplorer`. */
export type ResourceTypeExplorer = ResourceType | 'project' | 'column';

/** Resource types whose detail page has a Columns tab. Mirrors dbt-dag's
 *  `resourceTypesWithColumns`. */
export const RESOURCE_TYPES_WITH_COLUMNS: readonly ResourceType[] = [
  'model',
  'source',
  'seed',
  'snapshot',
];

export const RESOURCE_TYPE_LABEL: Record<string, string> = {
  model: 'Models',
  source: 'Sources',
  test: 'Tests',
  exposure: 'Exposures',
  group: 'Groups',
  metric: 'Metrics',
  semantic_model: 'Semantic models',
  seed: 'Seeds',
  macro: 'Macros',
  snapshot: 'Snapshots',
  saved_query: 'Saved queries',
  analysis: 'Analyses',
};

/** Singular display name — used in breadcrumbs and detail headers. */
export const RESOURCE_TYPE_SINGULAR: Record<string, string> = {
  model: 'Model',
  source: 'Source',
  test: 'Test',
  exposure: 'Exposure',
  group: 'Group',
  metric: 'Metric',
  semantic_model: 'Semantic model',
  seed: 'Seed',
  macro: 'Macro',
  snapshot: 'Snapshot',
  saved_query: 'Saved query',
  analysis: 'Analysis',
  unit_test: 'Unit test',
  function: 'Function',
};

export const RESOURCE_TYPE_ICON: Record<string, LucideIcon> = {
  model: Box,
  source: Database,
  test: ClipboardCheck,
  exposure: CircleGauge,
  group: Users,
  metric: ChartColumn,
  semantic_model: Waypoints,
  seed: Sprout,
  macro: FileText,
  snapshot: Camera,
  saved_query: Save,
  analysis: FileText,
};

export function iconForType(type: string): LucideIcon {
  return RESOURCE_TYPE_ICON[type] ?? FileText;
}

/** Canonical color key per resource type — mirrors the Pumpernickel
 *  storybook "Resource badge" palette. We hand-roll the visual instead of
 *  sourdough Badge because sourdough's saturated `type` colors are too
 *  loud on the dark canvas; we want the Pumpernickel pastel-soft look
 *  driven by biga's `--brand*Tinted` tokens. When sourdough ships its
 *  own `ResourceBadge`, this whole component swaps to one import line. */
export const RESOURCE_TYPE_BADGE_COLOR: Record<string, string> = {
  model: 'blue',
  source: 'green',
  test: 'green',
  exposure: 'orange',
  group: 'neutral',
  metric: 'yellow',
  semantic_model: 'pink',
  seed: 'green',
  macro: 'pink',
  snapshot: 'purple',
  saved_query: 'neutral',
  analysis: 'purple',
};

/** Renders the icon for a resource type. Uses `createElement` directly
 *  (no local component variable) since assigning a dynamically-selected
 *  component to a variable and rendering it as JSX is flagged as
 *  "component created during render", even inside a dedicated component. */
export function ResourceTypeIcon({
  type,
  className,
}: {
  type: string;
  className?: string;
}) {
  return createElement(iconForType(type), { className });
}

interface ResourceBadgeProps {
  type: string;
  size?: 'xs' | 'sm';
  /** Show the resource ryecon left of the label. Default true. */
  withRyecon?: boolean;
}

export function ResourceBadge({
  type,
  size = 'xs',
  withRyecon = true,
}: ResourceBadgeProps) {
  const color = RESOURCE_TYPE_BADGE_COLOR[type] ?? 'neutral';
  return (
    <span className={`resource-badge resource-badge--${color} resource-badge--${size}`}>
      {withRyecon && <ResourceTypeIcon type={type} className="size-3" />}
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
  if (p.includes('/staging/') || p.includes('/stg_') || p.startsWith('staging/')) {
    return 'Staging';
  }
  if (
    p.includes('/intermediate/') ||
    p.includes('/int_') ||
    p.startsWith('intermediate/')
  ) {
    return 'Intermediate';
  }
  if (
    p.includes('/marts/') ||
    p.includes('/dim_') ||
    p.includes('/fct_') ||
    p.startsWith('marts/')
  ) {
    return 'Marts';
  }
  return null;
}
