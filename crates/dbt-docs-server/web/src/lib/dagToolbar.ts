import type { ToolbarItem } from '@dbt-labs/dbt-dag';

/**
 * `ToolbarItem` with `ryecon` relaxed to optional, for label-only toolbar chips.
 *
 * The published `@dbt-labs/dbt-dag` types mark `ryecon` as required, but
 * `DagToolbar` forwards it straight to the underlying button, which simply renders
 * no icon when it is `undefined`. dbt-ui builds dbt-dag from source, where
 * `ryecon` is already optional; no published version carries that change yet
 * (3.10.0 and 3.10.1 both still require it).
 *
 * Delete this and annotate with `ToolbarItem[]` directly once a dbt-dag release
 * ships the optional `ryecon`.
 */
export type LabelOnlyToolbarItem = Omit<ToolbarItem, 'ryecon'> &
  Partial<Pick<ToolbarItem, 'ryecon'>>;

/** Hand label-only items to `<Dag toolbarItems={...} />`. */
export const asToolbarItems = (items: readonly LabelOnlyToolbarItem[]): ToolbarItem[] =>
  items as ToolbarItem[];
