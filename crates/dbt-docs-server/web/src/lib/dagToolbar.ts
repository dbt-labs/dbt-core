/**
 * Local stand-in for dbt-dag's `ToolbarItem`, with `ryecon` optional -- for
 * label-only toolbar chips.
 *
 * The published `@dbt-labs/dbt-dag` type marks `ryecon` as required, but
 * `DagToolbar` forwards it straight to the underlying button, which simply
 * renders no icon when it is `undefined`. Kept local (not imported from
 * dbt-dag) since only the old dbt-dag-based `LineageView`/`FullLineagePage`
 * need the real `ToolbarItem` shape, and they cast into it at their own
 * `<Dag toolbarItems={...}>` call site.
 */
export type LabelOnlyToolbarItem = {
  label?: string;
  tooltip: string;
  action?: () => void;
  testId?: string;
  isDisabled?: boolean;
  className?: string;
  ryecon?: unknown;
};

/** Hand label-only items to `<Dag toolbarItems={...} />`; the caller casts
 *  the result to dbt-dag's real `ToolbarItem[]` at that boundary. */
export const asToolbarItems = (
  items: readonly LabelOnlyToolbarItem[],
): LabelOnlyToolbarItem[] => items as LabelOnlyToolbarItem[];
