import { useMemo, useState, type ReactNode } from "react";
import {
  Icon,
  RyeconCamera,
  RyeconCaretDown,
  RyeconCaretRight,
  RyeconClipboardSuccess,
  RyeconColorDbt,
  RyeconColorSnowflake,
  RyeconDataGeography,
  RyeconDatabase,
  RyeconDuotoneStar,
  RyeconFile,
  RyeconGitBranch,
  RyeconGraphNodes,
  RyeconGroup,
  RyeconMeter,
  RyeconMetrics,
  RyeconModel,
  RyeconProjects,
  RyeconSave,
  RyeconSeed,
  RyeconStar,
  RyeconThemeDark,
  RyeconThemeLight,
  SegmentedButton,
} from "@dbt-labs/sourdough";
import type { Ryecon } from "@dbt-labs/sourdough";
import type { NodeSummary, Project, TableInfo } from "../api";
import { inferModelingLayer } from "../lib/resourceType";
import type { AssetFilters } from "../App";
import { UpgradeHookInline } from "./UpgradeHook";

/* ---------- Resource type metadata ---------- */

const RESOURCE_TYPE_ORDER = [
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

const RESOURCE_TYPE_LABEL: Record<string, string> = {
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

const RESOURCE_TYPE_RYECON: Record<string, Ryecon> = {
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

function adapterRyecon(adapter?: string): Ryecon {
  return adapter?.toLowerCase().includes("snowflake")
    ? RyeconColorSnowflake
    : RyeconColorDbt;
}

type Mode = "asset" | "tree" | "filter";

interface Props {
  project: Project;
  nodes: NodeSummary[];
  selectedId: string | null;
  /** Currently previewed node (for highlight). */
  previewId?: string | null;
  /** When in list view, the active type filter (null = "All assets"). */
  activeListType?: string | null;
  /** Single-click on a node row opens the peek drawer. */
  onPeek(uniqueId: string): void;
  onShowList(type: string | null): void;
  onShowAll(): void;
  query: string;
  loadingProgress?: { loaded: number; total: number } | null;
  theme: "dark" | "light";
  onSetTheme(theme: "dark" | "light"): void;
  favorites: {
    ids: Set<string>;
    has: (id: string) => boolean;
    toggle: (id: string) => void;
  };
  filters: AssetFilters;
  onSetFilters(next: AssetFilters): void;
  /** True in Query mode — Schema gets prime real estate, asset nav muted. */
  isQueryMode?: boolean;
  /** DuckDB tables, available in Query mode for the Schema section. */
  tables?: TableInfo[] | null;
  /** Click handler: writes preview SQL into the QueryPane editor and runs it. */
  onSelectSchemaTable?: (schema: string, name: string) => void;
  /** `${schema}.${name}` of the currently active table — brand-indigo highlight. */
  activeSchemaKey?: string | null;
  /** Click "Browse N docs assets" in Query mode → switch to Docs. */
  onSwitchToDocs?: () => void;
}

export function LocatePane({
  project,
  nodes,
  selectedId,
  previewId,
  activeListType,
  onPeek,
  onShowList,
  onShowAll,
  query,
  loadingProgress,
  theme,
  onSetTheme,
  favorites,
  filters,
  onSetFilters,
  isQueryMode = false,
  tables = null,
  onSelectSchemaTable,
  activeSchemaKey = null,
  onSwitchToDocs,
}: Props) {
  const [mode, setMode] = useState<Mode>("asset");
  // The "active" highlight follows the list view's type. When not in a
  // list view, nothing is highlighted.
  const typeFilter = activeListType === undefined ? null : activeListType;

  // Search-filtered but *before* applying the active type filter. The
  // sidebar type rows render counts from this so e.g. clicking "Models"
  // doesn't zero out the Sources/Tests counts — the rows always reflect
  // "how many of each type would show if you clicked this row".
  const searchFiltered = useMemo(() => {
    const needle = query.trim().toLowerCase();
    if (!needle) return nodes;
    return nodes.filter(
      (n) =>
        n.name.toLowerCase().includes(needle) ||
        n.unique_id.toLowerCase().includes(needle),
    );
  }, [nodes, query]);

  const filtered = useMemo(() => {
    if (!typeFilter) return searchFiltered;
    return searchFiltered.filter((n) => n.resource_type === typeFilter);
  }, [searchFiltered, typeFilter]);

  const typeCounts = useMemo(() => {
    const m = new Map<string, number>();
    for (const n of searchFiltered)
      m.set(n.resource_type, (m.get(n.resource_type) ?? 0) + 1);
    return m;
  }, [searchFiltered]);

  const packageGroups = useMemo(() => {
    const m = new Map<string, NodeSummary[]>();
    for (const n of filtered) {
      const pkg = n.package_name ?? "(no package)";
      const arr = m.get(pkg) ?? [];
      arr.push(n);
      m.set(pkg, arr);
    }
    return [...m.entries()].sort(([a], [b]) => {
      const aSelf = a === project.name ? 0 : 1;
      const bSelf = b === project.name ? 0 : 1;
      if (aSelf !== bSelf) return aSelf - bSelf;
      return a.localeCompare(b);
    });
  }, [filtered, project.name]);

  const favoriteNodes = useMemo(
    () => nodes.filter((n) => favorites.has(n.unique_id)),
    [nodes, favorites],
  );

  const presentTypes = useMemo(
    () => new Set(nodes.map((n) => n.resource_type)),
    [nodes],
  );

  return (
    <aside className="locate-pane">
      {/* Favorites card */}
      <FavoritesCard
        favoriteNodes={favoriteNodes}
        selectedId={previewId ?? selectedId}
        onSelect={onPeek}
        onToggleFavorite={favorites.toggle}
        loadingProgress={loadingProgress}
      />

      {/* Docs mode — full nav. Query mode demotes this to a single muted link
          below the Schema section so Schema gets the prime real estate. */}
      {!isQueryMode && (
        <>
          <div className="locate-pane__tabs">
            <SegmentedButton
              segments={[
                { label: "Asset", value: "asset" },
                { label: "Tree", value: "tree" },
                { label: "Filter", value: "filter" },
              ]}
              selectedValue={mode}
              onSelect={(v) => setMode(v as Mode)}
              size="sm"
              variant="stretch"
            />
          </div>
          <div className="locate-pane__body">
            {mode === "asset" && (
              <AssetMode
                nodes={nodes}
                packageGroups={packageGroups}
                typeCounts={typeCounts}
                presentTypes={presentTypes}
                typeFilter={typeFilter}
                onShowList={onShowList}
                onShowAll={onShowAll}
              />
            )}
            {mode === "tree" && (
              <TreeMode
                packageGroups={packageGroups}
                projectName={project.name}
                adapterType={project.adapter_type}
                selectedId={previewId ?? selectedId}
                onSelect={onPeek}
                isFavorite={favorites.has}
                onToggleFavorite={favorites.toggle}
              />
            )}
            {mode === "filter" && (
              <FilterMode
                project={project}
                nodes={nodes}
                presentTypes={presentTypes}
                typeCounts={typeCounts}
                typeFilter={typeFilter}
                onShowList={onShowList}
                filters={filters}
                onSetFilters={onSetFilters}
              />
            )}
          </div>
        </>
      )}

      {/* Query mode — Schema is the primary nav. Asset nav becomes a single
          muted "browse docs" affordance below. */}
      {isQueryMode && (
        <>
          <SchemaSection
            tables={tables}
            query={query}
            onSelectSchemaTable={onSelectSchemaTable}
            activeSchemaKey={activeSchemaKey}
            elevated
          />
          <button
            type="button"
            className="locate-pane__docs-link"
            onClick={onSwitchToDocs}
            title="Switch to Docs to browse the catalog"
          >
            <Icon ryecon={RyeconDataGeography} size="xs" alt="" />
            <span>Browse {nodes.length.toLocaleString()} docs assets</span>
            <span className="locate-pane__docs-link-hint">
              Docs
              <Icon ryecon={RyeconCaretRight} size="xs" alt="" />
            </span>
          </button>
        </>
      )}

      {/* Connect dbt platform — persistent, quiet rail-footer affordance.
          The "do it once" entry point that unlocks every proprietary
          surface. */}
      <UpgradeHookInline className="locate-pane__connect-hook" />

      {/* Footer — theme toggle + branch */}
      <footer className="locate-pane__footer">
        {project.git_branch && (
          <span className="locate-pane__branch" title="Git branch">
            <Icon ryecon={RyeconGitBranch} size="xs" alt="" />
            <span>{project.git_branch}</span>
            {project.git_is_dirty && (
              <span className="locate-pane__dirty" aria-label="uncommitted changes">●</span>
            )}
          </span>
        )}
        <div className="locate-pane__theme">
          <SegmentedButton
            segments={[
              { label: "Dark", value: "dark", startIcon: { ryecon: RyeconThemeDark } },
              { label: "Light", value: "light", startIcon: { ryecon: RyeconThemeLight } },
            ]}
            selectedValue={theme}
            onSelect={(v) => onSetTheme(v as "dark" | "light")}
            size="sm"
          />
        </div>
      </footer>
    </aside>
  );
}

/* ---------- Favorites card ---------- */

function FavoritesCard({
  favoriteNodes,
  selectedId,
  onSelect,
  onToggleFavorite,
  loadingProgress,
}: {
  favoriteNodes: NodeSummary[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  onToggleFavorite: (id: string) => void;
  loadingProgress?: { loaded: number; total: number } | null;
}) {
  return (
    <section className="locate-pane__fav-card">
      <header className="locate-pane__fav-header">
        <Icon ryecon={RyeconDuotoneStar} size="xs" alt="" />
        <span>Favorites</span>
        <span className="locate-pane__fav-count">{favoriteNodes.length}</span>
      </header>
      {favoriteNodes.length > 0 && (
        <ul className="locate-pane__fav-list">
          {favoriteNodes.map((n) => (
            <NodeRow
              key={n.unique_id}
              node={n}
              selectedId={selectedId}
              onSelect={onSelect}
              isFavorite
              onToggleFavorite={onToggleFavorite}
            />
          ))}
        </ul>
      )}
      {loadingProgress && loadingProgress.loaded < loadingProgress.total && (
        <p className="locate-pane__progress">
          Loading {loadingProgress.loaded.toLocaleString()} / {loadingProgress.total.toLocaleString()}
        </p>
      )}
    </section>
  );
}

/* ---------- Asset mode ---------- */

function AssetMode({
  nodes,
  packageGroups,
  typeCounts,
  presentTypes,
  typeFilter,
  onShowList,
  onShowAll,
}: {
  nodes: NodeSummary[];
  packageGroups: [string, NodeSummary[]][];
  typeCounts: Map<string, number>;
  presentTypes: Set<string>;
  typeFilter: string | null;
  onShowList: (type: string | null) => void;
  onShowAll: () => void;
}) {
  const orderedTypes = RESOURCE_TYPE_ORDER.filter((t) => presentTypes.has(t));

  return (
    <div className="locate-pane__asset">
      {/* "All Assets" header — clicking opens the cross-package table view.
          Type rows below show cross-package counts (every node, every package).
          docs v1 is per-project, so "all" means "across every dbt_package
          inside this project". For per-package drill-down, use Tree mode. */}
      <button
        type="button"
        className="locate-pane__hgroup locate-pane__hgroup--button"
        onClick={onShowAll}
        title="Browse all assets across every package"
      >
        <Icon ryecon={RyeconDataGeography} size="xs" alt="" />
        <span>All assets</span>
        <span className="locate-pane__hgroup-count">{nodes.length.toLocaleString()}</span>
      </button>

      {/* Type rows — cross-package counts; click opens that type's table view */}
      <ul className="locate-pane__type-list">
        {orderedTypes.map((t) => {
          const active = typeFilter === t;
          const count = typeCounts.get(t) ?? 0;
          return (
            <li key={t}>
              <button
                type="button"
                className={`locate-pane__type-row ${active ? "is-active" : ""}`}
                onClick={() => onShowList(t)}
              >
                <Icon ryecon={RESOURCE_TYPE_RYECON[t] ?? RyeconFile} size="xs" alt="" />
                <span className="locate-pane__type-label">{RESOURCE_TYPE_LABEL[t] ?? t}</span>
                <span className="locate-pane__type-count">{count.toLocaleString()}</span>
              </button>
            </li>
          );
        })}
      </ul>

      {packageGroups.length > 1 && (
        <p className="locate-pane__hint">
          {packageGroups.length} packages installed — switch to Tree mode for per-package browsing
        </p>
      )}
    </div>
  );
}

/* ---------- Tree mode ---------- */

function TreeMode({
  packageGroups,
  projectName,
  adapterType,
  selectedId,
  onSelect,
  isFavorite,
  onToggleFavorite,
}: {
  packageGroups: [string, NodeSummary[]][];
  projectName: string;
  adapterType?: string;
  selectedId: string | null;
  onSelect: (id: string) => void;
  isFavorite: (id: string) => boolean;
  onToggleFavorite: (id: string) => void;
}) {
  return (
    <div className="locate-pane__tree">
      {packageGroups.map(([pkg, items]) => (
        <PackageGroup
          key={pkg}
          name={pkg}
          isProject={pkg === projectName}
          adapterType={adapterType}
          items={items}
          selectedId={selectedId}
          onSelect={onSelect}
          isFavorite={isFavorite}
          onToggleFavorite={onToggleFavorite}
        />
      ))}
    </div>
  );
}

function PackageGroup({
  name,
  isProject,
  adapterType,
  items,
  selectedId,
  onSelect,
  isFavorite,
  onToggleFavorite,
}: {
  name: string;
  isProject: boolean;
  adapterType?: string;
  items: NodeSummary[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  isFavorite: (id: string) => boolean;
  onToggleFavorite: (id: string) => void;
}) {
  const [open, setOpen] = useState(isProject);
  const byType = new Map<string, NodeSummary[]>();
  for (const n of items) {
    const arr = byType.get(n.resource_type) ?? [];
    arr.push(n);
    byType.set(n.resource_type, arr);
  }
  const orderedTypes = RESOURCE_TYPE_ORDER.filter((t) => byType.has(t));

  return (
    <section className="locate-pane__tree-section">
      <button
        type="button"
        className={`locate-pane__pkg-head ${isProject ? "is-project" : ""}`}
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
      >
        <span className="locate-pane__caret">
          <Icon ryecon={RyeconCaretDown} size="xs" alt="" />
        </span>
        <Icon ryecon={RyeconProjects} size="xs" alt="" />
        <span className="locate-pane__pkg-name">{name}</span>
        {isProject && (
          <Icon ryecon={adapterRyecon(adapterType)} size="xs" alt={adapterType ?? ""} />
        )}
        <span className="locate-pane__type-count">{items.length.toLocaleString()}</span>
      </button>
      {open && (
        <div className="locate-pane__tree-types">
          {orderedTypes.map((t) => (
            <TypeBucket
              key={t}
              type={t}
              items={byType.get(t) ?? []}
              selectedId={selectedId}
              onSelect={onSelect}
              isFavorite={isFavorite}
              onToggleFavorite={onToggleFavorite}
            />
          ))}
        </div>
      )}
    </section>
  );
}

function TypeBucket({
  type,
  items,
  selectedId,
  onSelect,
  isFavorite,
  onToggleFavorite,
}: {
  type: string;
  items: NodeSummary[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  isFavorite: (id: string) => boolean;
  onToggleFavorite: (id: string) => void;
}) {
  const [open, setOpen] = useState(false);
  return (
    <div className="locate-pane__bucket">
      <button
        type="button"
        className="locate-pane__bucket-head"
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
      >
        <span className="locate-pane__caret">
          <Icon ryecon={RyeconCaretDown} size="xs" alt="" />
        </span>
        <Icon ryecon={RESOURCE_TYPE_RYECON[type] ?? RyeconFile} size="xs" alt="" />
        <span className="locate-pane__type-label">{RESOURCE_TYPE_LABEL[type] ?? type}</span>
        <span className="locate-pane__type-count">{items.length.toLocaleString()}</span>
      </button>
      {open && (
        <ul className="locate-pane__list">
          {items.map((n) => (
            <NodeRow
              key={n.unique_id}
              node={n}
              selectedId={selectedId}
              onSelect={onSelect}
              isFavorite={isFavorite(n.unique_id)}
              onToggleFavorite={onToggleFavorite}
            />
          ))}
        </ul>
      )}
    </div>
  );
}

/* ---------- Filter mode ---------- */

function FilterMode({
  project,
  nodes,
  presentTypes,
  typeCounts,
  typeFilter,
  onShowList,
  filters,
  onSetFilters,
}: {
  project: Project;
  nodes: NodeSummary[];
  presentTypes: Set<string>;
  typeCounts: Map<string, number>;
  typeFilter: string | null;
  onShowList: (type: string | null) => void;
  filters: AssetFilters;
  onSetFilters: (next: AssetFilters) => void;
}) {
  // Cross-filter counts. Each dimension's counts exclude its own filter
  // (so the count tells you "if you toggled THIS, you'd see N rows").
  const layerCounts = useMemo(() => countBy(
    nodes,
    (n) => inferModelingLayer(n.original_file_path),
    (n) =>
      (typeFilter ? n.resource_type === typeFilter : true) &&
      matchesAny(filters.materialization, n.materialized) &&
      matchesAny(filters.pkg, n.package_name),
  ), [nodes, typeFilter, filters.materialization, filters.pkg]);

  const matCounts = useMemo(() => countBy(
    nodes,
    (n) => n.materialized ?? null,
    (n) =>
      (typeFilter ? n.resource_type === typeFilter : true) &&
      matchesAny(filters.modelingLayer, inferModelingLayer(n.original_file_path)) &&
      matchesAny(filters.pkg, n.package_name),
  ), [nodes, typeFilter, filters.modelingLayer, filters.pkg]);

  const pkgCounts = useMemo(() => countBy(
    nodes,
    (n) => n.package_name ?? null,
    (n) =>
      (typeFilter ? n.resource_type === typeFilter : true) &&
      matchesAny(filters.modelingLayer, inferModelingLayer(n.original_file_path)) &&
      matchesAny(filters.materialization, n.materialized),
  ), [nodes, typeFilter, filters.modelingLayer, filters.materialization]);

  const modelingLayers = useMemo(() => [...layerCounts.keys()].sort(), [layerCounts]);
  const materializations = useMemo(() => [...matCounts.keys()].sort(), [matCounts]);
  const packages = useMemo(
    () =>
      [...pkgCounts.keys()].sort((a, b) => {
        const aSelf = a === project.name ? 0 : 1;
        const bSelf = b === project.name ? 0 : 1;
        if (aSelf !== bSelf) return aSelf - bSelf;
        return a.localeCompare(b);
      }),
    [pkgCounts, project.name],
  );

  const toggle = (key: keyof AssetFilters, value: string) => {
    const arr = filters[key];
    const next = arr.includes(value)
      ? arr.filter((v) => v !== value)
      : [...arr, value];
    onSetFilters({ ...filters, [key]: next });
  };

  const clear = (key: keyof AssetFilters) =>
    onSetFilters({ ...filters, [key]: [] });

  const selectAll = (key: keyof AssetFilters, options: string[]) =>
    onSetFilters({ ...filters, [key]: [...options] });

  return (
    <div className="locate-pane__filter">
      {/* Asset type — single-select via list view navigation */}
      <FilterSection title="Asset type">
        <CheckboxRow
          label="All"
          checked={typeFilter === null}
          onChange={() => onShowList(null)}
        />
        {RESOURCE_TYPE_ORDER.filter((t) => presentTypes.has(t)).map((t) => (
          <CheckboxRow
            key={t}
            label={RESOURCE_TYPE_LABEL[t] ?? t}
            count={typeCounts.get(t) ?? 0}
            checked={typeFilter === t}
            onChange={() => onShowList(t)}
            radio
          />
        ))}
      </FilterSection>

      {/* Modeling layer */}
      {modelingLayers.length > 0 && (
        <FilterSection
          title="Modeling layer"
          onSelectAll={() => selectAll("modelingLayer", modelingLayers)}
          onClear={() => clear("modelingLayer")}
        >
          {modelingLayers.map((l) => (
            <CheckboxRow
              key={l}
              label={l}
              count={layerCounts.get(l) ?? 0}
              checked={filters.modelingLayer.includes(l)}
              onChange={() => toggle("modelingLayer", l)}
            />
          ))}
        </FilterSection>
      )}

      {/* Materialization */}
      {materializations.length > 1 && (
        <FilterSection
          title="Materialization"
          onSelectAll={() => selectAll("materialization", materializations)}
          onClear={() => clear("materialization")}
        >
          {materializations.map((m) => (
            <CheckboxRow
              key={m}
              label={m}
              count={matCounts.get(m) ?? 0}
              checked={filters.materialization.includes(m)}
              onChange={() => toggle("materialization", m)}
            />
          ))}
        </FilterSection>
      )}

      {/* Package */}
      {packages.length > 1 && (
        <FilterSection
          title="Package"
          onSelectAll={() => selectAll("pkg", packages)}
          onClear={() => clear("pkg")}
        >
          {packages.map((p) => (
            <CheckboxRow
              key={p}
              label={p}
              count={pkgCounts.get(p) ?? 0}
              checked={filters.pkg.includes(p)}
              onChange={() => toggle("pkg", p)}
              emphasis={p === project.name}
            />
          ))}
        </FilterSection>
      )}

      <p className="locate-pane__filter-hint">
        More dimensions land here as the API exposes them — health, last-run status, owner, tags.
      </p>
    </div>
  );
}

/* ---------- Filter section primitives ---------- */

function FilterSection({
  title,
  children,
  onSelectAll,
  onClear,
}: {
  title: string;
  children: ReactNode;
  onSelectAll?: () => void;
  onClear?: () => void;
}) {
  return (
    <section className="locate-pane__filter-section">
      <header className="locate-pane__filter-section-head">
        <h4>{title}</h4>
      </header>
      <div className="locate-pane__filter-options">{children}</div>
      {(onSelectAll || onClear) && (
        <footer className="locate-pane__filter-section-foot">
          {onSelectAll && (
            <button type="button" className="locate-pane__filter-action" onClick={onSelectAll}>
              Select all
            </button>
          )}
          {onClear && (
            <button type="button" className="locate-pane__filter-action" onClick={onClear}>
              Clear all
            </button>
          )}
        </footer>
      )}
    </section>
  );
}

function CheckboxRow({
  label,
  count,
  checked,
  onChange,
  emphasis,
}: {
  label: string;
  count?: number;
  checked: boolean;
  onChange: () => void;
  emphasis?: boolean;
  /** Kept for API compatibility; the visual is always a checkbox. */
  radio?: boolean;
}) {
  return (
    <label className={`locate-pane__checkbox-row ${checked ? "is-checked" : ""}`}>
      <input
        type="checkbox"
        checked={checked}
        onChange={onChange}
        className="locate-pane__checkbox"
      />
      <span
        className={`locate-pane__checkbox-label ${emphasis ? "is-emphasis" : ""}`}
      >
        {label}
      </span>
      {count !== undefined && (
        <span className="locate-pane__checkbox-count">{count.toLocaleString()}</span>
      )}
    </label>
  );
}

/* ---------- Helpers ---------- */

/** True when `value` matches any of `selected`, or `selected` is empty
 *  (which means "no filter"). Used for multi-select cross-filtering. */
function matchesAny(selected: string[], value: string | null | undefined): boolean {
  if (selected.length === 0) return true;
  if (!value) return false;
  return selected.includes(value);
}

/** Count nodes by a key extractor, only including ones that pass a predicate.
 *  Null/undefined keys are skipped so counts represent real values. */
function countBy<T>(
  items: T[],
  key: (item: T) => string | null | undefined,
  predicate: (item: T) => boolean,
): Map<string, number> {
  const m = new Map<string, number>();
  for (const item of items) {
    if (!predicate(item)) continue;
    const k = key(item);
    if (!k) continue;
    m.set(k, (m.get(k) ?? 0) + 1);
  }
  return m;
}

/* ---------- Schema section (Query mode only) ---------- */

const SCHEMA_PREVIEW_LIMIT = 50;

function SchemaSection({
  tables,
  query,
  onSelectSchemaTable,
  activeSchemaKey,
  elevated,
}: {
  tables: TableInfo[] | null;
  query: string;
  onSelectSchemaTable?: (schema: string, name: string) => void;
  activeSchemaKey?: string | null;
  /** Query mode renders this as the primary nav — no top border, less padding. */
  elevated?: boolean;
}) {
  // Filter by the same topbar search needle that the asset nav uses.
  const filtered = useMemo(() => {
    if (!tables) return null;
    const needle = query.trim().toLowerCase();
    if (!needle) return tables;
    return tables.filter((t) =>
      `${t.schema}.${t.name}`.toLowerCase().includes(needle),
    );
  }, [tables, query]);

  // Split by schema; `dbt` is the project's own metadata, brand-accented.
  const groups = useMemo(() => {
    const dbt: TableInfo[] = [];
    const rt: TableInfo[] = [];
    const other: TableInfo[] = [];
    for (const t of filtered ?? []) {
      if (t.schema === "dbt") dbt.push(t);
      else if (t.schema === "dbt_rt") rt.push(t);
      else other.push(t);
    }
    return { dbt, rt, other };
  }, [filtered]);

  return (
    <section className={`locate-pane__schema-section ${elevated ? "is-elevated" : ""}`}>
      <header className="locate-pane__schema-section-head">
        <Icon ryecon={RyeconDatabase} size="xs" alt="" />
        <span>Schema</span>
      </header>
      {filtered == null ? (
        <p className="locate-pane__schema-muted">Loading tables…</p>
      ) : filtered.length === 0 ? (
        <p className="locate-pane__schema-muted">No tables match.</p>
      ) : (
        <>
          <SchemaGroup
            title="dbt"
            accent="brand"
            tables={groups.dbt}
            defaultOpen
            onSelect={onSelectSchemaTable}
            activeKey={activeSchemaKey}
          />
          <SchemaGroup
            title="dbt_rt"
            tables={groups.rt}
            onSelect={onSelectSchemaTable}
            activeKey={activeSchemaKey}
          />
          {groups.other.length > 0 && (
            <SchemaGroup
              title="other"
              tables={groups.other}
              onSelect={onSelectSchemaTable}
              activeKey={activeSchemaKey}
            />
          )}
        </>
      )}
    </section>
  );
}

function SchemaGroup({
  title,
  tables,
  accent,
  defaultOpen,
  onSelect,
  activeKey,
}: {
  title: string;
  tables: TableInfo[];
  accent?: "brand";
  defaultOpen?: boolean;
  onSelect?: (schema: string, name: string) => void;
  activeKey?: string | null;
}) {
  const [open, setOpen] = useState(!!defaultOpen);
  if (tables.length === 0) return null;
  return (
    <div className="locate-pane__schema-group">
      <button
        type="button"
        className={`locate-pane__schema-group-head ${accent === "brand" ? "is-brand" : ""}`}
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
      >
        <span className="locate-pane__caret">
          <Icon ryecon={RyeconCaretDown} size="xs" alt="" />
        </span>
        <span className="locate-pane__schema-group-title">{title}</span>
        <span className="locate-pane__schema-group-count">{tables.length}</span>
      </button>
      {open && (
        <ul className="locate-pane__schema-list">
          {tables.map((t) => (
            <SchemaRow
              key={`${t.schema}.${t.name}`}
              table={t}
              onSelect={onSelect}
              isActive={activeKey === `${t.schema}.${t.name}`}
            />
          ))}
        </ul>
      )}
    </div>
  );
}

function SchemaRow({
  table,
  onSelect,
  isActive,
}: {
  table: TableInfo;
  onSelect?: (schema: string, name: string) => void;
  isActive?: boolean;
}) {
  const [open, setOpen] = useState(false);
  return (
    <li className={`locate-pane__schema-item ${isActive ? "is-active" : ""}`}>
      <div className={`locate-pane__schema-row ${isActive ? "is-active" : ""}`}>
        <button
          type="button"
          className="locate-pane__schema-toggle"
          onClick={() => setOpen((v) => !v)}
          aria-label={open ? "Collapse columns" : "Expand columns"}
          aria-expanded={open}
        >
          <Icon ryecon={RyeconCaretDown} size="xs" alt="" />
        </button>
        <button
          type="button"
          className="locate-pane__schema-name"
          onClick={() => onSelect?.(table.schema, table.name)}
          title={`SELECT * FROM ${table.schema}.${table.name} LIMIT ${SCHEMA_PREVIEW_LIMIT}`}
        >
          <span className="locate-pane__schema-name-text">{table.name}</span>
          <span className="locate-pane__schema-name-count">{table.columns.length}</span>
        </button>
      </div>
      {open && (
        <ul className="locate-pane__schema-cols">
          {table.columns.map((c) => (
            <li key={c.name}>
              <span className="locate-pane__schema-col-name">{c.name}</span>
              <span className="locate-pane__schema-col-type">{c.data_type}</span>
            </li>
          ))}
        </ul>
      )}
    </li>
  );
}

/* ---------- Shared row ---------- */

function NodeRow({
  node,
  selectedId,
  onSelect,
  isFavorite,
  onToggleFavorite,
}: {
  node: NodeSummary;
  selectedId: string | null;
  onSelect: (id: string) => void;
  isFavorite: boolean;
  onToggleFavorite: (id: string) => void;
}) {
  const ryecon = RESOURCE_TYPE_RYECON[node.resource_type] ?? RyeconFile;
  return (
    <li className={`locate-pane__row ${selectedId === node.unique_id ? "is-active" : ""}`}>
      <button
        type="button"
        className="locate-pane__row-main"
        onClick={() => onSelect(node.unique_id)}
        title={node.unique_id}
      >
        <Icon ryecon={ryecon} size="xs" alt="" />
        <span className="locate-pane__row-name">{node.name}</span>
      </button>
      <button
        type="button"
        className={`locate-pane__row-fav ${isFavorite ? "is-on" : ""}`}
        onClick={(e) => {
          e.stopPropagation();
          onToggleFavorite(node.unique_id);
        }}
        aria-label={isFavorite ? "Remove favorite" : "Add favorite"}
      >
        <Icon ryecon={RyeconStar} size="xs" alt="" />
      </button>
    </li>
  );
}
