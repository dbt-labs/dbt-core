import { useMemo, useState } from "react";
import { Icon, Table } from "@dbt-labs/sourdough";
import type { SortingState } from "@dbt-labs/sourdough";
import { createColumnHelper } from "@tanstack/react-table";
import type { NodeSummary, Project } from "../api";
import type { AssetFilters } from "../App";
import {
  RESOURCE_TYPE_LABEL,
  ResourceBadge,
  inferModelingLayer,
  ryeconForType,
} from "../lib/resourceType";

const PAGE_SIZE = 200;

interface Props {
  project: Project;
  nodes: NodeSummary[];
  /** null = "All assets" across every package + every type. */
  type: string | null;
  /** Free-text search from the topbar. */
  query: string;
  /** Filters set in the locate pane's Filter mode. */
  filters: AssetFilters;
  /** Currently previewed node — highlights the row. */
  previewId: string | null;
  /** Single-click on a row opens the peek drawer. */
  onPeek(uniqueId: string): void;
}

type Row = NodeSummary & {
  modeling_layer: string;
};

const columnHelper = createColumnHelper<Row>();

export function AssetListView({
  project,
  nodes,
  type,
  query,
  filters,
  previewId,
  onPeek,
}: Props) {
  const [shown, setShown] = useState<number>(PAGE_SIZE);
  /** Sourdough Table only reports sort changes via onChangeSort — it doesn't
   *  re-order the data itself. We sort in the consumer so clicks actually
   *  reorder rows. */
  const [sortBy, setSortBy] = useState<SortingState>([{ id: "name", desc: false }]);

  const filtered = useMemo(() => {
    const needle = query.trim().toLowerCase();
    const ml = filters.modelingLayer;
    const mt = filters.materialization;
    const pk = filters.pkg;
    const passes: Row[] = [];
    for (const n of nodes) {
      if (type && n.resource_type !== type) continue;
      const layer = inferModelingLayer(n.original_file_path);
      if (ml.length > 0 && (!layer || !ml.includes(layer))) continue;
      if (mt.length > 0 && (!n.materialized || !mt.includes(n.materialized))) continue;
      if (pk.length > 0 && (!n.package_name || !pk.includes(n.package_name))) continue;
      if (
        needle &&
        !n.name.toLowerCase().includes(needle) &&
        !n.unique_id.toLowerCase().includes(needle)
      ) {
        continue;
      }
      passes.push({ ...n, modeling_layer: layer ?? "—" });
    }
    return passes;
  }, [nodes, type, filters, query]);

  const typeTotal = useMemo(
    () => (type ? nodes.filter((n) => n.resource_type === type).length : nodes.length),
    [nodes, type],
  );

  const sorted = useMemo(() => {
    if (sortBy.length === 0) return filtered;
    const [{ id, desc }] = sortBy;
    const sign = desc ? -1 : 1;
    return filtered.slice().sort((a, b) => {
      const va = (a as unknown as Record<string, unknown>)[id];
      const vb = (b as unknown as Record<string, unknown>)[id];
      if (va == null && vb == null) return 0;
      if (va == null) return 1;
      if (vb == null) return -1;
      if (typeof va === "number" && typeof vb === "number") return (va - vb) * sign;
      return String(va).localeCompare(String(vb), undefined, { numeric: true, sensitivity: "base" }) * sign;
    });
  }, [filtered, sortBy]);

  const visible = sorted.slice(0, shown);
  const hasMore = shown < sorted.length;
  const title = type ? RESOURCE_TYPE_LABEL[type] ?? type : "All assets";

  const columns = useMemo(() => {
    const cols = [
      columnHelper.accessor("name", {
        header: "Name",
        cell: (info) => {
          const n = info.row.original;
          return (
            <div className="asset-list__name-cell">
              <Icon ryecon={ryeconForType(n.resource_type)} size="xs" alt="" />
              <span className="asset-list__name-text">{info.getValue()}</span>
            </div>
          );
        },
      }),
    ];
    if (!type) {
      cols.push(
        columnHelper.accessor("resource_type", {
          header: "Type",
          cell: (info) => <ResourceBadge type={info.getValue()} size="xs" />,
        }) as never,
      );
    }
    cols.push(
      columnHelper.accessor("package_name", {
        header: "Package",
        cell: (info) => {
          const pkg = info.getValue() ?? "—";
          const isSelf = pkg === project.name;
          return (
            <span className={isSelf ? "asset-list__pkg-self" : ""}>{pkg}</span>
          );
        },
      }) as never,
      columnHelper.accessor("modeling_layer", {
        header: "Modeling layer",
      }) as never,
      columnHelper.accessor("materialized", {
        header: "Materialization",
        cell: (info) => info.getValue() ?? "—",
      }) as never,
      columnHelper.accessor("schema_name", {
        header: "Schema",
        cell: (info) => (
          <span className="asset-list__mono">{info.getValue() ?? "—"}</span>
        ),
      }) as never,
      columnHelper.accessor("description", {
        header: "Description",
        cell: (info) => {
          const v = info.getValue()?.trim();
          return v ? (
            <span className="asset-list__desc-cell">{v}</span>
          ) : (
            <span className="faint">—</span>
          );
        },
      }) as never,
    );
    return cols;
  }, [type, project.name]);

  return (
    <div className="asset-list">
      <nav className="asset-list__breadcrumb" aria-label="Breadcrumb">
        <span>{project.name}</span>
        <span aria-hidden>/</span>
        <span className="asset-list__breadcrumb-current">{title}</span>
      </nav>

      <header className="asset-list__header">
        <div className="asset-list__title-row">
          {type && <Icon ryecon={ryeconForType(type)} size="md" alt="" />}
          <h1 className="asset-list__title">{title}</h1>
        </div>
        <div className="asset-list__count">
          {filtered.length.toLocaleString()} of {typeTotal.toLocaleString()}
        </div>
      </header>

      <p className="asset-list__filter-caption">
        Click a column header to sort. Use the filter pane to narrow results.
      </p>

      <ActiveFilters filters={filters} />

      {filtered.length === 0 ? (
        <p className="asset-list__empty">
          {query.trim()
            ? `No matches for "${query.trim()}" with the current filters.`
            : "No assets match the current filters. Clear filters in the side panel to widen results."}
        </p>
      ) : (
        <div className="asset-list__sourdough-wrap">
          <Table
            columns={columns}
            data={visible}
            isSortable
            initialSortColumn="name"
            onChangeSort={setSortBy}
            cellVariant="COMPACT"
            onRowClickCustomHandler={(row) => onPeek(row.unique_id)}
          />
        </div>
      )}

      <footer className="asset-list__footer">
        {hasMore ? (
          <button
            type="button"
            className="asset-list__load-more"
            onClick={() => setShown((s) => s + PAGE_SIZE)}
          >
            Load {Math.min(PAGE_SIZE, filtered.length - shown)} more…
          </button>
        ) : null}
        <div className="asset-list__progress">
          Showing {visible.length.toLocaleString()} of {filtered.length.toLocaleString()}
          {previewId && filtered.find((n) => n.unique_id === previewId) ? " · 1 previewed" : ""}
        </div>
      </footer>
    </div>
  );
}

/* ---------- Active filter summary (unchanged) ---------- */

function ActiveFilters({ filters }: { filters: AssetFilters }) {
  const pills: { label: string; values: string[] }[] = [];
  if (filters.modelingLayer.length > 0) pills.push({ label: "Modeling layer", values: filters.modelingLayer });
  if (filters.materialization.length > 0) pills.push({ label: "Materialization", values: filters.materialization });
  if (filters.pkg.length > 0) pills.push({ label: "Package", values: filters.pkg });

  if (pills.length === 0) return null;

  return (
    <div className="asset-list__active-filters">
      <span className="asset-list__active-filters-label">Filters</span>
      {pills.map((p) => (
        <span key={p.label} className="asset-list__active-filter">
          <span className="asset-list__active-filter-key">{p.label}:</span>
          <span className="asset-list__active-filter-val">{p.values.join(", ")}</span>
        </span>
      ))}
    </div>
  );
}
