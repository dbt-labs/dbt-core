import { useEffect, useMemo, useRef, useState } from "react";
import {
  Icon,
  RyeconDataGeography,
  RyeconDatabase,
  RyeconFile,
  RyeconHome,
  RyeconLineage,
  RyeconMagnifyingGlass,
  RyeconStar,
  RyeconThemeDark,
  RyeconThemeLight,
} from "@dbt-labs/sourdough";
import type { Ryecon } from "@dbt-labs/sourdough";
import type { NodeSummary, TableInfo } from "../api";
import { RESOURCE_TYPE_SINGULAR, ryeconForType } from "../lib/resourceType";

/** Global quick-find. ⌘K from anywhere. Lets a user jump to any asset,
 *  any schema table (when Query mode is alive), or trigger an action
 *  (switch mode, toggle theme, go home, favorite) without leaving the
 *  keyboard. Strategy hook: surfaces "Open in dbt platform" when the
 *  capability is present — gentle, not pushy. */

interface Props {
  isOpen: boolean;
  onClose(): void;
  nodes: NodeSummary[];
  recentIds: string[];
  favoriteIds: Set<string>;
  tables: TableInfo[] | null;
  mode: "docs" | "query";
  theme: "dark" | "light";
  /** True when this asset is currently in <main>. Used for the favorite action. */
  currentDetailId: string | null;
  onSelectNode(id: string): void;
  onSelectSchemaTable(schema: string, name: string): void;
  onSetMode(mode: "docs" | "query"): void;
  onSetTheme(theme: "dark" | "light"): void;
  onResetHome(): void;
  onShowList(type: string | null): void;
  onToggleFavorite(id: string): void;
}

type Group = { title: string; items: Item[] };

type Item =
  | { id: string; kind: "node"; node: NodeSummary }
  | {
      id: string;
      kind: "schema";
      schema: string;
      name: string;
      columnCount: number;
    }
  | {
      id: string;
      kind: "action";
      label: string;
      hint?: string;
      ryecon: Ryecon;
      perform(): void;
    };

const MAX_ASSETS = 40;
const MAX_SCHEMA = 12;
const MAX_RECENTS = 6;

export function CommandPalette({
  isOpen,
  onClose,
  nodes,
  recentIds,
  favoriteIds,
  tables,
  mode,
  theme,
  currentDetailId,
  onSelectNode,
  onSelectSchemaTable,
  onSetMode,
  onSetTheme,
  onResetHome,
  onShowList,
  onToggleFavorite,
}: Props) {
  const [query, setQuery] = useState("");
  const [selected, setSelected] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const listRef = useRef<HTMLDivElement>(null);

  // Reset state every open. Auto-focus the input.
  useEffect(() => {
    if (isOpen) {
      setQuery("");
      setSelected(0);
      const t = window.setTimeout(() => inputRef.current?.focus(), 10);
      return () => window.clearTimeout(t);
    }
  }, [isOpen]);

  // Reset selection when the result set changes.
  useEffect(() => setSelected(0), [query, mode]);

  const nodesById = useMemo(() => {
    const m = new Map<string, NodeSummary>();
    for (const n of nodes) m.set(n.unique_id, n);
    return m;
  }, [nodes]);

  /** Quick-and-honest fuzzy scorer: starts-with > word-start > includes.
   *  Returns null when no match, lower score = better match. */
  function score(needle: string, hay: string): number | null {
    const h = hay.toLowerCase();
    const idx = h.indexOf(needle);
    if (idx === -1) return null;
    if (idx === 0) return 0;
    if (h[idx - 1] === "_" || h[idx - 1] === ".") return 1;
    return 2 + idx;
  }

  const groups: Group[] = useMemo(() => {
    const q = query.trim().toLowerCase();
    const built: Group[] = [];

    // ----- ACTIONS (always available, filtered by needle) -----
    const allActions: Item[] = [];
    allActions.push({
      id: "action:home",
      kind: "action",
      label: "Go home",
      hint: "Project hub",
      ryecon: RyeconHome,
      perform: () => {
        onResetHome();
        onClose();
      },
    });
    allActions.push({
      id: "action:mode-toggle",
      kind: "action",
      label: mode === "query" ? "Switch to Docs" : "Switch to Query",
      hint: mode === "query" ? "Browse the catalog" : "Run SQL against your project metadata",
      ryecon: mode === "query" ? RyeconDataGeography : RyeconDatabase,
      perform: () => {
        onSetMode(mode === "query" ? "docs" : "query");
        onClose();
      },
    });
    allActions.push({
      id: "action:theme",
      kind: "action",
      label: `Switch to ${theme === "dark" ? "light" : "dark"} theme`,
      ryecon: theme === "dark" ? RyeconThemeLight : RyeconThemeDark,
      perform: () => {
        onSetTheme(theme === "dark" ? "light" : "dark");
        onClose();
      },
    });
    allActions.push({
      id: "action:browse-models",
      kind: "action",
      label: "Browse all models",
      hint: "Open the models list",
      ryecon: ryeconForType("model"),
      perform: () => {
        onShowList("model");
        onClose();
      },
    });
    if (currentDetailId) {
      const isFav = favoriteIds.has(currentDetailId);
      allActions.push({
        id: "action:favorite",
        kind: "action",
        label: isFav ? "Unfavorite this asset" : "Favorite this asset",
        ryecon: RyeconStar,
        perform: () => {
          onToggleFavorite(currentDetailId);
          onClose();
        },
      });
    }
    // Strategy hook — column lineage as a discoverable platform-tier capability.
    allActions.push({
      id: "action:column-lineage",
      kind: "action",
      label: "View column lineage",
      hint: "Available in dbt platform",
      ryecon: RyeconLineage,
      perform: () => {
        window.open("https://www.getdbt.com/product/column-level-lineage", "_blank");
        onClose();
      },
    });

    // ----- RECENTS / ASSETS -----
    if (q.length === 0) {
      const recents = recentIds
        .map((id) => nodesById.get(id))
        .filter((n): n is NodeSummary => !!n)
        .slice(0, MAX_RECENTS)
        .map<Item>((n) => ({ id: `node:${n.unique_id}`, kind: "node", node: n }));
      if (recents.length > 0) {
        built.push({ title: "Recent", items: recents });
      }
      built.push({ title: "Actions", items: allActions });
      // Tease the schema if we're in Query mode but the user hasn't typed yet.
      if (mode === "query" && tables && tables.length > 0) {
        const teaser = tables
          .filter((t) => t.schema === "dbt")
          .slice(0, 4)
          .map<Item>((t) => ({
            id: `schema:${t.schema}.${t.name}`,
            kind: "schema",
            schema: t.schema,
            name: t.name,
            columnCount: t.columns.length,
          }));
        if (teaser.length > 0) {
          built.push({ title: "Schema · dbt", items: teaser });
        }
      }
      return built;
    }

    // Typed query — rank assets, schema, and actions.
    type Scored<T> = { score: number; item: T };
    const assetMatches: Scored<NodeSummary>[] = [];
    for (const n of nodes) {
      const s = Math.min(
        score(q, n.name) ?? Infinity,
        (score(q, n.unique_id) ?? Infinity) + 1,
      );
      if (s === Infinity) continue;
      assetMatches.push({ score: s, item: n });
      if (assetMatches.length > MAX_ASSETS * 4) break; // cap traversal
    }
    assetMatches.sort((a, b) => a.score - b.score || a.item.name.localeCompare(b.item.name));
    const assets = assetMatches.slice(0, MAX_ASSETS).map<Item>(({ item: n }) => ({
      id: `node:${n.unique_id}`,
      kind: "node",
      node: n,
    }));
    if (assets.length > 0) {
      built.push({ title: `Assets · ${assetMatches.length.toLocaleString()} matches`, items: assets });
    }

    const schemaMatches: Scored<{
      schema: string;
      name: string;
      columnCount: number;
    }>[] = [];
    for (const t of tables ?? []) {
      const s = score(q, t.name);
      if (s == null) continue;
      schemaMatches.push({
        score: s,
        item: { schema: t.schema, name: t.name, columnCount: t.columns.length },
      });
    }
    schemaMatches.sort((a, b) => a.score - b.score);
    const schemaItems = schemaMatches.slice(0, MAX_SCHEMA).map<Item>(({ item: t }) => ({
      id: `schema:${t.schema}.${t.name}`,
      kind: "schema",
      schema: t.schema,
      name: t.name,
      columnCount: t.columnCount,
    }));
    if (schemaItems.length > 0) {
      built.push({ title: "Schema tables", items: schemaItems });
    }

    const actionMatches = allActions.filter((a) =>
      a.kind === "action" && a.label.toLowerCase().includes(q),
    );
    if (actionMatches.length > 0) {
      built.push({ title: "Actions", items: actionMatches });
    }

    return built;
  }, [
    query,
    nodes,
    nodesById,
    tables,
    mode,
    theme,
    recentIds,
    favoriteIds,
    currentDetailId,
    onResetHome,
    onSetMode,
    onSetTheme,
    onShowList,
    onToggleFavorite,
    onClose,
  ]);

  const flatItems = useMemo(() => groups.flatMap((g) => g.items), [groups]);
  const noResults = flatItems.length === 0;

  // Clamp selection within bounds whenever the list shrinks.
  useEffect(() => {
    if (selected >= flatItems.length) setSelected(Math.max(0, flatItems.length - 1));
  }, [flatItems.length, selected]);

  // Keep the selected row in view.
  useEffect(() => {
    const el = listRef.current?.querySelector<HTMLElement>(
      `[data-palette-idx="${selected}"]`,
    );
    el?.scrollIntoView({ block: "nearest" });
  }, [selected]);

  function activate(item: Item) {
    if (item.kind === "node") {
      onSelectNode(item.node.unique_id);
      onClose();
    } else if (item.kind === "schema") {
      if (mode !== "query") onSetMode("query");
      onSelectSchemaTable(item.schema, item.name);
      onClose();
    } else {
      item.perform();
    }
  }

  function onInputKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setSelected((s) => (flatItems.length ? (s + 1) % flatItems.length : 0));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setSelected((s) =>
        flatItems.length ? (s - 1 + flatItems.length) % flatItems.length : 0,
      );
    } else if (e.key === "Enter") {
      e.preventDefault();
      const item = flatItems[selected];
      if (item) activate(item);
    } else if (e.key === "Escape") {
      e.preventDefault();
      onClose();
    }
  }

  // Global keymap close (covers Escape when focus drifts off the input).
  useEffect(() => {
    if (!isOpen) return;
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  // Walk groups to give each item a stable flat index for keyboard nav.
  let flatIdx = -1;

  return (
    <div
      className="palette-backdrop"
      role="dialog"
      aria-modal="true"
      aria-label="Quick find"
      onMouseDown={(e) => {
        // Click on the backdrop (not on the panel) closes.
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className="palette" onMouseDown={(e) => e.stopPropagation()}>
        <div className="palette__input-row">
          <Icon ryecon={RyeconMagnifyingGlass} size="sm" alt="Search" />
          <input
            ref={inputRef}
            type="text"
            className="palette__input"
            placeholder={
              mode === "query"
                ? "Find assets, schema tables, or actions…"
                : "Find assets or actions…"
            }
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={onInputKeyDown}
            aria-label="Quick find"
          />
          <kbd className="palette__kbd">esc</kbd>
        </div>

        <div ref={listRef} className="palette__list">
          {noResults && (
            <p className="palette__empty">
              No matches for "{query.trim()}". Try a shorter prefix, or press <kbd>esc</kbd>.
            </p>
          )}
          {groups.map((g) => (
            <section key={g.title} className="palette__group">
              <h4 className="palette__group-title">{g.title}</h4>
              <ul className="palette__group-list">
                {g.items.map((item) => {
                  flatIdx += 1;
                  const idx = flatIdx;
                  const isSelected = idx === selected;
                  return (
                    <li key={item.id}>
                      <button
                        type="button"
                        data-palette-idx={idx}
                        className={`palette__row ${isSelected ? "is-selected" : ""}`}
                        onMouseEnter={() => setSelected(idx)}
                        onClick={() => activate(item)}
                      >
                        <PaletteRowContent item={item} />
                        {isSelected && (
                          <span className="palette__row-hint" aria-hidden>↵</span>
                        )}
                      </button>
                    </li>
                  );
                })}
              </ul>
            </section>
          ))}
        </div>

        <footer className="palette__foot">
          <span>
            <kbd>↑</kbd>
            <kbd>↓</kbd> navigate
          </span>
          <span>
            <kbd>↵</kbd> open
          </span>
          <span>
            <kbd>esc</kbd> close
          </span>
        </footer>
      </div>
    </div>
  );
}

function PaletteRowContent({ item }: { item: Item }) {
  if (item.kind === "node") {
    const n = item.node;
    return (
      <>
        <Icon ryecon={ryeconForType(n.resource_type)} size="xs" alt="" />
        <span className="palette__row-primary">{n.name}</span>
        <span className="palette__row-meta">
          {RESOURCE_TYPE_SINGULAR[n.resource_type] ?? n.resource_type}
          {n.package_name ? ` · ${n.package_name}` : ""}
        </span>
      </>
    );
  }
  if (item.kind === "schema") {
    return (
      <>
        <Icon ryecon={RyeconDatabase} size="xs" alt="" />
        <span className="palette__row-primary palette__row-mono">
          {item.schema}.{item.name}
        </span>
        <span className="palette__row-meta">
          {item.columnCount} {item.columnCount === 1 ? "column" : "columns"}
        </span>
      </>
    );
  }
  return (
    <>
      <Icon ryecon={item.ryecon ?? RyeconFile} size="xs" alt="" />
      <span className="palette__row-primary">{item.label}</span>
      {item.hint && <span className="palette__row-meta">{item.hint}</span>}
    </>
  );
}
