import { useCallback, useEffect, useRef, useState } from "react";
import { Icon, RyeconColorDbt, RyeconMagnifyingGlass, SegmentedButton } from "@dbt-labs/sourdough";
import {
  api,
  type Capabilities,
  type NodeDetail as NodeDetailT,
  type NodeSummary,
  type Project,
  type TableInfo,
} from "./api";
import { LocatePane } from "./components/LocatePane";
import { NodeDetail } from "./components/NodeDetail";
import { CatalogHome } from "./components/CatalogHome";
import { AssetListView } from "./components/AssetListView";
import { PreviewDrawer } from "./components/PreviewDrawer";
import { CommandPalette } from "./components/CommandPalette";
// QueryPane is Elias's SQL/duckdb experiment (#view=query). Currently
// reachable only via direct URL — toggle UI deliberately hidden so the
// design demo stays focused on Catalog. Backend wiring stays intact.
import { QueryPane } from "./components/QueryPane";
import { useTheme } from "./hooks/useTheme";
import { useFavorites } from "./hooks/useFavorites";
import { useRecents } from "./hooks/useRecents";
import { useResizable } from "./hooks/useResizable";

/** View is the discriminated union that drives main-pane routing.
 *  - home: CatalogHome
 *  - list: AssetListView (type=null means "All assets")
 *  - detail: NodeDetail */
export type View =
  | { kind: "home" }
  | { kind: "list"; type: string | null }
  | { kind: "detail"; uniqueId: string };

function readViewFromHash(): View {
  const h = window.location.hash;
  const nodeMatch = h.match(/#node=([^&]+)/);
  if (nodeMatch) {
    return { kind: "detail", uniqueId: decodeURIComponent(nodeMatch[1]) };
  }
  const listMatch = h.match(/#list=([^&]+)/);
  if (listMatch) {
    const raw = decodeURIComponent(listMatch[1]);
    return { kind: "list", type: raw === "all" ? null : raw };
  }
  return { kind: "home" };
}

function hashFor(view: View): string {
  switch (view.kind) {
    case "home":
      return "";
    case "list":
      return `#list=${view.type ? encodeURIComponent(view.type) : "all"}`;
    case "detail":
      return `#node=${encodeURIComponent(view.uniqueId)}`;
  }
}

/** Shared filter state — surfaced via LocatePane Filter mode, applied by
 *  AssetListView. Empty arrays mean "no narrowing"; multiple values are
 *  OR'd within a dimension and AND'd across dimensions. */
export type AssetFilters = {
  modelingLayer: string[];
  materialization: string[];
  pkg: string[];
};
const EMPTY_FILTERS: AssetFilters = {
  modelingLayer: [],
  materialization: [],
  pkg: [],
};

const NODE_PAGE_SIZE = 1000;

export default function App() {
  const theme = useTheme();
  const favorites = useFavorites();
  const recents = useRecents();
  const locateWidth = useResizable("dbt-docs-v2:locate-pane-w", 320, 220, 480);
  const [project, setProject] = useState<Project | null>(null);
  const [capabilities, setCapabilities] = useState<Capabilities | null>(null);
  const [nodes, setNodes] = useState<NodeSummary[] | null>(null);
  const [nodeTotal, setNodeTotal] = useState<number | null>(null);
  const [view, setView] = useState<View>(() => readViewFromHash());
  const [isQueryMode, setIsQueryMode] = useState<boolean>(
    () => window.location.hash.includes("view=query"),
  );
  const [detail, setDetail] = useState<NodeDetailT | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [filters, setFilters] = useState<AssetFilters>(EMPTY_FILTERS);
  const [previewId, setPreviewId] = useState<string | null>(null);
  const [previewCache, setPreviewCache] = useState<Map<string, NodeDetailT>>(
    () => new Map(),
  );
  /** Tables (DuckDB schema) — loaded once, shared between LocatePane's
   *  Schema section and QueryPane (editor autocomplete + table previews). */
  const [tables, setTables] = useState<TableInfo[] | null>(null);
  /** Token-bumped each time the user clicks a schema table in LocatePane.
   *  QueryPane uses the token as a useEffect dep to insert + run the SQL. */
  const [pendingSql, setPendingSql] = useState<{ sql: string; token: number } | null>(null);
  /** Global quick-find — opens with ⌘K (or Ctrl+K). */
  const [paletteOpen, setPaletteOpen] = useState(false);

  const selectedId = view.kind === "detail" ? view.uniqueId : null;
  const previewDetail = previewId ? previewCache.get(previewId) ?? null : null;

  // Spin the topbar dbt mark briefly whenever a "parent filter" changes —
  // active view kind/type or the package filter. Detail navigations don't
  // trigger; this is meant for orientation moments.
  const [spinTrigger, setSpinTrigger] = useState(0);
  const firstRender = useRef(true);
  const parentKey = `${view.kind === "list" ? view.type ?? "all" : view.kind === "home" ? "home" : "detail"}|${filters.pkg.slice().sort().join(",")}`;
  useEffect(() => {
    if (firstRender.current) {
      firstRender.current = false;
      return;
    }
    setSpinTrigger((s) => s + 1);
  }, [parentKey]);

  // Load DuckDB schema once for both LocatePane (Query mode) and QueryPane.
  useEffect(() => {
    let cancelled = false;
    api
      .tables()
      .then((t) => {
        if (!cancelled) setTables(t);
      })
      .catch(() => {
        // Schema is best-effort; QueryPane can still run without autocomplete.
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // Load shell data on mount, then page through every remaining node.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const [p, c, firstPage] = await Promise.all([
          api.project(),
          api.capabilities(),
          api.nodes({ limit: NODE_PAGE_SIZE, offset: 0 }),
        ]);
        if (cancelled) return;
        setProject(p);
        setCapabilities(c);
        setNodeTotal(firstPage.total);

        const all: NodeSummary[] = [...firstPage.nodes];
        setNodes(all);

        let offset = firstPage.nodes.length;
        while (offset < firstPage.total) {
          const page = await api.nodes({
            limit: NODE_PAGE_SIZE,
            offset,
          });
          if (cancelled) return;
          all.push(...page.nodes);
          setNodes([...all]);
          offset += page.nodes.length;
          if (page.nodes.length === 0) break;
        }
      } catch (e: unknown) {
        if (!cancelled) {
          setError(e instanceof Error ? e.message : String(e));
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  // Load detail whenever selection changes.
  useEffect(() => {
    if (!selectedId) {
      setDetail(null);
      return;
    }
    let cancelled = false;
    setDetailLoading(true);
    api
      .node(selectedId)
      .then((d) => {
        if (!cancelled) setDetail(d);
      })
      .catch((e: unknown) => {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => {
        if (!cancelled) setDetailLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [selectedId]);

  // Keep hash in sync so deep links work + back-button transitions land
  // on the previous view.
  useEffect(() => {
    const desired = hashFor(view);
    if (window.location.hash !== desired) {
      window.history.pushState(
        null,
        "",
        `${window.location.pathname}${desired}`,
      );
    }
  }, [view]);

  // React to back/forward.
  useEffect(() => {
    const onHash = () => {
      setView(readViewFromHash());
      setIsQueryMode(window.location.hash.includes("view=query"));
    };
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  const onSelect = useCallback(
    (id: string) => {
      setView({ kind: "detail", uniqueId: id });
      recents.push(id);
      // Drilling into the full asset page closes the peek drawer — we
      // don't want the same asset open in two surfaces at once.
      setPreviewId(null);
      // If the user opens a node from Query mode (e.g., clicking a result
      // row's unique_id or a DAG lineage node), drop them into Docs so the
      // NodeDetail page actually renders.
      if (isQueryMode) {
        window.history.pushState(
          null,
          "",
          `${window.location.pathname}#node=${encodeURIComponent(id)}`,
        );
        setIsQueryMode(false);
      }
    },
    [recents, isQueryMode],
  );

  const onShowList = useCallback((type: string | null) => {
    setView({ kind: "list", type });
  }, []);

  const onShowAll = useCallback(() => {
    // Reset filters so the resulting table is a clean cross-package "all".
    setFilters(EMPTY_FILTERS);
    setView({ kind: "list", type: null });
  }, []);

  // Filter changes always drop us into the list view so the user can see
  // the impact immediately. Preserves the active type if one is set.
  const onSetFilters = useCallback((next: AssetFilters) => {
    setFilters(next);
    setView((v) => (v.kind === "list" ? v : { kind: "list", type: null }));
  }, []);

  // Top-left dbt logo → home reset (clear filters + search, return to home).
  const onResetHome = useCallback(() => {
    setFilters(EMPTY_FILTERS);
    setSearch("");
    setView({ kind: "home" });
    setPreviewId(null);
    if (isQueryMode) {
      window.history.pushState(null, "", window.location.pathname);
      setIsQueryMode(false);
    }
  }, [isQueryMode]);

  // Topbar Docs / Query toggle. Writes the hash directly so deep-links and
  // back/forward keep working.
  const onSetMode = useCallback((next: "docs" | "query") => {
    if (next === "query") {
      window.history.pushState(null, "", `${window.location.pathname}#view=query`);
      setIsQueryMode(true);
    } else {
      // Drop back to home. View stays whatever it was (typically home).
      window.history.pushState(null, "", window.location.pathname);
      setIsQueryMode(false);
    }
  }, []);

  /** Identifies the schema table currently driving the editor. Drives the
   *  brand-indigo "active" treatment in the LocatePane Schema list so the
   *  user can tell at a glance what they're querying. */
  const [activeSchemaKey, setActiveSchemaKey] = useState<string | null>(null);

  /** Open peek drawer for an asset clicked inside Query mode. Switches
   *  back to Docs first (drawer is a Docs surface) so the user can see
   *  the asset metadata without losing context — they can click the title
   *  to drill into the full NodeDetail. */
  const onPeekFromQuery = useCallback(
    (id: string) => {
      window.history.pushState(null, "", window.location.pathname);
      setIsQueryMode(false);
      setPreviewId(id);
      recents.push(id);
    },
    [recents],
  );

  // Schema-table click in LocatePane (Query mode). Token bump lets QueryPane
  // re-fire setSqlAndRun even if the same table is clicked twice.
  const onSelectSchemaTable = useCallback((schema: string, name: string) => {
    setPendingSql({
      sql: `SELECT * FROM ${schema}.${name} LIMIT 50;`,
      token: Date.now(),
    });
    setActiveSchemaKey(`${schema}.${name}`);
  }, []);

  /** Open the peek drawer for a node. Lazy-fetches detail and caches it. */
  const onPeek = useCallback(
    (id: string) => {
      setPreviewId(id);
      recents.push(id);
    },
    [recents],
  );

  const onClosePeek = useCallback(() => setPreviewId(null), []);

  // Lazy fetch + cache for peek-drawer details. Skips if already cached.
  useEffect(() => {
    if (!previewId || previewCache.has(previewId)) return;
    let cancelled = false;
    const t = window.setTimeout(() => {
      api
        .node(previewId)
        .then((d) => {
          if (cancelled) return;
          setPreviewCache((prev) => {
            const next = new Map(prev);
            next.set(previewId, d);
            return next;
          });
        })
        .catch(() => {
          // Swallow — drawer falls back to summary-only render.
        });
    }, 100); // debounce: coalesce rapid clicks
    return () => {
      cancelled = true;
      window.clearTimeout(t);
    };
  }, [previewId, previewCache]);

  // Global Cmd/Ctrl+K opens the command palette from anywhere.
  // Capture so we win over the browser's "search-in-page" / address bar.
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      const isCmdK = (e.metaKey || e.ctrlKey) && (e.key === "k" || e.key === "K");
      if (isCmdK) {
        e.preventDefault();
        setPaletteOpen((v) => !v);
      }
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  // ESC closes the drawer.
  useEffect(() => {
    if (!previewId) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setPreviewId(null);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [previewId]);

  if (error) {
    return (
      <div className="app">
        <Topbar project={null} search="" onSearch={() => {}} />
        <div style={{ padding: 32 }}>
          <div className="err">
            <strong>Server error</strong>
            <pre style={{ margin: "8px 0 0", whiteSpace: "pre-wrap" }}>
              {error}
            </pre>
          </div>
        </div>
      </div>
    );
  }

  if (!project || !capabilities || !nodes) {
    return (
      <div className="app">
        <Topbar project={null} search="" onSearch={() => {}} />
        <div className="muted" style={{ padding: 32 }}>
          Loading…
        </div>
      </div>
    );
  }

  // Unified shell — LocatePane on the left in both modes; <main> swaps
  // between Catalog content and QueryPane based on `isQueryMode`. Keeps the
  // rail stable across the Docs ↔ Query transition.
  return (
    <div className="app">
      <Topbar
        project={project}
        search={search}
        onSearch={setSearch}
        onResetHome={onResetHome}
        spinTrigger={spinTrigger}
        mode={isQueryMode ? "query" : "docs"}
        onSetMode={onSetMode}
        onOpenPalette={() => setPaletteOpen(true)}
      />
      <div
        className={`shell ${previewId ? "has-preview" : ""}`}
        style={{ "--locate-pane-w": `${locateWidth.width}px` } as React.CSSProperties}
      >
        <LocatePane
          project={project}
          nodes={nodes}
          selectedId={selectedId}
          previewId={previewId}
          activeListType={view.kind === "list" ? view.type : undefined}
          onPeek={onPeek}
          onShowList={onShowList}
          onShowAll={onShowAll}
          query={search}
          loadingProgress={
            nodeTotal != null
              ? { loaded: nodes.length, total: nodeTotal }
              : null
          }
          theme={theme.theme}
          onSetTheme={theme.setTheme}
          favorites={favorites}
          filters={filters}
          onSetFilters={onSetFilters}
          isQueryMode={isQueryMode}
          tables={tables}
          onSelectSchemaTable={onSelectSchemaTable}
          activeSchemaKey={activeSchemaKey}
          onSwitchToDocs={() => onSetMode("docs")}
        />
        <div
          className="shell__resize"
          role="separator"
          aria-orientation="vertical"
          aria-label="Resize side panel"
          onPointerDown={locateWidth.startDrag}
        />
        <main className="main">
          {isQueryMode ? (
            <QueryPane
              tables={tables}
              pendingSql={pendingSql}
              onOpenNode={onPeekFromQuery}
            />
          ) : (
            <>
              {view.kind === "home" && (
                <CatalogHome
                  project={project}
                  nodes={nodes}
                  capabilities={capabilities}
                  favoriteIds={favorites.ids}
                  recentIds={recents.ids}
                  previewId={previewId}
                  onPeek={onPeek}
                  onShowList={onShowList}
                />
              )}
              {view.kind === "list" && (
                <AssetListView
                  project={project}
                  nodes={nodes}
                  type={view.type}
                  query={search}
                  filters={filters}
                  previewId={previewId}
                  onPeek={onPeek}
                />
              )}
              {view.kind === "detail" &&
                (detailLoading || !detail ? (
                  <div className="main-inner muted">Loading…</div>
                ) : (
                  <NodeDetail
                    node={detail}
                    isFavorite={favorites.has(detail.unique_id)}
                    onToggleFavorite={favorites.toggle}
                    onSelect={onSelect}
                  />
                ))}
            </>
          )}
        </main>
        {previewId && !isQueryMode && (
          <PreviewDrawer
            project={project}
            previewId={previewId}
            summary={nodes.find((n) => n.unique_id === previewId) ?? null}
            detail={previewDetail}
            isFavorite={favorites.has(previewId)}
            onToggleFavorite={favorites.toggle}
            onClose={onClosePeek}
            onOpenFull={onSelect}
          />
        )}
      </div>
      <CommandPalette
        isOpen={paletteOpen}
        onClose={() => setPaletteOpen(false)}
        nodes={nodes}
        recentIds={recents.ids}
        favoriteIds={favorites.ids}
        tables={tables}
        mode={isQueryMode ? "query" : "docs"}
        theme={theme.theme}
        currentDetailId={view.kind === "detail" ? view.uniqueId : null}
        onSelectNode={onSelect}
        onSelectSchemaTable={onSelectSchemaTable}
        onSetMode={onSetMode}
        onSetTheme={theme.setTheme}
        onResetHome={onResetHome}
        onShowList={onShowList}
        onToggleFavorite={favorites.toggle}
      />
    </div>
  );
}

function Topbar({
  project,
  search,
  onSearch,
  onResetHome,
  spinTrigger,
  mode,
  onSetMode,
  onOpenPalette,
}: {
  project: Project | null;
  search: string;
  onSearch: (v: string) => void;
  onResetHome?: () => void;
  spinTrigger?: number;
  mode?: "docs" | "query";
  onSetMode?: (m: "docs" | "query") => void;
  onOpenPalette?: () => void;
}) {
  return (
    <header className="topbar-v2">
      <div className="topbar-v2__bg" aria-hidden />
      <div className="topbar-v2__left">
        <div className="topbar-v2__brand">
          <button
            type="button"
            className="topbar-v2__brand-btn"
            onClick={onResetHome}
            aria-label="Home — reset view"
            title="Home — reset view"
          >
            {/* `key` changes on every parent-filter transition; React remounts
                the wrapper, replaying the CSS spin animation from scratch. */}
            <span key={spinTrigger ?? 0} className="topbar-v2__brand-anim">
              <Icon ryecon={RyeconColorDbt} size="xl" alt="dbt" />
            </span>
          </button>
          {project && (
            <div className="topbar-v2__brand-text">
              <div className="topbar-v2__brand-name">{project.name}</div>
              <div className="topbar-v2__brand-sub">
                {project.adapter_type ?? "—"}
                {project.dbt_version ? ` · v${project.dbt_version}` : ""}
              </div>
            </div>
          )}
        </div>
        {onSetMode && (
          <div className="topbar-v2__mode">
            <SegmentedButton
              segments={[
                { label: "Docs", value: "docs" },
                { label: "Query", value: "query" },
              ]}
              selectedValue={mode ?? "docs"}
              onSelect={(v) => onSetMode(v as "docs" | "query")}
              size="sm"
            />
          </div>
        )}
      </div>
      <label className="topbar-v2__search">
        <Icon ryecon={RyeconMagnifyingGlass} size="sm" alt="Search" />
        <input
          type="search"
          placeholder={
            mode === "query"
              ? "Search assets or schema tables…"
              : "Search models, sources, tests, metrics…"
          }
          value={search}
          onChange={(e) => onSearch(e.target.value)}
          aria-label={project ? `Search ${project.name}` : "Search project"}
        />
        <button
          type="button"
          className="topbar-v2__kbd-btn"
          onClick={onOpenPalette}
          aria-label="Open quick find"
          title="Quick find — ⌘K"
        >
          <kbd className="topbar-v2__kbd" aria-hidden>⌘K</kbd>
        </button>
      </label>
      <div className="topbar-v2__actions" aria-hidden />
    </header>
  );
}

