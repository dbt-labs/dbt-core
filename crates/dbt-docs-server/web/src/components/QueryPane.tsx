import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { EditorState } from "@codemirror/state";
import { EditorView, keymap } from "@codemirror/view";
import { defaultKeymap, history, historyKeymap } from "@codemirror/commands";
import { SQLDialect, sql } from "@codemirror/lang-sql";
import {
  Badge,
  Button,
  Icon,
  RyeconLineage,
  RyeconLogoDbtBit,
  Table,
} from "@dbt-labs/sourdough";
import type { Sizes, SortingState } from "@dbt-labs/sourdough";
import { createColumnHelper, type ColumnDef } from "@tanstack/react-table";
import { Dag, type DbtDagNode } from "@dbt-labs/dbt-dag";
import { api, type QueryResponse, type TableInfo } from "../api";
import { ResourceBadge } from "../lib/resourceType";
import { useQueryHistory } from "../hooks/useQueryHistory";

const DUCKDB = SQLDialect.define({
  keywords:
    "select from where group by order limit offset having join inner outer left right full cross union all and or not in like ilike between is null exists with as case when then else end distinct asc desc on using create view table show describe values true false count sum avg min max",
});

const LOCAL_PROJECT = "local";
const LOCAL_PROJECT_ID = 0;

/** Starter queries shown on first load — invitations, not just docs. */
const STARTERS: { title: string; subtitle: string; sql: string }[] = [
  {
    title: "All models",
    subtitle: "Browse every model in your project",
    sql: "SELECT unique_id, name, materialized\nFROM dbt.nodes\nWHERE resource_type = 'model'\nLIMIT 100;",
  },
  {
    title: "Untested models",
    subtitle: "Find models without any tests pointed at them",
    sql: "SELECT unique_id, name\nFROM dbt.nodes\nWHERE resource_type = 'model'\n  AND unique_id NOT IN (\n    SELECT parent_unique_id FROM dbt.edges\n    WHERE child_unique_id IN (\n      SELECT unique_id FROM dbt.nodes WHERE resource_type = 'test'\n    )\n  )\nLIMIT 100;",
  },
  {
    title: "Largest packages",
    subtitle: "Which packages have the most assets?",
    sql: "SELECT package_name, COUNT(*) AS n\nFROM dbt.nodes\nGROUP BY package_name\nORDER BY n DESC;",
  },
];

interface QueryPaneProps {
  /** DuckDB tables fetched at App level. Used for editor autocomplete. */
  tables: TableInfo[] | null;
  /** When LocatePane's Schema section clicks a table, App bumps the token
   *  and passes new SQL. QueryPane reacts by running it. */
  pendingSql?: { sql: string; token: number } | null;
  onOpenNode?: (uniqueId: string) => void;
}

export function QueryPane({ tables, pendingSql, onOpenNode }: QueryPaneProps) {
  const editorEl = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);
  const sqlRef = useRef<string>("");

  const [result, setResult] = useState<QueryResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const [matchedNodeIds, setMatchedNodeIds] = useState<string[]>([]);
  const [lineageOpen, setLineageOpen] = useState(false);
  const [starterSpin, setStarterSpin] = useState(0);
  const history_ = useQueryHistory();

  const schema = useMemo(() => {
    if (!tables) return {};
    const out: Record<string, string[]> = {};
    for (const t of tables) {
      out[`${t.schema}.${t.name}`] = t.columns.map((c) => c.name);
    }
    return out;
  }, [tables]);

  const abortRef = useRef<AbortController | null>(null);
  const run = useCallback(
    async (sqlText: string) => {
      const trimmed = sqlText.trim();
      if (!trimmed) {
        setResult(null);
        setError(null);
        setMatchedNodeIds([]);
        return;
      }
      abortRef.current?.abort();
      const ctrl = new AbortController();
      abortRef.current = ctrl;
      setRunning(true);
      setError(null);
      setMatchedNodeIds([]);
      try {
        const res = await api.query(trimmed, ctrl.signal);
        if (ctrl.signal.aborted) return;
        if (res.kind === "ok") {
          setResult(res.value);
          history_.push(trimmed);
          void verifyNodeIds(res.value, ctrl.signal).then((ids) => {
            if (!ctrl.signal.aborted) setMatchedNodeIds(ids);
          });
        } else {
          setResult(null);
          setError(res.message);
        }
      } catch (e: unknown) {
        if ((e as { name?: string }).name === "AbortError") return;
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (abortRef.current === ctrl) setRunning(false);
      }
    },
    [history_],
  );

  const setSqlAndRun = useCallback(
    (newSql: string) => {
      const view = viewRef.current;
      if (view) {
        view.dispatch({
          changes: { from: 0, to: view.state.doc.length, insert: newSql },
        });
        view.focus();
      }
      sqlRef.current = newSql;
      void run(newSql);
    },
    [run],
  );

  const setSqlOnly = useCallback((newSql: string) => {
    const view = viewRef.current;
    if (view) {
      view.dispatch({
        changes: { from: 0, to: view.state.doc.length, insert: newSql },
      });
      view.focus();
    }
    sqlRef.current = newSql;
  }, []);

  // React to LocatePane Schema clicks. Token bumps every click, so even
  // clicking the same table twice re-fires the run.
  useEffect(() => {
    if (pendingSql) {
      setSqlAndRun(pendingSql.sql);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pendingSql?.token]);

  // Editor mount/teardown.
  useEffect(() => {
    if (!editorEl.current) return;
    viewRef.current?.destroy();
    const runQuery = () => {
      void run(sqlRef.current);
      return true;
    };
    const state = EditorState.create({
      doc: sqlRef.current,
      extensions: [
        history(),
        sql({ dialect: DUCKDB, schema, upperCaseKeywords: true }),
        keymap.of([
          { key: "Mod-Enter", run: runQuery },
          ...defaultKeymap,
          ...historyKeymap,
        ]),
        EditorView.theme({
          "&": { height: "100%", fontSize: "13px" },
          ".cm-content": {
            fontFamily:
              "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace",
            padding: "12px 0",
          },
          ".cm-gutters": { background: "transparent", border: "none" },
        }),
        EditorView.lineWrapping,
        EditorView.updateListener.of((u) => {
          if (u.docChanged) sqlRef.current = u.state.doc.toString();
        }),
      ],
    });
    const view = new EditorView({ state, parent: editorEl.current });
    viewRef.current = view;
    return () => {
      view.destroy();
      viewRef.current = null;
    };
  }, [schema, run]);

  // Always show starter cards. After the first run they shrink into a
  // single-line "Try another" row to stay reachable without crowding.
  const startersCompact = !!(result || error || running);

  return (
    <div className="qq">
      {/* Main work surface: editor + results + lineage preview. Schema browser
          lives in LocatePane now — see SchemaSection. */}
      <section className="qq-main">
        {/* Editor card */}
        <div className="qq-card qq-editor-card">
          <header className="qq-card-head">
            <button
              type="button"
              className="qq-run"
              onClick={() => void run(sqlRef.current)}
              disabled={running}
            >
              {running ? "Running…" : "Run"}
              <span className="qq-kbd" aria-hidden>⌘↵</span>
            </button>
            {result && !error && (
              <span className="qq-stats">
                <Badge
                  text={`${result.row_count.toLocaleString()} ${result.row_count === 1 ? "row" : "rows"}`}
                  type={result.truncated ? "warning" : "default"}
                  size="xs"
                />
                <span className="qq-stats-elapsed">{result.elapsed_ms} ms</span>
                {result.truncated && <span className="qq-stats-warn">truncated at 1000</span>}
              </span>
            )}
          </header>
          <div ref={editorEl} className="qq-editor" />
          {history_.entries.length > 0 && (
            <footer className="qq-card-foot qq-history">
              <span className="qq-history-label">Recent</span>
              {history_.entries.slice(0, 5).map((q, i) => (
                <Button
                  key={i}
                  type="tertiary"
                  size={"sm" as Sizes}
                  text={
                    <span className="qq-history-chip-text">
                      {q.replace(/\s+/g, " ").trim().slice(0, 60)}
                      {q.length > 60 ? "…" : ""}
                    </span>
                  }
                  onClick={() => setSqlOnly(q)}
                  tooltip={q}
                />
              ))}
            </footer>
          )}
        </div>

        {/* Starter queries — always visible. Full cards before the first
            run, compact row after. dbt icon spins each time a chip is
            clicked, signalling "running another". */}
        <div className={`qq-starters ${startersCompact ? "is-compact" : ""}`}>
          <div className="qq-starters-head">
            <span
              key={starterSpin}
              className="qq-starters-icon"
              aria-hidden
            >
              <Icon ryecon={RyeconLogoDbtBit} size="md" alt="" />
            </span>
            <div className="qq-starters-title-block">
              <h3 className="qq-starters-title">
                {startersCompact ? "Try another" : "Try it out"}
              </h3>
              {!startersCompact && (
                <p className="qq-starters-subtitle">
                  Run a query against your project's metadata
                </p>
              )}
            </div>
          </div>
          <div className="qq-starters-grid">
            {STARTERS.map((s) => (
              <button
                key={s.title}
                type="button"
                className="qq-starter"
                onClick={() => {
                  setStarterSpin((n) => n + 1);
                  setSqlAndRun(s.sql);
                }}
                title={s.subtitle}
              >
                <div className="qq-starter-title">{s.title}</div>
                {!startersCompact && (
                  <>
                    <div className="qq-starter-sub">{s.subtitle}</div>
                    <code className="qq-starter-sql">
                      {s.sql.split("\n")[0].slice(0, 50)}…
                    </code>
                  </>
                )}
              </button>
            ))}
          </div>
        </div>

        {/* Error card — danger header strip + neutral body, sourdough
            banner-style. Loud enough to notice, calm enough to read. */}
        {error && (
          <div className="qq-card qq-error-card">
            <header className="qq-card-head qq-error-head">
              <Badge text="SQL error" type="error" size="xs" />
              <span className="qq-error-hint">
                Editor still has your query — fix and re-run.
              </span>
            </header>
            <pre className="qq-error-body">{error}</pre>
          </div>
        )}

        {/* Results — caption sits above the table; the "Show N as lineage"
            CTA sits on the right of the same caption row. No card chrome
            around the table itself; sourdough Table provides its own frame. */}
        {result && !error && (
          <div className="qq-results-wrap">
            <div className="qq-results-caption">
              <span className="qq-results-meta">
                {result.columns.length} columns · {result.row_count.toLocaleString()} rows
              </span>
              {matchedNodeIds.length > 0 && (
                <Button
                  type={lineageOpen ? "brand" : "secondary"}
                  size={"sm" as Sizes}
                  ryecon={RyeconLineage}
                  iconPlacement="left"
                  text={`${lineageOpen ? "Hide" : "Show"} ${matchedNodeIds.length} as lineage`}
                  onClick={() => setLineageOpen((v) => !v)}
                />
              )}
            </div>
            <ResultsTable
              result={result}
              matchedNodeIds={matchedNodeIds}
              onOpenNode={onOpenNode}
            />
          </div>
        )}

        {/* Inline lineage preview — the wow. */}
        {result && !error && matchedNodeIds.length > 0 && lineageOpen && (
          <InlineLineagePreview
            uniqueIds={matchedNodeIds}
            onOpenNode={onOpenNode}
          />
        )}
      </section>
    </div>
  );
}

/* ---------- Side: verify which result unique_ids correspond to real nodes ---------- */

async function verifyNodeIds(
  result: QueryResponse,
  signal: AbortSignal,
): Promise<string[]> {
  if (!result.columns.some((c) => c.name === "unique_id")) return [];
  const ids = new Set<string>();
  for (const row of result.rows) {
    const v = row.unique_id;
    if (typeof v === "string" && v.length > 0) ids.add(v);
  }
  if (ids.size === 0) return [];
  const quoted = Array.from(ids)
    .slice(0, 1000)
    .map((s) => `'${s.replace(/'/g, "''")}'`)
    .join(",");
  const sqlText = `SELECT unique_id FROM dbt.nodes WHERE unique_id IN (${quoted})`;
  try {
    const res = await api.query(sqlText, signal);
    if (res.kind !== "ok") return [];
    const out: string[] = [];
    for (const row of res.value.rows) {
      const v = row.unique_id;
      if (typeof v === "string") out.push(v);
    }
    return out;
  } catch {
    return [];
  }
}

/* ---------- Results table ---------- */

type ResultRow = Record<string, unknown> & { __idx: number };

function ResultsTable({
  result,
  matchedNodeIds,
  onOpenNode,
}: {
  result: QueryResponse;
  matchedNodeIds: string[];
  onOpenNode?: (id: string) => void;
}) {
  const matched = useMemo(() => new Set(matchedNodeIds), [matchedNodeIds]);
  // Sourdough Table fires onChangeSort but doesn't actually re-order data —
  // it only manages the indicator. We hold sort state and sort the rows
  // ourselves before handing them back, so clicking a header reorders.
  const [sortBy, setSortBy] = useState<SortingState>([]);

  // Stable per-row id so react-table can track. The query results have no
  // primary key, so we use the row index.
  const data = useMemo<ResultRow[]>(() => {
    const rows = result.rows.map((row, idx) => ({ ...row, __idx: idx }));
    if (sortBy.length === 0) return rows;
    const [{ id, desc }] = sortBy;
    const sign = desc ? -1 : 1;
    return rows.slice().sort((a, b) => {
      const va = (a as Record<string, unknown>)[id];
      const vb = (b as Record<string, unknown>)[id];
      // Nulls always sink to the end regardless of direction.
      if (va == null && vb == null) return 0;
      if (va == null) return 1;
      if (vb == null) return -1;
      // Numeric where possible, else lexicographic.
      if (typeof va === "number" && typeof vb === "number") return (va - vb) * sign;
      const sa = typeof va === "string" ? va : String(va);
      const sb = typeof vb === "string" ? vb : String(vb);
      return sa.localeCompare(sb, undefined, { numeric: true, sensitivity: "base" }) * sign;
    });
  }, [result.rows, sortBy]);

  // Build ColumnDef[] dynamically from the query result. Column types
  // (utf8 / int / etc.) move to header tooltips instead of inline tags —
  // less noisy, still discoverable on hover.
  const columns = useMemo<ColumnDef<ResultRow, unknown>[]>(() => {
    const helper = createColumnHelper<ResultRow>();
    return result.columns.map((c) =>
      helper.accessor((row) => row[c.name] as unknown, {
        id: c.name,
        // Without an explicit sortingFn, react-table can't compare `unknown`
        // values; clicking the header set the indicator but didn't reorder
        // rows. "alphanumeric" handles mixed strings + numbers + nulls.
        sortingFn: "alphanumeric",
        sortUndefined: "last",
        header: () => <span title={c.data_type}>{c.name}</span>,
        cell: (info) => {
          const v = info.getValue();
          // Colored ResourceBadge whenever a column is the dbt-canonical
          // `resource_type`. Same chip the catalog uses, for visual parity
          // between the catalog Type column and any SQL result that selects it.
          if (c.name === "resource_type" && typeof v === "string" && v.length > 0) {
            return <ResourceBadge type={v} size="xs" />;
          }
          const isLinkable =
            c.name === "unique_id" &&
            typeof v === "string" &&
            matched.has(v) &&
            onOpenNode;
          if (isLinkable) {
            return (
              <button
                type="button"
                className="qq-row-link"
                onClick={(e) => {
                  e.stopPropagation();
                  onOpenNode(v as string);
                }}
                title="Open in Docs view"
              >
                {formatCell(v)}
              </button>
            );
          }
          return <span>{formatCell(v)}</span>;
        },
      }),
    );
  }, [result.columns, matched, onOpenNode]);

  if (result.row_count === 0) {
    return <div className="qq-results-empty">0 rows.</div>;
  }

  return (
    <div className="qq-results-scroll">
      <Table
        columns={columns}
        data={data}
        isSortable
        onChangeSort={setSortBy}
        cellVariant="COMPACT"
      />
    </div>
  );
}

function formatCell(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "string") return v;
  if (typeof v === "number" || typeof v === "boolean") return String(v);
  return JSON.stringify(v);
}

/* ---------- Inline lineage preview ----------
   Renders a mini-DAG of the matched unique_ids without navigating away.
   Fetches edges + node metadata via the same /api/v1/query endpoint so
   we don't need a new backend surface. */

function InlineLineagePreview({
  uniqueIds,
  onOpenNode,
}: {
  uniqueIds: string[];
  onOpenNode?: (id: string) => void;
}) {
  const [nodes, setNodes] = useState<DbtDagNode[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setNodes(null);
    setError(null);
    void buildDag(uniqueIds)
      .then((built) => {
        if (!cancelled) setNodes(built);
      })
      .catch((e) => {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      cancelled = true;
    };
  }, [uniqueIds]);

  if (error) {
    return (
      <div className="qq-card qq-lineage-card">
        <header className="qq-card-head">
          <Badge text="Lineage preview" type="default" size="xs" />
        </header>
        <p className="qq-lineage-error">Couldn't load lineage: {error}</p>
      </div>
    );
  }

  if (!nodes) {
    return (
      <div className="qq-card qq-lineage-card">
        <header className="qq-card-head">
          <Badge text="Lineage preview" type="default" size="xs" />
        </header>
        <p className="qq-lineage-loading">Building graph…</p>
      </div>
    );
  }

  if (nodes.length === 0) {
    return (
      <div className="qq-card qq-lineage-card">
        <header className="qq-card-head">
          <Badge text="Lineage preview" type="default" size="xs" />
        </header>
        <p className="qq-lineage-empty">
          No nodes resolved. Try a query that returns valid unique_ids.
        </p>
      </div>
    );
  }

  return (
    <div className="qq-card qq-lineage-card">
      <header className="qq-card-head">
        <span className="qq-lineage-eyebrow">
          <Icon ryecon={RyeconLineage} size="xs" alt="" /> Lineage preview ·{" "}
          {nodes.length} nodes
        </span>
        <span className="qq-lineage-hint">Click a node to open its docs page</span>
      </header>
      <div className="qq-lineage-canvas">
        <Dag
          nodes={nodes}
          activeDbtCloudProject={LOCAL_PROJECT}
          grain="project"
          primaryNodeIds={nodes.map((n) => n.id)}
          status="success"
          onNodeInteraction={(event) => {
            if (
              event.interactionType === "single_click" ||
              event.interactionType === "double_click"
            ) {
              if (event.targetNode && onOpenNode) onOpenNode(event.targetNode.id);
            }
          }}
        />
      </div>
    </div>
  );
}

/** Builds DbtDagNode[] for inline preview: fetches the matching node metadata
 *  and the edges between them via two SQL queries against the parquet views. */
async function buildDag(ids: string[]): Promise<DbtDagNode[]> {
  if (ids.length === 0) return [];
  const cap = ids.slice(0, 200); // cap inline preview at 200 nodes for perf
  const quoted = cap.map((s) => `'${s.replace(/'/g, "''")}'`).join(",");

  // Fetch node metadata.
  const nodesQ = `SELECT unique_id, name, resource_type, materialized FROM dbt.nodes WHERE unique_id IN (${quoted})`;
  const nodesRes = await api.query(nodesQ);
  if (nodesRes.kind !== "ok") throw new Error(nodesRes.message);

  // Fetch edges where both endpoints are in our set. The parquet view
  // uses parent_unique_id / child_unique_id (not from_node / to_node).
  const edgesQ = `SELECT parent_unique_id, child_unique_id FROM dbt.edges WHERE parent_unique_id IN (${quoted}) AND child_unique_id IN (${quoted})`;
  const edgesRes = await api.query(edgesQ);
  if (edgesRes.kind !== "ok") throw new Error(edgesRes.message);

  const parents = new Map<string, string[]>();
  for (const row of edgesRes.value.rows) {
    const from = row.parent_unique_id as string;
    const to = row.child_unique_id as string;
    if (typeof from !== "string" || typeof to !== "string") continue;
    const arr = parents.get(to) ?? [];
    arr.push(from);
    parents.set(to, arr);
  }

  return nodesRes.value.rows.map((row) => ({
    id: row.unique_id as string,
    parents: parents.get(row.unique_id as string) ?? [],
    label: (row.name as string) ?? (row.unique_id as string),
    resourceType: (row.resource_type as string) ?? "model",
    dbtCloudProject: LOCAL_PROJECT,
    projectId: LOCAL_PROJECT_ID,
    materializationType: (row.materialized as DbtDagNode["materializationType"]) ?? null,
  }));
}

