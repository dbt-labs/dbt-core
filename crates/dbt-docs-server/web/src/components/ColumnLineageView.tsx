import { useEffect, useState } from "react";
import {
  api,
  ApiError,
  type ColumnLineageEdge,
  type Gated,
  type ColumnLineageResponse,
} from "../api";

interface Props {
  rootUniqueId: string;
  onSelect(uniqueId: string): void;
}

type FetchState =
  | { kind: "idle" }
  | { kind: "loading" }
  | { kind: "ready"; result: Gated<ColumnLineageResponse> }
  | { kind: "error"; message: string };

export function ColumnLineageView({ rootUniqueId, onSelect }: Props) {
  const [state, setState] = useState<FetchState>({ kind: "idle" });

  useEffect(() => {
    setState({ kind: "idle" });
  }, [rootUniqueId]);

  const load = () => {
    setState({ kind: "loading" });
    api
      .columnLineage(rootUniqueId)
      .then((r) => setState({ kind: "ready", result: r }))
      .catch((e: unknown) =>
        setState({
          kind: "error",
          message:
            e instanceof ApiError || e instanceof Error
              ? e.message
              : String(e),
        }),
      );
  };

  if (state.kind === "idle") {
    return (
      <UpgradeTease
        title="Show how data flows column-by-column"
        body="Column lineage isn't loaded by default — it can be expensive on large projects."
        primaryLabel="Load column lineage"
        primaryClass="btn primary"
        onPrimary={load}
      />
    );
  }

  if (state.kind === "loading") {
    return (
      <p className="muted" style={{ fontSize: 13 }}>
        Loading column lineage…
      </p>
    );
  }

  if (state.kind === "error") {
    return (
      <div className="err">
        Failed to load column lineage: <code className="inline">{state.message}</code>{" "}
        <button type="button" onClick={load} className="btn sm">
          Retry
        </button>
      </div>
    );
  }

  const { result } = state;
  if (result.kind === "unavailable") {
    return (
      <UpgradeTease
        title="Column lineage isn't available"
        body={
          result.details.upgrade_path ??
          "Run `dbt --use-index <run|build|compile>` with static analysis enabled to populate column-level lineage."
        }
        primaryLabel="Learn more"
        primaryClass="btn upgrade"
      />
    );
  }

  const { edges } = result.value;
  if (edges.length === 0) {
    return (
      <p className="muted" style={{ fontSize: 13 }}>
        No column-level lineage edges touch this node.
      </p>
    );
  }

  const upstream: ColumnLineageEdge[] = [];
  const downstream: ColumnLineageEdge[] = [];
  for (const e of edges) {
    if (e.to_node === rootUniqueId) upstream.push(e);
    else if (e.from_node === rootUniqueId) downstream.push(e);
  }

  return (
    <div className="ref-grid">
      <EdgeBlock
        title={`Upstream into this node (${upstream.length})`}
        edges={upstream}
        rootSide="to"
        onSelect={onSelect}
      />
      <EdgeBlock
        title={`Downstream from this node (${downstream.length})`}
        edges={downstream}
        rootSide="from"
        onSelect={onSelect}
      />
    </div>
  );
}

function EdgeBlock({
  title,
  edges,
  rootSide,
  onSelect,
}: {
  title: string;
  edges: ColumnLineageEdge[];
  rootSide: "from" | "to";
  onSelect: (uniqueId: string) => void;
}) {
  return (
    <div className="ref-block">
      <h3>{title}</h3>
      {edges.length === 0 ? (
        <p className="ref-empty">—</p>
      ) : (
        <table className="tbl" style={{ borderRadius: 0 }}>
          <thead>
            <tr>
              <th>Other column</th>
              <th>This column</th>
              <th>Kind</th>
            </tr>
          </thead>
          <tbody>
            {edges.map((e, i) => {
              const otherNode = rootSide === "to" ? e.from_node : e.to_node;
              const otherCol = rootSide === "to" ? e.from_column : e.to_column;
              const thisCol = rootSide === "to" ? e.to_column : e.from_column;
              return (
                <tr key={i}>
                  <td>
                    <div>
                      <button
                        type="button"
                        className="cell-link"
                        onClick={() => onSelect(otherNode)}
                        title={otherNode}
                      >
                        {otherNode.split(".").pop()}
                      </button>
                    </div>
                    <div className="mono faint">{otherCol}</div>
                  </td>
                  <td className="mono">{thisCol}</td>
                  <td>
                    <span className="type-pill sm">{e.kind}</span>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </div>
  );
}

function UpgradeTease({
  title,
  body,
  primaryLabel,
  primaryClass,
  onPrimary,
}: {
  title: string;
  body: string;
  primaryLabel: string;
  primaryClass: string;
  onPrimary?: () => void;
}) {
  return (
    <div className="cll-tease">
      <div className="nodes" aria-hidden>
        <div className="node upstream">stg_orders.order_id</div>
        <span className="arrow">→</span>
        <div className="node target">customer_orders.id</div>
        <span className="arrow">→</span>
        <div className="node">customer_lifetime_value.cust_id</div>
      </div>
      <div className="pitch">
        <h4>{title}</h4>
        <p>{body}</p>
        <div className="actions">
          {onPrimary ? (
            <button type="button" className={primaryClass} onClick={onPrimary}>
              {primaryLabel}
            </button>
          ) : (
            <span className={primaryClass}>{primaryLabel}</span>
          )}
        </div>
      </div>
    </div>
  );
}
