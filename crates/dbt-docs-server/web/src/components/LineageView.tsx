import { useEffect, useMemo, useState } from "react";
import { Dag, type DbtDagNode } from "@dbt-labs/dbt-dag";
import { api, ApiError, type LineageResponse } from "../api";

interface Props {
  rootUniqueId: string;
  onSelect(uniqueId: string): void;
}

// We don't have a real dbt Cloud project, but the dbt-dag component
// requires `dbtCloudProject` and `projectId` per node and `activeDbtCloudProject`
// at the DAG level. Use stable placeholders so highlighting/grouping behaves.
const LOCAL_PROJECT = "local";
const LOCAL_PROJECT_ID = 0;

export function LineageView({ rootUniqueId, onSelect }: Props) {
  const [data, setData] = useState<LineageResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setData(null);
    setError(null);
    let cancelled = false;
    api
      .lineage(rootUniqueId)
      .then((r) => {
        if (!cancelled) setData(r);
      })
      .catch((e: unknown) => {
        if (!cancelled) {
          setError(
            e instanceof ApiError || e instanceof Error
              ? e.message
              : String(e),
          );
        }
      });
    return () => {
      cancelled = true;
    };
  }, [rootUniqueId]);

  const dagNodes = useMemo<DbtDagNode[]>(() => {
    if (!data) return [];
    // Build a map from child → list of parents from the edges (each
    // dbt-dag node holds its direct parents as a string[]).
    const parents = new Map<string, string[]>();
    for (const e of data.edges) {
      const arr = parents.get(e.to_id) ?? [];
      arr.push(e.from_id);
      parents.set(e.to_id, arr);
    }
    return data.nodes.map((n) => ({
      id: n.unique_id,
      parents: parents.get(n.unique_id) ?? [],
      label: n.name,
      resourceType: n.resource_type,
      dbtCloudProject: LOCAL_PROJECT,
      projectId: LOCAL_PROJECT_ID,
      materializationType: (n.materialized as DbtDagNode["materializationType"]) ?? null,
    }));
  }, [data]);

  if (error) {
    return (
      <div className="err">
        Failed to load lineage: <code className="inline">{error}</code>
      </div>
    );
  }
  if (!data) {
    return <p className="muted" style={{ fontSize: 13 }}>Loading lineage…</p>;
  }
  if (dagNodes.length <= 1 && data.edges.length === 0) {
    return (
      <p className="muted" style={{ fontSize: 13 }}>
        This node has no upstream or downstream connections.
      </p>
    );
  }

  return (
    <div className="lineage-frame">
      <Dag
        nodes={dagNodes}
        activeDbtCloudProject={LOCAL_PROJECT}
        grain="project"
        primaryNodeIds={[rootUniqueId]}
        status="success"
        onNodeInteraction={(event) => {
          if (
            event.interactionType === "single_click" ||
            event.interactionType === "double_click"
          ) {
            if (event.targetNode) onSelect(event.targetNode.id);
          }
        }}
      />
    </div>
  );
}
