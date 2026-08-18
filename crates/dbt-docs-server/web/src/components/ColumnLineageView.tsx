import { useCallback, useEffect, useMemo } from 'react';

import {
  Dag,
  type DbtDagNode,
  type ResourceTypeExplorer,
  type TransformationType,
  transformationTypes,
} from '@dbt-labs/dbt-dag';

import { decorateOutboundHref } from '../lib/outboundReferrer';
import {
  type ColumnLineageGraph,
  type ColumnLineageResult,
  Spinner,
  UpgradeCard,
  useColumnLineage as useSharedColumnLineage,
  type UserState,
} from '../shared';

/** Local snake-cased edge shape the subgraph BFS below walks in both
 *  directions. Sourced directly from the domain {@link ColumnLineageGraph}'s
 *  `edges` via a trivial field rename — no string parsing. */
type ColumnLineageEdge = {
  from_node: string;
  from_column: string;
  to_node: string;
  to_column: string;
  kind: string;
};

const LOCAL_PROJECT = 'local';
const LOCAL_PROJECT_ID = 0;

const TRANSFORMATION_VALUES = new Set<string>(transformationTypes);

function toTransformationType(kind: string | undefined): TransformationType {
  if (!kind) return 'UNKNOWN';
  const upper = kind.toUpperCase();
  return TRANSFORMATION_VALUES.has(upper) ? (upper as TransformationType) : 'UNKNOWN';
}

function resourceTypeFromUniqueId(uniqueId: string): ResourceTypeExplorer {
  const head = uniqueId.split('.')[0] ?? 'unknown';
  return head as ResourceTypeExplorer;
}

function sublabelFromUniqueId(uniqueId: string): string {
  const parts = uniqueId.split('.');
  return parts.slice(2).join('.') || uniqueId;
}

function splitCompositeId(id: string): { nodeUniqueId: string; column: string } {
  const lastDot = id.lastIndexOf('.');
  return lastDot >= 0
    ? { nodeUniqueId: id.slice(0, lastDot), column: id.slice(lastDot + 1) }
    : { nodeUniqueId: id, column: id };
}

const compositeId = (node: string, column: string) => `${node}.${column}`;

type FetchState =
  | { kind: 'idle' }
  | { kind: 'loading' }
  | { kind: 'ready'; result: ColumnLineageResult }
  | { kind: 'error'; message: string };

/** Per-node CLL fetch + cache, backed by the shared `useColumnLineage`. The
 *  Columns tab lazily loads on first column expand and reuses the cached
 *  response for subsequent expansions of other columns in the same node.
 *  Lazy: the query is disabled until `load()` triggers a fetch.
 *  Cache-backed per `rootUniqueId`: revisiting a node whose response is still
 *  in the react-query cache (within `gcTime`) renders immediately as `ready`
 *  without a fresh `load()`; only a cold cache key starts in `idle`. */
export function useColumnLineage(rootUniqueId: string) {
  const query = useSharedColumnLineage({ uniqueId: rootUniqueId }, { enabled: false });

  const state: FetchState = query.isFetching
    ? { kind: 'loading' }
    : query.error
      ? {
          kind: 'error',
          message:
            query.error instanceof Error ? query.error.message : String(query.error),
        }
      : query.data !== undefined
        ? { kind: 'ready', result: query.data }
        : { kind: 'idle' };

  const { refetch } = query;
  const load = useCallback(() => {
    void refetch();
  }, [refetch]);

  return { state, load };
}

interface MiniProps {
  rootUniqueId: string;
  columnName: string;
  state: FetchState;
  load: () => void;
  onSelect(uniqueId: string): void;
  userState: UserState | null;
}

/** Per-column expanded body: lazy-fires the node-scoped CLL fetch on
 *  first render (via the parent-owned hook so siblings reuse the
 *  cached response), then filters the edge set to the subgraph that
 *  touches `columnName` on `rootUniqueId`. */
export function ColumnLineageMini({
  rootUniqueId,
  columnName,
  state,
  load,
  onSelect,
  userState,
}: MiniProps) {
  useEffect(() => {
    if (state.kind === 'idle') load();
  }, [state.kind, load]);

  const subgraph = useMemo(() => {
    if (state.kind !== 'ready' || state.result.kind !== 'ok') return null;
    return buildColumnSubgraph(state.result.graph, rootUniqueId, columnName);
  }, [state, rootUniqueId, columnName]);

  if (state.kind === 'idle' || state.kind === 'loading') {
    return (
      <p className="muted flex items-center gap-2" style={{ fontSize: 13 }}>
        <Spinner /> Loading column lineage…
      </p>
    );
  }

  if (state.kind === 'error') {
    return (
      <div className="err">
        Failed to load column lineage: <code className="inline">{state.message}</code>{' '}
        <button type="button" onClick={load} className="btn sm">
          Retry
        </button>
      </div>
    );
  }

  const { result } = state;
  if (result.kind === 'gated') {
    if (userState) {
      return (
        <UpgradeCard
          kind="columnLineage"
          userState={userState}
          variant="inline"
          decorateOutboundHref={decorateOutboundHref}
        />
      );
    }
    return null;
  }

  if (!subgraph || subgraph.nodes.length === 0) {
    return (
      <p className="muted" style={{ fontSize: 13 }}>
        No column-level lineage edges touch this column.
      </p>
    );
  }

  const primaryNodeIds = subgraph.nodes
    .filter(
      (n) =>
        splitCompositeId(n.id).nodeUniqueId === rootUniqueId &&
        splitCompositeId(n.id).column === columnName,
    )
    .map((n) => n.id);

  return (
    <div className="lineage-frame" style={{ height: 320 }}>
      <Dag
        nodes={subgraph.nodes}
        activeDbtCloudProject={LOCAL_PROJECT}
        grain="column"
        primaryNodeIds={primaryNodeIds}
        status="success"
        onNodeInteraction={(event) => {
          if (
            event.interactionType === 'single_click' ||
            event.interactionType === 'double_click'
          ) {
            if (event.targetNode) {
              onSelect(splitCompositeId(event.targetNode.id).nodeUniqueId);
            }
          }
        }}
      />
    </div>
  );
}

/** Rename the domain graph's edges to the local snake-cased shape the BFS
 *  consumes. */
function edgesFromGraph(graph: ColumnLineageGraph): ColumnLineageEdge[] {
  return graph.edges.map((e) => ({
    from_node: e.fromNodeUniqueId,
    from_column: e.fromColumn,
    to_node: e.toNodeUniqueId,
    to_column: e.toColumn,
    kind: e.transformationType ?? '',
  }));
}

/** BFS the edge set both upstream and downstream from the target column,
 *  then materialize Dag nodes for the reachable composite ids. */
function buildColumnSubgraph(
  graph: ColumnLineageGraph,
  rootUniqueId: string,
  columnName: string,
): { nodes: DbtDagNode[] } {
  const edges = edgesFromGraph(graph);
  const target = compositeId(rootUniqueId, columnName);

  const outgoing = new Map<string, ColumnLineageEdge[]>();
  const incoming = new Map<string, ColumnLineageEdge[]>();
  for (const e of edges) {
    const fromId = compositeId(e.from_node, e.from_column);
    const toId = compositeId(e.to_node, e.to_column);
    (outgoing.get(fromId) ?? outgoing.set(fromId, []).get(fromId)!).push(e);
    (incoming.get(toId) ?? incoming.set(toId, []).get(toId)!).push(e);
  }

  const reachable = new Set<string>([target]);
  const walk = (start: string, adj: Map<string, ColumnLineageEdge[]>) => {
    const stack = [start];
    while (stack.length) {
      const id = stack.pop()!;
      for (const e of adj.get(id) ?? []) {
        const next =
          adj === outgoing
            ? compositeId(e.to_node, e.to_column)
            : compositeId(e.from_node, e.from_column);
        if (!reachable.has(next)) {
          reachable.add(next);
          stack.push(next);
        }
      }
    }
  };
  walk(target, outgoing);
  walk(target, incoming);

  if (reachable.size <= 1 && !outgoing.has(target) && !incoming.has(target)) {
    return { nodes: [] };
  }

  const parentMap = new Map<string, Array<{ parentId: string; kind: string }>>();
  for (const e of edges) {
    const fromId = compositeId(e.from_node, e.from_column);
    const toId = compositeId(e.to_node, e.to_column);
    if (!reachable.has(fromId) || !reachable.has(toId)) continue;
    const arr = parentMap.get(toId) ?? [];
    arr.push({ parentId: fromId, kind: e.kind });
    parentMap.set(toId, arr);
  }

  const nodes: DbtDagNode[] = Array.from(reachable).map((id) => {
    const { nodeUniqueId, column } = splitCompositeId(id);
    const incomingEdges = parentMap.get(id) ?? [];
    const parents = incomingEdges.map((p) => p.parentId);
    return {
      id,
      label: column,
      sublabel: sublabelFromUniqueId(nodeUniqueId),
      parents,
      resourceType: resourceTypeFromUniqueId(nodeUniqueId),
      transformationType:
        parents.length === 0 ? 'RAW' : toTransformationType(incomingEdges[0]?.kind),
      dbtCloudProject: LOCAL_PROJECT,
      projectId: LOCAL_PROJECT_ID,
    };
  });

  return { nodes };
}
