import { useMemo } from 'react';

import type { DbtDagNode } from '@dbt-labs/dbt-dag';

import { type LineageGraph, type ResourceType, useLineage } from '../shared';

export const LOCAL_PROJECT = 'local';
export const LOCAL_PROJECT_ID = 0;

export function fqnFromUniqueId(uniqueId: string): string {
  // unique_id: `<resource_type>.<package>.<...path>.<name>` → drop resource_type prefix
  return uniqueId.split('.').slice(1).join('.');
}

export function defaultSelectorFor(uniqueId: string, depth: number): string {
  return `${depth}+${fqnFromUniqueId(uniqueId)}+${depth}`;
}

/** Lineage's REST endpoint keys off unique_id alone, but the shared contract
 *  requires a resource type. The unique_id prefix is the resource type. */
function resourceTypeFromUniqueId(uniqueId: string): ResourceType {
  return (uniqueId.split('.')[0] ?? 'model') as ResourceType;
}

export function useLineageData(
  rootUniqueId: string,
  depth: number,
): {
  data: LineageGraph | null;
  error: Error | null;
  dagNodes: DbtDagNode[];
  selector: string;
  /** False when the active data source has no `fetchLineage`. */
  isSupported: boolean;
} {
  const query = useLineage(
    rootUniqueId
      ? {
          uniqueId: rootUniqueId,
          resourceType: resourceTypeFromUniqueId(rootUniqueId),
          depth,
        }
      : null,
  );
  const data = query.data ?? null;

  const dagNodes = useMemo<DbtDagNode[]>(() => {
    if (!data) return [];
    const parents = new Map<string, string[]>();
    for (const e of data.edges) {
      const arr = parents.get(e.downstreamUniqueId) ?? [];
      arr.push(e.upstreamUniqueId);
      parents.set(e.downstreamUniqueId, arr);
    }
    return data.nodes.map((n) => ({
      id: n.uniqueId,
      parents: parents.get(n.uniqueId) ?? [],
      label: n.name,
      resourceType: n.resourceType,
      dbtCloudProject: LOCAL_PROJECT,
      projectId: LOCAL_PROJECT_ID,
      materializationType:
        (n.materialized as DbtDagNode['materializationType']) ?? null,
    }));
  }, [data]);

  return {
    data,
    error: query.error,
    dagNodes,
    selector: defaultSelectorFor(rootUniqueId, depth),
    isSupported: query.isSupported,
  };
}
