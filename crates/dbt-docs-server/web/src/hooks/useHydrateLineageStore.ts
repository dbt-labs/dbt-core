import { useEffect } from 'react';

import { useLineageStore } from '../stores/lineageStore';
import { useLineageData } from './useLineageData';

/**
 * Bootstrap action for the lineage store: read the graph out of DuckDB for
 * `rootUniqueId` and push it into `useLineageStore`, laid out and ready to render.
 *
 * Call this once, from the surface that owns the lineage route. Everything below it
 * — the canvas and any sibling view of the same graph — reads nodes, edges and
 * hydration status straight off the store, so no child needs the query, the layout
 * pass, or props threaded down to it.
 *
 * Returns nothing on purpose: `useLineageStatus()` is the way to read the loading,
 * error and unsupported branches, and it works at any depth.
 */
export function useHydrateLineageStore(rootUniqueId: string, depth: number): void {
  const { data, error, isSupported, graphNodes, graphEdges } = useLineageData(
    rootUniqueId,
    depth,
  );
  const startHydration = useLineageStore((s) => s.startHydration);
  const hydrate = useLineageStore((s) => s.hydrate);
  const failHydration = useLineageStore((s) => s.failHydration);
  const markUnsupported = useLineageStore((s) => s.markUnsupported);
  const reset = useLineageStore((s) => s.reset);

  useEffect(() => {
    if (!rootUniqueId) {
      reset();
      return;
    }
    startHydration(rootUniqueId);
  }, [rootUniqueId, startHydration, reset]);

  useEffect(() => {
    if (!rootUniqueId) return;
    if (error) {
      failHydration(error);
      return;
    }
    if (!isSupported) {
      markUnsupported();
      return;
    }
    // Lineage resolves asynchronously; until it lands the store stays in `loading`.
    if (!data) return;
    hydrate({ rootUniqueId, nodes: graphNodes, edges: graphEdges });
  }, [
    rootUniqueId,
    data,
    error,
    isSupported,
    graphNodes,
    graphEdges,
    hydrate,
    failHydration,
    markUnsupported,
  ]);
}
