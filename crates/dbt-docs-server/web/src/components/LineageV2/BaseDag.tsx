import { useEffect } from 'react';
import {
  Background,
  ConnectionMode,
  Controls,
  ReactFlow,
  useReactFlow,
} from '@xyflow/react';

import { useHydrateLineageStore } from '../../hooks/useHydrateLineageStore';
import { useLayoutWhenMeasured } from '../../hooks/useLayoutWhenMeasured';
import { useTheme } from '../../hooks/useTheme';
import { Spinner } from '../../shared';
import { UNSUPPORTED_SURFACE_MESSAGE } from '../../shared/hooks/unsupportedSurface';
import {
  useLineageFlow,
  useLineageStatus,
  useLineageStore,
} from '../../stores/lineageStore';
import { DAG_NODE_TYPES } from './DagNode';

interface Props {
  rootUniqueId: string;
  depth?: number;
}

const LINEAGE_DEPTH = 3;

// Hoisted: an object literal here would be a new prop identity on every render, which
// React Flow treats as a changed edge default.
const DEFAULT_EDGE_OPTIONS = { type: 'smoothstep' };

function BaseDagCanvas() {
  const { nodes, edges, onNodesChange, onEdgesChange, onConnect } = useLineageFlow();
  const { status, error, rootUniqueId } = useLineageStatus();
  // Positions the cards once React Flow has measured them — see the hook. Must be
  // under the <ReactFlow> below, which is what does the measuring.
  useLayoutWhenMeasured();
  // A boolean rather than the nodes: this component only needs to know whether there
  // is a laid-out graph to frame, and a primitive doesn't change identity on every
  // drag frame.
  const isLaidOut = useLineageStore((s) => s.isLaidOut);
  const { fitView } = useReactFlow();
  // React Flow stamps `light` or `dark` on the canvas root for its own theming, and
  // biga's tokens are scoped by those exact class names (`:root .light` / `:root .dark`
  // in styles/tokens.css). So the canvas is a theme scope whether we want one or not,
  // and leaving it on the default (`light`) re-themes every token inside it — a dark
  // app would render a light node on a dark page. `colorMode="system"` is not an escape
  // hatch either: it resolves to a class from the media query. Feed it the app's
  // resolved theme instead.
  const { resolved } = useTheme();

  // Frame the graph once it has been laid out. Keyed on `isLaidOut`, not on `nodes` —
  // the latter changes on every drag and would yank the viewport out from under the
  // cursor. Before the layout there is nothing worth framing: the cards are stacked at
  // the origin waiting to be measured, and fitting on that would frame a single point
  // and then have to jump.
  useEffect(() => {
    if (!isLaidOut) return;
    const frame = requestAnimationFrame(() => fitView({ padding: 0.2, duration: 200 }));
    return () => cancelAnimationFrame(frame);
  }, [isLaidOut, rootUniqueId, fitView]);

  if (status === 'error' && error) {
    return (
      <div className="err">
        Failed to load lineage: <code className="inline">{error.message}</code>
      </div>
    );
  }
  if (status === 'unsupported') {
    return (
      <p className="muted" style={{ fontSize: 13 }}>
        {UNSUPPORTED_SURFACE_MESSAGE}
      </p>
    );
  }
  if (nodes.length === 0) {
    return (
      <p className="muted flex items-center gap-2" style={{ fontSize: 13 }}>
        <Spinner /> Loading lineage…
      </p>
    );
  }

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      onConnect={onConnect}
      connectionMode={ConnectionMode.Loose}
      attributionPosition="bottom-left"
      minZoom={0.1}
      maxZoom={2}
      defaultEdgeOptions={DEFAULT_EDGE_OPTIONS}
      nodeTypes={DAG_NODE_TYPES}
      colorMode={resolved}
      nodesConnectable={false}
      nodesDraggable={false}
      elementsSelectable={true}
    >
      <Background color="var(--muted-foreground)" gap={20} size={1} />
      <Controls position="bottom-right" showInteractive={false} showFitView={false} />
    </ReactFlow>
  );
}

/** Owns the one fetch: hydrates the app-wide lineage store from DuckDB, then renders
 *  a canvas that — like any other view of the same graph — reads it back out of the
 *  store rather than taking it as props. */
export function BaseDag({ rootUniqueId, depth = LINEAGE_DEPTH }: Props) {
  useHydrateLineageStore(rootUniqueId, depth);
  return <BaseDagCanvas />;
}
