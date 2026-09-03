import Dagre from '@dagrejs/dagre';
import { type Edge, type Node as ReactFlowNode, Position } from '@xyflow/react';

// these correspond to GraphLabel options in dagre
export interface LayoutOptions {
  /** Direction of the graph layout */
  rankdir?: 'TB' | 'LR';
  /** Vertical spacing between ranks */
  ranksep?: number;
  /** Horizontal spacing between nodes */
  nodesep?: number;
  /** Spacing around edges */
  edgesep?: number;
  /** Margin around the graph */
  marginx?: number;
  marginy?: number;
  /** Width of nodes */
  nodeWidth?: number;
  /** Height of nodes */
  nodeHeight?: number;
  /** Ranker used to layout nodes */
  ranker?: string;
  /** Acyclicer type */
  acyclicer?: string;
}

// Dagre layout constants.
//
// NODE_WIDTH / NODE_HEIGHT are a fallback, not the layout's idea of a card. The graph
// is laid out from `node.measured`, the size React Flow read off the rendered card, so
// a wide card gets a wide slot. These are only what an unmeasured node is given, and
// the store does not lay out until every card has been measured — so in practice they
// are reached only by a node dagre knows nothing about.
export const NODE_WIDTH = 245;
export const NODE_HEIGHT = 108;
export const RANK_SEPARATION = 100;
export const NODE_SEPARATION = 20;
export const EDGE_SEPARATION = 30;
export const GRAPH_MARGIN = 50;
export const DEFAULT_RANKER = 'longest-path';
export const DEFAULT_ACYCLICER = 'greedy';

export const DEFAULT_LAYOUT_OPTIONS: Required<LayoutOptions> = {
  rankdir: 'LR',
  ranksep: RANK_SEPARATION,
  nodesep: NODE_SEPARATION,
  edgesep: EDGE_SEPARATION,
  marginx: GRAPH_MARGIN,
  marginy: GRAPH_MARGIN,
  nodeWidth: NODE_WIDTH,
  nodeHeight: NODE_HEIGHT,
  ranker: 'longest-path',
  acyclicer: 'greedy',
};

function handlePositionsForRankdir(rankdir: 'TB' | 'LR') {
  return rankdir === 'TB'
    ? { sourcePosition: Position.Bottom, targetPosition: Position.Top }
    : { sourcePosition: Position.Right, targetPosition: Position.Left };
}

/** The box to reserve for a node: what React Flow measured off the rendered card,
 *  falling back to the design's geometry before the first measurement. `||` rather
 *  than `??` on purpose — a node measured at 0 is one that has not really been laid
 *  out by the browser yet, and 0 is not a size worth laying out around. */
function boxOf(node: ReactFlowNode, opts: Required<LayoutOptions>) {
  return {
    width: node.measured?.width || opts.nodeWidth,
    height: node.measured?.height || opts.nodeHeight,
  };
}

export function applyDagreLayout<T extends ReactFlowNode>(
  nodes: T[],
  edges: Edge[],
  options: LayoutOptions = {},
  onDagreLayoutFailure?: (error: unknown) => void,
): T[] {
  if (nodes.length === 0) {
    return nodes;
  }

  const opts = { ...DEFAULT_LAYOUT_OPTIONS, ...options };

  const g = new Dagre.graphlib.Graph({ directed: true, compound: false })
    .setGraph({
      rankdir: opts.rankdir,
      ranksep: opts.ranksep,
      nodesep: opts.nodesep,
      edgesep: opts.edgesep,
      // marginx: opts.marginx,
      // marginy: opts.marginy,
      ranker: opts.ranker,
      acyclicer: opts.acyclicer,
    })
    .setDefaultEdgeLabel(() => ({}));

  // Lay the graph out on the size each card actually renders to, not on one assumed
  // box. That is the whole point of measuring first: a card is as wide as its name.
  nodes.forEach((node) => {
    g.setNode(node.id, boxOf(node, opts));
  });

  // Add edges to dagre graph
  edges.forEach((edge) => {
    if (g.hasNode(edge.source) && g.hasNode(edge.target)) {
      g.setEdge(edge.source, edge.target);
    }
  });

  try {
    Dagre.layout(g);
  } catch (error) {
    onDagreLayoutFailure?.(error);
    return applyGridFallbackLayout(nodes, opts);
  }

  // Apply calculated positions to nodes
  const handles = handlePositionsForRankdir(opts.rankdir);
  return nodes.map((node) => {
    const dagreNode = g.node(node.id);

    if (!dagreNode) {
      return { ...node, ...handles };
    }

    return {
      ...node,
      ...handles,
      position: {
        // dagre reports a node's CENTRE; React Flow positions by the top-left corner.
        // Convert with the node's own box — `dagreNode.width`/`.height` are what was
        // handed to `setNode` above. Halving the constant instead would undo the
        // measuring: dagre would space the rank for a 600px card and then place it as
        // if it were 245px, dropping it 178px left of its slot and onto its neighbour.
        x: Math.round(dagreNode.x - dagreNode.width / 2),
        y: Math.round(dagreNode.y - dagreNode.height / 2),
      },
    };
  });
}

/** Last resort when dagre throws: a plain grid, spaced by the widest and tallest card
 *  so variable-size nodes still clear each other. */
export function applyGridFallbackLayout<T extends ReactFlowNode>(
  nodes: T[],
  opts: Required<LayoutOptions>,
): T[] {
  const cols = Math.ceil(Math.sqrt(nodes.length));
  const handles = handlePositionsForRankdir(opts.rankdir);
  const boxes = nodes.map((node) => boxOf(node, opts));
  const cellWidth = Math.max(...boxes.map((b) => b.width)) + opts.nodesep;
  const cellHeight = Math.max(...boxes.map((b) => b.height)) + opts.ranksep;
  return nodes.map((node, index) => {
    const col = index % cols;
    const row = Math.floor(index / cols);
    return {
      ...node,
      ...handles,
      position: {
        x: Math.round(opts.marginx + col * cellWidth),
        y: Math.round(opts.marginy + row * cellHeight),
      },
    };
  });
}

/**
 * Hide the cards while React Flow measures them.
 *
 * `style: { visibility: 'hidden' }`, deliberately, and NOT React Flow's `hidden`
 * property. A node with `hidden: true` renders `null` and never gets a ResizeObserver
 * attached, so it is never measured — the layout waiting on those measurements would
 * never run and the graph would never appear. `visibility: hidden` keeps the card in
 * the DOM at its true size and merely stops it being painted, which is the only form
 * of hidden that is still measurable.
 */
export function hideForMeasurement<T extends ReactFlowNode>(nodes: T[]): T[] {
  return nodes.map((node) => ({
    ...node,
    style: { ...node.style, visibility: 'hidden' as const },
  }));
}

/** Undo {@link hideForMeasurement}, once the cards have somewhere to be. */
export function reveal<T extends ReactFlowNode>(nodes: T[]): T[] {
  return nodes.map((node) => ({
    ...node,
    style: { ...node.style, visibility: 'visible' as const },
  }));
}

/** Whether every card carries the size React Flow measured for it — the precondition
 *  for laying out. One unmeasured card would be placed on the fallback box and land on
 *  its neighbours, which is the whole failure this protocol exists to avoid. */
export function allMeasured(nodes: ReactFlowNode[]): boolean {
  return (
    nodes.length > 0 && nodes.every((n) => n.measured?.width && n.measured?.height)
  );
}
