import { Handle, type Node, type NodeProps, Position } from '@xyflow/react';

import { Tooltip } from '../ui/Tooltip';

/** Registered node type. Nodes must carry `type: DAG_NODE_TYPE` to render as this. */
export const DAG_NODE_TYPE = 'dagNode';

/**
 * These bounds are defined by `.dag-node` in app.css but these constants are used by
 * Storybook to check that cards are within bounds. The actual layout uses the dynamic
 * sizes set by Reactflow based on the length of the node's name
 */
export const DAG_NODE_MIN_WIDTH = 245;
export const DAG_NODE_MAX_WIDTH = 600;
export const DAG_NODE_HEIGHT = 108;

export type DagNodeData = {
  /** Display name — the resource's own name, not its unique_id. */
  name: string;
  resourceType: string;
  /** Column count for the column-lineage chip. The chip is hidden when this is
   *  null or undefined, which is the no-column-lineage state. */
  columnCount?: number | null;
} & Record<string, unknown>;

export type DagNodeType = Node<DagNodeData, typeof DAG_NODE_TYPE>;

/** Type labels. Hand-rolled rather than imported so this component owes nothing to
 *  dbt-dag; unknown types fall through to the raw string. */
const TYPE_LABEL: Record<string, string> = {
  analysis: 'Analysis',
  exposure: 'Exposure',
  function: 'Function',
  group: 'Group',
  macro: 'Macro',
  metric: 'Metric',
  model: 'Model',
  saved_query: 'Saved query',
  seed: 'Seed',
  semantic_model: 'Semantic model',
  snapshot: 'Snapshot',
  source: 'Source',
  test: 'Test',
  unit_test: 'Unit test',
};

/**
 * Resource icon (the cube).
 *
 * The `viewBox` is offset rather than at the origin: the path is copied verbatim from
 * the design export, where it sits at (46.5, 72) in a larger canvas. Letting the
 * viewBox do that translation keeps the path byte-identical to the source instead of
 * re-basing every coordinate by hand.
 */
function ResourceIcon() {
  return (
    <svg
      className="dag-node__type-icon"
      width="16"
      height="16"
      viewBox="46.5 72 18 18"
      fill="currentColor"
      aria-hidden="true"
      focusable="false"
    >
      <path d="M63.5038 75.9285L55.7895 72.0714C55.6095 71.9814 55.3974 71.9814 55.2174 72.0714L47.4966 75.9285C47.2781 76.0378 47.1431 76.2628 47.1431 76.5007V85.5007C47.1431 85.745 47.2781 85.9635 47.4966 86.0728L55.2109 89.93C55.3009 89.975 55.3974 90.0007 55.5002 90.0007C55.6031 90.0007 55.6995 89.975 55.7895 89.93L63.5038 86.0728C63.7224 85.9635 63.8574 85.7385 63.8574 85.5007V76.5007C63.8574 76.2564 63.7224 76.0314 63.5038 75.9285ZM55.5002 73.3635L61.7745 76.5007L55.5002 79.6378L49.2259 76.5007L55.5002 73.3635ZM48.4288 77.5421L54.8574 80.7564V88.3164L48.4288 85.1021V77.5421ZM56.1431 88.3164V80.7564L62.5716 77.5421V85.1021L56.1431 88.3164Z" />
    </svg>
  );
}

/** Columns icon, offset viewBox for the same reason as `ResourceIcon`. */
function ColumnsIcon() {
  return (
    <svg
      width="21"
      height="21"
      viewBox="161.5 70.5 21 21"
      fill="currentColor"
      aria-hidden="true"
      focusable="false"
    >
      <path d="M179.5 72H175.75C175.75 71.175 175.075 70.5 174.25 70.5H169.75C168.925 70.5 168.25 71.175 168.25 72H164.5C163.675 72 163 72.675 163 73.5V88.5C163 89.325 163.675 90 164.5 90H168.25C168.25 90.825 168.925 91.5 169.75 91.5H174.25C175.075 91.5 175.75 90.825 175.75 90H179.5C180.325 90 181 89.325 181 88.5V73.5C181 72.675 180.325 72 179.5 72ZM164.5 88.5V73.5H168.25V88.5H164.5ZM174.25 90H169.75V72H174.25V90ZM179.5 88.5H175.75V73.5H179.5V88.5Z" />
    </svg>
  );
}

export function DagNode({
  data,
  selected,
  sourcePosition = Position.Right,
  targetPosition = Position.Left,
}: NodeProps<DagNodeType>) {
  const { name, resourceType, columnCount } = data;
  const label = TYPE_LABEL[resourceType] ?? resourceType;
  const hasColumnLineage = columnCount != null;

  return (
    <div className={`dag-node${selected ? ' dag-node--active' : ''}`}>
      {/* Handle positions follow the layout direction rather than being pinned
          left/right, so a top-to-bottom graph attaches its edges correctly too. */}
      <Handle type="target" position={targetPosition} isConnectable={false} />

      <div className="dag-node__header">
        {/* The card grows to its name, so most names need no tooltip at all —
            `displayOnlyWhenTruncated` is what keeps it to the ones that actually hit
            the max-width and got clipped, which in practice means dbt's generated test
            names. Note this mounts one Radix provider per node; real graphs here reach
            ~700 nodes, so if that starts to show, hoist a single provider above the
            canvas instead. */}
        <Tooltip
          content={name}
          displayOnlyWhenTruncated
          className="dag-node__name-wrap"
        >
          {(ref) => (
            <span ref={ref} className="dag-node__name">
              {name}
            </span>
          )}
        </Tooltip>
      </div>

      <div className="dag-node__body">
        <Tooltip
          content={label}
          displayOnlyWhenTruncated
          className="dag-node__type-wrap"
        >
          {(ref) => (
            <span
              className={`dag-node__type dag-node__type--${resourceType}`}
              data-resource-type={resourceType}
            >
              <ResourceIcon />
              <span ref={ref} className="dag-node__type-label">
                {label}
              </span>
            </span>
          )}
        </Tooltip>

        {hasColumnLineage && (
          <Tooltip content={`${columnCount} columns with lineage`}>
            <span className="dag-node__columns">
              <span className="dag-node__columns-icon">
                <ColumnsIcon />
              </span>
              <span className="dag-node__columns-count">{columnCount}</span>
            </span>
          </Tooltip>
        )}
      </div>

      <Handle type="source" position={sourcePosition} isConnectable={false} />
    </div>
  );
}

/** Hoisted so it is one stable object: React Flow re-creates every node when the
 *  `nodeTypes` identity changes. */
export const DAG_NODE_TYPES = { [DAG_NODE_TYPE]: DagNode };
