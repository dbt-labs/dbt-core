import type { Meta, StoryObj } from '@storybook/react-vite';
import { ReactFlow } from '@xyflow/react';
import { expect, userEvent, waitFor, within } from 'storybook/test';

import {
  DAG_NODE_HEIGHT,
  DAG_NODE_MAX_WIDTH,
  DAG_NODE_MIN_WIDTH,
  DAG_NODE_TYPE,
  DAG_NODE_TYPES,
  type DagNodeData,
  type DagNodeType,
} from './DagNode';

/**
 * `DagNode` is a registered React Flow node type, not a standalone component: its
 * `Handle`s need the canvas's store, and React Flow is what positions it. So every
 * story mounts a real (tiny) canvas — which is also the only way to see that edges
 * actually attach to the handles.
 */
type PreviewProps = {
  nodes: { data: DagNodeData; selected?: boolean }[];
  /** Draw an edge between consecutive nodes, to check handle placement. */
  connected?: boolean;
  /**
   * Has to be passed explicitly, and has to agree with the story's theme.
   *
   * React Flow stamps `light` or `dark` on the canvas root, and biga's tokens are
   * scoped by those same class names — so the canvas re-themes everything inside it,
   * and a canvas in the wrong mode renders a light node on a dark page. In the app
   * `BaseDag` feeds this from `useTheme`; here it is an arg, which means flipping
   * Storybook's global theme toolbar does *not* follow. Use the `LightMode` story to
   * review the light variant.
   */
  colorMode?: 'light' | 'dark';
};

/** A real component rather than an inline `render` closure, because the node list has
 *  to be built from args before React Flow sees it. */
function DagNodePreview({ nodes, connected, colorMode = 'dark' }: PreviewProps) {
  // Hand-positioned on a grid: these stories are about the card, not the layout, so
  // they skip the measure-then-lay-out cycle and just wrap so several nodes stay
  // legible instead of running off-canvas.
  const perRow = connected ? nodes.length : 3;
  const flowNodes: DagNodeType[] = nodes.map((node, i) => ({
    id: `n${i}`,
    type: DAG_NODE_TYPE,
    position: {
      x: (i % perRow) * (DAG_NODE_MIN_WIDTH + 260),
      y: Math.floor(i / perRow) * (DAG_NODE_HEIGHT + 48),
    },
    data: node.data,
    selected: node.selected,
  }));

  const edges = connected
    ? flowNodes.slice(1).map((node, i) => ({
        id: `e${i}`,
        source: `n${i}`,
        target: node.id,
        type: 'smoothstep',
      }))
    : [];

  return (
    <div style={{ width: '100%', height: 320 }}>
      <ReactFlow
        nodes={flowNodes}
        edges={edges}
        nodeTypes={DAG_NODE_TYPES}
        colorMode={colorMode}
        fitView
        fitViewOptions={{ padding: 0.25 }}
        nodesDraggable={false}
        nodesConnectable={false}
        proOptions={{ hideAttribution: true }}
      />
    </div>
  );
}

const meta: Meta<typeof DagNodePreview> = {
  component: DagNodePreview,
  args: {
    nodes: [
      { data: { name: 'dim_customers', resourceType: 'model', columnCount: 25 } },
    ],
  },
};

export default meta;
type Story = StoryObj<typeof DagNodePreview>;

/** The design's resting state: name header, resource-type badge, column-count chip. */
export const Default: Story = {
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement);
    await waitFor(() => expect(canvas.getByText('dim_customers')).toBeVisible());
    await expect(canvas.getByText('Model')).toBeVisible();
    await expect(canvas.getByText('25')).toBeVisible();
  },
};

/** The same node in light mode. Both the story theme and the canvas's `colorMode` are
 *  switched, since they are two independent knobs — see `colorMode` above. */
export const LightMode: Story = {
  args: { colorMode: 'light' },
  parameters: { themes: { themeOverride: 'light' } },
};

/** Selected. Only the border changes — the four-layer shadow is the node's resting
 *  elevation, not a selection cue, so it stays put. */
export const Active: Story = {
  args: {
    nodes: [
      {
        data: { name: 'dim_customers', resourceType: 'model', columnCount: 25 },
        selected: true,
      },
    ],
  },
};

/**
 * No column-level lineage. `columnCount` absent hides the chip entirely, which is the
 * common case for this site — the exporter only writes column lineage after a compile
 * with `--static-analysis strict`.
 */
export const WithoutColumnLineage: Story = {
  args: {
    nodes: [{ data: { name: 'dim_customers', resourceType: 'model' } }],
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement);
    await waitFor(() => expect(canvas.getByText('dim_customers')).toBeVisible());
    await expect(canvas.queryByTitle(/columns with lineage/)).toBeNull();
  },
};

/** Zero is a count, not an absence: the chip renders. Only `null`/`undefined` hides
 *  it, which is what separates "analysed, no columns" from "not analysed". */
export const ZeroColumns: Story = {
  args: {
    nodes: [{ data: { name: 'dim_customers', resourceType: 'model', columnCount: 0 } }],
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement);
    await waitFor(() => expect(canvas.getByText('0')).toBeVisible());
  },
};

/**
 * A long name widens the node instead of ellipsing. Names in a real project share long
 * prefixes — `int_order_items_…` clipped to 245px is indistinguishable from its
 * neighbours — so the card grows, and the layout reads that measured width back off
 * the DOM and spaces the rank around it. No model, source or seed in the three real
 * lineages this was measured against reaches the max, so this is the normal case.
 */
const LONG_NAME = 'int_order_items_joined_to_customers_and_products';

export const LongName: Story = {
  args: {
    nodes: [{ data: { name: LONG_NAME, resourceType: 'model', columnCount: 132 } }],
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement);
    const name = await waitFor(() => {
      const el = canvas.getByText(LONG_NAME);
      expect(el).toBeVisible();
      return el;
    });

    // Rendered in full, not clipped: `scrollWidth` would exceed `clientWidth` if the
    // ellipsis had kicked in.
    await expect(name.scrollWidth).toBe(name.clientWidth);

    const node = name.closest<HTMLElement>('.dag-node');
    expect(node).not.toBeNull();
    // fitView scales the canvas, so compare the unscaled offset box.
    await expect(node!.offsetWidth).toBeGreaterThan(DAG_NODE_MIN_WIDTH);
    await expect(node!.offsetWidth).toBeLessThanOrEqual(DAG_NODE_MAX_WIDTH);
  },
};

/**
 * Past the max the card stops growing and the name ellipses, with the tooltip back to
 * make it recoverable. This is what dbt's generated test names look like — the one
 * below is real, and 3737px wide if nothing caps it, which would set the width of its
 * whole rank and zoom the rest of the graph down to illegible.
 */
const GENERATED_TEST_NAME =
  'accepted_values_int__customer_oam_stage_summary_stage_action__Orchestration_Has_Models_Built__Users_Cloud_CLI_or_Cloud_IDE';

export const NamePastTheMaximum: Story = {
  args: {
    nodes: [{ data: { name: GENERATED_TEST_NAME, resourceType: 'test' } }],
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement);
    const name = await waitFor(() => {
      const el = canvas.getByText(GENERATED_TEST_NAME);
      expect(el).toBeVisible();
      return el;
    });

    const node = name.closest<HTMLElement>('.dag-node');
    expect(node).not.toBeNull();
    await expect(node!.offsetWidth).toBe(DAG_NODE_MAX_WIDTH);
    // Clipped, which is the precondition for the tooltip existing at all.
    await expect(name.scrollWidth).toBeGreaterThan(name.clientWidth);

    await userEvent.hover(name);
    // By role, not by text: the card itself still *contains* the full name (it is
    // clipped by CSS, not truncated in the DOM), so matching on text would be
    // ambiguous. Portalled to <body>, behind the tooltip's 200ms open delay.
    const tooltip = await within(document.body).findByRole('tooltip', undefined, {
      timeout: 3000,
    });
    await expect(tooltip).toHaveTextContent(GENERATED_TEST_NAME);
    // And it wraps inside its own bubble. A name this long is a single unbroken token,
    // so without `break-words` on the tooltip it renders 2286px wide across a 320px
    // box and the text spills across the canvas — the tooltip is there to make the
    // name recoverable, which it is not if it overflows.
    await expect(tooltip.scrollWidth).toBeLessThanOrEqual(tooltip.clientWidth + 1);
  },
};

/** Two nodes and an edge: the handles are invisible but present, so the edge meets the
 *  node border on the correct sides for a left-to-right layout. */
export const Connected: Story = {
  args: {
    connected: true,
    nodes: [
      { data: { name: 'stg_customers', resourceType: 'model', columnCount: 9 } },
      { data: { name: 'dim_customers', resourceType: 'model', columnCount: 25 } },
    ],
  },
};

/** The badge colour is per resource type, from the `--fgViz*` tokens. */
export const ResourceTypes: Story = {
  args: {
    nodes: [
      { data: { name: 'dim_customers', resourceType: 'model', columnCount: 25 } },
      { data: { name: 'raw_customers', resourceType: 'seed', columnCount: 4 } },
      { data: { name: 'jaffle_shop', resourceType: 'source', columnCount: 12 } },
      { data: { name: 'scd_orders', resourceType: 'snapshot', columnCount: 18 } },
      { data: { name: 'revenue', resourceType: 'metric' } },
      { data: { name: 'weekly_report', resourceType: 'exposure' } },
      { data: { name: 'cents_to_dollars', resourceType: 'macro' } },
      { data: { name: 'orders_semantics', resourceType: 'semantic_model' } },
    ],
  },
};

/** An unrecognised type still renders: neutral fill, and the raw string as the label
 *  rather than a blank badge. */
export const UnknownResourceType: Story = {
  args: {
    nodes: [{ data: { name: 'mystery', resourceType: 'sql_operation' } }],
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement);
    await waitFor(() => expect(canvas.getByText('sql_operation')).toBeVisible());
  },
};

/** A name shorter than the minimum does not shrink the card below it — short names
 *  would otherwise render as chips and a rank would lose its rhythm. Height is fixed
 *  either way, so a rank still reads as a row. See `LongName` for the other end. */
export const MinimumWidth: Story = {
  args: {
    nodes: [{ data: { name: 'orders', resourceType: 'model', columnCount: 4 } }],
  },
  play: async ({ canvasElement }) => {
    const node = await waitFor(() => {
      const el = canvasElement.querySelector<HTMLElement>('.dag-node');
      expect(el).not.toBeNull();
      expect(el).toBeVisible();
      return el as HTMLElement;
    });

    // fitView scales the canvas, so compare the unscaled offset box rather than the
    // rendered rect.
    await expect(node.offsetWidth).toBe(DAG_NODE_MIN_WIDTH);
    await expect(node.offsetHeight).toBe(DAG_NODE_HEIGHT);
  },
};
