import type { Meta, StoryObj } from '@storybook/react-vite';
import { expect, waitFor } from 'storybook/test';

import { storyLineage } from '../../shared/testing/storyFixtures';
import { storyDataSource } from '../../shared/testing/storySources';
import { BaseDag } from './BaseDag';
import { DAG_NODE_MIN_WIDTH } from './DagNode';

/** A card's box in flow coordinates. React Flow positions nodes with a transform, so
 *  the offset box is the unscaled size and the transform the unscaled position —
 *  `getBoundingClientRect` would fold in whatever zoom `fitView` settled on. */
function boxOf(card: HTMLElement) {
  const [x = 0, y = 0] = (card.style.transform.match(/-?[\d.]+(?=px)/g) ?? []).map(
    Number,
  );
  return { x, y, width: card.offsetWidth, height: card.offsetHeight };
}

type Box = ReturnType<typeof boxOf>;

function overlaps(a: Box, b: Box) {
  return (
    a.x < b.x + b.width &&
    b.x < a.x + a.width &&
    a.y < b.y + b.height &&
    b.y < a.y + a.height
  );
}

/** The cards, once they have been measured, positioned and revealed. Until then they
 *  are stacked at the origin with `visibility: hidden`, so waiting on visibility is
 *  what says the whole cycle completed rather than stalling half way. */
async function laidOutCards(canvasElement: HTMLElement) {
  return waitFor(() => {
    const cards = Array.from(
      canvasElement.querySelectorAll<HTMLElement>('.react-flow__node'),
    );
    expect(cards.length).toBeGreaterThan(1);
    for (const card of cards) expect(card).toBeVisible();
    return cards;
  });
}

const meta: Meta<typeof BaseDag> = {
  component: BaseDag,
  args: {
    rootUniqueId: 'model.jaffle_shop.customers',
  },
  // React Flow measures its parent, so the canvas needs a sized one to render into at all.
  decorators: [(Story) => <div className="h-[520px] w-full">{Story()}</div>],
};

export default meta;
type Story = StoryObj<typeof BaseDag>;

/**
 * The default story fixture's lineage.
 *
 * The play function guards the measure-then-lay-out cycle end to end: `hydrate` hands
 * the canvas a graph stacked at the origin with every card hidden, and only
 * `useLayoutWhenMeasured` reveals and positions them. If that stalls the canvas goes
 * blank rather than throwing, so assert the cards are visible and apart.
 */
export const Default: Story = {
  play: async ({ canvasElement }) => {
    const cards = await laidOutCards(canvasElement);

    // Every card landed somewhere of its own — none left stacked at the origin.
    const positions = cards.map((card) => card.style.transform);
    await expect(new Set(positions).size).toBe(positions.length);
  },
};

/**
 * Long names, which is what a real project is full of. Two things have to hold, and
 * they are the pair a fixed-width layout gets wrong: the card grows to its name rather
 * than ellipsing it, and dagre spaces the graph by that measured width. Laying out on
 * a constant 245px while the cards render wider is not a subtle bug — the ranks
 * collide — so overlap is the assertion.
 */
export const LongNames: Story = {
  args: { rootUniqueId: 'model.jaffle_shop.int_order_items_joined_to_customers' },
  parameters: {
    docsApp: {
      source: storyDataSource({
        fetchLineage: async () => {
          const names = [
            'stg_jaffle_shop__order_items_with_products',
            'stg_jaffle_shop__customers_deduplicated',
            'int_order_items_joined_to_customers',
            'fct_order_items_revenue_by_customer_cohort',
            'orders',
          ];
          const [a, b, root, wide, short] = names;
          const id = (name: string) => `model.jaffle_shop.${name}`;
          return {
            nodes: names.map((name) => ({
              uniqueId: id(name),
              name,
              resourceType: 'model' as const,
              description: null,
              packageName: 'jaffle_shop',
              tags: [],
              materialized: 'view',
            })),
            edges: [
              { upstreamUniqueId: id(a), downstreamUniqueId: id(root) },
              { upstreamUniqueId: id(b), downstreamUniqueId: id(root) },
              { upstreamUniqueId: id(root), downstreamUniqueId: id(wide) },
              { upstreamUniqueId: id(root), downstreamUniqueId: id(short) },
            ],
          };
        },
      }),
    },
  },
  play: async ({ canvasElement }) => {
    const boxes = (await laidOutCards(canvasElement)).map(boxOf);

    // Not all one width: the long names grew past the floor, the short one sat on it.
    await expect(Math.max(...boxes.map((b) => b.width))).toBeGreaterThan(
      DAG_NODE_MIN_WIDTH,
    );
    await expect(Math.min(...boxes.map((b) => b.width))).toBe(DAG_NODE_MIN_WIDTH);

    // And the layout used those widths: nothing collides.
    for (let i = 0; i < boxes.length; i++) {
      for (let j = i + 1; j < boxes.length; j++) {
        await expect(overlaps(boxes[i], boxes[j])).toBe(false);
      }
    }
  },
};

export const SimpleExample: Story = {
  args: {
    rootUniqueId: 'model.jaffle_shop.customers',
  },
  parameters: {
    docsApp: {
      source: storyDataSource({
        fetchLineage: async () => {
          const base = storyLineage();
          const extra = Array.from({ length: 10 }, (_, i) => ({
            uniqueId: `model.jaffle_shop.downstream_${i}`,
            name: `downstream_${i}`,
            resourceType: 'model' as const,
            description: null,
            packageName: 'jaffle_shop',
            tags: [],
            materialized: 'view',
          }));
          return {
            nodes: [...base.nodes, ...extra],
            edges: [
              ...base.edges,
              ...extra.map((n) => ({
                upstreamUniqueId: 'model.jaffle_shop.customers',
                downstreamUniqueId: n.uniqueId,
              })),
            ],
          };
        },
      }),
    },
  },
};
