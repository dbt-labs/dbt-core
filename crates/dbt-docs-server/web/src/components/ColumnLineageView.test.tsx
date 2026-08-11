import { render, screen } from '@testing-library/react';
import { describe, expect, test, vi } from 'vitest';

import type { ColumnLineageGraph } from '../shared';
import { ColumnLineageMini } from './ColumnLineageView';

// Capture the node ids the subgraph hands to the Dag so we can assert it was
// built from `graph.edges` (not reconstructed by string-parsing).
const dagNodeIds = vi.fn<(ids: string[]) => void>();
vi.mock('@dbt-labs/dbt-dag', () => ({
  Dag: ({ nodes }: { nodes: Array<{ id: string }> }) => {
    dagNodeIds(nodes.map((n) => n.id));
    return <div data-testid="dag" />;
  },
  transformationTypes: ['RENAME', 'RAW', 'UNKNOWN'],
}));

describe('ColumnLineageMini gated handling', () => {
  test('renders the UpgradeCard when result is gated and a userState exists', () => {
    render(
      <ColumnLineageMini
        rootUniqueId="model.shop.customers"
        columnName="id"
        state={{ kind: 'ready', result: { kind: 'gated' } }}
        load={vi.fn()}
        onSelect={vi.fn()}
        userState="core"
      />,
    );
    expect(screen.getByText('Column-level lineage')).toBeTruthy();
  });

  test('renders nothing when gated but no userState', () => {
    const { container } = render(
      <ColumnLineageMini
        rootUniqueId="model.shop.customers"
        columnName="id"
        state={{ kind: 'ready', result: { kind: 'gated' } }}
        load={vi.fn()}
        onSelect={vi.fn()}
        userState={null}
      />,
    );
    expect(container).toBeEmptyDOMElement();
  });
});

describe('ColumnLineageMini subgraph from graph.edges', () => {
  // Multi-parent target column + a source-only column that should not be
  // reachable from the target.
  const graph: ColumnLineageGraph = {
    nodes: [],
    edges: [
      {
        fromNodeUniqueId: 'source.shop.raw.orders',
        fromColumn: 'id',
        toNodeUniqueId: 'model.shop.customers',
        toColumn: 'customer_id',
        transformationType: 'rename',
      },
      {
        fromNodeUniqueId: 'source.shop.raw.users',
        fromColumn: 'uid',
        toNodeUniqueId: 'model.shop.customers',
        toColumn: 'customer_id',
        transformationType: 'rename',
      },
      {
        fromNodeUniqueId: 'source.shop.raw.orders',
        fromColumn: 'amount',
        toNodeUniqueId: 'model.shop.customers',
        toColumn: 'total',
        transformationType: 'rename',
      },
    ],
  };

  test('builds the subgraph touching the expanded column from graph.edges', () => {
    dagNodeIds.mockClear();
    render(
      <ColumnLineageMini
        rootUniqueId="model.shop.customers"
        columnName="customer_id"
        state={{ kind: 'ready', result: { kind: 'ok', graph } }}
        load={vi.fn()}
        onSelect={vi.fn()}
        userState={null}
      />,
    );

    expect(dagNodeIds).toHaveBeenCalled();
    const ids = dagNodeIds.mock.calls[0]![0];
    // Both parents of customer_id + the target are reachable.
    expect(ids).toEqual(
      expect.arrayContaining([
        'model.shop.customers.customer_id',
        'source.shop.raw.orders.id',
        'source.shop.raw.users.uid',
      ]),
    );
    // The unrelated `total` lineage (and its source column) is excluded.
    expect(ids).not.toContain('model.shop.customers.total');
    expect(ids).not.toContain('source.shop.raw.orders.amount');
  });
});
