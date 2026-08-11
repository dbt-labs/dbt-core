import { screen, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { renderWithProviders } from '../test/renderWithProviders';

vi.mock('../shared', async (importOriginal) => {
  const mod = await importOriginal<typeof import('../shared')>();
  return {
    ...mod,
    useResourceLink: () => ({ home: () => '/' }),
    SimpleLinkBreadcrumbs: ({ breadcrumbs }: { breadcrumbs: { text: string }[] }) => (
      <nav>{breadcrumbs.map((b) => b.text).join(' / ')}</nav>
    ),
  };
});
vi.mock('@dbt-labs/dbt-dag', () => ({
  resourceIconMap: new Proxy({}, { get: () => 'semantic_model' }),
}));
vi.mock('@dbt-labs/sourdough', async (importOriginal) => {
  const mod = await importOriginal<typeof import('@dbt-labs/sourdough')>();
  return { ...mod, Icon: () => null };
});

function makeResponse(data: unknown) {
  return vi.fn(() => Promise.resolve({ ok: true, json: () => Promise.resolve(data) }));
}

import { SemanticModelFilterView } from './SimpleFilterViews';

describe('<SemanticModelFilterView />', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('loads semantic models and shows total count', async () => {
    vi.stubGlobal(
      'fetch',
      makeResponse({
        data: [
          {
            unique_id: 'semantic_model.pkg.orders_sm',
            name: 'orders_sm',
            package_name: 'pkg',
            entities: [
              { name: 'order', type: 'primary' },
              { name: 'customer', type: 'foreign' },
            ],
            description: 'Orders semantic model',
            truncated: false,
          },
        ],
        page_info: {
          total_count: 1,
          has_next_page: false,
          start_cursor: null,
          end_cursor: null,
        },
      }),
    );
    renderWithProviders(
      <SemanticModelFilterView project={{ name: 'test_project' }} onPeek={vi.fn()} />,
    );
    await waitFor(() => expect(screen.getByText('Loaded 1 of 1')).toBeInTheDocument());
    expect(screen.getByText('Semantic models')).toBeInTheDocument();
  });

  it('shows empty state when no semantic models', async () => {
    vi.stubGlobal(
      'fetch',
      makeResponse({
        data: [],
        page_info: {
          total_count: 0,
          has_next_page: false,
          start_cursor: null,
          end_cursor: null,
        },
      }),
    );
    renderWithProviders(
      <SemanticModelFilterView project={{ name: 'test_project' }} onPeek={vi.fn()} />,
    );
    await waitFor(() =>
      expect(screen.getByText('No semantic models found.')).toBeInTheDocument(),
    );
  });
});
