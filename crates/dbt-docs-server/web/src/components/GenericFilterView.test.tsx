import type { ColumnDef } from '@tanstack/react-table';
import { screen, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { ModelSummary } from '../shared';
import { renderWithProviders } from '../test/renderWithProviders';
import { GenericFilterView } from './GenericFilterView';

/** Local mirror of the REST list envelope this test mocks (the adapter maps it
 *  into the domain shape). */
interface ModelListResponse {
  data: Record<string, unknown>[];
  page_info: {
    total_count: number;
    start_cursor: string | null;
    end_cursor: string | null;
    has_next_page: boolean;
  };
}

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

// Capture the sort-related props GenericFilterView forwards.
vi.mock('./ResourceFilterTable', () => ({
  ResourceFilterTable: ({
    isSortable,
    initialSortColumn,
    initialSortDesc,
  }: {
    isSortable?: boolean;
    initialSortColumn?: string;
    initialSortDesc?: boolean;
  }) => (
    <div
      data-testid="rft"
      data-sortable={String(isSortable)}
      data-initial-col={initialSortColumn ?? ''}
      data-initial-desc={String(initialSortDesc)}
    />
  ),
}));

const EMPTY_LIST: ModelListResponse = {
  data: [],
  page_info: {
    total_count: 0,
    has_next_page: false,
    end_cursor: null,
    start_cursor: null,
  },
};

const columns: ColumnDef<ModelSummary>[] = [
  { id: 'name', accessorFn: (row) => row.name },
];

describe('<GenericFilterView />', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('forwards sort props to ResourceFilterTable', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() =>
        Promise.resolve({ ok: true, json: () => Promise.resolve(EMPTY_LIST) }),
      ),
    );
    renderWithProviders(
      <GenericFilterView<ModelSummary>
        label="Models"
        project={{ name: 'p' }}
        resourceType="model"
        columns={columns}
        isSortable
        initialSortColumn="executed_at"
        initialSortDesc
      />,
    );
    const rft = await screen.findByTestId('rft');
    expect(rft.dataset.sortable).toBe('true');
    expect(rft.dataset.initialCol).toBe('executed_at');
    expect(rft.dataset.initialDesc).toBe('true');
  });

  it('threads sort into the list fetch (sort= in URL)', async () => {
    const fetchSpy = vi.fn((_url: string) =>
      Promise.resolve({ ok: true, json: () => Promise.resolve(EMPTY_LIST) }),
    );
    vi.stubGlobal('fetch', fetchSpy);
    renderWithProviders(
      <GenericFilterView<ModelSummary>
        label="Models"
        project={{ name: 'p' }}
        resourceType="model"
        columns={columns}
        sort={{ field: 'executed_at', desc: true }}
      />,
    );
    await waitFor(() =>
      expect(
        fetchSpy.mock.calls.some((c) =>
          String(c[0]).includes('sort=executed_at%3Adesc'),
        ),
      ).toBe(true),
    );
  });
});
