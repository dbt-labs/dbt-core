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
  resourceIconMap: new Proxy({}, { get: () => 'metric' }),
}));
vi.mock('@dbt-labs/sourdough', async (importOriginal) => {
  const mod = await importOriginal<typeof import('@dbt-labs/sourdough')>();
  return { ...mod, Icon: () => null };
});

function makeResponse(data: unknown) {
  return vi.fn(() => Promise.resolve({ ok: true, json: () => Promise.resolve(data) }));
}

import { MetricFilterView } from './SimpleFilterViews';

describe('<MetricFilterView />', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('loads metrics and shows total count', async () => {
    vi.stubGlobal(
      'fetch',
      makeResponse({
        data: [
          {
            unique_id: 'metric.pkg.revenue',
            name: 'revenue',
            resource_type: 'metric',
            package_name: 'pkg',
          },
        ],
        page_info: { total_count: 1, has_next_page: false, end_cursor: null },
      }),
    );
    renderWithProviders(
      <MetricFilterView project={{ name: 'test_project' }} onPeek={vi.fn()} />,
    );
    await waitFor(() => expect(screen.getByText('Loaded 1 of 1')).toBeInTheDocument());
    expect(screen.getByText('Metrics')).toBeInTheDocument();
  });

  it('shows empty state when no metrics', async () => {
    vi.stubGlobal(
      'fetch',
      makeResponse({
        data: [],
        page_info: { total_count: 0, has_next_page: false, end_cursor: null },
      }),
    );
    renderWithProviders(
      <MetricFilterView project={{ name: 'test_project' }} onPeek={vi.fn()} />,
    );
    await waitFor(() =>
      expect(screen.getByText('No metrics found.')).toBeInTheDocument(),
    );
  });
});
