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
  resourceIconMap: new Proxy({}, { get: () => 'exposure' }),
}));
vi.mock('@dbt-labs/sourdough', async (importOriginal) => {
  const mod = await importOriginal<typeof import('@dbt-labs/sourdough')>();
  return { ...mod, Icon: () => null };
});

function makeResponse(data: unknown) {
  return vi.fn(() => Promise.resolve({ ok: true, json: () => Promise.resolve(data) }));
}

import { ExposureFilterView } from './SimpleFilterViews';

describe('<ExposureFilterView />', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('loads exposures and shows total count', async () => {
    vi.stubGlobal(
      'fetch',
      makeResponse({
        data: [
          {
            unique_id: 'exposure.pkg.dashboard',
            name: 'dashboard',
            resource_type: 'exposure',
            package_name: 'pkg',
          },
        ],
        page_info: { total_count: 1, has_next_page: false, end_cursor: null },
      }),
    );
    renderWithProviders(
      <ExposureFilterView project={{ name: 'test_project' }} onPeek={vi.fn()} />,
    );
    await waitFor(() => expect(screen.getByText('Loaded 1 of 1')).toBeInTheDocument());
    expect(screen.getByText('Exposures')).toBeInTheDocument();
  });

  it('shows empty state when no exposures', async () => {
    vi.stubGlobal(
      'fetch',
      makeResponse({
        data: [],
        page_info: { total_count: 0, has_next_page: false, end_cursor: null },
      }),
    );
    renderWithProviders(
      <ExposureFilterView project={{ name: 'test_project' }} onPeek={vi.fn()} />,
    );
    await waitFor(() =>
      expect(screen.getByText('No exposures found.')).toBeInTheDocument(),
    );
  });
});
