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
  resourceIconMap: new Proxy({}, { get: () => 'group' }),
}));
vi.mock('@dbt-labs/sourdough', async (importOriginal) => {
  const mod = await importOriginal<typeof import('@dbt-labs/sourdough')>();
  return { ...mod, Icon: () => null };
});

function makeResponse(data: unknown) {
  return vi.fn(() => Promise.resolve({ ok: true, json: () => Promise.resolve(data) }));
}

import { GroupFilterView } from './SimpleFilterViews';

describe('<GroupFilterView />', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('loads groups and shows total count', async () => {
    vi.stubGlobal(
      'fetch',
      makeResponse({
        data: [
          {
            unique_id: 'group.pkg.finance',
            name: 'finance',
            resource_type: 'group',
            package_name: 'pkg',
          },
        ],
        page_info: { total_count: 1, has_next_page: false, end_cursor: null },
      }),
    );
    renderWithProviders(
      <GroupFilterView project={{ name: 'test_project' }} onPeek={vi.fn()} />,
    );
    await waitFor(() => expect(screen.getByText('Loaded 1 of 1')).toBeInTheDocument());
    expect(screen.getByText('Groups')).toBeInTheDocument();
  });

  it('shows empty state when no groups', async () => {
    vi.stubGlobal(
      'fetch',
      makeResponse({
        data: [],
        page_info: { total_count: 0, has_next_page: false, end_cursor: null },
      }),
    );
    renderWithProviders(
      <GroupFilterView project={{ name: 'test_project' }} onPeek={vi.fn()} />,
    );
    await waitFor(() =>
      expect(screen.getByText('No groups found.')).toBeInTheDocument(),
    );
  });
});
