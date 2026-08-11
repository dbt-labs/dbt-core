import { fireEvent, screen, waitFor } from '@testing-library/react';
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
    FilterDropdown: ({
      name,
      onChange,
      defaultOption,
      options,
    }: {
      name: string;
      onChange: (opt: { value: string }) => void;
      defaultOption: { label: string; value: string };
      options: { label: string; value: string }[];
    }) => (
      <select
        data-testid={`filter-${name}`}
        value={defaultOption.value}
        onChange={(e) => onChange({ value: e.target.value })}
      >
        {options.map((o) => (
          <option key={o.value} value={o.value}>
            {o.label}
          </option>
        ))}
      </select>
    ),
  };
});
vi.mock('@dbt-labs/dbt-dag', () => ({
  resourceIconMap: new Proxy({}, { get: () => 'macro' }),
}));
vi.mock('@dbt-labs/sourdough', async (importOriginal) => {
  const mod = await importOriginal<typeof import('@dbt-labs/sourdough')>();
  return { ...mod, Icon: () => null };
});

interface MacroFacetsBody {
  packages: { value: string; count: number | null }[];
}

function makeFetch(listData: unknown[], facets: MacroFacetsBody = { packages: [] }) {
  return vi.fn((url: string) => {
    const body = url.includes('facets')
      ? facets
      : {
          data: listData,
          page_info: {
            total_count: listData.length,
            has_next_page: false,
            end_cursor: null,
          },
        };
    return Promise.resolve({ ok: true, json: () => Promise.resolve(body) });
  });
}

import { MacroFilterView } from './MacroFilterView';

describe('<MacroFilterView />', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('loads macros and shows total count', async () => {
    vi.stubGlobal(
      'fetch',
      makeFetch([
        {
          unique_id: 'macro.pkg.generate_schema',
          name: 'generate_schema',
          resource_type: 'macro',
          package_name: 'pkg',
        },
      ]),
    );
    renderWithProviders(
      <MacroFilterView project={{ name: 'test_project' }} onPeek={vi.fn()} />,
    );
    await waitFor(() => expect(screen.getByText('Loaded 1 of 1')).toBeInTheDocument());
    expect(screen.getByText('Macros')).toBeInTheDocument();
  });

  it('shows empty state when no macros', async () => {
    vi.stubGlobal('fetch', makeFetch([]));
    renderWithProviders(
      <MacroFilterView project={{ name: 'test_project' }} onPeek={vi.fn()} />,
    );
    await waitFor(() =>
      expect(screen.getByText('No macros found.')).toBeInTheDocument(),
    );
  });

  it('renders package facet options and filters on selection', async () => {
    const fetchSpy = makeFetch([], {
      packages: [{ value: 'jaffle_shop', count: 5 }],
    });
    vi.stubGlobal('fetch', fetchSpy);
    renderWithProviders(
      <MacroFilterView project={{ name: 'test_project' }} onPeek={vi.fn()} />,
    );
    await waitFor(() =>
      expect(
        screen.getByRole('option', { name: 'jaffle_shop (5)' }),
      ).toBeInTheDocument(),
    );

    fireEvent.change(screen.getByTestId('filter-Package'), {
      target: { value: 'jaffle_shop' },
    });

    await waitFor(() =>
      expect(
        fetchSpy.mock.calls.some((c) => String(c[0]).includes('package=jaffle_shop')),
      ).toBe(true),
    );
  });
});
