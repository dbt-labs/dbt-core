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
  resourceIconMap: new Proxy({}, { get: () => 'test' }),
}));
vi.mock('@dbt-labs/sourdough', async (importOriginal) => {
  const mod = await importOriginal<typeof import('@dbt-labs/sourdough')>();
  return { ...mod, Icon: () => null };
});

const TESTS_RESPONSE = {
  data: [
    {
      unique_id: 'test.pkg.not_null_id',
      name: 'not_null_id',
      resource_type: 'test',
      package_name: 'pkg',
      tested_node_unique_id: 'model.pkg.customers',
      tested_column: 'id',
      execution_info: { status: 'pass' },
    },
  ],
  page_info: { total_count: 1, has_next_page: false, end_cursor: null },
};

interface TestFacetsBody {
  results: { value: string; count: number | null }[];
  test_types: { value: string; count: number | null }[];
}

const EMPTY_FACETS: TestFacetsBody = { results: [], test_types: [] };

/** Branches on `/facets` so the list and facet endpoints get distinct bodies. */
function makeFetch(listData: unknown, facets: TestFacetsBody = EMPTY_FACETS) {
  return vi.fn((url: string) => {
    const body = String(url).includes('facets') ? facets : listData;
    return Promise.resolve({ ok: true, json: () => Promise.resolve(body) });
  });
}

const EMPTY_LIST = {
  data: [],
  page_info: { total_count: 0, has_next_page: false, end_cursor: null },
};

import { TestFilterView } from './TestFilterView';

describe('<TestFilterView />', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('loads tests and shows total count', async () => {
    vi.stubGlobal('fetch', makeFetch(TESTS_RESPONSE));
    renderWithProviders(
      <TestFilterView project={{ name: 'test_project' }} onPeek={vi.fn()} />,
    );
    await waitFor(() => expect(screen.getByText('Loaded 1 of 1')).toBeInTheDocument());
    expect(screen.getByText('Tests')).toBeInTheDocument();
  });

  it('shows empty state when no tests', async () => {
    vi.stubGlobal('fetch', makeFetch(EMPTY_LIST));
    renderWithProviders(
      <TestFilterView project={{ name: 'test_project' }} onPeek={vi.fn()} />,
    );
    await waitFor(() =>
      expect(screen.getByText('No tests found.')).toBeInTheDocument(),
    );
  });

  it('renders capitalized result/test-type facet options', async () => {
    vi.stubGlobal(
      'fetch',
      makeFetch(EMPTY_LIST, {
        results: [{ value: 'pass', count: 10 }],
        test_types: [{ value: 'data', count: 7 }],
      }),
    );
    renderWithProviders(
      <TestFilterView project={{ name: 'test_project' }} onPeek={vi.fn()} />,
    );
    await waitFor(() =>
      expect(screen.getByRole('option', { name: 'Pass (10)' })).toBeInTheDocument(),
    );
    expect(screen.getByRole('option', { name: 'Data (7)' })).toBeInTheDocument();
  });

  it('re-issues the list with result= when a result is selected', async () => {
    const fetchSpy = makeFetch(EMPTY_LIST, {
      results: [{ value: 'pass', count: 10 }],
      test_types: [],
    });
    vi.stubGlobal('fetch', fetchSpy);
    renderWithProviders(
      <TestFilterView project={{ name: 'test_project' }} onPeek={vi.fn()} />,
    );
    await waitFor(() =>
      expect(screen.getByRole('option', { name: 'Pass (10)' })).toBeInTheDocument(),
    );

    fireEvent.change(screen.getByTestId('filter-Test result'), {
      target: { value: 'pass' },
    });

    await waitFor(() =>
      expect(
        fetchSpy.mock.calls.some((c) => String(c[0]).includes('result=pass')),
      ).toBe(true),
    );
  });
});
