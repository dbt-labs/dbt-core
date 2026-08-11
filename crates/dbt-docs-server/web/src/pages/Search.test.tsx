import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  type MockInstance,
  vi,
} from 'vitest';

import { api } from '../api';
import type { AssetFilters } from '../App';
import { initTelemetry, resetTelemetryForTests } from '../lib/telemetry';
import type { Project } from '../shared';
import { renderWithProviders } from '../test/renderWithProviders';
import Search from './Search';

// Drive the page off a controllable `useSearch` return and stub the heavy
// results list — this test only cares about the `search_performed` effect.
const searchReturn = {
  data: [] as unknown[],
  total: 0 as number | undefined,
  isPending: false,
  isFetchingNextPage: false,
  hasNextPage: false,
  error: null as unknown,
  errorCode: undefined as string | undefined,
  errorMessage: undefined as string | undefined,
  fetchNextPage: vi.fn(),
};

vi.mock('../shared', async (importOriginal) => {
  const mod = await importOriginal<typeof import('../shared')>();
  return {
    ...mod,
    useSearch: () => searchReturn,
    SearchResultsList: () => null,
  };
});

const EMPTY_FILTERS: AssetFilters = {
  resourceType: [],
  modelingLayer: [],
  materialization: [],
  pkg: [],
  tag: [],
};

const PROJECT = { name: 'demo' } as unknown as Project;

function renderSearch(query: string) {
  return renderWithProviders(
    <Search
      project={PROJECT}
      nodes={[]}
      query={query}
      filters={EMPTY_FILTERS}
      onUpdateFiltersInPlace={vi.fn()}
      previewId={null}
      onPeek={vi.fn()}
    />,
  );
}

describe('Search — search_performed analytics', () => {
  let analytics: MockInstance<typeof api.analytics>;

  beforeEach(() => {
    vi.useFakeTimers();
    analytics = vi
      .spyOn(api, 'analytics')
      .mockResolvedValue({ accepted: 1, skipped: 0 });
    searchReturn.total = 0;
    searchReturn.isPending = false;
  });

  afterEach(() => {
    resetTelemetryForTests();
    vi.runOnlyPendingTimers();
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('fires search_performed once with the trimmed query + result count', () => {
    initTelemetry({ is_logged_in: true, analytics_enabled: true });
    searchReturn.total = 7;
    renderSearch('  orders  ');
    vi.runAllTimers();

    expect(analytics).toHaveBeenCalledTimes(1);
    const event = (
      analytics.mock.calls[0][0] as unknown as { events: Record<string, unknown>[] }
    ).events[0];
    expect(event).toMatchObject({
      event_type: 'search_performed',
      search_query: 'orders',
      result_count: 7,
    });
  });

  it('does not fire for an empty query', () => {
    initTelemetry({ is_logged_in: true, analytics_enabled: true });
    renderSearch('   ');
    vi.runAllTimers();

    expect(analytics).not.toHaveBeenCalled();
  });

  it('does not fire while the query is still pending', () => {
    initTelemetry({ is_logged_in: true, analytics_enabled: true });
    searchReturn.isPending = true;
    renderSearch('orders');
    vi.runAllTimers();

    expect(analytics).not.toHaveBeenCalled();
  });

  it('does not fire when telemetry is not initialised', () => {
    searchReturn.total = 3;
    renderSearch('orders');
    vi.runAllTimers();

    expect(analytics).not.toHaveBeenCalled();
  });
});
