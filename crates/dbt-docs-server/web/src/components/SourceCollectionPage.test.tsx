import { Route, Routes } from 'react-router-dom';
import { screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { NodeSummary } from '../api';
import { renderWithProviders } from '../test/renderWithProviders';
import { SourceCollectionPage } from './SourceCollectionPage';

const SOURCE_NODE: NodeSummary = {
  unique_id: 'source.proj.my_source.tbl1',
  name: 'tbl1',
  resource_type: 'source',
  package_name: 'proj',
  database_name: 'db',
  schema_name: 'sch',
};

/** Shape of `GET /api/v1/sources/:id` consumed by the REST adapter's
 *  `fromSourceDetail` — freshness lives in a nested object. */
const SOURCE_DETAIL = {
  unique_id: SOURCE_NODE.unique_id,
  name: 'tbl1',
  resource_type: 'source',
  package_name: 'proj',
  database_name: 'db',
  schema_name: 'sch',
  identifier: 'tbl1',
  loader: 'fivetran',
  meta: null,
  tags: [],
  fqn: [],
  referenced_by: [],
  columns: [],
  freshness: { status: 'pass', max_loaded_at: '2026-06-01T12:00:00Z' },
  catalog: null,
};

describe('<SourceCollectionPage />', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('renders freshness from the per-source detail endpoint', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn((url: string) => {
        if (url.includes('/api/v1/sources/')) {
          return Promise.resolve({
            ok: true,
            json: () => Promise.resolve(SOURCE_DETAIL),
          });
        }
        return Promise.resolve({ ok: true, json: () => Promise.resolve({}) });
      }),
    );

    renderWithProviders(
      <Routes>
        <Route
          path="/sources/:sourceName"
          element={<SourceCollectionPage nodes={[SOURCE_NODE]} onSelect={() => {}} />}
        />
      </Routes>,
      { initialEntries: ['/sources/my_source'] },
    );

    // The header's "last loaded at" is derived from the per-source detail's
    // `freshnessMaxLoadedAt`. Its presence proves the freshness data now flows
    // from the adapter's `fetchAsset` (the table cell itself is virtualized and
    // doesn't render in jsdom). Match the year to stay timezone-agnostic.
    expect(await screen.findByText(/2026/)).toBeInTheDocument();
  });
});
