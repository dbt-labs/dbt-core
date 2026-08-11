import { HelmetProvider } from 'react-helmet-async';
import { MemoryRouter } from 'react-router-dom';
import { QueryClientProvider } from '@tanstack/react-query';
import { render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, vi } from 'vitest';

import App from './App';
import { LinkPrefixProvider, MetadataDataProvider } from './shared';
import { createRestDataSource } from './shared/data-sources/rest';
import { makeTestQueryClient } from './test/renderWithProviders';

const testDataSource = createRestDataSource();

type FetchOverrides = Record<string, unknown>;

function stubFetch(overrides: FetchOverrides = {}) {
  const defaults: FetchOverrides = {
    '/api/v1/project': {
      name: 'demo_project',
      description: 'A demo project description.',
    },
    '/api/v1/capabilities': { has_column_lineage: false },
    '/api/v1/distribution': { name: 'oss', version: '0.0.0', is_logged_in: false },
    '/api/v1/identity': { is_logged_in: false, analytics_enabled: false },
    '/api/v1/nodes': { nodes: [], total: 0, offset: 0, limit: 1000 },
    '/api/v1/files': { files: [] },
    '/api/v1/tables': [],
    // /api/v1/models?modeling_layer=Marts&first=12 — match by prefix below
    '/api/v1/models': {
      data: [
        {
          unique_id: 'model.demo.dim_customers',
          name: 'dim_customers',
          package_name: 'demo',
          original_file_path: 'models/marts/dim_customers.sql',
          modeling_layer: 'Marts',
          access_level: null,
          contract_enforced: null,
          owner: null,
          executed_at: null,
        },
      ],
      page_info: {
        total_count: 1,
        start_cursor: null,
        end_cursor: null,
        has_next_page: false,
      },
    },
    ...overrides,
  };

  vi.stubGlobal(
    'fetch',
    vi.fn((url: string) => {
      const path = url.split('?')[0];
      const body = defaults[path] ?? defaults[url];
      if (body === undefined) {
        return Promise.resolve(
          new Response(JSON.stringify({ error: 'not stubbed' }), { status: 404 }),
        );
      }
      return Promise.resolve(
        new Response(JSON.stringify(body), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        }),
      );
    }),
  );
}

function renderApp() {
  return render(
    <QueryClientProvider client={makeTestQueryClient()}>
      <HelmetProvider>
        <MemoryRouter initialEntries={['/']}>
          <MetadataDataProvider source={testDataSource}>
            <App />
          </MetadataDataProvider>
        </MemoryRouter>
      </HelmetProvider>
    </QueryClientProvider>,
  );
}

describe('<App />', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('renders topbar and loading state', () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => new Promise(() => {})),
    );

    renderApp();

    expect(screen.getByRole('searchbox')).toBeVisible();
    expect(screen.getByText('Loading…')).toBeVisible();
  });

  describe('home page', () => {
    beforeEach(() => stubFetch());

    it('does not render hero action buttons', async () => {
      renderApp();
      await waitFor(() => {
        expect(screen.getByText('demo_project', { selector: 'h1' })).toBeVisible();
      });
      expect(screen.queryByRole('button', { name: /set as home/i })).toBeNull();
      expect(screen.queryByRole('button', { name: /add to favorites/i })).toBeNull();
      expect(screen.queryByRole('button', { name: /in favorites/i })).toBeNull();
    });

    it('renders the "Get more from dbt" upgrade panel for Core users', async () => {
      // Test fixtures stub `has_column_lineage: false`, which maps to
      // `core` — per the gating doc, that surfaces the CLL + Mesh
      // upgrade panel on home.
      renderApp();
      await waitFor(() => {
        expect(screen.getByText('demo_project', { selector: 'h1' })).toBeVisible();
      });
      expect(screen.getByText(/Get more from dbt/i)).toBeVisible();
    });

    it('renders all asset types in the Explore grid, including zero-count types', async () => {
      renderApp();
      await waitFor(() => {
        expect(screen.getByText('Explore')).toBeVisible();
      });
      // Every canonical resource type renders, even when there are zero nodes.
      for (const label of [
        'Models',
        'Sources',
        'Tests',
        'Exposures',
        'Groups',
        'Metrics',
        'Semantic models',
        'Seeds',
        'Macros',
        'Snapshots',
        'Saved queries',
      ]) {
        expect(
          screen.getByRole('button', { name: new RegExp(`Browse ${label}`, 'i') }),
        ).toBeVisible();
      }
    });

    it('renders the project description when present', async () => {
      renderApp();
      await waitFor(() => {
        expect(screen.getByText('About this project')).toBeVisible();
      });
      expect(screen.getByText('A demo project description.')).toBeVisible();
    });

    it('hides the description section when description is empty', async () => {
      stubFetch({
        '/api/v1/project': { name: 'demo_project', description: '   ' },
      });
      renderApp();
      await waitFor(() => {
        expect(screen.getByText('demo_project', { selector: 'h1' })).toBeVisible();
      });
      expect(screen.queryByText('About this project')).toBeNull();
    });

    it('renders the marts section when marts exist', async () => {
      renderApp();
      await waitFor(() => {
        expect(screen.getByText('Marts')).toBeVisible();
      });
      expect(screen.getByText('dim_customers')).toBeVisible();
      expect(screen.getByRole('button', { name: /view all/i })).toBeVisible();
    });

    it('hides the marts section when no marts are returned', async () => {
      stubFetch({
        '/api/v1/models': {
          data: [],
          page_info: {
            total_count: 0,
            start_cursor: null,
            end_cursor: null,
            has_next_page: false,
          },
        },
      });
      renderApp();
      await waitFor(() => {
        expect(screen.getByText('demo_project', { selector: 'h1' })).toBeVisible();
      });
      expect(screen.queryByText('Marts')).toBeNull();
    });
  });

  describe('detail panel', () => {
    it('renders model detail when navigating to a detail route', async () => {
      stubFetch({
        '/api/v1/nodes': {
          nodes: [
            {
              unique_id: 'model.demo.dim_customers',
              name: 'dim_customers',
              resource_type: 'model',
              package_name: 'demo',
            },
          ],
          total: 1,
          offset: 0,
          limit: 1000,
        },
        '/api/v1/models/model.demo.dim_customers': {
          unique_id: 'model.demo.dim_customers',
          name: 'dim_customers',
          resource_type: 'model',
          package_name: 'demo',
          description: 'Customer dimension table',
          tags: [],
          fqn: ['demo', 'dim_customers'],
          columns: [],
          depends_on: [],
          referenced_by: [],
        },
      });

      render(
        <QueryClientProvider client={makeTestQueryClient()}>
          <HelmetProvider>
            <MemoryRouter initialEntries={['/details/model.demo.dim_customers']}>
              <LinkPrefixProvider prefix="/">
                <MetadataDataProvider source={testDataSource}>
                  <App />
                </MetadataDataProvider>
              </LinkPrefixProvider>
            </MemoryRouter>
          </HelmetProvider>
        </QueryClientProvider>,
      );

      await waitFor(() => {
        expect(screen.getByRole('heading', { name: 'dim_customers' })).toBeVisible();
      });
    });
  });
});
