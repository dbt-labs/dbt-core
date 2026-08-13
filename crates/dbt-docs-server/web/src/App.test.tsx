import { HelmetProvider } from 'react-helmet-async';
import { MemoryRouter } from 'react-router-dom';
import { QueryClientProvider } from '@tanstack/react-query';
import { render, screen, waitFor } from '@testing-library/react';

import App from './App';
import { BootstrapProvider } from './lib/bootstrapContext';
import { LinkPrefixProvider, MetadataDataProvider } from './shared';
import type { BootstrapData } from './shared/data-sources/duckdb/bootstrap';
import { DETAIL_REGISTRY } from './shared/data-sources/duckdb/details';
import {
  fromCapabilities,
  fromDistribution,
} from './shared/data-sources/mappers/fromWire';
import type { MetadataDataSource } from './shared/data-sources/MetadataDataSource';
import { createFakeDataSource } from './shared/testing/createFakeDataSource';
import { makeTestQueryClient } from './test/renderWithProviders';
import { pageFromWire } from './test/wireFixtures';

/**
 * The shell's data source, built from the same fixtures the fetch stub described.
 *
 * This test used to stub `fetch` and let the REST source read it. The fixtures are
 * still the right description of the shell's data, so they feed a fake source instead.
 */
function shellSource(overrides: Partial<MetadataDataSource> = {}): MetadataDataSource {
  return createFakeDataSource(
    {
      fetchProject: async () => ({
        name: 'demo_project',
        description: 'A demo project description.',
        dbtVersion: null,
        adapterType: null,
        git: null,
      }),
      fetchCapabilities: async () => fromCapabilities({ has_column_lineage: false }),
      fetchDistribution: async () =>
        fromDistribution({ name: 'oss', version: '0.0.0', is_logged_in: false }),
      fetchFiles: async () => [],
      fetchAssetCounts: async () => ({ model: 1 }),
      // The home page's Marts strip.
      fetchAssetList: async () =>
        pageFromWire('model', {
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
          page_info: { total_count: 1, has_next_page: false, end_cursor: null },
        }),
      ...overrides,
    } as never,
    { full: true },
  );
}

const testDataSource = shellSource();

/**
 * The first-paint read `main.tsx` starts before mounting.
 *
 * The shell renders nothing until this settles — it is where the node list, and so the
 * sidebar, file tree, and resource-type resolver, come from.
 */
function bootstrapRead(nodes: BootstrapData['nodes'] = []): Promise<BootstrapData> {
  return Promise.resolve({
    nodes,
    project: {
      name: 'demo_project',
      description: 'A demo project description.',
      dbtVersion: null,
      adapterType: null,
    },
    generation: null,
  } as unknown as BootstrapData);
}

function renderApp(
  source: MetadataDataSource = testDataSource,
  bootstrap: Promise<BootstrapData> = bootstrapRead(),
) {
  return render(
    <QueryClientProvider client={makeTestQueryClient()}>
      <HelmetProvider>
        <MemoryRouter initialEntries={['/']}>
          <BootstrapProvider value={bootstrap}>
            <MetadataDataProvider source={source}>
              <App />
            </MetadataDataProvider>
          </BootstrapProvider>
        </MemoryRouter>
      </HelmetProvider>
    </QueryClientProvider>,
  );
}

describe('<App />', () => {
  it('renders topbar and loading state', () => {
    // A read that never settles: the shell must show its chrome and a loading state
    // rather than an empty project.
    renderApp(testDataSource, new Promise<BootstrapData>(() => {}));

    expect(screen.getByRole('searchbox')).toBeVisible();
    expect(screen.getByText('Loading…')).toBeVisible();
  });

  describe('home page', () => {
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
      renderApp(
        shellSource({
          fetchProject: async () => ({
            name: 'demo_project',
            // Whitespace-only: the section should treat it as absent.
            description: '   ',
            dbtVersion: null,
            adapterType: null,
            git: null,
          }),
        } as never),
      );
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
      renderApp(
        shellSource({
          fetchAssetList: async () => ({ items: [], nextCursor: null, totalCount: 0 }),
        } as never),
      );
      await waitFor(() => {
        expect(screen.getByText('demo_project', { selector: 'h1' })).toBeVisible();
      });
      expect(screen.queryByText('Marts')).toBeNull();
    });
  });

  describe('detail panel', () => {
    it('renders model detail when navigating to a detail route', async () => {
      // The node index carries the resource type, which is what routes the detail
      // panel to the model view; the detail body itself comes from the source.
      const detailSource = shellSource({
        fetchAsset: async () =>
          DETAIL_REGISTRY.model!.map({
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
          }),
      } as never);

      render(
        <QueryClientProvider client={makeTestQueryClient()}>
          <HelmetProvider>
            <MemoryRouter initialEntries={['/details/model.demo.dim_customers']}>
              <LinkPrefixProvider prefix="/">
                <BootstrapProvider
                  value={bootstrapRead([
                    {
                      unique_id: 'model.demo.dim_customers',
                      name: 'dim_customers',
                      resource_type: 'model',
                      package_name: 'demo',
                    },
                  ])}
                >
                  <MetadataDataProvider source={detailSource}>
                    <App />
                  </MetadataDataProvider>
                </BootstrapProvider>
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
