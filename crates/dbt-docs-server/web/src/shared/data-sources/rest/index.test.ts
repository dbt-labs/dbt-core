import { afterEach, describe, expect, test, vi } from 'vitest';

import { createRestDataSource, REGISTRY } from './index';

/** `[resourceType, segment]` rows derived from the registry — the source of
 *  truth for REST routing, so these tables can't drift from the real dispatch. */
const REGISTRY_ROWS = Object.entries(REGISTRY).map(
  ([rt, entry]) => [rt, entry!.segment] as const,
);
const FACET_ROWS = Object.entries(REGISTRY)
  .filter(([, entry]) => entry!.facets)
  .map(([rt, entry]) => [rt, entry!.segment] as const);

function mockFetch(handler: (url: string) => Response | Promise<Response>) {
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL) => {
      const url = typeof input === 'string' ? input : input.toString();
      return Promise.resolve(handler(url));
    }),
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status });

describe('createRestDataSource.fetchColumnLineage', () => {
  test('412 → gated', async () => {
    mockFetch(() => new Response(null, { status: 412 }));
    const src = createRestDataSource();
    await expect(
      src.fetchColumnLineage!({ uniqueId: 'model.shop.customers' }),
    ).resolves.toEqual({ kind: 'gated' });
  });

  test('404 → ok with empty graph', async () => {
    mockFetch(() => new Response(null, { status: 404 }));
    const src = createRestDataSource();
    await expect(
      src.fetchColumnLineage!({ uniqueId: 'model.shop.customers' }),
    ).resolves.toEqual({ kind: 'ok', graph: { nodes: [], edges: [] } });
  });

  test('200 → ok with mapped graph', async () => {
    mockFetch(() =>
      json({
        root: 'model.shop.customers',
        edges: [
          {
            from_node: 'source.shop.raw.orders',
            from_column: 'id',
            to_node: 'model.shop.customers',
            to_column: 'customer_id',
            kind: 'rename',
          },
        ],
      }),
    );
    const src = createRestDataSource();
    const res = await src.fetchColumnLineage!({ uniqueId: 'model.shop.customers' });
    expect(res.kind).toBe('ok');
    if (res.kind === 'ok') {
      expect(res.graph.nodes).toHaveLength(1);
      expect(res.graph.nodes[0].parentColumns).toEqual(['source.shop.raw.orders:id']);
      expect(res.graph.edges).toEqual([
        {
          fromNodeUniqueId: 'source.shop.raw.orders',
          fromColumn: 'id',
          toNodeUniqueId: 'model.shop.customers',
          toColumn: 'customer_id',
          transformationType: 'rename',
        },
      ]);
    }
  });
});

describe('createRestDataSource.fetchLineage', () => {
  test('appends max_depth when depth provided', async () => {
    const fetchSpy = vi.fn((_url: string) =>
      Promise.resolve(json({ root: 'x', max_depth: 3, nodes: [], edges: [] })),
    );
    vi.stubGlobal('fetch', fetchSpy);
    const src = createRestDataSource();
    await src.fetchLineage!({
      uniqueId: 'model.shop.x',
      resourceType: 'model',
      depth: 3,
    });
    expect(fetchSpy.mock.calls[0]?.[0]).toContain('?max_depth=3');
  });

  test('omits query string when depth absent', async () => {
    const fetchSpy = vi.fn((_url: string) =>
      Promise.resolve(json({ root: 'x', max_depth: 0, nodes: [], edges: [] })),
    );
    vi.stubGlobal('fetch', fetchSpy);
    const src = createRestDataSource();
    await src.fetchLineage!({ uniqueId: 'model.shop.x', resourceType: 'model' });
    expect(fetchSpy.mock.calls[0]?.[0]).not.toContain('max_depth');
  });
});

describe('createRestDataSource.fetchAsset', () => {
  // Pins resourceType → /api/v1/<segment>/<id> dispatch routing, derived from
  // the registry. The two trailing rows are registry misses (analysis/function)
  // that must fall back to /api/v1/nodes/<id> — guarding the regression class
  // where a registered type silently 404s through the node endpoint (the
  // original saved_query foot-gun).
  test.each([
    ...REGISTRY_ROWS.map(([rt, segment]) => [rt, `/api/v1/${segment}/`] as const),
    ['analysis', '/api/v1/nodes/'] as const,
    ['function', '/api/v1/nodes/'] as const,
  ])('%s → %s<id>', async (resourceType, expectedPath) => {
    const fetchSpy = vi.fn((_url: string) =>
      Promise.resolve(
        // depends_on/dimensions/measures/entities keep the exposure +
        // semantic_model mappers (which read these non-optionally) from throwing.
        json({
          unique_id: 'x.shop.y',
          name: 'y',
          resource_type: resourceType,
          depends_on: [],
          dimensions: [],
          measures: [],
          entities: [],
        }),
      ),
    );
    vi.stubGlobal('fetch', fetchSpy);
    const src = createRestDataSource();
    await src.fetchAsset({ uniqueId: 'x.shop.y', resourceType: resourceType as never });
    expect(fetchSpy.mock.calls[0]?.[0]).toBe(`${expectedPath}x.shop.y`);
  });

  test('URL-encodes the uniqueId via enc()', async () => {
    const fetchSpy = vi.fn((_url: string) =>
      Promise.resolve(
        json({ unique_id: 'model.a b/c', name: 'y', resource_type: 'model' }),
      ),
    );
    vi.stubGlobal('fetch', fetchSpy);
    const src = createRestDataSource();
    await src.fetchAsset({ uniqueId: 'model.a b/c', resourceType: 'model' as never });
    expect(fetchSpy.mock.calls[0]?.[0]).toBe('/api/v1/models/model.a%20b%2Fc');
  });
});

const emptyList = () =>
  json({
    data: [],
    page_info: { total_count: 0, end_cursor: null, has_next_page: false },
  });

describe('createRestDataSource.fetchAssetList', () => {
  // Pins resourceType → /api/v1/<segment> list dispatch routing, derived from
  // the registry — mirrors the fetchAsset routing guard.
  test.each(REGISTRY_ROWS.map(([rt, segment]) => [rt, `/api/v1/${segment}`] as const))(
    '%s → %s',
    async (resourceType, expectedPath) => {
      const fetchSpy = vi.fn((_url: string) => Promise.resolve(emptyList()));
      vi.stubGlobal('fetch', fetchSpy);
      const src = createRestDataSource();
      await src.fetchAssetList!({ filter: { resourceTypes: [resourceType as never] } });
      expect(fetchSpy.mock.calls[0]?.[0]).toBe(expectedPath);
    },
  );

  test('encodes cursor → after, limit → first, sort → field:dir', async () => {
    const fetchSpy = vi.fn((_url: string) => Promise.resolve(emptyList()));
    vi.stubGlobal('fetch', fetchSpy);
    const src = createRestDataSource();
    await src.fetchAssetList!({
      filter: { resourceTypes: ['model'] },
      cursor: 'abc',
      limit: 25,
      sort: { field: 'executed_at', desc: true },
    });
    const url = fetchSpy.mock.calls[0]?.[0] as string;
    expect(url).toContain('after=abc');
    expect(url).toContain('first=25');
    expect(url).toContain('sort=executed_at%3Adesc');
  });

  test('encodes modelingLayers → modeling_layer (model-scoped)', async () => {
    const fetchSpy = vi.fn((_url: string) => Promise.resolve(emptyList()));
    vi.stubGlobal('fetch', fetchSpy);
    const src = createRestDataSource();
    await src.fetchAssetList!({
      filter: { resourceTypes: ['model'], modelingLayers: ['Marts'] },
    });
    const url = fetchSpy.mock.calls[0]?.[0] as string;
    expect(url).toContain('modeling_layer=Marts');
  });

  test('encodes owners/packages/results/testTypes → owner/package/result/test_type', async () => {
    const fetchSpy = vi.fn((_url: string) => Promise.resolve(emptyList()));
    vi.stubGlobal('fetch', fetchSpy);
    const src = createRestDataSource();
    await src.fetchAssetList!({
      filter: {
        resourceTypes: ['model'],
        owners: ['alice'],
        packages: ['jaffle_shop'],
        results: ['pass'],
        testTypes: ['data'],
      },
    });
    const url = fetchSpy.mock.calls[0]?.[0] as string;
    expect(url).toContain('owner=alice');
    expect(url).toContain('package=jaffle_shop');
    expect(url).toContain('result=pass');
    expect(url).toContain('test_type=data');
  });

  test('maps page_info → Page (nextCursor only when has_next_page)', async () => {
    mockFetch(() =>
      json({
        data: [{ unique_id: 'model.shop.x', name: 'x', package_name: 'shop' }],
        page_info: { total_count: 7, end_cursor: 'cur', has_next_page: true },
      }),
    );
    const src = createRestDataSource();
    const page = await src.fetchAssetList!({ filter: { resourceTypes: ['model'] } });
    expect(page.items).toHaveLength(1);
    expect(page.items[0]).toMatchObject({
      uniqueId: 'model.shop.x',
      resourceType: 'model',
    });
    expect(page.nextCursor).toBe('cur');
    expect(page.totalCount).toBe(7);
  });

  test('nextCursor is null on the last page', async () => {
    mockFetch(() =>
      json({
        data: [],
        page_info: { total_count: 0, end_cursor: 'ignored', has_next_page: false },
      }),
    );
    const src = createRestDataSource();
    const page = await src.fetchAssetList!({ filter: { resourceTypes: ['model'] } });
    expect(page.nextCursor).toBeNull();
  });

  test('404 body → empty page', async () => {
    mockFetch(() => new Response(null, { status: 404 }));
    const src = createRestDataSource();
    const page = await src.fetchAssetList!({ filter: { resourceTypes: ['model'] } });
    expect(page).toEqual({ items: [], nextCursor: null, totalCount: null });
  });

  test('throws on zero resourceTypes', async () => {
    const src = createRestDataSource();
    await expect(
      src.fetchAssetList!({ filter: { resourceTypes: [] } }),
    ).rejects.toThrow(/exactly one resourceType/);
  });

  test('throws on multiple resourceTypes', async () => {
    const src = createRestDataSource();
    await expect(
      src.fetchAssetList!({ filter: { resourceTypes: ['model', 'source'] } }),
    ).rejects.toThrow(/exactly one resourceType/);
  });
});

describe('createRestDataSource.fetchFacets', () => {
  // Every registry type carrying a facets mapper hits /api/v1/<segment>/facets.
  test.each(FACET_ROWS)('%s → /%s/facets', async (resourceType, segment) => {
    // 404 → the per-type empty payload feeds the mapper; we only assert the URL.
    const fetchSpy = vi.fn((_url: string) =>
      Promise.resolve(new Response(null, { status: 404 })),
    );
    vi.stubGlobal('fetch', fetchSpy);
    const src = createRestDataSource();
    await src.fetchFacets!({ resourceType: resourceType as never });
    expect(fetchSpy.mock.calls[0]?.[0]).toBe(`/api/v1/${segment}/facets`);
  });

  test('model → /models/facets, mapped to filter-field keys', async () => {
    const fetchSpy = vi.fn((_url: string) =>
      Promise.resolve(
        json({
          modeling_layers: [{ value: 'Marts', count: 3 }],
          owners: [{ value: 'alice', count: 2 }],
          packages: [{ value: 'jaffle_shop', count: 5 }],
        }),
      ),
    );
    vi.stubGlobal('fetch', fetchSpy);
    const src = createRestDataSource();
    const facets = await src.fetchFacets!({ resourceType: 'model' });
    expect(fetchSpy.mock.calls[0]?.[0]).toBe('/api/v1/models/facets');
    expect(facets).toEqual({
      modelingLayers: [{ value: 'Marts', count: 3 }],
      owners: [{ value: 'alice', count: 2 }],
      packages: [{ value: 'jaffle_shop', count: 5 }],
    });
  });

  test('test → /tests/facets, mapped to results/testTypes', async () => {
    const fetchSpy = vi.fn((_url: string) =>
      Promise.resolve(
        json({
          results: [{ value: 'pass', count: 10 }],
          test_types: [{ value: 'data', count: 7 }],
        }),
      ),
    );
    vi.stubGlobal('fetch', fetchSpy);
    const src = createRestDataSource();
    const facets = await src.fetchFacets!({ resourceType: 'test' });
    expect(fetchSpy.mock.calls[0]?.[0]).toBe('/api/v1/tests/facets');
    expect(facets).toEqual({
      results: [{ value: 'pass', count: 10 }],
      testTypes: [{ value: 'data', count: 7 }],
    });
  });

  test('macro → /macros/facets, mapped to packages', async () => {
    const fetchSpy = vi.fn((_url: string) =>
      Promise.resolve(json({ packages: [{ value: 'jaffle_shop', count: 4 }] })),
    );
    vi.stubGlobal('fetch', fetchSpy);
    const src = createRestDataSource();
    const facets = await src.fetchFacets!({ resourceType: 'macro' });
    expect(fetchSpy.mock.calls[0]?.[0]).toBe('/api/v1/macros/facets');
    expect(facets).toEqual({ packages: [{ value: 'jaffle_shop', count: 4 }] });
  });

  test('unsupported type → {} with no fetch', async () => {
    const fetchSpy = vi.fn((_url: string) => Promise.resolve(emptyList()));
    vi.stubGlobal('fetch', fetchSpy);
    const src = createRestDataSource();
    await expect(src.fetchFacets!({ resourceType: 'source' })).resolves.toEqual({});
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  test('count defaults to null when absent', async () => {
    mockFetch(() => json({ packages: [{ value: 'jaffle_shop' }] }));
    const src = createRestDataSource();
    const facets = await src.fetchFacets!({ resourceType: 'macro' });
    expect(facets.packages).toEqual([{ value: 'jaffle_shop', count: null }]);
  });
});

describe('createRestDataSource.fetchAssetCounts', () => {
  test('maps the counts response, dropping unknown keys', async () => {
    mockFetch(() => json({ model: 5, test: 12, doc: 3 }));
    const src = createRestDataSource();
    await expect(src.fetchAssetCounts!()).resolves.toEqual({ model: 5, test: 12 });
  });

  test('404 → empty counts', async () => {
    mockFetch(() => new Response(null, { status: 404 }));
    const src = createRestDataSource();
    await expect(src.fetchAssetCounts!()).resolves.toEqual({});
  });
});

describe('createRestDataSource.fetchSearch', () => {
  test('maps filter args to query params and returns an ok page', async () => {
    let captured = '';
    mockFetch((url) => {
      captured = url;
      return json({
        data: [
          {
            matched_field: 'name',
            highlight: '<b>c</b>',
            hit: {
              unique_id: 'model.shop.customers',
              resource_type: 'model',
              name: 'customers',
              package_name: 'shop',
            },
          },
        ],
        page_info: {
          total_count: 1,
          start_cursor: null,
          end_cursor: null,
          has_next_page: false,
        },
      });
    });
    const src = createRestDataSource();
    const result = await src.fetchSearch!({
      filter: {
        q: 'cust',
        resourceTypes: ['model', 'source'],
        packages: ['shop'],
        tags: ['pii'],
        modelingLayers: ['Marts'],
        materializations: ['table'],
      },
      limit: 50,
      cursor: 'abc',
    });

    expect(result.kind).toBe('ok');
    if (result.kind === 'ok') {
      expect(result.page.items).toHaveLength(1);
      expect(result.page.items[0].uniqueId).toBe('model.shop.customers');
    }
    expect(captured).toContain('q=cust');
    expect(captured).toContain('type=model%2Csource');
    expect(captured).toContain('package=shop');
    expect(captured).toContain('tag=pii');
    expect(captured).toContain('modeling_layer=Marts');
    expect(captured).toContain('materialization=table');
    expect(captured).toContain('first=50');
    expect(captured).toContain('after=abc');
  });

  test('400 → structured error result (not thrown)', async () => {
    mockFetch(() => json({ code: 'query_too_long', message: 'too long' }, 400));
    const src = createRestDataSource();
    const result = await src.fetchSearch!({ filter: { q: 'x'.repeat(9999) } });
    expect(result).toEqual({
      kind: 'error',
      code: 'query_too_long',
      message: 'too long',
    });
  });

  test('non-400 failure throws', async () => {
    mockFetch(() => new Response(null, { status: 500, statusText: 'Server Error' }));
    const src = createRestDataSource();
    await expect(src.fetchSearch!({ filter: { q: 'x' } })).rejects.toThrow();
  });
});

describe('createRestDataSource.fetchSearchFacets', () => {
  test('maps the facets response', async () => {
    mockFetch(() =>
      json({
        accesses: [{ value: 'public', count: 1 }],
        modeling_layers: [{ value: 'Marts', count: 2 }],
        materialization_types: [{ value: 'table', count: 3 }],
        tags: [{ value: 'pii', count: 4 }],
        packages: [{ value: 'shop', count: 5 }],
      }),
    );
    const src = createRestDataSource();
    await expect(src.fetchSearchFacets!()).resolves.toEqual({
      accesses: [{ value: 'public', count: 1 }],
      modelingLayers: [{ value: 'Marts', count: 2 }],
      materializationTypes: [{ value: 'table', count: 3 }],
      tags: [{ value: 'pii', count: 4 }],
      packages: [{ value: 'shop', count: 5 }],
    });
  });

  test('404 → empty facet lists', async () => {
    mockFetch(() => new Response(null, { status: 404 }));
    const src = createRestDataSource();
    await expect(src.fetchSearchFacets!()).resolves.toEqual({
      accesses: [],
      modelingLayers: [],
      materializationTypes: [],
      tags: [],
      packages: [],
    });
  });
});

describe('createRestDataSource.fetchDistribution', () => {
  test('maps the distribution response', async () => {
    mockFetch(() => json({ name: 'dbt', version: '2.0.0', is_logged_in: true }));
    const src = createRestDataSource();
    await expect(src.fetchDistribution!()).resolves.toEqual({
      isFusion: true,
      isLoggedIn: true,
      version: '2.0.0',
    });
  });
});

describe('createRestDataSource.fetchProject', () => {
  test('hits /api/v1/project and maps to camelCase', async () => {
    let captured = '';
    mockFetch((url) => {
      captured = url;
      return json({
        name: 'jaffle_shop',
        adapter_type: 'duckdb',
        dbt_version: '1.8.0',
      });
    });
    const src = createRestDataSource();
    await expect(src.fetchProject!()).resolves.toMatchObject({
      name: 'jaffle_shop',
      adapterType: 'duckdb',
      dbtVersion: '1.8.0',
    });
    expect(captured).toContain('/api/v1/project');
  });

  test('404 → empty-name project', async () => {
    mockFetch(() => new Response(null, { status: 404 }));
    const src = createRestDataSource();
    await expect(src.fetchProject!()).resolves.toEqual({ name: '' });
  });
});

describe('createRestDataSource.fetchFiles', () => {
  test('hits /api/v1/files and maps each entry to camelCase', async () => {
    let captured = '';
    mockFetch((url) => {
      captured = url;
      return json({
        total: 1,
        files: [
          {
            unique_id: 'model.pkg.x',
            name: 'x',
            resource_type: 'model',
            package_name: 'pkg',
            original_file_path: 'models/x.sql',
            patch_path: null,
          },
        ],
      });
    });
    const src = createRestDataSource();
    await expect(src.fetchFiles!()).resolves.toEqual([
      {
        uniqueId: 'model.pkg.x',
        name: 'x',
        resourceType: 'model',
        packageName: 'pkg',
        originalFilePath: 'models/x.sql',
        patchPath: null,
      },
    ]);
    expect(captured).toContain('/api/v1/files');
  });

  test('404 → empty list', async () => {
    mockFetch(() => new Response(null, { status: 404 }));
    const src = createRestDataSource();
    await expect(src.fetchFiles!()).resolves.toEqual([]);
  });
});
