import type {
  AssetArgs,
  AssetFilter,
  ColumnLineageArgs,
  FacetsArgs,
  LineageArgs,
  ListArgs,
} from '../../typings/args';
import type { Asset, AssetSummary, ResourceType } from '../../typings/domain/asset';
import type { Capabilities } from '../../typings/domain/capabilities';
import type { AssetCounts } from '../../typings/domain/counts';
import type { Distribution } from '../../typings/domain/distribution';
import type { Facets } from '../../typings/domain/facets';
import type { FileEntry } from '../../typings/domain/files';
import type { ColumnLineageResult, LineageGraph } from '../../typings/domain/lineage';
import type { Project } from '../../typings/domain/project';
import type {
  SearchFacets,
  SearchFilter,
  SearchResult,
} from '../../typings/domain/search';
import type { Page } from '../../typings/page';
import type { MetadataDataSource } from '../MetadataDataSource';
import {
  fromCapabilities,
  fromColumnLineageResponse,
  fromDistribution,
  fromExposureDetail,
  fromExposureSummary,
  fromFileList,
  fromGroupDetail,
  fromGroupSummary,
  fromLineageResponse,
  fromMacroDetail,
  fromMacroFacets,
  fromMacroSummary,
  fromMetricDetail,
  fromMetricSummary,
  fromModelDetail,
  fromModelFacets,
  fromModelSummary,
  fromNodeCounts,
  fromNodeDetail,
  fromProject,
  fromSavedQueryDetail,
  fromSavedQuerySummary,
  fromSearchFacets,
  fromSearchResponse,
  fromSeedDetail,
  fromSeedSummary,
  fromSemanticModelDetail,
  fromSemanticModelSummary,
  fromSnapshotDetail,
  fromSnapshotSummary,
  fromSourceDetail,
  fromSourceSummary,
  fromTestDetail,
  fromTestFacets,
  fromTestSummary,
  type RestCapabilities,
  type RestColumnLineageResponse,
  type RestDistribution,
  type RestFileListResponse,
  type RestLineageResponse,
  type RestListResponse,
  type RestMacroFacets,
  type RestModelFacets,
  type RestNodeCounts,
  type RestProject,
  type RestSearchFacets,
  type RestSearchResponse,
  type RestTestFacets,
} from './mappers/fromRest';

export interface RestDataSourceOptions {
  /** Base URL prefix prepended to every API call. Defaults to '' (same origin). */
  baseUrl?: string;
}

const enc = encodeURIComponent;

/** Fetch JSON from `url`. Returns `null` on 404; throws on other failures. */
async function getJson<T>(url: string): Promise<T | null> {
  const res = await fetch(url);
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(`${res.status} ${res.statusText} from ${url}`);
  return res.json() as Promise<T>;
}

/** Map a REST list envelope to a domain {@link Page}, applying `map` per row.
 *  Null body (404) → empty page. `nextCursor` is the `end_cursor` only while
 *  another page exists. */
function toPage<T>(
  r: RestListResponse<unknown> | null,
  map: (row: never) => T,
): Page<T> {
  if (!r) return { items: [], nextCursor: null, totalCount: null };
  return {
    items: r.data.map((row) => map(row as never)),
    nextCursor: r.page_info.has_next_page ? (r.page_info.end_cursor ?? null) : null,
    totalCount: r.page_info.total_count ?? null,
  };
}

/**
 * One row of the resource-type registry: the REST path segment for a type plus
 * the mappers that translate its detail / summary / facet payloads into the
 * domain shape. `fetchAsset`, `fetchAssetList`, and `fetchFacets` are generic
 * lookups over this table — segments stay REST-internal (never surface on
 * {@link MetadataDataSource}).
 */
interface RegistryEntry {
  /** Plural REST path segment, e.g. 'models', 'tests'. */
  segment: string;
  detail: (d: never) => Asset;
  summary: (d: never) => AssetSummary;
  /** Facet endpoint mapper + the empty payload used when the endpoint 404s.
   *  Present only for types with a `/facets` endpoint (model/test/macro). */
  facets?: { map: (d: never) => Facets; empty: unknown };
}

/**
 * Maps a {@link ResourceType} to its REST segment + mappers. Types absent here
 * have no per-type list/detail endpoint: `fetchAsset` falls back to
 * `/api/v1/nodes/<id>` (`fromNodeDetail`), `fetchAssetList` throws, and
 * `fetchFacets` resolves to `{}`. `test` and `unit_test` share the `tests`
 * endpoint and mappers.
 */
export const REGISTRY: Partial<Record<ResourceType, RegistryEntry>> = {
  model: {
    segment: 'models',
    detail: fromModelDetail,
    summary: fromModelSummary,
    facets: {
      map: fromModelFacets,
      empty: {
        modeling_layers: [],
        owners: [],
        packages: [],
      } satisfies RestModelFacets,
    },
  },
  seed: { segment: 'seeds', detail: fromSeedDetail, summary: fromSeedSummary },
  snapshot: {
    segment: 'snapshots',
    detail: fromSnapshotDetail,
    summary: fromSnapshotSummary,
  },
  source: { segment: 'sources', detail: fromSourceDetail, summary: fromSourceSummary },
  exposure: {
    segment: 'exposures',
    detail: fromExposureDetail,
    summary: fromExposureSummary,
  },
  metric: { segment: 'metrics', detail: fromMetricDetail, summary: fromMetricSummary },
  macro: {
    segment: 'macros',
    detail: fromMacroDetail,
    summary: fromMacroSummary,
    facets: { map: fromMacroFacets, empty: { packages: [] } satisfies RestMacroFacets },
  },
  semantic_model: {
    segment: 'semantic_models',
    detail: fromSemanticModelDetail,
    summary: fromSemanticModelSummary,
  },
  test: {
    segment: 'tests',
    detail: fromTestDetail,
    summary: fromTestSummary,
    facets: {
      map: fromTestFacets,
      empty: { results: [], test_types: [] } satisfies RestTestFacets,
    },
  },
  unit_test: {
    segment: 'tests',
    detail: fromTestDetail,
    summary: fromTestSummary,
    facets: {
      map: fromTestFacets,
      empty: { results: [], test_types: [] } satisfies RestTestFacets,
    },
  },
  saved_query: {
    segment: 'saved_queries',
    detail: fromSavedQueryDetail,
    summary: fromSavedQuerySummary,
  },
  group: { segment: 'groups', detail: fromGroupDetail, summary: fromGroupSummary },
};

/** Resource types the registry covers. Exported solely so the test fake's
 *  per-type builders derive from the same source of truth (test-only coupling,
 *  not part of the {@link MetadataDataSource} contract). */
export const REGISTRY_RESOURCE_TYPES = Object.keys(REGISTRY) as ResourceType[];

/**
 * Create a {@link MetadataDataSource} backed by the dbt-docs-server REST API.
 *
 * Implements `fetchAsset`, `fetchAssetList`, `fetchFacets`,
 * `fetchCapabilities`, `fetchLineage`, and `fetchColumnLineage`. The per-type
 * list endpoints honor `sort` plus type-specific facet params; `resourceTypes`
 * selects the endpoint and the facet fields (`modelingLayers`, `owners`,
 * `packages`, `results`, `testTypes`) map to the matching query params, so
 * `supportedFilters` advertises those — `search`/`tags` are not honored.
 * `fetchFacets` backs the list filter dropdowns.
 */
export function createRestDataSource(
  opts: RestDataSourceOptions = {},
): MetadataDataSource {
  const base = opts.baseUrl ?? '';

  async function fetchAsset(args: AssetArgs): Promise<Asset | null> {
    const { uniqueId, resourceType } = args;
    const id = enc(uniqueId);

    // Registry miss → the generic node endpoint (analysis/function/operation).
    const entry = REGISTRY[resourceType];
    if (!entry) {
      const d = await getJson(`${base}/api/v1/nodes/${id}`);
      return d ? fromNodeDetail(d as never) : null;
    }
    const d = await getJson(`${base}/api/v1/${entry.segment}/${id}`);
    return d ? entry.detail(d as never) : null;
  }

  /**
   * List assets of a single resource type, cursor-paginated. The REST list
   * endpoints are per-type, so `args.filter.resourceTypes` must hold exactly
   * one type — zero or more than one throws. `unit_test` and `test` both route
   * to `/api/v1/tests`.
   */
  async function fetchAssetList(
    args: ListArgs<AssetFilter>,
  ): Promise<Page<AssetSummary>> {
    const types = args.filter?.resourceTypes ?? [];
    if (types.length !== 1) {
      throw new Error(
        `fetchAssetList requires exactly one resourceType in filter.resourceTypes; received ${types.length}`,
      );
    }
    const resourceType = types[0];

    const params = new URLSearchParams();
    if (args.cursor) params.set('after', args.cursor);
    if (args.limit != null) params.set('first', String(args.limit));
    if (args.sort) {
      params.set('sort', `${args.sort.field}:${args.sort.desc ? 'desc' : 'asc'}`);
    }
    // Model-scoped (dbt-docs-server LAYER_CONDITIONS); comma-separated, honored
    // only by the `model` endpoint — harmless elsewhere.
    if (args.filter?.modelingLayers?.length) {
      params.set('modeling_layer', args.filter.modelingLayers.join(','));
    }
    // Type-scoped facet filters; comma-separated, each honored only by the
    // matching list endpoint — harmless on endpoints that ignore them.
    if (args.filter?.owners?.length) {
      params.set('owner', args.filter.owners.join(','));
    }
    if (args.filter?.packages?.length) {
      params.set('package', args.filter.packages.join(','));
    }
    if (args.filter?.results?.length) {
      params.set('result', args.filter.results.join(','));
    }
    if (args.filter?.testTypes?.length) {
      params.set('test_type', args.filter.testTypes.join(','));
    }
    const qs = params.toString();
    const suffix = qs ? `?${qs}` : '';

    async function list(resource: string): Promise<RestListResponse<unknown> | null> {
      return getJson<RestListResponse<unknown>>(`${base}/api/v1/${resource}${suffix}`);
    }

    const entry = REGISTRY[resourceType];
    if (!entry) {
      throw new Error(`fetchAssetList does not support resourceType "${resourceType}"`);
    }
    return toPage(await list(entry.segment), entry.summary);
  }

  /**
   * Facet options for a resource type's list filter dropdowns. Per-type
   * dispatch to the matching `/facets` endpoint; types without a facet endpoint
   * resolve to `{}` (no dropdowns). Keys mirror the {@link AssetFilter} field
   * each facet drives.
   */
  async function fetchFacets(args: FacetsArgs): Promise<Facets> {
    const entry = REGISTRY[args.resourceType];
    if (!entry?.facets) return {};
    const d = await getJson<unknown>(`${base}/api/v1/${entry.segment}/facets`);
    return entry.facets.map((d ?? entry.facets.empty) as never);
  }

  async function fetchCapabilities(): Promise<Capabilities> {
    const d = await getJson<RestCapabilities>(`${base}/api/v1/capabilities`);
    return fromCapabilities(d ?? { has_column_lineage: false });
  }

  async function fetchDistribution(): Promise<Distribution> {
    const d = await getJson<RestDistribution>(`${base}/api/v1/distribution`);
    return fromDistribution(d ?? { name: 'oss', is_logged_in: false });
  }

  /** Project-wide per-resource-type counts. No-arg aggregate over every
   *  resource table (not REGISTRY-routed); unknown keys are dropped by the
   *  mapper. */
  async function fetchAssetCounts(): Promise<AssetCounts> {
    const d = await getJson<RestNodeCounts>(`${base}/api/v1/nodes/counts`);
    return fromNodeCounts(d ?? {});
  }

  /** Project identity + git state. No-arg aggregate, like
   *  {@link fetchDistribution}. */
  async function fetchProject(): Promise<Project> {
    const d = await getJson<RestProject>(`${base}/api/v1/project`);
    return fromProject(d ?? { name: '' });
  }

  /** Flat list of every file-bearing resource. No-arg; null body (404) → empty
   *  list. */
  async function fetchFiles(): Promise<FileEntry[]> {
    const d = await getJson<RestFileListResponse>(`${base}/api/v1/files`);
    return fromFileList(d);
  }

  /**
   * Cross-type cursor search. Bypasses the per-type REGISTRY: maps the
   * {@link SearchFilter} fields to the flat `/api/v1/search` query params
   * (comma-joining multi-value filters) and surfaces a 400 as a structured
   * `{ kind: 'error' }` so the UI can render stable error codes inline.
   */
  async function fetchSearch(args: ListArgs<SearchFilter>): Promise<SearchResult> {
    const f = args.filter ?? {};
    const params = new URLSearchParams();
    if (f.q) params.set('q', f.q);
    if (f.resourceTypes?.length) params.set('type', f.resourceTypes.join(','));
    if (f.packages?.length) params.set('package', f.packages.join(','));
    if (f.tags?.length) params.set('tag', f.tags.join(','));
    if (f.modelingLayers?.length) {
      params.set('modeling_layer', f.modelingLayers.join(','));
    }
    if (f.materializations?.length) {
      params.set('materialization', f.materializations.join(','));
    }
    if (args.limit != null) params.set('first', String(args.limit));
    if (args.cursor) params.set('after', args.cursor);
    const qs = params.toString();
    const url = `${base}/api/v1/search${qs ? `?${qs}` : ''}`;

    const res = await fetch(url);
    // 400 = structured client error (invalid type/layer/cursor, query too long).
    // Preserve the stable code/message so consumers surface it inline.
    if (res.status === 400) {
      const body = (await res.json()) as { code: string; message: string };
      return { kind: 'error', code: body.code, message: body.message };
    }
    if (!res.ok) throw new Error(`${res.status} ${res.statusText} from ${url}`);
    const d = (await res.json()) as RestSearchResponse;
    return { kind: 'ok', page: fromSearchResponse(d) };
  }

  /** Project-wide distinct facet values for the cross-type search surface.
   *  No-arg (global); bypasses the per-type REGISTRY. */
  async function fetchSearchFacets(): Promise<SearchFacets> {
    const d = await getJson<RestSearchFacets>(`${base}/api/v1/search/facets`);
    return fromSearchFacets(
      d ?? {
        accesses: [],
        modeling_layers: [],
        materialization_types: [],
        tags: [],
        packages: [],
      },
    );
  }

  async function fetchLineage(args: LineageArgs): Promise<LineageGraph> {
    const qs = args.depth != null ? `?max_depth=${args.depth}` : '';
    const d = await getJson<RestLineageResponse>(
      `${base}/api/v1/nodes/${enc(args.uniqueId)}/lineage${qs}`,
    );
    return fromLineageResponse(
      d ?? { root: args.uniqueId, max_depth: 0, nodes: [], edges: [] },
    );
  }

  async function fetchColumnLineage(
    args: ColumnLineageArgs,
  ): Promise<ColumnLineageResult> {
    const url = `${base}/api/v1/nodes/${enc(args.uniqueId)}/column-lineage`;
    const res = await fetch(url);
    // 412 = gated (capability not available); preserve the signal so consumers
    // can render an upgrade upsell.
    if (res.status === 412) return { kind: 'gated' };
    // 404 = no lineage data → a valid empty graph.
    if (res.status === 404) return { kind: 'ok', graph: { nodes: [], edges: [] } };
    if (!res.ok) throw new Error(`${res.status} ${res.statusText} from ${url}`);
    const d = (await res.json()) as RestColumnLineageResponse;
    return { kind: 'ok', graph: fromColumnLineageResponse(d) };
  }

  return {
    id: `rest:${base || 'local'}`,
    supportedFilters: new Set<string>([
      'resourceTypes',
      'modelingLayers',
      'owners',
      'packages',
      'results',
      'testTypes',
    ]),
    fetchAsset,
    fetchAssetList,
    fetchFacets,
    fetchCapabilities,
    fetchDistribution,
    fetchAssetCounts,
    fetchProject,
    fetchFiles,
    fetchSearch,
    fetchSearchFacets,
    fetchLineage,
    fetchColumnLineage,
  };
}
