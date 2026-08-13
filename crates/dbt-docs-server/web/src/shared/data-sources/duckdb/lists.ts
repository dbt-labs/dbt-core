/**
 * Per-resource-type list queries, and the facets that drive their filters.
 *
 * Mirrors the REST source's `REGISTRY`: one table keyed by resource type, holding
 * the SQL that produces that type's summary rows plus the mapper that turns them
 * into domain objects. `fetchAssetList` and `fetchFacets` are generic lookups over
 * it, so adding a type means adding a row here and nothing else.
 *
 * Two things the Rust handlers carried that are gone:
 *
 * - **The "view might be missing" variants.** Each list handler built two to four
 *   SQL strings and probed them in order, because `--write-index` skips empty
 *   tables. It still does; the engine now declares an empty relation for the
 *   artifacts that can be absent (`EMPTY_RELATION_DDL`), so one query is enough.
 *   A column added to a query here needs adding there too.
 * - **Cursor pagination.** ADR-6 chose cursors partly because "a read-only parquet
 *   snapshot makes stable cursors free" — with the snapshot in this process that
 *   reasoning collapses into plain offsets. The `Page` contract still speaks
 *   cursors, so an offset is encoded into one and stays opaque to callers.
 */

import type { AssetFilter, ListArgs, ListSort } from '../../typings/args';
import type { AssetSummary, ResourceType } from '../../typings/domain/asset';
import type { Facets, FacetValue } from '../../typings/domain/facets';
import type { Page } from '../../typings/page';
import {
  fromExposureSummary,
  fromGroupSummary,
  fromMacroSummary,
  fromMetricSummary,
  fromModelSummary,
  fromSavedQuerySummary,
  fromSeedSummary,
  fromSemanticModelSummary,
  fromSnapshotSummary,
  fromSourceSummary,
  fromTestSummary,
} from '../mappers/fromWire';
import { sqlStr } from './sql';

/**
 * Reinterpret a query row as a mapper's wire type.
 *
 * Each projection above aliases its columns to exactly the `Rest*Summary` field
 * names, but that correspondence lives in the SQL string and TypeScript cannot see
 * it. Keeping the cast in one named place makes the assumption obvious rather than
 * scattering `as unknown as` through the table.
 */
function asRow<T>(row: Record<string, unknown>): T {
  return row as unknown as T;
}

/** Page size when the caller omits one. Matches the REST default. */
const DEFAULT_LIMIT = 50;

/**
 * Modeling-layer classification, from `LAYER_CONDITIONS` in `handlers/models.rs`.
 *
 * One source of truth for both the projected `CASE` and the `WHERE` filter, so the
 * two cannot drift — the same reason the Rust side kept them in one table.
 */
const LAYER_CONDITIONS: [string, string][] = [
  [
    'Staging',
    `lower(n.original_file_path) LIKE '%/staging/%'
     OR lower(n.original_file_path) LIKE '%/stg_%'
     OR lower(n.original_file_path) LIKE 'staging/%'`,
  ],
  [
    'Intermediate',
    `lower(n.original_file_path) LIKE '%/intermediate/%'
     OR lower(n.original_file_path) LIKE '%/int_%'
     OR lower(n.original_file_path) LIKE 'intermediate/%'`,
  ],
  [
    'Marts',
    `lower(n.original_file_path) LIKE '%/marts/%'
     OR lower(n.original_file_path) LIKE '%/dim_%'
     OR lower(n.original_file_path) LIKE '%/fct_%'
     OR lower(n.original_file_path) LIKE 'marts/%'`,
  ],
];

const MODELING_LAYER_CASE = `CASE
${LAYER_CONDITIONS.map(([layer, cond]) => `  WHEN ${cond} THEN '${layer}'`).join('\n')}
  ELSE NULL
END`;

/** Latest run per node, for `executed_at` and test statuses. */
const LAST_RUN_CTE = `last_run AS (
  SELECT unique_id, MAX(created_at) AS executed_at
  FROM dbt_rt.run_results
  GROUP BY unique_id
)`;

/** Catalog stats, split by `stat_id` the way `handlers/models.rs` does. */
const CATALOG_CTES = `cat_tables AS (
  SELECT unique_id FROM dbt.catalog_tables
), row_count_cte AS (
  SELECT unique_id, TRY_CAST(stat_value AS BIGINT) AS row_count_stat
  FROM dbt.catalog_stats WHERE stat_id IN ('row_count', 'num_rows')
), bytes_cte AS (
  SELECT unique_id, TRY_CAST(stat_value AS BIGINT) AS bytes_stat
  FROM dbt.catalog_stats WHERE stat_id IN ('bytes', 'num_bytes')
), last_modified_cte AS (
  SELECT unique_id, stat_value AS last_modified_stat
  FROM dbt.catalog_stats WHERE stat_id = 'last_modified'
)`;

/** How one resource type is listed. */
interface ListSpec {
  /** Rows for one page. `where` is already composed from the filter. */
  sql(where: string, order: string, limit: number, offset: number): string;
  /** Total matching rows, for the page envelope. */
  countSql(where: string): string;
  tables: string[];
  /** Turn one row into a domain summary. */
  map(row: Record<string, unknown>): AssetSummary;
  /** Filter fields this type honors, as `AssetFilter` keys. */
  filters: string[];
  /** Sortable fields → SQL expression. `name` is always available. */
  sortable: Record<string, string>;
  /** Compose the type's `WHERE` fragments from a filter. */
  where?(filter: AssetFilter): string[];
}

/** `SELECT … FROM dbt.nodes` for one `resource_type`, with shared plumbing. */
function nodeBacked(
  resourceType: string,
  columns: string,
  extras: { ctes?: string[]; joins?: string[] } = {},
): Pick<ListSpec, 'sql' | 'countSql'> {
  const ctes = extras.ctes?.length ? `WITH ${extras.ctes.join(',\n')}\n` : '';
  const joins = extras.joins?.length ? `\n${extras.joins.join('\n')}` : '';
  const base = `FROM dbt.nodes n${joins}\nWHERE n.resource_type = '${resourceType}'`;
  return {
    sql: (where, order, limit, offset) =>
      `${ctes}SELECT ${columns}\n${base}${where}\n${order}\nLIMIT ${limit} OFFSET ${offset}`,
    // The count carries the same CTEs and joins as the page. It has to: the joins
    // reference the CTEs, so omitting the `WITH` leaves a dangling table
    // reference, and a filter may predicate on a joined column.
    countSql: (where) => `${ctes}SELECT COUNT(*) AS total ${base}${where}`,
  };
}

/** `SELECT … FROM <table>` for types with their own artifact. */
function ownTable(
  table: string,
  columns: string,
  alias = 't',
): Pick<ListSpec, 'sql' | 'countSql'> {
  const base = `FROM ${table} ${alias}\nWHERE 1 = 1`;
  return {
    sql: (where, order, limit, offset) =>
      `SELECT ${columns}\n${base}${where}\n${order}\nLIMIT ${limit} OFFSET ${offset}`,
    countSql: (where) => `SELECT COUNT(*) AS total ${base}${where}`,
  };
}

/** `IN (…)` fragment, or `''` when the list is empty. */
function inList(expr: string, values: string[] | undefined): string {
  if (!values?.length) return '';
  return `\n  AND ${expr} IN (${values.map(sqlStr).join(', ')})`;
}

export const LIST_REGISTRY: Partial<Record<ResourceType, ListSpec>> = {
  model: {
    ...nodeBacked(
      'model',
      `n.unique_id,
       n.name,
       n.package_name,
       n.original_file_path,
       ${MODELING_LAYER_CASE} AS modeling_layer,
       n.access_level,
       n.contract_enforced,
       n.group_name AS owner,
       CAST(lr.executed_at AS VARCHAR) AS executed_at,
       ct.unique_id IS NOT NULL AS has_catalog,
       rcc.row_count_stat,
       bc.bytes_stat,
       lm.last_modified_stat`,
      {
        ctes: [LAST_RUN_CTE, CATALOG_CTES],
        joins: [
          'LEFT JOIN last_run lr ON lr.unique_id = n.unique_id',
          'LEFT JOIN cat_tables ct ON ct.unique_id = n.unique_id',
          'LEFT JOIN row_count_cte rcc ON rcc.unique_id = n.unique_id',
          'LEFT JOIN bytes_cte bc ON bc.unique_id = n.unique_id',
          'LEFT JOIN last_modified_cte lm ON lm.unique_id = n.unique_id',
        ],
      },
    ),
    tables: [
      'dbt.nodes',
      'dbt_rt.run_results',
      'dbt.catalog_tables',
      'dbt.catalog_stats',
      'dbt.groups',
    ],
    filters: ['modelingLayers', 'owners', 'packages'],
    sortable: {
      name: 'n.name',
      access_level: 'n.access_level',
      contract_enforced: 'n.contract_enforced',
      owner: 'n.group_name',
      executed_at: 'CAST(lr.executed_at AS VARCHAR)',
      modeling_layer: MODELING_LAYER_CASE,
    },
    where: (filter) => {
      const parts: string[] = [];
      if (filter.modelingLayers?.length) {
        // Built from the same table as the projected CASE, so a filtered layer
        // always matches the layer the row displays.
        const conds = LAYER_CONDITIONS.filter(([layer]) =>
          filter.modelingLayers!.includes(layer),
        ).map(([, cond]) => `(${cond})`);
        if (conds.length) parts.push(`\n  AND (${conds.join(' OR ')})`);
      }
      parts.push(inList('n.group_name', filter.owners));
      parts.push(inList('n.package_name', filter.packages));
      return parts;
    },
    map: (row) =>
      fromModelSummary({
        ...row,
        // The nested keys keep the `_stat` suffix — `fromModelSummary` reads
        // `catalog.row_count_stat`, not `catalog.row_count`. Getting this wrong is
        // silent: the mapper just yields nulls and the columns render empty.
        catalog: row.has_catalog
          ? {
              row_count_stat: row.row_count_stat ?? null,
              bytes_stat: row.bytes_stat ?? null,
              last_modified_stat: row.last_modified_stat ?? null,
            }
          : null,
      } as unknown as Parameters<typeof fromModelSummary>[0]),
  },

  source: {
    ...nodeBacked(
      'source',
      `n.unique_id,
       n.name,
       n.package_name,
       n.source_name,
       n.source_description,
       n.database_name,
       n.schema_name,
       n.identifier,
       n.loader,
       n.tags,
       sf.status AS freshness_status,
       CAST(sf.snapshotted_at AS VARCHAR) AS snapshotted_at,
       CAST(sf.max_loaded_at AS VARCHAR) AS max_loaded_at`,
      { joins: ['LEFT JOIN dbt.source_freshness sf ON sf.unique_id = n.unique_id'] },
    ),
    tables: ['dbt.nodes', 'dbt.source_freshness'],
    filters: ['packages'],
    sortable: { name: 'n.name' },
    where: (filter) => [inList('n.package_name', filter.packages)],
    map: (row) =>
      fromSourceSummary({
        ...row,
        freshness: row.freshness_status
          ? {
              status: row.freshness_status,
              snapshotted_at: row.snapshotted_at ?? null,
              max_loaded_at: row.max_loaded_at ?? null,
            }
          : null,
      } as unknown as Parameters<typeof fromSourceSummary>[0]),
  },

  seed: {
    ...nodeBacked(
      'seed',
      `n.unique_id, n.name, n.package_name, n.description,
       CAST(lr.executed_at AS VARCHAR) AS executed_at,
       rcc.row_count_stat`,
      {
        ctes: [
          LAST_RUN_CTE,
          `row_count_cte AS (
  SELECT unique_id, TRY_CAST(stat_value AS BIGINT) AS row_count_stat
  FROM dbt.catalog_stats WHERE stat_id IN ('row_count', 'num_rows')
)`,
        ],
        joins: [
          'LEFT JOIN last_run lr ON lr.unique_id = n.unique_id',
          'LEFT JOIN row_count_cte rcc ON rcc.unique_id = n.unique_id',
        ],
      },
    ),
    tables: ['dbt.nodes', 'dbt_rt.run_results', 'dbt.catalog_stats'],
    filters: ['packages'],
    sortable: { name: 'n.name' },
    where: (filter) => [inList('n.package_name', filter.packages)],
    map: (row) => fromSeedSummary(asRow<Parameters<typeof fromSeedSummary>[0]>(row)),
  },

  snapshot: {
    ...nodeBacked(
      'snapshot',
      `n.unique_id, n.name, n.package_name, n.description,
       n.database_name, n.schema_name, n.identifier,
       CAST(lr.executed_at AS VARCHAR) AS executed_at`,
      {
        ctes: [LAST_RUN_CTE],
        joins: ['LEFT JOIN last_run lr ON lr.unique_id = n.unique_id'],
      },
    ),
    tables: ['dbt.nodes', 'dbt_rt.run_results'],
    filters: ['packages'],
    sortable: { name: 'n.name' },
    where: (filter) => [inList('n.package_name', filter.packages)],
    map: (row) =>
      fromSnapshotSummary(asRow<Parameters<typeof fromSnapshotSummary>[0]>(row)),
  },

  macro: {
    ...ownTable('dbt.macros', 't.unique_id, t.name, t.package_name, t.description'),
    tables: ['dbt.macros'],
    filters: ['packages'],
    sortable: { name: 't.name' },
    where: (filter) => [inList('t.package_name', filter.packages)],
    map: (row) => fromMacroSummary(asRow<Parameters<typeof fromMacroSummary>[0]>(row)),
  },

  exposure: {
    ...ownTable(
      'dbt.exposures',
      `t.unique_id, t.name, t.package_name, t.exposure_type, t.maturity,
       t.owner_name, t.owner_email, t.url, t.tags`,
    ),
    tables: ['dbt.exposures'],
    filters: ['packages'],
    sortable: { name: 't.name' },
    where: (filter) => [inList('t.package_name', filter.packages)],
    map: (row) =>
      fromExposureSummary(asRow<Parameters<typeof fromExposureSummary>[0]>(row)),
  },

  metric: {
    ...ownTable(
      'dbt.metrics',
      't.unique_id, t.name, t.package_name, t.group_name, t.metric_type, t.tags',
    ),
    tables: ['dbt.metrics'],
    filters: ['packages'],
    sortable: { name: 't.name' },
    where: (filter) => [inList('t.package_name', filter.packages)],
    map: (row) =>
      fromMetricSummary(asRow<Parameters<typeof fromMetricSummary>[0]>(row)),
  },

  saved_query: {
    ...ownTable(
      'dbt.saved_queries',
      't.unique_id, t.name, t.package_name, t.description',
    ),
    tables: ['dbt.saved_queries'],
    filters: ['packages'],
    sortable: { name: 't.name' },
    where: (filter) => [inList('t.package_name', filter.packages)],
    map: (row) =>
      fromSavedQuerySummary(asRow<Parameters<typeof fromSavedQuerySummary>[0]>(row)),
  },

  semantic_model: {
    ...ownTable(
      'dbt.semantic_models',
      't.unique_id, t.name, t.package_name, t.description, t.model AS model_unique_id',
    ),
    tables: ['dbt.semantic_models'],
    filters: ['packages'],
    sortable: { name: 't.name' },
    where: (filter) => [inList('t.package_name', filter.packages)],
    map: (row) =>
      fromSemanticModelSummary(
        asRow<Parameters<typeof fromSemanticModelSummary>[0]>(row),
      ),
  },

  group: {
    // `model_count` is a correlated subquery, as in `handlers/groups.rs`: groups are
    // keyed by (name, package) rather than by unique_id, so a join would need both.
    ...ownTable(
      'dbt.groups',
      `t.unique_id, t.name, t.package_name, t.owner_name, t.owner_email,
       json_extract_string(t.config, '$.owner.github') AS owner_github,
       json_extract_string(t.config, '$.owner.slack') AS owner_slack,
       (SELECT COUNT(*) FROM dbt.nodes n
        WHERE n.group_name = t.name
          AND n.package_name = t.package_name
          AND n.resource_type = 'model') AS model_count`,
    ),
    tables: ['dbt.groups', 'dbt.nodes'],
    filters: [],
    sortable: { name: 't.name' },
    map: (row) => fromGroupSummary(asRow<Parameters<typeof fromGroupSummary>[0]>(row)),
  },

  test: {
    // `test` and `unit_test` share one list, per ADR-3: both render on the same page,
    // so the union happens here rather than in two surfaces.
    sql: (where, order, limit, offset) => `${testsUnionCte()}
SELECT u.*, CAST(lr.executed_at AS VARCHAR) AS executed_at, lr.status, lr.message
FROM tests_union u
LEFT JOIN last_run_status lr ON lr.unique_id = u.unique_id
WHERE 1 = 1${where}
${order}
LIMIT ${limit} OFFSET ${offset}`,
    countSql: (where) =>
      `${testsUnionCte()}\nSELECT COUNT(*) AS total FROM tests_union u WHERE 1 = 1${where}`,
    tables: ['dbt.nodes', 'dbt.test_metadata', 'dbt.unit_tests', 'dbt_rt.run_results'],
    filters: ['packages', 'testTypes'],
    sortable: { name: 'u.name' },
    where: (filter) => [
      inList('u.package_name', filter.packages),
      inList('u.test_type', filter.testTypes),
    ],
    map: (row) => fromTestSummary(mapTestRow(row)),
  },
};

/** The `test` ∪ `unit_test` union plus the run-status CTE it joins to. */
function testsUnionCte(): string {
  return `WITH tests_union AS (
  SELECT n.unique_id,
         n.name,
         'test' AS resource_type,
         n.package_name,
         'data' AS test_type,
         tm.attached_node AS tested_node_unique_id,
         tm.column_name AS tested_column,
         tm.severity
  FROM dbt.nodes n
  LEFT JOIN dbt.test_metadata tm ON tm.unique_id = n.unique_id
  WHERE n.resource_type = 'test'
  UNION ALL
  SELECT ut.unique_id,
         ut.name,
         'unit_test',
         ut.package_name,
         'unit',
         ut.depends_on_nodes[1],
         NULL,
         NULL
  FROM dbt.unit_tests ut
), last_run_status AS (
  SELECT unique_id, MAX(created_at) AS executed_at, ANY_VALUE(status) AS status,
         ANY_VALUE(message) AS message
  FROM dbt_rt.run_results
  GROUP BY unique_id
)`;
}

/** Assemble the nested `execution_info` a test summary expects. */
function mapTestRow(
  row: Record<string, unknown>,
): Parameters<typeof fromTestSummary>[0] {
  return {
    ...row,
    execution_info: row.status
      ? {
          status: row.status,
          completed_at: row.executed_at ?? null,
          error: row.message ?? null,
        }
      : null,
  } as unknown as Parameters<typeof fromTestSummary>[0];
}

/** Every filter field any registered type honors. Drives `supportedFilters`. */
export function supportedFilterFields(): Set<string> {
  const fields = new Set<string>(['resourceTypes']);
  for (const spec of Object.values(LIST_REGISTRY)) {
    spec?.filters.forEach((f) => fields.add(f));
  }
  return fields;
}

/**
 * Offsets, encoded so they stay opaque.
 *
 * The `Page` contract speaks cursors and callers must not parse them, but there is
 * nothing to keep stable across requests any more — the snapshot lives in this
 * process. `base64` rather than a bare number so a caller that peeks sees an
 * opaque token and does not start doing arithmetic on it.
 */
export function encodeCursor(offset: number): string {
  return btoa(String(offset));
}

export function decodeCursor(cursor: string | null | undefined): number {
  if (!cursor) return 0;
  const offset = Number.parseInt(atob(cursor), 10);
  return Number.isFinite(offset) && offset >= 0 ? offset : 0;
}

/** `ORDER BY` for a request, always tie-broken by unique_id for a stable page. */
function orderBy(spec: ListSpec, sort: ListSort | undefined, idExpr: string): string {
  const expr = (sort?.field && spec.sortable[sort.field]) || spec.sortable.name;
  const dir = sort?.desc ? 'DESC' : 'ASC';
  return `ORDER BY ${expr} ${dir} NULLS LAST, ${idExpr} ASC`;
}

export interface ListQuery {
  sql: string;
  countSql: string;
  tables: string[];
  offset: number;
  limit: number;
  map(row: Record<string, unknown>): AssetSummary;
}

/**
 * Build the queries for one list request, or `null` for a type with no list.
 *
 * Types absent from the registry (`analysis`, `function`, `operation`) had no REST
 * list endpoint either.
 */
export function buildListQuery(args: ListArgs<AssetFilter>): ListQuery | null {
  const resourceType = args.filter?.resourceTypes?.[0];
  const spec = resourceType ? LIST_REGISTRY[resourceType] : undefined;
  if (!spec) return null;

  const where = (spec.where?.(args.filter ?? {}) ?? []).join('');
  const limit = args.limit ?? DEFAULT_LIMIT;
  const offset = decodeCursor(args.cursor);
  const idExpr = spec.sortable.name.startsWith('u.')
    ? 'u.unique_id'
    : spec.sortable.name.startsWith('t.')
      ? 't.unique_id'
      : 'n.unique_id';

  return {
    // One extra row is *not* fetched: `totalCount` from the count query already
    // tells the caller whether another page exists.
    sql: spec.sql(where, orderBy(spec, args.sort, idExpr), limit, offset),
    countSql: spec.countSql(where),
    tables: spec.tables,
    offset,
    limit,
    map: spec.map,
  };
}

/** Assemble the domain page from the two query results. */
export function toPage(
  rows: Record<string, unknown>[],
  total: number,
  query: ListQuery,
): Page<AssetSummary> {
  const nextOffset = query.offset + rows.length;
  return {
    items: rows.map(query.map),
    nextCursor: nextOffset < total ? encodeCursor(nextOffset) : null,
    totalCount: total,
  };
}

/** Facet queries per type, from the `*_FACET_SQL` constants in the handlers. */
export const FACET_QUERIES: Partial<
  Record<ResourceType, { key: string; sql: string; tables: string[] }[]>
> = {
  model: [
    {
      key: 'owners',
      sql: 'SELECT DISTINCT name AS value FROM dbt.groups ORDER BY value',
      tables: ['dbt.groups'],
    },
    {
      key: 'materializations',
      sql: `SELECT DISTINCT materialized AS value FROM dbt.nodes
            WHERE resource_type = 'model' AND materialized IS NOT NULL
            ORDER BY value`,
      tables: ['dbt.nodes'],
    },
    {
      key: 'packages',
      sql: `SELECT DISTINCT package_name AS value FROM dbt.nodes
            WHERE resource_type = 'model' AND package_name IS NOT NULL
            ORDER BY value`,
      tables: ['dbt.nodes'],
    },
  ],
  macro: [
    {
      key: 'packages',
      sql: `SELECT DISTINCT package_name AS value FROM dbt.macros
            WHERE package_name IS NOT NULL ORDER BY value`,
      tables: ['dbt.macros'],
    },
  ],
  test: [
    {
      key: 'packages',
      sql: `SELECT DISTINCT package_name AS value FROM dbt.nodes
            WHERE resource_type = 'test' AND package_name IS NOT NULL
            ORDER BY value`,
      tables: ['dbt.nodes'],
    },
  ],
};

/**
 * Facet values the handlers hardcoded rather than queried.
 *
 * `modelingLayers` is derived from file paths, so its domain is fixed by
 * `LAYER_CONDITIONS` rather than by the data; `accesses` and `testTypes` are
 * likewise closed sets. Counts are null because none of these were counted
 * server-side either.
 */
export function constantFacets(resourceType: ResourceType): Facets {
  const values = (list: string[]): FacetValue[] =>
    list.map((value) => ({ value, count: null }));

  switch (resourceType) {
    case 'model':
      return {
        modelingLayers: values(LAYER_CONDITIONS.map(([layer]) => layer)),
        accesses: values(['private', 'protected', 'public']),
      };
    case 'test':
      return { testTypes: values(['data', 'unit']) };
    default:
      return {};
  }
}
