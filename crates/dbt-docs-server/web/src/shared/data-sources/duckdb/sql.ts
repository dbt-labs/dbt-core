/**
 * SQL the browser runs against the site's parquet.
 *
 * Ported from the Rust handlers of the same name, near-verbatim: both sides speak
 * DuckDB, so the queries carry over and stay diffable against
 * `crates/dbt-docs-server/src/handlers/`. Every projection keeps the handler's
 * snake_case aliases, which is what lets the existing `fromRest` mappers consume
 * these rows unchanged and keeps their tests meaningful.
 *
 * What does *not* carry over is the machinery that existed because it was a
 * network API: cursor envelopes, `total_count` round-trips, and the two-to-four
 * "view might be missing" variants per handler. The exporter guarantees every
 * artifact is present, so a single query per surface is enough.
 */

/** Literal-escape a string for interpolation, mirroring the Rust `escape_str`. */
export function sqlStr(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

/** `handlers/project.rs` — one row of project identity. */
export const PROJECT_SQL = `
SELECT project_name AS name,
       project_id,
       description,
       dbt_version,
       adapter_type,
       git_sha,
       git_branch,
       git_is_dirty
FROM dbt.project
LIMIT 1`;

export const PROJECT_TABLES = ['dbt.project'];

/**
 * The winning `{% docs __overview__ %}` block, or no row at all.
 *
 * Parity with dbt-docs v1, whose `OverviewCtrl` seeds from `doc.dbt.__overview__`
 * and then lets any doc named `__overview__` from a non-`dbt` package override it.
 * Three things here are load-bearing:
 *
 * - **The default is excluded, so "no authored overview" returns zero rows.** That
 *   is the signal the page falls back to its bundled default on. v1's built-in
 *   default described v1's own `Project`/`Database` tabs, so surfacing it here
 *   would be actively wrong.
 * - **We discriminate on `unique_id`, not `package_name <> 'dbt'`.** The ingest only
 *   started populating `package_name` recently; on an older index it is NULL
 *   everywhere, and `NULL <> 'dbt'` is NULL, which would silently drop every row —
 *   including a user's own overview. `unique_id` is non-null in every index ever
 *   written, and is exactly equivalent, since it is `doc.{package}.{name}` and no
 *   project may be named `dbt`.
 * - **Root project first, then package name.** v1 broke ties by manifest insertion
 *   order, which is filesystem-dependent and not reproducible here; this matches v1
 *   for any single-project case and is at least deterministic beyond it. `CASE WHEN`
 *   rather than `(package_name = x) DESC` keeps a NULL `package_name` in the ELSE
 *   branch with no NULLS FIRST/LAST subtlety, and `unique_id` makes the order total.
 */
export function overviewSql(rootPackage: string): string {
  return `
SELECT unique_id, package_name, block_contents
FROM dbt.docs
WHERE name = '__overview__'
  AND unique_id <> 'doc.dbt.__overview__'
  AND block_contents IS NOT NULL
  AND length(trim(block_contents)) > 0
ORDER BY CASE WHEN package_name = ${sqlStr(rootPackage)} THEN 0 ELSE 1 END,
         package_name,
         unique_id
LIMIT 1`;
}

export const OVERVIEW_TABLES = ['dbt.docs'];

/**
 * `handlers/nodes.rs::list_node_counts` — per-resource-type tallies.
 *
 * The resource-bearing tables sit outside `dbt.nodes`, so they are counted
 * separately and summed. `unit_test` folds into `test` in the mapper below rather
 * than here, matching how the handler did it in Rust after the query.
 */
export const COUNTS_SQL = `
WITH raw AS (
  SELECT resource_type, COUNT(*) AS count FROM dbt.nodes GROUP BY resource_type
  UNION ALL SELECT 'exposure',       COUNT(*) FROM dbt.exposures
  UNION ALL SELECT 'group',          COUNT(*) FROM dbt.groups
  UNION ALL SELECT 'macro',          COUNT(*) FROM dbt.macros
  UNION ALL SELECT 'metric',         COUNT(*) FROM dbt.metrics
  UNION ALL SELECT 'saved_query',    COUNT(*) FROM dbt.saved_queries
  UNION ALL SELECT 'semantic_model', COUNT(*) FROM dbt.semantic_models
)
SELECT resource_type, CAST(SUM(count) AS BIGINT) AS count
FROM raw
GROUP BY resource_type
ORDER BY resource_type`;

export const COUNTS_TABLES = [
  'dbt.nodes',
  'dbt.exposures',
  'dbt.groups',
  'dbt.macros',
  'dbt.metrics',
  'dbt.saved_queries',
  'dbt.semantic_models',
];

/**
 * `handlers/files.rs` — every file-bearing resource, unpaginated.
 *
 * The Rust version gated each arm on `table_has_rows` to avoid querying an absent
 * view; here every artifact exists, so the union runs as written. `patch_path` is
 * only real for nodes and macros, and `dbt.semantic_models` needs the null filter
 * because its rows can lack a file path entirely.
 */
export const FILES_SQL = `
SELECT unique_id, name, resource_type, package_name, original_file_path, patch_path
FROM dbt.nodes
UNION ALL
SELECT unique_id, name, 'exposure', package_name, original_file_path, NULL
FROM dbt.exposures
UNION ALL
SELECT unique_id, name, 'metric', package_name, original_file_path, NULL
FROM dbt.metrics
UNION ALL
SELECT unique_id, name, 'macro', package_name, original_file_path, patch_path
FROM dbt.macros
UNION ALL
SELECT unique_id, name, 'semantic_model', package_name, original_file_path, NULL
FROM dbt.semantic_models
WHERE original_file_path IS NOT NULL
UNION ALL
SELECT unique_id, name, 'group', package_name, original_file_path, NULL
FROM dbt.groups
UNION ALL
SELECT unique_id, name, 'unit_test', package_name, original_file_path, NULL
FROM dbt.unit_tests
UNION ALL
SELECT unique_id, name, 'saved_query', package_name, original_file_path, NULL
FROM dbt.saved_queries
ORDER BY original_file_path, name`;

export const FILES_TABLES = [
  'dbt.nodes',
  'dbt.exposures',
  'dbt.metrics',
  'dbt.macros',
  'dbt.semantic_models',
  'dbt.groups',
  'dbt.unit_tests',
  'dbt.saved_queries',
];

/** Both edge directions for one node, shaped as `RestEdgeRef` rows. */
export function nodeEdgesSql(uniqueId: string): string {
  const id = sqlStr(uniqueId);
  return `
SELECT 'depends_on' AS direction, parent_unique_id AS unique_id, edge_type
FROM dbt.edges
WHERE child_unique_id = ${id}
UNION ALL
SELECT 'referenced_by', child_unique_id, edge_type
FROM dbt.edges
WHERE parent_unique_id = ${id}`;
}

export const NODE_EDGES_TABLES = ['dbt.edges'];

/**
 * `handlers/lineage.rs` — the depth cap, which is hard rather than a default.
 *
 * The Rust handler refused a larger `?max_depth`, so keeping it fixed here holds
 * graph size (and DAG render cost) to what the UI was built for.
 */
export const LINEAGE_MAX_DEPTH = 3;

/** The two recursive walks both lineage queries share. */
function lineageWalks(uniqueId: string, maxDepth: number): string {
  const id = sqlStr(uniqueId);
  return `
WITH RECURSIVE
upstream AS (
  SELECT parent_unique_id AS unique_id, -1 AS depth
  FROM dbt.edges WHERE child_unique_id = ${id}
  UNION ALL
  SELECT e.parent_unique_id, u.depth - 1
  FROM dbt.edges e JOIN upstream u ON e.child_unique_id = u.unique_id
  WHERE u.depth > ${-maxDepth}
),
downstream AS (
  SELECT child_unique_id AS unique_id, 1 AS depth
  FROM dbt.edges WHERE parent_unique_id = ${id}
  UNION ALL
  SELECT e.child_unique_id, d.depth + 1
  FROM dbt.edges e JOIN downstream d ON e.parent_unique_id = d.unique_id
  WHERE d.depth < ${maxDepth}
)`;
}

/**
 * Nodes within `maxDepth` of the root, with a signed depth (negative upstream,
 * 0 root, positive downstream).
 *
 * The `metadata` CTE unions the three resource tables that appear in `dbt.edges`
 * but not in `dbt.nodes` — without them a metric or exposure in the graph would
 * resolve to no name and drop out of the join.
 */
export function lineageNodesSql(
  uniqueId: string,
  maxDepth = LINEAGE_MAX_DEPTH,
): string {
  const id = sqlStr(uniqueId);
  return `${lineageWalks(uniqueId, maxDepth)},
all_ids AS (
  SELECT ${id} AS unique_id, 0 AS depth
  UNION ALL
  SELECT unique_id, MIN(depth) FROM upstream GROUP BY unique_id
  UNION ALL
  SELECT unique_id, MAX(depth) FROM downstream GROUP BY unique_id
),
metadata AS (
  SELECT unique_id, name, resource_type, materialized FROM dbt.nodes
  UNION ALL
  SELECT unique_id, name, 'metric', NULL FROM dbt.metrics
  UNION ALL
  SELECT unique_id, name, 'semantic_model', NULL FROM dbt.semantic_models
  UNION ALL
  SELECT unique_id, name, 'exposure', NULL FROM dbt.exposures
)
SELECT m.unique_id, m.name, m.resource_type, m.materialized, MIN(a.depth) AS depth
FROM all_ids a JOIN metadata m ON m.unique_id = a.unique_id
GROUP BY m.unique_id, m.name, m.resource_type, m.materialized
ORDER BY depth, m.resource_type, m.name`;
}

/** Every edge whose both endpoints fall inside the subgraph. */
export function lineageEdgesSql(
  uniqueId: string,
  maxDepth = LINEAGE_MAX_DEPTH,
): string {
  const id = sqlStr(uniqueId);
  return `${lineageWalks(uniqueId, maxDepth)},
all_ids AS (
  SELECT ${id} AS unique_id
  UNION
  SELECT unique_id FROM upstream
  UNION
  SELECT unique_id FROM downstream
)
SELECT e.parent_unique_id AS from_id, e.child_unique_id AS to_id, e.edge_type
FROM dbt.edges e
WHERE e.parent_unique_id IN (SELECT unique_id FROM all_ids)
  AND e.child_unique_id IN (SELECT unique_id FROM all_ids)
ORDER BY e.parent_unique_id, e.child_unique_id`;
}

export const LINEAGE_TABLES = [
  'dbt.edges',
  'dbt.nodes',
  'dbt.metrics',
  'dbt.semantic_models',
  'dbt.exposures',
];

/** `dbt.saved_queries.depends_on_nodes` for the saved-query lineage special case. */
export function savedQueryDependsOnSql(uniqueId: string): string {
  return `
SELECT depends_on_nodes
FROM dbt.saved_queries
WHERE unique_id = ${sqlStr(uniqueId)}
LIMIT 1`;
}

export const SAVED_QUERY_TABLES = ['dbt.saved_queries'];

/**
 * Single-hop column lineage touching one node, optionally one column.
 *
 * One row per edge touching the node, in either direction. Column names are
 * lowercased on both sides because the index stores them as the warehouse
 * reported them, while the UI addresses them in lowercase.
 */
export function columnLineageSql(uniqueId: string, column?: string): string {
  const id = sqlStr(uniqueId);
  const to = column
    ? `to_node_unique_id = ${id} AND LOWER(to_column_name) = ${sqlStr(column.toLowerCase())}`
    : `to_node_unique_id = ${id}`;
  const from = column
    ? `from_node_unique_id = ${id} AND LOWER(from_column_name) = ${sqlStr(column.toLowerCase())}`
    : `from_node_unique_id = ${id}`;

  return `
SELECT from_node_unique_id AS from_node,
       LOWER(from_column_name) AS from_column,
       to_node_unique_id AS to_node,
       LOWER(to_column_name) AS to_column,
       lineage_kind
FROM dbt.column_lineage
WHERE (${to}) OR (${from})`;
}

export const COLUMN_LINEAGE_TABLES = ['dbt.column_lineage'];

/**
 * Normalize the index's raw `lineage_kind` to the vocabulary the UI renders.
 *
 * An unrecognized value passes through rather than being dropped, so a new kind
 * shows up as itself instead of vanishing.
 */
export function normalizeLineageKind(raw: string): string {
  switch (raw) {
    case 'copy':
      return 'passthrough';
    case 'mod':
      return 'transform';
    case 'scan':
      return 'indirect';
    default:
      return raw;
  }
}
