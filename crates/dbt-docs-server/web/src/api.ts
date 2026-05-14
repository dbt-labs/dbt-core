export interface Health {
  ok: boolean;
  version: string;
  index_dir: string;
}

/** Gated feature surfaces. Only column-level lineage is gated today;
 *  more fields will land here as proprietary surfaces (sample data, AI,
 *  etc.) are added. Plain SQL-over-parquet reads (nodes, sources,
 *  exposures, metrics, run results) are always available and have no
 *  capability flag. */
export interface Capabilities {
  has_column_lineage: boolean;
}

export interface Project {
  name: string;
  project_id?: string;
  description?: string | null;
  dbt_version?: string;
  adapter_type?: string;
  git_sha?: string | null;
  git_branch?: string | null;
  git_is_dirty?: boolean | null;
}

export interface NodeSummary {
  unique_id: string;
  name: string;
  resource_type: string;
  package_name?: string;
  materialized?: string | null;
  description?: string | null;
  database_name?: string | null;
  schema_name?: string | null;
  original_file_path?: string | null;
}

export interface NodeColumn {
  name: string;
  index?: number | null;
  data_type?: string | null;
  declared_type?: string | null;
  inferred_type?: string | null;
  catalog_type?: string | null;
  description?: string | null;
  label?: string | null;
  granularity?: string | null;
}

export interface EdgeRef {
  unique_id: string;
  edge_type: string;
}

export interface NodeDetail extends NodeSummary {
  relation_name?: string | null;
  identifier?: string | null;
  access_level?: string | null;
  group_name?: string | null;
  raw_code?: string | null;
  columns: NodeColumn[];
  depends_on: EdgeRef[];
  referenced_by: EdgeRef[];
}

export interface NodeListResponse {
  nodes: NodeSummary[];
  total: number;
  offset: number;
  limit: number;
}

/** A node in a model-level lineage subgraph. `depth` is signed: negative
 *  upstream, 0 root, positive downstream. */
export interface LineageNode {
  unique_id: string;
  name: string;
  resource_type: string;
  materialized?: string | null;
  depth: number;
}

export interface LineageEdge {
  from_id: string;
  to_id: string;
  edge_type: string;
}

export interface LineageResponse {
  root: string;
  max_depth: number;
  nodes: LineageNode[];
  edges: LineageEdge[];
}

export interface ColumnLineageEdge {
  from_node: string;
  from_column: string;
  to_node: string;
  to_column: string;
  kind: string;
}

export interface ColumnLineageResponse {
  root: string;
  edges: ColumnLineageEdge[];
}

/** Structured response from a 412 — column lineage gated. */
export interface UnavailableResponse {
  code: string;
  message: string;
  upgrade_path?: string;
}

/** Result type that captures a 412 distinctly from a 200 or other failure. */
export type Gated<T> =
  | { kind: "ok"; value: T }
  | { kind: "unavailable"; details: UnavailableResponse };

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

async function getJson<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, init);
  if (!res.ok) throw new ApiError(res.status, `${res.status} ${res.statusText} from ${path}`);
  return (await res.json()) as T;
}

async function getGated<T>(path: string): Promise<Gated<T>> {
  const res = await fetch(path);
  if (res.status === 412) {
    const details = (await res.json()) as UnavailableResponse;
    return { kind: "unavailable", details };
  }
  if (!res.ok) throw new ApiError(res.status, `${res.status} ${res.statusText} from ${path}`);
  return { kind: "ok", value: (await res.json()) as T };
}

export interface QueryColumn {
  name: string;
  data_type: string;
}

export interface QueryResponse {
  columns: QueryColumn[];
  rows: Record<string, unknown>[];
  row_count: number;
  truncated: boolean;
  elapsed_ms: number;
}

export interface TableInfo {
  schema: string;
  name: string;
  columns: QueryColumn[];
}

/** 400-shaped error body from `/api/v1/query` — DuckDB error or invalid input. */
export interface QueryErrorBody {
  error: string;
}

export interface QueryFailure {
  kind: "error";
  message: string;
}

export interface QuerySuccess {
  kind: "ok";
  value: QueryResponse;
}

export type QueryResult = QuerySuccess | QueryFailure;

export const api = {
  health: () => getJson<Health>("/api/v1/health"),
  capabilities: () => getJson<Capabilities>("/api/v1/capabilities"),
  project: () => getJson<Project>("/api/v1/project"),
  nodes: (
    params: {
      type?: string;
      package?: string;
      q?: string;
      limit?: number;
      offset?: number;
    } = {},
  ) => {
    const u = new URLSearchParams();
    if (params.type) u.set("type", params.type);
    if (params.package) u.set("package", params.package);
    if (params.q) u.set("q", params.q);
    if (params.limit != null) u.set("limit", String(params.limit));
    if (params.offset != null) u.set("offset", String(params.offset));
    const qs = u.toString();
    return getJson<NodeListResponse>(`/api/v1/nodes${qs ? `?${qs}` : ""}`);
  },
  node: (uniqueId: string) =>
    getJson<NodeDetail>(`/api/v1/nodes/${encodeURIComponent(uniqueId)}`),
  lineage: (uniqueId: string, maxDepth = 5) =>
    getJson<LineageResponse>(
      `/api/v1/nodes/${encodeURIComponent(uniqueId)}/lineage?max_depth=${maxDepth}`,
    ),
  columnLineage: (uniqueId: string) =>
    getGated<ColumnLineageResponse>(
      `/api/v1/nodes/${encodeURIComponent(uniqueId)}/column-lineage`,
    ),
  tables: () => getJson<TableInfo[]>("/api/v1/tables"),
  query: async (sql: string, signal?: AbortSignal): Promise<QueryResult> => {
    const res = await fetch("/api/v1/query", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ sql }),
      signal,
    });
    if (res.status === 400) {
      const body = (await res.json()) as QueryErrorBody;
      return { kind: "error", message: body.error };
    }
    if (!res.ok) {
      throw new ApiError(res.status, `${res.status} ${res.statusText} from /api/v1/query`);
    }
    const value = (await res.json()) as QueryResponse;
    return { kind: "ok", value };
  },
};
