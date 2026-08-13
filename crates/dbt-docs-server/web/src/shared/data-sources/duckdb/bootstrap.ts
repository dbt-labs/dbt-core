/**
 * First-paint data, read without DuckDB.
 *
 * DuckDB-WASM is ~6.8 MB brotli on a cold cache. Waiting for it before rendering
 * anything would be a visible regression against the REST app, so the shell,
 * sidebar and home page come from three small artifacts read with hyparquet — a
 * ~88 KB pure-JS parquet reader — while the engine streams in behind them.
 *
 * The site reads the index artifacts as written — there is no exported copy and no
 * derived projection, so `dbt.nodes` is the index's own table. Only the nine
 * `NodeSummary` columns the shell renders are decoded (`columns` below); the whole
 * file still crosses the wire, because artifacts are fetched whole rather than by
 * range. At 6,472 nodes that is 1.79 MB in place of a 343 KB projection — the cost
 * of having one artifact contract instead of two.
 *
 * Everything past first paint — detail pages, lists, search, lineage — goes
 * through DuckDB. This is not a second query engine, it is a fixed three-file
 * read with no SQL.
 */

import type { AsyncBuffer } from 'hyparquet';
import { parquetReadObjects } from 'hyparquet';
import { compressors } from 'hyparquet-compressors';

import type { NodeSummary } from '../../../types';
import type { RestProject } from '../mappers/fromWire';

/** The artifact holding the shell's node list. */
const NODES = 'dbt.nodes';

/**
 * The columns first paint decodes out of `dbt.nodes`.
 *
 * Mirrors `NODE_SUMMARY_COLUMNS` in the Rust crate. Parquet is columnar, so naming
 * them keeps the ~1 MB of `raw_code` / `compiled_code` from being decompressed here
 * even though it arrives in the same file.
 */
const NODE_SUMMARY_COLUMNS = [
  'unique_id',
  'name',
  'resource_type',
  'package_name',
  'materialized',
  'description',
  'database_name',
  'schema_name',
  'original_file_path',
];
/** Single-row conveniences: project identity and the ingest stamp. */
const PROJECT = 'dbt.project';
const GENERATION = 'dbt.generation';

export interface BootstrapData {
  /** Every node in the project, shaped exactly like the former `GET /api/v1/nodes`. */
  nodes: NodeSummary[];
  /** Project identity, or `null` when `dbt.project` is empty. */
  project: RestProject | null;
  /** When the index was ingested (RFC3339), or `null` if unstamped. */
  generation: string | null;
}

/**
 * Read the first-paint slice.
 *
 * Fetches the three artifacts in parallel and tolerates a missing `dbt.project` or
 * `dbt.generation` — both are single-row conveniences, and a site is still usable
 * without them. A missing or unreadable `dbt.nodes` is fatal: it is the shell.
 */
export async function readBootstrap(dataBaseUrl: string): Promise<BootstrapData> {
  const [nodes, project, generation] = await Promise.all([
    readRows(dataBaseUrl, NODES, NODE_SUMMARY_COLUMNS),
    readRows(dataBaseUrl, PROJECT).catch(() => []),
    readRows(dataBaseUrl, GENERATION).catch(() => []),
  ]);

  return {
    // The index's column names are already the `NodeSummary` field names, so this is
    // a cast rather than a mapping.
    nodes: nodes as unknown as NodeSummary[],
    project: toRestProject(project[0]),
    generation: asIsoString(generation[0]?.ingested_at) ?? null,
  };
}

/**
 * Shape a `dbt.project` row like `GET /api/v1/project` did.
 *
 * The one column that needs renaming: the table calls it `project_name`, and the
 * Rust handler aliased it to `name` in its `SELECT`. Reading the parquet directly
 * skips that alias, so it happens here instead — `fromProject` reads `name`, and
 * without this the project would silently render nameless.
 */
function toRestProject(row: Record<string, unknown> | undefined): RestProject | null {
  if (!row) return null;
  const { project_name: projectName, ...rest } = row;
  return {
    ...(rest as Omit<RestProject, 'name'>),
    name: typeof projectName === 'string' ? projectName : '',
  };
}

/** Read every row of one artifact. */
async function readRows(
  dataBaseUrl: string,
  table: string,
  columns?: string[],
): Promise<Record<string, unknown>[]> {
  const url = new URL(`${table}.parquet`, dataBaseUrl).href;
  const file = await wholeFile(url);
  return parquetReadObjects({
    file,
    // The index is written with ZSTD, which is not one of hyparquet's built-ins.
    compressors,
    rowFormat: 'object',
    ...(columns ? { columns } : {}),
  });
}

/**
 * Fetch a whole parquet file as an `AsyncBuffer`.
 *
 * Deliberately not `asyncBufferFromUrl`, which does a HEAD then byte-range reads.
 * These artifacts are small and read in full, so one request beats several — and
 * it keeps the bootstrap off range requests, which not every static host serves
 * reliably.
 */
async function wholeFile(url: string): Promise<AsyncBuffer> {
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText} fetching ${url}`);
  }
  const bytes = await res.arrayBuffer();
  return {
    byteLength: bytes.byteLength,
    slice: (start: number, end?: number) => bytes.slice(start, end),
  };
}

/**
 * Coerce a parquet timestamp to RFC3339.
 *
 * `ingested_at` is `TIMESTAMP(µs, UTC)`, which hyparquet may surface as a `Date`,
 * a number of milliseconds, or a `BigInt` of microseconds depending on how it
 * decodes the logical type. This is a staleness label, so an unrecognized shape
 * degrades to "unknown" rather than throwing.
 */
function asIsoString(value: unknown): string | null {
  if (value instanceof Date) return value.toISOString();
  if (typeof value === 'number') return new Date(value).toISOString();
  if (typeof value === 'bigint') return new Date(Number(value / 1000n)).toISOString();
  if (typeof value === 'string') return value;
  return null;
}
