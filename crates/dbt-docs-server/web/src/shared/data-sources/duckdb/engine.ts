/**
 * DuckDB-WASM over the site's parquet artifacts.
 *
 * The engine is fetched from a CDN at runtime and is never bundled: it is ~6.8 MB
 * brotli, it is versioned independently of this app, and jsDelivr serves it
 * `immutable` so it is cached once per visitor. Loading is lazy — nothing here
 * touches the network until the first query — which is what lets the shell paint
 * from the hyparquet bootstrap while this streams in behind it.
 *
 * Artifacts are fetched whole and handed to `registerFileBuffer` rather than read
 * over HTTP range requests. The whole index for a 6,472-node project is under
 * 5 MB, so ranges buy nothing, and they are actively hostile here: GitLab Pages
 * only serves them when artifacts are stored uncompressed, and duckdb-wasm has a
 * known Firefox × range × compression failure on GitHub Pages. Registration is
 * per-artifact and on demand, so a cold load never pulls column lineage.
 */

import type * as duckdb from '@duckdb/duckdb-wasm';

/**
 * Empty relations for index tables that may legitimately have no parquet.
 *
 * The index writes a table only when it has rows, so these four are absent from a
 * project that has not run, has no sources, or has no catalog. Every query reads them
 * through a `LEFT JOIN`, so declaring a zero-row relation with the right columns gives
 * exactly the intended result — nulls — and keeps one version of each query instead of
 * a present/absent pair.
 *
 * Column types mirror the row structs in `dbt-index-core`'s `parquet.rs`
 * (`RunResultRow`, `SourceFreshnessRow`, `CatalogStatRow`, `CatalogTableRow`). Only
 * the columns the queries actually read are declared; a column added to a query needs
 * adding here too, and DuckDB will say so loudly if it is missed.
 */
const EMPTY_RELATION_DDL: Partial<Record<TableName, string>> = {
  'dbt_rt.run_results':
    'CREATE OR REPLACE TABLE "dbt_rt"."run_results" ' +
    '(unique_id VARCHAR, created_at TIMESTAMP, status VARCHAR, message VARCHAR)',
  'dbt.source_freshness':
    'CREATE OR REPLACE TABLE "dbt"."source_freshness" ' +
    '(unique_id VARCHAR, status VARCHAR, max_loaded_at TIMESTAMP, snapshotted_at TIMESTAMP)',
  'dbt.catalog_stats':
    'CREATE OR REPLACE TABLE "dbt"."catalog_stats" ' +
    '(unique_id VARCHAR, stat_id VARCHAR, stat_value VARCHAR)',
  'dbt.catalog_tables':
    'CREATE OR REPLACE TABLE "dbt"."catalog_tables" (unique_id VARCHAR)',
};

/** Logical table name, e.g. `dbt.nodes`. Matches the artifact's file stem. */
export type TableName = string;

/**
 * Whether `bytes` are actually a parquet file.
 *
 * Checked before registering, because "the artifact is absent" does not reliably
 * arrive as a 404. A host that answers unknown paths with `index.html` — which
 * `dbt docs serve` does, and which most SPA hosts do by default — returns 200 and a
 * document, and registering that leaves DuckDB to fail on it with `No magic bytes
 * found at end of file`. The bytes are the only thing every host agrees on, so they
 * decide, and an expected empty relation stays an empty relation rather than
 * becoming a page that renders nothing.
 *
 * Parquet brackets the file with `PAR1` at both ends. The trailing copy is the one
 * that matters here — it is what DuckDB reads first, and what a truncated or
 * substituted file loses — but both are checked, since a document that happens to
 * end in `PAR1` is no more readable than one that does not. 12 bytes is the smallest
 * possible file: two magics plus the 4-byte footer length between them.
 */
export function isParquetBytes(bytes: Uint8Array): boolean {
  if (bytes.byteLength < 12) return false;
  const magic = [0x50, 0x41, 0x52, 0x31]; // 'PAR1'
  return magic.every(
    (byte, i) =>
      bytes[i] === byte && bytes[bytes.byteLength - magic.length + i] === byte,
  );
}

export interface EngineOptions {
  /** Absolute URL of the `data/` directory, with a trailing slash. */
  dataBaseUrl: string;
  /**
   * Package root the engine is loaded from, e.g.
   * `https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.32.0`.
   *
   * A mirror must serve the same layout: `/+esm` for the ESM entry and
   * `/dist/*.wasm` plus `/dist/*.worker.js` for the bundles.
   */
  cdnBase: string;
}

export interface DuckDbEngine {
  /**
   * Register `tables` if they are not already, then run `sql`.
   *
   * Rows come back as plain objects with the parquet's own snake_case column
   * names, so the existing `fromRest` mappers consume them unchanged.
   */
  query<T = Record<string, unknown>>(sql: string, tables: TableName[]): Promise<T[]>;
  /** Whether `table`'s artifact exists. Only meaningful after a `query` that asked for it. */
  hasTable(table: TableName): boolean;
  /** Resolve once the engine is usable. Callers that only need readiness, not a query. */
  ready(): Promise<void>;
}

interface Loaded {
  db: duckdb.AsyncDuckDB;
  conn: duckdb.AsyncDuckDBConnection;
}

export function createEngine(options: EngineOptions): DuckDbEngine {
  // Single-flight: many hooks fire at once on first paint and must share one
  // engine, one worker, and one fetch per artifact.
  let loading: Promise<Loaded> | null = null;
  const registered = new Map<TableName, Promise<boolean>>();
  const present = new Set<TableName>();

  async function load(): Promise<Loaded> {
    // `@vite-ignore` keeps Rollup from trying to resolve and inline the CDN URL,
    // which is the whole point: the wasm must not end up in `web/dist/`.
    const wasm: typeof duckdb = await import(
      /* @vite-ignore */ `${options.cdnBase}/+esm`
    );

    // Build the bundle map from `cdnBase` rather than calling
    // `getJsDelivrBundles()`, whose URLs are hardcoded to jsDelivr — otherwise
    // `--duckdb-cdn-base` would move the loader but not the wasm it fetches.
    // Only `mvp` and `eh` are offered, deliberately: the `coi` bundle needs
    // COOP/COEP cross-origin-isolation headers, which a static host generally
    // cannot set.
    const bundle = await wasm.selectBundle({
      mvp: {
        mainModule: `${options.cdnBase}/dist/duckdb-mvp.wasm`,
        mainWorker: `${options.cdnBase}/dist/duckdb-browser-mvp.worker.js`,
      },
      eh: {
        mainModule: `${options.cdnBase}/dist/duckdb-eh.wasm`,
        mainWorker: `${options.cdnBase}/dist/duckdb-browser-eh.worker.js`,
      },
    });

    if (!bundle.mainWorker) {
      throw new Error('duckdb-wasm returned a bundle with no worker entry');
    }

    // The worker script is cross-origin, so it cannot be a `new Worker(url)`
    // directly. Wrapping it in a same-origin blob that `importScripts` the real
    // one is duckdb-wasm's own documented workaround.
    const workerUrl = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' }),
    );
    try {
      const worker = new Worker(workerUrl);
      const db = new wasm.AsyncDuckDB(
        new wasm.ConsoleLogger(wasm.LogLevel.WARNING),
        worker,
      );
      await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
      const conn = await db.connect();
      // The artifact set uses two schemas, mirroring the index's own layout.
      await conn.query('CREATE SCHEMA IF NOT EXISTS dbt');
      await conn.query('CREATE SCHEMA IF NOT EXISTS dbt_rt');
      return { db, conn };
    } finally {
      URL.revokeObjectURL(workerUrl);
    }
  }

  function loaded(): Promise<Loaded> {
    loading ??= load();
    return loading;
  }

  /**
   * Fetch one artifact and create its view.
   *
   * Resolves `false` when the artifact is absent, which is not an error. The index
   * only writes a table that has rows, so a project that has never run has no
   * `dbt_rt.run_results`, one with no sources has no `dbt.source_freshness`, and so
   * on. Where a query only reads such a table through a `LEFT JOIN`, an empty
   * relation is the right answer and {@link EMPTY_RELATION_DDL} supplies one, so the
   * SQL needs no missing-table variant. Column lineage is the exception: it has no
   * DDL here because its absence is the capability signal itself.
   *
   * Absence is decided by {@link isParquetBytes} rather than by the status code: a
   * host that rewrites unknown paths to `index.html` reports a missing artifact as
   * 200 and a document, so a 404 is one way absence arrives and not the only one.
   * A status that is neither — a 500, a timeout — is still an error and still
   * throws; those are worth surfacing.
   */
  function register(table: TableName): Promise<boolean> {
    const existing = registered.get(table);
    if (existing) return existing;

    const attempt = (async () => {
      const { db, conn } = await loaded();
      const fileName = `${table}.parquet`;
      const res = await fetch(new URL(fileName, options.dataBaseUrl).href);
      if (!res.ok && res.status !== 404) {
        throw new Error(`${res.status} ${res.statusText} fetching ${fileName}`);
      }

      const bytes = res.ok ? new Uint8Array(await res.arrayBuffer()) : null;
      if (!bytes || !isParquetBytes(bytes)) {
        const ddl = EMPTY_RELATION_DDL[table];
        if (!ddl) return false;
        // Nothing to register: the relation is declared straight into the catalog.
        await conn.query(ddl);
        present.add(table);
        return true;
      }

      await db.registerFileBuffer(fileName, bytes);
      // Quoted so the `.` in `dbt.nodes` reads as schema.table rather than being
      // taken from the file name, which contains one too.
      const [schema, name] = splitTableName(table);
      await conn.query(
        `CREATE OR REPLACE VIEW "${schema}"."${name}" AS SELECT * FROM read_parquet('${fileName}')`,
      );
      present.add(table);
      return true;
    })();

    registered.set(table, attempt);
    return attempt;
  }

  return {
    async query<T>(sql: string, tables: TableName[]): Promise<T[]> {
      const { conn } = await loaded();
      await Promise.all(tables.map(register));
      const result = await conn.query(sql);
      return result.toArray().map((row) => normalizeRow(row.toJSON())) as T[];
    },

    hasTable(table: TableName): boolean {
      return present.has(table);
    },

    async ready(): Promise<void> {
      await loaded();
    },
  };
}

/** `dbt.nodes` → `['dbt', 'nodes']`; an unqualified name lands in `dbt`. */
function splitTableName(table: TableName): [string, string] {
  const idx = table.indexOf('.');
  if (idx === -1) return ['dbt', table];
  return [table.slice(0, idx), table.slice(idx + 1)];
}

/**
 * Make one Arrow row consumable by the `fromRest` mappers.
 *
 * DuckDB returns `BIGINT` as `BigInt`, which `JSON.stringify` throws on and which
 * compares unequal to a number literal. Counts and row totals are all well within
 * `Number`'s safe range here, so widening is lossless in practice. Nested values
 * (`LIST`, `STRUCT`) arrive as Arrow vectors; flattening them to plain arrays and
 * objects is what keeps the mappers protocol-agnostic.
 */
function normalizeRow(row: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(row)) {
    out[key] = normalizeValue(value);
  }
  return out;
}

function normalizeValue(value: unknown): unknown {
  if (typeof value === 'bigint') return Number(value);
  if (value === null || value === undefined) return null;
  if (Array.isArray(value)) return value.map(normalizeValue);
  // Arrow vectors and struct proxies both expose `toJSON`.
  if (
    typeof value === 'object' &&
    'toJSON' in value &&
    typeof value.toJSON === 'function'
  ) {
    return normalizeValue((value as { toJSON(): unknown }).toJSON());
  }
  return value;
}
