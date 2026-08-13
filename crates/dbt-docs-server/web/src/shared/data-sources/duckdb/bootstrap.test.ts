import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { readBootstrap } from './bootstrap';

/**
 * These read real parquet, not a mock.
 *
 * The fixtures are written by the DuckDB CLI with the same
 * `(FORMAT PARQUET, COMPRESSION ZSTD)` the exporter uses, because that is the
 * risky part: hyparquet has no built-in ZSTD, so the decode goes through
 * `hyparquet-compressors`. A mocked reader would prove nothing about whether the
 * browser can actually open what `--write-index` writes.
 *
 * `dbt.nodes.parquet` carries `raw_code` / `compiled_code` like the real index does,
 * so the column projection first paint relies on is genuinely exercised.
 */
const FIXTURES = join(__dirname, '../../../test/fixtures/parquet');

/** Serve fixture files for the requested parquet, 404 for anything else. */
function stubFetch(available: string[] = ['dbt.nodes', 'dbt.project']) {
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL) => {
      const url = typeof input === 'string' ? input : input.toString();
      const table = url.split('/').pop()?.replace('.parquet', '') ?? '';
      if (!available.includes(table)) {
        return Promise.resolve(new Response(null, { status: 404 }));
      }
      const bytes = readFileSync(join(FIXTURES, `${table}.parquet`));
      return Promise.resolve(
        new Response(new Uint8Array(bytes), {
          status: 200,
          headers: { 'content-type': 'application/octet-stream' },
        }),
      );
    }),
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('readBootstrap', () => {
  it('reads zstd parquet the index holds', async () => {
    stubFetch();
    const { nodes } = await readBootstrap('https://host/site/data/');

    expect(nodes).toHaveLength(3);
    expect(nodes[0]).toMatchObject({
      unique_id: 'model.jaffle.orders',
      name: 'orders',
      resource_type: 'model',
      package_name: 'jaffle',
      materialized: 'view',
      description: 'All orders',
      database_name: 'analytics',
      schema_name: 'main',
      original_file_path: 'models/orders.sql',
    });
  });

  it('projects only the NodeSummary columns, leaving the code behind', async () => {
    stubFetch();
    const { nodes } = await readBootstrap('https://host/site/data/');
    // The index's `dbt.nodes` carries `raw_code` / `compiled_code`; first paint asks
    // for nine columns by name so roughly a megabyte of code is never decoded. The
    // column names are already the domain field names, so nothing is remapped.
    expect(Object.keys(nodes[0]).sort()).toEqual([
      'database_name',
      'description',
      'materialized',
      'name',
      'original_file_path',
      'package_name',
      'resource_type',
      'schema_name',
      'unique_id',
    ]);
  });

  it('preserves nulls rather than coercing them', async () => {
    stubFetch();
    const { nodes } = await readBootstrap('https://host/site/data/');
    const customers = nodes.find((n) => n.name === 'customers');
    expect(customers?.description).toBeNull();
  });

  it('reads project identity, renaming project_name to name', async () => {
    stubFetch();
    const { project } = await readBootstrap('https://host/site/data/');
    // The Rust handler aliased `project_name AS name`; reading the parquet
    // directly skips that, so the bootstrap has to do it or the project renders
    // nameless.
    expect(project).toMatchObject({
      name: 'jaffle_shop',
      dbt_version: '2.0.0',
      adapter_type: 'duckdb',
      git_branch: 'main',
      git_is_dirty: false,
    });
    expect(project).not.toHaveProperty('project_name');
  });

  it('resolves artifact URLs against the data base, so subpath hosting works', async () => {
    stubFetch();
    await readBootstrap('https://host/group/project/data/');
    const calls = (fetch as unknown as { mock: { calls: unknown[][] } }).mock.calls;
    const urls = calls.map((c) => String(c[0]));
    expect(urls).toContain('https://host/group/project/data/dbt.nodes.parquet');
  });

  it('tolerates a missing project or generation artifact', async () => {
    // Both are single-row conveniences; a site is still usable without them.
    stubFetch(['dbt.nodes']);
    const result = await readBootstrap('https://host/site/data/');
    expect(result.nodes).toHaveLength(3);
    expect(result.project).toBeNull();
    expect(result.generation).toBeNull();
  });

  it('fails when the node index is missing, because that is the shell', async () => {
    stubFetch([]);
    await expect(readBootstrap('https://host/site/data/')).rejects.toThrow(/404/);
  });
});
