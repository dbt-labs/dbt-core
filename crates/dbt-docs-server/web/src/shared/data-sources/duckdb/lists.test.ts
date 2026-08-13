import { describe, expect, it } from 'vitest';

import type { ResourceType } from '../../typings/domain/asset';
import {
  buildListQuery,
  constantFacets,
  decodeCursor,
  encodeCursor,
  FACET_QUERIES,
  LIST_REGISTRY,
  supportedFilterFields,
  toPage,
} from './lists';

function queryFor(resourceType: ResourceType, extra = {}) {
  const query = buildListQuery({
    filter: { resourceTypes: [resourceType] },
    limit: 5,
    ...extra,
  });
  if (!query) throw new Error(`no list query for ${resourceType}`);
  return query;
}

describe('the list registry', () => {
  it('covers every type the REST source listed', () => {
    // Same set as the REST `REGISTRY`; `analysis`, `function` and `operation` had no
    // list endpoint there either.
    expect(Object.keys(LIST_REGISTRY).sort()).toEqual([
      'exposure',
      'group',
      'macro',
      'metric',
      'model',
      'saved_query',
      'seed',
      'semantic_model',
      'snapshot',
      'source',
      'test',
    ]);
  });

  it('returns null for a type with no list rather than inventing one', () => {
    expect(buildListQuery({ filter: { resourceTypes: ['analysis'] } })).toBeNull();
    expect(buildListQuery({ filter: {} })).toBeNull();
  });

  it('carries the same CTEs into the count as the page', () => {
    // Regression: the joins reference the CTEs, so a count without the `WITH` left a
    // dangling table reference and every model page failed to report a total.
    const query = queryFor('model');
    expect(query.sql).toContain('last_run AS (');
    expect(query.countSql).toContain('last_run AS (');
    expect(query.countSql).toContain('LEFT JOIN last_run');
  });

  it('asks only for the artifacts each type needs', () => {
    expect(queryFor('macro').tables).toEqual(['dbt.macros']);
    // No column lineage or code columns anywhere in a list.
    for (const type of Object.keys(LIST_REGISTRY) as ResourceType[]) {
      expect(queryFor(type).tables).not.toContain('dbt.column_lineage');
      expect(queryFor(type).tables).not.toContain('dbt.node_code');
    }
  });

  it('tie-breaks ordering on unique_id so a page is stable', () => {
    // Without it, two rows with equal sort values could swap between pages and one
    // would be shown twice while another vanished.
    for (const type of Object.keys(LIST_REGISTRY) as ResourceType[]) {
      expect(queryFor(type).sql).toMatch(/ORDER BY .+, [a-z]\.unique_id ASC/s);
    }
  });
});

describe('model filters', () => {
  it('builds the layer predicate from the same table as the projected column', () => {
    // One source of truth, so a filtered layer always matches the layer the row
    // displays — the reason the Rust side kept them in one table too.
    const query = queryFor('model', {
      filter: { resourceTypes: ['model'], modelingLayers: ['Marts'] },
    });
    expect(query.sql).toContain("THEN 'Marts'");
    expect(query.sql).toMatch(
      /AND \(\(lower\(n\.original_file_path\) LIKE '%\/marts\/%'/,
    );
    // Only the requested layer is filtered on.
    expect(query.sql).not.toMatch(
      /AND \(\(lower\(n\.original_file_path\) LIKE '%\/staging\/%'/,
    );
  });

  it('quotes filter values rather than interpolating them raw', () => {
    const query = queryFor('model', {
      filter: { resourceTypes: ['model'], packages: ["o'brien"] },
    });
    expect(query.sql).toContain("'o''brien'");
  });

  it('ignores an unsortable field instead of injecting it', () => {
    const query = queryFor('model', { sort: { field: 'not_a_column', desc: false } });
    expect(query.sql).not.toContain('not_a_column');
    expect(query.sql).toContain('ORDER BY n.name ASC');
  });

  it('honors an allowlisted sort', () => {
    const query = queryFor('model', { sort: { field: 'executed_at', desc: true } });
    expect(query.sql).toContain('ORDER BY CAST(lr.executed_at AS VARCHAR) DESC');
  });
});

describe('pagination', () => {
  it('round-trips an offset through an opaque cursor', () => {
    expect(decodeCursor(encodeCursor(150))).toBe(150);
    // Callers must not parse it, so it should not read as a number.
    expect(encodeCursor(150)).not.toBe('150');
  });

  it('treats a missing or malformed cursor as the first page', () => {
    expect(decodeCursor(null)).toBe(0);
    expect(decodeCursor(undefined)).toBe(0);
    expect(decodeCursor(btoa('-5'))).toBe(0);
  });

  it('stops offering a next cursor at the end of the results', () => {
    const query = queryFor('model');
    const rows = [{ unique_id: 'a' }, { unique_id: 'b' }];
    expect(toPage(rows, 10, query).nextCursor).not.toBeNull();
    expect(toPage(rows, 2, query).nextCursor).toBeNull();
  });

  it('reports the total so the UI can show a count', () => {
    expect(toPage([], 42, queryFor('model')).totalCount).toBe(42);
  });
});

describe('facets', () => {
  it('advertises exactly the filters some type honors', () => {
    const fields = supportedFilterFields();
    expect(fields.has('modelingLayers')).toBe(true);
    expect(fields.has('packages')).toBe(true);
    expect(fields.has('testTypes')).toBe(true);
    // Never honored by a list query, so advertising it would show a dead control.
    expect(fields.has('tags')).toBe(false);
  });

  it('serves closed sets as constants rather than querying them', () => {
    // These are fixed by the classification rules, not by the data.
    expect(constantFacets('model').modelingLayers?.map((f) => f.value)).toEqual([
      'Staging',
      'Intermediate',
      'Marts',
    ]);
    expect(constantFacets('test').testTypes?.map((f) => f.value)).toEqual([
      'data',
      'unit',
    ]);
    expect(constantFacets('macro')).toEqual({});
  });

  it('queries the open sets', () => {
    expect(FACET_QUERIES.model?.map((f) => f.key).sort()).toEqual([
      'materializations',
      'owners',
      'packages',
    ]);
  });
});

describe('summary mapping', () => {
  it('surfaces the model catalog stats', () => {
    // Regression: the nested keys were built without their `_stat` suffix, which
    // `fromModelSummary` reads — so row count, size and last-modified were silently
    // null on every model list. The SQL was right; only the assembly was wrong, which
    // is exactly what SQL-shape assertions cannot catch.
    const mapped = LIST_REGISTRY.model!.map({
      unique_id: 'model.pkg.big',
      name: 'big',
      package_name: 'pkg',
      has_catalog: true,
      row_count_stat: 1234567,
      bytes_stat: 4096,
      last_modified_stat: '2026-06-01T12:00:00Z',
    }) as { rowCountStat: number | null };

    expect(mapped.rowCountStat).toBe(1234567);
  });

  it('leaves the catalog null when the model has none', () => {
    const mapped = LIST_REGISTRY.model!.map({
      unique_id: 'model.pkg.plain',
      name: 'plain',
      package_name: 'pkg',
      has_catalog: false,
    }) as { rowCountStat: number | null };

    expect(mapped.rowCountStat).toBeNull();
  });
});
