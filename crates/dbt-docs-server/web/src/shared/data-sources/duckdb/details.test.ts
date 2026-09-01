import { describe, expect, it } from 'vitest';

import type { ResourceType } from '../../typings/domain/asset';
import { DETAIL_REGISTRY, detailSpecFor, nodeColumnsSql } from './details';

describe('the detail registry', () => {
  it('covers every type that had its own REST detail endpoint', () => {
    expect(Object.keys(DETAIL_REGISTRY).sort()).toEqual([
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

  it('falls back to the generic node path for types with no detail', () => {
    // `analysis`, `function` and `operation` were served that way under REST too.
    const spec = detailSpecFor('analysis');
    expect(spec).toBe(detailSpecFor('function'));
    // The fallback must not filter on resource_type, or it would match nothing.
    expect(spec.sql('analysis.a.b')).not.toContain("resource_type = ''");
    expect(spec.sql('analysis.a.b')).toContain('n.unique_id =');
  });

  it('scopes each query to its own resource type', () => {
    // Without it, a mistyped id of another type would render through the wrong mapper.
    expect(DETAIL_REGISTRY.model!.sql('model.a.b')).toContain(
      "n.resource_type = 'model'",
    );
    expect(DETAIL_REGISTRY.snapshot!.sql('snapshot.a.b')).toContain(
      "n.resource_type = 'snapshot'",
    );
  });

  it('only asks for columns on the types that have them', () => {
    // A macro or metric has no relation, so a columns query would always be empty.
    expect(DETAIL_REGISTRY.model!.wantsColumns).toBe(true);
    expect(DETAIL_REGISTRY.source!.wantsColumns).toBe(true);
    expect(DETAIL_REGISTRY.macro!.wantsColumns).toBe(false);
    expect(DETAIL_REGISTRY.metric!.wantsColumns).toBe(false);
  });

  it('declares the JSON-string columns each type needs parsed', () => {
    // The index stores these as VARCHAR; unparsed, an escaped JSON string reaches
    // the domain type (CC-7).
    expect(DETAIL_REGISTRY.model!.jsonColumns).toContain('meta');
    expect(DETAIL_REGISTRY.macro!.jsonColumns).toContain('arguments');
    expect(DETAIL_REGISTRY.metric!.jsonColumns).toContain('type_params');
    expect(DETAIL_REGISTRY.saved_query!.jsonColumns).toEqual([
      'query_params',
      'exports',
    ]);
  });

  it('escapes ids rather than breaking the query', () => {
    for (const type of Object.keys(DETAIL_REGISTRY) as ResourceType[]) {
      expect(DETAIL_REGISTRY[type]!.sql("model.a.o'brien")).toContain(
        "'model.a.o''brien'",
      );
    }
  });

  it('joins the test union explicitly rather than with USING', () => {
    // Regression: `USING (unique_id)` alongside the test_metadata join made the
    // reference ambiguous, so every test detail page failed to bind.
    const sql = DETAIL_REGISTRY.test!.sql('test.a.b');
    expect(sql).toContain('ON tm.unique_id = n.unique_id');
    expect(sql).not.toContain('USING (unique_id)');
  });

  it('keys semantic members on the parent id', () => {
    // Regression: these tables have no `semantic_model_unique_id`; members share the
    // parent's own `unique_id`, one row each.
    for (const extra of DETAIL_REGISTRY.semantic_model!.extras ?? []) {
      expect(extra.sql('semantic_model.a.b')).toContain('WHERE unique_id =');
      expect(extra.sql('semantic_model.a.b')).not.toContain('semantic_model_unique_id');
    }
  });

  it('matches group members on name and package, not id', () => {
    // Groups are keyed by (name, package); joining on unique_id would find nothing.
    const sql = DETAIL_REGISTRY.group!.extras![0]!.sql('group.a.b');
    expect(sql).toContain('g.name = n.group_name');
    expect(sql).toContain('g.package_name = n.package_name');
  });

  it('orders columns by their declared position', () => {
    // Column order is meaningful in a table; alphabetical would be wrong.
    expect(nodeColumnsSql('model.a.b')).toContain('ORDER BY column_index');
  });
});
