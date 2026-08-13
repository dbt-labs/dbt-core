import { describe, expect, test } from 'vitest';

import { RyeconFile, RyeconModel } from '@dbt-labs/sourdough';

import { inferModelingLayer, ryeconForType } from './resourceType';

describe('ryeconForType', () => {
  test('returns the mapped ryecon for a known type', () => {
    expect(ryeconForType('model')).toBe(RyeconModel);
  });

  test('falls back to RyeconFile for an unknown type', () => {
    expect(ryeconForType('unknown_type')).toBe(RyeconFile);
  });
});

describe('inferModelingLayer', () => {
  test('returns null for null', () => {
    expect(inferModelingLayer(null)).toBeNull();
  });

  test('returns null for undefined', () => {
    expect(inferModelingLayer(undefined)).toBeNull();
  });

  test('returns null for an empty string', () => {
    expect(inferModelingLayer('')).toBeNull();
  });

  test('returns Staging for a path containing /staging/', () => {
    expect(inferModelingLayer('models/staging/stg_orders.sql')).toBe('Staging');
  });

  test('returns Staging for a path containing /stg_', () => {
    expect(inferModelingLayer('models/other/stg_orders.sql')).toBe('Staging');
  });

  test('returns Staging for a path starting with staging/', () => {
    expect(inferModelingLayer('staging/orders.sql')).toBe('Staging');
  });

  test('matches case-insensitively', () => {
    expect(inferModelingLayer('models/Staging/stg_orders.sql')).toBe('Staging');
  });

  test('returns Intermediate for a path containing /intermediate/', () => {
    expect(inferModelingLayer('models/intermediate/int_orders.sql')).toBe(
      'Intermediate',
    );
  });

  test('returns Intermediate for a path containing /int_', () => {
    expect(inferModelingLayer('models/other/int_orders.sql')).toBe('Intermediate');
  });

  test('returns Intermediate for a path starting with intermediate/', () => {
    expect(inferModelingLayer('intermediate/orders.sql')).toBe('Intermediate');
  });

  test('returns Marts for a path containing /marts/', () => {
    expect(inferModelingLayer('models/marts/dim_orders.sql')).toBe('Marts');
  });

  test('returns Marts for a path containing /dim_', () => {
    expect(inferModelingLayer('models/other/dim_orders.sql')).toBe('Marts');
  });

  test('returns Marts for a path containing /fct_', () => {
    expect(inferModelingLayer('models/other/fct_orders.sql')).toBe('Marts');
  });

  test('returns Marts for a path starting with marts/', () => {
    expect(inferModelingLayer('marts/orders.sql')).toBe('Marts');
  });

  test('returns null when nothing matches', () => {
    expect(inferModelingLayer('models/raw/orders.sql')).toBeNull();
  });
});
