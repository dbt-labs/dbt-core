import { describe, expect, test } from 'vitest';

import type { Capabilities, Distribution } from '../shared';
import { deriveUpgradeCapabilities } from './useCapabilities';

const cap = (overrides: Partial<Capabilities> = {}): Capabilities => ({
  hasColumnLineage: false,
  hasQueryHistory: false,
  hasCostInsights: false,
  hasPerformance: false,
  hasRecommendations: false,
  hasHealthSignals: false,
  hasAutoExposures: false,
  hasMultiProject: false,
  hasMesh: false,
  hasRunResults: false,
  hasCatalogStats: false,
  hasDbtState: false,
  ...overrides,
});

const dist = (isFusion: boolean, isLoggedIn: boolean): Distribution => ({
  isFusion,
  isLoggedIn,
  version: '0.0.0',
});

describe('deriveUpgradeCapabilities', () => {
  test('returns null while capabilities are still loading', () => {
    expect(deriveUpgradeCapabilities(null, dist(false, false))).toBeNull();
  });

  test('returns null while distribution is still loading', () => {
    expect(deriveUpgradeCapabilities(cap(), null)).toBeNull();
  });

  test('core distribution → core flags', () => {
    expect(deriveUpgradeCapabilities(cap(), dist(false, false))).toEqual({
      hasCll: false,
      hasDbtState: false,
      isFusion: false,
      isLoggedIn: false,
    });
  });

  test('fusion + logged in + CLL → fully unlocked flags', () => {
    expect(
      deriveUpgradeCapabilities(cap({ hasColumnLineage: true }), dist(true, true)),
    ).toEqual({
      hasCll: true,
      hasDbtState: false,
      isFusion: true,
      isLoggedIn: true,
    });
  });

  test('fusion + not logged in → fusion but anon, CLL off', () => {
    expect(deriveUpgradeCapabilities(cap(), dist(true, false))).toEqual({
      hasCll: false,
      hasDbtState: false,
      isFusion: true,
      isLoggedIn: false,
    });
  });

  test('hasDbtState passthrough', () => {
    expect(
      deriveUpgradeCapabilities(cap({ hasDbtState: true }), dist(true, true))
        ?.hasDbtState,
    ).toBe(true);
  });
});
