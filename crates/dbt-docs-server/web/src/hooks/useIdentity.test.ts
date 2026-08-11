import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { Identity } from '../api';
import { createQueryWrapper } from '../test/renderWithProviders';
import { useIdentity } from './useIdentity';

function jsonResponse(data: Identity) {
  return { ok: true, json: () => Promise.resolve(data) };
}

describe('useIdentity', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('resolves to the identity payload when analytics is enabled', async () => {
    const identity: Identity = { is_logged_in: true, analytics_enabled: true };
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.resolve(jsonResponse(identity))),
    );

    const { result } = renderHook(() => useIdentity(), {
      wrapper: createQueryWrapper(),
    });

    await waitFor(() => expect(result.current.data).toEqual(identity));
  });

  it('passes through analytics_enabled: false', async () => {
    const identity: Identity = { is_logged_in: true, analytics_enabled: false };
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.resolve(jsonResponse(identity))),
    );

    const { result } = renderHook(() => useIdentity(), {
      wrapper: createQueryWrapper(),
    });

    await waitFor(() => expect(result.current.data).toEqual(identity));
  });

  it('fails closed to consent-denied on a non-200 response, warning to console', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    vi.stubGlobal(
      'fetch',
      vi.fn(() =>
        Promise.resolve(new Response('nope', { status: 500, statusText: 'boom' })),
      ),
    );

    const { result } = renderHook(() => useIdentity(), {
      wrapper: createQueryWrapper(),
    });

    await waitFor(() =>
      expect(result.current.data).toEqual({
        is_logged_in: false,
        analytics_enabled: false,
      }),
    );
    expect(warn).toHaveBeenCalled();
    warn.mockRestore();
  });

  it('fails closed to consent-denied on a network error', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.reject(new Error('network down'))),
    );

    const { result } = renderHook(() => useIdentity(), {
      wrapper: createQueryWrapper(),
    });

    await waitFor(() =>
      expect(result.current.data).toEqual({
        is_logged_in: false,
        analytics_enabled: false,
      }),
    );
    expect(warn).toHaveBeenCalled();
    warn.mockRestore();
  });
});
