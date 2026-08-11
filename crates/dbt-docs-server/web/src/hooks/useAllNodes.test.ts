import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { NodeListResponse, NodeSummary } from '../api';
import { createQueryWrapper } from '../test/renderWithProviders';
import { useAllNodes } from './useAllNodes';

const node = (id: string): NodeSummary => ({
  unique_id: `seed.pkg.${id}`,
  name: id,
  resource_type: 'seed',
  package_name: 'pkg',
  description: null,
});

function jsonResponse(data: NodeListResponse) {
  return { ok: true, json: () => Promise.resolve(data) };
}

describe('useAllNodes', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('auto-pages progressively until the full list lands', async () => {
    const page1: NodeListResponse = {
      nodes: [node('a')],
      total: 2,
      offset: 0,
      limit: 1000,
    };
    const page2: NodeListResponse = {
      nodes: [node('b')],
      total: 2,
      offset: 1,
      limit: 1000,
    };
    let calls = 0;
    vi.stubGlobal(
      'fetch',
      vi.fn(() => {
        calls += 1;
        return Promise.resolve(jsonResponse(calls === 1 ? page1 : page2));
      }),
    );

    const { result } = renderHook(() => useAllNodes(), {
      wrapper: createQueryWrapper(),
    });

    await waitFor(() => expect(result.current.nodes).toHaveLength(2));
    expect(result.current.total).toBe(2);
    expect(result.current.error).toBeNull();
  });

  it('stops auto-paging when a mid-pagination page fails permanently', async () => {
    const page1: NodeListResponse = {
      nodes: [node('a')],
      total: 2,
      offset: 0,
      limit: 1000,
    };
    const fetchMock = vi.fn(() => {
      // First call serves page 1; every subsequent page rejects forever.
      const call = fetchMock.mock.calls.length;
      return call === 1
        ? Promise.resolve(jsonResponse(page1))
        : Promise.reject(new Error('boom'));
    });
    vi.stubGlobal('fetch', fetchMock);

    const { result } = renderHook(() => useAllNodes(), {
      wrapper: createQueryWrapper(),
    });

    await waitFor(() => expect(result.current.error).not.toBeNull());

    // The effect must NOT keep re-firing fetchNextPage after the failure.
    const callsAtFailure = fetchMock.mock.calls.length;
    await new Promise((r) => setTimeout(r, 50));
    expect(fetchMock.mock.calls.length).toBe(callsAtFailure);
    // Bounded: page-1 success + the single failed page (retry off in tests).
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(result.current.nodes).toHaveLength(1);
  });
});
