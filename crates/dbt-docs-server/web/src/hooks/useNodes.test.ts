import { act, renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { NodeListResponse } from '../api';
import { createQueryWrapper } from '../test/renderWithProviders';
import { useNodes } from './useNodes';

function makeResponse(data: NodeListResponse) {
  return vi.fn(() => Promise.resolve({ ok: true, json: () => Promise.resolve(data) }));
}

const singleNode: NodeListResponse = {
  nodes: [
    {
      unique_id: 'seed.pkg.a',
      name: 'a',
      resource_type: 'seed',
      package_name: 'pkg',
      description: null,
    },
  ],
  total: 1,
  offset: 0,
  limit: 50,
};

describe('useNodes', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('loads first page on mount', async () => {
    vi.stubGlobal('fetch', makeResponse(singleNode));
    const { result } = renderHook(() => useNodes('seed'), {
      wrapper: createQueryWrapper(),
    });
    await waitFor(() => expect(result.current.isPending).toBe(false));
    expect(result.current.nodes).toHaveLength(1);
    expect(result.current.error).toBeNull();
  });

  it('resets nodes when filter changes', async () => {
    vi.stubGlobal('fetch', makeResponse(singleNode));
    const { result, rerender } = renderHook(
      ({ pkg }: { pkg?: string }) => useNodes('seed', { package: pkg }),
      {
        initialProps: { pkg: undefined as string | undefined },
        wrapper: createQueryWrapper(),
      },
    );
    await waitFor(() => expect(result.current.isPending).toBe(false));
    expect(result.current.nodes).toHaveLength(1);

    const emptyResponse: NodeListResponse = {
      nodes: [],
      total: 0,
      offset: 0,
      limit: 50,
    };
    vi.stubGlobal('fetch', makeResponse(emptyResponse));
    rerender({ pkg: 'other_pkg' });

    await waitFor(() => expect(result.current.nodes).toHaveLength(0));
  });

  it('sets error state on fetch failure', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.reject(new Error('network'))),
    );
    const { result } = renderHook(() => useNodes('seed'), {
      wrapper: createQueryWrapper(),
    });
    await waitFor(() => expect(result.current.error).not.toBeNull());
    expect(result.current.nodes).toHaveLength(0);
    expect(result.current.errorMessage).toBe('Failed to load seeds.');
  });

  it('sets error on fetchNextPage failure', async () => {
    const page1: NodeListResponse = {
      nodes: [
        {
          unique_id: 'seed.pkg.a',
          name: 'a',
          resource_type: 'seed',
          package_name: 'pkg',
          description: null,
        },
      ],
      total: 2,
      offset: 0,
      limit: 50,
    };
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.resolve({ ok: true, json: () => Promise.resolve(page1) })),
    );
    const { result } = renderHook(() => useNodes('seed'), {
      wrapper: createQueryWrapper(),
    });
    await waitFor(() => expect(result.current.isPending).toBe(false));

    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.reject(new Error('network'))),
    );
    act(() => {
      void result.current.fetchNextPage();
    });
    await waitFor(() => expect(result.current.error).not.toBeNull());
    expect(result.current.errorMessage).toBe('Failed to load seeds.');
  });
});
