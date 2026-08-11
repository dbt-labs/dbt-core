import { useEffect } from 'react';
import { useInfiniteQuery } from '@tanstack/react-query';

import { api, type NodeSummary } from '../api';

const NODE_PAGE_SIZE = 1000;

export interface UseAllNodesResult {
  /** Null until the first page lands; grows progressively as pages auto-fetch. */
  nodes: NodeSummary[] | null;
  total: number | null;
  isPending: boolean;
  error: Error | null;
}

/**
 * Load every node in the project, paging progressively. The effect auto-fetches
 * the next page as soon as the previous one lands, so `nodes.length` / `total`
 * gives the LocatePane a live progress signal while the full list streams in.
 */
export function useAllNodes(): UseAllNodesResult {
  const query = useInfiniteQuery({
    queryKey: ['allNodes'],
    queryFn: ({ pageParam }) => api.nodes({ limit: NODE_PAGE_SIZE, offset: pageParam }),
    initialPageParam: 0,
    getNextPageParam: (lastPage, allPages) => {
      const loaded = allPages.reduce((n, p) => n + p.nodes.length, 0);
      if (lastPage.nodes.length === 0) return undefined;
      return loaded < lastPage.total ? loaded : undefined;
    },
    select: (data) => ({
      nodes: data.pages.flatMap((p) => p.nodes),
      total: data.pages[0]?.total ?? null,
    }),
  });

  const { hasNextPage, isFetchingNextPage, fetchNextPage, isError } = query;
  useEffect(() => {
    if (hasNextPage && !isFetchingNextPage && !isError) void fetchNextPage();
  }, [hasNextPage, isFetchingNextPage, fetchNextPage, isError]);

  return {
    nodes: query.data?.nodes ?? null,
    total: query.data?.total ?? null,
    isPending: query.isPending,
    error: query.error,
  };
}
