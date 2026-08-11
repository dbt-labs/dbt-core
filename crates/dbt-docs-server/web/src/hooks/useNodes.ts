import { useInfiniteQuery } from '@tanstack/react-query';

import { api, type NodeSummary } from '../api';

const PAGE_SIZE = 50;

export interface NodeFilters {
  package?: string;
}

export interface UseNodesResult {
  nodes: NodeSummary[];
  total: number | null;
  isPending: boolean;
  isFetchingNextPage: boolean;
  hasNextPage: boolean;
  error: Error | null;
  /** Pre-formatted message for the list table; null when there's no error. */
  errorMessage: string | null;
  fetchNextPage(): void;
}

export function useNodes(
  resourceType: string,
  filters: NodeFilters = {},
): UseNodesResult {
  const query = useInfiniteQuery({
    queryKey: ['nodes', { type: resourceType, package: filters.package ?? null }],
    queryFn: ({ pageParam }) =>
      api.nodes({
        type: resourceType,
        package: filters.package,
        limit: PAGE_SIZE,
        offset: pageParam,
      }),
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

  return {
    nodes: query.data?.nodes ?? [],
    total: query.data?.total ?? null,
    isPending: query.isPending,
    isFetchingNextPage: query.isFetchingNextPage,
    hasNextPage: query.hasNextPage,
    error: query.error,
    errorMessage: query.error ? `Failed to load ${resourceType}s.` : null,
    fetchNextPage: query.fetchNextPage,
  };
}
