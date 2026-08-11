import { QueryClient } from '@tanstack/react-query';

import { ApiError } from './api';

/** Shared QueryClient. Mirrors the defaults used by the sibling
 *  `dbt-explorer` package: never refetch on window focus, and retry up to
 *  three times — but never retry a 404, since a missing resource won't
 *  appear on a later attempt. */
export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      retry: (numFailures, error) =>
        error instanceof ApiError && error.status === 404 ? false : numFailures < 3,
    },
  },
});
