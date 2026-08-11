import { useQuery, type UseQueryResult } from '@tanstack/react-query';

import { api, type Identity } from '../api';

/** Consent denied. Any failure to resolve identity resolves to this — telemetry
 *  never fails open. */
const CONSENT_DENIED: Identity = { is_logged_in: false, analytics_enabled: false };

/**
 * Fetch the telemetry consent gate from `GET /api/v1/identity`, once per
 * session. The query always resolves to an {@link Identity}: any failure
 * (non-200, network error, timeout) is caught and mapped to {@link
 * CONSENT_DENIED} with a console warning, so consent never fails open and
 * callers can gate telemetry on `data` without handling an error branch.
 */
export function useIdentity(): UseQueryResult<Identity> {
  return useQuery({
    queryKey: ['identity'],
    queryFn: async (): Promise<Identity> => {
      try {
        return await api.identity();
      } catch (error) {
        // eslint-disable-next-line no-console
        console.warn(
          '[telemetry] identity check failed; disabling analytics for this session',
          error,
        );
        return CONSENT_DENIED;
      }
    },
    // Consent doesn't change within a session, and the queryFn already resolves
    // (never throws), so retries add nothing.
    retry: false,
    staleTime: Infinity,
  });
}
