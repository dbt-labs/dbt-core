/** Gated feature surfaces. Only column-level lineage is gated today;
 *  more fields will land here as proprietary surfaces (sample data, AI,
 *  etc.) are added. Plain SQL-over-parquet reads (nodes, sources,
 *  exposures, metrics, run results) are always available and have no
 *  capability flag. */
export interface Capabilities {
  has_column_lineage: boolean;
  has_dbt_state?: boolean;
}

/** Metadata about the running docs-server distribution, returned by
 *  `GET /api/v1/distribution`. `name` identifies the build flavor —
 *  `"oss"` for the non-proprietary (dbt Core) build and `"dbt"` for the
 *  proprietary (Fusion) build. `is_logged_in` reflects whether the user
 *  has authenticated against the distribution. Together these let the FE
 *  separate "what binary" from "is the user authed", which the v1
 *  capabilities flag alone conflated. */
export interface DistInfo {
  name: string;
  version: string;
  is_logged_in: boolean;
}

/** Consent + identity gate for telemetry, returned by `GET /api/v1/identity`.
 *  Checked once on app load before any analytics SDK is initialised.
 *  `analytics_enabled` is the consent flag — telemetry init is gated on it
 *  (`!do_not_track && send_anonymous_usage_stats` server-side). `is_logged_in`
 *  is the V1 engagement-slice dimension carried on every subsequent event
 *  (authenticated vs anonymous). This is a distinct signal from
 *  {@link DistInfo}'s `is_logged_in`; the identity endpoint is authoritative
 *  for telemetry. */
export interface Identity {
  is_logged_in: boolean;
  analytics_enabled: boolean;
}

export interface NodeSummary {
  unique_id: string;
  name: string;
  resource_type: string;
  package_name?: string | null;
  materialized?: string | null;
  description?: string | null;
  database_name?: string | null;
  schema_name?: string | null;
  original_file_path?: string | null;
}

export interface NodeListResponse {
  nodes: NodeSummary[];
  total: number;
  offset: number;
  limit: number;
}

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

async function getJson<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, init);
  if (!res.ok)
    throw new ApiError(res.status, `${res.status} ${res.statusText} from ${path}`);
  return (await res.json()) as T;
}

/** POST `body` as JSON and parse the JSON response. Shares {@link ApiError}
 *  and the `init` escape hatch (e.g. `keepalive` for exit-time flushes) with
 *  {@link getJson}. */
async function postJson<T>(
  path: string,
  body: unknown,
  init?: RequestInit,
): Promise<T> {
  const res = await fetch(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    ...init,
  });
  if (!res.ok)
    throw new ApiError(res.status, `${res.status} ${res.statusText} from ${path}`);
  return (await res.json()) as T;
}

/**
 * Anonymised per-event context forwarded to the analytics relay
 * (`POST /api/v1/analytics/events`). Every field is optional and defaults to
 * `""` server-side. dbt-docs-v2 has no dbt Cloud account/project/environment
 * identity client-side (the identity endpoint returns only `is_logged_in` +
 * `analytics_enabled`), so in practice only `event_id` + `session_id` are set;
 * the `dbt_cloud_*` slots exist for wire-contract parity with other emitters.
 */
export interface AnalyticsEventContext {
  event_id?: string;
  session_id?: string;
  feature?: string;
  referrer_url?: string;
  dbt_cloud_account_id?: string;
  dbt_cloud_project_id?: string;
  dbt_cloud_environment_id?: string;
  dbt_cloud_user_id?: string;
}

/** Common envelope on every analytics event. `is_logged_in` is the one
 *  telemetry dimension carried on every event (authenticated vs anonymous). */
interface AnalyticsEventBase {
  is_logged_in: boolean;
  context?: AnalyticsEventContext;
}

export interface DocsSiteOpenedEvent extends AnalyticsEventBase {
  event_type: 'docs_site_opened';
  dbt_version: string;
  project_resource_count: number;
}

export interface ResourceViewedEvent extends AnalyticsEventBase {
  event_type: 'resource_viewed';
  resource_type: string;
  view_level: string;
  resource_id: string;
}

export interface LineageViewedEvent extends AnalyticsEventBase {
  event_type: 'lineage_viewed';
  lineage_type: string;
  resource_type: string;
  resource_id: string;
}

export interface SearchPerformedEvent extends AnalyticsEventBase {
  event_type: 'search_performed';
  search_query: string;
  result_count: number;
}

export interface UpsellPromptDisplayedEvent extends AnalyticsEventBase {
  event_type: 'upsell_prompt_displayed';
  upsell_track: string;
  prompt_format: string;
  prompt_location: string;
}

export interface UpsellPromptClickedEvent extends AnalyticsEventBase {
  event_type: 'upsell_prompt_clicked';
  upsell_track: string;
  cta_label: string;
  referral_code: string;
}

export interface UpsellPromptDismissedEvent extends AnalyticsEventBase {
  event_type: 'upsell_prompt_dismissed';
  upsell_track: string;
  dismiss_method: string;
}

export interface ReferralLinkClickedEvent extends AnalyticsEventBase {
  event_type: 'referral_link_clicked';
  referral_code: string;
  link_destination: string;
}

/** Discriminated union of every event the docs-v2 UI emits, keyed by the
 *  snake_case `event_type` tag the relay maps to a `docs.proto` wire type. */
export type AnalyticsEvent =
  | DocsSiteOpenedEvent
  | ResourceViewedEvent
  | LineageViewedEvent
  | SearchPerformedEvent
  | UpsellPromptDisplayedEvent
  | UpsellPromptClickedEvent
  | UpsellPromptDismissedEvent
  | ReferralLinkClickedEvent;

/** Batch request body for `POST /api/v1/analytics/events`. */
export interface AnalyticsBatchRequest {
  events: AnalyticsEvent[];
}

/** `202` response from the relay: how many events it forwarded vs dropped
 *  (consent denial surfaces as `{ accepted: 0, skipped: N }`, not an error). */
export interface AnalyticsRelayResponse {
  accepted: number;
  skipped: number;
}

/** Resource types accepted by `GET /api/v1/search`'s `type=` filter. Mirrors the
 *  backend allowlist in `dbt-docs-server/src/handlers/search.rs::ResourceType`.
 *  `analysis` is included here for future use; visibility is gated separately
 *  via `FEATURE_FLAGS.hasAnalysis` until BE adds `Analysis` to its enum. */
export const SEARCHABLE_RESOURCE_TYPES: ReadonlySet<string> = new Set([
  'model',
  'source',
  'seed',
  'snapshot',
  'test',
  'analysis',
  'unit_test',
  'exposure',
  'metric',
  'semantic_model',
  'saved_query',
  'macro',
  'group',
]);

/** Structured 400 envelope from `/api/v1/search`. The four documented codes —
 *  `invalid_type`, `invalid_modeling_layer`, `invalid_cursor`, `query_too_long` —
 *  are stable; new codes may be added. */
export type SearchErrorCode =
  'invalid_type' | 'invalid_modeling_layer' | 'invalid_cursor' | 'query_too_long';

/**
 * Local-only REST surface. Everything the UI fetches now flows through the
 * protocol-agnostic `MetadataDataSource` adapter in `src/shared/`;
 * `api.nodes` is the one remaining load-bearing exception — the whole-project
 * node index the shell preloads (render-gate, file tree, resource-type
 * resolver). `capabilities`/`distribution` are retained for parity with the
 * docs-server contract though the UI reads them via `useCapabilities` /
 * `useDistribution` on the adapter.
 */
export const api = {
  capabilities: () => getJson<Capabilities>('/api/v1/capabilities'),
  distribution: () => getJson<DistInfo>('/api/v1/distribution'),
  identity: () => getJson<Identity>('/api/v1/identity'),
  nodes: (
    params: {
      type?: string;
      package?: string;
      q?: string;
      limit?: number;
      offset?: number;
    } = {},
  ) => {
    const u = new URLSearchParams();
    if (params.type) u.set('type', params.type);
    if (params.package) u.set('package', params.package);
    if (params.q) u.set('q', params.q);
    if (params.limit != null) u.set('limit', String(params.limit));
    if (params.offset != null) u.set('offset', String(params.offset));
    const qs = u.toString();
    return getJson<NodeListResponse>(`/api/v1/nodes${qs ? `?${qs}` : ''}`);
  },
  /** Fire-and-forget analytics relay. Consent is enforced server-side; a
   *  `202 { accepted: 0, skipped: N }` on denial is a success, not an error.
   *  `init` lets callers pass `keepalive` for exit-time flushes. */
  analytics: (body: AnalyticsBatchRequest, init?: RequestInit) =>
    postJson<AnalyticsRelayResponse>('/api/v1/analytics/events', body, init),
};
