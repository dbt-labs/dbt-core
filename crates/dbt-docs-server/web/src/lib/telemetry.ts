import { useSyncExternalStore } from 'react';

import {
  type AnalyticsEvent,
  api,
  type DocsSiteOpenedEvent,
  type Identity,
  type LineageViewedEvent,
  type ReferralLinkClickedEvent,
  type ResourceViewedEvent,
  type SearchPerformedEvent,
  type UpsellPromptClickedEvent,
  type UpsellPromptDismissedEvent,
  type UpsellPromptDisplayedEvent,
} from '../api';

/** Base properties attached to every telemetry event once analytics is
 *  initialised. `isLoggedIn` is the V1 engagement-slice dimension (events are
 *  split by authenticated vs anonymous users). */
export interface TelemetryContext {
  isLoggedIn: boolean;
}

/** Distributive `Omit` — keeps the discriminated union intact instead of
 *  collapsing it to its common keys the way a plain `Omit<Union, K>` would. */
type DistributiveOmit<T, K extends keyof T> = T extends unknown ? Omit<T, K> : never;

/** An event as supplied by a call site: the wire {@link AnalyticsEvent} minus
 *  the fields {@link track} injects (`is_logged_in` + anonymised `context`). */
export type TrackableEvent = DistributiveOmit<
  AnalyticsEvent,
  'is_logged_in' | 'context'
>;

/** Endpoint the relay listens on. Mirrors `api.analytics`'s path so the
 *  exit-time `sendBeacon` fallback can target it directly. */
const ANALYTICS_PATH = '/api/v1/analytics/events';

/** Debounce before a queued batch is POSTed. Mirrors the server worker's
 *  flush cadence so we don't out-pace it. */
const FLUSH_DELAY_MS = 500;

let initialized = false;
let context: TelemetryContext | null = null;
let sessionId: string | null = null;
let queue: AnalyticsEvent[] = [];
let flushTimer: ReturnType<typeof setTimeout> | null = null;
let exitListener: (() => void) | null = null;

/** Subscribers notified when the `initialized` flag flips, so React consumers
 *  can re-render once consent resolves (init happens in a post-render effect). */
const initListeners = new Set<() => void>();

function notifyInitialized(): void {
  initListeners.forEach((listener) => listener());
}

/**
 * Consent-gated telemetry bootstrap. Call once, on app load, with the resolved
 * {@link Identity} from `GET /api/v1/identity`.
 *
 * If `analytics_enabled` is not `true` this is a no-op: no session is started,
 * no events are queued, and no network calls to any analytics endpoint are
 * made. Consent fails closed — the caller must treat an identity-check failure
 * as `analytics_enabled: false` (see {@link useIdentity}) so a failed check
 * never enables telemetry.
 *
 * Idempotent: a second call is ignored (React StrictMode double-invokes effects
 * in dev, and the identity query only resolves once per session anyway).
 */
export function initTelemetry(identity: Identity): void {
  if (initialized) return;
  if (!identity.analytics_enabled) return;

  initialized = true;
  context = { isLoggedIn: identity.is_logged_in };
  sessionId = crypto.randomUUID();

  // Flush on page exit so a batch queued in the last ~500ms isn't lost. Loss
  // on a hard crash is acceptable (ADR-9, fire-and-forget); this just narrows
  // the window for the common navigate-away case.
  exitListener = () => {
    if (document.visibilityState === 'hidden') flushOnExit();
  };
  window.addEventListener('pagehide', flushOnExit);
  document.addEventListener('visibilitychange', exitListener);

  notifyInitialized();
}

/** Whether telemetry has been initialised (i.e. consent was granted). */
export function isTelemetryInitialized(): boolean {
  return initialized;
}

/**
 * React-observable view of {@link isTelemetryInitialized}. Consent is resolved
 * in a post-render effect and `initTelemetry` mutates a module singleton, so a
 * component that decorates an href at render time (e.g. an outbound `<a>`) would
 * otherwise capture the pre-consent value with no re-render to reapply it.
 * Subscribing here forces one re-render when consent lands.
 */
export function useTelemetryInitialized(): boolean {
  return useSyncExternalStore((listener) => {
    initListeners.add(listener);
    return () => initListeners.delete(listener);
  }, isTelemetryInitialized);
}

/** Base event properties, or `null` if telemetry was never initialised. */
export function getTelemetryContext(): TelemetryContext | null {
  return context;
}

/**
 * Enqueue an analytics event. No-op until {@link initTelemetry} has run under
 * consent, so no event is ever emitted without consent. Injects the
 * `is_logged_in` dimension and an anonymised `context` (a fresh per-event
 * `event_id` + the per-session `session_id`), then schedules a batched flush.
 */
export function track(event: TrackableEvent): void {
  if (!initialized || !context) return;

  queue.push({
    ...event,
    is_logged_in: context.isLoggedIn,
    context: {
      event_id: crypto.randomUUID(),
      session_id: sessionId ?? undefined,
    },
  } as AnalyticsEvent);

  scheduleFlush();
}

function scheduleFlush(): void {
  if (flushTimer != null) return;
  flushTimer = setTimeout(() => {
    flushTimer = null;
    void flush();
  }, FLUSH_DELAY_MS);
}

/** POST the queued batch and clear it. Fire-and-forget: errors are swallowed
 *  (event loss is acceptable per ADR-9). */
async function flush(): Promise<void> {
  if (queue.length === 0) return;
  const events = queue;
  queue = [];
  try {
    await api.analytics({ events });
  } catch {
    // Intentionally ignored — analytics is best-effort.
  }
}

/** Synchronous exit-time flush. Prefers `sendBeacon`, which survives page
 *  teardown; falls back to a `keepalive` fetch. */
function flushOnExit(): void {
  if (flushTimer != null) {
    clearTimeout(flushTimer);
    flushTimer = null;
  }
  if (queue.length === 0) return;
  const events = queue;
  queue = [];
  const body = JSON.stringify({ events });
  if (typeof navigator !== 'undefined' && typeof navigator.sendBeacon === 'function') {
    navigator.sendBeacon(
      ANALYTICS_PATH,
      new Blob([body], { type: 'application/json' }),
    );
    return;
  }
  try {
    void api.analytics({ events }, { keepalive: true });
  } catch {
    // Best-effort.
  }
}

/* ---------- typed call-site helpers ---------- */

export function trackDocsSiteOpened(
  fields: Omit<DocsSiteOpenedEvent, 'event_type' | 'is_logged_in' | 'context'>,
): void {
  track({ event_type: 'docs_site_opened', ...fields });
}

export function trackResourceViewed(
  fields: Omit<ResourceViewedEvent, 'event_type' | 'is_logged_in' | 'context'>,
): void {
  track({ event_type: 'resource_viewed', ...fields });
}

export function trackSearchPerformed(
  fields: Omit<SearchPerformedEvent, 'event_type' | 'is_logged_in' | 'context'>,
): void {
  track({ event_type: 'search_performed', ...fields });
}

export function trackLineageViewed(
  fields: Omit<LineageViewedEvent, 'event_type' | 'is_logged_in' | 'context'>,
): void {
  track({ event_type: 'lineage_viewed', ...fields });
}

/** Emit one of the three upsell-prompt events. The caller supplies the tagged
 *  event so the discriminated fields stay type-checked per `event_type`. */
export function trackUpsellEvent(
  event: DistributiveOmit<
    UpsellPromptDisplayedEvent | UpsellPromptClickedEvent | UpsellPromptDismissedEvent,
    'is_logged_in' | 'context'
  >,
): void {
  track(event);
}

export function trackReferralLinkClicked(
  fields: Omit<ReferralLinkClickedEvent, 'event_type' | 'is_logged_in' | 'context'>,
): void {
  track({ event_type: 'referral_link_clicked', ...fields });
}

/** Test-only: reset the module singleton between cases. */
export function resetTelemetryForTests(): void {
  if (flushTimer != null) {
    clearTimeout(flushTimer);
    flushTimer = null;
  }
  if (exitListener) {
    window.removeEventListener('pagehide', flushOnExit);
    document.removeEventListener('visibilitychange', exitListener);
  }
  initialized = false;
  context = null;
  sessionId = null;
  queue = [];
  exitListener = null;
  notifyInitialized();
}
