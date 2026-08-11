import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  type MockInstance,
  vi,
} from 'vitest';

import { api, type Identity } from '../api';
import {
  getTelemetryContext,
  initTelemetry,
  isTelemetryInitialized,
  resetTelemetryForTests,
  trackDocsSiteOpened,
  trackLineageViewed,
  trackReferralLinkClicked,
  trackResourceViewed,
  trackSearchPerformed,
  trackUpsellEvent,
} from './telemetry';

const ENABLED: Identity = { is_logged_in: true, analytics_enabled: true };
const DISABLED: Identity = { is_logged_in: true, analytics_enabled: false };

describe('telemetry', () => {
  afterEach(() => resetTelemetryForTests());

  it('does not initialise when analytics is not consented', () => {
    initTelemetry(DISABLED);

    expect(isTelemetryInitialized()).toBe(false);
    expect(getTelemetryContext()).toBeNull();
  });

  it('initialises and captures the logged-in dimension when consented', () => {
    initTelemetry(ENABLED);

    expect(isTelemetryInitialized()).toBe(true);
    expect(getTelemetryContext()).toEqual({ isLoggedIn: true });
  });

  it('carries is_logged_in: false through to the context', () => {
    initTelemetry({ is_logged_in: false, analytics_enabled: true });

    expect(getTelemetryContext()).toEqual({ isLoggedIn: false });
  });

  it('is idempotent — a second call is a no-op', () => {
    initTelemetry(ENABLED);
    // A later call, even with different identity, must not re-initialise or
    // mutate the captured context.
    initTelemetry({ is_logged_in: false, analytics_enabled: true });

    expect(getTelemetryContext()).toEqual({ isLoggedIn: true });
  });

  it('stays uninitialised after a denied call, allowing a later consented init', () => {
    initTelemetry(DISABLED);
    expect(isTelemetryInitialized()).toBe(false);

    initTelemetry(ENABLED);
    expect(isTelemetryInitialized()).toBe(true);
    expect(getTelemetryContext()).toEqual({ isLoggedIn: true });
  });
});

describe('telemetry emission', () => {
  let analytics: MockInstance<typeof api.analytics>;

  beforeEach(() => {
    vi.useFakeTimers();
    analytics = vi
      .spyOn(api, 'analytics')
      .mockResolvedValue({ accepted: 1, skipped: 0 });
  });

  afterEach(() => {
    resetTelemetryForTests();
    vi.runOnlyPendingTimers();
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('track is a no-op before init', () => {
    trackSearchPerformed({ search_query: 'x', result_count: 1 });
    vi.runAllTimers();
    expect(analytics).not.toHaveBeenCalled();
  });

  it('track is a no-op after a consent-denied init', () => {
    initTelemetry(DISABLED);
    trackSearchPerformed({ search_query: 'x', result_count: 1 });
    vi.runAllTimers();
    expect(analytics).not.toHaveBeenCalled();
  });

  it('injects is_logged_in and an anonymised context, then flushes a batch', () => {
    initTelemetry(ENABLED);
    trackSearchPerformed({ search_query: 'orders', result_count: 3 });

    // Debounced — nothing posted until the flush timer fires.
    expect(analytics).not.toHaveBeenCalled();
    vi.runAllTimers();

    expect(analytics).toHaveBeenCalledTimes(1);
    const body = analytics.mock.calls[0][0] as { events: unknown[] };
    expect(body.events).toHaveLength(1);
    const event = body.events[0] as Record<string, unknown>;
    expect(event.event_type).toBe('search_performed');
    expect(event.is_logged_in).toBe(true);
    const ctx = event.context as Record<string, unknown>;
    expect(typeof ctx.event_id).toBe('string');
    expect(typeof ctx.session_id).toBe('string');
    // No PII: only the anonymised context keys are present.
    expect(Object.keys(ctx).sort()).toEqual(['event_id', 'session_id']);
  });

  it('batches multiple events into a single flush', () => {
    initTelemetry(ENABLED);
    trackSearchPerformed({ search_query: 'a', result_count: 1 });
    trackResourceViewed({
      resource_type: 'model',
      view_level: 'detail',
      resource_id: 'model.p.a',
    });
    vi.runAllTimers();

    expect(analytics).toHaveBeenCalledTimes(1);
    const body = analytics.mock.calls[0][0] as { events: unknown[] };
    expect(body.events).toHaveLength(2);
  });

  it('gives each event a distinct event_id but a shared session_id', () => {
    initTelemetry(ENABLED);
    trackSearchPerformed({ search_query: 'a', result_count: 1 });
    trackSearchPerformed({ search_query: 'b', result_count: 2 });
    vi.runAllTimers();

    const events = (
      analytics.mock.calls[0][0] as unknown as {
        events: Array<{ context: Record<string, string> }>;
      }
    ).events;
    expect(events[0].context.event_id).not.toBe(events[1].context.event_id);
    expect(events[0].context.session_id).toBe(events[1].context.session_id);
  });

  it('flushes the queue on pagehide via sendBeacon', () => {
    const sendBeacon = vi.fn().mockReturnValue(true);
    vi.stubGlobal('navigator', { ...navigator, sendBeacon });

    initTelemetry(ENABLED);
    trackSearchPerformed({ search_query: 'a', result_count: 1 });
    // Not yet flushed via the timer.
    window.dispatchEvent(new Event('pagehide'));

    expect(sendBeacon).toHaveBeenCalledTimes(1);
    const [path, blob] = sendBeacon.mock.calls[0];
    expect(path).toBe('/api/v1/analytics/events');
    expect(blob).toBeInstanceOf(Blob);
    // The timer flush must not double-send after the beacon drained the queue.
    vi.runAllTimers();
    expect(analytics).not.toHaveBeenCalled();

    vi.unstubAllGlobals();
  });

  it('resetTelemetryForTests clears the queue and session', () => {
    initTelemetry(ENABLED);
    trackSearchPerformed({ search_query: 'a', result_count: 1 });
    resetTelemetryForTests();

    // The pending event is gone and telemetry is uninitialised again.
    expect(isTelemetryInitialized()).toBe(false);
    vi.runAllTimers();
    expect(analytics).not.toHaveBeenCalled();
  });

  it.each([
    [
      () => trackDocsSiteOpened({ dbt_version: '1.9', project_resource_count: 42 }),
      {
        event_type: 'docs_site_opened',
        dbt_version: '1.9',
        project_resource_count: 42,
      },
    ],
    [
      () =>
        trackResourceViewed({
          resource_type: 'model',
          view_level: 'detail',
          resource_id: 'model.p.a',
        }),
      {
        event_type: 'resource_viewed',
        resource_type: 'model',
        view_level: 'detail',
        resource_id: 'model.p.a',
      },
    ],
    [
      () => trackSearchPerformed({ search_query: 'q', result_count: 5 }),
      { event_type: 'search_performed', search_query: 'q', result_count: 5 },
    ],
    [
      () =>
        trackLineageViewed({
          lineage_type: 'inline',
          resource_type: 'model',
          resource_id: 'model.p.a',
        }),
      {
        event_type: 'lineage_viewed',
        lineage_type: 'inline',
        resource_type: 'model',
        resource_id: 'model.p.a',
      },
    ],
    [
      () =>
        trackUpsellEvent({
          event_type: 'upsell_prompt_displayed',
          upsell_track: 'columnLineage',
          prompt_format: 'rail-card',
          prompt_location: 'rail',
        }),
      {
        event_type: 'upsell_prompt_displayed',
        upsell_track: 'columnLineage',
        prompt_format: 'rail-card',
        prompt_location: 'rail',
      },
    ],
    [
      () =>
        trackReferralLinkClicked({
          referral_code: '',
          link_destination: 'https://docs.getdbt.com',
        }),
      {
        event_type: 'referral_link_clicked',
        referral_code: '',
        link_destination: 'https://docs.getdbt.com',
      },
    ],
  ])('helper produces the correct event_type + fields', (emit, expected) => {
    initTelemetry(ENABLED);
    emit();
    vi.runAllTimers();

    const event = (
      analytics.mock.calls[0][0] as unknown as { events: Record<string, unknown>[] }
    ).events[0];
    expect(event).toMatchObject(expected);
  });
});
