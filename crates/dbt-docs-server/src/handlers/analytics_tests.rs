use std::sync::{Arc, Mutex};

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;

use super::*;
use crate::providers::Providers;
use crate::state::{AppState, DistInfo};

// ---------------------------------------------------------------------------
// Recording sink double
// ---------------------------------------------------------------------------

#[derive(Default)]
struct RecordingSink {
    events: Mutex<Vec<DocsEvent>>,
}

impl RecordingSink {
    fn recorded(&self) -> Vec<DocsEvent> {
        self.events.lock().unwrap().clone()
    }
}

impl AnalyticsSink for RecordingSink {
    fn emit(&self, event: DocsEvent) {
        self.events.lock().unwrap().push(event);
    }
}

fn make_state(
    do_not_track: bool,
    send_anonymous_usage_stats: bool,
    sink: Arc<RecordingSink>,
) -> Arc<AppState> {
    Arc::new(
        AppState::new(
            std::path::PathBuf::from("/tmp"),
            Providers::default(),
            false,
            send_anonymous_usage_stats,
        )
        .with_do_not_track(do_not_track)
        .with_analytics(sink),
    )
}

async fn body_json(response: Response) -> (StatusCode, serde_json::Value) {
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value = serde_json::from_slice(&bytes).unwrap();
    (status, value)
}

// ---------------------------------------------------------------------------
// Handler tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn happy_path_relays_mixed_batch() {
    let sink = Arc::new(RecordingSink::default());
    let state = make_state(false, true, sink.clone());
    let raw = serde_json::json!({
        "events": [
            {
                "event_type": "docs_site_opened",
                "is_logged_in": true,
                "context": { "event_id": "e1", "session_id": "s1" },
                "dbt_version": "1.9.0",
                "project_resource_count": 42
            },
            {
                "event_type": "resource_viewed",
                "is_logged_in": false,
                "context": { "event_id": "e2" },
                "resource_type": "model",
                "view_level": "detail",
                "resource_id": "model.foo.bar"
            },
            {
                "event_type": "search_performed",
                "is_logged_in": true,
                "context": {},
                "search_query": "orders",
                "result_count": 7
            }
        ]
    });

    let response = post_events(State(state), Json(raw)).await;
    let (status, body) = body_json(response).await;

    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(body, serde_json::json!({ "accepted": 3, "skipped": 0 }));
    assert_eq!(sink.recorded().len(), 3);
}

#[tokio::test]
async fn consent_off_via_do_not_track_skips_all() {
    let sink = Arc::new(RecordingSink::default());
    let state = make_state(true, true, sink.clone());
    let raw = serde_json::json!({
        "events": [
            { "event_type": "resource_viewed", "is_logged_in": false, "context": {} },
            { "event_type": "search_performed", "is_logged_in": false, "context": {} }
        ]
    });

    let response = post_events(State(state), Json(raw)).await;
    let (status, body) = body_json(response).await;

    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(body, serde_json::json!({ "accepted": 0, "skipped": 2 }));
    assert!(sink.recorded().is_empty());
}

#[tokio::test]
async fn consent_off_via_usage_stats_flag_skips_all() {
    let sink = Arc::new(RecordingSink::default());
    let state = make_state(false, false, sink.clone());
    let raw = serde_json::json!({
        "events": [
            { "event_type": "docs_site_opened", "is_logged_in": true, "context": {} }
        ]
    });

    let response = post_events(State(state), Json(raw)).await;
    let (status, body) = body_json(response).await;

    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(body, serde_json::json!({ "accepted": 0, "skipped": 1 }));
    assert!(sink.recorded().is_empty());
}

#[tokio::test]
async fn unknown_event_type_is_bad_request() {
    let sink = Arc::new(RecordingSink::default());
    let state = make_state(false, true, sink.clone());
    let raw = serde_json::json!({
        "events": [ { "event_type": "not_a_real_event", "is_logged_in": true } ]
    });

    let response = post_events(State(state), Json(raw)).await;
    let (status, body) = body_json(response).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "invalid_event");
    assert!(sink.recorded().is_empty());
}

#[tokio::test]
async fn malformed_body_is_bad_request() {
    let sink = Arc::new(RecordingSink::default());
    let state = make_state(false, true, sink.clone());
    // `events` should be an array, not an object.
    let raw = serde_json::json!({ "events": { "nope": 1 } });

    let response = post_events(State(state), Json(raw)).await;
    let (status, body) = body_json(response).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "invalid_event");
}

// ---------------------------------------------------------------------------
// into_event mapping tests (one per variant)
// ---------------------------------------------------------------------------

/// Distribution code used by the test hydration.
const DIST: &str = "oss";

/// Representative server hydration used by the per-variant map tests. Non-empty
/// so the tests exercise the server-wins override of `is_logged_in`,
/// `distribution`, `dbt_version`, and the dbt Cloud IDs.
fn hyd() -> TelemetryHydration {
    TelemetryHydration {
        distribution: DIST.to_string(),
        dbt_version: "2.0.0".to_string(),
        is_logged_in: true,
        dbt_cloud_account_identifier: "acct".to_string(),
        dbt_cloud_project_id: "proj".to_string(),
        dbt_cloud_environment_id: "env".to_string(),
    }
}

/// Parse a single-event JSON object into its `DocsEvent`, hydrated via [`hyd`].
fn map_one(value: serde_json::Value) -> DocsEvent {
    serde_json::from_value::<AnalyticsEventReq>(value)
        .expect("event should deserialize")
        .into_event(&hyd())
}

/// Expected wire context after hydration: client `event_id` flows through; the
/// dbt Cloud IDs come from [`hyd`].
fn ctx(event_id: &str) -> Option<VortexTelemetryDbtCloudContext> {
    Some(VortexTelemetryDbtCloudContext {
        event_id: event_id.to_string(),
        dbt_cloud_account_identifier: "acct".to_string(),
        dbt_cloud_project_id: "proj".to_string(),
        dbt_cloud_environment_id: "env".to_string(),
        ..Default::default()
    })
}

#[test]
fn map_docs_site_opened() {
    let got = map_one(serde_json::json!({
        "event_type": "docs_site_opened",
        "is_logged_in": true,
        "context": { "event_id": "e1" },
        "dbt_version": "1.9.0",
        "project_resource_count": 42
    }));
    assert_eq!(
        got,
        DocsEvent::DocsSiteOpened(DocsSiteOpened {
            context: ctx("e1"),
            is_logged_in: true,
            dbt_version: "2.0.0".to_string(),
            project_resource_count: 42,
            distribution: DIST.to_string(),
        })
    );
}

#[test]
fn map_resource_viewed() {
    let got = map_one(serde_json::json!({
        "event_type": "resource_viewed",
        "is_logged_in": false,
        "context": { "event_id": "e2" },
        "resource_type": "model",
        "view_level": "detail",
        "resource_id": "model.foo.bar"
    }));
    assert_eq!(
        got,
        DocsEvent::ResourceViewed(ResourceViewed {
            context: ctx("e2"),
            is_logged_in: true,
            resource_type: "model".to_string(),
            view_level: "detail".to_string(),
            resource_id: "model.foo.bar".to_string(),
            distribution: DIST.to_string(),
        })
    );
}

#[test]
fn map_lineage_viewed() {
    let got = map_one(serde_json::json!({
        "event_type": "lineage_viewed",
        "is_logged_in": true,
        "context": { "event_id": "e3" },
        "lineage_type": "column",
        "resource_type": "model",
        "resource_id": "model.foo.bar"
    }));
    assert_eq!(
        got,
        DocsEvent::LineageViewed(LineageViewed {
            context: ctx("e3"),
            is_logged_in: true,
            lineage_type: "column".to_string(),
            resource_type: "model".to_string(),
            resource_id: "model.foo.bar".to_string(),
            distribution: DIST.to_string(),
        })
    );
}

#[test]
fn map_search_performed() {
    let got = map_one(serde_json::json!({
        "event_type": "search_performed",
        "is_logged_in": false,
        "context": { "event_id": "e4" },
        "search_query": "orders",
        "result_count": 7
    }));
    assert_eq!(
        got,
        DocsEvent::SearchPerformed(SearchPerformed {
            context: ctx("e4"),
            is_logged_in: true,
            search_query: "orders".to_string(),
            result_count: 7,
            distribution: DIST.to_string(),
        })
    );
}

#[test]
fn map_filter_applied() {
    let got = map_one(serde_json::json!({
        "event_type": "filter_applied",
        "is_logged_in": false,
        "context": { "event_id": "e5" },
        "resource_type": "model",
        "filter_type": "test_result",
        "filter_value": "pass"
    }));
    assert_eq!(
        got,
        DocsEvent::FilterApplied(FilterApplied {
            context: ctx("e5"),
            is_logged_in: true,
            resource_type: "model".to_string(),
            filter_type: "test_result".to_string(),
            filter_value: "pass".to_string(),
            distribution: DIST.to_string(),
        })
    );
}

#[test]
fn map_upsell_prompt_displayed() {
    let got = map_one(serde_json::json!({
        "event_type": "upsell_prompt_displayed",
        "is_logged_in": false,
        "context": { "event_id": "e6" },
        "upsell_track": "columnLineage",
        "prompt_format": "banner",
        "prompt_location": "lineage_view"
    }));
    assert_eq!(
        got,
        DocsEvent::UpsellPromptDisplayed(UpsellPromptDisplayed {
            context: ctx("e6"),
            is_logged_in: true,
            upsell_track: "columnLineage".to_string(),
            prompt_format: "banner".to_string(),
            prompt_location: "lineage_view".to_string(),
            distribution: DIST.to_string(),
        })
    );
}

#[test]
fn map_upsell_prompt_clicked() {
    let got = map_one(serde_json::json!({
        "event_type": "upsell_prompt_clicked",
        "is_logged_in": false,
        "context": { "event_id": "e7" },
        "upsell_track": "columnLineage",
        "cta_label": "learn_more_about_cll",
        "referral_code": "ref123"
    }));
    assert_eq!(
        got,
        DocsEvent::UpsellPromptClicked(UpsellPromptClicked {
            context: ctx("e7"),
            is_logged_in: true,
            upsell_track: "columnLineage".to_string(),
            cta_label: "learn_more_about_cll".to_string(),
            referral_code: "ref123".to_string(),
            distribution: DIST.to_string(),
        })
    );
}

#[test]
fn map_upsell_prompt_dismissed() {
    let got = map_one(serde_json::json!({
        "event_type": "upsell_prompt_dismissed",
        "is_logged_in": false,
        "context": { "event_id": "e8" },
        "upsell_track": "dbtState",
        "dismiss_method": "close_button"
    }));
    assert_eq!(
        got,
        DocsEvent::UpsellPromptDismissed(UpsellPromptDismissed {
            context: ctx("e8"),
            is_logged_in: true,
            upsell_track: "dbtState".to_string(),
            dismiss_method: "close_button".to_string(),
            distribution: DIST.to_string(),
        })
    );
}

#[test]
fn map_referral_link_clicked() {
    let got = map_one(serde_json::json!({
        "event_type": "referral_link_clicked",
        "is_logged_in": false,
        "context": { "event_id": "e9" },
        "referral_code": "ref456",
        "link_destination": "platform_signup"
    }));
    assert_eq!(
        got,
        DocsEvent::ReferralLinkClicked(ReferralLinkClicked {
            context: ctx("e9"),
            is_logged_in: true,
            referral_code: "ref456".to_string(),
            link_destination: "platform_signup".to_string(),
            distribution: DIST.to_string(),
        })
    );
}

#[test]
fn map_account_logged_in() {
    let got = map_one(serde_json::json!({
        "event_type": "account_logged_in",
        "is_logged_in": true,
        "context": { "event_id": "e10" },
        "triggered_by_prompt": true,
        "upsell_track": "mesh"
    }));
    assert_eq!(
        got,
        DocsEvent::AccountLoggedIn(AccountLoggedIn {
            context: ctx("e10"),
            is_logged_in: true,
            triggered_by_prompt: true,
            upsell_track: "mesh".to_string(),
            distribution: DIST.to_string(),
        })
    );
}

#[test]
fn map_error_encountered() {
    let got = map_one(serde_json::json!({
        "event_type": "error_encountered",
        "is_logged_in": false,
        "context": { "event_id": "e11" },
        "error_category": "resource_load_failure",
        "error_message": "not found",
        "field_name": "unique_id"
    }));
    assert_eq!(
        got,
        DocsEvent::ErrorEncountered(ErrorEncountered {
            context: ctx("e11"),
            is_logged_in: true,
            error_category: "resource_load_failure".to_string(),
            error_message: "not found".to_string(),
            field_name: "unique_id".to_string(),
            distribution: DIST.to_string(),
        })
    );
}

#[test]
fn context_fields_default_to_empty() {
    // Missing context entirely → client-only fields stay empty, but the server
    // still hydrates the dbt Cloud IDs (see `ctx`).
    let got = map_one(serde_json::json!({
        "event_type": "resource_viewed",
        "is_logged_in": false
    }));
    let DocsEvent::ResourceViewed(inner) = got else {
        panic!("expected ResourceViewed");
    };
    assert_eq!(inner.context, ctx(""));
    assert_eq!(inner.resource_type, "");
}

// ---------------------------------------------------------------------------
// Hydration tests (server always wins)
// ---------------------------------------------------------------------------

/// A `DistInfoProvider` double returning a fixed hydration, for asserting the
/// handler applies server-side values.
struct FakeDistInfo(TelemetryHydration);

impl dbt_docs_core::DistInfoProvider for FakeDistInfo {
    fn dist_info(&self) -> DistInfo {
        DistInfo {
            name: self.0.distribution.clone(),
            version: "unused",
            is_logged_in: self.0.is_logged_in,
        }
    }
    fn telemetry_hydration(&self) -> TelemetryHydration {
        self.0.clone()
    }
}

fn state_with_hydration(hydration: TelemetryHydration, sink: Arc<RecordingSink>) -> Arc<AppState> {
    let providers = Providers {
        dist_info: Arc::new(FakeDistInfo(hydration)),
        ..Providers::default()
    };
    Arc::new(
        AppState::new(std::path::PathBuf::from("/tmp"), providers, false, true)
            .with_do_not_track(false)
            .with_analytics(sink),
    )
}

#[tokio::test]
async fn slim_batch_is_hydrated_server_side() {
    let sink = Arc::new(RecordingSink::default());
    let state = state_with_hydration(hyd(), sink.clone());
    // Slim payload: no distribution / dbt_version / is_logged_in / cloud IDs.
    let raw = serde_json::json!({
        "events": [
            { "event_type": "docs_site_opened", "context": { "event_id": "e1" } }
        ]
    });

    let response = post_events(State(state), Json(raw)).await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let recorded = sink.recorded();
    let DocsEvent::DocsSiteOpened(ev) = &recorded[0] else {
        panic!("expected DocsSiteOpened");
    };
    assert_eq!(ev.distribution, DIST);
    assert_eq!(ev.dbt_version, "2.0.0");
    assert!(ev.is_logged_in);
    let ctx = ev.context.as_ref().unwrap();
    assert_eq!(ctx.event_id, "e1");
    assert_eq!(ctx.dbt_cloud_account_identifier, "acct");
    assert_eq!(ctx.dbt_cloud_project_id, "proj");
    assert_eq!(ctx.dbt_cloud_environment_id, "env");
}

#[tokio::test]
async fn client_sent_values_are_overridden() {
    let sink = Arc::new(RecordingSink::default());
    let state = state_with_hydration(hyd(), sink.clone());
    // Client sends bogus values the server must overwrite.
    let raw = serde_json::json!({
        "events": [
            {
                "event_type": "docs_site_opened",
                "is_logged_in": false,
                "dbt_version": "9.9.9-bogus",
                "context": {
                    "event_id": "e1",
                    "dbt_cloud_account_identifier": "bogus-acct",
                    "dbt_cloud_project_id": "bogus-proj",
                    "dbt_cloud_environment_id": "bogus-env"
                }
            }
        ]
    });

    let response = post_events(State(state), Json(raw)).await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let recorded = sink.recorded();
    let DocsEvent::DocsSiteOpened(ev) = &recorded[0] else {
        panic!("expected DocsSiteOpened");
    };
    assert_eq!(ev.distribution, DIST);
    assert_eq!(ev.dbt_version, "2.0.0");
    assert!(ev.is_logged_in);
    let ctx = ev.context.as_ref().unwrap();
    assert_eq!(ctx.dbt_cloud_account_identifier, "acct");
    assert_eq!(ctx.dbt_cloud_project_id, "proj");
    assert_eq!(ctx.dbt_cloud_environment_id, "env");
}
