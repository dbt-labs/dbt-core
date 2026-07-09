//! `POST /api/v1/analytics/events` — server-side analytics relay (META-7739).
//!
//! The docs v2 browser UI does NOT talk to Vortex directly. It POSTs a batch of
//! events here; this handler enforces consent, maps each event to its concrete
//! `docs.proto` wire type, and forwards it to Vortex via the self-contained
//! [`crate::vortex_sender`] (fire-and-forget). Keeping the relay server-side lets
//! us gate on consent, keep the ingest URL off the browser, and guarantee no PII
//! reaches the wire.
//!
//! ## Slim client payload (server-side hydration)
//!
//! The server fills in every field it can know authoritatively (see
//! [`TelemetryHydration`]), so the browser sends a slim event and **omits**:
//! `distribution`, `dbt_version`, `is_logged_in`, and the three
//! `dbt_cloud_account_identifier` / `dbt_cloud_project_id` /
//! `dbt_cloud_environment_id` context fields. Any of these sent by the client is
//! ignored — the server always wins. The client still supplies its own context
//! (`event_id`, `session_id`, `snowplow_*`, `referrer_url`, numeric
//! `dbt_cloud_account_id`, `dbt_cloud_user_id`, `feature`) and per-event payload
//! fields.
//!
//! ## Wire contract
//!
//! The proto types below are hand-transcribed from `docs.proto`
//! (`v1.public.events.docs`), mirroring the dbt-index / codex-vortex precedent of
//! avoiding `proto-rust` and the shared `vortex-client`. Enum-ish fields are
//! `string` on the wire (Vortex converts proto enums to Int32 in Iceberg, losing
//! the label), carrying the lowercase dbt domain code. The `enrichment` field
//! (tag 1) is omitted — producers set it to None server-side and leaving it out
//! encodes identically.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::handlers::json::bad_request_coded;
use crate::state::{SharedState, TelemetryHydration};
use crate::vortex_sender;

/// Wire package for all docs analytics events.
const PACKAGE: &str = "v1.public.events.docs";

// ---------------------------------------------------------------------------
// Shared context (VortexTelemetryDbtCloudContext)
// ---------------------------------------------------------------------------

/// dbt Cloud context attached to every docs event. All fields are `string` on
/// the wire; anonymised IDs only — never email or other PII. Transcribed from
/// `v1.public.common.vortex_telemetry_contexts.VortexTelemetryDbtCloudContext`.
#[derive(Clone, PartialEq, prost::Message, Serialize)]
pub struct VortexTelemetryDbtCloudContext {
    #[prost(string, tag = "1")]
    pub event_id: String,
    #[prost(string, tag = "2")]
    pub feature: String,
    #[prost(string, tag = "3")]
    pub snowplow_domain_session_id: String,
    #[prost(string, tag = "4")]
    pub snowplow_domain_user_id: String,
    #[prost(string, tag = "5")]
    pub session_id: String,
    #[prost(string, tag = "6")]
    pub referrer_url: String,
    #[prost(string, tag = "7")]
    pub dbt_cloud_account_id: String,
    #[prost(string, tag = "8")]
    pub dbt_cloud_account_identifier: String,
    #[prost(string, tag = "9")]
    pub dbt_cloud_project_id: String,
    #[prost(string, tag = "10")]
    pub dbt_cloud_environment_id: String,
    #[prost(string, tag = "11")]
    pub dbt_cloud_user_id: String,
}

// ---------------------------------------------------------------------------
// Event messages (hand-rolled prost structs)
// ---------------------------------------------------------------------------

/// Generate the `impl prost::Name` boilerplate for each docs event type.
macro_rules! impl_docs_name {
    ($($ty:ty => $name:literal),+ $(,)?) => {
        $(
            impl prost::Name for $ty {
                const NAME: &'static str = $name;
                const PACKAGE: &'static str = PACKAGE;
            }
        )+
    };
}

/// Fired when a docs v2 site is opened.
#[derive(Clone, PartialEq, prost::Message, Serialize)]
pub struct DocsSiteOpened {
    #[prost(message, optional, tag = "2")]
    pub context: Option<VortexTelemetryDbtCloudContext>,
    #[prost(bool, tag = "3")]
    pub is_logged_in: bool,
    #[prost(string, tag = "4")]
    pub dbt_version: String,
    #[prost(int64, tag = "5")]
    pub project_resource_count: i64,
    #[prost(string, tag = "6")]
    pub distribution: String,
}

/// Fired when a resource is viewed (list or detail).
#[derive(Clone, PartialEq, prost::Message, Serialize)]
pub struct ResourceViewed {
    #[prost(message, optional, tag = "2")]
    pub context: Option<VortexTelemetryDbtCloudContext>,
    #[prost(bool, tag = "3")]
    pub is_logged_in: bool,
    #[prost(string, tag = "4")]
    pub resource_type: String,
    #[prost(string, tag = "5")]
    pub view_level: String,
    #[prost(string, tag = "6")]
    pub resource_id: String,
    #[prost(string, tag = "7")]
    pub distribution: String,
}

/// Fired when lineage is viewed for a resource.
#[derive(Clone, PartialEq, prost::Message, Serialize)]
pub struct LineageViewed {
    #[prost(message, optional, tag = "2")]
    pub context: Option<VortexTelemetryDbtCloudContext>,
    #[prost(bool, tag = "3")]
    pub is_logged_in: bool,
    #[prost(string, tag = "4")]
    pub lineage_type: String,
    #[prost(string, tag = "5")]
    pub resource_type: String,
    #[prost(string, tag = "6")]
    pub resource_id: String,
    #[prost(string, tag = "7")]
    pub distribution: String,
}

/// Fired when a search is performed.
#[derive(Clone, PartialEq, prost::Message, Serialize)]
pub struct SearchPerformed {
    #[prost(message, optional, tag = "2")]
    pub context: Option<VortexTelemetryDbtCloudContext>,
    #[prost(bool, tag = "3")]
    pub is_logged_in: bool,
    #[prost(string, tag = "4")]
    pub search_query: String,
    #[prost(int64, tag = "5")]
    pub result_count: i64,
    #[prost(string, tag = "6")]
    pub distribution: String,
}

/// Fired when a filter is applied to a resource list.
#[derive(Clone, PartialEq, prost::Message, Serialize)]
pub struct FilterApplied {
    #[prost(message, optional, tag = "2")]
    pub context: Option<VortexTelemetryDbtCloudContext>,
    #[prost(bool, tag = "3")]
    pub is_logged_in: bool,
    #[prost(string, tag = "4")]
    pub resource_type: String,
    #[prost(string, tag = "5")]
    pub filter_type: String,
    #[prost(string, tag = "6")]
    pub filter_value: String,
    #[prost(string, tag = "7")]
    pub distribution: String,
}

/// Fired when an upsell prompt is displayed.
#[derive(Clone, PartialEq, prost::Message, Serialize)]
pub struct UpsellPromptDisplayed {
    #[prost(message, optional, tag = "2")]
    pub context: Option<VortexTelemetryDbtCloudContext>,
    #[prost(bool, tag = "3")]
    pub is_logged_in: bool,
    #[prost(string, tag = "4")]
    pub upsell_track: String,
    #[prost(string, tag = "5")]
    pub prompt_format: String,
    #[prost(string, tag = "6")]
    pub prompt_location: String,
    #[prost(string, tag = "7")]
    pub distribution: String,
}

/// Fired when an upsell prompt is clicked.
#[derive(Clone, PartialEq, prost::Message, Serialize)]
pub struct UpsellPromptClicked {
    #[prost(message, optional, tag = "2")]
    pub context: Option<VortexTelemetryDbtCloudContext>,
    #[prost(bool, tag = "3")]
    pub is_logged_in: bool,
    #[prost(string, tag = "4")]
    pub upsell_track: String,
    #[prost(string, tag = "5")]
    pub cta_label: String,
    #[prost(string, tag = "6")]
    pub referral_code: String,
    #[prost(string, tag = "7")]
    pub distribution: String,
}

/// Fired when an upsell prompt is dismissed.
#[derive(Clone, PartialEq, prost::Message, Serialize)]
pub struct UpsellPromptDismissed {
    #[prost(message, optional, tag = "2")]
    pub context: Option<VortexTelemetryDbtCloudContext>,
    #[prost(bool, tag = "3")]
    pub is_logged_in: bool,
    #[prost(string, tag = "4")]
    pub upsell_track: String,
    #[prost(string, tag = "5")]
    pub dismiss_method: String,
    #[prost(string, tag = "6")]
    pub distribution: String,
}

/// Fired when a referral link is clicked.
#[derive(Clone, PartialEq, prost::Message, Serialize)]
pub struct ReferralLinkClicked {
    #[prost(message, optional, tag = "2")]
    pub context: Option<VortexTelemetryDbtCloudContext>,
    #[prost(bool, tag = "3")]
    pub is_logged_in: bool,
    #[prost(string, tag = "4")]
    pub referral_code: String,
    #[prost(string, tag = "5")]
    pub link_destination: String,
    #[prost(string, tag = "6")]
    pub distribution: String,
}

/// Fired when an account logs in.
#[derive(Clone, PartialEq, prost::Message, Serialize)]
pub struct AccountLoggedIn {
    #[prost(message, optional, tag = "2")]
    pub context: Option<VortexTelemetryDbtCloudContext>,
    #[prost(bool, tag = "3")]
    pub is_logged_in: bool,
    #[prost(bool, tag = "4")]
    pub triggered_by_prompt: bool,
    #[prost(string, tag = "5")]
    pub upsell_track: String,
    #[prost(string, tag = "6")]
    pub distribution: String,
}

/// Fired when an error is encountered.
#[derive(Clone, PartialEq, prost::Message, Serialize)]
pub struct ErrorEncountered {
    #[prost(message, optional, tag = "2")]
    pub context: Option<VortexTelemetryDbtCloudContext>,
    #[prost(bool, tag = "3")]
    pub is_logged_in: bool,
    #[prost(string, tag = "4")]
    pub error_category: String,
    #[prost(string, tag = "5")]
    pub error_message: String,
    #[prost(string, tag = "6")]
    pub field_name: String,
    #[prost(string, tag = "7")]
    pub distribution: String,
}

impl_docs_name! {
    DocsSiteOpened => "DocsSiteOpened",
    ResourceViewed => "ResourceViewed",
    LineageViewed => "LineageViewed",
    SearchPerformed => "SearchPerformed",
    FilterApplied => "FilterApplied",
    UpsellPromptDisplayed => "UpsellPromptDisplayed",
    UpsellPromptClicked => "UpsellPromptClicked",
    UpsellPromptDismissed => "UpsellPromptDismissed",
    ReferralLinkClicked => "ReferralLinkClicked",
    AccountLoggedIn => "AccountLoggedIn",
    ErrorEncountered => "ErrorEncountered",
}

// ---------------------------------------------------------------------------
// Typed dispatch enum
// ---------------------------------------------------------------------------

/// A concrete docs event ready to be emitted through the generic
/// `vortex_sender::log_proto`. The [`AnalyticsSink`] matches on this to recover
/// the concrete type (so `T::PACKAGE`/`T::NAME` resolve correctly).
#[derive(Clone, PartialEq, Debug)]
pub enum DocsEvent {
    DocsSiteOpened(DocsSiteOpened),
    ResourceViewed(ResourceViewed),
    LineageViewed(LineageViewed),
    SearchPerformed(SearchPerformed),
    FilterApplied(FilterApplied),
    UpsellPromptDisplayed(UpsellPromptDisplayed),
    UpsellPromptClicked(UpsellPromptClicked),
    UpsellPromptDismissed(UpsellPromptDismissed),
    ReferralLinkClicked(ReferralLinkClicked),
    AccountLoggedIn(AccountLoggedIn),
    ErrorEncountered(ErrorEncountered),
}

// ---------------------------------------------------------------------------
// Emission sink (testability seam)
// ---------------------------------------------------------------------------

/// Where mapped events go. Production forwards to Vortex; tests record.
pub trait AnalyticsSink: Send + Sync {
    fn emit(&self, event: DocsEvent);
}

/// Production sink: forwards each event to Vortex through the generic
/// `log_proto` so the concrete `prost::Name` type_url is preserved.
pub struct VortexSink;

impl AnalyticsSink for VortexSink {
    fn emit(&self, event: DocsEvent) {
        match event {
            DocsEvent::DocsSiteOpened(m) => {
                let _ = vortex_sender::log_proto(m);
            }
            DocsEvent::ResourceViewed(m) => {
                let _ = vortex_sender::log_proto(m);
            }
            DocsEvent::LineageViewed(m) => {
                let _ = vortex_sender::log_proto(m);
            }
            DocsEvent::SearchPerformed(m) => {
                let _ = vortex_sender::log_proto(m);
            }
            DocsEvent::FilterApplied(m) => {
                let _ = vortex_sender::log_proto(m);
            }
            DocsEvent::UpsellPromptDisplayed(m) => {
                let _ = vortex_sender::log_proto(m);
            }
            DocsEvent::UpsellPromptClicked(m) => {
                let _ = vortex_sender::log_proto(m);
            }
            DocsEvent::UpsellPromptDismissed(m) => {
                let _ = vortex_sender::log_proto(m);
            }
            DocsEvent::ReferralLinkClicked(m) => {
                let _ = vortex_sender::log_proto(m);
            }
            DocsEvent::AccountLoggedIn(m) => {
                let _ = vortex_sender::log_proto(m);
            }
            DocsEvent::ErrorEncountered(m) => {
                let _ = vortex_sender::log_proto(m);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/// Request context — mirrors [`VortexTelemetryDbtCloudContext`]. All fields
/// default to `""` so producers may send a partial (or empty) context.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct TelemetryContext {
    pub event_id: String,
    pub feature: String,
    pub snowplow_domain_session_id: String,
    pub snowplow_domain_user_id: String,
    pub session_id: String,
    pub referrer_url: String,
    pub dbt_cloud_account_id: String,
    pub dbt_cloud_account_identifier: String,
    pub dbt_cloud_project_id: String,
    pub dbt_cloud_environment_id: String,
    pub dbt_cloud_user_id: String,
}

impl TelemetryContext {
    /// Build the wire context, hydrating the server-authoritative dbt Cloud IDs
    /// from `h` (the server always wins — any client-sent value is overwritten).
    /// The client-only fields (`event_id`, `session_id`, `snowplow_*`,
    /// `referrer_url`, numeric `dbt_cloud_account_id`, `dbt_cloud_user_id`,
    /// `feature`) flow through from the request.
    fn into_proto(self, h: &TelemetryHydration) -> VortexTelemetryDbtCloudContext {
        VortexTelemetryDbtCloudContext {
            event_id: self.event_id,
            feature: self.feature,
            snowplow_domain_session_id: self.snowplow_domain_session_id,
            snowplow_domain_user_id: self.snowplow_domain_user_id,
            session_id: self.session_id,
            referrer_url: self.referrer_url,
            dbt_cloud_account_id: self.dbt_cloud_account_id,
            dbt_cloud_account_identifier: h.dbt_cloud_account_identifier.clone(),
            dbt_cloud_project_id: h.dbt_cloud_project_id.clone(),
            dbt_cloud_environment_id: h.dbt_cloud_environment_id.clone(),
            dbt_cloud_user_id: self.dbt_cloud_user_id,
        }
    }
}

/// Fields common to every event request.
///
/// `is_logged_in` stays deserializable for back-compat but is **ignored** —
/// the server hydrates it authoritatively (see [`TelemetryHydration`]).
#[derive(Clone, Debug, Default, Deserialize)]
pub struct EventBase {
    // Accepted for back-compat but ignored — the server hydrates it.
    #[allow(dead_code)]
    #[serde(default)]
    pub is_logged_in: bool,
    #[serde(default)]
    pub context: TelemetryContext,
}

/// A single event in the batch. Internally tagged on `event_type`; the base
/// fields (`is_logged_in`, `context`) are flattened into each variant.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum AnalyticsEventReq {
    DocsSiteOpened {
        #[serde(flatten)]
        base: EventBase,
        // Accepted for back-compat but ignored — the server hydrates it.
        #[allow(dead_code)]
        #[serde(default)]
        dbt_version: String,
        #[serde(default)]
        project_resource_count: i64,
    },
    ResourceViewed {
        #[serde(flatten)]
        base: EventBase,
        #[serde(default)]
        resource_type: String,
        #[serde(default)]
        view_level: String,
        #[serde(default)]
        resource_id: String,
    },
    LineageViewed {
        #[serde(flatten)]
        base: EventBase,
        #[serde(default)]
        lineage_type: String,
        #[serde(default)]
        resource_type: String,
        #[serde(default)]
        resource_id: String,
    },
    SearchPerformed {
        #[serde(flatten)]
        base: EventBase,
        #[serde(default)]
        search_query: String,
        #[serde(default)]
        result_count: i64,
    },
    FilterApplied {
        #[serde(flatten)]
        base: EventBase,
        #[serde(default)]
        resource_type: String,
        #[serde(default)]
        filter_type: String,
        #[serde(default)]
        filter_value: String,
    },
    UpsellPromptDisplayed {
        #[serde(flatten)]
        base: EventBase,
        #[serde(default)]
        upsell_track: String,
        #[serde(default)]
        prompt_format: String,
        #[serde(default)]
        prompt_location: String,
    },
    UpsellPromptClicked {
        #[serde(flatten)]
        base: EventBase,
        #[serde(default)]
        upsell_track: String,
        #[serde(default)]
        cta_label: String,
        #[serde(default)]
        referral_code: String,
    },
    UpsellPromptDismissed {
        #[serde(flatten)]
        base: EventBase,
        #[serde(default)]
        upsell_track: String,
        #[serde(default)]
        dismiss_method: String,
    },
    ReferralLinkClicked {
        #[serde(flatten)]
        base: EventBase,
        #[serde(default)]
        referral_code: String,
        #[serde(default)]
        link_destination: String,
    },
    AccountLoggedIn {
        #[serde(flatten)]
        base: EventBase,
        #[serde(default)]
        triggered_by_prompt: bool,
        #[serde(default)]
        upsell_track: String,
    },
    ErrorEncountered {
        #[serde(flatten)]
        base: EventBase,
        #[serde(default)]
        error_category: String,
        #[serde(default)]
        error_message: String,
        #[serde(default)]
        field_name: String,
    },
}

impl AnalyticsEventReq {
    /// Map a validated request event to its concrete wire type, hydrating the
    /// server-authoritative fields from `h`. Pure — no I/O.
    ///
    /// The server always wins: `distribution`, `is_logged_in`, the dbt Cloud
    /// IDs (via [`TelemetryContext::into_proto`]) and `DocsSiteOpened.dbt_version`
    /// come from `h`, overwriting any client-sent value. Client-supplied
    /// `is_logged_in`/`dbt_version` on the request are ignored.
    pub fn into_event(self, h: &TelemetryHydration) -> DocsEvent {
        match self {
            AnalyticsEventReq::DocsSiteOpened {
                base,
                dbt_version: _,
                project_resource_count,
            } => DocsEvent::DocsSiteOpened(DocsSiteOpened {
                context: Some(base.context.into_proto(h)),
                is_logged_in: h.is_logged_in,
                dbt_version: h.dbt_version.clone(),
                project_resource_count,
                distribution: h.distribution.clone(),
            }),
            AnalyticsEventReq::ResourceViewed {
                base,
                resource_type,
                view_level,
                resource_id,
            } => DocsEvent::ResourceViewed(ResourceViewed {
                context: Some(base.context.into_proto(h)),
                is_logged_in: h.is_logged_in,
                resource_type,
                view_level,
                resource_id,
                distribution: h.distribution.clone(),
            }),
            AnalyticsEventReq::LineageViewed {
                base,
                lineage_type,
                resource_type,
                resource_id,
            } => DocsEvent::LineageViewed(LineageViewed {
                context: Some(base.context.into_proto(h)),
                is_logged_in: h.is_logged_in,
                lineage_type,
                resource_type,
                resource_id,
                distribution: h.distribution.clone(),
            }),
            AnalyticsEventReq::SearchPerformed {
                base,
                search_query,
                result_count,
            } => DocsEvent::SearchPerformed(SearchPerformed {
                context: Some(base.context.into_proto(h)),
                is_logged_in: h.is_logged_in,
                search_query,
                result_count,
                distribution: h.distribution.clone(),
            }),
            AnalyticsEventReq::FilterApplied {
                base,
                resource_type,
                filter_type,
                filter_value,
            } => DocsEvent::FilterApplied(FilterApplied {
                context: Some(base.context.into_proto(h)),
                is_logged_in: h.is_logged_in,
                resource_type,
                filter_type,
                filter_value,
                distribution: h.distribution.clone(),
            }),
            AnalyticsEventReq::UpsellPromptDisplayed {
                base,
                upsell_track,
                prompt_format,
                prompt_location,
            } => DocsEvent::UpsellPromptDisplayed(UpsellPromptDisplayed {
                context: Some(base.context.into_proto(h)),
                is_logged_in: h.is_logged_in,
                upsell_track,
                prompt_format,
                prompt_location,
                distribution: h.distribution.clone(),
            }),
            AnalyticsEventReq::UpsellPromptClicked {
                base,
                upsell_track,
                cta_label,
                referral_code,
            } => DocsEvent::UpsellPromptClicked(UpsellPromptClicked {
                context: Some(base.context.into_proto(h)),
                is_logged_in: h.is_logged_in,
                upsell_track,
                cta_label,
                referral_code,
                distribution: h.distribution.clone(),
            }),
            AnalyticsEventReq::UpsellPromptDismissed {
                base,
                upsell_track,
                dismiss_method,
            } => DocsEvent::UpsellPromptDismissed(UpsellPromptDismissed {
                context: Some(base.context.into_proto(h)),
                is_logged_in: h.is_logged_in,
                upsell_track,
                dismiss_method,
                distribution: h.distribution.clone(),
            }),
            AnalyticsEventReq::ReferralLinkClicked {
                base,
                referral_code,
                link_destination,
            } => DocsEvent::ReferralLinkClicked(ReferralLinkClicked {
                context: Some(base.context.into_proto(h)),
                is_logged_in: h.is_logged_in,
                referral_code,
                link_destination,
                distribution: h.distribution.clone(),
            }),
            AnalyticsEventReq::AccountLoggedIn {
                base,
                triggered_by_prompt,
                upsell_track,
            } => DocsEvent::AccountLoggedIn(AccountLoggedIn {
                context: Some(base.context.into_proto(h)),
                is_logged_in: h.is_logged_in,
                triggered_by_prompt,
                upsell_track,
                distribution: h.distribution.clone(),
            }),
            AnalyticsEventReq::ErrorEncountered {
                base,
                error_category,
                error_message,
                field_name,
            } => DocsEvent::ErrorEncountered(ErrorEncountered {
                context: Some(base.context.into_proto(h)),
                is_logged_in: h.is_logged_in,
                error_category,
                error_message,
                field_name,
                distribution: h.distribution.clone(),
            }),
        }
    }
}

/// Request body: a batch of events.
#[derive(Clone, Debug, Deserialize)]
pub struct AnalyticsBatch {
    pub events: Vec<AnalyticsEventReq>,
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// Build the `202 Accepted` relay response.
fn accepted(accepted: usize, skipped: usize) -> Response {
    (
        StatusCode::ACCEPTED,
        axum::Json(serde_json::json!({ "accepted": accepted, "skipped": skipped })),
    )
        .into_response()
}

/// `POST /api/v1/analytics/events` — relay a batch of docs analytics events to
/// Vortex, consent-gated and fire-and-forget.
///
/// We take the body as raw `serde_json::Value` and deserialize it ourselves so a
/// malformed / unknown `event_type` returns the crate's `{code,message}` error
/// shape (`400 invalid_event`) rather than axum's default plain-text 400.
pub async fn post_events(
    State(state): State<SharedState>,
    axum::Json(raw): axum::Json<serde_json::Value>,
) -> Response {
    let batch = match serde_json::from_value::<AnalyticsBatch>(raw) {
        Ok(batch) => batch,
        Err(e) => return bad_request_coded("invalid_event", &e.to_string()),
    };

    let n = batch.events.len();

    // Consent gate — reuse the identity gate (see `GET /api/v1/identity`).
    let analytics_enabled = !state.do_not_track && state.send_anonymous_usage_stats;
    if !analytics_enabled {
        return accepted(0, n);
    }

    // Hydrate server-authoritative fields once, then apply to every event.
    let hydration = state.telemetry_hydration();
    for event in batch.events {
        state.analytics.emit(event.into_event(&hydration));
    }
    accepted(n, 0)
}

#[cfg(test)]
#[path = "analytics_tests.rs"]
mod tests;
