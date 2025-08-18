use dbt_telemetry::{
    BuildPhase, BuildPhaseInfo, SpanEndInfo, SpanStartInfo, SpanStatus, StatusCode,
    TelemetryAttributes,
};
use tracing::{Subscriber, span};
use tracing_subscriber::{Layer, layer::Context};

use crate::{
    constants::{ANALYZING, RENDERING, RUNNING},
    logging::{StatEvent, TermEvent},
};

/// A tracing layer that handles spans that should display progress bars on stdout.
///
/// As of today this is a bridge into existing logging-based setup, but eventually
/// should own the progress bar manager itself
pub struct ProgressBarLayer;

fn format_progress_item(unique_id: &str) -> String {
    // Split the unique_id into parts by '.' and take the first and last as the resource type and name
    let parts: Vec<&str> = unique_id.split('.').collect();
    let resource_type = parts.first().unwrap_or(&"unknown");
    let name = parts.last().unwrap_or(&"unknown");
    format!("{resource_type}:{name}")
}

fn get_progress_params(
    attributes: &TelemetryAttributes,
) -> Option<(&'static str, u64, Option<&str>)> {
    match attributes {
        TelemetryAttributes::Phase(phase_info) => {
            match phase_info {
                BuildPhaseInfo::Compiling {
                    node_count: total, ..
                } => Some((RENDERING, *total, None)),
                BuildPhaseInfo::Analyzing {
                    node_count: total, ..
                } => Some((ANALYZING, *total, None)),
                BuildPhaseInfo::Executing {
                    node_count: total, ..
                } => Some((RUNNING, *total, None)),
                _ => {
                    // Not one of the phase we support currently
                    None
                }
            }
        }
        TelemetryAttributes::Node { node_id, phase, .. } => {
            match phase {
                BuildPhase::Compiling => Some((RENDERING, 0, Some(node_id.unique_id.as_str()))),
                BuildPhase::Analyzing => Some((ANALYZING, 0, Some(node_id.unique_id.as_str()))),
                BuildPhase::Executing => Some((RUNNING, 0, Some(node_id.unique_id.as_str()))),
                _ => {
                    // Not one of the phase we support currently
                    None
                }
            }
        }
        _ => None,
    }
}

impl<S> Layer<S> for ProgressBarLayer
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_new_span(&self, _attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let span = ctx
            .span(id)
            .expect("Span must exist for id in the current context");

        // Get the info TelemetryRecord from extensions. It must be there unless we messed
        // up data layer / layer order.
        if let Some(record) = span.extensions().get::<SpanStartInfo>() {
            if let Some((bar_uid, total, item)) = get_progress_params(&record.attributes) {
                // TODO: switch to direct interface with progress bar ocntroller
                // Create progress bar via log

                match item {
                    // Main progress bar. Start only if total > 0
                    None if total > 0 => log::info!(
                        _TERM_ONLY_ = true,
                        _TERM_EVENT_:serde = TermEvent::start_bar(bar_uid.into(), total);
                        "Starting progress bar with uid: {bar_uid}, total: {total}"
                    ),
                    Some(item) => {
                        let formatted_item = format_progress_item(item);
                        log::info!(
                            _TERM_ONLY_ = true,
                            _TERM_EVENT_:serde = TermEvent::add_bar_context_item(bar_uid.into(), formatted_item.clone());
                            "Updating progress for uid: {bar_uid}, item: {formatted_item}"
                        )
                    }
                    _ => {}
                };
            }
        } else {
            unreachable!("Unexpectedly missing span start data!");
        };
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        let span = ctx
            .span(&id)
            .expect("Span must exist for id in the current context");

        // Get the info TelemetryRecord from extensions. It must be there unless we messed
        // up data layer / layer order.
        if let Some(record) = span.extensions().get::<SpanEndInfo>() {
            if let Some((bar_uid, total, item)) = get_progress_params(&record.attributes) {
                // TODO: switch to direct interface with progress bar ocntroller
                // Create progress bar via log
                match item {
                    // Main progress bar. stop only if total > 0
                    None if total > 0 => log::info!(
                        _TERM_ONLY_ = true,
                        _TERM_EVENT_:serde = TermEvent::remove_bar(bar_uid.into());
                        "Finishing progress bar with uid: {bar_uid}, total: {total}"
                    ),
                    Some(item) => {
                        let status = if let Some(SpanStatus {
                            code: StatusCode::Error,
                            ..
                        }) = &record.status
                        {
                            "failed"
                        } else {
                            "succeeded"
                        };

                        let formatted_item = format_progress_item(item);
                        log::info!(
                            _TERM_ONLY_ = true,
                            _STAT_EVENT_:serde = StatEvent::counter(
                                status,
                                1
                            ),
                            _TERM_EVENT_:serde = TermEvent::finish_bar_context_item(bar_uid.into(), formatted_item.clone());
                            "Finishing item: {bar_uid} on progress bar: {formatted_item}"
                        )
                    }
                    _ => {}
                };
            }
        } else {
            unreachable!("Unexpectedly missing span start data!");
        };
    }
}
