use dbt_telemetry::{
    ExecutionPhase, NodeEvaluated, PhaseExecuted, SpanEndInfo, SpanStartInfo, SpanStatus,
    StatusCode, TelemetryAttributes,
};
use tracing::level_filters::LevelFilter;

use crate::{
    constants::{ANALYZING, RENDERING, RUNNING},
    logging::{LogFormat, StatEvent, TermEvent},
    tracing::{
        data_provider::DataProvider,
        layer::{ConsumerLayer, TelemetryConsumer},
        shared_writer::SharedWriter,
    },
};

/// Build TUI layer that handles all terminal user interface on stdout and stderr, including progress bars
pub fn build_tui_layer<S: SharedWriter + 'static, E: SharedWriter + 'static>(
    std_writer: S,
    err_writer: E,
    max_log_verbosity: LevelFilter,
    log_format: LogFormat,
) -> ConsumerLayer {
    let is_terminal = std_writer.is_terminal();

    // Enables progress bar for now.
    let is_interactive = log_format == LogFormat::Default && is_terminal;
    let enable_color = is_terminal;

    Box::new(
        TuiLayer::new(
            Box::new(std_writer),
            Box::new(err_writer),
            is_interactive,
            enable_color,
        )
        .with_filter(max_log_verbosity),
    )
}

/// A tracing layer that handles all terminal user interface on stdout and stderr, including progress bars.
///
/// As of today this is a bridge into existing logging-based setup, but eventually
/// should own the progress bar manager itself
pub struct TuiLayer {
    #[allow(unused)]
    std_writer: Box<dyn SharedWriter>,
    #[allow(unused)]
    err_writer: Box<dyn SharedWriter>,
    /// Enables progress bar for now.
    is_interactive: bool,
    /// If true, text will be formatted with color.
    #[allow(unused)]
    enable_color: bool,
}

impl TuiLayer {
    pub fn new(
        std_writer: Box<dyn SharedWriter>,
        err_writer: Box<dyn SharedWriter>,
        is_interactive: bool,
        enable_color: bool,
    ) -> Self {
        Self {
            std_writer,
            err_writer,
            is_interactive,
            enable_color,
        }
    }
}

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
    // Check if this is a PhaseExecuted event
    if let Some(phase) = attributes.downcast_ref::<PhaseExecuted>() {
        let total = phase.node_count_total.unwrap_or_default();

        return match phase.phase() {
            ExecutionPhase::Render => Some((RENDERING, total, None)),
            ExecutionPhase::Analyze => Some((ANALYZING, total, None)),
            ExecutionPhase::Run => Some((RUNNING, total, None)),
            _ => {
                // Not one of the phase we support currently
                None
            }
        };
    }

    // Check if this is a NodeEvaluated event
    if let Some(node) = attributes.downcast_ref::<NodeEvaluated>() {
        let phase = node.phase();

        return match phase {
            ExecutionPhase::Render => Some((RENDERING, 0, Some(node.unique_id.as_str()))),
            ExecutionPhase::Analyze => Some((ANALYZING, 0, Some(node.unique_id.as_str()))),
            ExecutionPhase::Run => Some((RUNNING, 0, Some(node.unique_id.as_str()))),
            _ => {
                // Not one of the phase we support currently
                None
            }
        };
    }

    None
}

impl TelemetryConsumer for TuiLayer {
    fn on_span_start(&self, span: &SpanStartInfo, _: &DataProvider<'_>) {
        if !self.is_interactive {
            return;
        }
        if let Some((bar_uid, total, item)) = get_progress_params(&span.attributes) {
            // TODO: switch to direct interface with progress bar ocntroller
            // Create progress bar via log
            match item {
                None if total > 0 => log::info!(
                    _TERM_ONLY_ = true,
                    _TERM_EVENT_:serde = TermEvent::start_bar(bar_uid.into(), total);
                    "Starting progress bar with uid: {bar_uid}, total: {total}"
                ),
                Some(item) => {
                    let formatted_item = format_progress_item(item);
                    log::info!(
                        _TERM_ONLY_ = true,
                        _TERM_EVENT_:serde = TermEvent::add_bar_context_item(
                            bar_uid.into(),
                            formatted_item.clone()
                        );
                        "Updating progress for uid: {bar_uid}, item: {formatted_item}"
                    )
                }
                _ => {}
            };
        }
    }

    fn on_span_end(&self, span: &SpanEndInfo, _: &DataProvider<'_>) {
        if !self.is_interactive {
            return;
        }
        if let Some((bar_uid, total, item)) = get_progress_params(&span.attributes) {
            // TODO: switch to direct interface with progress bar ocntroller
            // Create progress bar via log
            match item {
                None if total > 0 => log::info!(
                    _TERM_ONLY_ = true,
                    _TERM_EVENT_:serde = TermEvent::remove_bar(bar_uid.into());
                    "Finishing progress bar with uid: {bar_uid}, total: {total}"
                ),
                Some(item) => {
                    let status = if let Some(SpanStatus {
                        code: StatusCode::Error,
                        ..
                    }) = &span.status
                    {
                        "failed"
                    } else {
                        "succeeded"
                    };

                    let formatted_item = format_progress_item(item);
                    log::info!(
                        _TERM_ONLY_ = true,
                        _STAT_EVENT_:serde = StatEvent::counter(status, 1),
                        _TERM_EVENT_:serde = TermEvent::finish_bar_context_item(
                            bar_uid.into(),
                            formatted_item.clone()
                        );
                        "Finishing item: {bar_uid} on progress bar: {formatted_item}"
                    )
                }
                _ => {}
            };
        }
    }
}
