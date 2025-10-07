use std::collections::HashSet;

use console::Term;
use dbt_telemetry::{
    ExecutionPhase, Invocation, NodeEvaluated, PhaseExecuted, SpanEndInfo, SpanStartInfo,
    SpanStatus, StatusCode, TelemetryAttributes,
};
use tracing::level_filters::LevelFilter;

use crate::{
    constants::{ANALYZING, RENDERING, RUNNING},
    io_args::ShowOptions,
    logging::{LogFormat, StatEvent, TermEvent},
    tracing::{
        data_provider::DataProvider,
        formatters::invocation::format_invocation_summary,
        layer::{ConsumerLayer, TelemetryConsumer},
    },
};

/// Build TUI layer that handles all terminal user interface on stdout and stderr, including progress bars
pub fn build_tui_layer(
    max_log_verbosity: LevelFilter,
    log_format: LogFormat,
    show_options: HashSet<ShowOptions>,
) -> ConsumerLayer {
    // Enables progress bar for now.
    let is_interactive = log_format == LogFormat::Default;

    Box::new(TuiLayer::new(is_interactive, show_options).with_filter(max_log_verbosity))
}

/// A tracing layer that handles all terminal user interface on stdout and stderr, including progress bars.
///
/// As of today this is a bridge into existing logging-based setup, but eventually
/// should own the progress bar manager itself
pub struct TuiLayer {
    stdout_term: Term,
    #[allow(dead_code)]
    stderr_writer: Term,
    /// Enables progress bar for now.
    is_interactive: bool,
    show_options: HashSet<ShowOptions>,
}

impl TuiLayer {
    pub fn new(is_interactive: bool, show_options: HashSet<ShowOptions>) -> Self {
        let stdout_term = Term::stdout();
        let is_interactive = is_interactive && stdout_term.is_term();

        Self {
            stdout_term,
            stderr_writer: Term::stderr(),
            is_interactive,
            show_options,
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
            // Non-interactive mode does not have progress bars
            return;
        }

        // Handle progress bars
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

    fn on_span_end(&self, span: &SpanEndInfo, data_provider: &DataProvider<'_>) {
        if let Some(invocation) = span.attributes.downcast_ref::<Invocation>() {
            self.handle_invocation_summary(span, invocation, data_provider);
            return;
        }

        if !self.is_interactive {
            // Non-interactive mode does not have progress bars
            return;
        }

        // Handle progress bars
        if let Some((bar_uid, total, item)) = get_progress_params(&span.attributes) {
            // TODO: switch to direct interface with progress bar controller
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

impl TuiLayer {
    fn handle_invocation_summary(
        &self,
        span: &SpanEndInfo,
        invocation: &Invocation,
        data_provider: &DataProvider<'_>,
    ) {
        let max_line_width = self
            .stdout_term
            .size_checked()
            .map(|(_, cols)| cols as usize);

        let formatted =
            format_invocation_summary(span, invocation, data_provider, true, max_line_width);

        // Per pre-migration logic, autofix line were always printed ignoring show options
        if let Some(line) = formatted.autofix_line() {
            let _ = self.stdout_term.write_line(line);
        }

        if !self.show_options.contains(&ShowOptions::Completed)
            && !self.show_options.contains(&ShowOptions::All)
        {
            return;
        }

        if let Some(summary_lines) = formatted.summary_lines() {
            for line in summary_lines {
                let _ = self.stdout_term.write_line(line.as_str());
            }
        }
    }
}
