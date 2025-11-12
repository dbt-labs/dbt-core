use std::{
    collections::HashSet,
    io::{self, Write},
    sync::atomic::{AtomicBool, Ordering},
};

use console::Term;
use dbt_telemetry::{
    ExecutionPhase, Invocation, ListItemOutput, LogMessage, LogRecordInfo, NodeEvaluated,
    NodeOutcome, NodeOutcomeDetail, NodeType, PhaseExecuted, SeverityNumber, SpanEndInfo,
    SpanStartInfo, SpanStatus, StatusCode, TelemetryAttributes, UserLogMessage,
};

use dbt_error::ErrorCode;
use tracing::level_filters::LevelFilter;

use crate::{
    constants::{ANALYZING, RENDERING, RUNNING},
    io_args::{FsCommand, ShowOptions},
    logging::{LogFormat, StatEvent, TermEvent, with_suspended_progress_bars},
    tracing::{
        data_provider::DataProvider,
        formatters::{
            format_delimiter, invocation::format_invocation_summary,
            log_message::format_log_message, test_result::format_test_failure,
        },
        layer::{ConsumerLayer, TelemetryConsumer},
        private_events::print_event::{StderrMessage, StdoutMessage},
    },
};

/// Build TUI layer that handles all terminal user interface on stdout and stderr, including progress bars
pub fn build_tui_layer(
    max_log_verbosity: LevelFilter,
    log_format: LogFormat,
    show_options: HashSet<ShowOptions>,
    command: FsCommand,
) -> ConsumerLayer {
    // Enables progress bar for now.
    let is_interactive = log_format == LogFormat::Default;

    Box::new(TuiLayer::new(is_interactive, show_options, command).with_filter(max_log_verbosity))
}

/// Holds a vector of strings to be printed at the end of the invocation
/// This is used to delay printing of unit test failure tables and errors/warning messages
/// towards the end, right before the invocation summary
struct DelayedMessage {
    message: String,
}

struct DelayedMessages {
    test_failures: Vec<DelayedMessage>,
    errors_and_warnings: Vec<DelayedMessage>,
}

/// A tracing layer that handles all terminal user interface on stdout and stderr, including progress bars.
///
/// As of today this is a bridge into existing logging-based setup, but eventually
/// should own the progress bar manager itself
pub struct TuiLayer {
    max_term_line_width: Option<usize>,
    /// Enables progress bar for now.
    is_interactive: bool,
    show_options: HashSet<ShowOptions>,
    command: FsCommand,
    /// Track if we've emitted the list header yet
    list_header_emitted: AtomicBool,
}

impl TuiLayer {
    pub fn new(
        is_interactive: bool,
        show_options: HashSet<ShowOptions>,
        command: FsCommand,
    ) -> Self {
        let stdout_term = Term::stdout();
        let is_interactive = is_interactive && stdout_term.is_term();
        let max_term_line_width = stdout_term.size_checked().map(|(_, cols)| cols as usize);

        Self {
            max_term_line_width,
            is_interactive,
            show_options,
            command,
            list_header_emitted: AtomicBool::new(false),
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
    fn on_span_start(&self, span: &SpanStartInfo, data_provider: &mut DataProvider<'_>) {
        // Init delayed messages storage on root span start
        if span.parent_span_id.is_none() {
            // Root span
            data_provider.init(DelayedMessages {
                test_failures: Vec::new(),
                errors_and_warnings: Vec::new(),
            });
        }

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

    fn on_span_end(&self, span: &SpanEndInfo, data_provider: &mut DataProvider<'_>) {
        if let Some(invocation) = span.attributes.downcast_ref::<Invocation>() {
            // Print any delayed messages first
            data_provider.with::<DelayedMessages>(|delayed_messages| {
                let mut stdout = io::stdout().lock();
                let mut stderr = io::stderr().lock();

                // Print test failures with header if any exist (historically on stdout)
                if !delayed_messages.test_failures.is_empty() {
                    stdout
                        .write_all(
                            format!(
                                "\n{}\n",
                                format_delimiter(" Test Failures ", self.max_term_line_width, true)
                            )
                            .as_bytes(),
                        )
                        .expect("failed to write to stdout");
                    for msg in &delayed_messages.test_failures {
                        stdout
                            .write_all(msg.message.as_bytes())
                            .expect("failed to write to stdout");
                    }
                }

                // Print errors and warnings with header if any exist (on stderr)
                if !delayed_messages.errors_and_warnings.is_empty() {
                    stderr
                        .write_all(
                            format!(
                                "\n{}\n",
                                format_delimiter(
                                    " Errors and Warnings ",
                                    self.max_term_line_width,
                                    true
                                )
                            )
                            .as_bytes(),
                        )
                        .expect("failed to write to stderr");

                    for msg in &delayed_messages.errors_and_warnings {
                        stderr
                            .write_all(msg.message.as_bytes())
                            .expect("failed to write to stderr");
                    }
                }

                // Flush streams if we had any messages
                if !delayed_messages.test_failures.is_empty()
                    || !delayed_messages.errors_and_warnings.is_empty()
                {
                    stderr.flush().expect("failed to write to stderr");
                    stdout.flush().expect("failed to write to stdout");
                }
            });

            // Then print the invocation summary
            self.handle_invocation_summary(span, invocation, data_provider);
            return;
        }

        // Capture and delay unit test summary messages
        if let Some(ne) = span.attributes.downcast_ref::<NodeEvaluated>()
            && (ne.node_type() == NodeType::Test || ne.node_type() == NodeType::UnitTest)
            && ne.node_outcome() == NodeOutcome::Success
            && let Some(NodeOutcomeDetail::NodeTestDetail(t_outcome)) = &ne.node_outcome_detail
            && let Some(diff_table) = t_outcome.diff_table.as_ref()
        {
            // This is a failed test, capture its summary diff table to be printed on stdout later
            data_provider.with_mut::<DelayedMessages>(|delayed_messages| {
                delayed_messages.test_failures.push(DelayedMessage {
                    message: format!("{}\n", format_test_failure(&ne.name, diff_table, true)),
                });
            });
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

    fn on_log_record(&self, log_record: &LogRecordInfo, data_provider: &mut DataProvider<'_>) {
        // Check if this is a LogMessage (error/warning)
        if let Some(log_msg) = log_record.attributes.downcast_ref::<LogMessage>() {
            // Format the message
            let formatted_message = format_log_message(
                log_msg
                    .code
                    .and_then(|c| u16::try_from(c).ok())
                    .and_then(|c| ErrorCode::try_from(c).ok()),
                &log_record.body,
                log_record.severity_number,
                true,
                true,
            );

            // Delay errors and warnings to be printed at the end
            if log_record.severity_number > SeverityNumber::Info {
                data_provider.with_mut::<DelayedMessages>(|delayed_messages| {
                    delayed_messages.errors_and_warnings.push(DelayedMessage {
                        message: format!("{}\n", formatted_message),
                    });
                });
            } else {
                // Print info and below messages immediately
                with_suspended_progress_bars(|| {
                    io::stdout()
                        .lock()
                        .write_all(format!("{}\n", formatted_message).as_bytes())
                        .expect("failed to write to stdout");
                });
            }

            return;
        }

        // Handle simple events that print just the body on it's own line: UserLogMessage
        if log_record.attributes.is::<UserLogMessage>() {
            // Print user log messages immediately to stdout
            with_suspended_progress_bars(|| {
                io::stdout()
                    .lock()
                    .write_all(format!("{}\n", log_record.body).as_bytes())
                    .expect("failed to write to stdout");
            });

            return;
        }

        // Handle simple events that print just the body: StdoutMessage
        if log_record.attributes.is::<StdoutMessage>() {
            // Print immediately to stdout
            with_suspended_progress_bars(|| {
                io::stdout()
                    .lock()
                    .write_all(log_record.body.as_bytes())
                    .expect("failed to write to stdout");
            });

            return;
        }

        // Handle ListItemOutput - only show if ShowOptions::Nodes is enabled
        if let Some(list_item) = log_record.attributes.downcast_ref::<ListItemOutput>() {
            if self.show_options.contains(&ShowOptions::Nodes) || self.command == FsCommand::List {
                with_suspended_progress_bars(|| {
                    let mut stdout = io::stdout().lock();

                    // Emit header once before first list item
                    if !self.list_header_emitted.swap(true, Ordering::Relaxed) {
                        let header = format_delimiter(
                            &ShowOptions::Nodes.title(),
                            self.max_term_line_width,
                            true,
                        );
                        stdout
                            .write_all(format!("{}\n", header).as_bytes())
                            .expect("failed to write header to stdout");
                    }

                    // Print list item content
                    stdout
                        .write_all(format!("{}\n", list_item.content).as_bytes())
                        .expect("failed to write to stdout");
                });
            }

            return;
        }

        // Handle StderrMessage
        if let Some(stderr_message) = log_record.attributes.downcast_ref::<StderrMessage>() {
            // Format the message
            let formatted_message = format_log_message(
                stderr_message.error_code(),
                &log_record.body,
                log_record.severity_number,
                true,
                true,
            );

            // Print user log messages immediately to stdout
            with_suspended_progress_bars(|| {
                io::stderr()
                    .lock()
                    .write_all(format!("{formatted_message}\n").as_bytes())
                    .expect("failed to write to stderr");
            });
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
        let formatted = format_invocation_summary(
            span,
            invocation,
            data_provider,
            true,
            self.max_term_line_width,
        );

        let mut stdout = io::stdout().lock();

        // Per pre-migration logic, autofix line were always printed ignoring show options
        if let Some(line) = formatted.autofix_line() {
            stdout
                .write_fmt(format_args!("{}\n", line))
                .expect("failed to write to stdout");
        }

        if !self.show_options.contains(&ShowOptions::Completed)
            && !self.show_options.contains(&ShowOptions::All)
        {
            return;
        }

        if let Some(summary_lines) = formatted.summary_lines() {
            for line in summary_lines {
                stdout
                    .write_fmt(format_args!("{}\n", line))
                    .expect("failed to write to stdout");
            }
        }
    }
}
