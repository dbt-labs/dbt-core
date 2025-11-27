use std::{
    collections::HashSet,
    io::{self, Write},
    sync::atomic::{AtomicBool, Ordering},
};

use console::Term;
use dbt_telemetry::{
    AnyTelemetryEvent, ExecutionPhase, Invocation, ListItemOutput, LogMessage, LogRecordInfo,
    NodeEvaluated, NodeOutcome, NodeProcessed, NodeSkipReason, NodeType, PhaseExecuted,
    QueryExecuted, SeverityNumber, ShowDataOutput, SpanEndInfo, SpanStartInfo, SpanStatus,
    StatusCode, TelemetryAttributes, TelemetryOutputFlags, UserLogMessage, node_processed,
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
            invocation::format_invocation_summary,
            layout::format_delimiter,
            log_message::format_log_message,
            node::{
                format_node_evaluated_end, format_node_evaluated_start, format_node_processed_end,
                format_skipped_test_group,
            },
            test_result::format_test_failure,
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

    Box::new(TuiLayer::new(
        max_log_verbosity,
        is_interactive,
        show_options,
        command,
    ))
}

/// A special non-exported event type used grouping all `NodeProcessed` spans under
/// a same root, used to report skipped test nodes on one line on the console in
/// interactive TUI mode.
#[derive(Debug, Clone, Copy)]
pub struct TuiAllProcessingNodesGroup;

impl AnyTelemetryEvent for TuiAllProcessingNodesGroup {
    fn event_type(&self) -> &'static str {
        "v1.internal.events.fusion.node.TuiAllProcessingNodesGroup"
    }

    fn event_display_name(&self) -> String {
        "TuiAllProcessingNodesGroup".to_string()
    }

    fn record_category(&self) -> dbt_telemetry::TelemetryEventRecType {
        dbt_telemetry::TelemetryEventRecType::Span
    }

    fn output_flags(&self) -> TelemetryOutputFlags {
        TelemetryOutputFlags::OUTPUT_CONSOLE
    }

    fn event_eq(&self, other: &dyn AnyTelemetryEvent) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .map(|other_ref| std::ptr::eq(self, other_ref))
            .unwrap_or(false)
    }

    fn has_sensitive_data(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn clone_box(&self) -> Box<dyn AnyTelemetryEvent> {
        Box::new(*self)
    }

    fn to_json(&self) -> Result<serde_json::Value, String> {
        Ok(serde_json::json!({}))
    }
}

/// Holds a vector of skipped test node names and flags indicating which types were seen
struct SkippedTestNodes {
    pending_names: Vec<String>,
    seen_test: bool,
    seen_unit_test: bool,
}

fn emit_pending_skips(data_provider: &mut DataProvider<'_>) {
    // This is a non-skipping node, check if there are pending skipped tests to emit
    let mut output_to_emit = None;
    data_provider.with_ancestor_mut::<TuiAllProcessingNodesGroup, SkippedTestNodes>(|skipped| {
        if !skipped.pending_names.is_empty() {
            // Format the summary and capture it for emission after lock is released
            output_to_emit = Some(format_skipped_test_group(
                &skipped.pending_names,
                skipped.seen_test,
                skipped.seen_unit_test,
                true,
            ));

            // Clear the pending names and flags
            skipped.pending_names.clear();
            skipped.seen_test = false;
            skipped.seen_unit_test = false;
        }
    });

    // Emit the output after the span lock has been released to avoid possible deadlocks
    if let Some(output) = output_to_emit {
        with_suspended_progress_bars(|| {
            io::stdout()
                .lock()
                .write_all(format!("{}\n", output).as_bytes())
                .expect("failed to write to stdout");
        });
    }
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
    /// TUI has complex filtering logic, so we store max log verbosity here,
    /// instead of applying a blanket filter on the whole layer
    max_log_verbosity: LevelFilter,
    max_term_line_width: Option<usize>,
    /// Enables progress bar for now.
    is_interactive: bool,
    show_options: HashSet<ShowOptions>,
    command: FsCommand,
    /// Track if we've emitted the list header yet
    list_header_emitted: AtomicBool,
    /// Whether to group skipped tests under TuiAllProcessingNodesGroup spans
    group_skipped_tests: bool,
}

impl TuiLayer {
    pub fn new(
        max_log_verbosity: LevelFilter,
        is_interactive: bool,
        show_options: HashSet<ShowOptions>,
        command: FsCommand,
    ) -> Self {
        let stdout_term = Term::stdout();
        let is_interactive = is_interactive && stdout_term.is_term();
        let max_term_line_width = stdout_term.size_checked().map(|(_, cols)| cols as usize);

        Self {
            max_log_verbosity,
            max_term_line_width,
            is_interactive,
            show_options,
            command,
            list_header_emitted: AtomicBool::new(false),
            group_skipped_tests: is_interactive && max_log_verbosity < LevelFilter::DEBUG,
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
    fn is_span_enabled(&self, span: &SpanStartInfo) -> bool {
        span.attributes
            .output_flags()
            .contains(TelemetryOutputFlags::OUTPUT_CONSOLE)
            // NodeEvaluated are at debug level, but should always be let through
            // because of progress bars relying on them. Their output is controlled
            // in the handler based on the verbosity level.
            && (span.attributes.is::<NodeEvaluated>()
                || span
                    .severity_number
                    .try_into()
                    .is_ok_and(|level: tracing::Level| level <= self.max_log_verbosity))
    }

    fn is_log_enabled(&self, log_record: &LogRecordInfo) -> bool {
        log_record
            .attributes
            .output_flags()
            .contains(TelemetryOutputFlags::OUTPUT_CONSOLE)
            && log_record
                .severity_number
                .try_into()
                .is_ok_and(|level: tracing::Level| level <= self.max_log_verbosity)
    }

    fn on_span_start(&self, span: &SpanStartInfo, data_provider: &mut DataProvider<'_>) {
        // Init delayed messages storage on root span start
        if span.parent_span_id.is_none() {
            // Root span
            data_provider.init_root(DelayedMessages {
                test_failures: Vec::new(),
                errors_and_warnings: Vec::new(),
            });
        }

        // Init skipped test nodes storage
        if span.attributes.is::<TuiAllProcessingNodesGroup>() {
            data_provider.init_cur::<SkippedTestNodes>(SkippedTestNodes {
                pending_names: Vec::new(),
                seen_test: false,
                seen_unit_test: false,
            });
        }

        // Handle NodeEvaluated start (only in debug mode)
        if self.max_log_verbosity >= LevelFilter::DEBUG {
            if let Some(ne) = span.attributes.downcast_ref::<NodeEvaluated>() {
                self.handle_node_evaluated_start(span, ne);
            }
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
        // Handle QueryExecuted events
        if let Some(query_data) = span.attributes.downcast_ref::<QueryExecuted>() {
            self.handle_query_executed(span, query_data);
            return;
        }

        // Handle NodeProcessed events for completed nodes
        if let Some(node_processed) = span.attributes.downcast_ref::<NodeProcessed>() {
            self.handle_node_processed(span, node_processed, data_provider);
        }

        // Handle NodeEvaluated end (only in debug mode)
        if self.max_log_verbosity >= LevelFilter::DEBUG {
            if let Some(ne) = span.attributes.downcast_ref::<NodeEvaluated>() {
                self.handle_node_evaluated_end(span, ne);
            }
        }

        // Handle close of TuiAllProcessingNodesGroup in case we have pending skipped tests to emit
        // after all nodes have been processed
        if span.attributes.is::<TuiAllProcessingNodesGroup>()
            && self.show_options.contains(&ShowOptions::Completed)
        {
            emit_pending_skips(data_provider);
        }

        if let Some(invocation) = span.attributes.downcast_ref::<Invocation>() {
            self.handle_invocation_end(span, invocation, data_provider);
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
            self.handle_log_message(log_msg, log_record, data_provider);
            return;
        }

        // Handle simple events that print just the body on it's own line: UserLogMessage
        if log_record.attributes.is::<UserLogMessage>() {
            self.handle_user_log_message(log_record);
            return;
        }

        // Handle simple events that print just the body: StdoutMessage
        if log_record.attributes.is::<StdoutMessage>() {
            self.handle_stdout_message(log_record);
            return;
        }

        // Handle ListItemOutput - only show for list commad unconditionally,
        // or if ShowOptions::Nodes is enabled for other commands
        if let Some(list_item) = log_record.attributes.downcast_ref::<ListItemOutput>() {
            self.handle_list_item_output(list_item);
            return;
        }

        // Handle ShowDataOutput - always show unconditionally. Call-sites decide whether to emit or not.
        if let Some(show_data) = log_record.attributes.downcast_ref::<ShowDataOutput>() {
            self.handle_show_data_output(show_data);
            return;
        }

        // Handle StderrMessage
        if let Some(stderr_message) = log_record.attributes.downcast_ref::<StderrMessage>() {
            self.handle_stderr_message(stderr_message, log_record);
        }
    }
}

impl TuiLayer {
    fn handle_invocation_end(
        &self,
        span: &SpanEndInfo,
        invocation: &Invocation,
        data_provider: &mut DataProvider<'_>,
    ) {
        // Print any delayed messages first
        data_provider.with_root::<DelayedMessages>(|delayed_messages| {
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
    }

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

    fn handle_node_evaluated_start(&self, _span: &SpanStartInfo, ne: &NodeEvaluated) {
        let formatted = format_node_evaluated_start(ne, true);
        with_suspended_progress_bars(|| {
            io::stdout()
                .lock()
                .write_all(format!("{}\n", formatted).as_bytes())
                .expect("failed to write to stdout");
        });
    }

    fn handle_node_evaluated_end(&self, span: &SpanEndInfo, ne: &NodeEvaluated) {
        let duration = span
            .end_time_unix_nano
            .duration_since(span.start_time_unix_nano)
            .unwrap_or_default();
        let formatted = format_node_evaluated_end(ne, duration, true);
        with_suspended_progress_bars(|| {
            io::stdout()
                .lock()
                .write_all(format!("{}\n", formatted).as_bytes())
                .expect("failed to write to stdout");
        });
    }

    fn handle_query_executed(&self, _span: &SpanEndInfo, query_data: &QueryExecuted) {
        let node_id = query_data.unique_id.as_deref().unwrap_or("unknown");
        let formatted_query = format!("Query executed on node {}:\n{}", node_id, query_data.sql);
        with_suspended_progress_bars(|| {
            io::stdout()
                .lock()
                .write_all(format!("{}\n", formatted_query).as_bytes())
                .expect("failed to write to stdout");
        });
    }

    fn handle_node_processed(
        &self,
        span: &SpanEndInfo,
        node: &NodeProcessed,
        data_provider: &mut DataProvider<'_>,
    ) {
        // Skip nodes with unspecified outcome
        if node.node_outcome() == NodeOutcome::Unspecified {
            return;
        }

        // Skip NoOp nodes (similar to the macro logic), which includes frontier nodes
        if matches!(node.node_skip_reason(), NodeSkipReason::NoOp) {
            return;
        }

        // Do not emit for non-selected nodes (e.g. when model is analyzed in test command)
        if !node.in_selection {
            return;
        }

        // Do not report successful nodes in compile, to reduce verbosity. Unless in debug mode.
        if node.node_outcome() == NodeOutcome::Success
            && self.command == FsCommand::Compile
            && self.max_log_verbosity < LevelFilter::DEBUG
        {
            return;
        }

        // Determine if the current node is a skipped test
        let is_current_node_skipped_test = (node.node_type() == NodeType::Test
            || node.node_type() == NodeType::UnitTest)
            && matches!(node.node_skip_reason(), NodeSkipReason::Upstream);

        // Check if we need to emit pending skipped tests summary before processing current node
        if !is_current_node_skipped_test
            && self.group_skipped_tests
            && self.show_options.contains(&ShowOptions::Completed)
        {
            emit_pending_skips(data_provider);
        }

        // Capture and delay unit test summary messages regardless of show options
        if (node.node_type() == NodeType::Test || node.node_type() == NodeType::UnitTest)
            && node.node_outcome() == NodeOutcome::Success
            && let Some(node_processed::NodeOutcomeDetail::NodeTestDetail(t_outcome)) =
                &node.node_outcome_detail
            && let Some(diff_table) = t_outcome.diff_table.as_ref()
        {
            // This is a failed test, capture its summary diff table to be printed on stdout later
            data_provider.with_root_mut::<DelayedMessages>(|delayed_messages| {
                delayed_messages.test_failures.push(DelayedMessage {
                    message: format!("{}\n", format_test_failure(&node.name, diff_table, true)),
                });
            });
        }

        // In interactive non-debug mode, accumulate skipped test nodes instead of printing them individually
        if is_current_node_skipped_test && self.group_skipped_tests {
            // Find an ancestor TuiAllProcessingNodesGroup span and add this node to it
            data_provider.with_ancestor_mut::<TuiAllProcessingNodesGroup, SkippedTestNodes>(
                |skipped| {
                    skipped.pending_names.push(node.name.clone());
                    if node.node_type() == NodeType::Test {
                        skipped.seen_test = true;
                    } else if node.node_type() == NodeType::UnitTest {
                        skipped.seen_unit_test = true;
                    }
                },
            );
            // Skip normal printing - our tests will catch if we break something
            return;
        }

        // Only show if ShowOptions::Completed is enabled
        if !self.show_options.contains(&ShowOptions::Completed) {
            return;
        }

        // Use the accumulated duration_ms from NodeProcessed, which reflects actual
        // processing time across all phases (excluding time waiting for upstream nodes).
        // Fall back to span duration if duration_ms is not available.
        let duration = node
            .duration_ms
            .map(std::time::Duration::from_millis)
            .unwrap_or_else(|| {
                span.end_time_unix_nano
                    .duration_since(span.start_time_unix_nano)
                    .unwrap_or_default()
            });

        // Format the output line using the formatter with color enabled
        let output = format_node_processed_end(node, duration, true);

        // Print to stdout with progress bars suspended
        with_suspended_progress_bars(|| {
            io::stdout()
                .lock()
                .write_all(format!("{}\n", output).as_bytes())
                .expect("failed to write to stdout");
        });
    }

    fn handle_log_message(
        &self,
        log_msg: &LogMessage,
        log_record: &LogRecordInfo,
        data_provider: &mut DataProvider<'_>,
    ) {
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
            data_provider.with_root_mut::<DelayedMessages>(|delayed_messages| {
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
    }

    fn handle_user_log_message(&self, log_record: &LogRecordInfo) {
        // Print user log messages immediately to stdout
        with_suspended_progress_bars(|| {
            io::stdout()
                .lock()
                .write_all(format!("{}\n", log_record.body).as_bytes())
                .expect("failed to write to stdout");
        });
    }

    fn handle_stdout_message(&self, log_record: &LogRecordInfo) {
        // Print immediately to stdout
        with_suspended_progress_bars(|| {
            io::stdout()
                .lock()
                .write_all(log_record.body.as_bytes())
                .expect("failed to write to stdout");
        });
    }

    fn handle_list_item_output(&self, list_item: &ListItemOutput) {
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
    }

    fn handle_show_data_output(&self, show_data: &ShowDataOutput) {
        with_suspended_progress_bars(|| {
            let mut stdout = io::stdout().lock();

            stdout
                .write_all(format!("{}\n", show_data.content).as_bytes())
                .expect("failed to write show data to stdout");
        });
    }

    fn handle_stderr_message(&self, stderr_message: &StderrMessage, log_record: &LogRecordInfo) {
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
