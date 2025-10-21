use std::time::Duration;

use console::Style;

use dbt_telemetry::{Invocation, NodeType, SpanEndInfo};
use itertools::Itertools;

use crate::{
    pretty_string::{BLUE, DIM, GREEN, MAGENTA, RED, WHITE, YELLOW},
    tracing::{
        data_provider::DataProvider,
        formatters::color::maybe_apply_color,
        metrics::{InvocationMetricKey, MetricKey},
    },
};

use super::duration::format_duration_for_summary;

/// Commands that should not include the result breakdown line.
const RESULT_LINE_OPT_OUT_COMMANDS: [&str; 1] = ["man"];
/// Commands that should include the extended evaluation/result breakdown.
const SUMMARY_COMMANDS: [&str; 6] = ["build", "compile", "run", "seed", "snapshot", "test"];

/// Type sfe way to get ordering of all supported node types for summary display.
const fn node_type_to_order(node_type: NodeType) -> u8 {
    match node_type {
        NodeType::Model => 1,
        NodeType::Test => 2,
        NodeType::Snapshot => 3,
        NodeType::Seed => 4,
        NodeType::Source => 5,
        NodeType::Exposure => 6,
        NodeType::Metric => 7,
        NodeType::SemanticModel => 8,
        NodeType::SavedQuery => 9,
        NodeType::Analysis => 10,
        NodeType::Operation => 11,
        NodeType::UnitTest => 12,
        NodeType::Function => 13,
        NodeType::Macro => 14,
        NodeType::DocsMacro => 15,
        NodeType::Unspecified => 16,
    }
}

/// Get the plural name of a NodeType
pub fn node_type_plural(node: NodeType) -> &'static str {
    match node {
        NodeType::Unspecified => "unspecified",
        NodeType::Model => "models",
        NodeType::Seed => "seeds",
        NodeType::Snapshot => "snapshots",
        NodeType::Source => "sources",
        NodeType::Test => "tests",
        NodeType::UnitTest => "unit tests",
        NodeType::Macro => "macros",
        NodeType::DocsMacro => "doc macros",
        NodeType::Analysis => "analyses",
        NodeType::Operation => "operations",
        NodeType::Exposure => "exposures",
        NodeType::Metric => "metrics",
        NodeType::SavedQuery => "saved queries",
        NodeType::SemanticModel => "semantic models",
        NodeType::Function => "functions",
    }
}

#[derive(Debug)]
struct InvocationOutcomeTotals {
    total: u64,
    success: u64,
    warn: u64,
    error: u64,
    reused: u64,
    skipped: u64,
    canceled: u64,
}

#[derive(Debug)]
struct InvocationMetricsSnapshot {
    warnings: u64,
    errors: u64,
    autofix: u64,
    outcomes: InvocationOutcomeTotals,
}

#[derive(Debug)]
struct InvocationSummaryInput<'a> {
    command: &'a str,
    target: Option<&'a str>,
    elapsed: Duration,
    metrics: InvocationMetricsSnapshot,
}

#[derive(Debug)]
pub struct FormattedInvocationSummary {
    summary_lines: Option<Vec<String>>,
    autofix_line: Option<String>,
}

impl FormattedInvocationSummary {
    pub fn summary_lines(&self) -> Option<&[String]> {
        self.summary_lines.as_deref()
    }

    pub fn autofix_line(&self) -> Option<&str> {
        self.autofix_line.as_deref()
    }
}

/// Extract structured invocation data from span attributes if available.
fn extract_invocation_command_and_target(attributes: &Invocation) -> (&str, Option<&str>) {
    let command = attributes
        .eval_args
        .as_ref()
        .map(|args| args.command.as_str())
        .unwrap_or("unknown");

    let target = attributes
        .eval_args
        .as_ref()
        .and_then(|args| args.target.as_deref());

    (command, target)
}

fn collect_outcome_totals(data_provider: &DataProvider<'_>) -> InvocationOutcomeTotals {
    let success = data_provider.get_metric(MetricKey::InvocationMetric(
        InvocationMetricKey::NodeTotalsSuccess,
    ));
    let warn = data_provider.get_metric(MetricKey::InvocationMetric(
        InvocationMetricKey::NodeTotalsWarning,
    ));
    let error = data_provider.get_metric(MetricKey::InvocationMetric(
        InvocationMetricKey::NodeTotalsError,
    ));
    let reused = data_provider.get_metric(MetricKey::InvocationMetric(
        InvocationMetricKey::NodeTotalsReused,
    ));
    let skipped = data_provider.get_metric(MetricKey::InvocationMetric(
        InvocationMetricKey::NodeTotalsSkipped,
    ));
    let canceled = data_provider.get_metric(MetricKey::InvocationMetric(
        InvocationMetricKey::NodeTotalsCanceled,
    ));

    InvocationOutcomeTotals {
        total: success + warn + error + reused + skipped + canceled,
        success,
        warn,
        error,
        reused,
        skipped,
        canceled,
    }
}

/// Collects invocation-level metrics exposed through the data provider for the Invocation span.
fn collect_invocation_metrics(data_provider: &DataProvider<'_>) -> InvocationMetricsSnapshot {
    let warnings = data_provider.get_metric(MetricKey::InvocationMetric(
        InvocationMetricKey::TotalWarnings,
    ));
    let errors = data_provider.get_metric(MetricKey::InvocationMetric(
        InvocationMetricKey::TotalErrors,
    ));
    let autofix = data_provider.get_metric(MetricKey::InvocationMetric(
        InvocationMetricKey::AutoFixSuggestions,
    ));

    let outcomes = collect_outcome_totals(data_provider);

    InvocationMetricsSnapshot {
        warnings,
        errors,
        autofix,
        outcomes,
    }
}

/// Formats invocation summary output (both colored and non-colored variants).
pub fn format_invocation_summary(
    span: &SpanEndInfo,
    invocation: &Invocation,
    data_provider: &DataProvider<'_>,
    colorize: bool,
    max_line_width: Option<usize>,
) -> FormattedInvocationSummary {
    let (command, target) = extract_invocation_command_and_target(invocation);

    // Exit early if the command is opted out of summary display
    if RESULT_LINE_OPT_OUT_COMMANDS
        .iter()
        .any(|cmd| cmd.eq_ignore_ascii_case(command))
    {
        return FormattedInvocationSummary {
            summary_lines: None,
            autofix_line: None,
        };
    }

    let metrics: InvocationMetricsSnapshot = collect_invocation_metrics(data_provider);
    let elapsed = span
        .end_time_unix_nano
        .duration_since(span.start_time_unix_nano)
        .unwrap_or_default();

    let summary = InvocationSummaryInput {
        command,
        target,
        elapsed,
        metrics,
    };

    let mut lines = Vec::new();

    // Start with a blank line for spacing
    lines.push(String::new());

    // Insert a centered execution summary delimiter line
    let max_line_width = max_line_width.unwrap_or(60);
    let header_raw = format!("{:=^width$}", " Execution Summary ", width = max_line_width);
    let header = maybe_apply_color(&DIM, &header_raw, colorize);
    lines.push(header);

    lines.push(format_status_line(&summary, colorize));

    if SUMMARY_COMMANDS
        .iter()
        .any(|command| command.eq_ignore_ascii_case(summary.command))
    {
        if let Some(evaluated) = format_evaluated_line(data_provider, colorize) {
            lines.push(evaluated);
        }
        if let Some(result) = format_result_line(&summary.metrics.outcomes, colorize) {
            lines.push(result);
        }
    }

    let autofix_line = if summary.metrics.autofix > 0 {
        Some(format_autofix_line(colorize))
    } else {
        None
    };

    FormattedInvocationSummary {
        summary_lines: Some(lines),
        autofix_line,
    }
}

fn format_status_line(summary: &InvocationSummaryInput<'_>, colorize: bool) -> String {
    let duration = maybe_apply_color(
        &DIM,
        format!("[{}]", format_duration_for_summary(summary.elapsed)).as_str(),
        colorize,
    );
    let status = format_status_text(summary.metrics.errors, summary.metrics.warnings, colorize);

    let command = maybe_apply_color(&WHITE, summary.command, colorize);
    let maybe_target = summary
        .target
        .map(|target| {
            format!(
                " for target '{}'",
                maybe_apply_color(&WHITE, target, colorize)
            )
        })
        .unwrap_or_default();

    format!("Finished '{command}' {status}{maybe_target} {duration}")
}

fn format_status_text(errors: u64, warnings: u64, colorize: bool) -> String {
    match (errors, warnings) {
        (0, 0) => maybe_apply_color(&GREEN, "successfully", colorize),
        (0, warn) => format!(
            "with {}",
            colored_count(warn, "warning", "warnings", colorize, &YELLOW)
        ),
        (err, 0) => format!(
            "with {}",
            colored_count(err, "error", "errors", colorize, &RED)
        ),
        (err, warn) => format!(
            "with {} and {}",
            colored_count(warn, "warning", "warnings", colorize, &RED),
            colored_count(err, "error", "errors", colorize, &RED),
        ),
    }
}

fn format_evaluated_line(data_provider: &DataProvider<'_>, _colorize: bool) -> Option<String> {
    let mut parts = Vec::new();

    for (node_type, count) in data_provider
        .get_all_metrics()
        .iter()
        .filter_map(|(key, count)| match key {
            MetricKey::NodeCounts(node_type) => Some((*node_type, count)),
            _ => None,
        })
        .sorted_by(|a, b| {
            let order_a = node_type_to_order(a.0);
            let order_b = node_type_to_order(b.0);
            order_a.cmp(&order_b)
        })
    {
        if *count == 0 {
            continue;
        }

        let word = if *count > 1 {
            node_type_plural(node_type)
        } else {
            node_type.pretty()
        };

        parts.push(format!("{} {}", count, word));
    }

    if parts.is_empty() {
        return None;
    }

    Some(format!("Processed: {}", parts.join(" | ")))
}

fn format_result_line(outcomes: &InvocationOutcomeTotals, colorize: bool) -> Option<String> {
    if outcomes.total == 0 {
        return None;
    }

    let mut segments = Vec::new();
    segments.push(colored_metric(outcomes.total, "total", colorize, &WHITE));

    if outcomes.success > 0 {
        segments.push(colored_metric(
            outcomes.success,
            "success",
            colorize,
            &GREEN,
        ));
    }

    if outcomes.reused > 0 {
        segments.push(colored_metric(
            outcomes.reused,
            "reused",
            colorize,
            &MAGENTA,
        ));
    }
    if outcomes.warn > 0 {
        segments.push(colored_metric(outcomes.warn, "warn", colorize, &YELLOW));
    }
    if outcomes.error > 0 {
        segments.push(colored_metric(outcomes.error, "error", colorize, &RED));
    }
    if outcomes.skipped > 0 {
        segments.push(colored_metric(outcomes.skipped, "skipped", colorize, &DIM));
    }
    if outcomes.canceled > 0 {
        segments.push(colored_metric(
            outcomes.canceled,
            "canceled",
            colorize,
            &DIM,
        ));
    }

    Some(format!("Summary: {}", segments.join(" | ")))
}

fn colored_metric(value: u64, label: &str, colorize: bool, style: &Style) -> String {
    let text = format!("{} {}", value, label);
    maybe_apply_color(style, &text, colorize)
}

fn colored_count(
    value: u64,
    label_single: &str,
    label_plural: &str,
    colorize: bool,
    style: &Style,
) -> String {
    let word = if value == 1 {
        label_single
    } else {
        label_plural
    };

    let text = format!("{} {}", value, word);
    maybe_apply_color(style, &text, colorize)
}

fn format_autofix_line(colorize: bool) -> String {
    let suggestion_label = maybe_apply_color(&BLUE, "suggestion:", colorize);
    let command = maybe_apply_color(&YELLOW, "dbt deps", colorize);
    let url = maybe_apply_color(&BLUE, "https://github.com/dbt-labs/dbt-autofix", colorize);

    format!(
        "{suggestion_label} Run '{}' to see the latest fusion compatible packages. For compatibility errors, try the autofix script: {url}",
        command
    )
}
