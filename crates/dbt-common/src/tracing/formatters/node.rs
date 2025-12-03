use dbt_telemetry::{
    AnyNodeOutcomeDetail, NodeEvaluated, NodeEvent, NodeMaterialization, NodeOutcome,
    NodeProcessed, NodeSkipReason, NodeType, TestOutcome, get_cache_detail,
    get_node_outcome_detail, get_test_outcome,
};

use super::{
    color::{BLUE, CYAN, GREEN, PLAIN, RED, YELLOW},
    constants::{
        ACTION_WIDTH, MAX_SCHEMA_DISPLAY_LEN, MIN_NODE_TYPE_WIDTH, UNIT_TEST_SCHEMA_SUFFIX,
    },
    duration::format_duration_fixed_width,
    phase::get_phase_action,
};

/// Extract num_failures from test details if available
pub fn get_num_failures(node: NodeEvent) -> Option<i32> {
    get_node_outcome_detail(node).and_then(|detail| {
        if let AnyNodeOutcomeDetail::NodeTestDetail(test_detail) = detail {
            Some(test_detail.failing_rows)
        } else {
            None
        }
    })
}

/// Format schema and alias with truncation for long schema names
/// If schema is longer than MAX characters, truncate to "long_schema_na....::alias"
pub fn format_schema_alias(schema: &str, alias: &str, colorize: bool) -> String {
    let schema = if schema.len() > MAX_SCHEMA_DISPLAY_LEN {
        format!("{}...", &schema[..MAX_SCHEMA_DISPLAY_LEN.saturating_sub(4)])
    } else {
        format!("{schema}.")
    };
    if !colorize {
        return format!("{}{}", schema, alias);
    }

    format!("{}{}", CYAN.apply_to(schema), BLUE.apply_to(alias))
}

/// Format node type with minimum width for alignment
/// Minimum width is 5 characters (length of "model") but allows longer strings
pub fn format_node_type_fixed_width(node_type: &str, colorize: bool) -> String {
    let formatted = if colorize {
        PLAIN.apply_to(node_type).to_string()
    } else {
        node_type.to_string()
    };

    // Pad if shorter than minimum width, otherwise return as-is
    if node_type.len() < MIN_NODE_TYPE_WIDTH {
        format!(
            "{}{}",
            formatted,
            " ".repeat(MIN_NODE_TYPE_WIDTH - node_type.len())
        )
    } else {
        formatted
    }
}

/// Format materialization without fixed width (for end of line)
pub fn format_materialization_suffix(materialization: Option<&str>, desc: Option<&str>) -> String {
    let truncated_mat = match materialization {
        Some("materialized_view") => Some("mat_view"),
        Some("streaming_table") => Some("streaming"),
        // Hide materialization label for tests and unit tests
        Some("test") | Some("unit_test") | Some("unit") | None => None,
        Some(other) => Some(other),
    };
    match (truncated_mat, desc) {
        (Some(mat), Some(desc)) => format!(" ({mat} - {desc})"),
        (Some(mat), None) => format!(" ({mat})"),
        (None, Some(desc)) => format!(" ({desc})"),
        (None, None) => String::new(),
    }
}

fn format_node_description(node: &NodeProcessed) -> Option<String> {
    let node_type = node.node_type();
    let node_outcome = node.node_outcome();

    // SAO enabled and not cached (reused) => "New changes detected"
    if (node_type != NodeType::Test && node_type != NodeType::UnitTest)
        && node_outcome == NodeOutcome::Success
    {
        return node
            .sao_enabled
            .and_then(|s| s.then_some("New changes detected".to_string()));
    }

    if let Some(cache_detail) = get_cache_detail(node.into()) {
        return Some(match cache_detail.node_cache_reason() {
            dbt_telemetry::NodeCacheReason::NoChanges => {
                "No new changes on any upstreams".to_string()
            }
            dbt_telemetry::NodeCacheReason::StillFresh => format!(
                "New changes detected. Did not meet build_after of {}. Last updated {} ago",
                humantime::format_duration(std::time::Duration::from_secs(
                    cache_detail.build_after_seconds()
                )),
                humantime::format_duration(std::time::Duration::from_secs(
                    cache_detail.last_updated_seconds()
                )),
            ),
            dbt_telemetry::NodeCacheReason::UpdateCriteriaNotMet => {
                "No new changes on all upstreams".to_string()
            }
        });
    }

    if matches!(node_type, NodeType::Test | NodeType::UnitTest)
        && get_test_outcome(node.into()) != Some(TestOutcome::Passed)
    {
        if let Some(line) = node.defined_at_line {
            if let Some(col) = node.defined_at_col {
                return Some(format!("{}:{}:{}", node.relative_path, line, col));
            } else {
                return Some(format!("{}:{}", node.relative_path, line));
            }
        } else {
            return Some(node.relative_path.clone());
        }
    }

    None
}

/// Formats the node outcome as a status string, optionally colorized.
/// This closely follows dbt-core's formatting for consistency.
pub fn format_node_outcome_as_status(
    node_outcome: NodeOutcome,
    skip_reason: Option<NodeSkipReason>,
    test_outcome: Option<TestOutcome>,
    colorize: bool,
) -> String {
    let outcome = match (node_outcome, skip_reason, test_outcome) {
        // Non test nodes. Success means "success"
        (NodeOutcome::Success, _, None) => "success",
        (NodeOutcome::Success, _, Some(t_outcome)) => match t_outcome {
            TestOutcome::Passed => "pass",
            TestOutcome::Warned => "warn",
            TestOutcome::Failed => "fail",
        },
        (NodeOutcome::Error, _, _) => "error",
        (NodeOutcome::Skipped, s_reason, _) => match s_reason {
            Some(NodeSkipReason::Upstream) => "skipped",
            Some(NodeSkipReason::Cached) => "reused",
            Some(NodeSkipReason::NoOp) => "no-op",
            // Other skip reasons are just "skipped"
            Some(NodeSkipReason::PhaseSkipped)
            | Some(NodeSkipReason::PhaseDisabled)
            | Some(NodeSkipReason::Unspecified)
            | None => "skipped",
        },
        (NodeOutcome::Canceled, _, _) => "cancelled", // Treat canceled as skipped for display
        (NodeOutcome::Unspecified, _, _) => "no-op",
    };

    if !colorize {
        return outcome.to_string();
    }

    match node_outcome {
        NodeOutcome::Success => GREEN.apply_to(outcome).to_string(),
        NodeOutcome::Error => RED.apply_to(outcome).to_string(),
        NodeOutcome::Skipped | NodeOutcome::Canceled | NodeOutcome::Unspecified => {
            YELLOW.apply_to(outcome).to_string()
        }
    }
}

/// Get the formatted (colored or plain) action text for a NodeProcessed event
/// This uses the padded action constants for info level, main TUI output
pub fn format_node_action(
    node_outcome: NodeOutcome,
    skip_reason: Option<NodeSkipReason>,
    test_outcome: Option<TestOutcome>,
    colorize: bool,
) -> String {
    let (action, color) = match (node_outcome, skip_reason, test_outcome) {
        // Non test nodes. Success means "success"
        (NodeOutcome::Success, _, None) => ("Succeeded", &GREEN),
        (NodeOutcome::Success, _, Some(t_outcome)) => match t_outcome {
            TestOutcome::Passed => ("Passed", &GREEN),
            TestOutcome::Warned => ("Warned", &YELLOW),
            TestOutcome::Failed => ("Failed", &RED),
        },
        (NodeOutcome::Error, _, _) => ("Failed", &RED),
        (NodeOutcome::Skipped, s_reason, _) => match s_reason {
            Some(NodeSkipReason::Upstream) => ("Skipped", &YELLOW),
            Some(NodeSkipReason::Cached) => ("Reused", &GREEN),
            Some(NodeSkipReason::NoOp) => ("Skipped", &YELLOW),
            // Other skip reasons are just "skipped"
            Some(NodeSkipReason::PhaseSkipped)
            | Some(NodeSkipReason::PhaseDisabled)
            | Some(NodeSkipReason::Unspecified)
            | None => ("Skipped", &YELLOW),
        },
        (NodeOutcome::Canceled, _, _) => ("Cancelled", &YELLOW), // Treat canceled as skipped for display
        (NodeOutcome::Unspecified, _, _) => ("Finished", &PLAIN),
    };

    // Right align and pad to ACTION_WIDTH characters
    debug_assert!(
        action.len() <= ACTION_WIDTH,
        "Action text too long for padding"
    );
    let action = format!("{:>width$}", action, width = ACTION_WIDTH);

    if !colorize {
        return action;
    }

    color.apply_to(action).to_string()
}

/// Format a NodeProcessed event for the start of processing (no duration)
///
/// Returns formatted string in the pattern:
/// `Started {node_type} {schema}.{alias}`
pub fn format_node_processed_start(node: &NodeProcessed, colorize: bool) -> String {
    let node_type = node.node_type();

    // Prepare schema and alias
    let mut schema = node.schema.clone().unwrap_or_default();
    let mut alias = node.identifier.clone().unwrap_or_else(|| node.name.clone());

    // Special handling for unit tests: display test schema + unit test name
    if node_type == NodeType::UnitTest {
        schema = format!("{}{}", schema, UNIT_TEST_SCHEMA_SUFFIX);
        alias = node.name.clone();
    }

    // Format components
    let schema_alias = format_schema_alias(&schema, &alias, colorize);

    format!("Started {} {}", node_type.pretty(), schema_alias)
}

/// Format a complete NodeProcessed event into a single output line
///
/// Returns formatted string in the pattern:
/// `{action} [{duration}] {node_type} {schema}.{alias}{materialization_suffix}`
pub fn format_node_processed_end(
    node: &NodeProcessed,
    duration: std::time::Duration,
    colorize: bool,
) -> String {
    let node_outcome = node.node_outcome();
    let node_type = node.node_type();

    // Force duration to 0 if skipped
    let duration = if node_outcome == NodeOutcome::Skipped {
        std::time::Duration::ZERO
    } else {
        duration
    };

    // Prepare schema and alias
    let mut schema = node.schema.clone().unwrap_or_default();
    let mut alias = node.identifier.clone().unwrap_or_else(|| node.name.clone());

    // Special handling for unit tests: display test schema + unit test name
    if node_type == NodeType::UnitTest {
        schema = format!("{}{}", schema, UNIT_TEST_SCHEMA_SUFFIX);
        alias = node.name.clone();
    }

    // Determine description based on outcome
    let desc = format_node_description(node);

    // Get materialization string - use custom_materialization if materialization is Custom
    let materialization_str = if node.materialization.is_some() {
        let mat = node.materialization();
        Some(if mat == NodeMaterialization::Custom {
            node.custom_materialization.clone().unwrap_or_default()
        } else {
            mat.as_static_ref().to_string()
        })
    } else {
        None
    };

    // Format components
    let schema_alias = format_schema_alias(&schema, &alias, colorize);
    let node_type_formatted = format_node_type_fixed_width(node_type.as_static_ref(), colorize);
    let materialization_suffix =
        format_materialization_suffix(materialization_str.as_deref(), desc.as_deref());
    let duration_formatted = format_duration_fixed_width(duration);
    let action_formatted = format_node_action(
        node_outcome,
        node.node_skip_reason.map(|_| node.node_skip_reason()),
        get_test_outcome(node.into()),
        colorize,
    );

    format!(
        "{} [{}] {} {}{}",
        action_formatted,
        duration_formatted,
        node_type_formatted,
        schema_alias,
        materialization_suffix
    )
}

/// Format a NodeEvaluated event for the start of evaluation (no duration)
///
/// Returns formatted string in the pattern:
/// `Started {phase_action} {node_type} {schema}.{alias}`
pub fn format_node_evaluated_start(node: &NodeEvaluated, colorize: bool) -> String {
    let node_type = node.node_type();
    let phase = node.phase();
    let phase_action = get_phase_action(phase);

    // Prepare schema and alias
    let schema = node.schema.clone().unwrap_or_default();
    let alias = node.identifier.clone().unwrap_or_else(|| node.name.clone());

    // Format components
    let schema_alias = format_schema_alias(&schema, &alias, colorize);
    let node_type_formatted = node_type.pretty();

    format!(
        "Started {} {} {}",
        phase_action, node_type_formatted, schema_alias
    )
}

/// Format a NodeEvaluated event for the end of evaluation (with duration and outcome)
///
/// Returns formatted string in the pattern:
/// `Finished {phase_action} [{duration}] {node_type} {schema}.{alias} [{outcome}]`
pub fn format_node_evaluated_end(
    node: &NodeEvaluated,
    duration: std::time::Duration,
    colorize: bool,
) -> String {
    let node_type = node.node_type();
    let node_outcome = node.node_outcome();
    let phase = node.phase();
    let phase_action = get_phase_action(phase);

    // Prepare schema and alias
    let schema = node.schema.clone().unwrap_or_default();
    let alias = node.identifier.clone().unwrap_or_else(|| node.name.clone());

    // Format components
    let schema_alias = format_schema_alias(&schema, &alias, colorize);
    let node_type_formatted = node_type.pretty();
    let duration_formatted = format_duration_fixed_width(duration);
    let outcome_formatted = format_node_outcome_as_status(
        node_outcome,
        node.node_skip_reason.map(|_| node.node_skip_reason()),
        get_test_outcome(node.into()),
        colorize,
    );

    format!(
        "Finished {} [{}] {} {} [{}]",
        phase_action, duration_formatted, node_type_formatted, schema_alias, outcome_formatted
    )
}

/// Format a skipped test group summary line
///
/// Returns formatted string in the pattern:
/// `{action} [{duration}] {resource_type} {message}`
pub fn format_skipped_test_group(
    node_names: &[String],
    seen_test: bool,
    seen_unit_test: bool,
    colorize: bool,
) -> String {
    // Format the message
    let message = if node_names.len() > 3 {
        format!(
            "{} and {} others",
            node_names
                .iter()
                .take(2)
                .map(|name| {
                    if colorize {
                        format!("'{}'", YELLOW.apply_to(name))
                    } else {
                        format!("'{}'", name)
                    }
                })
                .collect::<Vec<_>>()
                .join(", "),
            node_names.len() - 2
        )
    } else {
        node_names
            .iter()
            .map(|name| {
                if colorize {
                    format!("'{}'", YELLOW.apply_to(name))
                } else {
                    format!("'{}'", name)
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    };

    // Determine resource type based on which types were seen
    let resource_type = match (seen_test, seen_unit_test) {
        (true, true) => "test,unit_test",
        (true, false) => "test",
        (false, true) => "unit_test",
        (false, false) => "unknown",
    };

    // Format components - skipped nodes have 0 duration
    let resource_type_formatted = format_node_type_fixed_width(resource_type, colorize);
    let duration_formatted = format_duration_fixed_width(std::time::Duration::ZERO);
    let action_formatted = format_node_action(
        NodeOutcome::Skipped,
        Some(NodeSkipReason::Upstream),
        None,
        colorize,
    );

    format!(
        "{} [{}] {} {}",
        action_formatted, duration_formatted, resource_type_formatted, message
    )
}
