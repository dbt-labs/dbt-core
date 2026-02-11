use dbt_telemetry::{
    AnyNodeOutcomeDetail, CompiledCodeInline, ExecutionPhase, NodeEvaluated, NodeEvent,
    NodeMaterialization, NodeOutcome, NodeProcessed, NodeSkipReason, NodeType,
    SourceFreshnessOutcome, TestOutcome, get_cache_detail, get_freshness_detail,
    get_node_outcome_detail, get_test_outcome,
};

use crate::io_args::FsCommand;
use crate::tracing::formatters::phase::get_phase_progress_text;

use super::{
    color::{BLUE, CYAN, GREEN, PLAIN, RED, YELLOW},
    constants::{MAX_QUALIFIER_DISPLAY_LEN, MIN_NODE_TYPE_WIDTH, UNIT_TEST_SCHEMA_SUFFIX},
    duration::format_duration_fixed_width,
    layout::right_align_static_action,
    phase::get_phase_action,
};

/// Title used for compiled inline node output (matching dbt-core)
pub const COMPILED_INLINE_NODE_TITLE: &str = "Compiled inline node is:";

/// Get the display alias for a node based on its type.
///
/// For tests and unit tests, we use `name` which contains the original (untruncated)
/// test name for human-readable display. For other nodes, we use `identifier` (the
/// database-safe alias) with `name` as a fallback.
///
/// This matches dbt-core behavior where truncated test names (with MD5 hashes) are
/// used as database identifiers, but the full readable name is shown in CLI output.
pub fn get_node_display_alias(node_type: NodeType, identifier: Option<&str>, name: &str) -> String {
    match node_type {
        // Tests and unit tests always use `name` for display (contains original untruncated name)
        NodeType::Test | NodeType::UnitTest => name.to_string(),
        // Other nodes prefer `identifier` (database alias) with `name` as fallback
        _ => identifier
            .map(|s| s.to_string())
            .unwrap_or_else(|| name.to_string()),
    }
}

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

/// Format a qualifier (schema or node name) and alias with truncation for long names
/// If the qualifier is longer than MAX characters, truncate to "long_name....::alias"
pub fn format_qualifier_alias(qualifier: &str, alias: &str, colorize: bool) -> String {
    let qualifier = if qualifier.len() > MAX_QUALIFIER_DISPLAY_LEN {
        format!(
            "{}...",
            &qualifier[..MAX_QUALIFIER_DISPLAY_LEN.saturating_sub(4)]
        )
    } else {
        format!("{qualifier}.")
    };
    if !colorize {
        return format!("{}{}", qualifier, alias);
    }

    format!("{}{}", CYAN.apply_to(qualifier), BLUE.apply_to(alias))
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
/// Note: test_outcome and freshness_outcome are mutually exclusive (oneof in proto).
pub fn format_node_outcome_as_status(
    node_outcome: NodeOutcome,
    skip_reason: Option<NodeSkipReason>,
    test_outcome: Option<TestOutcome>,
    freshness_outcome: Option<SourceFreshnessOutcome>,
    colorize: bool,
) -> String {
    let (status, color) = match (node_outcome, skip_reason, test_outcome, freshness_outcome) {
        // Freshness outcomes (mutually exclusive with test_outcome)
        (NodeOutcome::Success, _, _, Some(f_outcome)) => match f_outcome {
            SourceFreshnessOutcome::OutcomePassed => ("pass", &GREEN),
            SourceFreshnessOutcome::OutcomeWarned => ("warn", &YELLOW),
            SourceFreshnessOutcome::OutcomeFailed => ("error", &RED),
        },
        // Test outcomes
        (NodeOutcome::Success, _, Some(t_outcome), None) => match t_outcome {
            TestOutcome::Passed => ("pass", &GREEN),
            TestOutcome::Warned => ("warn", &YELLOW),
            TestOutcome::Failed => ("fail", &RED),
        },
        // Non test/freshness nodes. Success means "success"
        (NodeOutcome::Success, _, None, None) => ("success", &GREEN),
        (NodeOutcome::Error, _, _, _) => ("error", &RED),
        (NodeOutcome::Skipped, s_reason, _, _) => match s_reason {
            Some(NodeSkipReason::Upstream) => ("skipped", &YELLOW),
            Some(NodeSkipReason::Cached) => ("reused", &GREEN),
            Some(NodeSkipReason::NoOp) => ("no-op", &YELLOW),
            // Other skip reasons are just "skipped"
            Some(NodeSkipReason::PhaseSkipped)
            | Some(NodeSkipReason::PhaseDisabled)
            | Some(NodeSkipReason::Unspecified)
            | None => ("skipped", &YELLOW),
        },
        (NodeOutcome::Canceled, _, _, _) => ("cancelled", &YELLOW),
        (NodeOutcome::Unspecified, _, _, _) => ("no-op", &YELLOW),
    };

    if colorize {
        color.apply_to(status).to_string()
    } else {
        status.to_string()
    }
}

/// Get the formatted (colored or plain) action text for a NodeProcessed event
/// This uses the padded action constants for info level, main TUI output
/// Note: test_outcome and freshness_outcome are mutually exclusive (oneof in proto).
pub fn format_node_action(
    node_outcome: NodeOutcome,
    skip_reason: Option<NodeSkipReason>,
    test_outcome: Option<TestOutcome>,
    freshness_outcome: Option<SourceFreshnessOutcome>,
    colorize: bool,
) -> String {
    let (action, color) = match (node_outcome, skip_reason, test_outcome, freshness_outcome) {
        // Freshness outcomes (mutually exclusive with test_outcome)
        (NodeOutcome::Success, _, _, Some(f_outcome)) => match f_outcome {
            SourceFreshnessOutcome::OutcomePassed => ("Passed", &GREEN),
            SourceFreshnessOutcome::OutcomeWarned => ("Warned", &YELLOW),
            SourceFreshnessOutcome::OutcomeFailed => ("Stale", &RED),
        },
        // Test outcomes
        (NodeOutcome::Success, _, Some(t_outcome), None) => match t_outcome {
            TestOutcome::Passed => ("Passed", &GREEN),
            TestOutcome::Warned => ("Warned", &YELLOW),
            TestOutcome::Failed => ("Failed", &RED),
        },
        // Non test/freshness nodes. Success means "success"
        (NodeOutcome::Success, _, None, None) => ("Succeeded", &GREEN),
        (NodeOutcome::Error, _, _, _) => ("Failed", &RED),
        (NodeOutcome::Skipped, s_reason, _, _) => match s_reason {
            Some(NodeSkipReason::Upstream) => ("Skipped", &YELLOW),
            Some(NodeSkipReason::Cached) => ("Reused", &GREEN),
            Some(NodeSkipReason::NoOp) => ("Skipped", &YELLOW),
            // Other skip reasons are just "skipped"
            Some(NodeSkipReason::PhaseSkipped)
            | Some(NodeSkipReason::PhaseDisabled)
            | Some(NodeSkipReason::Unspecified)
            | None => ("Skipped", &YELLOW),
        },
        (NodeOutcome::Canceled, _, _, _) => ("Cancelled", &YELLOW),
        (NodeOutcome::Unspecified, _, _, _) => ("Finished", &PLAIN),
    };

    // Right align action
    let action = right_align_static_action(action);

    if colorize {
        color.apply_to(action).to_string()
    } else {
        action
    }
}

/// Format a NodeProcessed event for the start of processing (no duration)
///
/// Returns formatted string in the pattern:
/// `Started {node_type} {schema}.{alias}`
pub fn format_node_processed_start(node: &NodeProcessed, colorize: bool) -> String {
    let node_type = node.node_type();

    // Prepare qualifier (schema for all nodes except sources) and alias
    let mut qualifier = node.schema.clone().unwrap_or_default();
    let alias = get_node_display_alias(node_type, node.identifier.as_deref(), &node.name);

    if node_type == NodeType::Source {
        // For sources we show source_name.identifier to match dbt-core output.
        if let Some(source_name) = node.source_name.as_ref() {
            qualifier = source_name.clone();
        }
    }

    // Special handling for unit tests: display test schema suffix
    if node_type == NodeType::UnitTest {
        qualifier = format!("{}{}", qualifier, UNIT_TEST_SCHEMA_SUFFIX);
    }

    // Format components
    let qualifier_alias = format_qualifier_alias(&qualifier, &alias, colorize);

    format!("Started {} {}", node_type.pretty(), qualifier_alias)
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

    // Special case for freshness phase - dispatch by phase, not node type
    if node.last_phase() == ExecutionPhase::FreshnessAnalysis {
        return format_freshness_result(node, duration, colorize);
    }

    // Force duration to 0 if skipped
    let duration = if node_outcome == NodeOutcome::Skipped {
        std::time::Duration::ZERO
    } else {
        duration
    };

    // Prepare qualifier (schema for all nodes except sources) and alias
    let mut qualifier = node.schema.clone().unwrap_or_default();
    let alias = get_node_display_alias(node_type, node.identifier.as_deref(), &node.name);

    if node_type == NodeType::Source {
        // For sources we show source_name.identifier to match dbt-core output.
        if let Some(source_name) = node.source_name.as_ref() {
            qualifier = source_name.clone();
        }
    }

    // Special handling for unit tests: display test schema suffix
    if node_type == NodeType::UnitTest {
        qualifier = format!("{}{}", qualifier, UNIT_TEST_SCHEMA_SUFFIX);
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
    let qualifier_alias = format_qualifier_alias(&qualifier, &alias, colorize);
    let node_type_formatted = format_node_type_fixed_width(node_type.as_static_ref(), colorize);
    let materialization_suffix =
        format_materialization_suffix(materialization_str.as_deref(), desc.as_deref());
    let duration_formatted = format_duration_fixed_width(duration);
    let action_formatted = format_node_action(
        node_outcome,
        node.node_skip_reason.map(|_| node.node_skip_reason()),
        get_test_outcome(node.into()),
        None, // freshness_outcome - not applicable for non-freshness nodes
        colorize,
    );

    format!(
        "{} [{}] {} {}{}",
        action_formatted,
        duration_formatted,
        node_type_formatted,
        qualifier_alias,
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

    // Prepare relation schema and alias
    let relation_schema = node.schema.clone().unwrap_or_default();
    let alias = get_node_display_alias(node_type, node.identifier.as_deref(), &node.name);

    // Format components
    let qualifier_alias = format_qualifier_alias(&relation_schema, &alias, colorize);
    let node_type_formatted = node_type.pretty();

    format!(
        "Started {} {} {}",
        phase_action, node_type_formatted, qualifier_alias
    )
}

/// Format a NodeEvaluated event for the start of evaluation (no duration)
/// using legacy non-interactive format
///
/// Returns formatted string in the pattern:
/// `{padded_action} {path_to_node}`
pub fn format_node_evaluated_start_legacy(node: &NodeEvaluated, command: FsCommand) -> String {
    if node.phase() == ExecutionPhase::Run && command == FsCommand::Show {
        // Show command generated very specific messages in run phase text output.
        return format!(
            "Previewing {} ({})",
            node.node_type().as_static_ref(),
            node.unique_id
        );
    }

    let phase = node.phase();
    let phase_action = if phase == ExecutionPhase::Compare {
        right_align_static_action("Comparing")
    } else if phase == ExecutionPhase::Run && command == FsCommand::Clone {
        right_align_static_action("Cloning")
    } else {
        let Some(phase_action) = get_phase_progress_text(phase) else {
            unreachable!("Phase action text should be available for NodeEvaluated start");
        };
        phase_action
    };

    // Generic tests are YAML-defined and should keep the test name for clarity.
    // Singular SQL tests should keep the old plain path output.
    let is_yaml_defined_test =
        node.relative_path.ends_with(".yml") || node.relative_path.ends_with(".yaml");

    if (node.node_type() == NodeType::Test || node.node_type() == NodeType::UnitTest)
        && is_yaml_defined_test
    {
        let display_path: std::borrow::Cow<str> = if let Some(line) = node.defined_at_line {
            if let Some(col) = node.defined_at_col {
                format!("{}:{}:{}", node.relative_path, line, col).into()
            } else {
                format!("{}:{}", node.relative_path, line).into()
            }
        } else {
            node.relative_path.as_str().into()
        };

        format!("{} {} ({})", phase_action, display_path, node.name)
    } else {
        format!("{} {}", phase_action, node.relative_path)
    }
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

    // Prepare relation schema and alias
    let relation_schema = node.schema.clone().unwrap_or_default();
    let alias = get_node_display_alias(node_type, node.identifier.as_deref(), &node.name);

    // Format components
    let qualifier_alias = format_qualifier_alias(&relation_schema, &alias, colorize);
    let node_type_formatted = node_type.pretty();
    let duration_formatted = format_duration_fixed_width(duration);
    let outcome_formatted = format_node_outcome_as_status(
        node_outcome,
        node.node_skip_reason.map(|_| node.node_skip_reason()),
        get_test_outcome(node.into()),
        None, // freshness_outcome - NodeEvaluated doesn't have freshness details
        colorize,
    );

    format!(
        "Finished {} [{}] {} {} [{}]",
        phase_action, duration_formatted, node_type_formatted, qualifier_alias, outcome_formatted
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
        None, // test_outcome
        None, // freshness_outcome
        colorize,
    );

    format!(
        "{} [{}] {} {}",
        action_formatted, duration_formatted, resource_type_formatted, message
    )
}

/// Format compiled inline code output
///
/// Returns formatted string with title and SQL:
/// `{title}\n{sql}`
pub fn format_compiled_inline_code(compiled_code: &CompiledCodeInline, colorize: bool) -> String {
    let title = if colorize {
        BLUE.apply_to(COMPILED_INLINE_NODE_TITLE).to_string()
    } else {
        COMPILED_INLINE_NODE_TITLE.to_string()
    };
    format!("{}\n{}", title, compiled_code.sql)
}

/// Format a source freshness result
///
/// Returns formatted string in the pattern:
/// `{action} [{duration}] source {schema}.{identifier} (last updated {age} ago)`
pub fn format_freshness_result(
    node: &NodeProcessed,
    duration: std::time::Duration,
    colorize: bool,
) -> String {
    let (freshness_outcome, description) = if let Some(freshness_detail) =
        get_freshness_detail(node.into())
    {
        // Format age duration
        let age_str = freshness_detail
            .age_seconds
            .map(|age| {
                humantime::format_duration(std::time::Duration::from_secs(age as u64)).to_string()
            })
            .unwrap_or_else(|| "unknown".to_string());

        (
            Some(freshness_detail.node_freshness_outcome()),
            format!(" (last updated {} ago)", age_str),
        )
    } else {
        // Early exit due to error, so no freshness info
        (None, "".to_string())
    };

    // Prepare source name and identifier (dbt-core logs `source_name.identifier`)
    let source_name = node.source_name.as_deref().unwrap_or("");
    let identifier = node.identifier.as_deref().unwrap_or(&node.name);

    // Format components
    let qualifier_alias = format_qualifier_alias(source_name, identifier, colorize);
    let node_type_formatted =
        format_node_type_fixed_width(node.node_type().as_static_ref(), colorize);
    let action_formatted = format_node_action(
        node.node_outcome(),
        node.node_skip_reason.map(|_| node.node_skip_reason()),
        None, // test_outcome
        freshness_outcome,
        colorize,
    );

    format!(
        "{} [{}] {} {}{}",
        action_formatted,
        format_duration_fixed_width(duration),
        node_type_formatted,
        qualifier_alias,
        description
    )
}
