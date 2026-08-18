use console::Style;
use dbt_telemetry::{
    AnyNodeOutcomeDetail, CompiledCode, CompiledCodeInline, ExecutionPhase, NodeEvaluated,
    NodeEvent, NodeMaterialization, NodeOutcome, NodeProcessed, NodeSkipReason, NodeType,
    SourceFreshnessOutcome, TestOutcome, get_cache_detail, get_freshness_detail,
    get_node_outcome_detail, get_test_outcome, has_node_warning, is_batched_test,
    is_statically_checked_test,
};

use crate::io_args::FsCommand;
use crate::tracing::formatters::phase::get_phase_progress_text;

use super::{
    color::{BLUE, CYAN, GREEN, PLAIN, RED, YELLOW, maybe_apply_color},
    constants::{
        DURATION_WIDTH, MAX_QUALIFIER_DISPLAY_LEN, MEMBER_TREE_INDENT, MIN_NODE_TYPE_WIDTH,
        UNIT_TEST_SCHEMA_SUFFIX,
    },
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
    } else if qualifier.is_empty() {
        String::new()
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

/// Format where a test is defined as `path`, `path:line` or `path:line:col`.
fn format_test_source_location(node: &NodeProcessed) -> String {
    if let Some(line) = node.defined_at_line {
        if let Some(col) = node.defined_at_col {
            format!("{}:{}:{}", node.relative_path, line, col)
        } else {
            format!("{}:{}", node.relative_path, line)
        }
    } else {
        node.relative_path.clone()
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
                "New changes detected. Did not meet lag_tolerance of {}. Last updated {} ago",
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
            dbt_telemetry::NodeCacheReason::ClonedExisting => {
                "Cloned from cached relation".to_string()
            }
            dbt_telemetry::NodeCacheReason::ClonedExistingStillFresh => {
                "Cloned from cached relation within freshness tolerance".to_string()
            }
        });
    }

    if is_statically_checked_test(node.into()) {
        return Some("Statically checked".to_string());
    }

    let aggregated = is_batched_test(node.into());

    if matches!(node_type, NodeType::Test | NodeType::UnitTest)
        && get_test_outcome(node.into()) != Some(TestOutcome::Passed)
    {
        let location = format_test_source_location(node);

        return Some(if aggregated {
            format!("Batched - {location}")
        } else {
            location
        });
    }

    aggregated.then(|| "Batched".to_string())
}

/// Formats the node outcome as a status string, optionally colorized.
/// This closely follows dbt-core's formatting for consistency.
/// Note: test_outcome and freshness_outcome are mutually exclusive (oneof in proto).
pub fn format_node_outcome_as_status(
    node_outcome: NodeOutcome,
    skip_reason: Option<NodeSkipReason>,
    test_outcome: Option<TestOutcome>,
    freshness_outcome: Option<SourceFreshnessOutcome>,
    has_warn: bool,
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
        // Non test/freshness nodes that succeeded with warnings
        (NodeOutcome::Success, _, None, None) if has_warn => ("warn", &YELLOW),
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

/// Resolve a node outcome to its unpadded action label and status colour.
/// Callers that pad the label themselves need the plain text, hence the split.
fn node_action_parts(
    node_outcome: NodeOutcome,
    skip_reason: Option<NodeSkipReason>,
    test_outcome: Option<TestOutcome>,
    freshness_outcome: Option<SourceFreshnessOutcome>,
    has_warn: bool,
) -> (&'static str, &'static Style) {
    match (node_outcome, skip_reason, test_outcome, freshness_outcome) {
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
        // Non test/freshness nodes that succeeded with warnings
        (NodeOutcome::Success, _, None, None) if has_warn => ("Warned", &YELLOW),
        // Non test/freshness nodes. Success means "Succeeded"
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
    has_warn: bool,
    colorize: bool,
) -> String {
    let (action, color) = node_action_parts(
        node_outcome,
        skip_reason,
        test_outcome,
        freshness_outcome,
        has_warn,
    );

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

    // Force duration to 0 if skipped or if a cached test preserved a warning/error verdict.
    let duration = if node_outcome == NodeOutcome::Skipped
        || (node_outcome == NodeOutcome::Success
            && node.node_skip_reason() == NodeSkipReason::Cached)
        || (node_outcome == NodeOutcome::Success && is_statically_checked_test(node.into()))
    {
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

    // For data tests, only show the schema qualifier when store_failures is enabled.
    // Without store_failures, dbt-core shows just the test name (no schema prefix).
    if node_type == NodeType::Test {
        let store_failures = get_node_outcome_detail(node.into())
            .and_then(|detail| {
                if let AnyNodeOutcomeDetail::NodeTestDetail(test_detail) = detail {
                    test_detail.store_failures
                } else {
                    None
                }
            })
            .unwrap_or(false);
        if !store_failures {
            qualifier = String::new();
        }
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
        has_node_warning(node.into()),
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

    // YAML-defined nodes should keep the entry name for clarity.
    // Singular SQL tests and legacy SQL snapshots should keep the old plain path output.
    let is_yaml_defined_node =
        node.relative_path.ends_with(".yml") || node.relative_path.ends_with(".yaml");

    if matches!(
        node.node_type(),
        NodeType::Test | NodeType::UnitTest | NodeType::Snapshot
    ) && is_yaml_defined_node
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
    let active_duration = duration.saturating_sub(std::time::Duration::from_millis(
        node.idle_time_ms.unwrap_or_default(),
    ));
    let active_duration = if node_outcome == NodeOutcome::Success
        && (node.node_skip_reason() == NodeSkipReason::Cached
            || is_statically_checked_test(node.into()))
    {
        std::time::Duration::ZERO
    } else {
        active_duration
    };
    let duration_formatted = format_duration_fixed_width(active_duration);
    let outcome_formatted = format_node_outcome_as_status(
        node_outcome,
        node.node_skip_reason.map(|_| node.node_skip_reason()),
        get_test_outcome(node.into()),
        None, // freshness_outcome - NodeEvaluated doesn't have freshness details
        has_node_warning(node.into()),
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
        None,  // test_outcome
        None,  // freshness_outcome
        false, // has_warn - skipped nodes never have warn detail
        colorize,
    );

    format!(
        "{} [{}] {} {}",
        action_formatted, duration_formatted, resource_type_formatted, message
    )
}

/// Format the header line of an aggregated generic-test group
///
/// Returns formatted string in the pattern:
/// `{worst_action} [{duration}] test {macro_name} on {model_name} ({member_count} batched)`
pub fn format_aggregated_test_group_header(
    macro_name: &str,
    attached_node: &str,
    member_count: usize,
    worst_outcome: TestOutcome,
    duration: std::time::Duration,
    colorize: bool,
) -> String {
    // unique_id is `<type>.<package>.<name>[.v<version>]`; keep the version suffix.
    let model_name = attached_node.splitn(3, '.').nth(2).unwrap_or(attached_node);
    let label = format!("{macro_name} on {model_name}");

    let action_formatted = format_node_action(
        NodeOutcome::Success,
        None, // skip_reason - an aggregated group always executed
        Some(worst_outcome),
        None,  // freshness_outcome
        false, // has_warn - the group's verdict comes from its members
        colorize,
    );
    let duration_formatted = format_duration_fixed_width(duration);
    debug_assert_eq!(
        duration_formatted.len(),
        DURATION_WIDTH,
        "duration must render at its declared fixed width"
    );

    format!(
        "{} [{}] {} {}{}",
        action_formatted,
        duration_formatted,
        format_node_type_fixed_width(NodeType::Test.as_static_ref(), colorize),
        format_qualifier_alias("", &label, colorize),
        format_materialization_suffix(None, Some(&format!("{member_count} batched")))
    )
}

/// Format one member line of an aggregated generic-test group as a tree branch under
/// the group header. `is_last` picks the terminating connector.
///
/// Returns formatted string in the pattern:
/// `{indent}{connector} {action} {node_type} {name}{source_location_suffix}`
pub fn format_aggregated_test_group_member(
    node: &NodeProcessed,
    is_last: bool,
    colorize: bool,
) -> String {
    // Members carry no duration bracket; the header states the query's cost once.
    let desc = (get_test_outcome(node.into()) != Some(TestOutcome::Passed))
        .then(|| format_test_source_location(node));

    let connector = if is_last { "└─" } else { "├─" };
    let (action_label, action_style) = node_action_parts(
        node.node_outcome(),
        node.node_skip_reason.map(|_| node.node_skip_reason()),
        get_test_outcome(node.into()),
        None, // freshness_outcome
        has_node_warning(node.into()),
    );

    // The action follows the connector unpadded, so a label longer than a test verdict
    // shifts the rest of its own line right. Deliberate: padding reopens a wide gap.
    format!(
        "{}{} {} {} {}{}",
        " ".repeat(MEMBER_TREE_INDENT),
        connector,
        maybe_apply_color(action_style, action_label, colorize),
        format_node_type_fixed_width(node.node_type().as_static_ref(), colorize),
        format_qualifier_alias("", &node.name, colorize),
        format_materialization_suffix(None, desc.as_deref())
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

/// Format compiled project node output.
///
/// Returns one-line path-oriented message:
/// `Compiled SQL for node {unique_id} at {relative_path}`
pub fn format_compiled_code(compiled_code: &CompiledCode, colorize: bool) -> String {
    if colorize {
        format!(
            "Compiled SQL for node {} at {}",
            BLUE.apply_to(&compiled_code.unique_id),
            compiled_code.relative_path
        )
    } else {
        format!(
            "Compiled SQL for node {} at {}",
            compiled_code.unique_id, compiled_code.relative_path
        )
    }
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
        false, // has_warn - freshness nodes use freshness_outcome instead
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

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_telemetry::{
        NodeOutcomeDetail, TestEvaluationDetail,
        node_processed::NodeOutcomeDetail as ProcessedDetail,
    };

    /// Stand-in for the batch's synthetic test node unique_id that members are stamped with.
    const BATCH_UNIQUE_ID: &str = "test.project.aggregated_accepted_values_orders";

    fn cached_warned_test_processed() -> NodeProcessed {
        let mut node = NodeProcessed::start(
            "test.project.accepted_values_orders_is_today_order__True".to_string(),
            "accepted_values_orders_is_today_order__True".to_string(),
            None,
            Some("dbt_test__audit".to_string()),
            None,
            None,
            None,
            NodeType::Test,
            Some(ExecutionPhase::Run),
            "models/marts/orders.yml".to_string(),
            Some(37),
            Some(13),
            "checksum".to_string(),
            true,
            None,
        );
        node.set_node_outcome(NodeOutcome::Success);
        node.set_node_skip_reason(NodeSkipReason::Cached);
        node.node_outcome_detail = Some(ProcessedDetail::NodeTestDetail(
            TestEvaluationDetail::new(TestOutcome::Warned, 2, None, None, None, None),
        ));
        node
    }

    fn test_processed(
        outcome: TestOutcome,
        failures: i32,
        statically_checked: Option<bool>,
        batch_unique_id: Option<&str>,
    ) -> NodeProcessed {
        let mut node = NodeProcessed::start(
            "test.project.accepted_values_orders_is_today_order__True".to_string(),
            "accepted_values_orders_is_today_order__True".to_string(),
            None,
            Some("dbt_test__audit".to_string()),
            None,
            None,
            None,
            NodeType::Test,
            Some(ExecutionPhase::Run),
            "models/marts/orders.yml".to_string(),
            Some(37),
            Some(13),
            "checksum".to_string(),
            true,
            None,
        );
        node.set_node_outcome(NodeOutcome::Success);
        node.node_outcome_detail =
            Some(ProcessedDetail::NodeTestDetail(TestEvaluationDetail::new(
                outcome,
                failures,
                None,
                None,
                statically_checked,
                batch_unique_id.map(str::to_string),
            )));
        node
    }

    fn cached_warned_test_evaluated() -> NodeEvaluated {
        let mut node = NodeEvaluated::start(
            "test.project.accepted_values_orders_is_today_order__True".to_string(),
            "accepted_values_orders_is_today_order__True".to_string(),
            None,
            Some("dbt_test__audit".to_string()),
            None,
            None,
            None,
            NodeType::Test,
            ExecutionPhase::Run,
            "models/marts/orders.yml".to_string(),
            Some(37),
            Some(13),
            "checksum".to_string(),
        );
        node.set_node_outcome(NodeOutcome::Success);
        node.set_node_skip_reason(NodeSkipReason::Cached);
        node.node_outcome_detail = Some(NodeOutcomeDetail::NodeTestDetail(
            TestEvaluationDetail::new(TestOutcome::Warned, 2, None, None, None, None),
        ));
        node
    }

    #[test]
    fn cached_warned_test_processed_formats_zero_duration_and_warning() {
        let output = format_node_processed_end(
            &cached_warned_test_processed(),
            std::time::Duration::from_millis(250),
            false,
        );

        assert!(output.contains("Warned"));
        assert!(output.contains("[-------]"));
        assert!(output.contains("accepted_values_orders_is_today_order__True"));
    }

    #[test]
    fn statically_checked_test_processed_formats_no_query_duration_and_description() {
        let output = format_node_processed_end(
            &test_processed(TestOutcome::Passed, 0, Some(true), None),
            std::time::Duration::from_millis(250),
            false,
        );

        assert!(output.contains("Passed"));
        assert!(output.contains("[-------]"));
        assert!(output.contains("Statically checked"));
    }

    #[test]
    fn non_statically_checked_passed_test_processed_keeps_duration_and_description() {
        let output = format_node_processed_end(
            &test_processed(TestOutcome::Passed, 0, None, None),
            std::time::Duration::from_millis(250),
            false,
        );

        assert!(output.contains("Passed"));
        assert!(!output.contains("[-------]"));
        assert!(!output.contains("Statically checked"));
        assert!(!output.contains("Batched"));
    }

    // `format_node_processed_end` feeds `logs/dbt.log` and `--log-format json`, which stay
    // one flat line per member test. Only stdout regroups; do not align these with it.
    #[test]
    fn aggregated_passed_test_processed_shows_flat_marker_for_log_sinks() {
        let output = format_node_processed_end(
            &test_processed(TestOutcome::Passed, 0, None, Some(BATCH_UNIQUE_ID)),
            std::time::Duration::from_millis(250),
            false,
        );

        assert!(output.contains("Passed"));
        assert!(output.contains("(Batched)"));
        // An aggregated group does issue a query, so the duration must survive.
        assert!(!output.contains("[-------]"));
    }

    #[test]
    fn aggregated_failed_test_processed_shows_flat_marker_and_location_for_log_sinks() {
        let output = format_node_processed_end(
            &test_processed(TestOutcome::Failed, 3, None, Some(BATCH_UNIQUE_ID)),
            std::time::Duration::from_millis(250),
            false,
        );

        assert!(output.contains("Failed"));
        assert!(output.contains("(Batched - models/marts/orders.yml:37:13)"));
    }

    #[test]
    fn non_aggregated_failed_test_processed_keeps_bare_location() {
        let output = format_node_processed_end(
            &test_processed(TestOutcome::Failed, 3, None, None),
            std::time::Duration::from_millis(250),
            false,
        );

        assert!(output.contains("Failed"));
        assert!(output.contains("(models/marts/orders.yml:37:13)"));
        assert!(!output.contains("Batched"));
    }

    #[test]
    fn aggregated_group_header_formats_worst_outcome_and_label() {
        let output = format_aggregated_test_group_header(
            "unique",
            "model.my_pkg.unproven_unique",
            2,
            TestOutcome::Failed,
            std::time::Duration::from_millis(250),
            false,
        );

        assert!(output.contains("Failed"));
        assert!(output.contains("unique on unproven_unique"));
        assert!(output.contains("(2 batched)"));
    }

    #[test]
    fn aggregated_group_header_uses_unqualified_model_name() {
        let output = format_aggregated_test_group_header(
            "unique",
            "model.my_pkg.unproven_unique",
            2,
            TestOutcome::Passed,
            std::time::Duration::from_millis(250),
            false,
        );

        assert!(output.contains("unique on unproven_unique"));
        assert!(!output.contains("model.my_pkg."));
    }

    #[test]
    fn aggregated_group_header_keeps_version_suffix() {
        let output = format_aggregated_test_group_header(
            "unique",
            "model.simpler.model_with_version.v1",
            2,
            TestOutcome::Passed,
            std::time::Duration::from_millis(250),
            false,
        );

        assert!(output.contains("unique on model_with_version.v1"));
        assert!(!output.contains("unique on v1"));
    }

    #[test]
    fn aggregated_group_header_warned_outcome() {
        let output = format_aggregated_test_group_header(
            "unique",
            "model.my_pkg.unproven_unique",
            2,
            TestOutcome::Warned,
            std::time::Duration::from_millis(250),
            false,
        );

        assert!(output.contains("Warned"));
    }

    #[test]
    fn aggregated_group_member_passed_has_no_bracket_and_no_marker() {
        let output = format_aggregated_test_group_member(
            &test_processed(TestOutcome::Passed, 0, None, Some(BATCH_UNIQUE_ID)),
            false,
            false,
        );

        assert_eq!(
            output,
            "    ├─ Passed test  accepted_values_orders_is_today_order__True"
        );
    }

    #[test]
    fn aggregated_group_member_failed_keeps_source_location() {
        let output = format_aggregated_test_group_member(
            &test_processed(TestOutcome::Failed, 3, None, Some(BATCH_UNIQUE_ID)),
            true,
            false,
        );

        assert_eq!(
            output,
            "    └─ Failed test  accepted_values_orders_is_today_order__True \
             (models/marts/orders.yml:37:13)"
        );
    }

    #[test]
    fn aggregated_group_member_branches_from_a_fixed_column() {
        // Display columns, not byte offsets: the connector glyphs are 3 bytes per column.
        let column_of = |line: &str, pat: &str| {
            let byte = line.find(pat).unwrap_or_else(|| panic!("{pat} in {line}"));
            line[..byte].chars().count()
        };

        let passed = test_processed(TestOutcome::Passed, 0, None, Some(BATCH_UNIQUE_ID));
        // A cancelled member renders the longest reachable action label ("Cancelled"),
        // the case that legitimately shifts its own node type column.
        let mut cancelled = test_processed(TestOutcome::Passed, 0, None, Some(BATCH_UNIQUE_ID));
        cancelled.set_node_outcome(NodeOutcome::Canceled);
        assert!(
            format_aggregated_test_group_member(&cancelled, false, false).contains("Cancelled")
        );

        // The connector column is the invariant: members branch from the column where the
        // header's verdict starts, whatever their own action label is.
        let header = format_aggregated_test_group_header(
            "unique",
            "model.my_pkg.unproven_unique",
            2,
            TestOutcome::Failed,
            std::time::Duration::from_millis(250),
            false,
        );
        for node in [&passed, &cancelled] {
            for (is_last, connector) in [(false, "├─"), (true, "└─")] {
                let member = format_aggregated_test_group_member(node, is_last, false);
                assert_eq!(column_of(&header, "Failed"), column_of(&member, connector));
            }
        }

        // Same-length labels share a node type column; a longer one pushes only its own
        // line rightwards, by exactly the extra characters. Padding it back would
        // reintroduce the wide gap this layout exists to remove.
        let passed_member = format_aggregated_test_group_member(&passed, false, false);
        let cancelled_member = format_aggregated_test_group_member(&cancelled, false, false);
        assert_eq!(
            column_of(&cancelled_member, "test ") - column_of(&passed_member, "test "),
            "Cancelled".len() - "Passed".len(),
        );
    }

    #[test]
    fn aggregated_group_member_last_differs_only_by_connector() {
        let node = test_processed(TestOutcome::Passed, 0, None, Some(BATCH_UNIQUE_ID));
        let non_last = format_aggregated_test_group_member(&node, false, false);
        let last = format_aggregated_test_group_member(&node, true, false);

        assert_ne!(non_last, last);
        assert_eq!(non_last.replacen("├─", "└─", 1), last);
    }

    #[test]
    fn cached_warned_test_evaluated_formats_zero_duration_and_warning() {
        let output = format_node_evaluated_end(
            &cached_warned_test_evaluated(),
            std::time::Duration::from_millis(250),
            false,
        );

        assert!(output.contains("Finished running"));
        assert!(output.contains("[-------]"));
        assert!(output.contains("[warn]"));
    }
}
