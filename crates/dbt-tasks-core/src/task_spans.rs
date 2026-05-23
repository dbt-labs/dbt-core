use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
};

use dbt_common::{
    FsResult,
    stats::NodeStatus,
    tracing::{
        layers::tui_layer::TuiAllProcessingNodesGroup,
        span_info::{
            read_span_attrs, read_span_start_info, record_span_status_from_attrs, update_span_attrs,
        },
    },
};
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use dbt_telemetry::StaticName as _;
use dbt_telemetry::{
    ExecutionPhase, NodeCacheDetail, NodeCacheReason, NodeErrorType, NodeEvaluated, NodeOutcome,
    NodeOutcomeDetail, NodeProcessed, NodeSkipReason, NodeSkipUpstreamDetail, NodeType,
    PhaseExecuted, set_node_warning_outcome_no_warnings, set_node_warning_outcome_warned,
    update_dbt_core_event_code_for_node_processed_end,
};
use petgraph::graph::DiGraph;

use crate::{
    span_manager::{
        ParentSpanBuilder, ParentSpanRef, ParentTaskSpanOnClose, ParentTaskSpanOnSkip, SpanLevel,
        SpanManager, SpanTreeRequest,
    },
    task::{TP, Task},
    visitor::SkipReason,
};

/// Helper functions for generating span keys used in registration and task requests
/// Returns the span key for a PhaseExecuted span
pub fn phase_span_key(phase: ExecutionPhase) -> String {
    format!("{}::{:?}", PhaseExecuted::FULL_NAME, phase)
}

/// Returns the span key for a NodeProcessed span
pub fn node_processed_span_key(unique_id: &str) -> String {
    format!("{}::{}", NodeProcessed::FULL_NAME, unique_id)
}

/// Returns the span key for the TuiAllProcessingNodesGroup span
fn all_processing_nodes_span_key() -> String {
    "__all_processing_nodes__".to_string()
}

pub fn update_node_outcome_from_skip_reason(ev: &mut NodeEvaluated, skip_reason: &SkipReason) {
    ev.set_node_outcome(NodeOutcome::Skipped);
    match skip_reason {
        SkipReason::FailedPhase => {
            ev.set_node_skip_reason(NodeSkipReason::PhaseSkipped);
        }
        SkipReason::FailedUpstream(upstream_id) => {
            ev.set_node_skip_reason(NodeSkipReason::Upstream);
            ev.node_outcome_detail = Some(NodeOutcomeDetail::NodeSkipUpstreamDetail(
                NodeSkipUpstreamDetail::new(upstream_id.clone()),
            ));
        }
        SkipReason::Reused => {
            ev.set_node_skip_reason(NodeSkipReason::Cached);
            ev.node_outcome_detail = Some(NodeOutcomeDetail::NodeCacheDetail(
                NodeCacheDetail::new(NodeCacheReason::NoChanges, None, None, None),
            ));
        }
    }
}

/// TODO: this is a temporary reverse mapping from legacy status to new outcome model.
/// We should revert to using outcome directly in the task result and calculate status
/// from that - since outcome is more expressive, and current implementation is lossy.
fn update_node_outcome_from_legacy_status(event: &mut NodeEvaluated, status: NodeStatus) {
    match status {
        NodeStatus::Succeeded => {
            event.set_node_outcome(NodeOutcome::Success);
            set_node_warning_outcome_no_warnings(event);
        }
        NodeStatus::SucceededWithWarning => {
            event.set_node_outcome(NodeOutcome::Success);
            // TODO: Stamp WithWarnings authoritatively. The middleware stamps it when a warn log fires
            // inside the span, but warnings detected before the span opens (e.g. the pre-
            // registration column_types check) can't be caught by the middleware. Stamping here
            // is idempotent: if the middleware already set it, this is a no-op.
            set_node_warning_outcome_warned(event);
        }
        NodeStatus::TestPassed => {
            // Handled inside the test task
        }
        NodeStatus::TestWarned => {
            // Handled inside the test task
        }
        NodeStatus::Errored => {
            if event.node_type() == NodeType::Test || event.node_type() == NodeType::UnitTest {
                // Normally handled inside the test task, but if node outcome is unspecified,
                // set to error. This happens in some scenarios, e.g. statically analyzed error.
                if event.node_outcome() == NodeOutcome::Unspecified {
                    event.set_node_outcome(NodeOutcome::Error);
                    event.set_node_error_type(NodeErrorType::User);
                }
            } else {
                event.set_node_outcome(NodeOutcome::Error);
                event.set_node_error_type(NodeErrorType::User);
            };
        }
        NodeStatus::SkippedUpstreamFailed => {
            event.set_node_outcome(NodeOutcome::Skipped);
            event.set_node_skip_reason(NodeSkipReason::Upstream);
        }
        NodeStatus::ReusedNoChanges(ref message) => {
            event.set_node_outcome(NodeOutcome::Skipped);
            event.set_node_skip_reason(NodeSkipReason::Cached);

            // Carry the task-supplied message so the formatter can render
            // decision-specific text (e.g. "Cloned from cached relation")
            // without needing a dedicated NodeCacheReason variant.
            event.node_outcome_detail =
                Some(NodeOutcomeDetail::NodeCacheDetail(NodeCacheDetail::new(
                    NodeCacheReason::NoChanges,
                    None,
                    None,
                    Some(message.clone()),
                )));
        }
        NodeStatus::ReusedStillFresh(_, freshness_sec, last_updated_secs) => {
            event.set_node_outcome(NodeOutcome::Skipped);
            event.set_node_skip_reason(NodeSkipReason::Cached);

            event.node_outcome_detail =
                Some(NodeOutcomeDetail::NodeCacheDetail(NodeCacheDetail::new(
                    NodeCacheReason::StillFresh,
                    Some(freshness_sec),
                    Some(last_updated_secs),
                    None,
                )));
        }
        NodeStatus::ReusedStillFreshNoChanges(_) => {
            event.set_node_outcome(NodeOutcome::Skipped);
            event.set_node_skip_reason(NodeSkipReason::Cached);

            event.node_outcome_detail = Some(NodeOutcomeDetail::NodeCacheDetail(
                NodeCacheDetail::new(NodeCacheReason::UpdateCriteriaNotMet, None, None, None),
            ));
        }
        NodeStatus::NoOp => {
            event.set_node_outcome(NodeOutcome::Skipped);
            event.set_node_skip_reason(NodeSkipReason::NoOp);
        }
    };
}

/// Callback for task span on_close: updates NodeEvaluated with outcome from result.
pub fn task_span_on_close(task_span: &tracing::Span, result: &FsResult<NodeStatus>) {
    record_span_status_from_attrs(task_span, |attrs| {
        if let Some(ev) = attrs.downcast_mut::<NodeEvaluated>() {
            match result {
                Ok(ns) => update_node_outcome_from_legacy_status(ev, ns.clone()),
                Err(_) => {
                    ev.set_node_outcome(NodeOutcome::Error);
                    ev.set_node_error_type(NodeErrorType::Internal);
                }
            };
        }
    })
}

/// Callback for task span on_skip: updates NodeEvaluated with outcome from skip reason.
pub fn task_span_on_skip(task_span: &tracing::Span, skip_reason: &SkipReason) {
    record_span_status_from_attrs(task_span, |attrs| {
        if let Some(ev) = attrs.downcast_mut::<NodeEvaluated>() {
            update_node_outcome_from_skip_reason(ev, skip_reason);
        }
    })
}

pub fn create_task_span_for_node(
    node: &dyn InternalDbtNodeAttributes,
    phase: ExecutionPhase,
    span_manager: &SpanManager<FsResult<NodeStatus>, SkipReason>,
    in_dir: &Path,
    out_dir: &Path,
) -> FsResult<tracing::Span> {
    let mut builder = SpanTreeRequest::builder(
        SpanLevel::Debug,
        node.get_node_evaluated_event(phase, in_dir, out_dir).into(),
        Some(Box::new(task_span_on_close)),
        Some(Box::new(task_span_on_skip)),
    );
    builder.with_parent(SpanLevel::Info, phase_span_key(phase));
    let key = node_processed_span_key(&node.unique_id());
    builder.add_related_span(SpanLevel::Info, &key);
    let request = builder.build();
    span_manager.get_task_span(request)
}

/// Information about node counts across phases, used for span creation.
#[derive(Debug, Clone)]
struct NodeCounts {
    /// Total number of unique nodes being processed
    node_count_total: u64,
    /// Number of nodes in each phase
    node_counts_by_phase: HashMap<TP, u64>,
    /// Number of tasks for each unique node ID
    node_unique_id_to_task_count: HashMap<String, u8>,
}

/// Calculate node counts from the task schedule.
///
/// This determines how many nodes are in each phase and how many tasks
/// each unique node has, which is used for span lifecycle management.
fn calculate_node_counts(schedule: &DiGraph<Arc<dyn Task>, ()>) -> NodeCounts {
    let mut node_counts_by_phase: HashMap<TP, u64> = HashMap::new();
    let mut node_unique_id_to_task_count: HashMap<String, u8> = HashMap::new();
    let mut all_unique_ids = HashSet::new();

    for task in schedule.node_weights() {
        if let Some(tp) = task.task_phase() {
            let count = task.dbt_nodes().len() as u64;
            *node_counts_by_phase.entry(tp).or_insert(0) += count;
            for unique_id in task
                .dbt_nodes()
                .iter()
                .map(|node| node.common().unique_id.as_str())
            {
                all_unique_ids.insert(unique_id.to_string());
                *node_unique_id_to_task_count
                    .entry(unique_id.to_string())
                    .or_insert(0) += 1
            }
        }
    }

    let node_count_total = all_unique_ids.len() as u64;

    NodeCounts {
        node_count_total,
        node_counts_by_phase,
        node_unique_id_to_task_count,
    }
}

fn update_node_processed_from_node_evaluated(sum_ev: &mut NodeProcessed, phase_ev: &NodeEvaluated) {
    if phase_ev.phase() == ExecutionPhase::NodeCacheHydration {
        // Special case - save error outcome, but otherwise save as no-op
        // This will make frontier nodes a no-op, since they don't have any other phases
        if phase_ev.node_outcome() == NodeOutcome::Error {
            sum_ev.set_node_outcome(NodeOutcome::Error);
            sum_ev.node_error_type = phase_ev.node_error_type;
        } else {
            sum_ev.set_node_outcome(NodeOutcome::Skipped);
            sum_ev.set_node_skip_reason(NodeSkipReason::NoOp);
        }
        sum_ev.last_phase = phase_ev.phase;
        return;
    }

    // Determine if the new incoming closed NodeEvaluated event should update
    // the previous outcome recorded for this node in NodeProcessed.
    let update_previous = match phase_ev.node_outcome() {
        NodeOutcome::Skipped => match phase_ev.node_skip_reason() {
            NodeSkipReason::Upstream => true,
            NodeSkipReason::Cached => true,
            NodeSkipReason::NoOp => true,
            // If we skipped the phase, or skipped because the phase is disabled,
            // we keep whatever previous outcome was recorded for the node
            NodeSkipReason::PhaseSkipped
            | NodeSkipReason::PhaseDisabled
            | NodeSkipReason::Unspecified => false,
        },
        NodeOutcome::Success => true,
        NodeOutcome::Error => true,
        NodeOutcome::Canceled => true,
        NodeOutcome::Unspecified => false, // should never happen
    };

    if !update_previous {
        return;
    }

    sum_ev.sao_enabled = phase_ev.sao_enabled;
    sum_ev.node_outcome = phase_ev.node_outcome;
    sum_ev.node_skip_reason = phase_ev.node_skip_reason;
    sum_ev.node_error_type = phase_ev.node_error_type;
    sum_ev.node_cancel_reason = phase_ev.node_cancel_reason;
    sum_ev.node_outcome_detail = phase_ev
        .node_outcome_detail
        .as_ref()
        .map(|d| d.clone().into());
    sum_ev.last_phase = phase_ev.phase;
    sum_ev.rows_affected = phase_ev.rows_affected;
}

fn accumulate_node_processed_phase_duration(
    sum_ev: &mut NodeProcessed,
    wall_duration_ms: Option<u64>,
    phase_ev: Option<&NodeEvaluated>,
) {
    let idle_time_ms = phase_ev.and_then(|ev| ev.idle_time_ms).unwrap_or_default();

    if let Some(duration) = wall_duration_ms {
        let net_duration = duration.saturating_sub(idle_time_ms);
        sum_ev.duration_ms = Some(
            sum_ev
                .duration_ms
                .unwrap_or_default()
                .saturating_add(net_duration),
        );
    }

    if idle_time_ms > 0 {
        sum_ev.idle_time_ms = Some(
            sum_ev
                .idle_time_ms
                .unwrap_or_default()
                .saturating_add(idle_time_ms),
        );
    }
}

/// Factory for phase parent on_task_close callback.
fn create_phase_on_task_close(
    phase_finished_counter: Arc<AtomicU64>,
    node_count_in_phase_total: u64,
) -> ParentTaskSpanOnClose<FsResult<NodeStatus>> {
    Box::new(
        move |phase_span, _task_span, result: &FsResult<NodeStatus>| {
            update_span_attrs(phase_span, |attrs: &mut PhaseExecuted| {
                let outcome = result
                    .as_ref()
                    .map(|r| r.clone().into())
                    .unwrap_or(NodeOutcome::Error);
                match outcome {
                    NodeOutcome::Error => {
                        if let Some(c) = attrs.node_count_error.as_mut() {
                            *c += 1;
                        };
                    }
                    // Should not happen, since we have a separate skip handler,
                    // but just in case
                    NodeOutcome::Skipped => {
                        if let Some(c) = attrs.node_count_skipped.as_mut() {
                            *c += 1;
                        };
                    }
                    _ => {}
                }
            });

            // Increment finished counter
            let cur_finished = phase_finished_counter.fetch_add(1, Ordering::AcqRel) + 1;

            // Close when all tasks are done
            cur_finished == node_count_in_phase_total
        },
    )
}

/// Factory for phase parent on_task_skip callback.
fn create_phase_on_task_skip(
    phase_finished_counter: Arc<AtomicU64>,
    node_count_in_phase_total: u64,
) -> ParentTaskSpanOnSkip<SkipReason> {
    Box::new(move |phase_span, _task_span, _skip_reason| {
        update_span_attrs(phase_span, |attrs: &mut PhaseExecuted| {
            if let Some(c) = attrs.node_count_skipped.as_mut() {
                *c += 1;
            };
        });

        // Increment finished counter
        let cur_finished = phase_finished_counter.fetch_add(1, Ordering::AcqRel) + 1;

        // Close when all tasks are done
        cur_finished == node_count_in_phase_total
    })
}

/// Factory for node processed on_task_close callback.
fn create_node_processed_on_task_close(
    node_processed_finished_counter: Arc<AtomicU8>,
    task_count_total: u8,
) -> ParentTaskSpanOnClose<FsResult<NodeStatus>> {
    Box::new(move |this_span, task_span, _| {
        // Calculate duration of this NodeEvaluated span
        let phase_duration_ms = read_span_start_info(task_span, |start_info| {
            let now = std::time::SystemTime::now();
            now.duration_since(start_info.start_time_unix_nano)
                .ok()
                .map(|d| d.as_millis() as u64)
        })
        .flatten();

        update_span_attrs(this_span, |ev: &mut NodeProcessed| {
            // Update NodeProcessed from NodeEvaluated attributes
            let has_node_attrs = read_span_attrs::<NodeEvaluated, _>(task_span, |attrs| {
                accumulate_node_processed_phase_duration(ev, phase_duration_ms, Some(attrs));
                update_node_processed_from_node_evaluated(ev, attrs);
            })
            .is_some();

            if !has_node_attrs {
                accumulate_node_processed_phase_duration(ev, phase_duration_ms, None);
            }
        });

        // Increment finished counter
        let cur_finished = node_processed_finished_counter.fetch_add(1, Ordering::AcqRel) + 1;

        // Close when all tasks are done
        cur_finished == task_count_total
    })
}

/// Factory for node processed on_task_skip callback.
fn create_node_processed_on_task_skip(
    node_processed_finished_counter: Arc<AtomicU8>,
    task_count_total: u8,
) -> ParentTaskSpanOnSkip<SkipReason> {
    Box::new(move |this_span, task_span, _| {
        // Calculate duration of this NodeEvaluated span
        let phase_duration_ms = read_span_start_info(task_span, |start_info| {
            let now = std::time::SystemTime::now();
            now.duration_since(start_info.start_time_unix_nano)
                .ok()
                .map(|d| d.as_millis() as u64)
        })
        .flatten();

        update_span_attrs(this_span, |ev: &mut NodeProcessed| {
            // Update NodeProcessed from NodeEvaluated attributes
            let has_node_attrs = read_span_attrs::<NodeEvaluated, _>(task_span, |attrs| {
                accumulate_node_processed_phase_duration(ev, phase_duration_ms, Some(attrs));
                update_node_processed_from_node_evaluated(ev, attrs);
            })
            .is_some();

            if !has_node_attrs {
                accumulate_node_processed_phase_duration(ev, phase_duration_ms, None);
            }
        });

        // Increment finished counter
        let cur_finished = node_processed_finished_counter.fetch_add(1, Ordering::AcqRel) + 1;

        // Close when all tasks are done
        cur_finished == task_count_total
    })
}

/// Creates a builder_fn closure for the Phase parent span.
fn create_phase_parent_builder_fn(
    phase: ExecutionPhase,
    node_count_in_phase_total: u64,
) -> impl FnOnce() -> ParentSpanBuilder<FsResult<NodeStatus>, SkipReason> {
    move || {
        let phase_finished_counter = Arc::new(AtomicU64::new(0));
        let phase_finished_counter_clone = phase_finished_counter.clone();

        ParentSpanBuilder::new(
            PhaseExecuted::start_with_node_count(phase, node_count_in_phase_total).into(),
            // No special handling on close. PhaseExecuted automatically infers span status from
            // node counts.
            None,
            // Track when child tasks finish for counting
            Some(create_phase_on_task_close(
                phase_finished_counter_clone,
                node_count_in_phase_total,
            )),
            Some(create_phase_on_task_skip(
                phase_finished_counter,
                node_count_in_phase_total,
            )),
        )
    }
}

/// Creates a builder_fn closure for the NodeProcessed related span.
fn create_node_processed_builder_fn(
    node: Arc<dyn InternalDbtNodeAttributes>,
    in_dir: PathBuf,
    out_dir: PathBuf,
    initial_phase: ExecutionPhase,
    task_count_total: u8,
    in_selection: bool,
) -> impl FnOnce() -> ParentSpanBuilder<FsResult<NodeStatus>, SkipReason> {
    let node_processed_finished_counter = Arc::new(AtomicU8::new(0));
    let node_processed_finished_counter_clone = node_processed_finished_counter.clone();

    move || {
        ParentSpanBuilder::new(
            node.get_node_processed_event(
                Some(initial_phase),
                in_dir.as_path(),
                out_dir.as_path(),
                in_selection,
            )
            .into(),
            // Update dbt core code, status should auto-infer from attributes
            Some(Box::new(|this_span| {
                update_span_attrs(this_span, |ev: &mut NodeProcessed| {
                    update_dbt_core_event_code_for_node_processed_end(ev);
                })
            })),
            Some(create_node_processed_on_task_close(
                node_processed_finished_counter,
                task_count_total,
            )),
            Some(create_node_processed_on_task_skip(
                node_processed_finished_counter_clone,
                task_count_total,
            )),
        )
    }
}

/// Creates a builder_fn closure for the all processing nodes group parent span.
fn create_node_group_builder_fn(
    node_count_total: u64,
) -> impl FnOnce() -> ParentSpanBuilder<FsResult<NodeStatus>, SkipReason> {
    move || {
        let node_group_finished_counter = Arc::new(AtomicU64::new(0));
        let node_group_finished_counter_clone = node_group_finished_counter.clone();

        ParentSpanBuilder::new(
            TuiAllProcessingNodesGroup.into(),
            // No callbacks, pure TUI grouping span
            None,
            // Track when child tasks finish for counting
            Some(Box::new(move |_, _, _| {
                // Increment finished counter
                let cur_finished = node_group_finished_counter.fetch_add(1, Ordering::AcqRel) + 1;

                // Close when all tasks are done
                cur_finished == node_count_total
            })),
            Some(Box::new(move |_, _, _| {
                // Increment finished counter
                let cur_finished =
                    node_group_finished_counter_clone.fetch_add(1, Ordering::AcqRel) + 1;

                // Close when all tasks are done
                cur_finished == node_count_total
            })),
        )
    }
}

/// Pre-populate the span manager with all expected parent span builders.
///
/// These are lazily instantiated when visitor visits tasks, which reference
/// them by span key. See `Task::telemetry_tree` method for details.
///
/// This must be called before any task spans are created. It registers:
/// - Phase spans (one per phase with nodes in that phase)
/// - NodeProcessed spans (one per unique node)
/// - TuiAllProcessingNodesGroup span (single span for all nodes)
///
/// # Returns
/// - `Ok(())`: All builders registered successfully
/// - `Err`: A duplicate span key was encountered
pub fn populate_span_manager(
    span_manager: &SpanManager<FsResult<NodeStatus>, SkipReason>,
    schedule: &DiGraph<Arc<dyn Task>, ()>,
    in_dir: &Path,
    out_dir: &Path,
    selected_nodes: &std::collections::BTreeSet<String>,
) -> FsResult<()> {
    let node_counts = calculate_node_counts(schedule);

    // Register phase parent spans (no parent - root level)
    for (phase, &count) in &node_counts.node_counts_by_phase {
        let execution_phase: ExecutionPhase = (*phase).into();
        span_manager.register_parent_span_builder(
            phase_span_key(execution_phase),
            None, // Phase spans have no managed parent (will be placed under visitor span)
            Box::new(create_phase_parent_builder_fn(execution_phase, count)),
        )?;
    }

    // Register NodeProcessed spans (one per unique node)
    let mut seen_nodes = HashSet::new();
    for task in schedule.node_weights() {
        for node in task.dbt_nodes() {
            let unique_id = node.common().unique_id.clone();
            if seen_nodes.insert(unique_id.clone()) {
                let task_count_total = *node_counts
                    .node_unique_id_to_task_count
                    .get(&unique_id)
                    .unwrap_or(&1);

                let in_selection = selected_nodes.contains(&unique_id);

                span_manager.register_parent_span_builder(
                    node_processed_span_key(&unique_id),
                    // NodeProcessed has TuiAllProcessingNodesGroup as parent
                    Some(ParentSpanRef::new(
                        SpanLevel::Info,
                        all_processing_nodes_span_key().into(),
                    )),
                    Box::new(create_node_processed_builder_fn(
                        node.clone(),
                        in_dir.to_path_buf(),
                        out_dir.to_path_buf(),
                        ExecutionPhase::Unspecified,
                        task_count_total,
                        in_selection,
                    )),
                )?;
            }
        }
    }

    // Register the single TuiAllProcessingNodesGroup span
    span_manager.register_parent_span_builder(
        all_processing_nodes_span_key(),
        None, // TuiAllProcessingNodesGroup has no managed parent (will be placed under visitor span)
        Box::new(create_node_group_builder_fn(node_counts.node_count_total)),
    )?;

    Ok(())
}
