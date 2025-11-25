use dbt_telemetry::{ExecutionPhase, PhaseExecuted};

use super::duration::format_duration_fixed_width;

// Phase action constants (lowercase, no padding)
pub const ANALYZING: &str = "analyzing";
pub const RENDERING: &str = "rendering";
pub const RUNNING: &str = "running";
pub const HYDRATING: &str = "hydrating";
pub const COMPARING: &str = "comparing";
pub const CLEANING: &str = "cleaning";
pub const LOADING_PROJECT: &str = "loading project";
pub const PARSING: &str = "parsing";
pub const SCHEDULING: &str = "scheduling";
pub const INITIALIZING_ADAPTER: &str = "initializing adapter";
pub const DEFER_HYDRATING: &str = "defer hydrating";
pub const BUILDING_TASK_GRAPH: &str = "building task graph";
pub const ANALYZING_FRESHNESS: &str = "analyzing freshness";
pub const GENERATING_LINEAGE: &str = "generating lineage";
pub const DEBUGGING: &str = "debugging";

/// Get the phase action text for a given ExecutionPhase
/// Returns empty string for phases that don't have a corresponding action constant
pub fn get_phase_action(phase: ExecutionPhase) -> &'static str {
    match phase {
        ExecutionPhase::Render => RENDERING,
        ExecutionPhase::Analyze => ANALYZING,
        ExecutionPhase::Run => RUNNING,
        ExecutionPhase::NodeCacheHydration => HYDRATING,
        ExecutionPhase::Compare => COMPARING,
        ExecutionPhase::Clean => CLEANING,
        ExecutionPhase::LoadProject => LOADING_PROJECT,
        ExecutionPhase::Parse => PARSING,
        ExecutionPhase::Schedule => SCHEDULING,
        ExecutionPhase::InitAdapter => INITIALIZING_ADAPTER,
        ExecutionPhase::DeferHydration => DEFER_HYDRATING,
        ExecutionPhase::TaskGraphBuild => BUILDING_TASK_GRAPH,
        ExecutionPhase::FreshnessAnalysis => ANALYZING_FRESHNESS,
        ExecutionPhase::Lineage => GENERATING_LINEAGE,
        ExecutionPhase::Debug => DEBUGGING,
        ExecutionPhase::Unspecified => "unknown phase",
    }
}

/// Format a PhaseExecuted event for the start of a phase
///
/// Returns formatted string in the pattern:
/// `Started {phase_action}` or `Started {phase_action} ({total} nodes)` if total > 0
pub fn format_phase_executed_start(phase: &PhaseExecuted) -> String {
    let phase_action = get_phase_action(phase.phase());
    let total = phase.node_count_total.unwrap_or_default();

    if total > 0 {
        format!("Started {} ({} nodes)", phase_action, total)
    } else {
        format!("Started {}", phase_action)
    }
}

/// Format a PhaseExecuted event for the end of a phase
///
/// Returns formatted string in the pattern:
/// `Finished {phase_action} [duration]` or
/// `Finished {phase_action} [duration] ({total} nodes, {skipped} skipped, {errors} errors)` if total > 0
pub fn format_phase_executed_end(phase: &PhaseExecuted, duration: std::time::Duration) -> String {
    let phase_action = get_phase_action(phase.phase());
    let duration_formatted = format_duration_fixed_width(duration);
    let total = phase.node_count_total.unwrap_or_default();

    if total > 0 {
        let skipped = phase.node_count_skipped.unwrap_or_default();
        let errors = phase.node_count_error.unwrap_or_default();
        format!(
            "Finished {} [{}] ({} nodes, {} skipped, {} errors)",
            phase_action, duration_formatted, total, skipped, errors
        )
    } else {
        format!("Finished {} [{}]", phase_action, duration_formatted)
    }
}
