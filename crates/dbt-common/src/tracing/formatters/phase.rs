use dbt_telemetry::{ExecutionPhase, PhaseExecuted};

use super::{constants::ACTION_WIDTH, duration::format_duration_fixed_width};

/// Get the phase action text for a given `ExecutionPhase` (lowercase, no padding)
pub fn get_phase_action(phase: ExecutionPhase) -> &'static str {
    match phase {
        ExecutionPhase::Analyze => "analyzing",
        ExecutionPhase::Clean => "cleaning",
        ExecutionPhase::Compare => "comparing",
        ExecutionPhase::Debug => "debugging",
        ExecutionPhase::DeferHydration => "defer hydrating",
        ExecutionPhase::FreshnessAnalysis => "analyzing freshness",
        ExecutionPhase::InitAdapter => "initializing adapter",
        ExecutionPhase::Lineage => "generating lineage",
        ExecutionPhase::LoadProject => "loading project",
        ExecutionPhase::NodeCacheHydration => "hydrating",
        ExecutionPhase::Parse => "parsing",
        ExecutionPhase::Render => "rendering",
        ExecutionPhase::Run => "running",
        ExecutionPhase::Schedule => "scheduling",
        ExecutionPhase::TaskGraphBuild => "building task graph",
        ExecutionPhase::Unspecified => "unknown phase",
    }
}

/// Get the padded phase action text for the progress bar or spinner.
/// Returns `None` for some phases as of now.
pub fn get_phase_progress_text(phase: ExecutionPhase) -> Option<String> {
    let action = match phase {
        ExecutionPhase::Analyze => "Analyzing",
        ExecutionPhase::Clean => "Cleaning",
        ExecutionPhase::Compare => "Comparing",
        ExecutionPhase::Debug => "Debugging",
        ExecutionPhase::DeferHydration => "Hydrating",
        ExecutionPhase::FreshnessAnalysis => "Analyzing",
        ExecutionPhase::InitAdapter => return None,
        ExecutionPhase::Lineage => "Generating",
        ExecutionPhase::LoadProject => "Loading",
        ExecutionPhase::NodeCacheHydration => "Hydrating",
        ExecutionPhase::Parse => "Parsing",
        ExecutionPhase::Render => "Rendering",
        ExecutionPhase::Run => "Running",
        ExecutionPhase::Schedule => "Scheduling",
        ExecutionPhase::TaskGraphBuild => "Building",
        ExecutionPhase::Unspecified => return None,
    };

    Some(format!("{:>width$}", action, width = ACTION_WIDTH))
}

/// Format a `PhaseExecuted` event for the start of a phase
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
