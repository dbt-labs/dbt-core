//! Unified compilation pipeline abstraction for dbt

use dbt_common::{
    FsResult,
    cancellation::CancellationToken,
    io_args::LocalExecutionBackendKind,
    io_utils::{CSV_EXT, SQL_EXT},
    node_selector::{IndirectSelection, MethodName, SelectExpression, SelectionCriteria},
    tracing::emit::create_info_span,
    tracing::span_info::SpanStatusRecorder as _,
};
use dbt_compilation::core::DbtLoadedProject;
use dbt_dag::schedule::Schedule;
use dbt_scheduler::{
    args::SchedulerArgs,
    schedule::{build_schedule, modify_schedule_for_sidecar_compute_boundaries},
};
use dbt_schemas::{
    schemas::{
        InternalDbtNodeAttributes, StateArtifacts, common::DbtMaterialization, profiles::Execute,
    },
    state::{CacheState, ResolverState},
};
use dbt_telemetry::{ExecutionPhase, PhaseExecuted};

use tracing::Instrument as _;

use crate::debug::DebugArgs;

/// Common compilation pipeline phases
struct CompilationPipeline;

impl CompilationPipeline {
    /// Phase 3: Schedule
    /// Note: Phases 1 (Load) + 2 (Resolve) live in dbt-compilation/src/core.rs.
    pub async fn schedule_phase(
        schedule_args: SchedulerArgs,
        resolved_state: &ResolverState,
        previous_state: Option<&StateArtifacts>,
        atoms: Option<Vec<SelectExpression>>,
        local_execution_backend: LocalExecutionBackendKind,
        token: &CancellationToken,
    ) -> FsResult<Schedule<String>> {
        // Build selectors for incremental or use existing
        let mut resolved_selectors = resolved_state.resolved_selectors.clone();

        // Check if inline model exists
        let maybe_inline_model_name = resolved_state
            .nodes
            .models
            .values()
            .find(|model| model.materialized() == DbtMaterialization::Inline)
            .map(|model| model.__common_attr__.name.clone());

        // Handle inline model selection - override all selectors to select only the inline model
        if let Some(inline_model) = maybe_inline_model_name {
            // Create a selector that exactly matches the inline model name
            resolved_selectors.include = Some(SelectExpression::Atom(SelectionCriteria {
                method: MethodName::Fqn,
                method_args: vec![],
                value: inline_model,
                childrens_parents: false,
                parents_depth: None,
                children_depth: None,
                indirect: None,
                exclude: None,
            }));
            // Clear any excludes to ensure the inline model is selected
            resolved_selectors.exclude = None;
        }

        // Includes new selectors if given
        if let Some(ref atoms) = atoms {
            let expr = SelectExpression::Or(atoms.clone());

            // Merge with existing selectors if any
            resolved_selectors.include = match resolved_selectors.include {
                Some(existing) => Some(SelectExpression::And(vec![expr, existing])),
                None => Some(expr),
            };
        }

        // Create schedule
        let mut schedule = build_schedule(
            &schedule_args,
            &resolved_state.nodes,
            previous_state,
            &resolved_selectors,
            token,
            resolved_state.adapter_type,
        )?;
        let execute = Execute::from_compute_flag(local_execution_backend);
        if matches!(execute, Execute::Sidecar | Execute::Service) {
            modify_schedule_for_sidecar_compute_boundaries(&mut schedule, &resolved_state.nodes);
        }
        Ok(schedule)
    }

    fn build_atoms_from_cache_state(cache_state: &CacheState) -> Vec<SelectExpression> {
        let mut atoms = Vec::new();
        for path in cache_state
            .file_changes
            .new_files
            .iter()
            .chain(cache_state.file_changes.impacted_files.iter())
            .filter(|p| p.has_extension(SQL_EXT) || p.has_extension(CSV_EXT))
        {
            let criteria = SelectionCriteria {
                method: MethodName::Path,
                method_args: vec![],
                value: path.to_str().unwrap_or_default().to_string(),
                childrens_parents: false,
                parents_depth: Some(u32::MAX),
                children_depth: Some(u32::MAX),
                indirect: Some(IndirectSelection::Eager),
                exclude: None,
            };
            atoms.push(SelectExpression::Atom(criteria));
        }
        atoms
    }

    /// Helper: Build selector atoms from unique ids
    pub fn build_atoms_from_unique_ids(
        unique_ids: &[String],
        include_parents: bool,
        include_children: bool,
    ) -> Vec<SelectExpression> {
        let mut atoms = Vec::new();

        for unique_id in unique_ids {
            // Determine the appropriate selector method based on node type.
            // Sources must use source: selector, not FQN (matches dbt-core behavior
            // where QualifiedNameSelectorMethod uses non_source_nodes()).
            let (method, value) = if unique_id.starts_with("source.") {
                // Convert source unique_id to source selector pattern
                // source.package.source_name.table_name -> package.source_name.table_name
                let pattern = unique_id.strip_prefix("source.").unwrap_or(unique_id);
                (MethodName::Source, pattern.to_string())
            } else {
                (MethodName::Fqn, unique_id.to_string())
            };

            let criteria = SelectionCriteria {
                method,
                method_args: vec![],
                value,
                childrens_parents: false,
                parents_depth: {
                    if include_parents {
                        Some(u32::MAX)
                    } else {
                        None
                    }
                },
                children_depth: {
                    if include_children {
                        Some(u32::MAX)
                    } else {
                        None
                    }
                },
                indirect: Some(IndirectSelection::Eager),
                exclude: None,
            };
            atoms.push(SelectExpression::Atom(criteria));
        }
        atoms
    }
}

pub async fn schedule(
    resolved_state: &ResolverState,
    schedule_args: SchedulerArgs,
    previous_state: Option<&StateArtifacts>,
    local_execution_backend: LocalExecutionBackendKind,
    token: &CancellationToken,
) -> FsResult<Schedule<String>> {
    CompilationPipeline::schedule_phase(
        schedule_args,
        resolved_state,
        previous_state,
        None,
        local_execution_backend,
        token,
    )
    .await
}

/// Schedule with explicit select expressions (used by Pull command)
/// This REPLACES the resolved_selectors.include instead of merging,
/// making Pull behave like Run/Build with the given select.
pub async fn schedule_with_select(
    resolved_state: &ResolverState,
    mut schedule_args: SchedulerArgs,
    previous_state: Option<&StateArtifacts>,
    select_expr: SelectExpression,
    exclude_expr: Option<SelectExpression>,
    local_execution_backend: LocalExecutionBackendKind,
    token: &CancellationToken,
) -> FsResult<Schedule<String>> {
    // Clear resource_types to allow all node types (like Build does)
    // Pull's --select should work on models, not just sources
    schedule_args.resource_types.clear();
    schedule_args.exclude_resource_types.clear();

    // Create a modified resolved_state with the select expression as the selector
    let resolved_selectors = dbt_schemas::schemas::selectors::ResolvedSelector {
        include: Some(select_expr),
        exclude: exclude_expr,
        ..Default::default()
    };

    // Call build_schedule directly with the overridden selectors
    let mut schedule = build_schedule(
        &schedule_args,
        &resolved_state.nodes,
        previous_state,
        &resolved_selectors,
        token,
        resolved_state.adapter_type,
    )?;
    let execute = Execute::from_compute_flag(local_execution_backend);
    if matches!(execute, Execute::Sidecar | Execute::Service) {
        modify_schedule_for_sidecar_compute_boundaries(&mut schedule, &resolved_state.nodes);
    }
    Ok(schedule)
}

#[allow(clippy::too_many_arguments)]
pub async fn schedule_with_unique_ids(
    resolved_state: &ResolverState,
    schedule_args: SchedulerArgs,
    previous_state: Option<&StateArtifacts>,
    unique_ids: &[String],
    include_parents: bool,
    include_children: bool,
    local_execution_backend: LocalExecutionBackendKind,
    token: &CancellationToken,
) -> FsResult<Schedule<String>> {
    let atoms = CompilationPipeline::build_atoms_from_unique_ids(
        unique_ids,
        include_parents,
        include_children,
    );
    CompilationPipeline::schedule_phase(
        schedule_args,
        resolved_state,
        previous_state,
        Some(atoms),
        local_execution_backend,
        token,
    )
    .await
}

pub async fn schedule_with_cache_state(
    resolved_state: &ResolverState,
    schedule_args: SchedulerArgs,
    previous_state: Option<&StateArtifacts>,
    cache_state: &CacheState,
    local_execution_backend: LocalExecutionBackendKind,
    token: &CancellationToken,
) -> FsResult<Schedule<String>> {
    let atoms = CompilationPipeline::build_atoms_from_cache_state(cache_state);
    CompilationPipeline::schedule_phase(
        schedule_args,
        resolved_state,
        previous_state,
        Some(atoms),
        local_execution_backend,
        token,
    )
    .await
}

pub mod loaded_project {
    use crate::debug;

    use super::*;

    pub async fn debug(
        loaded_project: &DbtLoadedProject,
        debug_args: DebugArgs,
        token: &CancellationToken,
    ) -> FsResult<()> {
        let span = create_info_span(PhaseExecuted::start_general(ExecutionPhase::Debug));

        debug::debug(&debug_args, loaded_project, token.clone())
            .instrument(span.clone())
            .await
            .record_status(&span)
    }
}
