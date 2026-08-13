use std::collections::BTreeMap;
use std::sync::Arc;

use dbt_adapter::Adapter;
use dbt_common::FsResult;
use dbt_common::io_args::{EvalArgs, Phases};
use dbt_common::io_utils::checkpoint_maybe_exit;
use dbt_dag::schedule::Schedule;
use dbt_freshness::freshness::{
    FreshnessTimestamps, emit_freshness_stats, freshness_results_to_context, run_freshness,
    write_freshness_results_parquet, write_sources_json,
};
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_schemas::state::ResolverState;
use dbt_tasks_core::PreTaskRunData;
use dbt_tasks_sa::run_operation::run_operation_on_run;
use minijinja::Value;

/// Runs `dbt source freshness` to completion: on-run-start operations, the
/// freshness checks themselves, stats output, artifact writes (`sources.json`
/// and, with `--write-metadata`, the parquet layer), on-run-end operations with
/// the results in context, and the freshness-phase checkpoint.
///
/// The command has no task graph, so none of this can be left to the task
/// runner; it all happens before the runner starts.
///
/// `dbt source freshness` sets `phase` to [`Phases::Freshness`], so the
/// checkpoint ends the invocation and the returned value is rarely observed.
pub async fn run_source_freshness(
    arg: &EvalArgs,
    jinja_env: &JinjaEnv,
    resolved_state: &ResolverState,
    schedule: &Schedule<String>,
    adapter: Arc<Adapter>,
    base_context: &BTreeMap<String, Value>,
) -> FsResult<Option<Box<dyn PreTaskRunData>>> {
    let empty_results: Vec<()> = vec![];
    for operation in resolved_state.operations.on_run_start.iter() {
        run_operation_on_run(
            operation,
            &arg.io,
            &None,
            &None,
            None,
            jinja_env,
            base_context,
            Value::from_serialize(&empty_results),
        )
        .await?;
    }

    let results = run_freshness(
        &arg.io,
        schedule,
        resolved_state,
        adapter,
        jinja_env,
        true,
        arg.check_all,
    )
    .await?;
    emit_freshness_stats(&arg.io, &results);

    write_sources_json(
        &arg.io.out_dir,
        &arg.io.in_dir,
        &arg.io.invocation_id,
        resolved_state,
        &results,
    )?;
    if arg.write_metadata {
        write_freshness_results_parquet(&arg.io, resolved_state, &results);
    }

    let freshness_context_results = freshness_results_to_context(resolved_state, &results);
    for operation in resolved_state.operations.on_run_end.iter() {
        run_operation_on_run(
            operation,
            &arg.io,
            &None,
            &None,
            None,
            jinja_env,
            base_context,
            Value::from_serialize(&freshness_context_results),
        )
        .await?;
    }

    checkpoint_maybe_exit(arg, Phases::Freshness)?;

    Ok(Some(Box::new(FreshnessTimestamps(results))))
}
