use std::sync::Arc;

use crate::materialize::{materialize_microbatch_model, materialize_model};
use crate::microbatch::{BatchContext, MicrobatchBuilder};
use crate::runnable::cache::cache_materialization_return_value;
use crate::runnable::microbatch::{build_event_time_mapping, is_incremental};
use dbt_common::FsResult;
use dbt_common::stats::NodeStatus;
use dbt_common::tracing::span_info::find_and_update_span_attrs;
use dbt_common::{ErrorCode, fs_err};
use dbt_jinja_utils::phases::run::build_run_node_context;
use dbt_jinja_utils::utils::add_task_context;
use dbt_schemas::schemas::DbtModel;
use dbt_schemas::schemas::common::{DbtIncrementalStrategy, DbtMaterialization};
use dbt_schemas::schemas::{InternalDbtNode, InternalDbtNodeAttributes};
use dbt_tasks_core::context::TaskRunnerCtx;
use dbt_telemetry::{ExecutionPhase, NodeEvaluated, NodeEvent, has_node_warning};

use minijinja::Value;
use tracing::debug;

use dbt_tasks_core::task::TaskResult;

/// Check if a model uses the microbatch incremental strategy.
pub fn try_get_microbatch_model(node: &dyn InternalDbtNodeAttributes) -> Option<&DbtModel> {
    if let Some(model) = node.as_any().downcast_ref::<DbtModel>()
        && model.materialized() == DbtMaterialization::Incremental
        && model.__model_attr__.incremental_strategy == Some(DbtIncrementalStrategy::Microbatch)
    {
        Some(model)
    } else {
        None
    }
}

/// A self-contained microbatch unit, ready to be executed.
#[derive(Clone)]
pub struct MicrobatchExecUnit {
    pub batch_ctx: BatchContext,
    pub raw_sql: Arc<String>,
    pub node: Arc<dyn InternalDbtNodeAttributes>,
    pub run_node_context: Arc<std::collections::BTreeMap<String, Value>>,
    pub event_time_mapping: Arc<std::collections::BTreeMap<String, String>>,
    pub is_incremental: bool,
}

/// Prepare microbatch batches for execution.
///
/// Returns concurrency groups: `[[first], [middle...], [last]]`.
/// Groups must be executed sequentially; batches within a group can run in parallel.
/// TODO(chasewalden): The "default" behavior is auto-detected on use of `{{ this }}` in the model
/// - `{{ this }}` used -> default to `false`
/// - `{{ this }}` not used -> default to `true`
///   See: https://docs.getdbt.com/docs/build/parallel-batch-execution#how-parallel-batch-execution-works
///   The actual logic from dbt-core: https://github.com/dbt-labs/dbt-core/blob/2857d45cc02c0b01cb018ec26be1a9079e1634a0/core/dbt/task/run.py#L413-L427
///
/// TODO(chasewalden): `dbt retry` can be used to re-process only the failed batches.
///  Seems like the `retry` subcommand doesn't exist in `fs` though...
pub fn prepare_microbatch_batches(
    node: Arc<dyn InternalDbtNodeAttributes>,
    ctx: &TaskRunnerCtx,
    task_result: &TaskResult,
) -> FsResult<Vec<Vec<MicrobatchExecUnit>>> {
    let mut base_context = ctx.inner.base_context.clone();

    add_task_context(&mut base_context, node.common(), &ctx.thread_id);

    let sql_header = task_result
        .config_map
        .get("sql_header")
        .map(|v| v.value().clone());

    let model = node
        .as_any()
        .downcast_ref::<DbtModel>()
        .expect("MicrobatchTask node must be a DbtModel");
    let unique_id = &model.__common_attr__.unique_id;

    let raw_sql = Arc::new(model.__common_attr__.raw_code.clone().ok_or_else(|| {
        fs_err!(
            ErrorCode::InvalidConfig,
            "Microbatch model {} has no raw_code populated. Raw code is required for batch-aware ref filtering.",
            unique_id,
        )
    })?);

    let batch_builder = MicrobatchBuilder::from_config(
        model.deprecated_config.batch_size.clone(),
        model.deprecated_config.begin.as_deref(),
        model.deprecated_config.lookback,
    )?;

    // Model-level `full_refresh` config overrides the CLI `--full-refresh` flag.
    // Matches dbt-core's `_is_incremental` function
    // https://github.com/dbt-labs/dbt-core/blob/ecefc59e660eae4b34194ee4150fd4302836f4ea/core/dbt/task/run.py#L745-L746
    let full_refresh = model
        .deprecated_config
        .full_refresh
        .unwrap_or(ctx.inner.arg.full_refresh);
    let is_incremental = is_incremental(model, full_refresh, ctx.adapter_type(), ctx.env.clone());

    let end_time = batch_builder.build_end_time(ctx.inner.arg.event_time_end.clone())?;
    let start_time = batch_builder.build_start_time(
        Some(end_time),
        ctx.inner.arg.event_time_start.clone(),
        is_incremental,
    )?;
    let batches = batch_builder.build_batches(start_time, end_time);

    if batches.is_empty() {
        debug!(
            "Microbatch model {} has no batches to process (start={}, end={})",
            unique_id, start_time, end_time
        );
        return Ok(vec![]);
    }

    debug!(
        "Executing microbatch model {} with {} batches (start={}, end={})",
        unique_id,
        batches.len(),
        start_time,
        end_time
    );

    let event_time_mapping = Arc::new(build_event_time_mapping(model, ctx.nodes()));

    let run_node_context = Arc::new(build_run_node_context(
        model,
        &model.deprecated_config,
        ctx.adapter_type(),
        None,
        &base_context,
        &ctx.inner.arg.io,
        ExecutionPhase::Run,
        sql_header,
        ctx.runtime_config().dependencies.keys().cloned().collect(),
    ));

    // Group batches: [[first], [mid1, mid2, ...], [last]]
    let concurrent = model.deprecated_config.concurrent_batches.unwrap_or(false);
    let groups = batches
        .chunk_by(|l, r| !l.is_first() && !r.is_last() && concurrent)
        .map(|group| {
            group
                .iter()
                .map(|batch| MicrobatchExecUnit {
                    batch_ctx: batch.clone(),
                    raw_sql: raw_sql.clone(),
                    node: node.clone(),
                    run_node_context: run_node_context.clone(),
                    event_time_mapping: event_time_mapping.clone(),
                    is_incremental,
                })
                .collect()
        })
        .collect();

    Ok(groups)
}

/// Execute a single microbatch task.
pub fn execute_microbatch_batch(mb_unit: MicrobatchExecUnit, ctx: &TaskRunnerCtx) -> FsResult<()> {
    let model = mb_unit
        .node
        .as_any()
        .downcast_ref::<DbtModel>()
        .expect("MicrobatchTask node must be a DbtModel");

    // Inject incremental override flags per batch
    let mut ctx_for_batch = (*mb_unit.run_node_context).clone();
    if !mb_unit.batch_ctx.is_first() || mb_unit.is_incremental {
        ctx_for_batch.insert(
            "is_incremental".to_string(),
            Value::from_function(|_args: &[Value]| Ok(Value::from(true))),
        );
        ctx_for_batch.insert(
            "should_full_refresh".to_string(),
            Value::from_function(|_args: &[Value]| Ok(Value::from(false))),
        );
    }

    match materialize_microbatch_model(
        &mb_unit.raw_sql,
        model,
        ctx.node_resolver(),
        ctx.runtime_config(),
        &ctx.inner.materialization_resolver,
        ctx.env.clone(),
        &mb_unit.batch_ctx,
        ctx_for_batch,
        mb_unit.event_time_mapping,
        &ctx.inner.arg.io,
    ) {
        Ok(relations_map) => {
            let _ = cache_materialization_return_value(ctx.env.clone(), &relations_map);
            Ok(())
        }
        Err(e) => Err(e),
    }
}

pub fn execute_model_remote(
    model: &DbtModel,
    ctx: &TaskRunnerCtx,
    task_result: &TaskResult,
) -> FsResult<NodeStatus> {
    let mut base_context = ctx.inner.base_context.clone();

    add_task_context(&mut base_context, model.common(), &ctx.thread_id);

    // return early if the node is a ephemeral or inline model since we don't need to execute it.
    if model.materialized() == DbtMaterialization::Ephemeral
        || model.materialized() == DbtMaterialization::Inline
    {
        return Ok(NodeStatus::NoOp);
    }

    let sql_header = task_result
        .config_map
        .get("sql_header")
        .map(|v| v.value().clone());

    // Traditional warehouse execution via Jinja materialization macros
    match materialize_model(
        &task_result.sql_instruction.sql,
        model,
        ctx.adapter_type(),
        ctx.runtime_config(),
        &ctx.inner.materialization_resolver,
        ctx.env.clone(),
        &base_context,
        &ctx.inner.arg.io,
        sql_header,
    ) {
        Ok(relations_map) => {
            let _ = cache_materialization_return_value(ctx.env.clone(), &relations_map);
        }
        Err(e) => {
            return Err(e);
        }
    }

    let mut had_warning = false;
    find_and_update_span_attrs(|attrs: &mut NodeEvaluated| {
        had_warning = has_node_warning(NodeEvent::Evaluated(attrs));
    });
    if had_warning {
        Ok(NodeStatus::SucceededWithWarning)
    } else {
        Ok(NodeStatus::Succeeded)
    }
}
