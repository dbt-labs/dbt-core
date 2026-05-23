use crate::materialize::materialize_snapshot;
use crate::runnable::cache::cache_materialization_return_value;
use crate::task::TaskResult;
use dbt_common::FsResult;
use dbt_common::stats::NodeStatus;
use dbt_jinja_utils::utils::add_task_context;
use dbt_schemas::schemas::{DbtSnapshot, InternalDbtNode};
use dbt_tasks_core::context::TaskRunnerCtx;

pub fn execute_snapshot_remote(
    snapshot: &DbtSnapshot,
    ctx: &TaskRunnerCtx,
    task_result: &TaskResult,
) -> FsResult<NodeStatus> {
    let sql_instruction = &task_result.sql_instruction;

    let mut base_context = ctx.inner.base_context.clone();
    add_task_context(&mut base_context, snapshot.common(), &ctx.thread_id);

    let relations_map = materialize_snapshot(
        &sql_instruction.sql,
        snapshot,
        ctx.adapter_type(),
        ctx.runtime_config(),
        &ctx.inner.materialization_resolver,
        ctx.env.clone(),
        &base_context,
        &ctx.inner.arg.io,
    )?;
    let _ = cache_materialization_return_value(ctx.env.clone(), &relations_map);

    Ok(NodeStatus::Succeeded)
}
