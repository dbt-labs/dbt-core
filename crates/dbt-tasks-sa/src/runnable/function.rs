use crate::materialize::materialize_function;
use dbt_common::{FsResult, io_args::FsCommand, stats::NodeStatus};
use dbt_jinja_utils::utils::add_task_context;
use dbt_schemas::schemas::{DbtFunction, InternalDbtNode};
use dbt_tasks_core::context::TaskRunnerCtx;
use dbt_tasks_core::task::TaskResult;

pub fn execute_function_remote(
    function: &DbtFunction,
    ctx: &TaskRunnerCtx,
    task_result: &TaskResult,
) -> FsResult<NodeStatus> {
    // Functions should only execute during "build" command, not "run" command
    if ctx.inner.arg.command == FsCommand::Run {
        return Ok(NodeStatus::NoOp);
    }

    if ctx.dbt_profile().db_config.trino_inline_udfs_enabled() {
        return Ok(NodeStatus::NoOp);
    }

    let mut base_context = ctx.inner.base_context.clone();

    add_task_context(&mut base_context, function.common(), &ctx.thread_id);

    let sql_instruction = &task_result.sql_instruction;

    // Execute the function materialization
    let _result = materialize_function(
        &sql_instruction.sql,
        function,
        ctx.adapter_type(),
        ctx.runtime_config(),
        &ctx.inner.materialization_resolver,
        ctx.env.clone(),
        &base_context,
        &ctx.inner.arg.io,
    )?;

    Ok(NodeStatus::Succeeded)
}
