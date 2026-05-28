use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Instant;

use crate::runnable::function::execute_function_remote;
use crate::runnable::snapshot::execute_snapshot_remote;
use crate::runnable::test::execute_test_remote;
use dbt_adapter::time_machine::{SaoStatus, global_recorder, global_replayer};
use dbt_common::constants::RUNNING;
use dbt_common::stats::{NodeStatus, Stat};
use dbt_common::status_reporter::report_completed;
use dbt_common::tracing::emit::{emit_error_log_from_fs_error, emit_warn_log_message};
use dbt_common::tracing::span_info::find_and_update_span_attrs;
use dbt_common::{ErrorCode, FsResult, fs_err};
use dbt_schemas::schemas::manifest::saved_query::DbtSavedQuery;
use dbt_schemas::schemas::{
    DbtFunction, DbtModel, DbtSeed, DbtSnapshot, DbtSource, DbtTest, DbtUnitTest,
    InternalDbtNodeAttributes, NodePathKind, Nodes,
};
use dbt_tasks_core::context::TaskRunnerCtx;
use dbt_tasks_core::run_task_hooks::RunTaskHooks;
use dbt_tasks_core::task::TaskResult;
use dbt_tasks_core::task::{TP, Task, TaskOp};
use dbt_telemetry::{NodeEvaluated, NodeType};

use tokio::task::JoinSet;
use tracing::Instrument;
use vortex_events::run_model_event;

use crate::runnable::model::{
    execute_microbatch_batch, execute_model_remote, prepare_microbatch_batches,
    try_get_microbatch_model,
};
use crate::runnable::seed::{execute_seed_remote, maybe_resolve_remote_seed_column_hint};
use crate::runnable::unit_test::execute_unit_test_remote;
use dbt_tasks_core::run_cache::run_cache_service::{
    RunCacheAfterSuccess, RunCacheServiceDecision, confirm_run_cache_service_execution,
    execute_run_cache_service_clone, insert_compiled_view_definition,
    record_run_cache_service_execution, refresh_final_last_modified_epoch_for_node,
    run_cache_service_before_execution,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunExecutionPath {
    Local,
    Remote,
    SideCar,
}

pub struct RunTask {
    node: Arc<dyn InternalDbtNodeAttributes>,
    // Channel receiver for getting results
    result_receiver: parking_lot::Mutex<Option<mpsc::Receiver<TaskResult>>>,
    execution_path: RunExecutionPath,
    task_hooks: Arc<dyn RunTaskHooks>,
}

impl RunTask {
    pub fn new(
        node: Arc<dyn InternalDbtNodeAttributes>,
        result_receiver: Option<mpsc::Receiver<TaskResult>>,
        execution_path: RunExecutionPath,
        task_hooks: Arc<dyn RunTaskHooks>,
    ) -> Self {
        Self {
            node,
            result_receiver: parking_lot::Mutex::new(result_receiver),
            execution_path,
            task_hooks,
        }
    }
}

impl Task for RunTask {
    // Backpressure applied granularly at the node level
    fn run_task_with_backpressure<'a>(
        &'a self,
        ctx: &'a mut TaskRunnerCtx,
    ) -> Pin<Box<dyn Future<Output = FsResult<NodeStatus>> + Send + 'a>> {
        self.run_task(ctx)
    }

    fn run_task<'a>(
        &'a self,
        ctx: &'a mut TaskRunnerCtx,
    ) -> Pin<Box<dyn Future<Output = FsResult<NodeStatus>> + Send + 'a>> {
        Box::pin(async move {
            let unique_id = self.node.unique_id();
            let adapter_type = ctx.adapter_type();
            let max_threads = ctx.dbt_profile().threads;
            let mut result_receiver = { self.result_receiver.lock().take() };
            let task_result = receive_task_result(&unique_id, &mut result_receiver)?;
            let start_time = chrono::Utc::now();
            // Status lines (RUNNING + report_completed) cite the definition path so the
            // user sees where they wrote the code, not the run-phase artifact under target/.
            // Error messages, which are emitted with their own phase-accurate locations
            // attached during inner execution, are unaffected by this choice.
            let display_path = self
                .node
                .get_node_path(
                    NodePathKind::Definition,
                    ctx.inner.arg.io.in_dir.as_path(),
                    ctx.inner.arg.io.out_dir.as_path(),
                )
                .display()
                .to_string();

            if let Some(reporter) = ctx.inner.arg.io.status_reporter.as_ref() {
                reporter.show_progress(RUNNING, display_path.as_ref(), None);
            }

            let cache_enabled = self.execution_path == RunExecutionPath::Remote
                && !ctx.inner.run_cache_ctx.run_cache_service_requested
                && ctx.inner.arg.run_cache_mode.write_cache()
                && node_runs_with_cache(self.node.as_ref());

            let execution_started_at = Instant::now();
            let mut after_success = RunCacheAfterSuccess::None;
            let result = match self.execution_path {
                RunExecutionPath::Remote => {
                    // Step 0: Replay a cached SAO result if one was recorded
                    if let Some(status) = maybe_replay_remote_run(&self.node.unique_id()) {
                        return Ok(status);
                    }

                    // Nodes without a task_result (e.g. sources) go straight to execution
                    let Some(task_result) = task_result else {
                        return execute_remote_node_no_result(
                            self.node.as_ref(),
                            ctx,
                            &self.task_hooks,
                        )
                        .in_current_span()
                        .await;
                    };

                    // dbt State service and SAO are mutually exclusive — pick the
                    // active path here. Both feed back into a single
                    // `RunCacheServiceDecision`: the service path may emit
                    // Skip/Clone/Execute (with `after_success`); the SAO path may
                    // emit Skip (with `sao_stored_hash`), Execute (with
                    // `sao_guard`), or Disabled.
                    let run_cache_service_requested =
                        ctx.inner.run_cache_ctx.run_cache_service_requested;
                    let decision = if run_cache_service_requested {
                        insert_compiled_view_definition(ctx, self.node.as_ref(), &task_result);
                        run_cache_service_before_execution(ctx, self.node.as_ref(), &task_result)
                            .await
                    } else if cache_enabled {
                        TaskOp::r#async(self.task_hooks.check_sao_cache(
                            ctx,
                            Arc::clone(&self.node),
                            &task_result.sql_instruction.sql,
                        ))
                        .await?
                    } else {
                        RunCacheServiceDecision::Disabled
                    };

                    if let Some(node_hash) = decision.node_hash() {
                        ctx.inner
                            .node_hashes
                            .insert(unique_id.to_string(), node_hash);
                    }

                    if let RunCacheServiceDecision::Skip {
                        status,
                        sao_stored_hash,
                        cached_test_failures,
                    } = &decision
                    {
                        let source = sao_stored_hash.as_deref().unwrap_or("run-cache-service");
                        // For data tests, override the generic
                        // ReusedNoChanges status with a test-shaped verdict
                        // and insert a Stat carrying the cached failures
                        // count plus a NO-OP marker, so run_results.json
                        // looks like dbt-core's _DataTestAdapterProxy
                        // produces.
                        let final_status = if let (Some(failures), Some(test)) = (
                            cached_test_failures,
                            self.node.as_any().downcast_ref::<DbtTest>(),
                        ) {
                            let failures_usize = (*failures).max(0) as usize;
                            let severity =
                                test.deprecated_config.severity.clone().unwrap_or_default();
                            // Mirror the non-cached test path: bump the
                            // matching invocation counter so the dbt
                            // command's returncode reflects test failures /
                            // warnings. Without this, cached failing tests
                            // still report exit 0 even though their status
                            // is correct.
                            let (test_status, metric_key) = if failures_usize == 0 {
                                (NodeStatus::TestPassed, None)
                            } else {
                                match severity {
                                    dbt_schemas::schemas::common::Severity::Warn => (
                                        NodeStatus::TestWarned,
                                        Some(
                                            dbt_common::tracing::dbt_metrics::InvocationMetricKey::TotalWarnings,
                                        ),
                                    ),
                                    dbt_schemas::schemas::common::Severity::Error => (
                                        NodeStatus::Errored,
                                        Some(
                                            dbt_common::tracing::dbt_metrics::InvocationMetricKey::TotalErrors,
                                        ),
                                    ),
                                }
                            };
                            if let Some(key) = metric_key {
                                dbt_common::tracing::metrics::increment_metric(
                                    dbt_common::tracing::dbt_metrics::FusionMetricKey::InvocationMetric(key),
                                    1,
                                );
                            }
                            ctx.inner.run_stats.insert(
                                unique_id.clone(),
                                Stat::new(
                                    unique_id.clone(),
                                    start_time.into(),
                                    Some(failures_usize),
                                    test_status.clone(),
                                    Some("NO-OP - cached test result".to_string()),
                                    ctx.thread_id,
                                ),
                            );
                            test_status
                        } else {
                            record_cache_skip(&unique_id, status, source);
                            status.clone()
                        };
                        Ok(final_status)
                    } else {
                        // Clone case is handled inline so the Err path can surface
                        // a warn (see PR #10184 review). Other decisions just
                        // derive after_success and fall through to normal exec.
                        let mut clone_status: Option<NodeStatus> = None;
                        let after_success_inner = match &decision {
                            RunCacheServiceDecision::Clone { clone } => {
                                match execute_run_cache_service_clone(
                                    ctx,
                                    self.node.as_ref(),
                                    clone,
                                    adapter_type,
                                    max_threads,
                                )
                                .await
                                {
                                    Ok(status) => {
                                        confirm_run_cache_service_execution(
                                            ctx,
                                            self.node.as_ref(),
                                            clone.success_confirmation(),
                                            Some(elapsed_millis(execution_started_at)),
                                        )
                                        .await;
                                        clone_status = Some(status);
                                        RunCacheAfterSuccess::None
                                    }
                                    Err(err) => {
                                        emit_warn_log_message(
                                            ErrorCode::RunCacheServiceWarn,
                                            format!(
                                                "dbt State service clone failed for node {}: {err}; executing normally",
                                                self.node.unique_id()
                                            ),
                                            None,
                                        );
                                        clone
                                            .fallback_confirmation()
                                            .map(RunCacheAfterSuccess::Confirm)
                                            .unwrap_or(RunCacheAfterSuccess::None)
                                    }
                                }
                            }
                            RunCacheServiceDecision::Execute { after_success, .. } => {
                                after_success.clone()
                            }
                            _ => RunCacheAfterSuccess::None,
                        };

                        after_success = after_success_inner;

                        // Clone succeeded — skip the model body but stay in the
                        // outer flow so run_stats and post-processing fire.
                        if let Some(status) = clone_status {
                            Ok(status)
                        } else {
                            // Node is Microbatch
                            // Requires us to produce batches and ensure each batch respects connection limits
                            let res = if try_get_microbatch_model(self.node.as_ref()).is_some() {
                                let batch_groups = prepare_microbatch_batches(
                                    self.node.clone(),
                                    ctx,
                                    &task_result,
                                )?;
                                for group in batch_groups {
                                    let batch_span = tracing::Span::current();
                                    let mut batch_tasks = group
                                        .into_iter()
                                        .map(|task| {
                                            let ctx = ctx.clone();
                                            TaskOp::BlockingWithConnection {
                                                f: Box::new(move || {
                                                    execute_microbatch_batch(task, &ctx)
                                                }),
                                                adapter_type,
                                                max_threads,
                                            }
                                            .run()
                                            .instrument(batch_span.clone())
                                        })
                                        .collect::<JoinSet<_>>();

                                    while let Some(res) = batch_tasks.join_next().await {
                                        res.map_err(Into::into).flatten()??;
                                    }
                                }
                                Ok(NodeStatus::Succeeded)
                            // Node is Saved Query
                            } else if let Some(saved_query) =
                                self.node.as_any().downcast_ref::<DbtSavedQuery>()
                            {
                                self.task_hooks
                                    .execute_saved_query(ctx, saved_query)
                                    .await
                                    .map(|_| NodeStatus::Succeeded)
                            // Node is Unit Test
                            } else if let Some(unit_test) =
                                self.node.as_any().downcast_ref::<DbtUnitTest>()
                            {
                                // Unit tests require a special remote execution path because we
                                // may need to compare the results in the sidecar (which is async)
                                // to determine pass/fail
                                let ctx_inner = ctx.clone();
                                let task_result_inner = task_result.clone();
                                let node_inner = self.node.clone();
                                let (status, result) = TaskOp::BlockingWithConnection {
                                    f: Box::new(move || {
                                        let unit_test = node_inner
                                            .as_any()
                                            .downcast_ref::<DbtUnitTest>()
                                            .unwrap();
                                        execute_unit_test_remote(
                                            unit_test,
                                            &ctx_inner,
                                            &task_result_inner,
                                        )
                                    }),
                                    adapter_type,
                                    max_threads,
                                }
                                .run()
                                .await??;
                                if let Some(result) = result {
                                    self.task_hooks
                                        .did_run_unit_test(
                                            ctx,
                                            unit_test,
                                            &task_result,
                                            result.passed,
                                            result.diff_num_rows,
                                            result.diff,
                                        )
                                        .await?;
                                }
                                Ok(status)
                            // Default execution for other node types
                            } else {
                                let ctx_inner = ctx.clone();
                                let task_result_inner = task_result.clone();
                                let node = self.node.clone();
                                let res = TaskOp::BlockingWithConnection {
                                    f: Box::new(move || {
                                        execute_remote_node(
                                            node.as_ref(),
                                            &ctx_inner,
                                            &task_result_inner,
                                        )
                                    }),
                                    adapter_type,
                                    max_threads,
                                }
                                .run()
                                .await?;
                                maybe_resolve_remote_seed_column_hint(res, self.node.as_ref(), ctx)
                                    .await
                            };
                            if res.is_ok() {
                                decision.finalize(ctx).await?;
                            }
                            res
                        }
                    }
                }
                RunExecutionPath::SideCar => {
                    self.task_hooks
                        .run_alt_compute_sidecar(ctx, Arc::clone(&self.node), task_result.clone())
                        .await
                }
                // Out of Scope for Connection Backpressure (or splitting)
                RunExecutionPath::Local => {
                    self.task_hooks
                        .run_alt_compute_local(ctx, Arc::clone(&self.node), task_result.clone())
                        .await
                }
            };

            if result.is_ok() {
                run_cache_after_success_action(
                    ctx,
                    self.node.as_ref(),
                    std::mem::replace(&mut after_success, RunCacheAfterSuccess::None),
                    Some(elapsed_millis(execution_started_at)),
                )
                .await;
            }

            find_and_update_span_attrs(|attrs: &mut NodeEvaluated| {
                attrs.sao_enabled = Some(cache_enabled);
            });

            // Get status and insert stats
            // Note: Inner visit_run implementations may insert their own stats on success,
            // but we need to ensure stats are inserted even when errors occur early.
            // The DashMap will just overwrite if there's a duplicate.
            // Ensure stats are always inserted, even if inner visit_run had early returns
            let node_status = match result {
                Ok(node_status) => {
                    // Show completion in the same style as remote
                    report_completed(
                        &node_status,
                        self.node.defined_at().cloned(),
                        display_path.as_str(),
                        cache_enabled,
                        ctx.inner.arg.io.status_reporter.as_ref(),
                    );

                    // Insert stats for success case if not already inserted by inner implementation
                    // This ensures stats are present even if inner code didn't insert them
                    if !ctx.inner.run_stats.contains_key(&unique_id) {
                        ctx.inner.run_stats.insert(
                            unique_id.clone(),
                            Stat::new(
                                unique_id.clone(),
                                start_time.into(),
                                None,
                                node_status.clone(),
                                None,
                                ctx.thread_id,
                            ),
                        );
                    }

                    node_status
                }
                Err(e) => {
                    // TODO: At some point, these should log as part of the same event
                    let node_status = NodeStatus::Errored;
                    report_completed(
                        &NodeStatus::Errored,
                        self.node.defined_at().cloned(),
                        display_path.as_str(),
                        false,
                        ctx.inner.arg.io.status_reporter.as_ref(),
                    );

                    if matches!(
                        self.execution_path,
                        RunExecutionPath::Remote | RunExecutionPath::SideCar
                    ) {
                        emit_error_log_from_fs_error(
                            e.as_ref(),
                            ctx.inner.arg.io.status_reporter.as_ref(),
                        );
                    }

                    // Insert stats for the error case so it appears in run_results.json
                    ctx.inner.run_stats.insert(
                        unique_id.clone(),
                        Stat::new(
                            unique_id.clone(),
                            start_time.into(),
                            None,
                            node_status.clone(),
                            Some(e.to_string()),
                            ctx.thread_id,
                        ),
                    );

                    node_status
                }
            };

            // Regression guard: run_stats must be populated before telemetry
            // emission. PR #9146 broke this by emitting before stats were
            // inserted for dbt State-reused models.
            debug_assert!(
                ctx.inner.run_stats.contains_key(&unique_id),
                "run_stats missing for {unique_id} before telemetry emission"
            );

            // TODO: migrate this to Vortex tracing layer
            // TODO: migrate this to structured logger
            if ctx.inner.arg.io.send_anonymous_usage_stats {
                emit_run_usage_stats(self.node.as_ref(), ctx, self.execution_path);
            }

            Ok(node_status)
        })
    }

    fn task_type(&self) -> &str {
        "run"
        // "run_local"
    }

    fn resource_type(&self) -> NodeType {
        self.node.resource_type()
    }

    fn work_node_id(&self) -> &str {
        self.node.common().unique_id.as_str()
    }

    fn dbt_nodes(&self) -> Vec<Arc<dyn InternalDbtNodeAttributes>> {
        vec![self.node.clone()]
    }

    fn task_phase(&self) -> Option<TP> {
        Some(TP::Run)
    }
}

fn maybe_replay_remote_run(unique_id: &str) -> Option<NodeStatus> {
    // Check for SAO skip events during time machine replay.
    // If this node was skipped due to SAO during recording, we should skip it during replay too.
    let replayer = global_replayer()?;
    let sao_event = replayer.get_sao_event(unique_id)?;
    Some(sao_event.to_node_status())
}

async fn run_cache_after_success_action(
    ctx: &TaskRunnerCtx,
    node: &dyn InternalDbtNodeAttributes,
    after_success: RunCacheAfterSuccess,
    execution_runtime_ms: Option<i64>,
) {
    match after_success {
        RunCacheAfterSuccess::None => {
            // The dbt State submission path (`submit_seed` / `submit_*`) probes
            // `last_modified_epoch_for_node` before deciding whether to
            // submit. When the target table doesn't exist on the warehouse
            // yet — which is always true on a node's first build — that
            // probe caches `Some(None)` in `run_cache_metadata` and the
            // submit is skipped. The node then materializes, but nothing in
            // the current invocation invalidates the cached `None`. The
            // prefetch miss-filter in `prefetch_last_modified_epochs` treats
            // `Some(None)` as a hit (not a miss), so downstream models
            // never re-query and see the upstream as missing — tripping
            // their own `metadata_complete = false` and submit-skipping
            // them too. Re-fetching the just-executed node's epoch here
            // replaces that stale `None` with the real value so downstream
            // consumers see the relation correctly.
            // The `Confirm` arm gets the same refresh as the first step of
            // `confirm_run_cache_service_execution`; we mirror it here for
            // the no-confirmation case so the cache stays coherent
            // regardless of which path the submit took.
            if ctx.inner.run_cache_ctx.run_cache_service_requested
                && let Err(err) = refresh_final_last_modified_epoch_for_node(ctx, node).await
            {
                emit_warn_log_message(
                    ErrorCode::RunCacheServiceWarn,
                    format!(
                        "dbt State post-execution metadata refresh failed for node {}: {err}; command remains successful",
                        node.unique_id()
                    ),
                    None,
                );
            }
        }
        RunCacheAfterSuccess::Confirm(mut confirmation) => {
            // Data tests: lift the just-executed failures count from
            // run_stats into the confirmation so future runs can replay it.
            // process_test_result inserts the stat before this hook fires,
            // so the value is available here.
            if node.as_any().is::<DbtTest>() {
                if let Some(stat) = ctx.inner.run_stats.get(node.unique_id().as_str()) {
                    if let Some(failures) = stat.num_rows {
                        confirmation.set_test_execution_results(failures as i64);
                    }
                }
            }
            confirm_run_cache_service_execution(
                ctx,
                node,
                Some(confirmation),
                execution_runtime_ms,
            )
            .await;
        }
        RunCacheAfterSuccess::Record(record) => {
            record_run_cache_service_execution(ctx, node, Some(*record), execution_runtime_ms)
                .await;
        }
    }
}

fn elapsed_millis(started_at: Instant) -> i64 {
    i64::try_from(started_at.elapsed().as_millis()).unwrap_or(i64::MAX)
}

/// Receives the task result from the channel, consuming the receiver.
/// Called once in `run_task()` before the local/remote split so both paths
/// work with `Option<TaskResult>` instead of threading the receiver through.
fn receive_task_result(
    unique_id: &str,
    result_receiver: &mut Option<mpsc::Receiver<TaskResult>>,
) -> FsResult<Option<TaskResult>> {
    if let Some(receiver) = result_receiver {
        match receiver.try_recv() {
            Ok(result) => Ok(Some(result)),
            Err(mpsc::TryRecvError::Empty) => Err(fs_err!(
                ErrorCode::Generic,
                "Failed to receive render result for '{}'. Empty Channel.",
                unique_id
            )),
            Err(mpsc::TryRecvError::Disconnected) => Err(fs_err!(
                ErrorCode::Generic,
                "Failed to receive render result for '{}'. Channel Disconnected.",
                unique_id
            )),
        }
    } else {
        Ok(None)
    }
}

fn record_cache_skip(unique_id: &str, task_status: &NodeStatus, source: &str) {
    let Some(recorder) = global_recorder() else {
        return;
    };

    let sao_status = match task_status {
        NodeStatus::ReusedNoChanges(message) => Some((SaoStatus::ReusedNoChanges, message.clone())),
        NodeStatus::ReusedStillFresh(message, freshness, last_updated) => Some((
            SaoStatus::ReusedStillFresh {
                freshness_seconds: *freshness,
                last_updated_seconds: *last_updated,
            },
            message.clone(),
        )),
        NodeStatus::ReusedStillFreshNoChanges(message) => {
            Some((SaoStatus::ReusedStillFreshNoChanges, message.clone()))
        }
        NodeStatus::ReusedCloned(freshness) => Some((
            SaoStatus::ReusedCloned {
                freshness_seconds: *freshness,
            },
            task_status.default_message(),
        )),
        _ => None,
    };

    if let Some((status, message)) = sao_status {
        recorder.record_sao_skip(unique_id, status, &message, source);
    }
}

fn emit_run_usage_stats(
    node: &dyn InternalDbtNodeAttributes,
    ctx: &TaskRunnerCtx,
    execution_path: RunExecutionPath,
) {
    let (maybe_incremental_strategy, is_contract_enforced, has_group, table_format, catalog_name) =
        match execution_path {
            RunExecutionPath::Remote | RunExecutionPath::SideCar => {
                if let Some(model) = node.as_any().downcast_ref::<DbtModel>() {
                    (
                        model
                            .__model_attr__
                            .incremental_strategy
                            .as_ref()
                            .map(|s| s.to_string()),
                        model
                            .__model_attr__
                            .contract
                            .as_ref()
                            .map(|c| c.enforced)
                            .unwrap_or(false),
                        model.__model_attr__.group.is_some(),
                        model.__model_attr__.table_format.clone(),
                        model.__model_attr__.catalog_name.clone(),
                    )
                } else {
                    (None, false, false, None, None)
                }
            }
            RunExecutionPath::Local => (None, false, false, None, None),
        };

    run_model_event(
        ctx.inner.arg.io.invocation_id.to_string(),
        &ctx.inner.run_stats,
        node,
        maybe_incremental_strategy,
        is_contract_enforced,
        has_group,
        table_format,
        catalog_name,
    );
}

fn node_runs_with_cache(node: &dyn InternalDbtNodeAttributes) -> bool {
    node.as_any().is::<DbtModel>()
        || node.as_any().is::<DbtSnapshot>()
        || node.as_any().is::<DbtSeed>()
        || node.as_any().is::<DbtUnitTest>()
}

fn execute_remote_node(
    node: &dyn InternalDbtNodeAttributes,
    ctx: &TaskRunnerCtx,
    task_result: &TaskResult,
) -> FsResult<NodeStatus> {
    if let Some(model) = node.as_any().downcast_ref::<DbtModel>() {
        execute_model_remote(model, ctx, task_result)
    } else if let Some(test) = node.as_any().downcast_ref::<DbtTest>() {
        execute_test_remote(test, ctx, task_result)
    } else if let Some(snapshot) = node.as_any().downcast_ref::<DbtSnapshot>() {
        execute_snapshot_remote(snapshot, ctx, task_result)
    } else if let Some(seed) = node.as_any().downcast_ref::<DbtSeed>() {
        execute_seed_remote(seed, ctx)
    } else if node.as_any().downcast_ref::<DbtSource>().is_some() {
        Ok(NodeStatus::Succeeded)
    } else if let Some(function) = node.as_any().downcast_ref::<DbtFunction>() {
        execute_function_remote(function, ctx, task_result)
    } else {
        Err(fs_err!(
            ErrorCode::Unexpected,
            "Node {} is not runnable in remote mode",
            node.unique_id()
        ))
    }
}

/// Execute a remote node that has no task result (e.g. sources, seeds without render).
async fn execute_remote_node_no_result(
    node: &dyn InternalDbtNodeAttributes,
    ctx: &TaskRunnerCtx,
    task_hooks: &Arc<dyn RunTaskHooks>,
) -> FsResult<NodeStatus> {
    if node.as_any().downcast_ref::<DbtSource>().is_some() {
        Ok(NodeStatus::Succeeded)
    } else if let Some(seed) = node.as_any().downcast_ref::<DbtSeed>() {
        let res = execute_seed_remote(seed, ctx);
        maybe_resolve_remote_seed_column_hint(res, node, ctx).await
    } else if let Some(saved_query) = node.as_any().downcast_ref::<DbtSavedQuery>() {
        task_hooks
            .execute_saved_query(ctx, saved_query)
            .await
            .map(|_| NodeStatus::Succeeded)
    } else {
        Err(fs_err!(
            ErrorCode::Generic,
            "Failed to receive render result for {}",
            node.unique_id()
        ))
    }
}

/// Determine whether to prefer SQL over LP for local execution of a node.
pub fn prefer_sql_for_node(node: &dyn InternalDbtNodeAttributes, ctx: &TaskRunnerCtx) -> bool {
    if node.as_any().is::<DbtSnapshot>() {
        true
    } else if node.as_any().is::<DbtModel>() {
        ctx.adapter_type() == dbt_adapter::AdapterType::DuckDB
    } else {
        false
    }
}

pub fn runnable_remote_task(
    nodes: &Nodes,
    unique_id: &str,
) -> Option<Arc<dyn InternalDbtNodeAttributes>> {
    if let Some(model) = nodes.models.get(unique_id) {
        Some(model.clone() as Arc<dyn InternalDbtNodeAttributes>)
    } else if let Some(test) = nodes.tests.get(unique_id) {
        Some(test.clone() as Arc<dyn InternalDbtNodeAttributes>)
    } else if let Some(snapshot) = nodes.snapshots.get(unique_id) {
        Some(snapshot.clone() as Arc<dyn InternalDbtNodeAttributes>)
    } else if let Some(seed) = nodes.seeds.get(unique_id) {
        Some(seed.clone() as Arc<dyn InternalDbtNodeAttributes>)
    } else if let Some(source) = nodes.sources.get(unique_id) {
        Some(source.clone() as Arc<dyn InternalDbtNodeAttributes>)
    } else if let Some(unit_test) = nodes.unit_tests.get(unique_id) {
        Some(unit_test.clone() as Arc<dyn InternalDbtNodeAttributes>)
    } else if let Some(function) = nodes.functions.get(unique_id) {
        Some(function.clone() as Arc<dyn InternalDbtNodeAttributes>)
    } else if let Some(saved_query) = nodes.saved_queries.get(unique_id) {
        Some(saved_query.clone() as Arc<dyn InternalDbtNodeAttributes>)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn elapsed_millis_saturates_for_large_durations() {
        assert!(elapsed_millis(Instant::now()) >= 0);
    }
}
