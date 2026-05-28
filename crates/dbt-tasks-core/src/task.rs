use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use dbt_adapter::connection::{ConnectionBackpressure, ThreadLocalConnectionRecycleGuard};
use dbt_adapter_core::AdapterType;
use dbt_common::stats::NodeStatus;
use dbt_common::{ErrorCode, FsResult, fs_err};
use dbt_dag::schedule::Schedule;
use dbt_schemas::schemas::profiles::Execute;
use dbt_schemas::schemas::properties::UnitTestOverrides;
use dbt_schemas::schemas::telemetry::{ExecutionPhase, NodeType};
use dbt_schemas::schemas::{InternalDbtNodeAttributes, Nodes};

use crate::context::TaskRunnerCtx;
use crate::render_task_hooks::RenderTaskHooks;
use crate::run_task_hooks::RunTaskHooks;
use crate::span_manager::{SpanLevel, SpanTreeRequest};
use crate::task_spans::{
    node_processed_span_key, phase_span_key, task_span_on_close, task_span_on_skip,
    update_node_outcome_from_skip_reason,
};
use crate::test_aggregation::GenericTestAggregation;
use crate::visitor::SkipReason;

/// Stands for Task Phase
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum TP {
    Render,
    Analyze,
    Run,
    Compare,
    Show,
}

impl fmt::Display for TP {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                TP::Render => "render",
                TP::Analyze => "analyze",
                TP::Run => "run",
                TP::Compare => "compare",
                TP::Show => "show",
            }
        )
    }
}

impl From<TP> for ExecutionPhase {
    fn from(tp: TP) -> Self {
        match tp {
            TP::Render => ExecutionPhase::Render,
            TP::Analyze => ExecutionPhase::Analyze,
            TP::Run => ExecutionPhase::Run,
            TP::Show => ExecutionPhase::Run,
            TP::Compare => ExecutionPhase::Compare,
        }
    }
}

/// Awaitable scheduling dispatch for a single unit of work.
///
/// Each variant handles its own thread pool dispatch (spawn_blocking, async, backpressure).
/// Use this to build purely functional pipelines where each step's output feeds the next:
///
/// ```ignore
/// let relations = TaskOp::blocking(|| discover(ctx)).run().await??;
/// TaskOp::r#async(fetch(relations, ctx)).run().await??;
/// let result = TaskOp::blocking(|| render(ctx, relations)).run().await??;
/// ```
pub enum TaskOp<T: Send + 'static> {
    /// CPU-bound work, dispatched to `tokio::task::spawn_blocking`.
    Blocking(Box<dyn FnOnce() -> T + Send>),
    /// CPU-bound work that needs a DB connection.
    /// Waits for connection backpressure, then dispatched to `spawn_blocking`.
    BlockingWithConnection {
        f: Box<dyn FnOnce() -> T + Send>,
        adapter_type: AdapterType,
        max_threads: Option<usize>,
    },
}

impl<T: Send + 'static> TaskOp<T> {
    /// Marker: wraps an async future for pipeline visibility.
    /// Just awaits the future — no thread pool dispatch.
    pub async fn r#async<F: Future<Output = T>>(fut: F) -> T {
        fut.await
    }
    /// Marker: runs a sync closure inline on the current async task.
    /// No thread pool dispatch — use for lightweight sync work that
    /// doesn't justify a `spawn_blocking` thread.
    pub fn inline_blocking(f: impl FnOnce() -> T) -> T {
        f()
    }
    /// Execute this operation on the appropriate thread pool.
    pub async fn run(self) -> FsResult<T> {
        match self {
            TaskOp::Blocking(f) => dbt_common::tracing::spawn_blocking_traced(f)
                .await
                .map_err(|e| fs_err!(ErrorCode::Generic, "spawn_blocking join error: {}", e)),
            TaskOp::BlockingWithConnection {
                f,
                adapter_type,
                max_threads,
            } => {
                let _guard = ConnectionBackpressure::from_config(adapter_type, max_threads).await;
                dbt_common::tracing::spawn_blocking_traced(Box::new(move || {
                    let _recycle_guard = ThreadLocalConnectionRecycleGuard::new();
                    f()
                }))
                .await
                .map_err(|e| fs_err!(ErrorCode::Generic, "spawn_blocking join error: {}", e))
            }
        }
    }
}

pub trait Task: Send + Sync {
    fn run_task<'a>(
        &'a self,
        ctx: &'a mut TaskRunnerCtx,
    ) -> Pin<Box<dyn Future<Output = FsResult<NodeStatus>> + Send + 'a>>;

    /// Apply backpressure to task scheduling.
    ///
    /// This tells the `tokio` runtime to poll for readiness later when demand
    /// for database connections is below the maximum level we target.
    fn run_task_with_backpressure<'a>(
        &'a self,
        ctx: &'a mut TaskRunnerCtx,
    ) -> Pin<Box<dyn Future<Output = FsResult<NodeStatus>> + Send + 'a>> {
        Box::pin(async move {
            let backpressure =
                ConnectionBackpressure::from_config(ctx.adapter_type(), ctx.dbt_profile().threads);
            let _wake_next_on_drop = backpressure.await;
            self.run_task(ctx).await
        })
    }

    fn resource_type(&self) -> NodeType;

    fn task_type(&self) -> &str;

    /// Returns the identifier that groups together tasks operating on the same logical work node.
    fn work_node_id(&self) -> &str;

    /// Returns the collection of underlying dbt nodes that this task operates on.
    fn dbt_nodes(&self) -> Vec<Arc<dyn InternalDbtNodeAttributes>>;

    /// Returns the task phase if applicable, otherwise None.
    /// As of today only the barrier task does not belong to a phase.
    fn task_phase(&self) -> Option<TP>;

    /// Returns telemetry (sub)tree request that task visitor should create for this task
    /// via span manager.
    ///
    /// Uses span keys that reference pre-registered builders in the span manager.
    /// The span manager must be pre-populated with builders for all referenced keys
    /// before this method is called.
    ///
    /// By default:
    /// - Tasks that operate on a single dbt node will run under a `NodeEvaluated` span with:
    ///   - Parent: `PhaseExecuted` span for the task's phase
    ///   - Related: `NodeProcessed` span for the node
    ///   - see `task_spans` module for details.
    /// - Tasks that operate on multiple dbt nodes will request
    ///   visitor to use current visitor span for the task itself.
    ///   Such tasks MUST create similar task-like spans for each underlying node themselves.
    ///
    /// If `skip_reason` is provided, the default implementation applies the same shared skip
    /// mapping used on span skip-close callbacks so `NodeEvaluated` starts with a skipped
    /// outcome already set.
    ///
    /// # Panics
    /// - If task reports zero nodes but doesn't override this method.
    /// - If task reports has no phase but doesn't override this method.
    fn telemetry_request(
        &self,
        in_dir: &Path,
        out_dir: &Path,
        skip_reason: Option<&SkipReason>,
    ) -> SpanTreeRequest<FsResult<NodeStatus>, SkipReason> {
        let mut nodes = self.dbt_nodes().into_iter();

        let Some(node) = nodes.next() else {
            unreachable!("Task should always operate on at least a single node");
        };

        if nodes.next().is_some() {
            // task for multiple nodes use current span as task span.
            // Task inner code should create actual spans for themselves.
            return SpanTreeRequest::use_current();
        }

        let Some(task_phase) = self.task_phase() else {
            unreachable!(
                "Task that doesn't belong to a phase should override telemetry_tree method"
            );
        };

        let phase = task_phase.into();
        let mut attrs = node.get_node_evaluated_event(phase, in_dir, out_dir);
        if let Some(skip_reason) = skip_reason {
            update_node_outcome_from_skip_reason(&mut attrs, skip_reason);
        }

        let task_span_level = match phase {
            ExecutionPhase::Run | ExecutionPhase::Compare => SpanLevel::Info,
            _ => SpanLevel::Debug,
        };
        let mut builder = SpanTreeRequest::builder(
            task_span_level,
            attrs.into(),
            Some(Box::new(task_span_on_close)),
            Some(Box::new(task_span_on_skip)),
        );

        // Reference Phase span as direct parent (must be pre-registered)
        builder.with_parent(SpanLevel::Info, phase_span_key(phase));

        // Reference NodeProcessed span as direct related span (must be pre-registered)
        // Span manager will automatically create its parent (TuiAllProcessingNodesGroup)
        let node_key = node_processed_span_key(&node.unique_id());
        builder.add_related_span(SpanLevel::Info, &node_key);

        builder.build()
    }
}

pub struct TasksForNode {
    pub renderable: Option<Arc<dyn Task>>,
    pub analyzeable: Option<Arc<dyn Task>>,
    pub runnable: Option<Arc<dyn Task>>,
    pub showable: Option<Arc<dyn Task>>,
}

/// Get the tasks for a node based on the phases and execute type
pub trait TasksForNodeFactory: Send + Sync {
    fn tasks_for_node(
        &self,
        unique_id: &str,
        nodes: &Nodes,
        schedule: &Schedule<String>,
        execute: Execute,
        phases: &[TP],
        generic_test_aggregation: Option<&GenericTestAggregation>,
        unit_test_overrides: Option<&UnitTestOverrides>,
        reverse_deps: &HashMap<String, HashSet<String>>,
    ) -> TasksForNode;

    fn create_render_task_hooks(&self) -> Arc<dyn RenderTaskHooks>;

    fn create_run_task_hooks(&self) -> Arc<dyn RunTaskHooks>;
}
