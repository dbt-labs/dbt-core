use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use dbt_adapter::Adapter;
use dbt_common::FsError;
use dbt_common::FsResult;
use dbt_common::cancellation::CancellationToken;
use dbt_common::create_info_span;
use dbt_common::io_args::FsCommand;
use dbt_common::tracing::span_info::{SpanStatusRecorder, record_span_status_with_attrs};
use dbt_dag::schedule::Schedule;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_scheduler::schedule::summarize_stats;
use dbt_schema_store::DataStoreTrait;
use dbt_schema_store::SchemaStoreTrait;
use dbt_schema_store::store::SchemaStore;
use dbt_schemas::schemas::profiles::Execute;
use dbt_schemas::state::ResolverState;
use dbt_schemas::stats::Stats;
use dbt_tasks_core::RunTaskResults;
use dbt_tasks_core::RunTasksArgs;
use dbt_tasks_core::TaskRunnerStats;
use dbt_tasks_core::context::TaskRunnerCtx;
use dbt_tasks_core::context_factory::TaskRunnerCtxFactory;
use dbt_tasks_core::static_analysis_buckets::StaticAnalysisBuckets;
use dbt_tasks_core::task::Task;
use dbt_tasks_core::task_runner_hooks::TaskRunnerHooks;
use dbt_tasks_core::test_aggregation::GenericTestRelationships;
use dbt_tasks_core::{CompiledSqlCache, PreTaskRunData};
use dbt_telemetry::{ExecutionPhase, PhaseExecuted};
use dbt_telemetry::{HookOutcome, HookProcessed, HookType};

use petgraph::Graph;
use tracing::Instrument;
use tracing::instrument;

use crate::register_seeds;
use crate::run_operation::run_operation_on_run_with_ctx;
use crate::utils::filter_missing_schemas;
use crate::utils::get_catalog_schemas_and_ids;
use crate::utils::register_catalog_schemas_remote;
use crate::visitor::visit_parallel;
use crate::visitor::visit_sequential;

pub fn summarize_task_runner_stats(
    ctx: &TaskRunnerCtx,
    schedule: &Schedule<String>,
    resolved_state: &ResolverState,
) -> TaskRunnerStats {
    let compile = Stats {
        stats: summarize_stats(schedule, &ctx.inner.analyze_stats),
        nodes: None,
        batch_results: Default::default(),
    };
    let batch_results = ctx
        .inner
        .batch_results_map
        .iter()
        .map(|entry| (entry.key().clone(), entry.value().clone()))
        .collect();
    let run = Stats {
        stats: summarize_stats(schedule, &ctx.inner.run_stats),
        nodes: Some(resolved_state.nodes.clone()),
        batch_results,
    };
    TaskRunnerStats { compile, run }
}

pub struct TaskRunner {
    hooks: Box<dyn TaskRunnerHooks>,
    adapter: Arc<Adapter>,
    pub resolved_state: Arc<ResolverState>,
    jinja_env: Arc<JinjaEnv>,
    schema_store: Arc<SchemaStore>,
    data_store: Arc<dyn DataStoreTrait>,
    compiled_sql_cache: Arc<dyn CompiledSqlCache>,
    ctx_factory: Arc<dyn TaskRunnerCtxFactory>,
    static_analysis_buckets: Arc<dyn StaticAnalysisBuckets>,
}

impl TaskRunner {
    pub fn new(
        hooks: Box<dyn TaskRunnerHooks>,
        adapter: Arc<Adapter>,
        resolved_state: Arc<ResolverState>,
        jinja_env: Arc<JinjaEnv>,
        schema_store: Arc<SchemaStore>,
        data_store: Arc<dyn DataStoreTrait>,
        compiled_sql_cache: Arc<dyn CompiledSqlCache>,
        ctx_factory: Arc<dyn TaskRunnerCtxFactory>,
        static_analysis_buckets: Arc<dyn StaticAnalysisBuckets>,
    ) -> Self {
        Self {
            hooks,
            adapter,
            resolved_state,
            jinja_env,
            schema_store,
            data_store,
            compiled_sql_cache,
            ctx_factory,
            static_analysis_buckets,
        }
    }

    pub fn into_empty_results(self) -> FsResult<RunTaskResults> {
        Ok(RunTaskResults {
            stats: TaskRunnerStats {
                compile: Stats::default(),
                run: Stats::default(),
            },
            storeables: Vec::new(),
            showables: Vec::new(),
            jinja_env: self.jinja_env,
            resolved_state: self.resolved_state,
            task_runner_ctx: None,
            preview: None,
        })
    }

    pub async fn register_seeds_for_selected_ids(
        &self,
        run_task_args: &RunTasksArgs,
        schedule: &Schedule<String>,
    ) -> FsResult<()> {
        let adapter_type = self.resolved_state.dbt_profile.db_config.adapter_type();
        // Pre-register only *selected* seeds (not frontier dependencies) so that
        // frontier seeds don't mask "missing in remote" static analysis errors.
        let selected_seed_ids: Vec<&String> = schedule
            .sorted_nodes
            .iter()
            .filter(|uid| {
                self.resolved_state.nodes.seeds.contains_key(*uid)
                    && schedule.selected_nodes.contains(*uid)
            })
            .collect();
        register_seeds::pre_register_seeds(
            &selected_seed_ids,
            &self.resolved_state.nodes.seeds,
            adapter_type,
            Arc::clone(&self.schema_store) as Arc<dyn SchemaStoreTrait>,
            Arc::clone(&self.data_store),
            Arc::clone(self.adapter.engine().type_ops()),
            &run_task_args.io.in_dir,
        )
        .await;

        Ok(())
    }

    /// Print the task graph for debugging
    ///
    /// Make sure to gate this behind a show option since this function will eagerly
    /// calculate and emit an event which can be expensive for large graphs.
    pub fn show_taskgraph(&self, graph: &Graph<Arc<dyn Task>, ()>) {
        self.hooks.show_taskgraph(graph);
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_context(
        &self,
        run_task_args: Arc<RunTasksArgs>,
        generic_test_relationships: GenericTestRelationships,
        graph: &Graph<Arc<dyn Task>, ()>,
        base_context: BTreeMap<String, minijinja::Value>,
        schedule: Schedule<String>,
        freshness_results: Option<Box<dyn PreTaskRunData>>,
    ) -> Result<TaskRunnerCtx, Box<FsError>> {
        let extended_ctx_factory = self
            .hooks
            .create_extended_ctx_factory(&run_task_args)
            .await?;
        let invocation_id = run_task_args.io.invocation_id.to_string();
        Arc::clone(&self.ctx_factory)
            .build(
                run_task_args,
                invocation_id,
                Arc::clone(&self.resolved_state),
                extended_ctx_factory,
                generic_test_relationships,
                graph,
                Arc::clone(&self.schema_store) as Arc<dyn SchemaStoreTrait>,
                Arc::clone(&self.data_store),
                Arc::clone(&self.compiled_sql_cache),
                base_context,
                schedule,
                Arc::clone(&self.jinja_env),
                freshness_results,
                Arc::clone(&self.static_analysis_buckets),
                Arc::clone(&self.adapter),
            )
            .await
    }

    fn should_register_schemas(&self, run_task_args: &RunTasksArgs) -> bool {
        let execute = Execute::from_compute_flag(run_task_args.local_execution_backend);
        (run_task_args.is_runnable() && execute == Execute::Remote)
            || run_task_args.command == FsCommand::Clone
    }

    async fn register_schemas(
        &self,
        run_task_args: &RunTasksArgs,
        schedule: &Schedule<String>,
        base_context: BTreeMap<String, minijinja::Value>,
    ) -> FsResult<()> {
        let state = self.jinja_env.new_state_with_context(base_context);

        let selected_catalog_schemas =
            get_catalog_schemas_and_ids(&self.resolved_state.nodes, schedule);

        let catalog_schemas_to_register =
            filter_missing_schemas(&self.adapter, &state, &selected_catalog_schemas)?;

        register_catalog_schemas_remote(
            &run_task_args.io,
            &self.adapter,
            &state,
            catalog_schemas_to_register,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument(name = "run_tasks_with_listener", skip_all, level = "trace")]
    pub async fn run(
        mut self,
        run_task_args: Arc<RunTasksArgs>,
        schedule: Schedule<String>,
        base_context: BTreeMap<String, minijinja::Value>,
        mut ctx: TaskRunnerCtx,
        graph: Graph<Arc<dyn Task>, ()>,
        has_dynamic_closure: bool,
        token: CancellationToken,
    ) -> Result<RunTaskResults, Box<FsError>> {
        self.hooks.will_run(&run_task_args, &schedule);

        let registered_schemas = if self.should_register_schemas(run_task_args.as_ref()) {
            self.register_schemas(run_task_args.as_ref(), &schedule, base_context)
                .await?;
            true
        } else {
            false
        };

        self.hooks
            .did_register_schemas(registered_schemas, &run_task_args, &schedule, &mut ctx)
            .await?;

        let mut on_run_start_sqls = Vec::new();

        // Create span for on-run-start phase if there are any hooks
        let on_run_start_span = if !self.resolved_state.operations.on_run_start.is_empty() {
            Some(create_info_span(PhaseExecuted::start_with_node_count(
                ExecutionPhase::OnRunStart,
                self.resolved_state.operations.on_run_start.len() as u64,
            )))
        } else {
            None
        };

        // Execute all on-run-start hooks and record status on phase span
        if let Some(ref span) = on_run_start_span {
            let result: FsResult<()> = async {
                for (idx, operation) in self
                    .resolved_state
                    .operations
                    .on_run_start
                    .iter()
                    .enumerate()
                {
                    // Create span for individual hook with HookProcessed event
                    let hook_span = create_info_span(HookProcessed::start_on_run(
                        operation.__common_attr__.package_name.as_str(),
                        operation.__common_attr__.name.as_str(),
                        HookType::OnRunStart,
                        idx as u32,
                        operation.__common_attr__.unique_id.as_str(),
                    ));

                    let result =
                        run_operation_on_run_with_ctx(operation, &ctx, &None, &None, &None)
                            .instrument(hook_span.clone())
                            .await;

                    let (hook_outcome, error_message) = match &result {
                        Ok(rendered_sql) => {
                            on_run_start_sqls.push(rendered_sql.clone());
                            (HookOutcome::Success, None)
                        }
                        Err(e) => (HookOutcome::Error, Some(e.message().to_string())),
                    };

                    record_span_status_with_attrs(
                        &hook_span,
                        |attrs| {
                            if let Some(hook_attrs) = attrs.downcast_mut::<HookProcessed>() {
                                hook_attrs.set_hook_outcome(hook_outcome);
                            }
                        },
                        error_message.as_deref(),
                    );

                    result?;
                }
                Ok(())
            }
            .instrument(span.clone())
            .await
            .record_status(span);

            result?;
        }

        // Explicitly drop the on-run-start span so it's closed before model execution
        drop(on_run_start_span);

        self.hooks
            .will_visit_taskgraph(
                &run_task_args,
                &schedule,
                has_dynamic_closure,
                &on_run_start_sqls,
                &graph,
                &mut ctx,
                &token,
            )
            .await?;
        if run_task_args.no_parallel {
            visit_sequential(&run_task_args.io, &graph, &mut ctx, &token)
                .in_current_span()
                .await?;
        } else {
            visit_parallel(&run_task_args.io, &graph, &mut ctx, &token)
                .in_current_span()
                .await?;
        }
        self.hooks
            .did_visit_taskgraph(&run_task_args, &schedule, &graph, &mut ctx, &token)
            .await?;

        let stats = summarize_task_runner_stats(&ctx, &schedule, self.resolved_state.as_ref());
        let results = stats.collect_as_results();
        let successful_relational_nodes =
            stats.collect_successful_relational_nodes(&self.resolved_state);

        let schemas: Vec<String> = successful_relational_nodes
            .iter()
            .map(|(_, schema)| schema.clone())
            .collect::<HashSet<_>>() // Deduplicate
            .into_iter()
            .collect();

        let database_schemas: Vec<(String, String)> =
            successful_relational_nodes.into_iter().collect();

        let schemas_option = Some(schemas.clone());
        let database_schemas_option = Some(database_schemas);
        let results_option = Some(results.clone());

        let execute = Execute::from_compute_flag(run_task_args.local_execution_backend);
        if execute == Execute::Remote && !run_task_args.skip_post_hooks {
            // Create span for on-run-end phase if there are any hooks
            let on_run_end_span = if !self.resolved_state.operations.on_run_end.is_empty() {
                Some(create_info_span(PhaseExecuted::start_with_node_count(
                    ExecutionPhase::OnRunEnd,
                    self.resolved_state.operations.on_run_end.len() as u64,
                )))
            } else {
                None
            };

            // Execute all on-run-end hooks and record status on phase span
            if let Some(ref span) = on_run_end_span {
                let result: FsResult<()> = async {
                    for (idx, operation) in
                        self.resolved_state.operations.on_run_end.iter().enumerate()
                    {
                        // Create span for individual hook with HookProcessed event
                        let hook_span = create_info_span(HookProcessed::start_on_run(
                            operation.__common_attr__.package_name.as_str(),
                            operation.__common_attr__.name.as_str(),
                            HookType::OnRunEnd,
                            (idx + 1) as u32,
                            operation.__common_attr__.unique_id.as_str(),
                        ));

                        let result = run_operation_on_run_with_ctx(
                            operation,
                            &ctx,
                            &schemas_option,
                            &database_schemas_option,
                            &results_option,
                        )
                        .instrument(hook_span.clone())
                        .await;

                        let (hook_outcome, error_message) = match &result {
                            Ok(_) => (HookOutcome::Success, None),
                            Err(e) => (HookOutcome::Error, Some(e.message().to_string())),
                        };

                        record_span_status_with_attrs(
                            &hook_span,
                            |attrs| {
                                if let Some(hook_attrs) = attrs.downcast_mut::<HookProcessed>() {
                                    hook_attrs.set_hook_outcome(hook_outcome);
                                }
                            },
                            error_message.as_deref(),
                        );

                        result?;
                    }
                    Ok(())
                }
                .instrument(span.clone())
                .await
                .record_status(span);

                result?;
            }
        }

        let showables = self.hooks.collect_showables(&mut ctx);
        let preview = self.hooks.collect_preview(&mut ctx);
        let storeables = self.hooks.collect_storeables(&run_task_args, &mut ctx);

        self.hooks
            .did_collect_all_run_task_results(&run_task_args, &mut ctx)
            .await;

        Ok(RunTaskResults {
            stats,
            storeables,
            showables,
            jinja_env: self.jinja_env,
            resolved_state: self.resolved_state,
            task_runner_ctx: Some(ctx),
            preview,
        })
    }
}
