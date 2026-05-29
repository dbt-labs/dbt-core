//! dbt State service shadow path for remote task execution.
//!
//! This module translates task-layer state into service requests before a node
//! executes, interprets the service decision, and confirms successful execution
//! back to the service when a request id was returned. The integration is
//! deliberately fail-open: unsupported nodes, missing metadata, service errors,
//! and confirmation failures all fall back to normal execution so service
//! availability does not change command success.
//!
//! The module owns task-specific concerns such as rendered SQL extraction,
//! adapter relation rendering, warehouse metadata lookups, and skip policy.
//! Stable service DTO construction lives in `dbt-run-cache::request_builder`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use crate::context::TaskRunnerCtx;
use crate::task::{TaskOp, TaskResult};
use dbt_adapter::AdapterResult;
use dbt_adapter::errors::{Cancellable, into_fs_error};
use dbt_adapter::metadata::{FreshnessOverride, MetadataQueryOptions};
use dbt_adapter::relation::{create_relation, create_relation_from_node};
use dbt_adapter::sql_types::TypeOps;
use dbt_adapter_core::AdapterType;
use dbt_common::adapter::dialect_of;
use dbt_common::cancellation::never_cancels;
use dbt_common::io_args::RunCacheMode;
use dbt_common::stats::NodeStatus;
use dbt_common::tracing::emit::{emit_trace_log_message, emit_warn_log_message};
use dbt_common::{ErrorCode, FsResult, fs_err};
use dbt_frontend_common::Dialect;
use dbt_frontend_common::ident::FullyQualifiedName;
use dbt_run_cache::node_session::ExecutionGuard;
use dbt_run_cache::proto::query_cache::{
    ConfirmExecutionRequest, ExplainedDecision, NodeFuncMapping, QueryDependency,
    RecordExecutionsRequest, SkipExecutionResponse, Struct, SubmitEnrichedSqlRequest,
    SubmitSqlResponse, SubmitValuesRequest, TableModifiedInfo, submit_sql_response,
};
use dbt_run_cache::request_builder::{
    ExecutionOutcomeInput, sql_execution_record_from_submit_request,
    values_execution_record_from_submit_request,
};
use dbt_schemas::schemas::common::{DbtMaterialization, ModelFreshnessRules, ResolvedQuoting};
use dbt_schemas::schemas::profiles::DbConfig;
use dbt_schemas::schemas::properties::ModelState;
use dbt_schemas::schemas::relations::base::BaseRelation;
use dbt_schemas::schemas::{
    DbtModel, DbtSeed, DbtSnapshot, DbtSource, DbtTest, InternalDbtNode, InternalDbtNodeAttributes,
};
use dbt_xdbc::QueryCtx;

use crate::run_cache::run_cache_request::{
    SeedRunCacheRequestContext, SqlRunCacheRequestContext, build_model_sql_request,
    build_seed_values_request, build_snapshot_sql_request, build_test_sql_request, node_identity,
};

pub fn collect_upstream_hashes(ctx: &TaskRunnerCtx, unique_id: &str) -> HashMap<String, String> {
    ctx.inner
        .schedule
        .deps
        .get(unique_id)
        .map(|deps| {
            deps.iter()
                .filter_map(|dep| {
                    ctx.inner
                        .node_hashes
                        .get(dep)
                        .map(|hash| (dep.clone(), hash.value().clone()))
                })
                .collect()
        })
        .unwrap_or_default()
}

pub enum RunCacheServiceDecision {
    Execute {
        after_success: RunCacheAfterSuccess,
        sao_guard: Option<ExecutionGuard>,
    },
    Clone {
        clone: RunCacheCloneDecision,
    },
    Skip {
        status: NodeStatus,
        sao_stored_hash: Option<String>,
        /// Cached test verdict (failures count) when the skipped node is a
        /// data test. The dispatcher in `runnable/mod.rs` uses it to replace
        /// the generic `ReusedNoChanges` status with a test-shaped status
        /// (TestPassed/Errored) and a NO-OP-marked stat.
        cached_test_failures: Option<i64>,
    },
    Disabled,
}

impl RunCacheServiceDecision {
    fn execute_without_confirmation() -> Self {
        Self::Execute {
            after_success: RunCacheAfterSuccess::None,
            sao_guard: None,
        }
    }

    fn execute_with_confirmation(request_id: String, failed_to_clone: bool) -> Self {
        Self::Execute {
            after_success: RunCacheExecutionConfirmation::new(request_id, failed_to_clone)
                .map(RunCacheAfterSuccess::Confirm)
                .unwrap_or(RunCacheAfterSuccess::None),
            sao_guard: None,
        }
    }

    fn execute_with_record(record: RunCachePendingExecutionRecord) -> Self {
        Self::Execute {
            after_success: RunCacheAfterSuccess::Record(Box::new(record)),
            sao_guard: None,
        }
    }

    /// The authoritative SAO node hash, if this decision carries one.
    ///
    /// `Skip` carries the stored hash from a prior successful run; `Execute`
    /// with an `sao_guard` carries the hash the guard will write on
    /// finalize. Service-only outcomes (no guard / no stored hash) return
    /// `None` because the service is the source of truth in that mode.
    pub fn node_hash(&self) -> Option<String> {
        match self {
            Self::Skip {
                sao_stored_hash, ..
            } => sao_stored_hash.clone(),
            Self::Execute {
                sao_guard: Some(guard),
                ..
            } => Some(guard.node_hash().to_string()),
            _ => None,
        }
    }

    pub async fn finalize(self, ctx: &TaskRunnerCtx) -> FsResult<()> {
        match self {
            RunCacheServiceDecision::Execute {
                sao_guard: Some(guard),
                ..
            } => {
                let upstreams = collect_upstream_hashes(ctx, guard.unique_id());
                guard
                    .finalize(upstreams)
                    .await
                    .map_err(|e| fs_err!(ErrorCode::Generic, "stop_task failed: {}", e))
            }
            _ => Ok(()),
        }
    }
}

impl PartialEq for RunCacheServiceDecision {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                RunCacheServiceDecision::Execute {
                    after_success: a1, ..
                },
                RunCacheServiceDecision::Execute {
                    after_success: a2, ..
                },
            ) => a1 == a2,
            (
                RunCacheServiceDecision::Clone { clone: c1 },
                RunCacheServiceDecision::Clone { clone: c2 },
            ) => c1 == c2,
            (
                RunCacheServiceDecision::Skip {
                    status: s1,
                    sao_stored_hash: h1,
                    cached_test_failures: f1,
                },
                RunCacheServiceDecision::Skip {
                    status: s2,
                    sao_stored_hash: h2,
                    cached_test_failures: f2,
                },
            ) => s1 == s2 && h1 == h2 && f1 == f2,
            (RunCacheServiceDecision::Disabled, RunCacheServiceDecision::Disabled) => true,
            _ => false,
        }
    }
}

impl std::fmt::Debug for RunCacheServiceDecision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunCacheServiceDecision::Execute { after_success, .. } => f
                .debug_struct("RunCacheServiceDecision::Execute")
                .field("after_success", after_success)
                .finish(),
            RunCacheServiceDecision::Clone { clone } => f
                .debug_struct("RunCacheServiceDecision::Clone")
                .field("clone", clone)
                .finish(),
            RunCacheServiceDecision::Skip {
                status,
                sao_stored_hash,
                cached_test_failures,
            } => f
                .debug_struct("RunCacheServiceDecision::Skip")
                .field("status", status)
                .field("sao_stored_hash", sao_stored_hash)
                .field("cached_test_failures", cached_test_failures)
                .finish(),
            RunCacheServiceDecision::Disabled => write!(f, "RunCacheServiceDecision::Disabled"),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum RunCacheAfterSuccess {
    None,
    Confirm(RunCacheExecutionConfirmation),
    Record(Box<RunCachePendingExecutionRecord>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct RunCacheExecutionConfirmation {
    request_id: String,
    failed_to_clone: bool,
    execution_results: Option<Struct>,
    execution_runtime_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RunCachePendingExecutionRecord {
    input: RunCachePendingExecutionInput,
}

#[derive(Clone, Debug, PartialEq)]
enum RunCachePendingExecutionInput {
    Sql(SubmitEnrichedSqlRequest),
    Values(SubmitValuesRequest),
}

impl RunCacheExecutionConfirmation {
    fn new(request_id: String, failed_to_clone: bool) -> Option<Self> {
        if request_id.is_empty() {
            None
        } else {
            Some(Self {
                request_id,
                failed_to_clone,
                execution_results: None,
                execution_runtime_ms: None,
            })
        }
    }

    fn with_execution_metadata(
        request_id: String,
        failed_to_clone: bool,
        execution_results: Option<Struct>,
        execution_runtime_ms: Option<i64>,
    ) -> Option<Self> {
        Self::new(request_id, failed_to_clone).map(|mut confirmation| {
            confirmation.execution_results = execution_results;
            confirmation.execution_runtime_ms = execution_runtime_ms;
            confirmation
        })
    }

    /// Attach a `{failures, should_warn, should_error}` payload to this
    /// confirmation. Used by the data-test Confirm path so subsequent runs
    /// can replay the cached verdict via `parse_cached_test_skip`.
    pub fn set_test_execution_results(&mut self, failures: i64) {
        if self.execution_results.is_none() {
            self.execution_results = Some(build_test_execution_results_struct(failures));
        }
    }
}

/// Build the `{failures, should_warn, should_error}` payload Fusion sends
/// in `ConfirmExecutionRequest.execution_results` so subsequent runs can
/// replay the cached verdict. Mirrors dbt-core's
/// `_DataTestAdapterProxy.execute`, which lower-cases the agate row before
/// confirming. `should_warn` and `should_error` both come from
/// `count(*) != 0` in the templated test SQL, so they are simply
/// `failures > 0`.
pub fn build_test_execution_results_struct(failures: i64) -> Struct {
    use dbt_run_cache::proto::query_cache::{Value, value::Kind};
    let fail_value = Value {
        kind: Some(Kind::IntValue(failures)),
    };
    let bool_value = Value {
        kind: Some(Kind::BoolValue(failures > 0)),
    };
    let mut fields = HashMap::new();
    fields.insert("failures".to_string(), fail_value);
    fields.insert("should_warn".to_string(), bool_value.clone());
    fields.insert("should_error".to_string(), bool_value);
    Struct { fields }
}

/// Decode the cached `{failures, ...}` payload from a
/// `SkipExecutionResponse`. Returns the failures count if the field is
/// present and decodable.
pub fn parse_cached_test_failures(response: &SkipExecutionResponse) -> Option<i64> {
    use dbt_run_cache::proto::query_cache::value::Kind;
    let results = response.execution_results.as_ref()?;
    let value = results.fields.get("failures")?;
    match value.kind.as_ref()? {
        Kind::IntValue(i) => Some(*i),
        Kind::DoubleValue(d) => Some(*d as i64),
        _ => None,
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RunCacheCloneDecision {
    request_id: String,
    clone_sqls: Vec<String>,
    clone_source: String,
    clone_target: String,
    required_source_epoch: Option<i64>,
    execution_results: Option<Struct>,
    execution_runtime_ms: Option<i64>,
    freshness_tolerance_seconds: u64,
    explained_decision: Option<ExplainedDecision>,
    transformed_nodes_by_query: HashMap<String, NodeFuncMapping>,
    execution_decision_id: Option<String>,
}

impl RunCacheCloneDecision {
    pub fn from_response(
        response: &dbt_run_cache::proto::query_cache::ReadyToCloneResponse,
        freshness_tolerance_seconds: i64,
    ) -> Self {
        Self {
            request_id: response.request_id.clone(),
            clone_sqls: response.clone_sqls.clone(),
            clone_source: response.clone_source.clone(),
            clone_target: response.clone_target.clone(),
            required_source_epoch: response.clone_required_last_modified_epoch,
            execution_results: response.clone_execution_results.clone(),
            execution_runtime_ms: response.execution_runtime_ms,
            freshness_tolerance_seconds: freshness_tolerance_seconds.max(0) as u64,
            explained_decision: response.explained_decision,
            transformed_nodes_by_query: response.transformed_nodes_by_query.clone(),
            execution_decision_id: response.execution_decision_id.clone(),
        }
    }

    pub fn success_confirmation(&self) -> Option<RunCacheExecutionConfirmation> {
        RunCacheExecutionConfirmation::with_execution_metadata(
            self.request_id.clone(),
            false,
            self.execution_results.clone(),
            self.execution_runtime_ms,
        )
    }

    pub fn fallback_confirmation(&self) -> Option<RunCacheExecutionConfirmation> {
        RunCacheExecutionConfirmation::new(self.request_id.clone(), true)
    }

    fn success_status(&self) -> NodeStatus {
        if self
            .explained_decision
            .as_ref()
            .is_some_and(|decision| decision.is_stale)
        {
            NodeStatus::ReusedCloned(Some(self.freshness_tolerance_seconds))
        } else {
            NodeStatus::ReusedCloned(None)
        }
    }
}

/// If the node is a selected (non-deferred) view model, insert its
/// compiled SQL into the run-scoped traverser cache so downstream
/// models do not remotely fetch the view's DDL.
///
/// Deferred view models are not inserted — they are resolved via the
/// remote fetch performed by [`run_cache_service_before_run`].
/// Non-view materializations are no-ops (their compiled SQL is not
/// view DDL).
pub fn insert_compiled_view_definition(
    ctx: &TaskRunnerCtx,
    node: &dyn InternalDbtNodeAttributes,
    task_result: &TaskResult,
) {
    if !ctx.inner.run_cache_ctx.run_cache_service_requested {
        return;
    }
    let Some(traverser) = ctx.inner.run_cache_ctx.view_traverser.as_ref() else {
        return;
    };
    let Some(model) = node.as_any().downcast_ref::<DbtModel>() else {
        return;
    };
    if model.materialized() != DbtMaterialization::View {
        return;
    }
    let compiled_sql = task_result.sql_instruction.sql.as_str();
    if compiled_sql.is_empty() {
        return;
    }

    let adapter_type = ctx.adapter_type();
    let Ok(relation) = create_relation_from_node(adapter_type, node, None) else {
        return;
    };

    // Deferred nodes are resolved remotely by the start-of-run traversal.
    // Fail closed on canonical-fqn errors: without a cfqn we cannot rule
    // out that this node is deferred, and inserting a deferred view's
    // local compiled SQL would shadow the production definition for
    // downstream lookups keyed by `semantic_fqn`.
    let Ok(cfqn) = relation.get_canonical_fqn() else {
        emit_warn_log_message(
            ErrorCode::RunCacheServiceWarn,
            format!(
                "Skipping compiled view definition insert for node {}: canonical FQN unavailable; cannot determine deferral status",
                node.unique_id()
            ),
            None,
        );
        return;
    };
    if ctx
        .inner
        .run_cache_ctx
        .run_cache_deferred_fqns
        .contains(&cfqn.to_string())
    {
        return;
    }

    let Some(dialect) = dialect_of(adapter_type) else {
        return;
    };
    // Derive default_catalog / default_schema by parsing the relation's
    // canonical FQN, mirroring what `fetch_view_definitions` does on the
    // adapter side. `node.database()` / `node.schema()` come from the
    // user's profile and may preserve lowercase, which on Snowflake then
    // produces quoted-lowercase synthetic relations downstream that don't
    // resolve in the warehouse — see `test_transitive_dependencies_tracked`.
    let fqn = relation.semantic_fqn();
    let (default_catalog, default_schema) = match dialect.parse_fqn(&fqn) {
        Ok(parsed) => (
            parsed.catalog().name().to_string(),
            parsed.schema().name().to_string(),
        ),
        Err(_) => (node.database(), node.schema()),
    };
    traverser.insert_view_definition(dbt_adapter::metadata::ViewDefinition {
        fqn,
        definition: compiled_sql.to_string(),
        dialect,
        default_catalog,
        default_schema,
    });
}

/// Start-of-run traversal hook. Fires once before any model executes.
///
/// Collects all relations referenced by selected nodes (excluding the
/// selected nodes themselves) and drives one `ViewDefinitionTraverser`
/// traversal, warming the shared cache for the rest of the run.
///
/// No-op when no `view_traverser` is available (e.g. parse-only mode
/// with no metadata adapter).
///
/// Returns the originating `AdapterError` on traversal failure; the
/// caller is responsible for surfacing it as a run-level failure.
pub async fn run_cache_service_before_run(ctx: &TaskRunnerCtx) -> AdapterResult<()> {
    let Some(traverser) = ctx.inner.run_cache_ctx.view_traverser.as_ref() else {
        return Ok(());
    };
    let roots = collect_view_traversal_roots(ctx);
    if roots.is_empty() {
        return Ok(());
    }
    let token = ctx
        .env
        .get_adapter_ref()
        .map(|a| a.cancellation_token())
        .unwrap_or_else(never_cancels);
    let _ = traverser.traverse(&roots, token).await?;
    Ok(())
}

/// Submits a runnable node to the dbt State service before local execution.
///
/// The returned decision tells the caller either to skip execution with a reused
/// status, or to execute normally with an optional confirmation token to report
/// the final warehouse state after a successful run. All service and request
/// assembly failures are fail-open and return `Execute { after_success: None }`.
pub async fn run_cache_service_before_execution(
    ctx: &TaskRunnerCtx,
    node: &dyn InternalDbtNodeAttributes,
    task_result: &TaskResult,
) -> RunCacheServiceDecision {
    if !ctx.inner.run_cache_ctx.run_cache_service_requested {
        emit_warn_log_message(
            ErrorCode::RunCacheServiceWarn,
            format!(
                "dbt State service hook reached while service mode is disabled for node {}; executing normally",
                node.unique_id()
            ),
            None,
        );
        return RunCacheServiceDecision::execute_without_confirmation();
    }

    let Some(client) = ctx.inner.run_cache_ctx.run_cache_service_client.as_ref() else {
        emit_warn_log_message(
            ErrorCode::RunCacheServiceWarn,
            format!(
                "dbt State service was requested but no validated client is available for node {}; executing normally",
                node.unique_id()
            ),
            None,
        );
        return RunCacheServiceDecision::execute_without_confirmation();
    };

    if !should_honor_service_skip(ctx) {
        let result = prepare_write_only_execution_record(ctx, node, task_result).await;
        return match result {
            Ok(Some(record)) => RunCacheServiceDecision::execute_with_record(record),
            Ok(None) => {
                let unique_id = node.unique_id();
                emit_trace_log_message(|| {
                    format!(
                        "dbt State service record skipped for node {unique_id}; executing normally"
                    )
                });
                RunCacheServiceDecision::execute_without_confirmation()
            }
            Err(err) => {
                emit_warn_log_message(
                    ErrorCode::RunCacheServiceWarn,
                    format!(
                        "dbt State service record preparation failed for node {}: {err}; executing normally",
                        node.unique_id()
                    ),
                    None,
                );
                RunCacheServiceDecision::execute_without_confirmation()
            }
        };
    }

    let result = if let Some(model) = node.as_any().downcast_ref::<DbtModel>() {
        if is_no_op_model_materialization(model.materialized()) {
            let unique_id = node.unique_id();
            let materialization = model.materialized().to_string();
            emit_trace_log_message(|| {
                format!(
                    "dbt State service submit skipped for no-op model materialization (node {unique_id}, materialization {materialization})"
                )
            });
            return RunCacheServiceDecision::execute_without_confirmation();
        }
        if model.common().language.as_deref() != Some("sql") {
            record_unsupported_node(node, "non-SQL model");
            return RunCacheServiceDecision::execute_without_confirmation();
        }
        submit_model(ctx, model, task_result, client).await
    } else if let Some(snapshot) = node.as_any().downcast_ref::<DbtSnapshot>() {
        submit_snapshot(ctx, snapshot, task_result, client).await
    } else if let Some(seed) = node.as_any().downcast_ref::<DbtSeed>() {
        submit_seed(ctx, seed, client).await
    } else if let Some(test) = node.as_any().downcast_ref::<DbtTest>() {
        submit_test(ctx, test, task_result, client).await
    } else {
        record_unsupported_node(node, "unsupported node type");
        return RunCacheServiceDecision::execute_without_confirmation();
    };

    match result {
        Ok(Some(outcome)) => {
            let decision = record_service_decision(
                node.unique_id().as_str(),
                &outcome.response,
                outcome.freshness_tolerance_seconds,
                should_honor_service_skip(ctx),
            );
            // A node that completed a dev clone earlier in this invocation
            // should surface as "Cloned from cached relation" when the
            // service decides Skip, matching the dbt-core plugin's
            // `_dev_cloned_nodes` mapping.
            let is_dev_cloned = ctx
                .inner
                .run_cache_ctx
                .run_cache_dev_cloned_nodes
                .contains_key(node.unique_id().as_str());
            relabel_skip_for_dev_cloned_node(is_dev_cloned, decision)
        }
        Ok(None) => {
            let unique_id = node.unique_id();
            emit_trace_log_message(|| {
                format!("dbt State service submit skipped for node {unique_id}; executing normally")
            });
            RunCacheServiceDecision::execute_without_confirmation()
        }
        Err(err) => {
            emit_warn_log_message(
                ErrorCode::RunCacheServiceWarn,
                format!(
                    "dbt State service submit failed for node {}: {err}; executing normally",
                    node.unique_id()
                ),
                None,
            );
            RunCacheServiceDecision::execute_without_confirmation()
        }
    }
}

/// Confirms a successful local execution back to the dbt State service.
///
/// Confirmation is best-effort: if no confirmation token was returned by the
/// pre-execution submit, final metadata is unavailable, or the service RPC
/// fails, the dbt command remains successful and the failure is only logged.
pub async fn confirm_run_cache_service_execution(
    ctx: &TaskRunnerCtx,
    node: &dyn InternalDbtNodeAttributes,
    confirmation: Option<RunCacheExecutionConfirmation>,
    execution_runtime_ms: Option<i64>,
) {
    let is_test = node.unique_id().starts_with("test.");
    // Data tests submit with `target_table=None`. The service's DB CHECK
    // `execution_last_modified_epoch_target_table_check` requires
    // `last_modified_epoch=NULL` whenever `target_table=NULL`, so never send
    // the audit relation's epoch on confirm — including when it does exist
    // (the `store_failures_as=table/view` case). Skipping the warehouse
    // lookup also avoids unnecessary work for `store_failures_as=None`
    // where there is no audit relation to query.
    let final_last_modified_epoch = if is_test {
        None
    } else {
        match refresh_final_last_modified_epoch_for_node(ctx, node).await {
            Ok(epoch) => epoch,
            Err(err) => {
                emit_warn_log_message(
                    ErrorCode::RunCacheServiceWarn,
                    format!(
                        "dbt State service final metadata lookup failed for node {}: {err}; command remains successful",
                        node.unique_id()
                    ),
                    None,
                );
                return;
            }
        }
    };

    let Some(confirmation) = confirmation else {
        return;
    };

    let Some(client) = ctx.inner.run_cache_ctx.run_cache_service_client.as_ref() else {
        emit_warn_log_message(
            ErrorCode::RunCacheServiceWarn,
            format!(
                "dbt State service confirmation skipped because no validated client is available (node {}, request_id {})",
                node.unique_id(),
                confirmation.request_id
            ),
            None,
        );
        return;
    };

    let request = match confirmation
        .into_confirm_execution_request(ctx, node, final_last_modified_epoch, execution_runtime_ms)
        .await
    {
        Ok(Some(request)) => request,
        Ok(None) => return,
        Err(err) => {
            emit_warn_log_message(
                ErrorCode::RunCacheServiceWarn,
                format!(
                    "dbt State service confirmation metadata lookup failed for node {}: {err}; command remains successful",
                    node.unique_id()
                ),
                None,
            );
            return;
        }
    };

    let request_id = request.request_id.clone();
    if let Err(err) = client.confirm_execution(request).await {
        emit_warn_log_message(
            ErrorCode::RunCacheServiceWarn,
            format!(
                "dbt State service confirmation failed for node {} (request_id {request_id}): {err}; command remains successful",
                node.unique_id()
            ),
            None,
        );
    } else {
        let unique_id = node.unique_id();
        emit_trace_log_message(|| {
            format!(
                "dbt State service execution confirmed for node {unique_id} (request_id {request_id})"
            )
        });
    }
}

/// Records a successful local execution directly through the dbt State service.
///
/// Recording is best-effort and only used by write-only mode, where dbt State
/// lookup must be bypassed entirely. Missing final metadata or RPC failures are
/// logged and do not change dbt command success.
pub async fn record_run_cache_service_execution(
    ctx: &TaskRunnerCtx,
    node: &dyn InternalDbtNodeAttributes,
    record: Option<RunCachePendingExecutionRecord>,
    execution_runtime_ms: Option<i64>,
) {
    let Some(record) = record else {
        return;
    };

    let Some(client) = ctx.inner.run_cache_ctx.run_cache_service_client.as_ref() else {
        emit_warn_log_message(
            ErrorCode::RunCacheServiceWarn,
            format!(
                "dbt State service record skipped for node {} because no validated client is available",
                node.unique_id()
            ),
            None,
        );
        return;
    };

    let request = match record
        .into_record_executions_request(ctx, node, execution_runtime_ms)
        .await
    {
        Ok(Some(request)) => request,
        Ok(None) => return,
        Err(err) => {
            emit_warn_log_message(
                ErrorCode::RunCacheServiceWarn,
                format!(
                    "dbt State service record metadata lookup failed for node {}: {err}; command remains successful",
                    node.unique_id()
                ),
                None,
            );
            return;
        }
    };

    if let Err(err) = client.record_executions(request).await {
        emit_warn_log_message(
            ErrorCode::RunCacheServiceWarn,
            format!(
                "dbt State service record failed for node {}: {err}; command remains successful",
                node.unique_id()
            ),
            None,
        );
    } else {
        let unique_id = node.unique_id();
        emit_trace_log_message(|| {
            format!("dbt State service execution recorded for node {unique_id}")
        });
    }
}

pub async fn execute_run_cache_service_clone(
    ctx: &TaskRunnerCtx,
    node: &dyn InternalDbtNodeAttributes,
    clone: &RunCacheCloneDecision,
    adapter_type: AdapterType,
    max_threads: Option<usize>,
) -> FsResult<NodeStatus> {
    verify_clone_source_freshness(ctx, node, clone).await?;
    if clone.clone_sqls.is_empty() {
        return Err(fs_err!(
            ErrorCode::Generic,
            "dbt State service clone response did not include clone SQL"
        ));
    }

    // TODO: Honor model_config.state.execute_hooks_on_reuse once Fusion has a
    // narrow node-level hook lifecycle for service no-op/clone paths.
    // Materialization hooks are currently embedded in normal node execution
    // macros.
    let clone_sqls = clone.clone_sqls.clone();
    let node_unique_id = node.unique_id();
    let ctx_inner = ctx.clone();
    TaskOp::BlockingWithConnection {
        f: Box::new(move || execute_clone_sqls_blocking(&ctx_inner, &node_unique_id, &clone_sqls)),
        adapter_type,
        max_threads,
    }
    .run()
    .await??;

    let target_relation = create_relation_from_node(ctx.adapter_type(), node, None)?;
    let target_relation: Arc<dyn BaseRelation> = target_relation.into();
    ctx.inner
        .run_cache_ctx
        .run_cache_metadata
        .invalidate_relation_metadata(&target_relation.semantic_fqn());
    cache_cloned_relation(ctx, node)?;
    Ok(clone.success_status())
}

struct RunCacheSubmitOutcome {
    response: SubmitSqlResponse,
    /// Freshness tolerance window (seconds) that Fusion sent with the request.
    /// Used to format the "Did not meet build_after of …" message when the
    /// service admits a candidate despite a stale upstream. Echoing the local
    /// value avoids a proto round-trip — the service already evaluates against
    /// the same number.
    freshness_tolerance_seconds: i64,
}

impl RunCacheExecutionConfirmation {
    async fn into_confirm_execution_request(
        self,
        ctx: &TaskRunnerCtx,
        node: &dyn InternalDbtNodeAttributes,
        last_modified_epoch: Option<i64>,
        execution_runtime_ms: Option<i64>,
    ) -> FsResult<Option<ConfirmExecutionRequest>> {
        // Data tests submit with `target_table=None` and the caller always
        // passes `last_modified_epoch=None` for them (the service's DB CHECK
        // `execution_last_modified_epoch_target_table_check` requires
        // `last_modified_epoch=NULL` whenever `target_table=NULL`), so let
        // test confirms through with `None` rather than skipping them — the
        // service still needs to record the execution to serve future Skips.
        let is_test = node.unique_id().starts_with("test.");
        if last_modified_epoch.is_none() && !is_test {
            let unique_id = node.unique_id();
            let request_id = self.request_id.clone();
            emit_trace_log_message(|| {
                format!(
                    "dbt State service confirmation skipped because final last-modified metadata is unavailable (node {unique_id}, request_id {request_id})"
                )
            });
            return Ok(None);
        }

        Ok(Some(ConfirmExecutionRequest {
            request_id: self.request_id,
            last_modified_epoch,
            failed_to_clone: self.failed_to_clone,
            table_type: table_type_for_node(ctx, node).await?,
            execution_results: self.execution_results,
            execution_runtime_ms: self.execution_runtime_ms.or(execution_runtime_ms),
            labels: node_identity(node).labels(),
        }))
    }
}

impl RunCachePendingExecutionRecord {
    fn sql(request: SubmitEnrichedSqlRequest) -> Self {
        Self {
            input: RunCachePendingExecutionInput::Sql(request),
        }
    }

    fn values(request: SubmitValuesRequest) -> Self {
        Self {
            input: RunCachePendingExecutionInput::Values(request),
        }
    }

    async fn into_record_executions_request(
        self,
        ctx: &TaskRunnerCtx,
        node: &dyn InternalDbtNodeAttributes,
        execution_runtime_ms: Option<i64>,
    ) -> FsResult<Option<RecordExecutionsRequest>> {
        let last_modified_epoch = refresh_final_last_modified_epoch_for_node(ctx, node).await?;
        let Some(last_modified_epoch) = last_modified_epoch else {
            let unique_id = node.unique_id();
            emit_trace_log_message(|| {
                format!(
                    "dbt State service record skipped for node {unique_id} because final last-modified metadata is unavailable"
                )
            });
            return Ok(None);
        };

        let outcome = ExecutionOutcomeInput {
            last_modified_epoch: Some(last_modified_epoch),
            table_type: table_type_for_node(ctx, node).await?,
            execution_runtime_ms,
        };
        let record = match self.input {
            RunCachePendingExecutionInput::Sql(request) => {
                sql_execution_record_from_submit_request(request, outcome)
            }
            RunCachePendingExecutionInput::Values(request) => {
                values_execution_record_from_submit_request(request, outcome)
            }
        };

        Ok(Some(RecordExecutionsRequest {
            records: vec![record],
        }))
    }
}

async fn verify_clone_source_freshness(
    ctx: &TaskRunnerCtx,
    node: &dyn InternalDbtNodeAttributes,
    clone: &RunCacheCloneDecision,
) -> FsResult<()> {
    let Some(required_epoch) = clone.required_source_epoch else {
        return Ok(());
    };
    if clone.clone_source.is_empty() {
        return Err(fs_err!(
            ErrorCode::Generic,
            "dbt State service clone response requires source freshness verification but clone_source is empty"
        ));
    }

    let source_relation = relation_from_rendered_name(ctx, node, &clone.clone_source)?;
    let actual_epoch =
        refresh_last_modified_epoch_for_relation(ctx, &clone.clone_source, source_relation).await?;
    match actual_epoch {
        Some(actual_epoch) if actual_epoch == required_epoch => Ok(()),
        Some(actual_epoch) => Err(fs_err!(
            ErrorCode::Generic,
            "dbt State service clone source freshness mismatch for {}: required {}, found {}",
            clone.clone_source,
            required_epoch,
            actual_epoch
        )),
        None => Err(fs_err!(
            ErrorCode::Generic,
            "dbt State service clone source freshness unavailable for {}",
            clone.clone_source
        )),
    }
}

fn relation_from_rendered_name(
    ctx: &TaskRunnerCtx,
    node: &dyn InternalDbtNodeAttributes,
    rendered_name: &str,
) -> FsResult<Arc<dyn BaseRelation>> {
    let Some(dialect) = dialect_of(ctx.adapter_type()) else {
        return Err(fs_err!(
            ErrorCode::Generic,
            "dbt State service clone source parsing is unsupported for adapter {:?}",
            ctx.adapter_type()
        ));
    };
    let fqn = FullyQualifiedName::parse(rendered_name, dialect).map_err(|err| {
        fs_err!(
            ErrorCode::Generic,
            "Failed to parse dbt State clone relation {}: {}",
            rendered_name,
            err
        )
    })?;
    Ok(create_relation(
        ctx.adapter_type(),
        fqn.catalog().to_value(),
        fqn.schema().to_value(),
        Some(fqn.table().to_value()),
        None,
        node.quoting(),
    )?
    .into())
}

fn execute_clone_sqls_blocking(
    ctx: &TaskRunnerCtx,
    node_unique_id: &str,
    clone_sqls: &[String],
) -> FsResult<()> {
    let Some(adapter) = ctx.env.get_adapter_ref() else {
        return Err(fs_err!(
            ErrorCode::Generic,
            "dbt State service clone cannot execute because no adapter is available"
        ));
    };
    let query_ctx = QueryCtx::default()
        .with_node_id(node_unique_id.to_string())
        .with_desc("dbt State clone");
    for sql in clone_sqls {
        adapter
            .execute_without_state(Some(&query_ctx), sql, false)
            .map_err(|err| into_fs_error(Cancellable::Error(err)))?;
    }
    Ok(())
}

fn cache_cloned_relation(
    ctx: &TaskRunnerCtx,
    node: &dyn InternalDbtNodeAttributes,
) -> FsResult<()> {
    if let Some(base_adapter) = ctx.env.get_base_adapter() {
        let relation = create_relation_from_node(ctx.adapter_type(), node, None)?;
        let _ = base_adapter.cache_added(&ctx.env.empty_state(), relation.into());
    }
    Ok(())
}

async fn submit_model(
    ctx: &TaskRunnerCtx,
    model: &DbtModel,
    task_result: &TaskResult,
    client: &dbt_run_cache::service_client::SharedRunCacheServiceClient,
) -> FsResult<Option<RunCacheSubmitOutcome>> {
    let full_refresh = effective_full_refresh(
        ctx.inner.arg.full_refresh,
        model.deprecated_config.full_refresh,
    );
    if full_refresh && full_refresh_blocks_model_submit(model.materialized()) {
        record_submit_skipped(model, "full refresh");
        return Ok(None);
    }

    let context = build_sql_context(
        ctx,
        model,
        task_result.sql_instruction.sql.clone(),
        model.materialized() == DbtMaterialization::View,
        full_refresh,
    )
    .await?;
    if !context.metadata_complete {
        record_submit_skipped(model, "missing metadata");
        return Ok(None);
    }
    let freshness_tolerance_seconds = context.request.freshness_tolerance_seconds;
    let request = build_model_sql_request(model, context.request)?;

    let response = client.submit_enriched_sql(request).await.map_err(|e| {
        fs_err!(
            ErrorCode::Generic,
            "dbt State service SubmitEnrichedSQL failed: {}",
            e
        )
    })?;
    Ok(Some(RunCacheSubmitOutcome {
        response,
        freshness_tolerance_seconds,
    }))
}

/// Builds the dbt State record input for write-only mode without contacting the
/// service.
///
/// Write-only must never ask the service for a dbt State decision. This prepares
/// the SQL or seed payload before execution, then the caller records it through
/// `RecordExecutions` only after the node succeeds and final metadata is
/// available.
async fn prepare_write_only_execution_record(
    ctx: &TaskRunnerCtx,
    node: &dyn InternalDbtNodeAttributes,
    task_result: &TaskResult,
) -> FsResult<Option<RunCachePendingExecutionRecord>> {
    if let Some(model) = node.as_any().downcast_ref::<DbtModel>() {
        if is_no_op_model_materialization(model.materialized()) {
            record_submit_skipped(model, "no-op model materialization");
            return Ok(None);
        }
        if model.common().language.as_deref() != Some("sql") {
            record_unsupported_node(node, "non-SQL model");
            return Ok(None);
        }

        let full_refresh = effective_full_refresh(
            ctx.inner.arg.full_refresh,
            model.deprecated_config.full_refresh,
        );
        let mut context = build_sql_context(
            ctx,
            model,
            task_result.sql_instruction.sql.clone(),
            model.materialized() == DbtMaterialization::View,
            full_refresh,
        )
        .await?;
        if !context.metadata_complete {
            record_submit_skipped(model, "missing metadata");
            return Ok(None);
        }
        remove_cache_decision_fields(&mut context.request);
        Ok(Some(RunCachePendingExecutionRecord::sql(
            build_model_sql_request(model, context.request)?,
        )))
    } else if let Some(snapshot) = node.as_any().downcast_ref::<DbtSnapshot>() {
        let mut context = build_sql_context(
            ctx,
            snapshot,
            task_result.sql_instruction.sql.clone(),
            false,
            effective_full_refresh(
                ctx.inner.arg.full_refresh,
                snapshot.deprecated_config.full_refresh,
            ),
        )
        .await?;
        if !context.metadata_complete {
            record_submit_skipped(snapshot, "missing metadata");
            return Ok(None);
        }
        remove_cache_decision_fields(&mut context.request);
        Ok(Some(RunCachePendingExecutionRecord::sql(
            build_snapshot_sql_request(snapshot, context.request)?,
        )))
    } else if let Some(seed) = node.as_any().downcast_ref::<DbtSeed>() {
        let request = build_seed_values_request(
            seed,
            SeedRunCacheRequestContext {
                adapter_type: ctx.adapter_type(),
                dialect: run_cache_dialect(ctx),
                project_root: ctx.inner.arg.io.in_dir.as_path(),
                last_modified_epoch: None,
                clone_time_travel_limit: None,
                clone_table_properties: None,
            },
        )?;
        Ok(Some(RunCachePendingExecutionRecord::values(request)))
    } else {
        record_unsupported_node(node, "unsupported node type");
        Ok(None)
    }
}

async fn submit_snapshot(
    ctx: &TaskRunnerCtx,
    snapshot: &DbtSnapshot,
    task_result: &TaskResult,
    client: &dbt_run_cache::service_client::SharedRunCacheServiceClient,
) -> FsResult<Option<RunCacheSubmitOutcome>> {
    let context = build_sql_context(
        ctx,
        snapshot,
        task_result.sql_instruction.sql.clone(),
        false,
        effective_full_refresh(
            ctx.inner.arg.full_refresh,
            snapshot.deprecated_config.full_refresh,
        ),
    )
    .await?;
    if !context.metadata_complete {
        record_submit_skipped(snapshot, "missing metadata");
        return Ok(None);
    }
    let freshness_tolerance_seconds = context.request.freshness_tolerance_seconds;
    let request = build_snapshot_sql_request(snapshot, context.request)?;

    let response = client.submit_enriched_sql(request).await.map_err(|e| {
        fs_err!(
            ErrorCode::Generic,
            "dbt State service SubmitEnrichedSQL failed: {}",
            e
        )
    })?;
    Ok(Some(RunCacheSubmitOutcome {
        response,
        freshness_tolerance_seconds,
    }))
}

async fn submit_seed(
    ctx: &TaskRunnerCtx,
    seed: &DbtSeed,
    client: &dbt_run_cache::service_client::SharedRunCacheServiceClient,
) -> FsResult<Option<RunCacheSubmitOutcome>> {
    if effective_full_refresh(
        ctx.inner.arg.full_refresh,
        seed.deprecated_config.full_refresh,
    ) {
        record_submit_skipped(seed, "full refresh");
        return Ok(None);
    }

    // Mirrors dbt-core's `_build_submit_values_request`: always submit, even
    // when the target table doesn't exist yet. The service treats a None
    // `last_modified_epoch` as `target_table_exists=false` and returns
    // ReadyToExecute, then ConfirmExecution registers the run after the seed
    // materializes. Bailing out on first run would delay registration by a
    // build and break the two-run dbt State cycle the dbt-core plugin implements.
    let last_modified_epoch = last_modified_epoch_for_node(ctx, seed).await?;
    let clone_time_travel_limit = ctx
        .inner
        .run_cache_ctx
        .run_cache_service_config
        .as_ref()
        .and_then(|config| config.clone_time_travel_limit_seconds);
    let request = build_seed_values_request(
        seed,
        SeedRunCacheRequestContext {
            adapter_type: ctx.adapter_type(),
            dialect: run_cache_dialect(ctx),
            project_root: ctx.inner.arg.io.in_dir.as_path(),
            last_modified_epoch,
            clone_time_travel_limit,
            clone_table_properties: None,
        },
    )?;

    let response = client.submit_values(request).await.map_err(|e| {
        fs_err!(
            ErrorCode::Generic,
            "dbt State service SubmitValues failed: {}",
            e
        )
    })?;
    Ok(Some(RunCacheSubmitOutcome {
        response,
        freshness_tolerance_seconds: 0,
    }))
}

/// Mirrors dbt-core's `_DataTestAdapterProxy._on_data_test_query`: submit a
/// data test's count(*) SQL with `execution_type=DbtDataTest`. The cached
/// `{failures, should_warn, should_error}` payload flows back through
/// `SkipExecutionResponse.execution_results` and is decoded by
/// `parse_cached_test_failures`. On `ReadyToExecute`, the dispatcher confirms
/// after the test runs (see `set_test_execution_results`).
async fn submit_test(
    ctx: &TaskRunnerCtx,
    test: &DbtTest,
    task_result: &TaskResult,
    client: &dbt_run_cache::service_client::SharedRunCacheServiceClient,
) -> FsResult<Option<RunCacheSubmitOutcome>> {
    let context = build_sql_context(
        ctx,
        test,
        task_result.sql_instruction.sql.clone(),
        false, // tests aren't views
        false, // full_refresh is meaningless for tests
    )
    .await?;
    if !context.metadata_complete {
        record_submit_skipped(test, "missing metadata");
        return Ok(None);
    }
    let freshness_tolerance_seconds = context.request.freshness_tolerance_seconds;
    let request = build_test_sql_request(test, context.request)?;

    let response = client.submit_enriched_sql(request).await.map_err(|e| {
        fs_err!(
            ErrorCode::Generic,
            "dbt State service SubmitEnrichedSQL failed: {}",
            e
        )
    })?;
    Ok(Some(RunCacheSubmitOutcome {
        response,
        freshness_tolerance_seconds,
    }))
}

struct BuiltSqlRunCacheContext {
    request: SqlRunCacheRequestContext,
    metadata_complete: bool,
}

async fn build_sql_context(
    ctx: &TaskRunnerCtx,
    node: &dyn InternalDbtNodeAttributes,
    sql: String,
    is_view: bool,
    full_refresh: bool,
) -> FsResult<BuiltSqlRunCacheContext> {
    let Some(config) = ctx.inner.run_cache_ctx.run_cache_service_config.as_ref() else {
        return Err(fs_err!(
            ErrorCode::Generic,
            "dbt State service config is unavailable"
        ));
    };

    let query_dependencies = collect_query_dependencies(ctx, node, &sql, is_view).await?;
    let tables = collect_table_modified_infos(
        ctx,
        node,
        is_view,
        &query_dependencies.seen_tables,
        &query_dependencies.parser_seen_relations,
    )
    .await?;
    let metadata_complete = tables.metadata_complete && query_dependencies.metadata_complete;
    let lenient_dependencies = build_lenient_dependencies(
        config.enable_lenient_dependencies,
        &ctx.inner.run_cache_ctx.run_cache_deferred_fqns,
        &tables.tables,
        &query_dependencies.dependencies,
    );

    let stale_upstream_policy = stale_upstream_policy_for_node(node);

    Ok(BuiltSqlRunCacheContext {
        request: SqlRunCacheRequestContext {
            adapter_type: ctx.adapter_type(),
            dialect: run_cache_dialect(ctx),
            sql,
            tables: tables.tables,
            query_dependencies: query_dependencies.dependencies,
            freshness_tolerance_seconds: if is_view {
                0
            } else {
                freshness_tolerance_seconds_for_node(node, config.freshness_tolerance_seconds)
            },
            lenient_dependencies,
            tolerate_nondeterminism: resolve_tolerate_nondeterminism(
                node,
                config.tolerate_nondeterminism,
            ),
            full_refresh,
            clone_time_travel_limit: config.clone_time_travel_limit_seconds,
            clone_table_properties: None,
            stale_upstream_policy,
        },
        metadata_complete,
    })
}

fn model_state_for_node(node: &dyn InternalDbtNodeAttributes) -> Option<&ModelState> {
    node.as_any()
        .downcast_ref::<DbtModel>()
        .and_then(|model| model.__model_attr__.state.as_ref())
}

fn freshness_tolerance_seconds_for_node(
    node: &dyn InternalDbtNodeAttributes,
    service_default: i64,
) -> i64 {
    model_state_for_node(node)
        .and_then(|state| state.lag_tolerance.as_ref())
        .and_then(freshness_rule_to_seconds)
        .unwrap_or(service_default)
}

fn freshness_rule_to_seconds(rule: &ModelFreshnessRules) -> Option<i64> {
    (rule.count.is_some() && rule.period.is_some()).then(|| rule.to_seconds())
}

/// Per-node override for the dbt State service's `tolerate_nondeterminism`
/// wire flag. The aligned `state.evaluate_volatile_sql` config takes
/// precedence. The legacy `meta["run_cache_tolerate_nondeterminism"]` form is
/// retained as a fallback for compatibility.
fn resolve_tolerate_nondeterminism(
    node: &dyn InternalDbtNodeAttributes,
    service_default: bool,
) -> bool {
    if let Some(evaluate_volatile_sql) =
        model_state_for_node(node).and_then(|state| state.evaluate_volatile_sql)
    {
        return evaluate_volatile_sql;
    }

    const KEY: &str = "run_cache_tolerate_nondeterminism";
    let Some(value) = node.meta().get(KEY).cloned() else {
        return service_default;
    };
    if let Some(b) = value.as_bool() {
        return b;
    }
    if let Some(i) = value.as_i64() {
        return i != 0;
    }
    if let Some(s) = value.as_str() {
        match s.trim().to_ascii_lowercase().as_str() {
            "true" | "yes" | "1" | "on" => return true,
            "false" | "no" | "0" | "off" => return false,
            _ => {}
        }
    }
    emit_warn_log_message(
        ErrorCode::RunCacheServiceWarn,
        format!(
            "Ignoring meta.{KEY} on node {}: value is not a bool, int, or recognized string",
            node.unique_id()
        ),
        None,
    );
    service_default
}

/// Translate the node's freshness policy into the dbt State service's wire
/// enum. The aligned `state.require_fresh_data_from` config takes precedence.
/// Legacy `freshness.build_after.updates_on` remains as a fallback for
/// compatibility. ANY = every upstream must be within tolerance; ALL = at
/// least one upstream must be within tolerance.
fn stale_upstream_policy_for_node(
    node: &dyn InternalDbtNodeAttributes,
) -> dbt_run_cache::proto::query_cache::StaleUpstreamPolicy {
    use dbt_run_cache::proto::query_cache::StaleUpstreamPolicy;
    use dbt_schemas::schemas::common::UpdatesOn;

    let updates_on = model_state_for_node(node)
        .and_then(|state| state.require_fresh_data_from.as_ref())
        .or_else(|| {
            node.as_any()
                .downcast_ref::<DbtModel>()
                .and_then(|model| model.__model_attr__.freshness.as_ref())
                .and_then(|freshness| freshness.build_after.as_ref())
                .and_then(|build_after| build_after.updates_on.as_ref())
        });

    match updates_on {
        Some(UpdatesOn::All) => StaleUpstreamPolicy::All,
        Some(UpdatesOn::Any) | None => StaleUpstreamPolicy::Any,
    }
}

fn metadata_query_options_for_warehouses(
    profile_warehouse: Option<String>,
    legacy_service_warehouse: Option<String>,
) -> MetadataQueryOptions {
    MetadataQueryOptions {
        warehouse: profile_warehouse.or(legacy_service_warehouse),
    }
}

pub(crate) fn run_cache_metadata_query_options(ctx: &TaskRunnerCtx) -> MetadataQueryOptions {
    let profile_warehouse = match &ctx.dbt_profile().db_config {
        DbConfig::Snowflake(config) => config.metadata_warehouse.clone(),
        _ => None,
    };
    let legacy_service_warehouse = ctx
        .inner
        .run_cache_ctx
        .run_cache_service_config
        .as_ref()
        .and_then(|config| config.snowflake_metadata_warehouse.clone());

    metadata_query_options_for_warehouses(profile_warehouse, legacy_service_warehouse)
}

/// Returns deferred dependencies that should be treated leniently by the dbt
/// State service for this specific request.
///
/// Auto-deferral can rewrite unselected upstreams to the configured `defer_to`
/// target. When those upstreams appear in the submitted table freshness or view
/// query-dependency metadata, marking them lenient tells the service they were
/// intentionally deferred. The result is limited to dependencies present in the
/// request so unrelated deferred nodes do not affect the dbt State decision.
fn build_lenient_dependencies(
    enable_lenient_dependencies: bool,
    deferred_fqns: &BTreeSet<String>,
    tables: &[TableModifiedInfo],
    query_dependencies: &[QueryDependency],
) -> Vec<String> {
    if !enable_lenient_dependencies {
        return Vec::new();
    }

    let request_dependencies = tables
        .iter()
        .map(|table| table.name.as_str())
        .chain(
            query_dependencies
                .iter()
                .map(|dependency| dependency.name.as_str()),
        )
        .collect::<BTreeSet<_>>();

    deferred_fqns
        .iter()
        .filter(|fqn| request_dependencies.contains(fqn.as_str()))
        .cloned()
        .collect()
}

struct CollectedTableModifiedInfos {
    tables: Vec<TableModifiedInfo>,
    metadata_complete: bool,
}

struct CollectedViewQueryDependencies {
    dependencies: Vec<QueryDependency>,
    /// Leaf-table closure produced by the view traversal. Empty when the
    /// model has no parseable upstream refs in its compiled SQL or its
    /// upstreams are all views. Failure paths use `incomplete()`, which
    /// trips `metadata_complete = false` and skips the cache submit
    /// entirely — so this field always reflects a real traversal result.
    seen_tables: BTreeSet<String>,
    /// Upstream relations to backfill into `collect_table_modified_infos`'s
    /// relation map. Two sources, both keyed by `semantic_fqn()` (the same
    /// canonical scheme the DAG-deps loop in `collect_table_modified_infos`
    /// uses):
    ///   1. The SQL parser's view of the model's own compiled SQL — picks up
    ///      raw `from <schema>.<table>` references with no `ref()`/`source()`
    ///      that have no DAG edge but were syntactically observed.
    ///   2. View-traversal leaves — the non-view base tables reached by
    ///      recursing through upstream view DDL. Without these,
    ///      `last_modified_epoch` for a transitive base table is never sent,
    ///      and the service's freshness check defaults to "fresh" on the
    ///      NULL/NULL match path (see `test_transitive_dependencies_tracked`).
    parser_seen_relations: BTreeMap<String, Arc<dyn BaseRelation>>,
    metadata_complete: bool,
}

impl CollectedViewQueryDependencies {
    fn complete(
        dependencies: Vec<QueryDependency>,
        seen_tables: BTreeSet<String>,
        parser_seen_relations: BTreeMap<String, Arc<dyn BaseRelation>>,
    ) -> Self {
        Self {
            dependencies,
            seen_tables,
            parser_seen_relations,
            metadata_complete: true,
        }
    }

    fn incomplete() -> Self {
        Self {
            dependencies: Vec::new(),
            seen_tables: BTreeSet::new(),
            parser_seen_relations: BTreeMap::new(),
            metadata_complete: false,
        }
    }

    /// Empty, completed result used for views.
    ///
    /// Views are re-evaluated on every read, so the dbt State service only
    /// checks the view's own `last_modified_epoch` and SQL hash to decide
    /// reuse — upstream view DDL and base-table freshness are irrelevant.
    /// Mirrors the dbt-state Python plugin's view path
    /// (clients/dbt_state/src/dbt_state/run_cache.py:1116-1146), which sends
    /// `query_dependencies=[]`. `metadata_complete` must stay `true` so the
    /// submit isn't silently skipped.
    fn for_view() -> Self {
        Self::complete(Vec::new(), BTreeSet::new(), BTreeMap::new())
    }
}

async fn collect_table_modified_infos(
    ctx: &TaskRunnerCtx,
    node: &dyn InternalDbtNodeAttributes,
    target_only: bool,
    leaf_tables: &BTreeSet<String>,
    parser_seen_relations: &BTreeMap<String, Arc<dyn BaseRelation>>,
) -> FsResult<CollectedTableModifiedInfos> {
    let mut relations = BTreeMap::new();
    let mut metadata_complete = true;

    let (target_name, target_relation) = relation_for_node(ctx, node)?;
    relations.insert(target_name.clone(), target_relation);

    let mut freshness_overrides: BTreeMap<String, FreshnessOverride> = BTreeMap::new();

    if !target_only && let Some(deps) = ctx.inner.runtime_deps.get(node.unique_id().as_str()) {
        for dep_id in deps {
            let Some(dep_node) = ctx.nodes().get_node(dep_id) else {
                continue;
            };
            if dep_node.as_any().is::<DbtModel>()
                || dep_node.as_any().is::<DbtSnapshot>()
                || dep_node.as_any().is::<DbtSeed>()
                || dep_id.starts_with("source.")
            {
                if let Ok((name, relation)) = relation_for_node(ctx, dep_node) {
                    // For sources with `loaded_at_query` or `loaded_at_field` set,
                    // build an override entry keyed by the relation's semantic_fqn —
                    // that's what MetadataAdapter::freshness_with_overrides expects.
                    // Empty strings come from the deserializer for absent values, so
                    // treat empty as "not set" (matches the dbt-core plugin guard).
                    if let Some(source) = dep_node.as_any().downcast_ref::<DbtSource>() {
                        // Query takes precedence over field (matches dbt-core plugin),
                        // so only look at field when no query is set.
                        let trimmed_nonempty = |s: &str| {
                            let t = s.trim();
                            (!t.is_empty()).then(|| t.to_string())
                        };
                        let override_kind = source
                            .__source_attr__
                            .loaded_at_query
                            .as_deref()
                            .and_then(trimmed_nonempty)
                            .map(FreshnessOverride::Query)
                            .or_else(|| {
                                source
                                    .__source_attr__
                                    .loaded_at_field
                                    .as_deref()
                                    .and_then(trimmed_nonempty)
                                    .map(FreshnessOverride::Field)
                            });
                        if let Some(kind) = override_kind {
                            freshness_overrides.insert(relation.semantic_fqn(), kind);
                        }
                    }
                    relations.insert(name, relation);
                } else {
                    metadata_complete = false;
                }
            }
        }
    }

    // Backfill any tables the SQL parser saw but the DAG didn't declare
    // (raw `from <schema>.<table>` references with no `ref()`/`source()`).
    // Without this, those upstreams' `last_modified_epoch` never reaches the
    // dbt State service, so the service can't detect drift and `is_stale` is
    // always false. Mirrors dbt-core's plugin, which sources `tables`
    // directly from sqlglot's `find_tables` rather than the manifest DAG.
    if !target_only {
        for (fqn, relation) in parser_seen_relations {
            relations
                .entry(fqn.clone())
                .or_insert_with(|| Arc::clone(relation));
        }
    }

    // Only leaf tables (plus the target) go into the request's `tables`
    // field. Upstream views are published via `query_dependencies`; if we
    // also emitted them here, the dbt State server would prefer the table
    // entry's stored `semantic_hash` over recursing into the view's DDL,
    // hiding transitive DDL changes (see test_run_upstream_view_model_changes).
    let leaf_table_relations: BTreeMap<String, Arc<dyn BaseRelation>> = relations
        .iter()
        .filter(|(fqn, rel)| {
            leaf_tables.contains(rel.semantic_fqn().as_str()) || *fqn == &target_name
        })
        .map(|(fqn, rel)| (fqn.clone(), Arc::clone(rel)))
        .collect();
    prefetch_last_modified_epochs(ctx, &leaf_table_relations, &freshness_overrides).await;

    let mut table_infos = Vec::new();
    for (name, relation) in leaf_table_relations {
        if let Some(last_modified_epoch) =
            last_modified_epoch_for_relation(ctx, &name, relation).await?
        {
            table_infos.push(TableModifiedInfo {
                name,
                last_modified_epoch,
            });
        } else if name != target_name {
            metadata_complete = false;
        }
    }

    Ok(CollectedTableModifiedInfos {
        tables: table_infos,
        metadata_complete,
    })
}

async fn last_modified_epoch_for_node(
    ctx: &TaskRunnerCtx,
    node: &dyn InternalDbtNodeAttributes,
) -> FsResult<Option<i64>> {
    let (name, relation) = relation_for_node(ctx, node)?;
    last_modified_epoch_for_relation(ctx, &name, relation).await
}

pub async fn refresh_final_last_modified_epoch_for_node(
    ctx: &TaskRunnerCtx,
    node: &dyn InternalDbtNodeAttributes,
) -> FsResult<Option<i64>> {
    let (name, relation) = relation_for_node(ctx, node)?;
    ctx.inner
        .run_cache_ctx
        .run_cache_metadata
        .remove_last_modified_epoch(&name);
    refresh_last_modified_epoch_for_relation(ctx, &name, relation).await
}

async fn last_modified_epoch_for_relation(
    ctx: &TaskRunnerCtx,
    name: &str,
    relation: Arc<dyn BaseRelation>,
) -> FsResult<Option<i64>> {
    if let Some(epoch) = ctx
        .inner
        .run_cache_ctx
        .run_cache_metadata
        .last_modified_epoch(name)
    {
        return Ok(epoch);
    }

    refresh_last_modified_epoch_for_relation(ctx, name, relation).await
}

async fn refresh_last_modified_epoch_for_relation(
    ctx: &TaskRunnerCtx,
    name: &str,
    relation: Arc<dyn BaseRelation>,
) -> FsResult<Option<i64>> {
    let mut relations = BTreeMap::new();
    relations.insert(name.to_string(), relation);
    // Per-relation refreshes aren't used for sources (sources are populated
    // upfront via the bulk prefetch), so no overrides apply here.
    refresh_last_modified_epochs(ctx, &relations, &BTreeMap::new()).await?;
    Ok(ctx
        .inner
        .run_cache_ctx
        .run_cache_metadata
        .last_modified_epoch(name)
        .flatten())
}

async fn prefetch_last_modified_epochs(
    ctx: &TaskRunnerCtx,
    relations: &BTreeMap<String, Arc<dyn BaseRelation>>,
    overrides: &BTreeMap<String, FreshnessOverride>,
) {
    let misses = relations
        .iter()
        .filter(|(name, _)| {
            ctx.inner
                .run_cache_ctx
                .run_cache_metadata
                .last_modified_epoch(name)
                .is_none()
        })
        .map(|(name, relation)| (name.clone(), Arc::clone(relation)))
        .collect::<BTreeMap<_, _>>();
    if misses.is_empty() {
        return;
    }
    if let Err(err) = refresh_last_modified_epochs(ctx, &misses, overrides).await {
        emit_trace_log_message(|| format!("dbt State metadata prefetch failed: {err}"));
    }
}

async fn refresh_last_modified_epochs(
    ctx: &TaskRunnerCtx,
    relations: &BTreeMap<String, Arc<dyn BaseRelation>>,
    overrides: &BTreeMap<String, FreshnessOverride>,
) -> FsResult<()> {
    let Some(adapter) = ctx.env.get_adapter_ref() else {
        for name in relations.keys() {
            ctx.inner
                .run_cache_ctx
                .run_cache_metadata
                .insert_last_modified_epoch(name, None);
        }
        return Ok(());
    };
    let Some(metadata_adapter) = adapter.metadata_adapter() else {
        for name in relations.keys() {
            ctx.inner
                .run_cache_ctx
                .run_cache_metadata
                .insert_last_modified_epoch(name, None);
        }
        return Ok(());
    };

    for grouped_relations in group_relations_by_database_and_schema(relations).into_values() {
        let semantic_to_name = grouped_relations
            .iter()
            .map(|(name, relation)| (relation.semantic_fqn(), name.clone()))
            .collect::<BTreeMap<_, _>>();
        let relation_values = grouped_relations.values().cloned().collect::<Vec<_>>();
        let metadata_options = run_cache_metadata_query_options(ctx);
        let freshness = metadata_adapter
            .freshness_with_overrides_and_options(
                &relation_values,
                overrides,
                &metadata_options,
                adapter.cancellation_token(),
            )
            .await
            .map_err(into_fs_error)?;

        for (semantic_fqn, name) in semantic_to_name {
            let epoch = freshness
                .get(&semantic_fqn)
                .map(|metadata| metadata.last_altered.timestamp_millis());
            ctx.inner
                .run_cache_ctx
                .run_cache_metadata
                .insert_last_modified_epoch(name, epoch);
        }
    }
    Ok(())
}

fn group_relations_by_database_and_schema(
    relations: &BTreeMap<String, Arc<dyn BaseRelation>>,
) -> BTreeMap<(Option<String>, Option<String>), BTreeMap<String, Arc<dyn BaseRelation>>> {
    let mut grouped = BTreeMap::new();
    for (name, relation) in relations {
        grouped
            .entry((
                relation.database().map(str::to_string),
                relation.schema().map(str::to_string),
            ))
            .or_insert_with(BTreeMap::new)
            .insert(name.clone(), Arc::clone(relation));
    }
    grouped
}

/// Derives the SQL table-type keyword for a node (e.g. `"TRANSIENT TABLE"` /
/// `"TABLE"` / `"DYNAMIC TABLE"` on Snowflake) that downstream callers send
/// to the dbt State service for clone-SQL composition.
///
/// We derive this from the dbt node config, not from warehouse
/// introspection, mirroring the dbt-core plugin's
/// `get_relation_table_type` (see
/// `run-cache/clients/dbt_run_cache/src/dbt_run_cache/adapters/snowflake.py`).
/// Two reasons we follow that design:
///
///   * On Snowflake the warehouse query Fusion uses for bulk relation
///     listing (`SHOW OBJECTS`) does not expose the transient/permanent
///     bit at all — `kind` is `'TABLE'` for both, and there is no
///     `is_transient` column in the result set. Reaching for the bit via
///     `INFORMATION_SCHEMA.TABLES.table_type` is possible but adds
///     round-trips.
///   * The config IS the source of truth: dbt-snowflake's materialization
///     macros read `config.transient` to decide between
///     `CREATE [OR REPLACE] TRANSIENT TABLE` and `CREATE [OR REPLACE]
///     TABLE`. Going to the warehouse just round-trips the same value
///     through a lossy serializer.
///
/// Returns `None` for adapters that don't need this keyword (everyone
/// except Snowflake today) and for node kinds whose materialization isn't
/// table-like (views, ephemerals, tests, sources, ...). In those cases the
/// dbt State service falls back to its default of `TABLE`.
fn config_derived_table_type(
    node: &dyn InternalDbtNodeAttributes,
    adapter_type: AdapterType,
) -> Option<String> {
    if adapter_type != AdapterType::Snowflake {
        return None;
    }
    let (materialized, transient) = if let Some(model) = node.as_any().downcast_ref::<DbtModel>() {
        (
            model.base().materialized.clone(),
            model
                .deprecated_config
                .__warehouse_specific_config__
                .transient,
        )
    } else if let Some(snapshot) = node.as_any().downcast_ref::<DbtSnapshot>() {
        (
            snapshot.base().materialized.clone(),
            snapshot
                .deprecated_config
                .__warehouse_specific_config__
                .transient,
        )
    } else {
        return None;
    };
    match materialized {
        DbtMaterialization::DynamicTable => Some(
            if transient == Some(true) {
                "TRANSIENT DYNAMIC TABLE"
            } else {
                "DYNAMIC TABLE"
            }
            .to_string(),
        ),
        // dbt-snowflake defaults table/incremental/snapshot to TRANSIENT
        // unless the user explicitly opts out via `transient: false`.
        DbtMaterialization::Table
        | DbtMaterialization::Incremental
        | DbtMaterialization::Snapshot => Some(
            if transient.unwrap_or(true) {
                "TRANSIENT TABLE"
            } else {
                "TABLE"
            }
            .to_string(),
        ),
        _ => None,
    }
}

async fn table_type_for_node(
    ctx: &TaskRunnerCtx,
    node: &dyn InternalDbtNodeAttributes,
) -> FsResult<Option<String>> {
    Ok(config_derived_table_type(node, ctx.adapter_type()))
}

fn relation_for_node(
    ctx: &TaskRunnerCtx,
    node: &dyn InternalDbtNodeAttributes,
) -> FsResult<(String, Arc<dyn BaseRelation>)> {
    let relation = create_relation_from_node(ctx.adapter_type(), node, None)?;
    // Canonical (`semantic_fqn`) key so lenient-dependency matching, the
    // metadata cache, and the wire payload all agree regardless of how the
    // relation's database/schema/identifier were originally cased.
    let name = relation.semantic_fqn();
    Ok((name, relation.into()))
}

async fn collect_query_dependencies(
    ctx: &TaskRunnerCtx,
    node: &dyn InternalDbtNodeAttributes,
    sql: &str,
    is_view: bool,
) -> FsResult<CollectedViewQueryDependencies> {
    if is_view {
        return Ok(CollectedViewQueryDependencies::for_view());
    }
    let relations = parse_sql_relations_for_run_cache(
        ctx,
        sql,
        &node.database(),
        &node.schema(),
        node.base().quoting_ignore_case,
    )?;
    if relations.is_empty() {
        return Ok(CollectedViewQueryDependencies::complete(
            Vec::new(),
            BTreeSet::new(),
            BTreeMap::new(),
        ));
    }

    let Some(adapter) = ctx.env.get_adapter_ref() else {
        return Ok(CollectedViewQueryDependencies::incomplete());
    };

    let Some(traverser) = ctx.inner.run_cache_ctx.view_traverser.as_deref() else {
        return Ok(CollectedViewQueryDependencies::incomplete());
    };

    let relation_values = relations.values().cloned().collect::<Vec<_>>();
    let traversal = match traverser
        .traverse(&relation_values, adapter.cancellation_token())
        .await
    {
        Ok(traversal) => traversal,
        Err(err) => {
            let unique_id = node.unique_id();
            emit_trace_log_message(|| {
                format!(
                    "dbt State view dependency enrichment failed for node {unique_id}: {err}; continuing without query dependencies"
                )
            });
            return Ok(CollectedViewQueryDependencies::incomplete());
        }
    };

    let seen_tables = traversal.seen_tables;
    let dependencies = traversal
        .view_definitions
        .into_values()
        .map(|definition| QueryDependency {
            name: definition.fqn,
            query: definition.definition,
            default_catalog: definition.default_catalog,
            default_schema: definition.default_schema,
        })
        .collect();
    let mut parser_seen_relations = relations;
    for (fqn, leaf_relation) in traversal.leaf_relations {
        parser_seen_relations.entry(fqn).or_insert(leaf_relation);
    }
    Ok(CollectedViewQueryDependencies::complete(
        dependencies,
        seen_tables,
        parser_seen_relations,
    ))
}

fn parse_sql_relations_for_run_cache(
    ctx: &TaskRunnerCtx,
    sql: &str,
    default_catalog: &str,
    default_schema: &str,
    quoted_name_ignore_case: bool,
) -> FsResult<BTreeMap<String, Arc<dyn BaseRelation>>> {
    let Some(adapter) = ctx.env.get_adapter_ref() else {
        return Ok(BTreeMap::new());
    };
    let type_ops = adapter.engine().type_ops();
    parse_sql_relations_for_adapter(
        ctx.adapter_type(),
        sql,
        default_catalog,
        default_schema,
        quoted_name_ignore_case,
        type_ops.as_ref(),
    )
}

fn parse_sql_relations_for_adapter(
    adapter_type: AdapterType,
    sql: &str,
    default_catalog: &str,
    default_schema: &str,
    quoted_name_ignore_case: bool,
    type_ops: &dyn TypeOps,
) -> FsResult<BTreeMap<String, Arc<dyn BaseRelation>>> {
    let Some(dialect) = dialect_of(adapter_type) else {
        return Ok(BTreeMap::new());
    };

    let upstreams = type_ops.try_extract_identifiers(
        sql,
        default_catalog,
        default_schema,
        quoted_name_ignore_case,
    )?;

    let mut relations = BTreeMap::new();
    for upstream in upstreams.into_iter() {
        if upstream.table().as_str().starts_with('@') {
            continue;
        }
        let relation = create_relation(
            adapter_type,
            upstream.catalog().to_string(),
            upstream.schema().to_string(),
            Some(upstream.table().to_string()),
            None,
            quoting_for_upstream(dialect, &upstream, type_ops),
        )?;
        // Canonical key so a parser-seen relation collapses against the same
        // upstream surfaced via DAG dependencies (`relation_for_node`) and the
        // deferred-FQN set, regardless of the casing in the compiled SQL.
        let name = relation.semantic_fqn();
        relations.insert(name, relation.into());
    }

    Ok(relations)
}

fn quoting_for_upstream(
    _dialect: Dialect,
    upstream: &dbt_frontend_common::named_reference::NamedReference<FullyQualifiedName>,
    type_ops: &dyn TypeOps,
) -> ResolvedQuoting {
    ResolvedQuoting {
        database: type_ops.need_quotes_for_ident(upstream.catalog().as_str()),
        schema: type_ops.need_quotes_for_ident(upstream.schema().as_str()),
        identifier: type_ops.need_quotes_for_ident(upstream.table().as_str()) || upstream.is_prefix,
    }
}

fn run_cache_dialect(ctx: &TaskRunnerCtx) -> String {
    dialect_of(ctx.adapter_type())
        .map(|dialect| dialect.to_string())
        .unwrap_or_else(|| ctx.adapter_type().to_string())
}

fn should_honor_service_skip(ctx: &TaskRunnerCtx) -> bool {
    effective_run_cache_service_use_cache(
        &ctx.inner.arg.run_cache_mode,
        ctx.inner.run_cache_ctx.run_cache_service_requested,
    )
}

fn remove_cache_decision_fields(context: &mut SqlRunCacheRequestContext) {
    context.freshness_tolerance_seconds = 0;
    context.lenient_dependencies.clear();
    context.tolerate_nondeterminism = false;
    context.clone_time_travel_limit = None;
    context.clone_table_properties = None;
}

fn effective_full_refresh(cli_full_refresh: bool, config_full_refresh: Option<bool>) -> bool {
    config_full_refresh.unwrap_or(cli_full_refresh)
}

fn full_refresh_blocks_model_submit(materialization: DbtMaterialization) -> bool {
    matches!(
        materialization,
        DbtMaterialization::Incremental
            | DbtMaterialization::View
            | DbtMaterialization::MaterializedView
            | DbtMaterialization::DynamicTable
            | DbtMaterialization::StreamingTable
            | DbtMaterialization::Unknown(_)
    )
}

fn effective_run_cache_service_use_cache(
    run_cache_mode: &RunCacheMode,
    service_requested: bool,
) -> bool {
    run_cache_mode.use_cache()
        || (service_requested && matches!(run_cache_mode, RunCacheMode::Noop))
}

fn is_no_op_model_materialization(materialization: DbtMaterialization) -> bool {
    matches!(
        materialization,
        DbtMaterialization::Ephemeral | DbtMaterialization::Inline
    )
}

fn record_service_decision(
    unique_id: &str,
    response: &SubmitSqlResponse,
    freshness_tolerance_seconds: i64,
    honor_skip: bool,
) -> RunCacheServiceDecision {
    match response.response.as_ref() {
        Some(submit_sql_response::Response::ReadyToExecute(response)) => {
            let request_id = response.request_id.clone();
            emit_trace_log_message(|| {
                format!(
                    "dbt State service decision: ready to execute (node {unique_id}, request_id {request_id})"
                )
            });
            RunCacheServiceDecision::execute_with_confirmation(response.request_id.clone(), false)
        }
        Some(submit_sql_response::Response::SkipExecution(response)) => {
            if !honor_skip {
                emit_trace_log_message(|| {
                    format!(
                        "dbt State service decision: skip ignored in write-only mode for node {unique_id}; executing normally"
                    )
                });
                return RunCacheServiceDecision::execute_without_confirmation();
            }

            emit_trace_log_message(|| {
                format!("dbt State service decision: skip execution for node {unique_id}")
            });
            // For data tests, parse the cached `failures` count out of the
            // service's `execution_results` so the dispatcher in
            // `runnable/mod.rs` can replace the generic `ReusedNoChanges`
            // status with a test-shaped verdict and a NO-OP-marked stat.
            let cached_test_failures = if unique_id.starts_with("test.") {
                parse_cached_test_failures(response)
            } else {
                None
            };
            RunCacheServiceDecision::Skip {
                status: skip_node_status_from_response(response, freshness_tolerance_seconds),
                sao_stored_hash: None,
                cached_test_failures,
            }
        }
        Some(submit_sql_response::Response::ReadyToClone(response)) => {
            if honor_skip {
                let request_id = response.request_id.clone();
                let clone_source = response.clone_source.clone();
                let clone_target = response.clone_target.clone();
                emit_trace_log_message(|| {
                    format!(
                        "dbt State service decision: ready to clone (node {unique_id}, request_id {request_id}, clone_source {clone_source}, clone_target {clone_target})"
                    )
                });
                RunCacheServiceDecision::Clone {
                    clone: RunCacheCloneDecision::from_response(
                        response,
                        freshness_tolerance_seconds,
                    ),
                }
            } else {
                let request_id = response.request_id.clone();
                emit_trace_log_message(|| {
                    format!(
                        "dbt State service decision: clone ignored in write-only mode (node {unique_id}, request_id {request_id}); executing normally"
                    )
                });
                RunCacheServiceDecision::execute_with_confirmation(
                    response.request_id.clone(),
                    true,
                )
            }
        }
        None => {
            emit_trace_log_message(|| {
                format!(
                    "dbt State service decision: empty response ignored for node {unique_id}; executing normally"
                )
            });
            RunCacheServiceDecision::execute_without_confirmation()
        }
    }
}

/// Map a `SkipExecutionResponse` to the [`NodeStatus`] used for downstream
/// reporting. When the service admitted a candidate despite at least one
/// upstream having changed (`explained_decision.is_stale == true`), emit
/// [`NodeStatus::ReusedStillFresh`] so the terminal/run_results message
/// reads "New changes detected..." instead of "No new changes on any
/// upstreams".
///
/// `freshness_tolerance_seconds` is the same value Fusion sent in the request,
/// echoed locally to fill the formatter's `build_after` slot. The "last
/// updated" magnitude is not visible to Fusion (only the service sees the
/// cached-side per-dep timestamps), so it is reported as 0.
fn skip_node_status_from_response(
    response: &SkipExecutionResponse,
    freshness_tolerance_seconds: i64,
) -> NodeStatus {
    let is_stale = response
        .explained_decision
        .as_ref()
        .map(|d| d.is_stale)
        .unwrap_or(false);
    if !is_stale {
        return NodeStatus::ReusedNoChanges("No new changes on any upstreams".to_string());
    }

    let tolerance_secs = freshness_tolerance_seconds.max(0) as u64;
    let message = format!(
        "New changes detected within freshness tolerance of {}",
        humantime::format_duration(std::time::Duration::from_secs(tolerance_secs)),
    );
    NodeStatus::ReusedStillFresh(message, tolerance_secs, 0)
}

/// If `is_dev_cloned` and the service decided Skip, rewrite the status to a
/// structured clone-from-cache cache reason so run_results matches the dbt-core
/// plugin (`run_cache.py:_process_query_cache_response`).
fn relabel_skip_for_dev_cloned_node(
    is_dev_cloned: bool,
    decision: RunCacheServiceDecision,
) -> RunCacheServiceDecision {
    let RunCacheServiceDecision::Skip {
        status,
        sao_stored_hash,
        cached_test_failures,
    } = decision
    else {
        return decision;
    };
    if !is_dev_cloned {
        return RunCacheServiceDecision::Skip {
            status,
            sao_stored_hash,
            cached_test_failures,
        };
    }
    let relabelled = match status {
        NodeStatus::ReusedNoChanges(_) => NodeStatus::ReusedCloned(None),
        NodeStatus::ReusedStillFresh(_, tolerance_secs, _) => {
            NodeStatus::ReusedCloned(Some(tolerance_secs))
        }
        other => other,
    };
    RunCacheServiceDecision::Skip {
        status: relabelled,
        sao_stored_hash,
        cached_test_failures,
    }
}

fn record_unsupported_node(node: &dyn InternalDbtNodeAttributes, reason: &'static str) {
    let unique_id = node.unique_id();
    emit_trace_log_message(|| {
        format!("dbt State service submit skipped for node {unique_id}: {reason}")
    });
}

fn record_submit_skipped(node: &dyn InternalDbtNodeAttributes, reason: &'static str) {
    let unique_id = node.unique_id();
    emit_trace_log_message(|| {
        format!("dbt State service submit skipped for node {unique_id}: {reason}")
    });
}

/// Collects the root relation set for the start-of-run view-definition
/// traversal: every relation referenced by any selected node, excluding
/// relations belonging to selected nodes themselves. The compiled SQL
/// of selected view models is fed into the cache by
/// [`insert_compiled_view_definition`] once each model has rendered.
fn collect_view_traversal_roots(ctx: &TaskRunnerCtx) -> Vec<Arc<dyn BaseRelation>> {
    let adapter_type = ctx.adapter_type();
    let selected = &ctx.inner.runnable_set;
    let mut roots: BTreeMap<String, Arc<dyn BaseRelation>> = BTreeMap::new();

    for selected_id in selected {
        let Some(upstream_ids) = ctx.inner.runtime_deps.get(selected_id) else {
            continue;
        };
        for upstream_id in upstream_ids {
            if selected.contains(upstream_id) {
                continue;
            }
            let Some(node) = ctx.nodes().get_node(upstream_id) else {
                continue;
            };
            let Ok(relation) = create_relation_from_node(adapter_type, node, None) else {
                continue;
            };
            roots
                .entry(relation.semantic_fqn())
                .or_insert_with(|| relation.into());
        }
    }

    roots.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_common::io_args::RunCacheMode;
    use dbt_run_cache::proto::query_cache::{ReadyToCloneResponse, ReadyToExecuteResponse};
    use dbt_schemas::schemas::common::{FreshnessPeriod, UpdatesOn};
    use dbt_schemas::schemas::properties::{ModelFreshness, ModelState};

    fn model_with_state(state: ModelState) -> DbtModel {
        let mut model = DbtModel::default();
        model.__common_attr__.unique_id = "model.test.orders".to_string();
        model.__model_attr__.state = Some(state);
        model
    }

    #[test]
    fn ready_to_execute_confirms_after_execution() {
        assert_eq!(
            record_service_decision("model.test.orders", &ready_to_execute_response(), 0, true),
            RunCacheServiceDecision::Execute {
                after_success: RunCacheAfterSuccess::Confirm(RunCacheExecutionConfirmation {
                    request_id: "execute-request".to_string(),
                    failed_to_clone: false,
                    execution_results: None,
                    execution_runtime_ms: None,
                }),
                sao_guard: None
            }
        );
    }

    #[test]
    fn skip_response_is_honored_in_read_write_mode() {
        assert!(matches!(
            record_service_decision("model.test.orders", &skip_execution_response(), 0, true),
            RunCacheServiceDecision::Skip {
                status: NodeStatus::ReusedNoChanges(_),
                sao_stored_hash: None,
                cached_test_failures: None,
            }
        ));
    }

    #[test]
    fn stale_skip_response_emits_still_fresh_with_message() {
        let response = SubmitSqlResponse {
            response: Some(submit_sql_response::Response::SkipExecution(
                SkipExecutionResponse {
                    explained_decision: Some(ExplainedDecision {
                        is_stale: true,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
        };
        match record_service_decision("model.test.orders", &response, 3600, true) {
            RunCacheServiceDecision::Skip {
                status: NodeStatus::ReusedStillFresh(message, tolerance, _),
                sao_stored_hash: None,
                cached_test_failures: None,
            } => {
                assert!(
                    message.contains("New changes detected"),
                    "message did not advertise stale-skip: {message}"
                );
                assert_eq!(tolerance, 3600);
            }
            other => panic!("expected ReusedStillFresh, got {other:?}"),
        }
    }

    #[test]
    fn relabel_skip_for_dev_cloned_node_rewrites_still_fresh_to_clone_still_fresh() {
        let original = RunCacheServiceDecision::Skip {
            status: NodeStatus::ReusedStillFresh(
                "New changes detected within freshness tolerance of 1h".to_string(),
                3600,
                42,
            ),
            sao_stored_hash: None,
            cached_test_failures: None,
        };

        match relabel_skip_for_dev_cloned_node(true, original) {
            RunCacheServiceDecision::Skip {
                status: NodeStatus::ReusedCloned(Some(tolerance)),
                ..
            } => {
                assert_eq!(tolerance, 3600);
            }
            other => panic!("expected ReusedCloned(Some(_)), got {other:?}"),
        }
    }

    #[test]
    fn relabel_skip_for_dev_cloned_node_rewrites_reused_no_changes_to_clone() {
        let original = RunCacheServiceDecision::Skip {
            status: NodeStatus::ReusedNoChanges("No new changes on any upstreams".to_string()),
            sao_stored_hash: None,
            cached_test_failures: None,
        };

        match relabel_skip_for_dev_cloned_node(true, original) {
            RunCacheServiceDecision::Skip {
                status: NodeStatus::ReusedCloned(None),
                ..
            } => {}
            other => panic!("expected ReusedCloned(None), got {other:?}"),
        }
    }

    #[test]
    fn relabel_skip_for_dev_cloned_node_passes_through_when_not_dev_cloned() {
        let make = || RunCacheServiceDecision::Skip {
            status: NodeStatus::ReusedNoChanges("No new changes on any upstreams".to_string()),
            sao_stored_hash: None,
            cached_test_failures: None,
        };
        let relabelled = relabel_skip_for_dev_cloned_node(false, make());
        assert_eq!(relabelled, make());
    }

    #[test]
    fn relabel_skip_for_dev_cloned_node_passes_through_non_skip_decision() {
        let make = || RunCacheServiceDecision::Execute {
            after_success: RunCacheAfterSuccess::None,
            sao_guard: None,
        };
        let relabelled = relabel_skip_for_dev_cloned_node(true, make());
        assert_eq!(relabelled, make());
    }

    #[test]
    fn skip_response_is_ignored_in_write_only_mode() {
        assert_eq!(
            record_service_decision("model.test.orders", &skip_execution_response(), 0, false),
            RunCacheServiceDecision::Execute {
                after_success: RunCacheAfterSuccess::None,
                sao_guard: None,
            }
        );
    }

    #[test]
    fn full_refresh_blocks_only_affected_model_materializations() {
        assert!(full_refresh_blocks_model_submit(
            DbtMaterialization::Incremental
        ));
        assert!(full_refresh_blocks_model_submit(DbtMaterialization::View));
        assert!(!full_refresh_blocks_model_submit(DbtMaterialization::Table));
    }

    #[test]
    fn no_op_model_materializations_are_not_submitted() {
        assert!(is_no_op_model_materialization(
            DbtMaterialization::Ephemeral
        ));
        assert!(is_no_op_model_materialization(DbtMaterialization::Inline));
        assert!(!is_no_op_model_materialization(DbtMaterialization::View));
        assert!(!is_no_op_model_materialization(DbtMaterialization::Table));
    }

    #[test]
    fn env_requested_service_uses_read_write_when_cli_mode_is_noop() {
        assert!(effective_run_cache_service_use_cache(
            &RunCacheMode::Noop,
            true
        ));
        assert!(!effective_run_cache_service_use_cache(
            &RunCacheMode::Noop,
            false
        ));
        assert!(!effective_run_cache_service_use_cache(
            &RunCacheMode::WriteOnly,
            true
        ));
        assert!(effective_run_cache_service_use_cache(
            &RunCacheMode::ReadWrite,
            false
        ));
    }

    #[test]
    fn metadata_query_options_prefers_profile_metadata_warehouse() {
        let options = metadata_query_options_for_warehouses(
            Some("profile_wh".to_string()),
            Some("legacy_wh".to_string()),
        );

        assert_eq!(options.warehouse.as_deref(), Some("profile_wh"));
    }

    #[test]
    fn metadata_query_options_falls_back_to_legacy_service_warehouse() {
        let options = metadata_query_options_for_warehouses(None, Some("legacy_wh".to_string()));

        assert_eq!(options.warehouse.as_deref(), Some("legacy_wh"));
    }

    #[test]
    fn freshness_tolerance_uses_state_lag_tolerance() {
        let model = model_with_state(ModelState {
            lag_tolerance: Some(ModelFreshnessRules {
                count: Some(2),
                period: Some(FreshnessPeriod::hour),
                updates_on: None,
            }),
            require_fresh_data_from: None,
            evaluate_volatile_sql: None,
            pre_clone: None,
            execute_hooks_on_reuse: None,
        });

        assert_eq!(freshness_tolerance_seconds_for_node(&model, 2700), 7200);
    }

    #[test]
    fn state_require_fresh_data_from_overrides_legacy_updates_on() {
        let mut model = model_with_state(ModelState {
            lag_tolerance: None,
            require_fresh_data_from: Some(UpdatesOn::Any),
            evaluate_volatile_sql: None,
            pre_clone: None,
            execute_hooks_on_reuse: None,
        });
        model.__model_attr__.freshness = Some(ModelFreshness {
            build_after: Some(ModelFreshnessRules {
                count: Some(1),
                period: Some(FreshnessPeriod::hour),
                updates_on: Some(UpdatesOn::All),
            }),
        });

        assert_eq!(
            stale_upstream_policy_for_node(&model),
            dbt_run_cache::proto::query_cache::StaleUpstreamPolicy::Any
        );
    }

    #[test]
    fn state_evaluate_volatile_sql_overrides_legacy_meta() {
        let mut model = model_with_state(ModelState {
            lag_tolerance: None,
            require_fresh_data_from: None,
            evaluate_volatile_sql: Some(false),
            pre_clone: None,
            execute_hooks_on_reuse: None,
        });
        model.__common_attr__.meta.insert(
            "run_cache_tolerate_nondeterminism".to_string(),
            dbt_yaml::Value::Bool(true, dbt_yaml::Span::default()),
        );

        assert!(!resolve_tolerate_nondeterminism(&model, true));
    }

    #[test]
    fn lenient_dependencies_follow_config_and_final_deferred_fqns() {
        let deferred_fqns = BTreeSet::from([
            "prod.analytics.customers".to_string(),
            "prod.analytics.orders".to_string(),
            "prod.analytics.unrelated".to_string(),
        ]);
        let tables = vec![TableModifiedInfo {
            name: "prod.analytics.customers".to_string(),
            last_modified_epoch: 123,
        }];
        let query_dependencies = vec![QueryDependency {
            name: "prod.analytics.orders".to_string(),
            query: "select * from prod.raw.orders".to_string(),
            default_catalog: "prod".to_string(),
            default_schema: "analytics".to_string(),
        }];

        assert_eq!(
            build_lenient_dependencies(true, &deferred_fqns, &tables, &query_dependencies),
            vec![
                "prod.analytics.customers".to_string(),
                "prod.analytics.orders".to_string(),
            ]
        );
        assert_eq!(
            build_lenient_dependencies(true, &deferred_fqns, &[], &[]),
            Vec::<String>::new()
        );
        assert!(
            build_lenient_dependencies(false, &deferred_fqns, &tables, &query_dependencies)
                .is_empty()
        );
    }

    #[test]
    fn collected_view_query_dependencies_for_view_is_empty_and_complete() {
        // The view fast-path in `collect_query_dependencies` must produce no
        // upstream dependencies, no seen tables, and no parser relations, while
        // still marking the result complete so the submit isn't skipped.
        // Matches the dbt-state Python plugin's view path
        // (clients/dbt_state/src/dbt_state/run_cache.py:1116-1146).
        let deps = CollectedViewQueryDependencies::for_view();

        assert!(deps.dependencies.is_empty());
        assert!(deps.seen_tables.is_empty());
        assert!(deps.parser_seen_relations.is_empty());
        assert!(deps.metadata_complete);
    }

    #[test]
    fn lenient_dependencies_can_use_query_dependencies_without_tables() {
        let deferred_fqns = BTreeSet::from([
            "prod.analytics.customers".to_string(),
            "prod.analytics.orders".to_string(),
        ]);
        let query_dependencies = vec![QueryDependency {
            name: "prod.analytics.orders".to_string(),
            query: "select * from prod.raw.orders".to_string(),
            default_catalog: "prod".to_string(),
            default_schema: "analytics".to_string(),
        }];

        assert_eq!(
            build_lenient_dependencies(true, &deferred_fqns, &[], &query_dependencies),
            vec!["prod.analytics.orders".to_string()]
        );
    }

    #[test]
    fn ready_to_clone_returns_clone_decision_in_read_write_mode() {
        let decision =
            record_service_decision("model.test.orders", &ready_to_clone_response(), 0, true);

        let RunCacheServiceDecision::Clone { clone } = decision else {
            panic!("expected clone decision");
        };
        assert_eq!(clone.request_id, "clone-request");
        assert_eq!(clone.clone_sqls, vec!["create table target clone source"]);
        assert_eq!(clone.clone_source, "source");
        assert_eq!(clone.clone_target, "target");
        assert_eq!(clone.required_source_epoch, Some(123));
        assert_eq!(clone.execution_runtime_ms, Some(456));
        assert!(clone.execution_results.is_some());
        assert_eq!(
            clone.success_confirmation(),
            Some(RunCacheExecutionConfirmation {
                request_id: "clone-request".to_string(),
                failed_to_clone: false,
                execution_results: clone.execution_results.clone(),
                execution_runtime_ms: Some(456),
            })
        );
        assert_eq!(
            clone.fallback_confirmation(),
            Some(RunCacheExecutionConfirmation {
                request_id: "clone-request".to_string(),
                failed_to_clone: true,
                execution_results: None,
                execution_runtime_ms: None,
            })
        );
    }

    #[test]
    fn ready_to_clone_stale_decision_reports_clone_still_fresh_status() {
        let mut response = ready_to_clone_response();
        let Some(submit_sql_response::Response::ReadyToClone(clone_response)) =
            response.response.as_mut()
        else {
            panic!("expected clone response");
        };
        clone_response.explained_decision = Some(ExplainedDecision {
            is_stale: true,
            ..Default::default()
        });

        let decision = record_service_decision("model.test.orders", &response, 3600, true);
        let RunCacheServiceDecision::Clone { clone } = decision else {
            panic!("expected clone decision");
        };
        assert_eq!(clone.success_status(), NodeStatus::ReusedCloned(Some(3600)));
    }

    #[test]
    fn ready_to_clone_is_ignored_in_write_only_mode() {
        assert_eq!(
            record_service_decision("model.test.orders", &ready_to_clone_response(), 0, false),
            RunCacheServiceDecision::Execute {
                after_success: RunCacheAfterSuccess::Confirm(RunCacheExecutionConfirmation {
                    request_id: "clone-request".to_string(),
                    failed_to_clone: true,
                    execution_results: None,
                    execution_runtime_ms: None,
                }),
                sao_guard: None,
            }
        );
    }

    #[test]
    fn empty_response_executes_without_confirmation() {
        assert_eq!(
            record_service_decision("model.test.orders", &empty_response(), 0, true),
            RunCacheServiceDecision::Execute {
                after_success: RunCacheAfterSuccess::None,
                sao_guard: None,
            }
        );
    }

    fn ready_to_execute_response() -> SubmitSqlResponse {
        SubmitSqlResponse {
            response: Some(submit_sql_response::Response::ReadyToExecute(
                ReadyToExecuteResponse {
                    request_id: "execute-request".to_string(),
                    ..Default::default()
                },
            )),
        }
    }

    fn skip_execution_response() -> SubmitSqlResponse {
        SubmitSqlResponse {
            response: Some(submit_sql_response::Response::SkipExecution(
                SkipExecutionResponse::default(),
            )),
        }
    }

    fn ready_to_clone_response() -> SubmitSqlResponse {
        SubmitSqlResponse {
            response: Some(submit_sql_response::Response::ReadyToClone(
                ReadyToCloneResponse {
                    request_id: "clone-request".to_string(),
                    clone_sqls: vec!["create table target clone source".to_string()],
                    clone_source: "source".to_string(),
                    clone_target: "target".to_string(),
                    clone_required_last_modified_epoch: Some(123),
                    clone_execution_results: Some(Struct::default()),
                    execution_runtime_ms: Some(456),
                    ..Default::default()
                },
            )),
        }
    }

    fn empty_response() -> SubmitSqlResponse {
        SubmitSqlResponse { response: None }
    }
}
