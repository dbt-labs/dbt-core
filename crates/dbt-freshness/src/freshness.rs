use arrow::compute::cast_with_options;
use arrow::{
    self,
    datatypes::{DataType, TimeUnit},
};
use chrono::{DateTime, Utc};
use datafusion_common::{cast::as_timestamp_nanosecond_array, format::DEFAULT_CAST_OPTIONS};
use dbt_adapter::{
    Adapter,
    errors::into_fs_error,
    relation::{RelationObject, create_relation_from_node, do_create_relation},
};
use dbt_adapter_core::AdapterType;
use dbt_agate::AgateTable;
use dbt_common::constants::DBT_SOURCES_JSON;
use dbt_common::io_args::{IoArgs, ShowOptions};
use dbt_common::tracing::dbt_emit::emit_info_log_message;
use dbt_common::tracing::event_info::store_event_attributes;
use dbt_common::tracing::span_info::{record_span_status_with_attrs, update_span_attrs};
use dbt_common::tracing::{
    dbt_emit::{
        emit_error_log_from_fs_error, emit_info_progress_message, emit_warn_log_from_fs_error,
        emit_warn_log_message,
    },
    emit::{create_info_span, emit_info_event},
};
use dbt_common::{
    CodeLocationWithFile, ErrorCode, FsError, FsResult, err, fs_err, stdfs::File, unexpected_fs_err,
};
use dbt_dag::schedule::Schedule;
use dbt_jinja_ctx::{CompileBaseCtx, CustomSqlRenderCtx, DummyConfig, JinjaObject, OperationCtx};
use dbt_jinja_utils::{
    jinja_environment::JinjaEnv,
    phases::{build_compile_base_ctx, run::build_run_node_ctx},
};
use dbt_schemas::schemas::telemetry::{ExecutionPhase, PhaseExecuted};
use dbt_schemas::schemas::{
    DbtModel, DbtSource, FreshnessResultsArtifact, FreshnessResultsMetadata, FreshnessResultsNode,
    InternalDbtNode, InternalDbtNodeAttributes, NodePathKind,
    common::{FreshnessPeriod, FreshnessRules, FreshnessStatus},
    relations::base::BaseRelation,
};
use dbt_schemas::state::ResolverState;
use dbt_tasks_core::PreTaskRunData;
use dbt_telemetry::{
    ArtifactType, ArtifactWritten, NodeOutcome, NodeProcessed, ProgressMessage, ShowResult,
    SourceFreshnessDetail, SourceFreshnessOutcome, node_processed,
    update_dbt_core_event_code_for_node_processed_end,
};
use itertools::Itertools;
use std::collections::HashMap;
use std::path::Path;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Debug, Clone)]
pub struct FreshnessResult {
    pub max_loaded_at: DateTime<Utc>,
    pub snapshotted_at: DateTime<Utc>,
    pub age: i64,
    pub status: FreshnessStatus,
}

impl FreshnessResult {
    pub fn from(
        max_loaded_at: DateTime<Utc>,
        snapshotted_at: DateTime<Utc>,
        age: i64,
    ) -> FreshnessResult {
        FreshnessResult {
            max_loaded_at,
            snapshotted_at,
            age,
            status: FreshnessStatus::Pass,
        }
    }
}

pub struct FreshnessTimestamps(pub BTreeMap<String, FreshnessResult>);

impl PreTaskRunData for FreshnessTimestamps {
    fn get(&self, node_id: &str) -> Option<String> {
        self.0.get(node_id).map(|r| r.max_loaded_at.to_string())
    }
}

fn period_to_seconds(period: &FreshnessPeriod) -> i64 {
    match period {
        FreshnessPeriod::second => 1,
        FreshnessPeriod::minute => 60,
        FreshnessPeriod::hour => 60 * 60,
        FreshnessPeriod::day => 60 * 60 * 24,
    }
}

pub fn calculate_seconds(freshness: &FreshnessRules) -> FsResult<i64> {
    // F2 safety net: parse-time validation now demotes a partially-populated
    // freshness rule to a warning so `parse` / `run` / `build` are not
    // aborted. When the rule is actually consumed (e.g. by
    // `dbt source freshness`), revalidate and surface the same `dbt1007`
    // error rather than panicking on the missing field.
    FreshnessRules::validate(Some(freshness))?;
    let count = freshness.count.ok_or_else(|| {
        fs_err!(
            ErrorCode::InvalidArgument,
            "count and period are required when freshness is provided, count: {:?}, period: {:?}",
            freshness.count,
            freshness.period
        )
    })?;
    let period = freshness.period.as_ref().ok_or_else(|| {
        fs_err!(
            ErrorCode::InvalidArgument,
            "count and period are required when freshness is provided, count: {:?}, period: {:?}",
            freshness.count,
            freshness.period
        )
    })?;
    Ok(count * period_to_seconds(period))
}

const MAX_LOADED_AT_COLUMN: usize = 0;
const SNAPSHOTTED_AT_COLUMN: usize = 1;

#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip_all, level = "trace")]
pub async fn calculate_freshness(
    loaded_at_field: &str,
    filter: &str,
    source: &DbtSource,
    adapter_type: AdapterType,
    jinja_env: &JinjaEnv,
    compile_base: &CompileBaseCtx,
    io_args: &IoArgs,
    dependencies: BTreeSet<String>,
) -> FsResult<FreshnessResult> {
    let relation = source
        .__base_attr__
        .relation_name
        .as_ref()
        .ok_or_else(|| unexpected_fs_err!("Source needs a relation name"))?
        .as_str();
    let macro_expr = if filter.is_empty() {
        format!("collect_freshness('{relation}', '{loaded_at_field}').table")
    } else {
        format!("collect_freshness('{relation}', '{loaded_at_field}', '{filter}').table")
    };

    calculate_freshness_common(
        &macro_expr,
        source,
        adapter_type,
        jinja_env,
        compile_base,
        io_args,
        dependencies,
        "loaded_at_field",
    )
    .await
}

#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip_all, level = "trace")]
pub async fn calculate_freshness_custom_sql(
    loaded_at_query: &str,
    source: &DbtSource,
    adapter_type: AdapterType,
    jinja_env: &JinjaEnv,
    compile_base: &CompileBaseCtx,
    io_args: &IoArgs,
    dependencies: BTreeSet<String>,
) -> FsResult<FreshnessResult> {
    let relation = source
        .__base_attr__
        .relation_name
        .as_ref()
        .ok_or_else(|| unexpected_fs_err!("Source needs a relation name"))?
        .as_str();

    // Create a context with `this` relation for rendering the loaded_at_query
    // This allows users to use {{this}} in their loaded_at_query to reference the source table
    let this_relation = RelationObject::new(Arc::from(
        do_create_relation(
            adapter_type,
            source.__base_attr__.database.clone(),
            source.__base_attr__.schema.clone(),
            Some(source.__base_attr__.alias.clone()),
            None,
            source.__base_attr__.quoting,
        )
        .map_err(|e| {
            fs_err!(
                code => ErrorCode::Unexpected,
                loc => source.get_node_path(NodePathKind::Definition, io_args.in_dir.as_path(), io_args.out_dir.as_path()).into_owned(),
                "Failed to create 'this' relation for source '{}': {}",
                source.__common_attr__.unique_id,
                e
            )
        })?,
    ))
    .into_value();

    // Pre-render context: operation scope (base + no-op `config`) plus
    // `this`/`database`/`schema`/`identifier` so loaded_at_query can reference
    // `{{ this }}`. Rendered typed, no BTreeMap.
    let render_context = CustomSqlRenderCtx {
        operation: OperationCtx {
            base: compile_base.clone(),
            config: JinjaObject::new(DummyConfig {}),
        },
        this: this_relation,
        database: source.__base_attr__.database.clone(),
        schema: source.__base_attr__.schema.clone(),
        identifier: source.__base_attr__.alias.clone(),
    };

    // Pre-render the loaded_at_query to resolve any Jinja expressions like {{this}}
    let source_path = source
        .get_node_path(
            NodePathKind::Definition,
            io_args.in_dir.as_path(),
            io_args.out_dir.as_path(),
        )
        .into_owned();
    let rendered_query = jinja_env
        .render_named_str(
            &source_path.to_string_lossy(),
            loaded_at_query,
            &render_context,
            &[],
        )
        .map_err(|e| {
            let loc = e.significant_span().map(|span| {
                CodeLocationWithFile::new(
                    span.start_line,
                    span.start_col,
                    span.start_offset,
                    source_path.clone(),
                )
            });
            let fs_err = FsError::from_jinja_err(e, "Failed to render the Jinja str");
            match loc {
                Some(loc) => fs_err.with_location(loc),
                None => fs_err.with_location(source_path.clone()),
            }
        })?;
    // Insert loaded_at_query as an escaped string
    let macro_expr = format!(
        "collect_freshness_custom_sql('{relation}', '{}').table",
        rendered_query.escape_default()
    );

    calculate_freshness_common(
        &macro_expr,
        source,
        adapter_type,
        jinja_env,
        compile_base,
        io_args,
        dependencies,
        "loaded_at_query",
    )
    .await
}

#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip_all, level = "trace")]
async fn calculate_freshness_common(
    macro_expr: &str,
    source: &DbtSource,
    adapter_type: AdapterType,
    jinja_env: &JinjaEnv,
    compile_base: &CompileBaseCtx,
    io_args: &IoArgs,
    dependencies: BTreeSet<String>,
    error_message: &str,
) -> FsResult<FreshnessResult> {
    let context = build_run_node_ctx(
        source,
        &source.deprecated_config,
        adapter_type,
        None,
        compile_base,
        io_args,
        ExecutionPhase::Run,
        None,
        dependencies,
    );

    let expr = jinja_env.compile_expression(macro_expr)?;
    let table = expr
        .eval(&context, &[])?
        .downcast_object::<AgateTable>()
        .ok_or_else(|| unexpected_fs_err!("Agate table expected"))?;

    let batch = table.original_record_batch();

    // check if table has one row and 2 columns, if not raise an error
    if batch.num_rows() != 1 || batch.num_columns() != 2 {
        return err!(
            code => ErrorCode::Unexpected,
            loc => source.get_node_path(NodePathKind::Definition, io_args.in_dir.as_path(), io_args.out_dir.as_path()).into_owned(),
            "Source '{}' freshness result table should have 1 row and 2 columns, but got {} rows and {} columns",
            source.__common_attr__.unique_id,
            batch.num_rows(),
            batch.num_columns()
        );
    }

    // A user provided query/field has to have a timestamp type of any precision
    // and we convert it to nanoseconds later.
    let max_loaded_at_column = validate_and_extract_timestamp_column(
        &batch,
        MAX_LOADED_AT_COLUMN,
        source,
        error_message,
        io_args,
    )?;

    let snapshotted_at = validate_and_extract_timestamp_column(
        &batch,
        SNAPSHOTTED_AT_COLUMN,
        source,
        error_message,
        io_args,
    )?;

    let max_loaded_at = as_timestamp_nanosecond_array(&cast_with_options(
        max_loaded_at_column.as_ref(),
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        &DEFAULT_CAST_OPTIONS,
    )?)?
    .value(0);

    let snapshotted_at = as_timestamp_nanosecond_array(&cast_with_options(
        snapshotted_at.as_ref(),
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        &DEFAULT_CAST_OPTIONS,
    )?)?
    .value(0);

    let age = (snapshotted_at - max_loaded_at) / 1_000_000_000;
    let result = FreshnessResult::from(
        DateTime::from_timestamp_nanos(max_loaded_at),
        DateTime::from_timestamp_nanos(snapshotted_at),
        age,
    );

    Ok(result)
}

/// Whether a source takes part in the freshness pass that state-aware orchestration
/// (`dbt build` / `dbt run`) runs before the task runner, to decide whether downstream
/// models must rebuild.
///
/// A source opts out of freshness with the documented escape hatch
/// `config: freshness: null`, which is the *only* way `__source_attr__.freshness` ends up
/// `None`: a source that configures nothing still resolves to an empty `FreshnessDefinition`
/// (see `merge_freshness_unwrapped` in `dbt-parser`'s `resolve_sources`, META-5461), so those
/// sources keep their metadata tracked here. An opted-out source has no rule to evaluate, so
/// querying `INFORMATION_SCHEMA.TABLES` for it buys nothing and warns for objects that don't
/// live there at all — e.g. Snowflake SEQUENCE objects declared as sources, which is what
/// dbt-labs/dbt-core#14534 reported.
///
/// A `loaded_at_field` / `loaded_at_query` is still honored on an opted-out source: the user
/// named a column/query to measure freshness from, so it is measured.
///
/// Skipped sources have no entry in the returned results, which the run-cache treats like any
/// other source without freshness data: the source counts as just-updated and its dependents
/// rebuild.
fn collects_freshness_during_build(node: &DbtSource) -> bool {
    node.__source_attr__.freshness.is_some()
        || !node.get_loaded_at_field().is_empty()
        || !node.get_loaded_at_query().is_empty()
}

/// This function is used to calculate the freshness of the sources and extended models.
#[allow(clippy::cognitive_complexity)]
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(
    skip_all,
    fields(
        _e = ?store_event_attributes(PhaseExecuted::start_general(ExecutionPhase::FreshnessAnalysis)),
    )
)]
pub async fn run_freshness(
    io_args: &IoArgs,
    schedule: &Schedule<String>,
    resolver_state: &ResolverState,
    adapter: Arc<Adapter>,
    env: &JinjaEnv,
    is_source_freshness_command: bool,
    check_all: bool,
) -> FsResult<BTreeMap<String, FreshnessResult>> {
    // First, collect all sources and extended models
    let set_of_nodes = schedule
        .all_selected_nodes
        .clone()
        .into_iter()
        .chain(schedule.frontier_nodes.clone().into_iter())
        .collect::<BTreeSet<_>>();

    let mut sources = set_of_nodes
        .iter()
        .map(|unique_id| {
            resolver_state
                .nodes
                .get_node(unique_id)
                .ok_or_else(|| unexpected_fs_err!("Node must be resolved"))
        })
        .filter_map_ok(|node| node.as_any().downcast_ref::<DbtSource>())
        .collect::<Result<Vec<_>, _>>()?;

    // F2 safety net: parse-time validation of freshness rules is now demoted
    // to a warning so partial rules don't abort `parse` / `run` / `build`. When
    // `dbt source freshness` actually consumes the rule, surface the same
    // `dbt1007` error before we issue any warehouse query, matching the
    // pre-fix user-visible behavior of the command.
    if is_source_freshness_command {
        for node in sources.iter() {
            if let Some(freshness) = node.__source_attr__.freshness.as_ref() {
                let loc = node.__common_attr__.name_span.start.clone();
                FreshnessRules::validate(freshness.error_after.as_ref())
                    .map_err(|e| e.with_location(loc.clone()))?;
                FreshnessRules::validate(freshness.warn_after.as_ref())
                    .map_err(|e| e.with_location(loc))?;
            }
        }
    }

    // If we are not checking all
    let extended_models = if is_source_freshness_command {
        if !check_all {
            sources = sources
                .into_iter()
                .filter(|node| {
                    node.__source_attr__
                        .freshness
                        .as_ref()
                        .is_some_and(|f| f.error_after.is_some() || f.warn_after.is_some())
                })
                .collect::<Vec<_>>();
        }
        // If this is a source freshness command, then we don't need to check extended models
        vec![]
    } else {
        sources.retain(|node| collects_freshness_during_build(node));

        // If this is not a source freshness command, then we need to check extended models
        set_of_nodes
            .iter()
            .map(|unique_id| {
                resolver_state
                    .nodes
                    .get_node(unique_id)
                    .ok_or_else(|| unexpected_fs_err!("Node must be resolved"))
            })
            .filter_map_ok(|node| {
                if node.is_extended_model() {
                    node.as_any().downcast_ref::<DbtModel>()
                } else {
                    None
                }
            })
            .collect::<Result<Vec<_>, _>>()?
    };

    // Early out if we have no sources or extended models to check freshness for
    if sources.is_empty() && extended_models.is_empty() {
        // If this is a source freshness command, issue "Nothing to do" message.
        if is_source_freshness_command {
            emit_warn_log_message(
                ErrorCode::FreshnessConfigInvalid,
                "Nothing to do. No sources with freshness config.",
            );
        }
        return Ok(BTreeMap::new());
    }

    // Create spans for all sources BEFORE issuing any queries
    // These spans will be updated with outcomes after freshness is calculated.
    // When freshness is collected as part of a build rather than by
    // `dbt source freshness`, no spans are created so users are not confused by
    // extra counts of processed nodes. We instead issue a progress log line.
    let source_spans: HashMap<&str, tracing::Span> = if is_source_freshness_command {
        sources
            .iter()
            .map(|node| {
                let node_processed_event = node.get_node_processed_event(
                    Some(ExecutionPhase::FreshnessAnalysis),
                    io_args.in_dir.as_path(),
                    io_args.out_dir.as_path(),
                    true,
                );
                let span = create_info_span(node_processed_event);
                (node.__common_attr__.unique_id.as_str(), span)
            })
            .collect()
    } else {
        HashMap::new()
    };

    // Build the typed compile base once for the whole freshness pass and
    // thread `&CompileBaseCtx` into the node contexts below.
    let namespace_keys: Vec<String> = env
        .env
        .get_macro_namespace_registry()
        .map(|r| r.keys().map(|k| k.to_string()).collect())
        .unwrap_or_default();
    let compile_base = build_compile_base_ctx(
        resolver_state.node_resolver.clone(),
        &resolver_state.root_project_name,
        &resolver_state.nodes,
        resolver_state.defer_nodes.as_ref(),
        resolver_state.runtime_config.clone(),
        namespace_keys,
    );

    let results = run_freshness_with_spans(
        &sources,
        &extended_models,
        resolver_state,
        adapter,
        env,
        &compile_base,
        io_args,
        is_source_freshness_command,
        &source_spans,
    )
    .await;

    if is_source_freshness_command && let Err(ref err) = results {
        let error_message = err.to_string();
        for span in source_spans.values() {
            record_span_status_with_attrs(
                span,
                |attrs| {
                    if let Some(node_processed) = attrs.downcast_mut::<NodeProcessed>() {
                        node_processed.node_outcome = NodeOutcome::Error as i32;
                        update_dbt_core_event_code_for_node_processed_end(node_processed);
                    }
                },
                Some(error_message.as_str()),
            );
        }
    }

    // source_spans are dropped here, which closes the spans and emits the end events
    results
}

#[allow(clippy::too_many_arguments)]
async fn run_freshness_with_spans(
    sources: &Vec<&DbtSource>,
    extended_models: &Vec<&DbtModel>,
    resolver_state: &ResolverState,
    adapter: Arc<Adapter>,
    env: &JinjaEnv,
    compile_base: &CompileBaseCtx,
    io_args: &IoArgs,
    is_source_freshness_command: bool,
    source_spans: &HashMap<&str, tracing::Span>,
) -> FsResult<BTreeMap<String, FreshnessResult>> {
    let mut results: BTreeMap<String, FreshnessResult> = BTreeMap::new();
    // Collect all source relations that do not specify a 'loaded_at' field or a custom sql query.
    let (relations, name_map) =
        collect_relations(sources, extended_models, resolver_state.adapter_type)?;

    // Get the freshness information for the collected source relations.
    if !relations.is_empty()
        && let Some(metadata_adapter) = adapter.metadata_adapter()
    {
        let freshness_result = metadata_adapter
            .freshness(&relations, adapter.cancellation_token())
            .await
            .map_err(into_fs_error);

        // When freshness is collected as part of a build, metadata query failures
        // (e.g. authorization errors for databases the role cannot access) should not
        // fail the build. Sources without freshness data will be handled by the
        // downstream fallback (treated as updated).
        let freshness = match freshness_result {
            Ok(f) => f,
            Err(e) if !is_source_freshness_command => {
                emit_warn_log_message(
                    ErrorCode::FreshnessMetadataWarning,
                    format!(
                        "Failed to extract freshness metadata: {e}. Affected sources will be treated as updated."
                    ),
                );
                BTreeMap::new()
            }
            Err(e) => return Err(e),
        };

        let snapshotted_at = Utc::now();
        for (relation, metadata) in freshness {
            let unique_ids = name_map
                .get(&relation)
                .ok_or_else(|| unexpected_fs_err!("key must exist"))?;
            let mut max_loaded_at = metadata.last_altered;
            if metadata.is_view {
                // Views are considered to have fresh data by default.
                max_loaded_at = snapshotted_at;
                // Only show warning for sources, not models
                for unique_id in unique_ids {
                    if unique_id.starts_with("source.") {
                        emit_warn_log_message(
                            ErrorCode::FreshnessConfigInvalid,
                            format!(
                                "{} is a view with no 'loaded_at_field' or 'loaded_at_query' so will always be considered fresh",
                                unique_id
                            ),
                        );
                    }
                }
            }
            let freshness_result = FreshnessResult::from(
                max_loaded_at,
                snapshotted_at,
                snapshotted_at.timestamp() - max_loaded_at.timestamp(),
            );
            for unique_id in unique_ids {
                results.insert(unique_id.clone(), freshness_result.clone());
            }
        }
    }

    // Run 'loaded_at' and custom queries.
    for node in sources.iter().filter(|node| {
        !node.get_loaded_at_field().is_empty() || !node.get_loaded_at_query().is_empty()
    }) {
        let loaded_at_field = node.get_loaded_at_field();
        let loaded_at_query = node.get_loaded_at_query();
        let dependencies: BTreeSet<String> = resolver_state
            .runtime_config
            .dependencies
            .keys()
            .cloned()
            .collect();
        if !loaded_at_field.is_empty() {
            let filter = node
                .__source_attr__
                .freshness
                .as_ref()
                .and_then(|f| f.filter.as_deref())
                .unwrap_or("");
            let result = calculate_freshness(
                &loaded_at_field.escape_default().to_string(),
                &filter.escape_default().to_string(),
                node,
                resolver_state.adapter_type,
                env,
                compile_base,
                io_args,
                dependencies.clone(),
            )
            .await?;
            results.insert(node.__common_attr__.unique_id.clone(), result);
        }
        if !loaded_at_query.is_empty() {
            let result = calculate_freshness_custom_sql(
                loaded_at_query,
                node,
                resolver_state.adapter_type,
                env,
                compile_base,
                io_args,
                dependencies,
            )
            .await?;
            results.insert(node.__common_attr__.unique_id.clone(), result);
        }
    }

    for node in sources {
        if let Some(result) = results.get_mut(&node.__common_attr__.unique_id) {
            if !is_source_freshness_command {
                // Emit a freshness info log when freshness is collected as part of a
                // build. The source freshness command generates `NodeProcessed`, so it
                // does not need this.
                emit_info_progress_message(ProgressMessage::new_from_action_and_target(
                    "Freshness".to_string(),
                    format!(
                        "{} last updated {} ago",
                        node.__common_attr__.unique_id,
                        humantime::format_duration(std::time::Duration::from_secs(
                            result.age as u64
                        ))
                    ),
                ));
            }
            let error_after = node
                .__source_attr__
                .freshness
                .as_ref()
                .and_then(|x| x.error_after.as_ref())
                .map(calculate_seconds)
                .transpose()?;

            let warn_after = node
                .__source_attr__
                .freshness
                .as_ref()
                .and_then(|x| x.warn_after.as_ref())
                .map(calculate_seconds)
                .transpose()?;

            // Determine freshness outcome based on thresholds
            let freshness_outcome = if error_after.is_some_and(|x| result.age > x) {
                result.status = FreshnessStatus::Error;
                SourceFreshnessOutcome::OutcomeFailed
            } else if warn_after.is_some_and(|x| result.age > x) {
                result.status = FreshnessStatus::Warn;
                SourceFreshnessOutcome::OutcomeWarned
            } else {
                result.status = FreshnessStatus::Pass;
                SourceFreshnessOutcome::OutcomePassed
            };

            // Update the span status with freshness outcome
            // The span was created before the queries, now we update it with the result
            if is_source_freshness_command {
                if let Some(span) = source_spans.get(node.__common_attr__.unique_id.as_str()) {
                    update_span_attrs(span, |ev: &mut NodeProcessed| {
                        ev.node_outcome = NodeOutcome::Success as i32;
                        ev.node_outcome_detail =
                            Some(node_processed::NodeOutcomeDetail::NodeFreshnessOutcome(
                                SourceFreshnessDetail {
                                    node_freshness_outcome: freshness_outcome as i32,
                                    age_seconds: Some(result.age),
                                },
                            ));
                        update_dbt_core_event_code_for_node_processed_end(ev);
                    });
                }

                // Emit error/warn logs for source freshness command (for backward compatibility)
                if freshness_outcome == SourceFreshnessOutcome::OutcomeFailed {
                    let err = fs_err!(
                        code => ErrorCode::StaleSource,
                        loc => node.get_node_path(NodePathKind::Definition, io_args.in_dir.as_path(), io_args.out_dir.as_path()).into_owned(),
                        "Stale source {}",
                        node.__common_attr__.unique_id
                    );
                    emit_error_log_from_fs_error(*err);
                } else if freshness_outcome == SourceFreshnessOutcome::OutcomeWarned {
                    let err = fs_err!(
                        code => ErrorCode::StaleSource,
                        loc => node.get_node_path(NodePathKind::Definition, io_args.in_dir.as_path(), io_args.out_dir.as_path()).into_owned(),
                        "Stale source {}",
                        node.__common_attr__.unique_id
                    );
                    emit_warn_log_from_fs_error(*err);
                }
            }
        } else if is_source_freshness_command {
            return err!(
                    code => ErrorCode::Unexpected,
                    loc => node.__common_attr__.name_span.start.clone(),
                    "Could not find freshness information for source '{}'. Please verify that you have access to view metadata for this source in the warehouse.",
                    node.__common_attr__.unique_id
            );
        } else {
            // When freshness is collected as part of a build, missing freshness info is
            // non-fatal. The downstream task runner falls back to Utc::now() for sources
            // without results, conservatively forcing dependent models to rebuild.
            emit_warn_log_message(
                ErrorCode::FreshnessMetadataWarning,
                format!(
                    "Could not find freshness information for source '{}'. The source will be treated as updated, and dependent models will rebuild.",
                    node.__common_attr__.unique_id
                ),
            );
        }
    }

    Ok(results)
}

// Helper function to create relation and update name map
fn create_relation_and_map(
    node: &impl InternalDbtNodeAttributes,
    adapter_type: AdapterType,
    name_map: &mut BTreeMap<String, Vec<String>>,
) -> FsResult<Arc<dyn BaseRelation>> {
    let relation = create_relation_from_node(adapter_type, node, None)?;
    name_map
        .entry(relation.semantic_fqn())
        .or_default()
        .push(node.unique_id());
    Ok(relation.into())
}

/// Return collection of relations and a map from relation FQN to node unique_ids.
///
/// Multiple source nodes from different packages may point to the same physical
/// table (same FQN). The map therefore uses `Vec<String>` so every unique_id
/// that shares a FQN receives freshness data.  The relations list is deduplicated
/// so the warehouse is queried only once per physical table.
#[allow(clippy::type_complexity)]
fn collect_relations(
    sources: &[&DbtSource],
    extended_models: &Vec<&DbtModel>,
    adapter_type: AdapterType,
) -> FsResult<(Vec<Arc<dyn BaseRelation>>, BTreeMap<String, Vec<String>>)> {
    let mut name_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut all_relations = Vec::new();

    // Collect relations from sources
    let source_relations = sources
        .iter()
        .filter(|node| {
            node.get_loaded_at_field().is_empty() && node.get_loaded_at_query().is_empty()
        })
        .map(|node| create_relation_and_map(*node, adapter_type, &mut name_map))
        .collect::<FsResult<Vec<_>>>()?;

    all_relations.extend(source_relations);

    // Collect relations from extended models
    let model_relations = extended_models
        .iter()
        .map(|node| create_relation_and_map(*node, adapter_type, &mut name_map))
        .collect::<FsResult<Vec<_>>>()?;

    all_relations.extend(model_relations);

    // Deduplicate relations so each physical table is queried only once.
    // name_map already holds all unique_ids per FQN, so dropping duplicates
    // from the relations list is safe.
    let mut seen = BTreeSet::new();
    all_relations.retain(|r| seen.insert(r.semantic_fqn()));

    Ok((all_relations, name_map))
}

// Helper function to validate and extract timestamp column
fn validate_and_extract_timestamp_column(
    batch: &arrow::record_batch::RecordBatch,
    column_index: usize,
    source: &DbtSource,
    error_message: &str,
    io_args: &IoArgs,
) -> FsResult<arrow::array::ArrayRef> {
    match batch.column(column_index).data_type() {
        DataType::Timestamp(_, None) => {
            // No timezone info is attached so we issue a warning as we are about to make some timezone up.
            let err = fs_err!(
                code => ErrorCode::Unexpected,
                loc => source.get_node_path(NodePathKind::Definition, io_args.in_dir.as_path(), io_args.out_dir.as_path()).into_owned(),
                "Source '{}' {} has a timestamp type without a timezone, we will assume a UTC timezone",
                source.__common_attr__.unique_id,
                error_message
            );
            emit_warn_log_from_fs_error(*err);

            Ok(batch.column(column_index).clone())
        }
        DataType::Timestamp(_, Some(_)) => Ok(batch.column(column_index).clone()),
        _ => {
            err!(
                code => ErrorCode::Unexpected,
                loc => source.get_node_path(NodePathKind::Definition, io_args.in_dir.as_path(), io_args.out_dir.as_path()).into_owned(),
                "Source '{}' {} should have a timestamp type, but got {} type",
                source.__common_attr__.unique_id,
                error_message,
                batch.column(column_index).data_type()
            )
        }
    }
}

pub fn freshness_results_to_nodes(
    resolver_state: &ResolverState,
    results: &BTreeMap<String, FreshnessResult>,
) -> Vec<FreshnessResultsNode> {
    results
        .iter()
        .map(|(unique_id, result)| FreshnessResultsNode {
            unique_id: unique_id.clone(),
            max_loaded_at: result.max_loaded_at,
            snapshotted_at: result.snapshotted_at,
            max_loaded_at_time_ago_in_s: result.age as f64,
            status: result.status.clone(),
            criteria: resolver_state
                .nodes
                .get_node(unique_id)
                .expect("node must exist")
                .as_any()
                .downcast_ref::<DbtSource>()
                .expect("source expected")
                .__source_attr__
                .freshness
                .clone()
                .unwrap_or_default(),
            adapter_response: BTreeMap::new(),
            timing: vec![],
            thread_id: format!(
                "Thread-{}",
                format!("{:?}", std::thread::current().id())
                    .trim_start_matches("ThreadId(")
                    .trim_end_matches(")")
            ),
            execution_time: 0.0,
            node: None,
        })
        .collect()
}

/// Builds freshness results for the Jinja `on_run_end` context, including the source node.
///
/// Unlike `freshness_results_to_nodes` (used for the sources.json artifact),
/// this variant attaches the resolved source node so that macros like elementary's
/// `upload_source_freshness` can access `result.node.unique_id` and other node fields.
pub fn freshness_results_to_context(
    resolver_state: &ResolverState,
    results: &BTreeMap<String, FreshnessResult>,
) -> Vec<FreshnessResultsNode> {
    results
        .iter()
        .map(|(unique_id, result)| {
            let node = resolver_state
                .nodes
                .get_node_owned(unique_id)
                .expect("node must exist");
            let criteria = node
                .as_any()
                .downcast_ref::<DbtSource>()
                .expect("source expected")
                .__source_attr__
                .freshness
                .clone()
                .unwrap_or_default();
            FreshnessResultsNode {
                unique_id: unique_id.clone(),
                max_loaded_at: result.max_loaded_at,
                snapshotted_at: result.snapshotted_at,
                max_loaded_at_time_ago_in_s: result.age as f64,
                status: result.status.clone(),
                criteria,
                adapter_response: BTreeMap::new(),
                timing: vec![],
                thread_id: format!(
                    "Thread-{}",
                    format!("{:?}", std::thread::current().id())
                        .trim_start_matches("ThreadId(")
                        .trim_end_matches(")")
                ),
                execution_time: 0.0,
                node: Some(node),
            }
        })
        .collect()
}

/// Builds the `sources.json` artifact. Kept separate from the write so callers can
/// hand it back in memory even when the write fails.
pub fn build_sources_artifact(
    invocation_id: &uuid::Uuid,
    resolver_state: &ResolverState,
    results: &BTreeMap<String, FreshnessResult>,
) -> FreshnessResultsArtifact {
    let generated_at: DateTime<Utc> = Utc::now();
    let metadata = FreshnessResultsMetadata {
        dbt_schema_version: "https://schemas.getdbt.com/dbt/sources/v3.json".to_string(),
        dbt_version: env!("CARGO_PKG_VERSION").to_string(),
        generated_at,
        invocation_id: invocation_id.to_string(),
        invocation_started_at: None,
        env: dbt_common::constants::collect_dbt_custom_envs(),
    };

    FreshnessResultsArtifact {
        metadata,
        results: freshness_results_to_nodes(resolver_state, results),
        elapsed_time: 0.0,
    }
}

pub fn write_sources_json(
    out_dir: &Path,
    in_dir: &Path,
    sources_artifact: &FreshnessResultsArtifact,
) -> FsResult<()> {
    let results_path = out_dir.join(DBT_SOURCES_JSON);

    let rel_path = pathdiff::diff_paths(&results_path, in_dir)
        .unwrap_or_else(|| results_path.clone())
        .to_string_lossy()
        .into_owned();

    let _sp = create_info_span(ArtifactWritten {
        artifact_type: ArtifactType::Sources as i32,
        relative_path: rel_path,
    })
    .entered();

    let results_file = File::create(results_path)?;

    serde_json::to_writer(results_file, sources_artifact)?;
    emit_info_log_message("Successfully wrote sources.json");
    Ok(())
}

pub fn write_freshness_results_parquet(
    io: &IoArgs,
    resolver_state: &ResolverState,
    results: &BTreeMap<String, FreshnessResult>,
) {
    use dbt_metadata_parquet::runtime_freshness::{FreshnessResultRow, write_freshness_results};

    let freshness_dir = io.out_dir.join("metadata").join("run").join("freshness");

    let ingested_at: i64 = Utc::now().timestamp_nanos_opt().unwrap_or(0);

    let rows: Vec<FreshnessResultRow> = results
        .iter()
        .filter_map(|(unique_id, result)| {
            let source = resolver_state
                .nodes
                .get_node(unique_id)?
                .as_any()
                .downcast_ref::<DbtSource>()?;
            let criteria = source.__source_attr__.freshness.clone().unwrap_or_default();
            Some(FreshnessResultRow {
                invocation_id: io.invocation_id.to_string(),
                unique_id: unique_id.clone(),
                status: result.status.to_string(),
                max_loaded_at: Some(result.max_loaded_at.to_rfc3339()),
                snapshotted_at: Some(result.snapshotted_at.to_rfc3339()),
                max_loaded_at_time_ago: Some(result.age as f64),
                execution_time: None,
                warn_after_count: criteria
                    .warn_after
                    .as_ref()
                    .and_then(|w| w.count.map(|c| c as i32)),
                warn_after_period: criteria
                    .warn_after
                    .as_ref()
                    .and_then(|w| w.period.as_ref().map(|p| p.to_string())),
                error_after_count: criteria
                    .error_after
                    .as_ref()
                    .and_then(|e| e.count.map(|c| c as i32)),
                error_after_period: criteria
                    .error_after
                    .as_ref()
                    .and_then(|e| e.period.as_ref().map(|p| p.to_string())),
                ingested_at,
            })
        })
        .collect();

    if let Err(e) = write_freshness_results(&freshness_dir, &rows) {
        emit_warn_log_message(
            ErrorCode::IoError,
            format!("Failed to write freshness results parquet: {e}"),
        );
    }
}

pub fn emit_freshness_stats(io_args: &IoArgs, results: &BTreeMap<String, FreshnessResult>) {
    if io_args.should_show(ShowOptions::Stats) {
        let info = results
            .iter()
            .map(|(unique_id, result)| format!("{unique_id} {result:?}"))
            .collect::<Vec<_>>()
            .join("\n");
        emit_info_event(
            ShowResult::new_text(info, "stats", "Source freshness stats"),
            None,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_schemas::schemas::common::FreshnessDefinition;
    use dbt_schemas::schemas::nodes::DbtSourceAttr;

    /// A source whose `freshness` resolved to `Some(..)`, i.e. anything that did not opt out
    /// with `config: freshness: null`. `default()` is what a source that configures no freshness
    /// at all resolves to (META-5461).
    fn source_with_freshness(freshness: Option<FreshnessDefinition>) -> DbtSource {
        DbtSource {
            __source_attr__: DbtSourceAttr {
                freshness,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn source_without_any_freshness_config_is_collected() {
        // No `freshness` anywhere in the project resolves to an empty rule, not to `None`
        // (`merge_freshness_unwrapped`, META-5461). These sources are the ones state-aware
        // orchestration tracks off `INFORMATION_SCHEMA.TABLES.LAST_ALTERED`, so they must keep
        // being collected — skipping them would make every dependent model rebuild every run.
        let node = source_with_freshness(Some(FreshnessDefinition::default()));
        assert!(collects_freshness_during_build(&node));
    }

    #[test]
    fn source_with_freshness_rules_is_collected() {
        let node = source_with_freshness(Some(FreshnessDefinition {
            warn_after: Some(FreshnessRules {
                count: Some(12),
                period: Some(FreshnessPeriod::hour),
            }),
            ..Default::default()
        }));
        assert!(collects_freshness_during_build(&node));
    }

    #[test]
    fn source_opted_out_with_freshness_null_is_skipped() {
        // `config: freshness: null` — dbt-labs/dbt-core#14534. Nothing to evaluate, so the
        // source is dropped before any metadata query is issued.
        let node = source_with_freshness(None);
        assert!(!collects_freshness_during_build(&node));
    }

    #[test]
    fn source_opted_out_but_with_loaded_at_field_is_collected() {
        let mut node = source_with_freshness(None);
        node.__source_attr__.loaded_at_field = Some("updated_at".to_string());
        assert!(collects_freshness_during_build(&node));
    }

    #[test]
    fn source_opted_out_but_with_loaded_at_query_is_collected() {
        let mut node = source_with_freshness(None);
        node.__source_attr__.loaded_at_query = Some("select max(updated_at) from t".to_string());
        assert!(collects_freshness_during_build(&node));
    }

    #[test]
    fn empty_loaded_at_strings_do_not_count_as_configured() {
        // `apply_freshness_loaded_at_override` writes `Some("")` for the peer field when only
        // one of `loaded_at_field` / `loaded_at_query` is set, so an empty string must read as
        // "unset" here.
        let mut node = source_with_freshness(None);
        node.__source_attr__.loaded_at_field = Some(String::new());
        node.__source_attr__.loaded_at_query = Some(String::new());
        assert!(!collects_freshness_during_build(&node));
    }
}
