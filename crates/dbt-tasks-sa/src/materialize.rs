use dbt_schemas::schemas::InternalDbtNodeAttributes;
use itertools::{EitherOrBoth, Itertools};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    rc::Rc,
    sync::Arc,
};

use crate::{
    microbatch::BatchContext,
    runnable::microbatch::{extend_microbatch_node_context, render_batch_sql},
};
use arrow::array::{Date32Array, Float32Array};
use arrow::{
    self,
    array::{
        Array, ArrayRef, BooleanArray, Decimal128Array, Float64Array, Int32Array, Int64Array,
        RecordBatch, StringArray, TimestampNanosecondArray, TimestampSecondArray,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    util::pretty::{pretty_format_batches, print_batches},
};
use chrono::DateTime;
use dbt_adapter::Adapter;
use dbt_adapter::adapter::NodeOverride;
use dbt_adapter::connection::drop_thread_local_connection;
use dbt_adapter::relation::{RelationObject, do_create_relation};
use dbt_adapter_core::AdapterType;
use dbt_agate::{AgateTable, MappedSequence, Tuple};
use dbt_common::{
    ErrorCode, FsResult, constants::DBT_COMPILED_DIR_NAME, fs_err, io_args::IoArgs,
    path::get_target_write_path, unexpected_fs_err,
};
use dbt_jinja_utils::{
    jinja_environment::JinjaEnv, listener::JinjaTraceListener, phases::run::build_run_node_context,
};
use dbt_schemas::{
    materialization_resolver::MaterializationResolver,
    schemas::{
        DbtFunction, DbtModel, DbtSeed, DbtSnapshot, DbtTest, DbtUnitTest, InternalDbtNode,
        NodePathKind, Nodes,
        common::{DbtMaterialization, Severity},
    },
    state::{DbtRuntimeConfig, NodeResolverTracker, ResolverState},
};
use dbt_tasks_core::test_aggregation::GenericTestRelationships;
use dbt_telemetry::ExecutionPhase;
use dbt_yaml::Verbatim;
use minijinja::Value;
use minijinja::listener::RenderingEventListener;
use tracing::debug;

/// Macro to handle NULL values in Arrow arrays
macro_rules! null_or {
    ($arr:expr, $index:expr, $value_expr:expr) => {
        if $arr.is_null($index) {
            "NULL".to_string()
        } else {
            $value_expr
        }
    };
}

#[derive(Debug, Clone)]
pub struct CompareRecordBatchResult {
    pub actual_rows: usize,
    pub expected_rows: usize,
    pub diff_batch: RecordBatch,
    pub has_differences: bool,
}

#[allow(clippy::too_many_arguments)]
fn execute_materialization_macro(
    jinja_env: Arc<JinjaEnv>,
    macro_name: &str,
    context: &mut BTreeMap<String, Value>,
    resource_type: &str,
    unique_id: &str,
    node_alias: &str,
    run_path: PathBuf,
) -> FsResult<Value> {
    let macro_string = format!("{macro_name}()");
    let expr = jinja_env.compile_expression(&macro_string)?;
    with_jinja_trace(&run_path, unique_id, |listeners| {
        expr.eval(context, listeners).map_err(|e| {
            if e.code.is_database_error() {
                // Format like dbt-core: show model name and path first, then the raw
                // database error message indented.
                let indented_body = e
                    .context
                    .lines()
                    .map(|line| format!("  {line}"))
                    .collect::<Vec<_>>()
                    .join("\n");
                let message = format!(
                    "Database Error in {resource_type} {node_alias} ({})\n{indented_body}",
                    run_path.display(),
                );
                Box::new(dbt_common::FsError::new(e.code, message))
            } else if e.code == ErrorCode::JinjaWarnUpgradedToError {
                // warn() upgraded to error via warn-error-options: format like dbt-core.
                // emit_warn_log_from_fs_error already reported this to stderr.
                let indented_body = e
                    .context
                    .lines()
                    .map(|line| format!("  {line}"))
                    .collect::<Vec<_>>()
                    .join("\n");
                let message = format!(
                    "Compilation Error in {resource_type} {node_alias} ({})\n{indented_body}",
                    run_path.display(),
                );
                Box::new(dbt_common::FsError::new(e.code, message))
            } else {
                // For non-database errors (macro syntax errors, config errors, etc.)
                // keep the verbose format which helps debug the macro call chain.
                let message = format!(
                    "Error executing materialization macro '{macro_name}' for {resource_type} {unique_id}: {}",
                    e.context
                );
                let err = e.with_location(run_path.clone());
                Box::new(err.with_context(message))
            }
        })
    })
}

fn apply_node_overrides(
    adapter: &Adapter,
    adapter_type: AdapterType,
    custom_warehouse: Option<String>,
    target_database: &str,
    unique_id: &str,
) -> FsResult<Vec<NodeOverride>> {
    let mut node_overrides = Vec::new();
    if let Some(warehouse) = custom_warehouse
        && let Some(node_override) = adapter.use_warehouse(Some(warehouse), unique_id)?
    {
        node_overrides.push(node_override);
    }
    if adapter_type == AdapterType::Redshift
        && let Some(node_override) = adapter.use_database(target_database, unique_id)?
    {
        node_overrides.push(node_override);
    }
    Ok(node_overrides)
}

/// Reset the per-node connection overrides (`USE` database / `use warehouse`) after a
/// materialization. If a reset fails the connection is stuck in the wrong scope, so drop it
/// (do NOT recycle) so no other node inherits it, then fail this node loudly with a clear
/// error; the run continues on other nodes.
///
/// TODO: redundant once the recycling pool is segregated by config fingerprint.
fn reset_node_overrides(
    adapter: &Adapter,
    unique_id: &str,
    targets: &[NodeOverride],
) -> FsResult<()> {
    for target in targets.iter().copied() {
        let result = match target {
            NodeOverride::Database => adapter.reset_database(unique_id).map_err(|e| {
                fs_err!(
                    ErrorCode::ExecutionError,
                    "Failed to reset database context (RESET USE) for node '{unique_id}': {e}"
                )
            }),
            NodeOverride::Warehouse => adapter.restore_warehouse(unique_id).map_err(|e| {
                fs_err!(
                    ErrorCode::ExecutionError,
                    "Failed to restore warehouse for node '{unique_id}': {e}"
                )
            }),
        };

        if let Err(err) = result {
            // A failed reset leaves the connection in the wrong scope: drop it
            // so no other node inherits it.
            drop_thread_local_connection();
            return Err(err);
        }
    }
    Ok(())
}

fn with_jinja_trace<F, T>(compiled_path: &Path, unique_id: &str, f: F) -> FsResult<T>
where
    F: FnOnce(&[Rc<dyn RenderingEventListener>]) -> FsResult<T>,
{
    let trace_listener =
        if dbt_adapter::time_machine::is_replaying() || dbt_adapter::time_machine::is_recording() {
            Some(Rc::new(JinjaTraceListener::new()))
        } else {
            None
        };
    let listeners: Vec<Rc<dyn RenderingEventListener>> = trace_listener
        .iter()
        .map(|l| l.clone() as Rc<dyn RenderingEventListener>)
        .collect();

    f(&listeners).inspect_err(|_| {
        if let Some(ref listener) = trace_listener {
            dump_jinja_trace(listener, compiled_path, unique_id);
        }
    })
}

fn dump_jinja_trace(listener: &JinjaTraceListener, compiled_path: &Path, unique_id: &str) {
    if listener.is_empty() {
        return;
    }
    let trace = listener.format_trace();
    let sanitized = unique_id.replace(['/', '\\', '.'], "_");
    let filename = format!("jinja_trace_{sanitized}.txt");
    if let Some(dir) = compiled_path.parent() {
        let path = dir.join(&filename);
        if std::fs::write(&path, &trace).is_ok() {
            eprintln!("Jinja trace dump written to: {}", path.display());
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeHookPhase {
    Pre,
    Post,
}

/// Describes how a node's materialization macro invokes pre/post hooks.
///
/// Reuse paths run hooks outside the normal materialization macro, so they must
/// mirror the adapter macro's hook shape to avoid reuse-only hook side effects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeHookStyle {
    SplitTransaction,
    Plain,
}

/// Returns the hook invocation style used by a model's adapter materialization.
pub fn model_hook_style(
    adapter_type: AdapterType,
    materialization: &DbtMaterialization,
) -> NodeHookStyle {
    use AdapterType::*;
    use DbtMaterialization::*;

    match (adapter_type, materialization) {
        (Bigquery | Snowflake | Spark | Databricks, View | Table | Incremental) => {
            NodeHookStyle::Plain
        }
        (Snowflake, DynamicTable) => NodeHookStyle::Plain,
        _ => NodeHookStyle::SplitTransaction,
    }
}

fn node_hook_expression(
    context: &BTreeMap<String, Value>,
    style: NodeHookStyle,
    phase: NodeHookPhase,
) -> Option<&'static str> {
    match style {
        NodeHookStyle::Plain => match phase {
            NodeHookPhase::Pre => context
                .contains_key("pre_hooks")
                .then_some("run_hooks(pre_hooks)"),
            NodeHookPhase::Post => context
                .contains_key("post_hooks")
                .then_some("run_hooks(post_hooks)"),
        },
        NodeHookStyle::SplitTransaction => match phase {
            NodeHookPhase::Pre => context.contains_key("pre_hooks").then_some(
                "run_hooks(pre_hooks, inside_transaction=False) ~ run_hooks(pre_hooks, inside_transaction=True)",
            ),
            NodeHookPhase::Post => {
                if context.contains_key("post_hooks") {
                    Some(
                        "run_hooks(post_hooks, inside_transaction=True) ~ adapter.commit() ~ run_hooks(post_hooks, inside_transaction=False)",
                    )
                } else {
                    Some("adapter.commit()")
                }
            }
        },
    }
}

#[allow(clippy::too_many_arguments)]
pub fn execute_node_hooks<S: serde::Serialize>(
    node: &dyn InternalDbtNode,
    deprecated_config: &S,
    adapter_type: AdapterType,
    runtime_config: &DbtRuntimeConfig,
    jinja_env: Arc<JinjaEnv>,
    base_context: &BTreeMap<String, Value>,
    io_args: &IoArgs,
    sql: Option<&str>,
    style: NodeHookStyle,
    error_path_kind: NodePathKind,
    phase: NodeHookPhase,
) -> FsResult<()> {
    let mut context = build_run_node_context(
        node,
        deprecated_config,
        adapter_type,
        None,
        base_context,
        io_args,
        ExecutionPhase::Run,
        None,
        runtime_config.dependencies.keys().cloned().collect(),
    );

    if let Some(sql) = sql {
        context.insert("sql".to_string(), Value::from(sql));
        context.insert("compiled_code".to_string(), Value::from(sql));
    }

    let hook_name = match phase {
        NodeHookPhase::Pre => "pre_hooks",
        NodeHookPhase::Post => "post_hooks",
    };
    let Some(hook_expression) = node_hook_expression(&context, style, phase) else {
        return Ok(());
    };
    let expr = jinja_env.compile_expression(hook_expression)?;
    expr.eval(&context, &[]).map(|_| ()).map_err(|e| {
        let resource_type = node.resource_type().as_static_ref();
        let message = format!(
            "Error executing {hook_name} for {resource_type} {}: {}",
            node.common().unique_id,
            e.context
        );
        Box::new(
            e.with_location(node.get_node_path_abs(
                error_path_kind,
                &io_args.in_dir,
                &io_args.out_dir,
            ))
            .with_context(message),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::{NodeHookPhase, NodeHookStyle, model_hook_style, node_hook_expression};
    use dbt_adapter_core::AdapterType;
    use dbt_schemas::schemas::common::DbtMaterialization;
    use minijinja::Value;
    use std::collections::BTreeMap;

    #[test]
    fn split_transaction_post_hooks_commit_even_when_post_hooks_are_absent() {
        let mut context = BTreeMap::new();
        context.insert("pre_hooks".to_string(), Value::from(Vec::<Value>::new()));

        assert_eq!(
            node_hook_expression(
                &context,
                NodeHookStyle::SplitTransaction,
                NodeHookPhase::Post
            ),
            Some("adapter.commit()")
        );
    }

    #[test]
    fn split_transaction_post_hooks_preserve_post_hook_execution_when_configured() {
        let mut context = BTreeMap::new();
        context.insert("post_hooks".to_string(), Value::from(Vec::<Value>::new()));

        assert_eq!(
            node_hook_expression(
                &context,
                NodeHookStyle::SplitTransaction,
                NodeHookPhase::Post
            ),
            Some(
                "run_hooks(post_hooks, inside_transaction=True) ~ adapter.commit() ~ run_hooks(post_hooks, inside_transaction=False)"
            )
        );
    }

    #[test]
    fn split_transaction_pre_hooks_skip_when_pre_hooks_are_absent() {
        assert_eq!(
            node_hook_expression(
                &BTreeMap::new(),
                NodeHookStyle::SplitTransaction,
                NodeHookPhase::Pre
            ),
            None
        );
    }

    #[test]
    fn plain_hooks_match_non_transactional_adapter_materializations() {
        let mut context = BTreeMap::new();
        context.insert("pre_hooks".to_string(), Value::from(Vec::<Value>::new()));
        context.insert("post_hooks".to_string(), Value::from(Vec::<Value>::new()));

        assert_eq!(
            node_hook_expression(&context, NodeHookStyle::Plain, NodeHookPhase::Pre),
            Some("run_hooks(pre_hooks)")
        );
        assert_eq!(
            node_hook_expression(&context, NodeHookStyle::Plain, NodeHookPhase::Post),
            Some("run_hooks(post_hooks)")
        );
    }

    #[test]
    fn plain_post_hooks_skip_when_post_hooks_are_absent() {
        assert_eq!(
            node_hook_expression(&BTreeMap::new(), NodeHookStyle::Plain, NodeHookPhase::Post),
            None
        );
    }

    #[test]
    fn model_hook_style_matches_adapter_materialization_macros() {
        assert_eq!(
            model_hook_style(AdapterType::Bigquery, &DbtMaterialization::Table),
            NodeHookStyle::Plain
        );
        assert_eq!(
            model_hook_style(AdapterType::Snowflake, &DbtMaterialization::Incremental),
            NodeHookStyle::Plain
        );
        assert_eq!(
            model_hook_style(AdapterType::Spark, &DbtMaterialization::View),
            NodeHookStyle::Plain
        );
        assert_eq!(
            model_hook_style(AdapterType::Databricks, &DbtMaterialization::Table),
            NodeHookStyle::Plain
        );
        assert_eq!(
            model_hook_style(AdapterType::Redshift, &DbtMaterialization::Table),
            NodeHookStyle::SplitTransaction
        );
        assert_eq!(
            model_hook_style(AdapterType::Bigquery, &DbtMaterialization::MaterializedView),
            NodeHookStyle::SplitTransaction
        );
    }
}

#[allow(clippy::too_many_arguments)]
pub fn materialize_clone<S: serde::Serialize>(
    node: &dyn InternalDbtNode,
    deprecated_config: &S,
    adapter_type: AdapterType,
    runtime_config: &DbtRuntimeConfig,
    defer_nodes: Option<&Nodes>,
    materialization_resolver: &Arc<MaterializationResolver>,
    jinja_env: Arc<JinjaEnv>,
    base_context: &BTreeMap<String, Value>,
    io_args: &IoArgs,
    custom_warehouse: Option<String>,
) -> FsResult<Value> {
    let mut context = build_run_node_context(
        node,
        &deprecated_config,
        adapter_type,
        None,
        base_context,
        io_args,
        ExecutionPhase::Run,
        None,
        runtime_config.dependencies.keys().cloned().collect(),
    );

    let macro_name = materialization_resolver.find_materialization_macro_by_name("clone")?;

    let unique_id = node.common().unique_id.clone();
    let defer_option = defer_nodes
        .as_ref()
        .and_then(|nodes| nodes.get_node(&unique_id));

    if let Some(defer) = defer_option {
        let relation: Arc<dyn dbt_schemas::schemas::relations::base::BaseRelation> = Arc::from(
            do_create_relation(
                adapter_type,
                defer.base().database.clone(),
                defer.base().schema.clone(),
                Some(defer.base().alias.clone()),
                None,
                defer.quoting(),
            )
            .unwrap(),
        );

        context.insert(
            "defer_relation".to_string(),
            RelationObject::new(Arc::clone(&relation)).into_value(),
        );

        let sql = format!("SELECT * FROM {}", relation.render_self_as_str());

        context.insert("sql".to_string(), Value::from(&sql));
        context.insert("compiled_code".to_string(), Value::from(&sql));
    }

    let adapter = jinja_env
        .get_base_adapter()
        .ok_or_else(|| unexpected_fs_err!("No adapter found for model {}", &unique_id))?;
    let node_alias = node.base().alias.clone();
    // Runtime-phase errors report the run/Executable path per the path-requirements matrix.
    let run_path = node
        .get_node_path(NodePathKind::Executable, &io_args.in_dir, &io_args.out_dir)
        .into_owned();

    let node_overrides = apply_node_overrides(
        &adapter,
        adapter_type,
        custom_warehouse,
        &node.base().database,
        &unique_id,
    )?;

    let res = execute_materialization_macro(
        jinja_env,
        &macro_name,
        &mut context,
        "clone",
        &unique_id,
        &node_alias,
        run_path,
    );

    reset_node_overrides(&adapter, &unique_id, &node_overrides)?;
    res
}
#[allow(clippy::too_many_arguments)]
pub fn materialize_seed(
    seed: &DbtSeed,
    adapter_type: AdapterType,
    runtime_config: &DbtRuntimeConfig,
    materialization_resolver: &Arc<MaterializationResolver>,
    jinja_env: Arc<JinjaEnv>,
    base_context: &BTreeMap<String, Value>,
    agate_table: AgateTable,
    io_args: &IoArgs,
) -> FsResult<Value> {
    let macro_name = materialization_resolver.find_materialization_macro_by_name("seed")?;

    let mut context = build_run_node_context(
        seed,
        &seed.deprecated_config,
        adapter_type,
        Some(agate_table),
        base_context,
        io_args,
        ExecutionPhase::Run,
        None,
        runtime_config.dependencies.keys().cloned().collect(),
    );

    let run_path = seed
        .get_node_path(NodePathKind::Executable, &io_args.in_dir, &io_args.out_dir)
        .into_owned();

    execute_materialization_macro(
        jinja_env,
        &macro_name,
        &mut context,
        "seed",
        &seed.__common_attr__.unique_id,
        &seed.__base_attr__.alias,
        run_path,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn materialize_model(
    sql: &str,
    model: &DbtModel,
    adapter_type: AdapterType,
    runtime_config: &DbtRuntimeConfig,
    materialization_resolver: &Arc<MaterializationResolver>,
    jinja_env: Arc<JinjaEnv>,
    base_context: &BTreeMap<String, Value>,
    io_args: &IoArgs,
    sql_header: Option<Value>,
) -> FsResult<Value> {
    // get materialization
    let mut context = build_run_node_context(
        model,
        &model.deprecated_config,
        adapter_type,
        None,
        base_context,
        io_args,
        ExecutionPhase::Run,
        sql_header,
        runtime_config.dependencies.keys().cloned().collect(),
    );
    let materialization = model.__base_attr__.materialized.clone();

    let macro_name = materialization_resolver
        .find_materialization_macro_by_name(&materialization.to_string())?;
    context.insert("sql".to_string(), Value::from(sql));
    context.insert("compiled_code".to_string(), Value::from(sql));

    let unique_id = model.__common_attr__.unique_id.clone();
    let node_alias = model.__base_attr__.alias.clone();
    // Runtime-phase errors report the run/Executable path per the path-requirements matrix.
    let run_path = model
        .get_node_path(NodePathKind::Executable, &io_args.in_dir, &io_args.out_dir)
        .into_owned();

    let adapter = jinja_env.get_base_adapter().ok_or_else(|| {
        unexpected_fs_err!(
            "No adapter found for model {}",
            model.__common_attr__.unique_id
        )
    })?;

    let custom_warehouse = if let Some(snowflake_attr) = &model.__adapter_attr__.snowflake_attr {
        snowflake_attr.snowflake_warehouse.clone()
    } else {
        None
    };

    let node_overrides = apply_node_overrides(
        &adapter,
        adapter_type,
        custom_warehouse,
        &model.__base_attr__.database,
        &unique_id,
    )?;

    let result = execute_materialization_macro(
        jinja_env,
        &macro_name,
        &mut context,
        "model",
        &unique_id,
        &node_alias,
        run_path,
    );

    reset_node_overrides(&adapter, &unique_id, &node_overrides)?;
    result
}

/// Checks whether the latest version pointer should be created for this model.
///
/// Returns `true` when:
/// - The model is versioned (`version` is set)
/// - It is the latest version (`version == latest_version`)
/// - `latest_version_pointer.enabled` is explicitly `true`, OR `enabled` is `None` and
///   the project flag `latest_version_pointer_enabled_by_default` is `true`
pub fn should_create_latest_version_pointer(
    model: &DbtModel,
    runtime_config: &DbtRuntimeConfig,
) -> bool {
    let version = match &model.__model_attr__.version {
        Some(v) => v,
        None => return false,
    };
    let latest_version = match &model.__model_attr__.latest_version {
        Some(v) => v,
        None => return false,
    };
    if version.to_string() != latest_version.to_string() {
        return false;
    }
    model
        .deprecated_config
        .latest_version_pointer
        .as_ref()
        .and_then(|p| p.enabled)
        .unwrap_or(
            runtime_config
                .inner
                .latest_version_pointer_enabled_by_default,
        )
}

/// Determines the pointer view identifier by calling
/// `generate_latest_version_pointer_alias(custom_alias_name, model)`.
///
/// The default macro (shipped in dbt-adapters) returns `custom_alias_name`
/// if set, otherwise the unsuffixed model name. Users can override this macro
/// in their project to customize pointer naming globally.
fn latest_version_pointer_identifier(
    model: &DbtModel,
    jinja_env: &JinjaEnv,
    context: &mut BTreeMap<String, Value>,
) -> FsResult<String> {
    let custom_alias = model
        .deprecated_config
        .latest_version_pointer
        .as_ref()
        .and_then(|p| p.alias.as_ref())
        .map(|a| a.trim().to_string())
        .filter(|a| !a.is_empty());

    let alias_arg = custom_alias
        .as_deref()
        .map(Value::from)
        .unwrap_or_else(|| Value::from(()));
    context.insert("__lvp_custom_alias__".to_string(), alias_arg);

    let macro_template = "{{ generate_latest_version_pointer_alias(__lvp_custom_alias__, model) }}";
    let render_result = jinja_env.render_str(macro_template, &mut *context, &[]);
    context.remove("__lvp_custom_alias__");

    let rendered = render_result.map_err(|e| {
        fs_err!(
            ErrorCode::Generic,
            "Failed to render `generate_latest_version_pointer_alias` macro for model '{}': {}. \
             Check your project's macro override for syntax errors.",
            model.__common_attr__.unique_id,
            e
        )
    })?;

    let trimmed = rendered.trim();
    if trimmed.is_empty() {
        return Err(fs_err!(
            ErrorCode::InvalidConfig,
            "`generate_latest_version_pointer_alias` returned an empty string for model '{}'. \
             The macro must return a non-empty identifier.",
            model.__common_attr__.unique_id,
        ));
    }
    Ok(trimmed.to_string())
}

/// After the latest version of a versioned model materializes, create a view
/// at the unsuffixed name (or custom alias) pointing to the latest version.
///
/// This mirrors dbt-core's `_materialize_latest_version_view` behavior: it
/// creates a synthetic context for the view materialization macro, pointing
/// the `this` relation at the pointer identifier with SQL = `SELECT * FROM <latest>`.
///
/// The pointer identifier is resolved via `generate_latest_version_pointer_alias`
/// macro if defined, otherwise falls back to `config.alias` or the unsuffixed name.
#[allow(clippy::too_many_arguments)]
pub fn materialize_latest_version_pointer(
    model: &DbtModel,
    adapter_type: AdapterType,
    runtime_config: &DbtRuntimeConfig,
    materialization_resolver: &Arc<MaterializationResolver>,
    jinja_env: Arc<JinjaEnv>,
    base_context: &BTreeMap<String, Value>,
    io_args: &IoArgs,
) -> FsResult<Value> {
    // Build a context from the original model first so macros can access `model`
    let mut resolve_context = build_run_node_context(
        model,
        &model.deprecated_config,
        adapter_type,
        None,
        base_context,
        io_args,
        ExecutionPhase::Run,
        None,
        runtime_config.dependencies.keys().cloned().collect(),
    );

    let pointer_identifier =
        latest_version_pointer_identifier(model, &jinja_env, &mut resolve_context)?;
    let model_alias = &model.__base_attr__.alias;

    // Build the source relation (the versioned model's actual relation in the warehouse).
    // This is pure in-memory construction (no warehouse round-trip).
    let source_relation = do_create_relation(
        adapter_type,
        model.__base_attr__.database.clone(),
        model.__base_attr__.schema.clone(),
        Some(model_alias.clone()),
        None,
        model.__base_attr__.quoting,
    )
    .map_err(|e| {
        fs_err!(
            ErrorCode::Generic,
            "Failed to create source relation for latest version pointer: {}",
            e
        )
    })?;

    // Collision check: compare source (latest version) and pointer as the warehouse
    // would resolve them, not as raw strings. `identifier_as_resolved_str` applies the
    // adapter's quoting/casing policy, so an unquoted alias like `DIM_CUSTOMERS` and a
    // pointer name `dim_customers` are correctly detected as the same relation on
    // case-insensitive adapters (e.g. Snowflake).
    let pointer_relation = do_create_relation(
        adapter_type,
        model.__base_attr__.database.clone(),
        model.__base_attr__.schema.clone(),
        Some(pointer_identifier.clone()),
        None,
        model.__base_attr__.quoting,
    )
    .map_err(|e| {
        fs_err!(
            ErrorCode::Generic,
            "Failed to create pointer relation for latest version pointer: {}",
            e
        )
    })?;
    let source_identifier = source_relation.identifier_as_resolved_str().map_err(|e| {
        fs_err!(
            ErrorCode::Generic,
            "Failed to resolve source identifier: {}",
            e
        )
    })?;
    let pointer_resolved_identifier =
        pointer_relation.identifier_as_resolved_str().map_err(|e| {
            fs_err!(
                ErrorCode::Generic,
                "Failed to resolve pointer identifier: {}",
                e
            )
        })?;
    if source_identifier == pointer_resolved_identifier {
        return Err(fs_err!(
            ErrorCode::InvalidConfig,
            "Cannot create latest version pointer: the latest version of '{}' \
             is already aliased to '{}'. Set `latest_version_pointer: {{enabled: false}}` \
             or remove the conflicting alias.",
            model.__common_attr__.name,
            pointer_identifier,
        ));
    }

    let source_relation_str = source_relation.render_self_as_str();

    let pointer_sql = format!("SELECT * FROM {source_relation_str}");

    // Build a synthetic model with the pointer's alias and view materialization.
    // Clone the model and override:
    //   - alias → pointer identifier
    //   - materialization → "view"
    //   - hooks cleared (pre/post hooks should not run for the pointer)
    //   - persist_docs cleared (avoid duplicating doc persistence)
    let mut pointer_model = model.clone();
    pointer_model.__base_attr__.alias = pointer_identifier.clone();
    pointer_model.__base_attr__.materialized = DbtMaterialization::View;
    pointer_model.deprecated_config.materialized = Some(DbtMaterialization::View);
    pointer_model.deprecated_config.pre_hook = Verbatim::from(None);
    pointer_model.deprecated_config.post_hook = Verbatim::from(None);
    pointer_model.deprecated_config.persist_docs = None;

    debug!(
        "Creating latest version pointer view '{}' -> '{}' for model '{}'",
        pointer_identifier, model_alias, model.__common_attr__.unique_id
    );

    let mut context = build_run_node_context(
        &pointer_model,
        &pointer_model.deprecated_config,
        adapter_type,
        None,
        base_context,
        io_args,
        ExecutionPhase::Run,
        None,
        runtime_config.dependencies.keys().cloned().collect(),
    );

    let macro_name = materialization_resolver.find_materialization_macro_by_name("view")?;
    context.insert("sql".to_string(), Value::from(pointer_sql.as_str()));
    context.insert(
        "compiled_code".to_string(),
        Value::from(pointer_sql.as_str()),
    );

    let unique_id = format!(
        "{}__latest_version_pointer",
        model.__common_attr__.unique_id
    );

    let run_path = model
        .get_node_path(NodePathKind::Executable, &io_args.in_dir, &io_args.out_dir)
        .into_owned();

    execute_materialization_macro(
        jinja_env,
        &macro_name,
        &mut context,
        "model",
        &unique_id,
        &pointer_identifier,
        run_path,
    )
}

/// Executes a single batch of a microbatch model.
#[allow(clippy::too_many_arguments)]
pub fn materialize_microbatch_model(
    sql_template: &str,
    model: &DbtModel,
    node_resolver: Arc<dyn NodeResolverTracker>,
    runtime_config: &DbtRuntimeConfig,
    materialization_resolver: &Arc<MaterializationResolver>,
    jinja_env: Arc<JinjaEnv>,
    batch_ctx: &BatchContext,
    adapter_type: AdapterType,
    mut run_node_context: BTreeMap<String, Value>,
    event_time_mapping: Arc<BTreeMap<String, String>>,
    io_args: &IoArgs,
) -> FsResult<Value> {
    // Microbatch base context is shared
    extend_microbatch_node_context(
        batch_ctx,
        adapter_type,
        model,
        node_resolver,
        runtime_config,
        &mut run_node_context,
        event_time_mapping,
    );

    // Re-render the SQL template to get batch-filtered SQL
    // The template should use {{ ref(...) }} which will now be filtered
    let batch_sql = render_batch_sql(
        sql_template,
        jinja_env.clone(),
        &run_node_context,
        &io_args.out_dir,
    )?;

    // Insert the batch SQL into context
    run_node_context.insert("sql".to_string(), Value::from(batch_sql.as_str()));
    run_node_context.insert("compiled_code".to_string(), Value::from(batch_sql.as_str()));

    // Get the incremental materialization macro
    let macro_name = materialization_resolver
        .find_materialization_macro_by_name(&DbtMaterialization::Incremental.to_string())?;

    let adapter = jinja_env.get_base_adapter().ok_or_else(|| {
        fs_err!(
            ErrorCode::Generic,
            "No adapter found for microbatch model {}",
            batch_ctx.id,
        )
    })?;

    let custom_warehouse = if let Some(snowflake_attr) = &model.__adapter_attr__.snowflake_attr {
        snowflake_attr.snowflake_warehouse.clone()
    } else {
        None
    };

    let node_alias = model.__base_attr__.alias.clone();
    // Runtime-phase errors report the run/Executable path per the path-requirements matrix.
    let run_path = model
        .get_node_path(NodePathKind::Executable, &io_args.in_dir, &io_args.out_dir)
        .into_owned();
    let unique_id = model.__common_attr__.unique_id.clone();

    let node_overrides = apply_node_overrides(
        &adapter,
        adapter_type,
        custom_warehouse,
        &model.__base_attr__.database,
        &unique_id,
    )?;

    let result = execute_materialization_macro(
        jinja_env,
        &macro_name,
        &mut run_node_context,
        "model",
        &unique_id,
        &node_alias,
        run_path,
    );

    reset_node_overrides(&adapter, &unique_id, &node_overrides)?;
    result
}

#[allow(clippy::too_many_arguments)]
pub fn materialize_snapshot(
    sql: &str,
    snapshot: &DbtSnapshot,
    adapter_type: AdapterType,
    runtime_config: &DbtRuntimeConfig,
    materialization_resolver: &Arc<MaterializationResolver>,
    jinja_env: Arc<JinjaEnv>,
    base_context: &BTreeMap<String, Value>,
    io_args: &IoArgs,
) -> FsResult<Value> {
    // get materialization
    let mut snapshot = snapshot.clone();
    snapshot.compiled = Some(true);
    snapshot.compiled_code = Some(sql.to_string());

    let mut context = build_run_node_context(
        &snapshot,
        &snapshot.serialized_config(),
        adapter_type,
        None,
        base_context,
        io_args,
        ExecutionPhase::Run,
        None,
        runtime_config.dependencies.keys().cloned().collect(),
    );

    context.insert("sql".to_string(), Value::from(sql));
    context.insert("compiled_code".to_string(), Value::from(sql));

    let macro_name = materialization_resolver.find_materialization_macro_by_name("snapshot")?;

    let unique_id = snapshot.__common_attr__.unique_id.clone();
    let node_alias = snapshot.__base_attr__.alias.clone();
    // Runtime-phase errors report the run/Executable path per the path-requirements matrix.
    let run_path = snapshot
        .get_node_path(NodePathKind::Executable, &io_args.in_dir, &io_args.out_dir)
        .into_owned();

    let adapter = jinja_env.get_base_adapter().ok_or_else(|| {
        unexpected_fs_err!(
            "No adapter found for snapshot {}",
            snapshot.__common_attr__.unique_id
        )
    })?;

    let custom_warehouse = if let Some(snowflake_attr) = &snapshot.__adapter_attr__.snowflake_attr {
        snowflake_attr.snowflake_warehouse.clone()
    } else {
        None
    };

    let node_overrides = apply_node_overrides(
        &adapter,
        adapter_type,
        custom_warehouse,
        &snapshot.__base_attr__.database,
        &unique_id,
    )?;

    let result = execute_materialization_macro(
        jinja_env,
        &macro_name,
        &mut context,
        "snapshot",
        &unique_id,
        &node_alias,
        run_path,
    );

    reset_node_overrides(&adapter, &unique_id, &node_overrides)?;
    result
}

pub fn materialize_unit_test(
    sql: &str,
    unit_test: &DbtUnitTest,
    resolver_state: Arc<ResolverState>,
    materialization_resolver: Arc<MaterializationResolver>,
    jinja_env: Arc<JinjaEnv>,
    base_context: &BTreeMap<String, Value>,
    io_args: &IoArgs,
) -> FsResult<bool> {
    let adapter_type = resolver_state.adapter_type;
    let mut context = build_run_node_context(
        unit_test,
        &unit_test.deprecated_config,
        adapter_type,
        None,
        base_context,
        io_args,
        ExecutionPhase::Run,
        None,
        resolver_state
            .runtime_config
            .dependencies
            .keys()
            .cloned()
            .collect(),
    );
    let materialization = DbtMaterialization::Unit;
    let macro_name = materialization_resolver
        .find_materialization_macro_by_name(&materialization.to_string())?;

    context.insert("sql".to_string(), Value::from(sql));
    context.insert("compiled_code".to_string(), Value::from(sql));

    // Compiled path of the unit test itself (the .sql we wrote when compiling). The
    // helper yields target/compiled/<package>/<dir-of-yaml>/<yaml-filename>; we then
    // swap in <unit_test_name>.sql to match the actual on-disk artifact.
    let compiled_path = get_target_write_path(
        &io_args.in_dir,
        &io_args.out_dir.join(DBT_COMPILED_DIR_NAME),
        &unit_test.__common_attr__.package_name,
        &unit_test.__common_attr__.path,
        &unit_test.__common_attr__.original_file_path,
    )
    .with_file_name(format!("{}.sql", unit_test.__common_attr__.name));

    let _ = with_jinja_trace(
        &compiled_path,
        &unit_test.__common_attr__.unique_id,
        |listeners| {
            jinja_env
                .render_str(
                    &format!("{{{{ {macro_name}() }}}}"),
                    &mut context,
                    listeners,
                )
                .map_err(|e| {
                    Box::new(
                        fs_err!(
                            ErrorCode::JinjaError,
                            "Error materializing unit test {}: {}",
                            unit_test.__common_attr__.unique_id,
                            e
                        )
                        .with_location(compiled_path.clone()),
                    )
                })
        },
    )?;

    let expr = jinja_env.compile_expression("load_result('main').table")?;
    let table = expr
        .eval(&context, &[])
        .unwrap()
        .downcast_object::<AgateTable>()
        .unwrap();
    // print_batches(&[table.to_record_batch().as_ref().clone()])?;
    let CompareRecordBatchResult {
        has_differences,
        diff_batch,
        ..
    } = compare_record_batches(table.original_record_batch().as_ref())?;
    if has_differences {
        print_batches(&[diff_batch])?;
    }

    Ok(!has_differences)
}

pub fn materialize_unit_test_fast_pass(
    sql: &str,
    unit_test: &DbtUnitTest,
    adapter_type: AdapterType,
    runtime_config: &DbtRuntimeConfig,
    jinja_env: Arc<JinjaEnv>,
    base_context: &BTreeMap<String, Value>,
    io_args: &IoArgs,
) -> FsResult<(bool, usize, String)> {
    let mut context = build_run_node_context(
        unit_test,
        &unit_test.deprecated_config,
        adapter_type,
        None,
        base_context,
        io_args,
        ExecutionPhase::Run,
        None,
        runtime_config.dependencies.keys().cloned().collect(),
    );

    context.insert("sql".to_string(), Value::from(sql));

    let materialization = r#"
    {% set res, table = adapter.execute(sql, fetch=True) %}
    {% do store_result('main', response=res, agate_table=table) %}
"#;

    // Compiled path of the unit test itself; same construction as materialize_unit_test.
    let compiled_path = get_target_write_path(
        &io_args.in_dir,
        &io_args.out_dir.join(DBT_COMPILED_DIR_NAME),
        &unit_test.__common_attr__.package_name,
        &unit_test.__common_attr__.path,
        &unit_test.__common_attr__.original_file_path,
    )
    .with_file_name(format!("{}.sql", unit_test.__common_attr__.name));

    let _render_str = with_jinja_trace(
        &compiled_path,
        &unit_test.__common_attr__.unique_id,
        |listeners| {
            jinja_env
                .render_str(materialization, &mut context, listeners)
                .map_err(|e| {
                    Box::new(
                        fs_err!(
                            ErrorCode::JinjaError,
                            "Error materializing unit test {}: {}",
                            unit_test.__common_attr__.unique_id,
                            e
                        )
                        .with_location(compiled_path.clone()),
                    )
                })
        },
    )?;

    let expr = jinja_env.compile_expression("load_result('main').table")?;
    let table = expr
        .eval(&context, &[])
        .unwrap()
        .downcast_object::<AgateTable>()
        .unwrap();
    let CompareRecordBatchResult {
        has_differences,
        diff_batch,
        actual_rows,
        expected_rows,
    } = compare_record_batches(table.to_record_batch().as_ref())?;
    let diff_num_rows = diff_batch.num_rows();

    Ok((!has_differences, diff_num_rows, {
        let mut s = pretty_format_batches(&[diff_batch])?.to_string();
        s.push('\n');
        s.push_str(&format!("{diff_num_rows} row(s) differ."));

        if actual_rows != expected_rows {
            s.push_str(&format!(
                "\nExpected {expected_rows} row(s), got {actual_rows} row(s)."
            ))
        }

        s
    }))
}

#[derive(Debug)]
pub struct TestResult {
    pub column_name: Option<String>,
    pub failures: i64,
    pub should_warn: bool,
    pub should_error: bool,
}

impl TestResult {
    pub fn new(
        column_name: Option<String>,
        failures: i64,
        should_warn: bool,
        should_error: bool,
    ) -> Self {
        TestResult {
            column_name,
            failures,
            should_warn,
            should_error,
        }
    }
}

fn get_test_results(table: &AgateTable) -> FsResult<Vec<TestResult>> {
    let column_names = table.column_names();
    let column_name_idx = column_names
        .iter()
        .position(|n| n.eq_ignore_ascii_case("column_name"));
    let failures_idx = column_names
        .iter()
        .position(|n| n.eq_ignore_ascii_case("failures"));
    let should_warn_idx = column_names
        .iter()
        .position(|n| n.eq_ignore_ascii_case("should_warn"));
    let should_error_idx = column_names
        .iter()
        .position(|n| n.eq_ignore_ascii_case("should_error"));

    if column_name_idx.is_some() {
        let mut results = Vec::new();
        for row in 0..table.num_rows() {
            results.push(get_column_test_result(
                &table.columns().values(),
                row,
                column_name_idx,
                failures_idx,
                should_warn_idx,
                should_error_idx,
            ));
        }
        Ok(results)
    } else {
        if table.num_rows() != 1 || table.num_columns() != 3 {
            return Err(fs_err!(
                ErrorCode::Unexpected,
                "Test result table should have 1 row and 3 columns, but got {} rows and {} columns",
                table.num_rows(),
                table.num_columns()
            ));
        }

        let columns: Tuple = table.columns().values();
        let failures = columns.get(0).unwrap().get_item_by_index(0).ok();
        let should_warn = columns.get(1).unwrap().get_item_by_index(0).ok();
        let should_error = columns.get(2).unwrap().get_item_by_index(0).ok();

        let failures_val = failures.and_then(|v| v.as_i64()).unwrap_or(-1);
        let should_warn_val = should_warn.map(|v| v.is_true()).unwrap_or(false);
        let should_error_val = should_error.map(|v| v.is_true()).unwrap_or(false);

        Ok(vec![TestResult {
            column_name: None,
            failures: failures_val,
            should_warn: should_warn_val,
            should_error: should_error_val,
        }])
    }
}

fn get_column_test_result(
    values: &Tuple,
    row: usize,
    column_name_idx: Option<usize>,
    failures_idx: Option<usize>,
    should_warn_idx: Option<usize>,
    should_error_idx: Option<usize>,
) -> TestResult {
    let column_name = get_cell_value(values, row, column_name_idx)
        .and_then(|v| v.as_str().map(|s| s.to_string()));

    let failures = get_cell_value(values, row, failures_idx)
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    let should_warn = get_cell_value(values, row, should_warn_idx)
        .map(|v| v.is_true())
        .unwrap_or(false);

    let should_error = get_cell_value(values, row, should_error_idx)
        .map(|v| v.is_true())
        .unwrap_or(false);

    TestResult {
        column_name,
        failures,
        should_warn,
        should_error,
    }
}

fn get_cell_value(values: &Tuple, row: usize, column: Option<usize>) -> Option<Value> {
    column
        .and_then(|idx| values.get(idx as isize))
        .and_then(|col| col.get_item_by_index(row).ok())
}

#[allow(clippy::too_many_arguments)]
pub fn materialize_test(
    sql: &str,
    test: &DbtTest,
    relationships: &GenericTestRelationships,
    adapter_type: AdapterType,
    runtime_config: &DbtRuntimeConfig,
    materialization_resolver: &Arc<MaterializationResolver>,
    jinja_env: Arc<JinjaEnv>,
    base_context: &BTreeMap<String, Value>,
    io_args: &IoArgs,
) -> FsResult<(Vec<TestResult>, Option<RecordBatch>)> {
    let packages = runtime_config.dependencies.keys().cloned().collect();
    let mut context = build_run_node_context(
        test,
        &test.deprecated_config,
        adapter_type,
        None,
        base_context,
        io_args,
        ExecutionPhase::Run,
        None,
        packages,
    );

    let is_aggregated = relationships.unique_ids.contains_key(&test.common().name);
    let materialization_name = if is_aggregated {
        "aggregated_test"
    } else {
        "test"
    };
    let macro_name =
        materialization_resolver.find_materialization_macro_by_name(materialization_name)?;

    context.insert("sql".to_string(), Value::from(sql));

    let compiled_path = get_target_write_path(
        &io_args.in_dir,
        &io_args.out_dir.join(DBT_COMPILED_DIR_NAME),
        &test.__common_attr__.package_name,
        &test.__common_attr__.path,
        &test.__common_attr__.original_file_path,
    );

    let adapter = jinja_env.get_base_adapter().ok_or_else(|| {
        unexpected_fs_err!(
            "No adapter found for test {}",
            test.__common_attr__.unique_id
        )
    })?;

    let custom_warehouse = if let Some(snowflake_attr) = &test.__adapter_attr__.snowflake_attr {
        snowflake_attr.snowflake_warehouse.clone()
    } else {
        None
    };

    let unique_id = test.__common_attr__.unique_id.clone();
    // Run path: where target/run/.../<alias>.sql lives; surfaced in runtime (database)
    // errors so the user can open the actual file the database executed.
    let run_display_path = test
        .get_node_path(NodePathKind::Executable, &io_args.in_dir, &io_args.out_dir)
        .display()
        .to_string();

    let node_overrides = apply_node_overrides(
        &adapter,
        adapter_type,
        custom_warehouse,
        &test.__base_attr__.database,
        &unique_id,
    )?;

    let render_result = with_jinja_trace(&compiled_path, &unique_id, |listeners| {
        jinja_env
            .render_str(
                &format!("{{{{ {macro_name}() }}}}"),
                &mut context,
                listeners,
            )
            .map_err(|e| {
                if e.code.is_database_error() {
                    let indented_body = e
                        .context
                        .lines()
                        .map(|line| format!("  {line}"))
                        .collect::<Vec<_>>()
                        .join("\n");
                    let message = format!("{unique_id} ({run_display_path})\n{indented_body}",);
                    Box::new(dbt_common::FsError::new(e.code, message))
                } else {
                    Box::new(
                        dbt_common::FsError::new(
                            ErrorCode::JinjaError,
                            format!("Error running test {unique_id}: {e}"),
                        )
                        .with_location(compiled_path.clone()),
                    )
                }
            })
    });

    reset_node_overrides(&adapter, &unique_id, &node_overrides)?;

    // If render_result is an error but the test is configured with severity warn, return a warning result
    if render_result.is_err() && matches!(test.deprecated_config.severity, Some(Severity::Warn)) {
        return Ok((
            vec![TestResult {
                column_name: None,
                failures: 0,
                should_warn: true,
                should_error: false,
            }],
            None,
        ));
    }

    let _ = render_result?; // ensure we don't swallow errors

    let expr = jinja_env.compile_expression("load_result('main').table")?;
    let table = expr
        .eval(&context, &[])
        .unwrap()
        .downcast_object::<AgateTable>()
        .unwrap();

    let test_results = get_test_results(&table)?;
    Ok((test_results, None))
}

pub fn compare_record_batches(
    batch: &RecordBatch,
) -> arrow::error::Result<CompareRecordBatchResult> {
    let schema = batch.schema();

    let label_col_index = schema
        .fields()
        .iter()
        .position(|f| f.name().to_lowercase() == "actual_or_expected")
        .ok_or_else(|| {
            arrow::error::ArrowError::SchemaError(
                "Missing 'actual_or_expected' column in unit test result. \
                 This may indicate an issue with unit test execution in the current adapter mode."
                    .to_string(),
            )
        })?;

    let mut actual_rows = vec![];
    let mut expected_rows = vec![];

    let label_array = batch
        .column(label_col_index)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("'actual_or_expected' column must be StringArray");

    for i in 0..batch.num_rows() {
        match label_array.value(i) {
            "actual" => actual_rows.push(i),
            "expected" => expected_rows.push(i),
            _ => {
                return Err(arrow::error::ArrowError::ComputeError(format!(
                    "Invalid value in 'actual_or_expected' column: '{}'. Expected 'actual' or 'expected'",
                    label_array.value(i)
                )));
            }
        }
    }

    // Prepare new columns - include all data columns in output
    let mut new_columns: Vec<ArrayRef> = vec![];
    let mut new_fields: Vec<Field> = vec![];
    let mut has_differences = actual_rows.len() != expected_rows.len();

    for (col_index, field) in schema.fields().iter().enumerate() {
        if col_index == label_col_index {
            continue; // skip the label column
        }

        let col = batch.column(col_index);
        let data_type = field.data_type();

        let diffs: Vec<String> = actual_rows
            .iter()
            .zip_longest(expected_rows.iter())
            .map(|pair| match pair {
                EitherOrBoth::Both(a, e) => {
                    let actual_val = value_as_string(col, *a, data_type);
                    let expected_val = value_as_string(col, *e, data_type);

                    if actual_val == expected_val {
                        expected_val
                    } else {
                        has_differences = true;
                        format!("{expected_val} -> {actual_val}")
                    }
                }
                EitherOrBoth::Left(a) => {
                    let actual_val = value_as_string(col, *a, data_type);
                    has_differences = true;
                    format!("∅ -> {actual_val}")
                }
                EitherOrBoth::Right(e) => {
                    let expected_val = value_as_string(col, *e, data_type);
                    has_differences = true;
                    format!("{expected_val} -> ∅")
                }
            })
            .collect();

        let new_array = Arc::new(StringArray::from(diffs)) as ArrayRef;
        new_columns.push(new_array);
        new_fields.push(Field::new(field.name(), DataType::Utf8, false));
    }

    // Handle the case where no columns have differences
    let diff_batch = if !has_differences {
        // Create a summary batch showing that all columns matched
        let summary_schema = Arc::new(Schema::new(vec![Field::new(
            "summary",
            DataType::Utf8,
            false,
        )]));
        let summary_data = Arc::new(StringArray::from(vec![
            format!(
                "✅ All {} columns matched perfectly",
                schema.fields().len() - 1
            ), // -1 for label column
        ])) as ArrayRef;
        RecordBatch::try_new(summary_schema, vec![summary_data])?
    } else {
        let new_schema = Arc::new(Schema::new(new_fields));
        RecordBatch::try_new(new_schema, new_columns)?
    };

    Ok(CompareRecordBatchResult {
        actual_rows: actual_rows.len(),
        expected_rows: expected_rows.len(),
        diff_batch,
        has_differences,
    })
}

fn value_as_string(array: &ArrayRef, index: usize, data_type: &DataType) -> String {
    match data_type {
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            null_or!(arr, index, arr.value(index).to_string())
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            null_or!(arr, index, arr.value(index).to_string())
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            null_or!(arr, index, format!("{:.1}", arr.value(index)))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            null_or!(arr, index, format!("{:.1}", arr.value(index)))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            null_or!(arr, index, arr.value(index).to_string())
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            null_or!(arr, index, arr.value(index).to_string())
        }
        DataType::Decimal128(_, scale) => {
            let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            null_or!(arr, index, {
                let raw_value = arr.value(index);
                let scale_factor = 10i128.pow(*scale as u32);
                let integer_part = raw_value / scale_factor;
                let fractional_part = (raw_value % scale_factor).abs();
                format!("{integer_part}.{fractional_part}")
            })
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            null_or!(arr, index, {
                DateTime::from_timestamp(arr.value(index), 0)
                    .unwrap()
                    .to_string()
            })
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            null_or!(arr, index, {
                DateTime::from_timestamp(arr.value(index) / 1_000_000_000, 0)
                    .unwrap()
                    .to_string()
            })
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            null_or!(arr, index, {
                let days = arr.value(index) as i64;
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                (epoch + chrono::Duration::days(days)).to_string()
            })
        }
        _ => "[unsupported]".to_string(),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn materialize_function(
    sql: &str,
    function: &DbtFunction,
    adapter_type: AdapterType,
    runtime_config: &DbtRuntimeConfig,
    materialization_resolver: &Arc<MaterializationResolver>,
    jinja_env: Arc<JinjaEnv>,
    base_context: &BTreeMap<String, Value>,
    io_args: &IoArgs,
) -> FsResult<Value> {
    let mut context = build_run_node_context(
        function,
        &function.deprecated_config,
        adapter_type,
        None,
        base_context,
        io_args,
        ExecutionPhase::Run,
        None,
        runtime_config.dependencies.keys().cloned().collect(),
    );

    // Find the function materialization macro
    let macro_name = materialization_resolver.find_materialization_macro_by_name("function")?;

    context.insert("sql".to_string(), Value::from(sql));
    context.insert("compiled_code".to_string(), Value::from(sql));

    let compiled_path =
        function.get_node_path_abs(NodePathKind::Compiled, &io_args.in_dir, &io_args.out_dir);

    let unique_id = function.__common_attr__.unique_id.clone();
    let node_alias = function.__base_attr__.alias.clone();
    let run_path = function
        .get_node_path(NodePathKind::Executable, &io_args.in_dir, &io_args.out_dir)
        .into_owned();

    let _adapter = jinja_env.get_base_adapter().ok_or_else(|| {
        unexpected_fs_err!(
            "No adapter found for function {}",
            function.__common_attr__.unique_id
        )
    })?;

    let result = execute_materialization_macro(
        jinja_env,
        &macro_name,
        &mut context,
        "function",
        &unique_id,
        &node_alias,
        run_path,
    );

    // Write compiled SQL to file
    if let Some(parent) = compiled_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::write(&compiled_path, sql);

    result
}
