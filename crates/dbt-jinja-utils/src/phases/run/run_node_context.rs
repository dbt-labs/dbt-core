//! This module contains the scope for materializing nodes

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

use dbt_adapter_core::AdapterType;
use dbt_agate::AgateTable;
use dbt_common::ErrorCode;
use dbt_common::constants::DBT_RUN_DIR_NAME;
use dbt_common::io_args::IoArgs;
use dbt_common::path::get_target_write_path;
use dbt_common::serde_utils::convert_yml_to_value_map;

use dbt_adapter::load_store::ResultStore;
use dbt_common::stdfs;
use dbt_common::tracing::emit::emit_warn_log_message;
use dbt_schemas::schemas::InternalDbtNode;
use dbt_schemas::schemas::NodePathKind;
use dbt_schemas::schemas::telemetry::NodeType;
use minijinja::State;
use minijinja::listener::RenderingEventListener;
use minijinja::machinery::Span;
use minijinja::{Error, ErrorKind, Value as MinijinjaValue, value::Object};
use serde::Serialize;

use dbt_jinja_ctx::{
    HookConfig, JinjaObject, LazyModelWrapper, MacroLookupContext, RunNodeCtx, to_jinja_btreemap,
};

use super::run_config::RunConfig;
use dbt_schemas::schemas::project::ConfigKeys;

type YmlValue = dbt_yaml::Value;

/// Per-node fields computed from the YAML node config and used to construct
/// the `RunNodeCtx` overlay. Replaces the historical
/// `extend_with_model_context(&mut base_context, ...)` mutator: returning
/// the values lets `build_run_node_context` construct a typed `RunNodeCtx`
/// in one shot rather than incrementally mutating a `BTreeMap`.
struct ModelContextFields {
    this: MinijinjaValue,
    database: String,
    schema: String,
    identifier: String,
    pre_hooks: Option<MinijinjaValue>,
    post_hooks: Option<MinijinjaValue>,
    config: MinijinjaValue,
    model: JinjaObject<LazyModelWrapper>,
    node: JinjaObject<LazyModelWrapper>,
}

#[allow(clippy::too_many_arguments)]
fn build_model_context_fields<S: Serialize>(
    node: &dyn InternalDbtNode,
    deprecated_config: &S,
    adapter_type: AdapterType,
    io_args: &IoArgs,
    sql_header: Option<MinijinjaValue>,
) -> ModelContextFields {
    let model = node.serialize();
    let common_attr = node.common();
    let base_attr = node.base();
    let resource_type = node.resource_type();
    // Create a relation for 'this' using config values
    let this_relation = dbt_adapter::relation::RelationObject::new(Arc::from(
        dbt_adapter::relation::do_create_relation(
            adapter_type,
            base_attr.database.clone(),
            base_attr.schema.clone(),
            Some(base_attr.alias.clone()),
            None,
            base_attr.quoting,
        )
        .unwrap(),
    ))
    .into_value();

    let config_yml = dbt_yaml::to_value(deprecated_config).expect("Failed to serialize object");

    let pre_hooks = config_yml.get("pre_hook").map(|pre_hook| {
        let values: Vec<HookConfig> = match pre_hook {
            YmlValue::String(_, _) | YmlValue::Mapping(_, _) => {
                parse_hook_item(pre_hook).into_iter().collect()
            }
            YmlValue::Sequence(arr, _) => arr.iter().filter_map(parse_hook_item).collect(),
            YmlValue::Null(_) => vec![],
            _ => {
                emit_warn_log_message(
                    ErrorCode::Generic,
                    format!("Unknown pre-hook type: {:?}", pre_hook),
                    io_args.status_reporter.as_ref(),
                );
                vec![]
            }
        };
        values
            .iter()
            .map(|hook| MinijinjaValue::from_object(hook.clone()))
            .collect::<Vec<MinijinjaValue>>()
            .into()
    });

    let post_hooks = config_yml.get("post_hook").map(|post_hook| {
        let values: Vec<HookConfig> = match post_hook {
            YmlValue::String(_, _) | YmlValue::Mapping(_, _) => {
                parse_hook_item(post_hook).into_iter().collect()
            }
            YmlValue::Sequence(arr, _) => arr.iter().filter_map(parse_hook_item).collect(),
            YmlValue::Null(_) => vec![],
            _ => {
                emit_warn_log_message(
                    ErrorCode::Generic,
                    format!("Unknown post-hook type: {:?}", post_hook),
                    io_args.status_reporter.as_ref(),
                );
                vec![]
            }
        };
        values
            .iter()
            .map(|hook| MinijinjaValue::from_object(hook.clone()))
            .collect::<Vec<MinijinjaValue>>()
            .into()
    });

    let mut config_map = convert_yml_to_value_map(config_yml);
    if let Some(sql_header) = sql_header {
        config_map.insert("sql_header".to_string(), sql_header);
    }

    let mut model_map = convert_yml_to_value_map(model);

    // We are reading the raw_sql here for snapshots and models
    let raw_sql_path = match resource_type {
        // For snapshots, use path (generated file path) since original_file_path tracks the source
        NodeType::Snapshot => Some(io_args.out_dir.join(common_attr.path.clone())),
        NodeType::Model => Some(io_args.in_dir.join(common_attr.original_file_path.clone())),
        _ => None,
    };
    if let Some(raw_sql_path) = raw_sql_path {
        if let Ok(raw_sql) = stdfs::read_to_string(&raw_sql_path) {
            model_map.insert("raw_sql".to_owned(), MinijinjaValue::from(raw_sql));
        } else {
            emit_warn_log_message(
                ErrorCode::Generic,
                format!("Failed to read raw_sql: {}", raw_sql_path.display()),
                io_args.status_reporter.as_ref(),
            );
        };
    }

    // Get valid config keys based on resource type
    let valid_keys = match resource_type {
        NodeType::Model => dbt_schemas::schemas::project::ModelConfig::valid_field_names(),
        NodeType::Seed => dbt_schemas::schemas::project::SeedConfig::valid_field_names(),
        NodeType::Test => dbt_schemas::schemas::project::DataTestConfig::valid_field_names(),
        NodeType::Snapshot => dbt_schemas::schemas::project::SnapshotConfig::valid_field_names(),
        NodeType::Source => dbt_schemas::schemas::project::SourceConfig::valid_field_names(),
        NodeType::UnitTest => dbt_schemas::schemas::project::UnitTestConfig::valid_field_names(),
        NodeType::Function => dbt_schemas::schemas::project::FunctionConfig::valid_field_names(),
        _ => {
            // For other types, use an empty set to avoid warnings
            std::collections::HashSet::new()
        }
    };

    let node_config = RunConfig {
        model_config: config_map,
        model: model_map.clone(),
        valid_keys,
    };

    // Create the lazy wrapper for the model with the compiled path
    let compiled_path =
        node.get_node_path_abs(NodePathKind::Compiled, &io_args.in_dir, &io_args.out_dir);
    let lazy_model = LazyModelWrapper::new(model_map.clone(), compiled_path.clone());
    let lazy_node = LazyModelWrapper::new(model_map, compiled_path);

    ModelContextFields {
        this: this_relation,
        database: base_attr.database.clone(),
        schema: base_attr.schema.clone(),
        identifier: common_attr.name.clone(),
        pre_hooks,
        post_hooks,
        config: MinijinjaValue::from_object(node_config),
        model: JinjaObject::new(lazy_model),
        node: JinjaObject::new(lazy_node),
    }
}

/// Extend the base context with stateful functions
pub fn extend_base_context_stateful_fn(
    base_context: &mut BTreeMap<String, MinijinjaValue>,
    root_project_name: &str,
    packages: BTreeSet<String>,
) {
    let result_store = ResultStore::default();
    base_context.insert(
        "store_result".to_owned(),
        MinijinjaValue::from_function(result_store.store_result()),
    );
    base_context.insert(
        "load_result".to_owned(),
        MinijinjaValue::from_function(result_store.load_result()),
    );
    base_context.insert(
        "store_raw_result".to_owned(),
        MinijinjaValue::from_function(result_store.store_raw_result()),
    );

    // Add submit_python_job context function using a separate helper
    base_context.insert(
        "submit_python_job".to_owned(),
        MinijinjaValue::from_function(submit_python_job_context_fn()),
    );

    let mut packages = packages;
    packages.insert(root_project_name.to_string());

    base_context.insert(
        "context".to_owned(),
        MinijinjaValue::from_object(MacroLookupContext {
            root_project_name: root_project_name.to_string(),
            current_project_name: None,
            packages,
        }),
    );
}

/// Build a run context - parent function that orchestrates the context building
#[allow(clippy::too_many_arguments)]
pub fn build_run_node_context<S: Serialize>(
    node: &dyn InternalDbtNode,
    deprecated_config: &S,
    adapter_type: AdapterType,
    agate_table: Option<AgateTable>,
    base_context: &BTreeMap<String, MinijinjaValue>,
    io_args: &IoArgs,
    sql_header: Option<MinijinjaValue>,
    packages: BTreeSet<String>,
) -> BTreeMap<String, MinijinjaValue> {
    let common_attr = node.common();
    let base_attr = node.base();
    let resource_type = node.resource_type();

    // Stateful fns: store_result/load_result/store_raw_result/submit_python_job + context.
    // These were `extend_base_context_stateful_fn` mutations into the BTreeMap;
    // pull the same closures + MacroLookupContext into local bindings so we
    // can construct the typed overlay below.
    let result_store = ResultStore::default();
    let store_result = MinijinjaValue::from_function(result_store.store_result());
    let load_result = MinijinjaValue::from_function(result_store.load_result());
    let store_raw_result = MinijinjaValue::from_function(result_store.store_raw_result());
    let submit_python_job = MinijinjaValue::from_function(submit_python_job_context_fn());

    let mut packages_with_root = packages;
    packages_with_root.insert(common_attr.package_name.clone());
    let context_lookup = JinjaObject::new(MacroLookupContext {
        root_project_name: common_attr.package_name.clone(),
        current_project_name: None,
        packages: packages_with_root,
    });

    // Per-node model-specific fields (this/database/schema/identifier, hooks,
    // config, model, node).
    let model_fields =
        build_model_context_fields(node, deprecated_config, adapter_type, io_args, sql_header);

    // Use alias for the run file path (target/run/<alias>.sql) rather than name.
    // For most nodes, alias == name. For generic tests with long names, the name is the
    // full (human-readable) form while alias is the truncated form. Using alias avoids
    // ENAMETOOLONG (OS error 36) on Linux when the test name exceeds 255 chars.
    let model_name = if base_attr.alias.is_empty() {
        common_attr.name.clone()
    } else {
        base_attr.alias.clone()
    };

    let write_value = MinijinjaValue::from_object(WriteConfig {
        node_name: model_name,
        resource_type: resource_type.as_static_ref().to_string(),
        project_root: io_args.in_dir.clone(),
        target_path: io_args.out_dir.clone(),
        package_name: common_attr.package_name.clone(),
        path: common_attr.path.clone(),
        original_file_path: common_attr.original_file_path.clone(),
    });

    let load_agate_table = agate_table.map(|agate_table| {
        MinijinjaValue::from_function(move |_args: &[MinijinjaValue]| {
            MinijinjaValue::from_object(agate_table.clone())
        })
    });

    // Builtins overlay: clone the compile-base map and insert the per-node
    // RunConfig. The map underlying `builtins` MUST be
    // `BTreeMap<String, MinijinjaValue>` exactly (downstream macro code
    // downcasts to that type) — same trap as `MACRO_DISPATCH_ORDER`.
    let mut base_builtins = if let Some(builtins) = base_context.get("builtins") {
        builtins
            .as_object()
            .unwrap()
            .downcast_ref::<BTreeMap<String, MinijinjaValue>>()
            .unwrap()
            .clone()
    } else {
        BTreeMap::new()
    };
    let node_config = model_fields
        .config
        .as_object()
        .unwrap()
        .downcast_ref::<RunConfig>()
        .unwrap()
        .clone();
    base_builtins.insert(
        "config".to_string(),
        MinijinjaValue::from_object(node_config),
    );

    let abs_compiled =
        node.get_node_path_abs(NodePathKind::Compiled, &io_args.in_dir, &io_args.out_dir);
    let relative_path = abs_compiled
        .strip_prefix(&io_args.out_dir)
        .map(|p| p.to_path_buf())
        .unwrap_or(abs_compiled);

    let overlay = RunNodeCtx {
        this: model_fields.this,
        database: model_fields.database,
        schema: model_fields.schema,
        identifier: model_fields.identifier,
        pre_hooks: model_fields.pre_hooks,
        post_hooks: model_fields.post_hooks,
        config: model_fields.config,
        model: model_fields.model,
        node: model_fields.node,
        connection_name: String::new(),
        store_result,
        load_result,
        store_raw_result,
        submit_python_job,
        context: context_lookup,
        write: write_value,
        load_agate_table,
        builtins: MinijinjaValue::from_object(base_builtins),
        target_package_name: common_attr.package_name.clone(),
        current_path: relative_path.to_string_lossy().into_owned(),
        current_span: MinijinjaValue::from_serialize(Span::default()),
    };

    // Today's caller still consumes `BTreeMap<String, MinijinjaValue>`. We
    // serialize the typed overlay and `.extend(...)` onto a clone of the
    // base — same last-write-wins shadowing semantic the original
    // BTreeMap-based code produced. PR 9 (cleanup) flows the typed struct
    // directly through `render_named_str<S: Serialize>` and drops the
    // conversion.
    let mut context = base_context.clone();
    context.extend(to_jinja_btreemap(&overlay));
    context
}

fn parse_hook_item(item: &YmlValue) -> Option<HookConfig> {
    match item {
        YmlValue::String(s, _) => Some(HookConfig {
            sql: s.to_string(),
            transaction: true,
        }),
        YmlValue::Mapping(map, _) => {
            let sql = map.get("sql")?.as_str()?.to_string();
            let transaction = map
                .get("transaction")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            Some(HookConfig { sql, transaction })
        }
        _ => {
            eprintln!("Pre hook unknown type: {item:?}");
            None
        }
    }
}

/// Context function that writes a payload to file
#[derive(Debug)]
pub struct WriteConfig {
    /// The node ({operation, model}) name or alias (for ENAMETOOLONG safety on Linux)
    pub node_name: String,
    /// The resource type string (see `fusion::node::NodeType`)
    pub resource_type: String,
    /// The DBT project root
    pub project_root: PathBuf,
    /// The directory to write the file into
    pub target_path: PathBuf,
    /// The package (project) name, used to mirror dbt-core's target/run/{pkg}/... structure
    pub package_name: String,
    /// The node's path relative to the project root (common_attr.path)
    pub path: PathBuf,
    /// The node's original source file path (common_attr.original_file_path)
    pub original_file_path: PathBuf,
}

impl Object for WriteConfig {
    fn call(
        self: &Arc<Self>,
        _state: &State<'_, '_>,
        args: &[MinijinjaValue],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<MinijinjaValue, Error> {
        if args.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                "write function requires payload argument".to_string(),
            ));
        }

        // Extract payload from args
        let payload = match args[0].as_str() {
            Some(s) => s,
            None => {
                return Err(Error::new(
                    ErrorKind::InvalidOperation,
                    "Failed to convert payload to string".to_string(),
                ));
            }
        };

        // Write the file
        match write_file(
            &self.project_root,
            &self.target_path,
            &self.node_name,
            &self.package_name,
            &self.path,
            &self.original_file_path,
            &self.resource_type,
            payload,
        ) {
            Ok(_) => {}
            Err(e) => {
                return Err(Error::new(
                    ErrorKind::InvalidOperation,
                    format!("Failed to write file: {e}"),
                ));
            }
        }

        // Return empty string on success
        Ok(MinijinjaValue::from(""))
    }
}

/// Write a file to disk
#[allow(clippy::too_many_arguments)]
fn write_file(
    project_root: &Path,
    target_path: &Path,
    node_name: &str,
    package_name: &str,
    path: &Path,
    original_file_path: &Path,
    resource_type: &str,
    payload: &str,
) -> Result<(), Error> {
    // Check if model is a Macro or SourceDefinition
    if resource_type == "macro" || resource_type == "source" {
        return Err(Error::new(
            ErrorKind::InvalidOperation,
            "Macros and sources cannot be written to disk",
        ));
    }

    // Mirror dbt-core's target/run/{package_name}/{original_file_path} structure.
    // The directory comes from get_target_write_path; the filename is the alias (node_name)
    // to avoid ENAMETOOLONG on Linux for generic tests with very long names.
    // project_root.join(target_path) handles both absolute and relative target_path correctly.
    let abs_run_path = get_target_write_path(
        project_root,
        &project_root.join(target_path).join(DBT_RUN_DIR_NAME),
        package_name,
        path,
        original_file_path,
    );
    let full_path = abs_run_path
        .parent()
        .unwrap_or(&abs_run_path)
        .join(format!("{node_name}.sql"));

    // Create parent directories if needed
    if let Some(parent) = full_path.parent()
        && !parent.exists()
        && let Err(e) = fs::create_dir_all(parent)
    {
        return Err(Error::new(
            ErrorKind::InvalidOperation,
            format!("Failed to create directory {}: {}", parent.display(), e),
        ));
    }

    match fs::write(&full_path, payload) {
        Ok(_) => Ok(()),
        Err(e) => Err(Error::new(
            ErrorKind::InvalidOperation,
            format!("Failed to write to {}: {}", full_path.display(), e),
        )),
    }
}

/// Returns the function used for the submit_python_job context.
fn submit_python_job_context_fn()
-> impl Fn(&State, &[MinijinjaValue]) -> Result<MinijinjaValue, Error> + Copy {
    |state: &State, args: &[MinijinjaValue]| {
        // Parse arguments: submit_python_job(parsed_model, compiled_code)
        if args.len() != 2 {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                format!("submit_python_job expects 2 arguments, got {}", args.len()),
            ));
        }
        let parsed_model = &args[0];
        let compiled_code = args[1].as_str().ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidOperation,
                "compiled_code must be a string",
            )
        })?;

        // Note(Ani):
        // dbt-core validates:
        //   - macro_stack.depth == 2
        //   - call_stack[1] == "macro.dbt.statement"
        //   - "materialization" in call_stack[0]
        //
        // In fusion, we shouldn't need to do this because this funciton is only registered in the run node context
        // so if a user tries to use it outside of a statement.sql macro, in a materialization macro, it will fail earlier due to an unrecongized function call.

        // Get adapter from context and call submit_python_job
        let adapter = state
            .lookup("adapter", &[])
            .ok_or_else(|| Error::new(ErrorKind::UndefinedError, "adapter not found in context"))?;
        adapter.call_method(
            state,
            "submit_python_job",
            &[parsed_model.clone(), MinijinjaValue::from(compiled_code)],
            &[],
        )
    }
}
