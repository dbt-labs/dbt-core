//! This module contains the scope for materializing nodes

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

use dbt_agate::AgateTable;
use dbt_common::constants::DBT_RUN_DIR_NAME;
use dbt_common::fs_err;
use dbt_common::io_args::IoArgs;
use dbt_common::serde_utils::convert_json_to_map;
use dbt_common::show_warning;
use dbt_common::tokiofs;
use dbt_common::ErrorCode;
use dbt_fusion_adapter::load_store::ResultStore;
use dbt_fusion_adapter::relation_object::create_relation;
use dbt_schemas::schemas::CommonAttributes;
use dbt_schemas::schemas::NodeBaseAttributes;
use minijinja::listener::RenderingEventListener;
use minijinja::State;
use minijinja::{value::Object, Error, ErrorKind, Value as MinijinjaValue};
use serde::Serialize;
use serde_json::Value;

use crate::phases::MacroLookupContext;

use super::run_config::RunConfig;

/// Build model-specific context (model, common_attr, alias, quoting, config, resource_type, sql_header)
#[allow(clippy::too_many_arguments)]
async fn extend_with_model_context<S: Serialize>(
    base_context: &mut BTreeMap<String, MinijinjaValue>,
    model: Value,
    common_attr: &CommonAttributes,
    base_attr: &NodeBaseAttributes,
    deprecated_config: &S,
    adapter_type: &str,
    io_args: &IoArgs,
    resource_type: &str,
    sql_header: Option<MinijinjaValue>,
) {
    // Create a relation for 'this' using config values
    let this_relation = create_relation(
        adapter_type.to_string(),
        base_attr.database.clone(),
        base_attr.schema.clone(),
        Some(base_attr.alias.clone()),
        None,
        base_attr.quoting,
    )
    .unwrap()
    .as_value();

    base_context.insert("this".to_owned(), this_relation);
    base_context.insert(
        "database".to_owned(),
        MinijinjaValue::from(base_attr.database.clone()),
    );
    base_context.insert(
        "schema".to_owned(),
        MinijinjaValue::from(base_attr.schema.clone()),
    );
    base_context.insert(
        "identifier".to_owned(),
        MinijinjaValue::from(common_attr.name.clone()),
    );

    let config_json = serde_json::to_value(deprecated_config).expect("Failed to serialize object");

    if let Some(pre_hook) = config_json.get("pre_hook") {
        let values: Vec<HookConfig> = match pre_hook {
            Value::String(_) | Value::Object(_) => parse_hook_item(pre_hook).into_iter().collect(),
            Value::Array(arr) => arr.iter().filter_map(parse_hook_item).collect(),
            Value::Null => vec![],
            _ => {
                show_warning!(
                    io_args,
                    fs_err!(ErrorCode::Generic, "Unknown pre-hook type: {:?}", pre_hook)
                );
                vec![]
            }
        };
        let pre_hooks_vals: MinijinjaValue = values
            .iter()
            .map(|hook| MinijinjaValue::from_object(hook.clone()))
            .collect::<Vec<MinijinjaValue>>()
            .into();
        base_context.insert("pre_hooks".to_owned(), pre_hooks_vals);
    }
    if let Some(post_hook) = config_json.get("post_hook") {
        let values: Vec<HookConfig> = match post_hook {
            Value::String(_) | Value::Object(_) => parse_hook_item(post_hook).into_iter().collect(),
            Value::Array(arr) => arr.iter().filter_map(parse_hook_item).collect(),
            Value::Null => vec![],
            _ => {
                show_warning!(
                    io_args,
                    fs_err!(
                        ErrorCode::Generic,
                        "Unknown post-hook type: {:?}",
                        post_hook
                    )
                );
                vec![]
            }
        };
        let post_hooks_vals: MinijinjaValue = values
            .iter()
            .map(|hook| MinijinjaValue::from_object(hook.clone()))
            .collect::<Vec<MinijinjaValue>>()
            .into();
        base_context.insert("post_hooks".to_owned(), post_hooks_vals);
    }

    let mut config_map = convert_json_to_map(config_json);
    if let Some(sql_header) = sql_header {
        config_map.insert("sql_header".to_string(), sql_header);
    }

    let mut model_map = convert_json_to_map(model);

    // We are reading the raw_sql here for snapshots and models
    let raw_sql_path = match resource_type {
        "snapshot" => Some(io_args.out_dir.join(common_attr.original_file_path.clone())),
        "model" => Some(io_args.in_dir.join(common_attr.original_file_path.clone())),
        _ => None,
    };
    if let Some(raw_sql_path) = raw_sql_path {
        if let Ok(raw_sql) = tokiofs::read_to_string(&raw_sql_path).await {
            model_map.insert("raw_sql".to_owned(), MinijinjaValue::from(raw_sql));
        } else {
            show_warning!(
                io_args,
                fs_err!(
                    ErrorCode::Generic,
                    "Failed to read raw_sql: {}",
                    raw_sql_path.display()
                )
            );
        };
    }

    let node_config = RunConfig {
        model_config: config_map,
        model: model_map.clone(),
    };

    base_context.insert(
        "config".to_owned(),
        MinijinjaValue::from_object(node_config),
    );

    base_context.insert("model".to_owned(), MinijinjaValue::from_object(model_map));
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
pub async fn build_run_node_context<S: Serialize>(
    model: Value,
    common_attr: &CommonAttributes,
    base_attr: &NodeBaseAttributes,
    deprecated_config: &S,
    adapter_type: &str,
    agate_table: Option<AgateTable>,
    base_context: &BTreeMap<String, MinijinjaValue>,
    io_args: &IoArgs,
    resource_type: &str,
    sql_header: Option<MinijinjaValue>,
    packages: BTreeSet<String>,
) -> BTreeMap<String, MinijinjaValue> {
    // Build model-specific context
    let mut context = base_context.clone();
    extend_base_context_stateful_fn(&mut context, &common_attr.package_name, packages);

    extend_with_model_context(
        &mut context,
        model,
        common_attr,
        base_attr,
        deprecated_config,
        adapter_type,
        io_args,
        resource_type,
        sql_header,
    )
    .await;

    let model_name = common_attr.name.clone();
    // Add write function
    context.insert(
        "write".to_owned(),
        MinijinjaValue::from_object(WriteConfig {
            model_name,
            resource_type: resource_type.to_string(),
            project_root: io_args.in_dir.clone(),
            target_path: io_args.out_dir.clone(),
        }),
    );

    if let Some(agate_table) = agate_table {
        context.insert(
            "load_agate_table".to_owned(),
            MinijinjaValue::from_function(move |_args: &[MinijinjaValue]| {
                MinijinjaValue::from_object(agate_table.clone())
            }),
        );
    }

    let mut base_builtins = if let Some(builtins) = context.get("builtins") {
        builtins
            .as_object()
            .unwrap()
            .downcast_ref::<BTreeMap<String, MinijinjaValue>>()
            .unwrap()
            .clone()
    } else {
        BTreeMap::new()
    };

    // Get the config from model context to pass to general context
    let node_config = context
        .get("config")
        .unwrap()
        .as_object()
        .unwrap()
        .downcast_ref::<RunConfig>()
        .unwrap();

    base_builtins.insert(
        "config".to_string(),
        MinijinjaValue::from_object(node_config.clone()),
    );

    // Register builtins as a global
    context.insert(
        "builtins".to_owned(),
        MinijinjaValue::from_object(base_builtins),
    );

    context
}

fn parse_hook_item(item: &Value) -> Option<HookConfig> {
    match item {
        Value::String(s) => Some(HookConfig {
            sql: s.to_string(),
            transaction: true,
        }),
        Value::Object(map) => {
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

#[derive(Clone)]
struct HookConfig {
    pub sql: String,

    pub transaction: bool,
}
impl Object for HookConfig {
    fn get_value(self: &Arc<Self>, key: &MinijinjaValue) -> Option<MinijinjaValue> {
        match key.as_str() {
            Some("sql") => Some(MinijinjaValue::from(self.sql.clone())),
            Some("transaction") => Some(MinijinjaValue::from(self.transaction)),
            _ => None,
        }
    }
    fn render(self: &Arc<Self>, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.sql)
    }
}
// iplement std::fmt::Debug for HookConfig
impl std::fmt::Debug for HookConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HookConfig {{ sql: {} }}", self.sql)
    }
}

#[derive(Debug)]
pub struct WriteConfig {
    pub model_name: String,
    pub resource_type: String,
    pub project_root: PathBuf,
    pub target_path: PathBuf,
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
            &self.model_name,
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
fn write_file(
    project_root: &Path,
    target_path: &Path,
    model_name: &str,
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

    // Construct build path - simple implementation
    let build_path = target_path
        .join(DBT_RUN_DIR_NAME)
        .join(format!("{model_name}.sql"));
    let full_path = if build_path.is_absolute() {
        build_path
    } else {
        project_root.join(&build_path)
    };

    // Create parent directories if needed
    if let Some(parent) = full_path.parent() {
        if !parent.exists() {
            if let Err(e) = fs::create_dir_all(parent) {
                return Err(Error::new(
                    ErrorKind::InvalidOperation,
                    format!("Failed to create directory {}: {}", parent.display(), e),
                ));
            }
        }
    }

    match fs::write(&full_path, payload) {
        Ok(_) => Ok(()),
        Err(e) => Err(Error::new(
            ErrorKind::InvalidOperation,
            format!("Failed to write to {}: {}", full_path.display(), e),
        )),
    }
}
