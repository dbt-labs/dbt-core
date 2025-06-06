//! This module contains the scope for materializing nodes

use std::collections::BTreeMap;
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
use dbt_fusion_adapter::adapters::load_store::ResultStore;
use dbt_fusion_adapter::adapters::relation_object::create_relation;
use dbt_schemas::schemas::{common::ResolvedQuoting, manifest::CommonAttributes};
use minijinja::listener::RenderingEventListener;
use minijinja::State;
use minijinja::{value::Object, Error, ErrorKind, Value as MinijinjaValue};
use serde::Serialize;

use super::run_config::RunConfig;

/// Build a run context
#[allow(clippy::too_many_arguments)]
pub async fn build_run_node_context<T: Serialize, S: Serialize>(
    model: &T,
    common_attr: &CommonAttributes,
    alias: &str,
    quoting: ResolvedQuoting,
    config: &S,
    adapter_type: &str,
    agate_table: Option<AgateTable>,
    base_context: &BTreeMap<String, MinijinjaValue>,
    io_args: &IoArgs,
    resource_type: &str,
    sql_header: Option<MinijinjaValue>,
) -> BTreeMap<String, MinijinjaValue> {
    let mut context = base_context.clone();
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

    // Create a relation for 'this' using config values
    let this_relation = create_relation(
        adapter_type.to_string(),
        common_attr.database.clone(),
        common_attr.schema.clone(),
        Some(alias.to_string()),
        None,
        quoting,
    )
    .unwrap()
    .as_value();

    context.insert("this".to_owned(), this_relation);
    context.insert(
        "database".to_owned(),
        MinijinjaValue::from(common_attr.database.clone()),
    );
    context.insert(
        "schema".to_owned(),
        MinijinjaValue::from(common_attr.schema.clone()),
    );
    context.insert(
        "identifier".to_owned(),
        MinijinjaValue::from(common_attr.name.clone()),
    );

    let config_json = serde_json::to_value(config).expect("Failed to serialize object");

    if let Some(pre_hook) = config_json.get("pre_hook") {
        let values: Vec<HookConfig> = match pre_hook {
            serde_json::Value::String(_) | serde_json::Value::Object(_) => {
                parse_hook_item(pre_hook).into_iter().collect()
            }
            serde_json::Value::Array(arr) => arr.iter().filter_map(parse_hook_item).collect(),
            serde_json::Value::Null => vec![],
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
        context.insert("pre_hooks".to_owned(), pre_hooks_vals);
    }
    if let Some(post_hook) = config_json.get("post_hook") {
        let values: Vec<HookConfig> = match post_hook {
            serde_json::Value::String(_) | serde_json::Value::Object(_) => {
                parse_hook_item(post_hook).into_iter().collect()
            }
            serde_json::Value::Array(arr) => arr.iter().filter_map(parse_hook_item).collect(),
            serde_json::Value::Null => vec![],
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
        context.insert("post_hooks".to_owned(), post_hooks_vals);
    }

    let mut config_map = convert_json_to_map(config_json);
    if let Some(sql_header) = sql_header {
        config_map.insert("sql_header".to_string(), sql_header);
    }

    let mut model_map =
        convert_json_to_map(serde_json::to_value(model).expect("Failed to serialize object"));

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

    context.insert(
        "config".to_owned(),
        MinijinjaValue::from_object(node_config.clone()),
    );
    base_builtins.insert(
        "config".to_string(),
        MinijinjaValue::from_object(node_config),
    );

    context.insert("model".to_owned(), MinijinjaValue::from_object(model_map));

    // Register builtins as a global
    context.insert(
        "builtins".to_owned(),
        MinijinjaValue::from_object(base_builtins),
    );

    let result_store = ResultStore::default();
    context.insert(
        "store_result".to_owned(),
        MinijinjaValue::from_function(result_store.store_result()),
    );
    context.insert(
        "load_result".to_owned(),
        MinijinjaValue::from_function(result_store.load_result()),
    );
    context.insert(
        "store_raw_result".to_owned(),
        MinijinjaValue::from_function(result_store.store_raw_result()),
    );

    if let Some(agate_table) = agate_table {
        context.insert(
            "load_agate_table".to_owned(),
            MinijinjaValue::from_function(move |_args: &[MinijinjaValue]| {
                MinijinjaValue::from_object(agate_table.clone())
            }),
        );
    }

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

    context
}

fn parse_hook_item(item: &serde_json::Value) -> Option<HookConfig> {
    match item {
        serde_json::Value::String(s) => Some(HookConfig {
            sql: s.to_string(),
            transaction: true,
        }),
        serde_json::Value::Object(map) => {
            let sql = map.get("sql")?.as_str()?.to_string();
            let transaction = map
                .get("transaction")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            Some(HookConfig { sql, transaction })
        }
        _ => {
            eprintln!("Pre hook unknown type: {:?}", item);
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
        _listener: Rc<dyn RenderingEventListener>,
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
                    format!("Failed to write file: {}", e),
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
        .join(format!("{}.sql", model_name));
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
