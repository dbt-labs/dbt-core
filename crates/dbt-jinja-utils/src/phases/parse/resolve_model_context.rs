//! This module contains the scope guard for resolving models.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    path::PathBuf,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use chrono::TimeZone;
use chrono_tz::{Europe::London, Tz};
use dbt_common::{io_args::StaticAnalysisKind, serde_utils::convert_json_to_map, FsResult};
use dbt_frontend_common::error::CodeLocation;
use dbt_fusion_adapter::{load_store::ResultStore, relation_object::create_relation};
use dbt_schemas::schemas::{
    common::{Access, DbtMaterialization, ResolvedQuoting},
    project::{DefaultTo, ModelConfig},
    InternalDbtNode,
};
use dbt_schemas::{
    dbt_types::RelationType,
    schemas::{
        common::{DbtChecksum, DbtContract, DbtQuoting, NodeDependsOn},
        CommonAttributes, DbtModel, NodeBaseAttributes,
    },
    state::DbtRuntimeConfig,
};
use minijinja::{
    arg_utils::ArgParser,
    constants::TARGET_UNIQUE_ID,
    listener::RenderingEventListener,
    value::{Object, ObjectRepr, Value as MinijinjaValue, ValueKind},
    Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, State,
};
use minijinja_contrib::modules::{py_datetime::datetime::PyDateTime, pytz::PytzTimezone};

use crate::{jinja_environment::JinjaEnvironment, phases::MacroLookupContext};

use super::sql_resource::SqlResource;

/// To be used as the `execute` flag in resolve-model context
/// This is to record if an `execute` is encountered during parse in a .sql file
#[derive(Debug, Clone)]
struct ParseExecute(Arc<AtomicBool>);

impl Object for ParseExecute {
    fn is_true(self: &Arc<Self>) -> bool {
        self.0.store(true, Ordering::Relaxed);
        false
    }
}

/// Builds a context for resolving models
#[allow(clippy::too_many_arguments)]
pub fn build_resolve_model_context<T: DefaultTo<T> + 'static>(
    config: &T,
    adapter_type: &str,
    database: &str,
    schema: &str,
    model_name: &str,
    fqn: Vec<String>,
    package_name: &str,
    root_project_name: &str,
    package_quoting: DbtQuoting,
    runtime_config: Arc<DbtRuntimeConfig>,
    sql_resources: Arc<Mutex<Vec<SqlResource<T>>>>,
    execute_exists: Arc<AtomicBool>,
) -> BTreeMap<String, MinijinjaValue> {
    // Create a relation for 'this' using config values
    let mut context = BTreeMap::new();
    let this_relation = create_relation(
        adapter_type.to_string(),
        database.to_string(),
        schema.to_string(),
        Some(model_name.to_string()),
        None,
        package_quoting
            .try_into()
            .expect("Failed to convert quoting to resolved quoting"),
    )
    .unwrap()
    .as_value();

    context.insert("this".to_owned(), this_relation);

    // Create a BTreeMap for builtins
    let mut builtins = BTreeMap::new();

    // Create ref function
    let sql_resources_clone = sql_resources.clone();
    let ref_function = ResolveRefFunction {
        database: database.to_string(),
        schema: schema.to_string(),
        adapter_type: adapter_type.to_string(),
        sql_resources: sql_resources_clone,
        runtime_config: runtime_config.clone(),
        package_quoting,
    };
    let ref_value = MinijinjaValue::from_object(ref_function);
    context.insert("ref".to_owned(), ref_value.clone());
    builtins.insert("ref".to_string(), ref_value);

    // Create source function
    let source_function = ResolveSourceFunction {
        database: database.to_string(),
        schema: schema.to_string(),
        sql_resources: sql_resources.clone(),
        adapter_type: adapter_type.to_string(),
        package_quoting,
    };
    let source_value = MinijinjaValue::from_object(source_function);
    context.insert("source".to_owned(), source_value.clone());
    builtins.insert("source".to_string(), source_value);

    let sql_resources_clone = sql_resources.clone();
    context.insert(
        "metric".to_owned(),
        MinijinjaValue::from_function(move |args: &[MinijinjaValue]| {
            if args.is_empty() || args.len() > 3 {
                return Err(MinijinjaError::new(
                    MinijinjaErrorKind::InvalidOperation,
                    "invalid number of arguments for metric macro",
                ));
            }
            let mut parser = ArgParser::new(args, None);
            // If there are two positional args, the first is the package name and the second is the model name
            let arg0 = parser.get::<String>("")?;
            let arg1 = parser.get_optional::<String>("");
            let (package_name, metric_name) = match (arg0, arg1) {
                (package_name, Some(metric_name)) => (Some(package_name), metric_name),
                (metric_name, None) => (None, metric_name),
            };

            // Push the SqlResource with all available information
            sql_resources_clone
                .lock()
                .unwrap()
                .push(SqlResource::Metric((
                    metric_name.clone(),
                    package_name.clone(),
                )));

            // Create and return the DbtMetricReference
            Ok(MinijinjaValue::from_object(ParseMetricReference {
                metric_name,
                _package_name: package_name,
            }))
        }),
    );
    // Register the config function
    sql_resources
        .lock()
        .unwrap()
        .push(SqlResource::Config(Box::new(config.clone())));

    let is_incremental = config.is_incremental();
    context.insert(
        "is_incremental".to_owned(),
        MinijinjaValue::from_function(move |_args: &[MinijinjaValue]| {
            Ok(MinijinjaValue::from(is_incremental))
        }),
    );
    let is_enabled = config.get_enabled().unwrap_or(true);
    context.insert(
        "config".to_owned(),
        MinijinjaValue::from_object(ParseConfig {
            enabled: is_enabled,
            sql_resources: sql_resources.clone(),
        }),
    );
    builtins.insert(
        "config".to_string(),
        MinijinjaValue::from_object(ParseConfig {
            enabled: is_enabled,
            sql_resources,
        }),
    );

    // TODO (Ani): Make this more extensible and depending on the resouce it could be model, macro, or source
    let model = DbtModel {
        common_attr: CommonAttributes {
            database: database.to_string(),
            schema: schema.to_string(),
            name: model_name.to_owned(),
            package_name: package_name.to_owned(),
            path: PathBuf::from(""),
            original_file_path: PathBuf::from(""),
            patch_path: None,
            unique_id: format!("{}.{}", package_name, model_name),
            fqn,
            description: None,
        },
        base_attr: NodeBaseAttributes {
            alias: model_name.to_string(),
            relation_name: None,
            compiled_path: None,
            build_path: None,
            columns: BTreeMap::new(),
            depends_on: NodeDependsOn {
                macros: vec![],
                nodes: vec![],
                nodes_with_ref_location: vec![],
            },
            refs: vec![],
            sources: vec![],
            raw_code: None,
            compiled: None,
            compiled_code: None,
            unrendered_config: BTreeMap::new(),
            doc_blocks: None,
            extra_ctes: None,
            extra_ctes_injected: None,
            metrics: vec![],
            checksum: DbtChecksum::default(),
            language: None,
            contract: DbtContract::default(),
            created_at: None,
        },
        materialized: DbtMaterialization::View,
        quoting: ResolvedQuoting::trues(),
        introspection: None,
        other: BTreeMap::new(),
        version: None,
        latest_version: None,
        constraints: vec![],
        deprecation_date: None,
        primary_key: vec![],
        time_spine: None,
        is_extended_model: false,
        access: Access::default(),
        group: None,
        tags: vec![],
        meta: BTreeMap::new(),
        enabled: true,
        static_analysis: StaticAnalysisKind::On,
        deprecated_config: ModelConfig::default(),
        contract: None,
        incremental_strategy: None,
        freshness: None,
    };

    let mut model_map = convert_json_to_map(model.serialize());
    model_map.insert(
        "batch".to_string(),
        MinijinjaValue::from_object(init_batch_context()),
    );

    context.insert("model".to_owned(), MinijinjaValue::from_object(model_map));

    // Register builtins as a global
    context.insert("builtins".to_owned(), MinijinjaValue::from_object(builtins));

    context.insert("graph".to_owned(), MinijinjaValue::UNDEFINED);

    context.insert(
        TARGET_UNIQUE_ID.to_string(),
        MinijinjaValue::from(format!("{}.{}", package_name, model_name)),
    );

    // Result Store
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

    context.insert(
        "execute".to_owned(),
        MinijinjaValue::from_object(ParseExecute(execute_exists)),
    );

    let mut packages: BTreeSet<String> = runtime_config.dependencies.keys().cloned().collect();
    packages.insert(root_project_name.to_string());

    context.insert(
        "context".to_owned(),
        MinijinjaValue::from_object(MacroLookupContext {
            root_project_name: root_project_name.to_string(),
            current_project_name: package_name.to_string(),
            packages,
        }),
    );

    context
}

/// Batch Context (stubbing this on the fly for now. We'll need to implement this in the future)
fn init_batch_context() -> BTreeMap<String, MinijinjaValue> {
    // TODO: batch map should have valid event_time_start and event_time_end
    // for now, we are just using now
    let datetime = London.with_ymd_and_hms(2025, 1, 1, 1, 1, 1).unwrap();
    let mut batch_map = BTreeMap::new();
    batch_map.insert("id".to_string(), MinijinjaValue::from(""));
    batch_map.insert(
        "event_time_start".to_string(),
        MinijinjaValue::from_object(PyDateTime::new_aware(
            datetime,
            Some(PytzTimezone::new(Tz::UTC)),
        )),
    );
    batch_map.insert(
        "event_time_end".to_string(),
        MinijinjaValue::from_object(PyDateTime::new_aware(
            datetime,
            Some(PytzTimezone::new(Tz::UTC)),
        )),
    );
    batch_map
}

#[derive(Debug)]
struct ResolveRefFunction<T: DefaultTo<T> + 'static> {
    database: String,
    schema: String,
    adapter_type: String,
    sql_resources: Arc<Mutex<Vec<SqlResource<T>>>>,
    runtime_config: Arc<DbtRuntimeConfig>,
    package_quoting: DbtQuoting,
}

impl<T: DefaultTo<T>> Object for ResolveRefFunction<T> {
    fn get_value(self: &Arc<Self>, key: &MinijinjaValue) -> Option<MinijinjaValue> {
        match key.as_str()? {
            "config" => Some(MinijinjaValue::from_object(
                self.runtime_config.to_minijinja_map(),
            )),
            "function_name" => Some(MinijinjaValue::from("ref")),
            _ => None,
        }
    }

    fn call(
        self: &Arc<Self>,
        _state: &State<'_, '_>,
        args: &[MinijinjaValue],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<MinijinjaValue, MinijinjaError> {
        if args.is_empty() || args.len() > 4 {
            return Err(MinijinjaError::new(
                MinijinjaErrorKind::InvalidOperation,
                "invalid number of arguments for ref macro",
            ));
        }
        let mut parser = ArgParser::new(args, None);

        let name: String;
        let mut package: Option<String> = None;

        if parser.positional_len() == 1 {
            name = parser.get::<String>("")?;
        } else if parser.positional_len() == 2 {
            let package_arg = parser.get::<String>("")?;
            let name_arg = parser.get::<String>("")?;
            package = Some(package_arg);
            name = name_arg;
        } else {
            return Err(MinijinjaError::new(
                MinijinjaErrorKind::InvalidOperation,
                "ref() takes at most 2 positional arguments",
            ));
        }

        // Check for version in kwargs
        let version = parser.consume_optional_either_from_kwargs::<String>("version", "v");

        let model_name = name;
        let namespace = package;
        let location: MinijinjaValue = parser.get("location")?;
        let (source_line, source_col, source_index): (usize, usize, usize) = (
            location.get_item_by_index(0).unwrap().as_usize().unwrap(),
            location.get_item_by_index(1).unwrap().as_usize().unwrap(),
            location.get_item_by_index(2).unwrap().as_usize().unwrap(),
        );
        let location = CodeLocation::new(source_line, source_col, source_index);
        self.sql_resources.lock().unwrap().push(SqlResource::Ref((
            model_name.clone(),
            namespace,
            version,
            location,
        )));

        // At resolve time, fqn do not have to be accurate
        Ok(create_relation(
            self.adapter_type.clone(),
            self.database.clone(),
            self.schema.clone(),
            Some(model_name),
            None,
            self.package_quoting
                .try_into()
                .expect("Failed to convert quoting to resolved quoting"),
        )
        .unwrap()
        .as_value())
    }
}

#[derive(Debug)]
struct ResolveSourceFunction<T: DefaultTo<T>> {
    database: String,
    schema: String,
    adapter_type: String,
    sql_resources: Arc<Mutex<Vec<SqlResource<T>>>>,
    package_quoting: DbtQuoting,
}

impl<T: DefaultTo<T>> Object for ResolveSourceFunction<T> {
    fn get_value(self: &Arc<Self>, key: &MinijinjaValue) -> Option<MinijinjaValue> {
        match key.as_str()? {
            "function_name" => Some(MinijinjaValue::from("source")),
            _ => None,
        }
    }

    fn call(
        self: &Arc<Self>,
        _state: &State<'_, '_>,
        args: &[MinijinjaValue],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<MinijinjaValue, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        if args.len() == 3 {
            let name = parser.get::<String>("name")?;
            let table_name = parser.get::<String>("table_name")?;
            let location: MinijinjaValue = parser.get("location")?;
            let (source_line, source_col, source_index): (usize, usize, usize) = (
                location.get_item_by_index(0).unwrap().as_usize().unwrap(),
                location.get_item_by_index(1).unwrap().as_usize().unwrap(),
                location.get_item_by_index(2).unwrap().as_usize().unwrap(),
            );
            let location = CodeLocation::new(source_line, source_col, source_index);
            // https://github.com/dbt-labs/dbt-core/blob/8a8857a85c0cc66c7e3de9eb7e9ca7fd63d553a4/core/dbt/context/providers.py#L666
            // at parse time dbt collects the source but returns a relation populated with the current model
            // TODO: Support Compile+Runtime Source Resolving
            self.sql_resources
                .lock()
                .unwrap()
                .push(SqlResource::Source((
                    name,
                    table_name.to_string(),
                    location,
                )));

            // At resolve time, fqn do not have to be accurate
            Ok(create_relation(
                self.adapter_type.clone(),
                self.database.clone(),
                self.schema.clone(),
                Some(table_name),
                Some(RelationType::External),
                self.package_quoting
                    .try_into()
                    .expect("Failed to convert quoting to resolved quoting"),
            )
            .unwrap()
            .as_value())
        } else {
            Err(MinijinjaError::new(
                MinijinjaErrorKind::InvalidOperation,
                "source requires 2 string arguments",
            ))
        }
    }
}

/// A struct that represents a parse metric reference object returned by the `metric` macro during parsing
pub struct ParseMetricReference {
    /// Name of the metric, e.g. `metric('metric_name')`
    pub metric_name: String,
    /// Name of the package, if provided e.g. `metric('package_name', 'metric_name')`
    pub _package_name: Option<String>,
}

impl Object for ParseMetricReference {
    fn repr(self: &Arc<Self>) -> ObjectRepr {
        ObjectRepr::Plain
    }
}

impl Debug for ParseMetricReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.metric_name)
    }
}

/// A struct that represents a parse config object to be used during parsing
#[derive(Debug)]
pub struct ParseConfig<T: DefaultTo<T> + 'static> {
    /// A pointer to a vector of sql resources to be collected during parsing
    pub sql_resources: Arc<Mutex<Vec<SqlResource<T>>>>,
    /// Whether the model is enabled (based on upstream config)
    pub enabled: bool,
}

impl<T: DefaultTo<T>> Object for ParseConfig<T> {
    /// Implement the call method on the config object
    fn call(
        self: &Arc<Self>,
        _state: &State<'_, '_>,
        args: &[MinijinjaValue],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<MinijinjaValue, MinijinjaError> {
        let mut args = ArgParser::new(args, None);
        // If there is a positional argument, it must be a map
        let kwargs = if args.positional_len() == 1 {
            let positional_val: MinijinjaValue = args.next_positional::<MinijinjaValue>()?;
            if positional_val.kind() != ValueKind::Map {
                return Err(MinijinjaError::new(
                    MinijinjaErrorKind::InvalidOperation,
                    format!(
                        "Invalid config argument kind specified: {}",
                        positional_val.kind()
                    ),
                ));
            }
            positional_val
                .as_object()
                .unwrap()
                .try_iter_pairs()
                .expect("Invalid config object specified")
                .map(|(k, v)| {
                    (
                        k.as_str()
                            .expect("Invalid config object specified. Keys must be strings")
                            .to_string(),
                        v,
                    )
                })
                .collect()
        } else {
            args.drain_kwargs()
        };

        let mut result = BTreeMap::new();
        for key in kwargs.keys() {
            let value: &MinijinjaValue = kwargs.get(key).unwrap();
            if value.is_undefined() {
                return Err(minijinja::Error::new(
                    minijinja::ErrorKind::InvalidOperation,
                    "config requires all arguments to be defined",
                ));
            }

            let value = if let Some(dyn_obj) = value.as_object() {
                if let Some(pydatetime) = dyn_obj.downcast::<PyDateTime>() {
                    MinijinjaValue::from_serialize(pydatetime.chrono_dt())
                } else {
                    value.clone()
                }
            } else {
                value.clone()
            };

            result.insert(key.to_string(), value);
        }

        // Get or insert enabled
        let enabled = result
            .remove("enabled")
            .unwrap_or(MinijinjaValue::from(self.enabled));
        result.insert("enabled".to_string(), enabled.clone());

        let value = serde_json::to_value(result).map_err(|e| {
            MinijinjaError::new(
                MinijinjaErrorKind::InvalidOperation,
                format!("Failed to serialize config into json: {}", e),
            )
        })?;
        let config: T = serde_json::from_value(value).map_err(|e| {
            MinijinjaError::new(
                MinijinjaErrorKind::InvalidOperation,
                format!("Failed to parse node configuration: {}", e),
            )
        })?;
        self.sql_resources
            .lock()
            .unwrap()
            .push(SqlResource::Config(Box::new(config)));
        if !enabled.is_true() {
            return Err(MinijinjaError::new(
                MinijinjaErrorKind::DisabledModel,
                "Model is disabled".to_string(),
            ));
        }
        Ok(MinijinjaValue::UNDEFINED)
    }

    fn call_method(
        self: &Arc<Self>,
        _state: &State<'_, '_>,
        name: &str,
        args: &[MinijinjaValue],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<MinijinjaValue, MinijinjaError> {
        match name {
            // At compile time, this will return the value of the config variable if it exists
            // Here, we just return an empty string
            "get" => {
                let mut args = ArgParser::new(args, None);
                let _: String = args.get("name")?;
                Ok(MinijinjaValue::from(""))
            }
            // At compile time, this just returns an empty string
            "set" => {
                let mut args = ArgParser::new(args, None);
                let _: String = args.get("name")?;
                Ok(MinijinjaValue::from(""))
            }
            // At compile time, this will throw an error if the config required does not exist
            "require" => {
                let mut args = ArgParser::new(args, None);
                let _: String = args.get("name")?;
                Ok(MinijinjaValue::from(""))
            }
            _ => Err(MinijinjaError::new(
                MinijinjaErrorKind::UnknownMethod("ParseConfig".to_string(), name.to_string()),
                format!("Unknown method on parse: {}", name),
            )),
        }
    }
}

/// Render a reference or source string and return the corresponding SqlResource
pub fn render_extract_ref_or_source_expr<T: DefaultTo<T>>(
    jinja_env: &JinjaEnvironment<'static>,
    resolve_model_context: &BTreeMap<String, MinijinjaValue>,
    sql_resources: Arc<Mutex<Vec<SqlResource<T>>>>,
    ref_str: &str,
) -> FsResult<SqlResource<T>> {
    let expr = jinja_env.compile_expression(ref_str)?;
    let _ = expr.eval(resolve_model_context, &[])?;
    // Remove from Mutex and return last item
    let mut sql_resources = sql_resources.lock().unwrap();
    let sql_resource = sql_resources.pop().unwrap();
    Ok(sql_resource)
}

#[cfg(test)]
mod test {
    use dbt_schemas::schemas::relations::DEFAULT_DBT_QUOTING;

    use super::*;
    #[test]
    fn test_resolve_source_function_rendering() {
        let adapter_type = "postgres".to_string();
        let sql_resources = Arc::new(Mutex::new(Vec::new()));

        // Create a minijinja environment to test rendering
        let mut env = minijinja::Environment::new();

        let source_function: ResolveSourceFunction<ModelConfig> = ResolveSourceFunction {
            database: "test_db".to_string(),
            schema: "test_schema".to_string(),
            sql_resources,
            adapter_type,
            package_quoting: DEFAULT_DBT_QUOTING,
        };
        let source_value = MinijinjaValue::from_object(source_function);
        env.add_global("source", source_value);

        // Create a template that uses the source function
        let template = env
            .template_from_str("{{ source('my_source', 'my_table').render() }}")
            .unwrap();

        // Render the template
        let result = template.render(minijinja::context!(), &[]).unwrap();

        assert!(result.contains("test_db"));
        assert!(result.contains("test_schema"));
        assert!(result.contains("my_table"));
    }
}
