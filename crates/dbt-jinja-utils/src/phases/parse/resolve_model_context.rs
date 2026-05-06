//! This module contains the scope guard for resolving models.

use dbt_adapter_core::AdapterType;
use indexmap::IndexMap;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    path::{Path, PathBuf},
    rc::Rc,
    sync::{Arc, Mutex, atomic::AtomicBool},
};

use chrono::TimeZone;
use chrono_tz::{Europe::London, Tz};
use dbt_adapter::{cast_util::THIS_RELATION_KEY, load_store::ResultStore};
use dbt_common::{
    io_args::{IoArgs, StaticAnalysisKind},
    serde_utils::convert_yml_to_value_map,
};
use dbt_frontend_common::error::CodeLocation;
use dbt_schemas::schemas::{
    DbtModelAttr, InternalDbtNode, IntrospectionKind,
    common::{Access, ResolvedQuoting},
    nodes::AdapterAttr,
    project::{ModelConfig, ResolvableConfig},
};
use dbt_schemas::{
    dbt_types::RelationType,
    schemas::{
        CommonAttributes, DbtModel, NodeBaseAttributes,
        common::{DbtChecksum, DbtQuoting, NodeDependsOn},
        serde::yml_value_to_minijinja,
    },
    state::DbtRuntimeConfig,
};
use minijinja::{
    Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, State,
    arg_utils::ArgParser,
    constants::{CURRENT_PATH, CURRENT_SPAN, TARGET_UNIQUE_ID},
    listener::RenderingEventListener,
    machinery::Span,
    value::{
        Enumerator, Object, ObjectRepr, Value as MinijinjaValue, ValueKind,
        function_object::FunctionObject,
    },
};
use minijinja_contrib::modules::{py_datetime::datetime::PyDateTime, pytz::PytzTimezone};
use serde::Serialize;

use dbt_jinja_ctx::{JinjaObject, ParseExecute, ResolveModelCtx, to_jinja_btreemap};

use crate::{phases::MacroLookupContext, serde::into_typed_with_error};

use super::sql_resource::SqlResource;

/// Builds a context for resolving models
#[allow(clippy::too_many_arguments)]
pub fn build_resolve_model_context<T: ResolvableConfig<T> + Serialize + 'static>(
    config: &T,
    adapter_type: AdapterType,
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
    display_path: &Path,
    io_args: &IoArgs,
    global_static_analysis: Option<StaticAnalysisKind>,
) -> BTreeMap<String, MinijinjaValue> {
    // Create a relation for 'this' using config values
    let sql_resources_clone = sql_resources.clone();
    let this_relation = ResolveThisFunction {
        relation: dbt_adapter::relation::RelationObject::new(Arc::from(
            dbt_adapter::relation::do_create_relation(
                adapter_type,
                database.to_string(),
                schema.to_string(),
                Some(model_name.to_string()),
                None,
                package_quoting
                    .try_into()
                    .expect("Failed to convert quoting to resolved quoting"),
            )
            .unwrap(),
        ))
        .into_value(),
        sql_resources: sql_resources_clone,
    };
    let this_value = MinijinjaValue::from_object(this_relation);

    // Create a BTreeMap for builtins
    let mut builtins = BTreeMap::new();

    // Create ref function
    let sql_resources_clone = sql_resources.clone();
    let ref_function = ResolveRefFunction {
        database: database.to_string(),
        schema: schema.to_string(),
        adapter_type,
        sql_resources: sql_resources_clone,
        runtime_config: runtime_config.clone(),
        package_quoting,
    };
    let ref_value = MinijinjaValue::from_object(ref_function);
    builtins.insert("ref".to_string(), ref_value.clone());

    // Create source function
    let source_function = ResolveSourceFunction {
        database: database.to_string(),
        schema: schema.to_string(),
        sql_resources: sql_resources.clone(),
        adapter_type,
        package_quoting,
    };
    let source_value = MinijinjaValue::from_object(source_function);
    builtins.insert("source".to_string(), source_value.clone());

    // Create function function
    let function_function = ResolveFunctionFunction {
        database: database.to_string(),
        schema: schema.to_string(),
        sql_resources: sql_resources.clone(),
        adapter_type,
        package_quoting,
    };
    let function_value = MinijinjaValue::from_object(function_function);
    builtins.insert("function".to_string(), function_value.clone());

    let sql_resources_clone = sql_resources.clone();
    let metric_value = MinijinjaValue::from_function(move |args: &[MinijinjaValue]| {
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
    });
    let package_dependency = if package_name == root_project_name {
        None
    } else {
        Some(package_name.to_string())
    };
    // Pre-finalization read: used to initialize the Jinja context before rendering starts,
    // before the root overlay has been applied. The optimistic `true` default is intentional.
    let is_enabled = config.get_enabled_with_default();
    let config_value = MinijinjaValue::from_object(ParseConfig {
        enabled: is_enabled,
        sql_resources: sql_resources.clone(),
        io_args: io_args.clone().into(),
        package_dependency: package_dependency.clone(),
        error_path: Some(display_path.to_path_buf()),
    });
    builtins.insert(
        "config".to_string(),
        MinijinjaValue::from_object(ParseConfig {
            enabled: is_enabled,
            sql_resources,
            io_args: io_args.clone().into(),
            package_dependency,
            error_path: Some(display_path.to_path_buf()),
        }),
    );

    // TODO (Ani): Make this more extensible and depending on the resouce it could be model, macro, or source
    let model = DbtModel {
        __common_attr__: CommonAttributes {
            name: model_name.to_owned(),
            package_name: package_name.to_owned(),
            path: PathBuf::from(""),
            name_span: dbt_common::Span::default(),
            original_file_path: PathBuf::from(""),
            patch_path: None,
            unique_id: format!("{package_name}.{model_name}"),
            fqn,
            description: None,
            raw_code: None,
            checksum: DbtChecksum::default(),
            language: None,
            tags: vec![],
            meta: IndexMap::new(),
        },
        __base_attr__: NodeBaseAttributes {
            database: database.to_string(),
            schema: schema.to_string(),
            alias: model_name.to_string(),
            relation_name: None,
            materialized: ModelConfig::default_materialized(),
            static_analysis: global_static_analysis.unwrap_or_default().into(),
            static_analysis_off_reason: None,
            enabled: true,
            extended_model: false,
            persist_docs: None,
            quoting: ResolvedQuoting::trues(),
            quoting_ignore_case: false,
            columns: vec![],
            depends_on: NodeDependsOn {
                macros: vec![],
                nodes: vec![],
                nodes_with_ref_location: vec![],
            },
            refs: vec![],
            sources: vec![],
            functions: vec![],
            metrics: vec![],
            unrendered_config: Default::default(),
        },
        __model_attr__: DbtModelAttr {
            introspection: IntrospectionKind::None,
            version: None,
            latest_version: None,
            constraints: vec![],
            deprecation_date: None,
            primary_key: vec![],
            time_spine: None,
            access: Access::default(),
            group: None,
            incremental_strategy: None,
            freshness: None,
            contract: None,
            event_time: None,
            catalog_name: None,
            table_format: None,
            sync: None,
        },
        __adapter_attr__: AdapterAttr::default(),
        __other__: BTreeMap::new(),
        deprecated_config: ModelConfig::default(),
    };

    let mut model_map = convert_yml_to_value_map(InternalDbtNode::serialize(&model));
    // Stub `DbtModel` uses `ModelConfig::default()` for `config` in YAML serialization. At parse
    // time, kwargs to `config(...)` (e.g. `post_hook=my_macro(model)`) are evaluated while
    // rendering; macros must see the merged node config (`properties_config` / `BaseConfig`),
    // matching dbt-core (dbt-fusion#1414).
    //
    // Use `dbt_yaml::to_value` + `yml_value_to_minijinja` — same pipeline as
    // `DbtModel::serialized_config()` — not `MinijinjaValue::from_serialize`, so later
    // `dbt_yaml::to_value(model)` → `InternalDbtNodeWrapper::deserialize` in adapter helpers
    // (`get_view_options`, `get_config_from_model`, …) round-trips correctly.
    let config_yml = dbt_yaml::to_value(config)
        .expect("Failed to serialize merged node config to dbt_yaml::Value for parse model.config");
    model_map.insert("config".to_owned(), yml_value_to_minijinja(config_yml));
    model_map.insert(
        "batch".to_owned(),
        MinijinjaValue::from_object(init_batch_context()),
    );

    let result_store = ResultStore::default();
    let mut packages: BTreeSet<String> = runtime_config.dependencies.keys().cloned().collect();
    packages.insert(root_project_name.to_string());

    // Object-typed slots are wrapped via `MinijinjaValue::from_object(...)` /
    // `MinijinjaValue::from_function(closure)` HERE rather than in the typed
    // ctx struct, because going through serde's `serialize_map` /
    // `serialize_seq` paths would change the underlying Object's concrete
    // type. `model` and `builtins` in particular get downcast to
    // `BTreeMap<String, MinijinjaValue>` by compile/run-node-context code;
    // the original `Vec<String>` regression in `MACRO_DISPATCH_ORDER`
    // (dbt-fusion#…) showed why this matters. See `ResolveModelCtx`'s
    // doc comment.
    let ctx = ResolveModelCtx {
        this: this_value,
        ref_fn: ref_value,
        source: source_value,
        function: function_value,
        metric: metric_value,
        config: config_value,
        model: MinijinjaValue::from_object(model_map),
        builtins: MinijinjaValue::from_object(builtins),
        graph: MinijinjaValue::UNDEFINED,
        store_result: MinijinjaValue::from_function(result_store.store_result()),
        load_result: MinijinjaValue::from_function(result_store.load_result()),
        store_raw_result: MinijinjaValue::from_function(result_store.store_raw_result()),
        execute: JinjaObject::new(ParseExecute::new(execute_exists)),
        context: JinjaObject::new(MacroLookupContext {
            root_project_name: root_project_name.to_string(),
            current_project_name: None,
            packages,
        }),
        target_unique_id: format!("{package_name}.{model_name}"),
        current_path: display_path.to_string_lossy().into_owned(),
        current_span: MinijinjaValue::from_serialize(Span::default()),
    };

    // Sanity: the constants downstream code uses to look up these keys must
    // match the field-rename strings on `ResolveModelCtx`. Compile-time
    // assertions; no runtime cost.
    debug_assert_eq!(TARGET_UNIQUE_ID, "TARGET_UNIQUE_ID");
    debug_assert_eq!(CURRENT_PATH, "__minijinja_current_path");
    debug_assert_eq!(CURRENT_SPAN, "__minijinja_current_span");

    to_jinja_btreemap(&ctx)
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
struct ResolveRefFunction<T: ResolvableConfig<T> + 'static> {
    database: String,
    schema: String,
    adapter_type: AdapterType,
    sql_resources: Arc<Mutex<Vec<SqlResource<T>>>>,
    runtime_config: Arc<DbtRuntimeConfig>,
    package_quoting: DbtQuoting,
}

impl<T: ResolvableConfig<T>> Object for ResolveRefFunction<T> {
    fn get_value(self: &Arc<Self>, key: &MinijinjaValue) -> Option<MinijinjaValue> {
        match key.as_str()? {
            "config" => Some(MinijinjaValue::from_dyn_object(self.runtime_config.clone())),
            "function_name" => Some(MinijinjaValue::from("ref")),
            _ => None,
        }
    }

    fn call(
        self: &Arc<Self>,
        state: &State<'_, '_>,
        args: &[MinijinjaValue],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<MinijinjaValue, MinijinjaError> {
        if args.is_empty() || args.len() > 3 {
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
        let span = state.current_instruction_span();
        let location = CodeLocation::new(span.start_line, span.start_col, span.start_offset);
        self.sql_resources.lock().unwrap().push(SqlResource::Ref((
            model_name.clone(),
            namespace,
            version,
            location,
        )));

        let relation = dbt_adapter::relation::RelationObject::new(Arc::from(
            dbt_adapter::relation::do_create_relation(
                self.adapter_type,
                self.database.clone(),
                self.schema.clone(),
                Some(model_name),
                None,
                self.package_quoting
                    .try_into()
                    .expect("Failed to convert quoting to resolved quoting"),
            )
            .unwrap(),
        ))
        .into_value();
        // At resolve time, fqn do not have to be accurate
        Ok(relation)
    }
}

#[derive(Debug)]
struct ResolveSourceFunction<T: ResolvableConfig<T>> {
    database: String,
    schema: String,
    adapter_type: AdapterType,
    sql_resources: Arc<Mutex<Vec<SqlResource<T>>>>,
    package_quoting: DbtQuoting,
}

impl<T: ResolvableConfig<T>> Object for ResolveSourceFunction<T> {
    fn get_value(self: &Arc<Self>, key: &MinijinjaValue) -> Option<MinijinjaValue> {
        match key.as_str()? {
            "function_name" => Some(MinijinjaValue::from("source")),
            _ => None,
        }
    }

    fn call(
        self: &Arc<Self>,
        state: &State<'_, '_>,
        args: &[MinijinjaValue],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<MinijinjaValue, MinijinjaError> {
        let mut parser = ArgParser::new(args, None);
        if args.len() == 2 {
            let name = parser.get::<String>("name")?;
            let table_name = parser.get::<String>("table_name")?;
            let span = state.current_instruction_span();
            let location = CodeLocation::new(span.start_line, span.start_col, span.start_offset);
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
            Ok(dbt_adapter::relation::RelationObject::new(Arc::from(
                dbt_adapter::relation::do_create_relation(
                    self.adapter_type,
                    self.database.clone(),
                    self.schema.clone(),
                    Some(table_name),
                    Some(RelationType::External),
                    self.package_quoting
                        .try_into()
                        .expect("Failed to convert quoting to resolved quoting"),
                )
                .unwrap(),
            ))
            .into_value())
        } else {
            Err(MinijinjaError::new(
                MinijinjaErrorKind::InvalidOperation,
                "source requires 2 string arguments",
            ))
        }
    }
}

#[derive(Debug)]
struct ResolveFunctionFunction<T: ResolvableConfig<T>> {
    database: String,
    schema: String,
    adapter_type: AdapterType,
    sql_resources: Arc<Mutex<Vec<SqlResource<T>>>>,
    package_quoting: DbtQuoting,
}

impl<T: ResolvableConfig<T>> Object for ResolveFunctionFunction<T> {
    fn get_value(self: &Arc<Self>, key: &MinijinjaValue) -> Option<MinijinjaValue> {
        match key.as_str()? {
            "function_name" => Some(MinijinjaValue::from("function")),
            _ => None,
        }
    }

    fn call(
        self: &Arc<Self>,
        state: &State<'_, '_>,
        args: &[MinijinjaValue],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<MinijinjaValue, MinijinjaError> {
        if args.is_empty() || args.len() > 2 {
            return Err(MinijinjaError::new(
                MinijinjaErrorKind::InvalidOperation,
                "invalid number of arguments for function macro",
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
                "function() takes at most 2 positional arguments",
            ));
        }

        let function_name = name;
        let namespace = package;
        let span = state.current_instruction_span();
        let location = CodeLocation::new(span.start_line, span.start_col, span.start_offset);
        self.sql_resources
            .lock()
            .unwrap()
            .push(SqlResource::Function((
                function_name.clone(),
                namespace,
                location,
            )));

        let relation = dbt_adapter::relation::do_create_relation(
            self.adapter_type,
            self.database.clone(),
            self.schema.clone(),
            Some(function_name),
            None,
            self.package_quoting
                .try_into()
                .expect("Failed to convert quoting to resolved quoting"),
        )
        .unwrap();

        // Create a FunctionObject instead of returning the relation directly
        let qualified_name = relation.render_self_as_str();
        let function_object = FunctionObject::new(qualified_name);

        Ok(function_object.into_value())
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

/// A stub value returned by `config.get()` during parsing.
///
/// We haven't populated `config` yet, so this stub value could be any type;
/// therefore any method could be called on it. This prevents a crash during parse.
///
/// TODO: This is not a complete fix. If someone is using the config value in a
/// type-specific way, there could still be issues.
#[derive(Debug)]
struct ParseConfigValue;

impl Object for ParseConfigValue {
    fn get_value(self: &Arc<Self>, _key: &MinijinjaValue) -> Option<MinijinjaValue> {
        Some(MinijinjaValue::from_object(ParseConfigValue))
    }

    fn call_method(
        self: &Arc<Self>,
        _state: &State<'_, '_>,
        _name: &str,
        _args: &[MinijinjaValue],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<MinijinjaValue, MinijinjaError> {
        Ok(MinijinjaValue::from_object(ParseConfigValue))
    }

    fn render(self: &Arc<Self>, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "")
    }
}

/// A struct that represents a parse config object to be used during parsing
#[derive(Debug)]
pub struct ParseConfig<T: ResolvableConfig<T> + 'static> {
    /// A pointer to a vector of sql resources to be collected during parsing
    pub sql_resources: Arc<Mutex<Vec<SqlResource<T>>>>,
    /// Whether the model is enabled (based on upstream config)
    pub enabled: bool,
    /// IoArgs to be used for error reporting
    pub io_args: Arc<IoArgs>,
    // Current package name
    pub package_dependency: Option<String>,
    /// Error path to be used for error reporting
    pub error_path: Option<PathBuf>,
}

impl<T: ResolvableConfig<T>> Object for ParseConfig<T> {
    /// Implement the call method on the config object
    fn call(
        self: &Arc<Self>,
        state: &State<'_, '_>,
        args: &[MinijinjaValue],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<MinijinjaValue, MinijinjaError> {
        let mut args = ArgParser::new(args, None);
        // If there is a positional argument, it must be a map
        let mut kwargs = if args.positional_len() == 1 {
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

        let enabled = if !kwargs.contains_key("enabled") {
            kwargs.insert("enabled".to_string(), MinijinjaValue::from(self.enabled));
            self.enabled
        } else {
            kwargs.get("enabled").unwrap().is_true()
        };
        // TODO: propgate span info for individual args
        let span = {
            let Span {
                start_line,
                start_col,
                start_offset,
                end_line,
                end_col,
                end_offset,
            } = state.current_instruction_span();
            dbt_yaml::Span {
                start: dbt_yaml::Marker::new(
                    start_offset as usize,
                    start_line as usize,
                    start_col as usize,
                ),
                end: dbt_yaml::Marker::new(
                    end_offset as usize,
                    end_line as usize,
                    end_col as usize,
                ),
                filename: self.error_path.as_ref().map(|p| Arc::new(p.to_path_buf())),
            }
        };

        let mut mapping = dbt_yaml::Mapping::with_capacity(kwargs.len());
        for (key, value) in kwargs.into_iter() {
            if value.is_undefined() {
                return Err(minijinja::Error::new(
                    minijinja::ErrorKind::InvalidOperation,
                    "config requires all arguments to be defined",
                ));
            }

            let value = if let Some(dyn_obj) = value.as_object()
                && let Some(pydatetime) = dyn_obj.downcast::<PyDateTime>()
            {
                dbt_yaml::to_value(pydatetime.chrono_dt())
            } else {
                dbt_yaml::to_value(value)
            }
            .map_err(|e| {
                MinijinjaError::new(
                    MinijinjaErrorKind::InvalidOperation,
                    format!("Failed to serialize config into yaml: {e}"),
                )
            })?
            .with_span(span.clone());

            mapping.insert(dbt_yaml::Value::String(key, span.clone()), value);
        }

        let yaml_value = dbt_yaml::Value::Mapping(mapping, span);
        let config: T = into_typed_with_error(
            &self.io_args,
            yaml_value,
            true,
            self.package_dependency.as_deref(),
            self.error_path.clone(),
        )
        .map_err(|e| {
            MinijinjaError::new(
                MinijinjaErrorKind::InvalidOperation,
                format!("Failed to parse node configuration: {e}"),
            )
        })?;
        self.sql_resources
            .lock()
            .unwrap()
            .push(SqlResource::ConfigCall(Box::new(config)));
        if !enabled {
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
            // At compile time, this will return the value of the config variable if it exists.
            // During parse, config isn't populated yet.  If the caller supplied a default
            // value we return it so that downstream code that requires a concrete type
            // (e.g. adapter.get_relation(identifier=...)) receives a usable value.
            // Without a default we fall back to the ParseConfigValue stub.
            "get" => {
                let mut args = ArgParser::new(args, None);
                let _: String = args.get("name")?;
                if let Some(default) = args.get_optional::<MinijinjaValue>("default") {
                    Ok(default)
                } else {
                    Ok(MinijinjaValue::from_object(ParseConfigValue))
                }
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
            // At parse time, this just returns an empty string (for consistency with compile/run configs)
            "meta_get" => {
                let mut args = ArgParser::new(args, None);
                let _: String = args.get("name")?;
                Ok(MinijinjaValue::from(""))
            }
            // At parse time, this just returns an empty string (for consistency with compile/run configs)
            "meta_require" => {
                let mut args = ArgParser::new(args, None);
                let _: String = args.get("name")?;
                Ok(MinijinjaValue::from(""))
            }
            // At parse time, return false (no column docs persistence during parsing)
            "persist_column_docs" => Ok(MinijinjaValue::from(false)),
            // At parse time, return false (no relation docs persistence during parsing)
            "persist_relation_docs" => Ok(MinijinjaValue::from(false)),
            _ => Err(MinijinjaError::new(
                MinijinjaErrorKind::UnknownMethod,
                format!("Unknown method on parse: {name}"),
            )),
        }
    }
}

#[derive(Debug)]
struct ResolveThisFunction<T: ResolvableConfig<T> + 'static> {
    relation: MinijinjaValue,
    sql_resources: Arc<Mutex<Vec<SqlResource<T>>>>,
}

impl<T: ResolvableConfig<T>> Object for ResolveThisFunction<T> {
    fn call_method(
        self: &Arc<Self>,
        state: &State<'_, '_>,
        name: &str,
        args: &[MinijinjaValue],
        listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<MinijinjaValue, MinijinjaError> {
        self.relation
            .as_object()
            .expect("Failed to convert relation to object")
            .call_method(state, name, args, listeners)
    }

    fn get_value(self: &Arc<Self>, key: &MinijinjaValue) -> Option<MinijinjaValue> {
        // This is a special case for the this relation macro to be able to downcast this to a RelationObject
        if key.as_str() == Some(THIS_RELATION_KEY) {
            return Some(self.relation.clone());
        }
        // TODO: This can be more fine-grained (i.e. if the user only asks for database etc.)
        if key.as_str()? == "database" || key.as_str()? == "schema" {
            self.sql_resources.lock().unwrap().push(SqlResource::This);
        }
        self.relation
            .as_object()
            .expect("Failed to convert relation to object")
            .get_value(key)
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        self.relation
            .as_object()
            .expect("Failed to convert relation to object")
            .enumerate()
    }

    fn render(self: &Arc<Self>, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.sql_resources.lock().unwrap().push(SqlResource::This);
        self.relation
            .as_object()
            .expect("Failed to convert relation to object")
            .render(f)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use dbt_schemas::schemas::relations::DEFAULT_DBT_QUOTING;
    use dbt_test_primitives::assert_contains;
    #[test]
    fn test_resolve_source_function_rendering() {
        let sql_resources = Arc::new(Mutex::new(Vec::new()));

        // Create a minijinja environment to test rendering
        let mut env = minijinja::Environment::new();

        let source_function: ResolveSourceFunction<ModelConfig> = ResolveSourceFunction {
            database: "test_db".to_string(),
            schema: "test_schema".to_string(),
            sql_resources,
            adapter_type: AdapterType::Postgres,
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

        assert_contains!(result, "test_db");
        assert_contains!(result, "test_schema");
        assert_contains!(result, "my_table");
    }

    /// Creates a ParseConfig for use in unit tests.
    fn make_test_parse_config() -> ParseConfig<ModelConfig> {
        ParseConfig {
            sql_resources: Arc::new(Mutex::new(Vec::new())),
            enabled: true,
            io_args: Arc::new(IoArgs::default()),
            package_dependency: None,
            error_path: None,
        }
    }

    /// Regression test: config.get('key', 'default') should return the default
    /// value during parse phase, not a ParseConfigValue stub.
    /// Without this, passing the result to adapter.get_relation(identifier=...)
    /// fails with "incompatible type ParseConfigValue; value is not a string".
    #[test]
    fn test_config_get_with_default_returns_default() {
        let mut env = minijinja::Environment::new();

        let config = make_test_parse_config();
        env.add_global("config", MinijinjaValue::from_object(config));

        // config.get('alias', 'my_fallback') should render as "my_fallback"
        let template = env
            .template_from_str("{{ config.get('alias', 'my_fallback') }}")
            .unwrap();
        let result = template.render(minijinja::context!(), &[]).unwrap();

        assert_eq!(result, "my_fallback");
    }

    /// Regression test: the rendered default from config.get must be usable as
    /// a string argument (i.e. pass Value::as_str()).  This simulates what
    /// adapter.get_relation(identifier=config.get('alias', 'tbl')) does.
    #[test]
    fn test_config_get_default_is_string_typed() {
        let mut env = minijinja::Environment::new();

        let config = make_test_parse_config();
        env.add_global("config", MinijinjaValue::from_object(config));

        // Evaluate config.get with a default and inspect the Value directly
        let expr = env
            .compile_expression("config.get('alias', 'tbl')")
            .unwrap();
        let value = expr.eval(minijinja::context!(), &[]).unwrap();

        assert!(
            value.as_str().is_some(),
            "config.get with a default must return a string-typed Value, got: {:?}",
            value
        );
        assert_eq!(value.as_str().unwrap(), "tbl");
    }

    /// Verify that config.get('key') without a default still returns a
    /// ParseConfigValue stub (preserving backward compatibility).
    #[test]
    fn test_config_get_without_default_returns_stub() {
        let mut env = minijinja::Environment::new();

        let config = make_test_parse_config();
        env.add_global("config", MinijinjaValue::from_object(config));

        // Without a default, should render as empty string (ParseConfigValue::render)
        let template = env.template_from_str("{{ config.get('alias') }}").unwrap();
        let result = template.render(minijinja::context!(), &[]).unwrap();

        assert_eq!(result, "");
    }
}
