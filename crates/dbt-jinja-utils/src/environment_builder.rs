use crate::{
    functions::register_base_functions, jinja_environment::JinjaEnv, listener::ListenerFactory,
};
use dbt_common::{FsError, FsResult, io_args::IoArgs, unexpected_fs_err};
use dbt_fusion_adapter::BaseAdapter;
use minijinja::{
    Environment, Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, Value,
    constants::{
        DBT_AND_ADAPTERS_NAMESPACE, MACRO_NAMESPACE_REGISTRY, MACRO_TEMPLATE_REGISTRY,
        NON_INTERNAL_PACKAGES, ROOT_PACKAGE_NAME,
    },
    dispatch_object::get_internal_packages,
    macro_unit::MacroUnit,
    value::ValueKind,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::{path::Path, sync::Arc};

type PackageName = String;

/// A wrapper struct that contains a map of macros.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct MacroUnitsWrapper {
    /// A map of macros.
    pub macros: BTreeMap<PackageName, Vec<MacroUnit>>,
}

impl MacroUnitsWrapper {
    /// Create a new MacrosWrapper.
    pub fn new(macros: BTreeMap<PackageName, Vec<MacroUnit>>) -> Self {
        Self { macros }
    }
}

/// A builder struct that configures and returns a Minijinja Environment.
/// You can add additional fields and methods as needed.
// Default Jinja Env Behaves differently than Envirornment::new()
pub struct JinjaEnvBuilder {
    env: Environment<'static>,
    adapter: Option<Arc<dyn BaseAdapter>>,
    globals: BTreeMap<String, Value>,
    root_package: Option<String>,
    io_args: IoArgs,
}

impl JinjaEnvBuilder {
    /// Create a new JinjaEnvBuilder with a default Environment.
    pub fn new() -> Self {
        Self {
            env: Environment::new(),
            adapter: None,
            globals: BTreeMap::new(),
            root_package: None,
            io_args: IoArgs::default(),
        }
    }

    pub fn with_root_package(mut self, root_package: String) -> Self {
        self.root_package = Some(root_package);
        self.env.add_global(
            ROOT_PACKAGE_NAME,
            Value::from(self.root_package.clone().unwrap()),
        );
        self
    }

    /// Specify an adapter type (e.g. "parse", "compile") or other distinguishing feature.
    pub fn with_adapter(mut self, adapter: Arc<dyn BaseAdapter>) -> Self {
        self.adapter = Some(adapter);
        self
    }

    /// Add a global to the environment.
    /// TODO: create a typed struct to validate we recieve all the globals we need
    pub fn with_globals(mut self, globals: BTreeMap<String, Value>) -> Self {
        self.globals = globals;
        self
    }

    /// Add IoArgs
    pub fn with_io_args(mut self, io_args: IoArgs) -> Self {
        self.io_args = io_args;
        self
    }

    /// Register macros with the environment.
    pub fn try_with_macros(
        mut self,
        macros: MacroUnitsWrapper,
        listener_factory: Option<Arc<dyn ListenerFactory>>,
    ) -> FsResult<Self> {
        let adapter = self.adapter.as_ref().ok_or_else(|| {
            unexpected_fs_err!("try_with_macros requires adapter configuration to be set")
        })?;

        // Get the root package name
        let root_package = self
            .root_package
            .clone()
            .ok_or_else(|| unexpected_fs_err!("try_with_macros requires root package to be set"))?;

        // Get internal packages for this adapter
        let internal_packages = get_internal_packages(adapter.adapter_type().as_ref());

        // Initialize all registries
        let mut macro_namespace_registry = BTreeMap::new(); // package_name → [macro_names]
        let mut macro_template_registry = BTreeMap::new(); // template_name → macro_info

        let mut non_internal_packages: BTreeMap<Value, Value> = BTreeMap::new(); // package_name → [macro_names]
        let mut internal_packages_macros: BTreeMap<String, BTreeMap<String, Value>> =
            BTreeMap::new(); // package_name → {macro_name → info}

        // Process all macros
        for (package_name, macro_units) in macros.macros {
            // Add package to namespace registry
            macro_namespace_registry.insert(
                Value::from(package_name.clone()),
                Value::from_serialize(
                    macro_units
                        .iter()
                        .map(|m| m.info.name.clone())
                        .collect::<Vec<_>>(),
                ),
            );
            // Internal packages (dbt, dbt_postgres, etc.)
            let is_internal = internal_packages.contains(&package_name);

            // For non-internal packages, copy the entry from macro_namespace_registry
            // contains root and non-internal packages
            if !is_internal {
                if let Some(macro_names) =
                    macro_namespace_registry.get(&Value::from(package_name.clone()))
                {
                    non_internal_packages
                        .insert(Value::from(package_name.clone()), macro_names.clone());
                }
            }

            for macro_unit in macro_units {
                let filename = macro_unit.info.path.to_string_lossy().to_string();
                let offset = dbt_frontend_common::error::CodeLocation::new(
                    macro_unit.info.span.start_line as usize,
                    macro_unit.info.span.start_col as usize,
                    macro_unit.info.span.start_offset as usize,
                );
                let listeners = listener_factory
                    .as_ref()
                    .map(|factory| factory.create_listeners(Path::new(&filename), &offset))
                    .unwrap_or_default();
                let macro_name = macro_unit.info.name.clone();
                let template_name = format!("{package_name}.{macro_name}");

                // Add to environment and template registry
                self.env
                    .add_template_owned(
                        template_name.clone(),
                        macro_unit.sql.clone(),
                        Some(filename.clone()),
                        &listeners,
                    )
                    .map_err(|e| FsError::from_jinja_err(e, "Failed to add template"))?;
                for listener in listeners {
                    if let Some(factory) = listener_factory.as_ref() {
                        factory.destroy_listener(Path::new(&filename), listener)
                    };
                }

                macro_template_registry.insert(
                    Value::from(template_name),
                    Value::from_serialize(macro_unit.info.clone()),
                );

                if is_internal {
                    internal_packages_macros
                        .entry(package_name.clone())
                        .or_default()
                        .insert(
                            macro_name.clone(),
                            Value::from_serialize(macro_unit.info.clone()),
                        );
                }
            }
        }

        // Process internal packages in reverse order (like dbt)
        let mut dbt_and_adapters_namespace = BTreeMap::new();
        for pkg in internal_packages.iter().rev() {
            if let Some(pkg_macros) = internal_packages_macros.get(pkg) {
                for macro_name in pkg_macros.keys() {
                    dbt_and_adapters_namespace
                        .insert(Value::from(macro_name.clone()), Value::from(pkg.clone()));
                }
            }
        }

        // Ensure the root package is ALWAYS in non_internal_packages even if it has no macros
        non_internal_packages
            .entry(Value::from(root_package))
            .or_insert_with(|| Value::from_serialize(Vec::<String>::new()));

        // Add all registries to environment
        self.env.add_global(
            MACRO_NAMESPACE_REGISTRY,
            Value::from_object(macro_namespace_registry),
        );
        self.env.add_global(
            MACRO_TEMPLATE_REGISTRY,
            Value::from_object(macro_template_registry),
        );
        self.env.add_global(
            NON_INTERNAL_PACKAGES,
            Value::from_object(non_internal_packages),
        );
        self.env.add_global(
            DBT_AND_ADAPTERS_NAMESPACE,
            Value::from_object(dbt_and_adapters_namespace),
        );

        Ok(self)
    }

    /// Build the Minijinja Environment with all configured settings.
    pub fn build(mut self) -> JinjaEnv {
        // Register filters (as_bool, as_number, as_native, as_text)
        // These are used to convert values to the appropriate type that might be
        // expected by the jinja template.

        self.register_filters();

        // Register tests
        self.register_tests();

        // Register "base" dbt style functions.
        register_base_functions(&mut self.env, self.io_args);

        // Register all configured global values.
        // TODO (Ani) type the globals struct to validate we recieve all the globals we need
        for (key, val) in self.globals {
            self.env.add_global(key, val);
        }

        // Any extra steps (unknown method callback, etc.)
        minijinja_contrib::add_to_environment(&mut self.env);

        // Pull in the pycompat methods
        self.env
            .set_unknown_method_callback(minijinja_contrib::pycompat::unknown_method_callback);

        let mut jinja_env = JinjaEnv::new(self.env);
        if let Some(adapter) = self.adapter {
            jinja_env.set_adapter(adapter);
        }

        jinja_env
    }

    fn register_filters(&mut self) {
        // TODO: This might not be enough (just passing through the value)
        // This is because it is likely that the value is a string and needs
        // to be converted to a native type (i.e. parsed)
        self.env.add_filter("as_native", |value: Value| Ok(value));
        self.env
            .add_filter("as_text", |value: Value| match value.kind() {
                ValueKind::Bool => Ok(Value::from(format!("{value}"))),
                ValueKind::String => Ok(value),
                ValueKind::Number => Ok(Value::from(format!("{value}"))),
                ValueKind::None => Ok(Value::from("")),
                _ => {
                    // Try to see if Value is an Object - use debug to render if so
                    if let Some(object) = value.as_object() {
                        // Call the render method on the object
                        let debug = format!("{object:?}");
                        Ok(Value::from(debug))
                    } else {
                        Err(MinijinjaError::new(
                            MinijinjaErrorKind::InvalidOperation,
                            format!("Failed applying 'as_text' filter to {}", value.kind()),
                        ))
                    }
                }
            });

        self.env
            .add_filter("as_bool", |value: Value| match value.kind() {
                ValueKind::Undefined => Ok(Value::UNDEFINED),
                ValueKind::None => Ok(Value::from(false)),
                ValueKind::Bool | ValueKind::Number => Ok(value),

                ValueKind::String => {
                    let str_ref = value.as_str().unwrap();
                    if str_ref == "False" || str_ref == "false" {
                        Ok(Value::from(false))
                    } else if str_ref == "True" || str_ref == "true" {
                        Ok(Value::from(true))
                    } else {
                        Ok(value)
                    }
                }
                ValueKind::Bytes => {
                    let bytes_ref = value.as_bytes().unwrap();
                    let string_from_bytes =
                        String::from_utf8(bytes_ref.to_vec()).map_err(|_| {
                            MinijinjaError::new(
                                MinijinjaErrorKind::InvalidOperation,
                                "Failed applying 'as_number' filter to bytes",
                            )
                        })?;
                    let bool = string_from_bytes.parse::<bool>().map_err(|_| {
                        MinijinjaError::new(
                            MinijinjaErrorKind::InvalidOperation,
                            format!(
                                "Failed applying 'as_number' filter to bytes string '{string_from_bytes}'"
                            ),
                        )
                    })?;
                    Ok(Value::from(bool))
                }
                ValueKind::Seq | ValueKind::Map | ValueKind::Iterable | ValueKind::Plain => {
                    Ok(Value::from(value.is_true()))
                }
                ValueKind::Invalid => Err(MinijinjaError::new(
                    MinijinjaErrorKind::InvalidOperation,
                    "Failed applying 'as_bool' filter to invalid value",
                )),
                _ => Err(MinijinjaError::new(
                    MinijinjaErrorKind::InvalidOperation,
                    "Failed applying 'as_bool' filter to unknown value",
                )),
            });

        self.env
            .add_filter("as_number", |value: Value| match value.kind() {
                ValueKind::Undefined => Ok(Value::UNDEFINED),
                ValueKind::None => Ok(Value::from(0)),
                ValueKind::Bool => Ok(value),
                ValueKind::Number => Ok(value),
                ValueKind::String => {
                    let str_ref = value.as_str().unwrap();
                    let num = match str_ref.parse::<i32>() {
                        Ok(num) => num,
                        Err(_) => {
                            return Ok(value);
                        }
                    };
                    Ok(Value::from(num))
                }
                ValueKind::Bytes => {
                    let bytes_ref = value.as_bytes().unwrap();
                    let string_from_bytes =
                        String::from_utf8(bytes_ref.to_vec()).map_err(|_| {
                            MinijinjaError::new(
                                MinijinjaErrorKind::InvalidOperation,
                                "Failed applying 'as_number' filter to bytes",
                            )
                        })?;
                    let num = string_from_bytes.parse::<i32>().map_err(|_| {
                        MinijinjaError::new(
                            MinijinjaErrorKind::InvalidOperation,
                            format!(
                                "Failed applying 'as_number' filter to bytes string '{string_from_bytes}'"
                            ),
                        )
                    })?;
                    Ok(Value::from(num))
                }
                ValueKind::Seq | ValueKind::Map | ValueKind::Iterable | ValueKind::Plain => {
                    Err(MinijinjaError::new(
                        MinijinjaErrorKind::InvalidOperation,
                        "Failed applying 'as_number' filter to map, seq, or iterable",
                    ))
                }
                ValueKind::Invalid => Err(MinijinjaError::new(
                    MinijinjaErrorKind::InvalidOperation,
                    "Failed applying 'as_number' filter to invalid value",
                )),
                _ => Err(MinijinjaError::new(
                    MinijinjaErrorKind::InvalidOperation,
                    "Failed applying 'as_number' filter to unknown value",
                )),
            });
    }

    fn register_tests(&mut self) {
        self.env
            .add_test("callable", |value: Value| value.as_object().is_some());
    }
}

impl Default for JinjaEnvBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, path::PathBuf, sync::Mutex};

    use dbt_common::cancellation::never_cancels;
    use dbt_fusion_adapter::parse::adapter::create_parse_adapter;
    use dbt_schemas::schemas::relations::DEFAULT_DBT_QUOTING;
    use minijinja::{
        constants::MACRO_DISPATCH_ORDER, context, dispatch_object::THREAD_LOCAL_DEPENDENCIES,
        machinery::Span, macro_unit::MacroInfo,
    };

    use super::*;
    use insta::assert_snapshot;
    fn create_macro_unit(name: &str, sql: &str) -> MacroUnit {
        MacroUnit {
            info: MacroInfo {
                name: name.to_string(),
                path: PathBuf::from("test"),
                span: Span {
                    start_line: 0,
                    start_col: 0,
                    start_offset: 0,
                    end_line: 0,
                    end_col: 0,
                    end_offset: 0,
                },
            },
            sql: sql.to_string(),
        }
    }

    #[test]
    fn test_filter_none() {
        let mut builder = JinjaEnvBuilder::new();
        builder.register_filters();
        let env = builder.build();
        let rv = env
            .render_str(
                r#"
    {%- set x = y | as_bool -%}
    {%- set x = y | as_number -%}   
    all okay!
    "#,
                context! {},
                &[],
            )
            .unwrap();
        assert_snapshot!(rv, @r"

all okay!");
    }

    #[test]
    fn test_dispatch_mode() {
        THREAD_LOCAL_DEPENDENCIES
            .set(Mutex::new(BTreeSet::new()))
            .unwrap();
        let mut macro_units = MacroUnitsWrapper::new(BTreeMap::new());
        macro_units.macros.insert(
            "dbt".to_string(),
            vec![
                create_macro_unit(
                    "default__one",
                    "{% macro default__one() %}dbt default one{% endmacro %}",
                ),
                create_macro_unit(
                    "default__two",
                    "{% macro default__two() %}dbt default two{% endmacro %}",
                ),
                create_macro_unit(
                    "default__three",
                    "{% macro default__three() %}dbt default three{% endmacro %}",
                ),
            ],
        );
        macro_units.macros.insert(
            "dbt_postgres".to_string(),
            vec![
                create_macro_unit(
                    "postgres__one",
                    "{% macro postgres__one() %}postgres one{% endmacro %}",
                ),
                create_macro_unit(
                    "postgres__two",
                    "{% macro postgres__two() %}postgres two{% endmacro %}",
                ),
                create_macro_unit(
                    "postgres__some_macro",
                    "{% macro postgres__some_macro() %}some_macro in postgres{% endmacro %}",
                ),
            ],
        );
        macro_units.macros.insert(
            "a_package".to_string(),
            vec![
                create_macro_unit(
                    "postgres__some_macro",
                    "{% macro postgres__some_macro() %}some_macro in a_package{% endmacro %}",
                ),
                create_macro_unit(
                    "default__another_macro",
                    "{% macro default__another_macro() %}default another_macro{% endmacro %}",
                ),
            ],
        );
        macro_units.macros.insert(
            "test_package".to_string(),
            vec![create_macro_unit(
                "default__one",
                "{% macro default__one() %}test_package one{% endmacro %}",
            )],
        );
        let builder: JinjaEnvBuilder = JinjaEnvBuilder::new()
            .with_adapter(
                create_parse_adapter("postgres", DEFAULT_DBT_QUOTING, never_cancels()).unwrap(),
            )
            .with_root_package("test_package".to_string())
            .try_with_macros(macro_units, None)
            .expect("Failed to register macros");
        let env = builder.build();
        // one exists in test_package, dbt_postgres, and dbt
        let rv = env
            .render_str("{{adapter.dispatch('one', 'dbt')()}}", context! {}, &[])
            .unwrap();
        assert_snapshot!(rv, "test_package one");
        // two exists in dbt_postgres, and dbt
        let rv = env
            .render_str("{{adapter.dispatch('two', 'dbt')()}}", context! {}, &[])
            .unwrap();
        assert_snapshot!(rv, "postgres two");
        // three exists in dbt
        let rv = env
            .render_str("{{adapter.dispatch('three', 'dbt')()}}", context! {}, &[])
            .unwrap();
        assert_snapshot!(rv, "dbt default three");

        // one exists in test_package, dbt_postgres, and dbt, but package is not specified, so last one wins
        let rv = env
            .render_str("{{adapter.dispatch('one')()}}", context! {}, &[])
            .unwrap();
        assert_snapshot!(rv, "test_package one");

        // some_macro exists in a_package, and dbt_postgres, but package is not specified, so last one wins
        let rv = env
            .render_str("{{adapter.dispatch('some_macro')()}}", context! {}, &[])
            .unwrap();
        assert_snapshot!(rv, "some_macro in a_package");

        // some_macro exists in a_package, postgres, and dbt_postgres, but package is specified, so a_package wins
        let rv = env
            .render_str(
                "{{adapter.dispatch('some_macro', 'a_package')()}}",
                context! {},
                &[],
            )
            .unwrap();
        assert_snapshot!(rv, "some_macro in a_package");
    }
    #[test]
    fn test_dispatch_config() {
        let mut macro_units = MacroUnitsWrapper::new(BTreeMap::new());
        macro_units.macros.insert(
            "dbt".to_string(),
            vec![create_macro_unit(
                "default__some_macro",
                "{% macro default__some_macro() %}some_macro in dbt{% endmacro %}",
            )],
        );
        macro_units.macros.insert(
            "a_package".to_string(),
            vec![
                create_macro_unit(
                    "postgres__some_macro",
                    "{% macro postgres__some_macro() %}some_macro in a_package{% endmacro %}",
                ),
                create_macro_unit(
                    "default__some_macro",
                    "{% macro default__some_macro() %}default some_macro{% endmacro %}",
                ),
            ],
        );
        macro_units.macros.insert(
            "test_package".to_string(),
            vec![
                create_macro_unit(
                    "postgres__some_macro",
                    "{% macro postgres__some_macro() %}some_macro in test_package{% endmacro %}",
                ),
                create_macro_unit(
                    "default__some_macro",
                    "{% macro default__some_macro() %}default some_macro{% endmacro %}",
                ),
            ],
        );
        let builder: JinjaEnvBuilder = JinjaEnvBuilder::new()
            .with_adapter(
                create_parse_adapter("postgres", DEFAULT_DBT_QUOTING, never_cancels()).unwrap(),
            )
            .with_root_package("test_package".to_string())
            .try_with_macros(macro_units, None)
            .expect("Failed to register macros");
        let env = builder.build();

        // Set non-default dispatch order of a_package and dbt
        let ctx = BTreeMap::from([(
            MACRO_DISPATCH_ORDER,
            Value::from_object(BTreeMap::from([
                (
                    Value::from("a_package".to_string()),
                    Value::from(vec!["test_package".to_string(), "a_package".to_string()]),
                ),
                (
                    Value::from("dbt".to_string()),
                    Value::from(vec!["dbt".to_string(), "test_package".to_string()]),
                ),
                (
                    Value::from("test_package".to_string()),
                    Value::from(vec!["b_package".to_string(), "test_package".to_string()]),
                ),
            ])),
        )]);

        // some_macro dispatched with a_package now resolves to test_package
        let rv = env
            .render_str(
                "{{adapter.dispatch('some_macro', 'a_package')()}}",
                &ctx,
                &[],
            )
            .unwrap();
        assert_eq!(rv, "some_macro in test_package");

        // some_macro dispatched with dbt now resolves to dbt
        let rv = env
            .render_str("{{adapter.dispatch('some_macro', 'dbt')()}}", &ctx, &[])
            .unwrap();
        assert_eq!(rv, "some_macro in dbt");

        // some_macro does not exist in b_package, so when dispatched with test_package still resolves to test_package
        let rv = env
            .render_str(
                "{{adapter.dispatch('some_macro', 'test_package')()}}",
                ctx,
                &[],
            )
            .unwrap();
        assert_snapshot!(rv, "some_macro in test_package");
    }

    #[test]
    fn test_macro_assignment() {
        let env = JinjaEnvBuilder::new()
            .with_root_package("test_package".to_string())
            .with_adapter(create_parse_adapter("postgres", DEFAULT_DBT_QUOTING, never_cancels()).unwrap())
            .try_with_macros(MacroUnitsWrapper::new(BTreeMap::from([(
                "test_package".to_string(),
                vec![
                    MacroUnit {
                        info: MacroInfo {
                            name: "some_macro".to_string(),
                            path: PathBuf::from("test"),
                            span: Span {
                                start_line: 1,
                                start_col: 1,
                                start_offset: 0,
                                end_line: 1,
                                end_col: 1,
                                end_offset: 0,
                            },
                        },
                        sql: "{% macro some_macro() %}hello{% endmacro %}".to_string(),
                    },
                    MacroUnit {
                        info: MacroInfo {
                            name: "macro_b".to_string(),
                            path: PathBuf::from("test"),
                            span: Span::default(),
                        },
                        sql: "{% macro macro_b() %}{%- set small_macro_name = some_macro -%} {{ small_macro_name() }}{% endmacro %}".to_string(),
                    },
                ],
            )]),
        ), None)
            .unwrap()
            .build();
        // Test assigning macro to variable and using it
        let rv = env.render_str("{{macro_b()}}", context! {}, &[]).unwrap();

        // The first print should show the macro object, second print shows the macro output
        // assert!(rv.contains("<macro 'some_macro'>"));
        assert!(rv.contains("hello"));
    }
    #[test]
    fn test_date_format() {
        let env = JinjaEnvBuilder::new().build();
        let rv = env
            .render_str(
                "{{modules.pytz.utc}} {{- modules.datetime.datetime.now(modules.pytz.utc).isoformat() -}}",
                context! {},
                &[],
            )
            .unwrap()
            ;
        assert!(rv.contains("UTC"));
        assert!(rv.contains("+00:00"));
    }
    #[test]
    fn test_datetime_strftime_with_timedelta() {
        let env = JinjaEnvBuilder::new().build();
        let rv = env
            .render_str(
                "
                {%- set today = modules.datetime.date.today() -%}
                {%- set now = modules.datetime.datetime.now().astimezone(modules.pytz.timezone('UTC')) -%}
                {{modules.datetime.datetime.strftime(today - modules.datetime.timedelta(days=1), '%Y-%m-%d')}}",
                context! {},
                &[],
            )
            .unwrap()
            ;
        // Extract the date from the rendered string and verify it's in the expected format
        let date_pattern = regex::Regex::new(r"^\d{4}-\d{2}-\d{2}$").unwrap();
        assert!(
            date_pattern.is_match(rv.trim()),
            "Expected date in YYYY-MM-DD format, got: '{}'",
            rv.trim()
        );

        // Verify the date is yesterday by comparing with today's date
        let today = chrono::Local::now().date_naive();
        let yesterday = (today - chrono::Duration::days(1))
            .format("%Y-%m-%d")
            .to_string();
        assert_eq!(
            rv.trim(),
            yesterday,
            "Expected yesterday's date ({}), got: {}",
            yesterday,
            rv.trim()
        );
    }
    #[test]
    fn test_root_package_in_non_internal_packages() {
        // Create macro units with macros only in other packages, not in root
        let mut macro_units = MacroUnitsWrapper::new(BTreeMap::new());

        // Add macros to a non-root package
        macro_units.macros.insert(
            "other_package".to_string(),
            vec![create_macro_unit(
                "some_macro",
                "{% macro some_macro() %}some_macro in other_package{% endmacro %}",
            )],
        );

        // Root package has no macros

        // Build environment with the empty root package
        let builder: JinjaEnvBuilder = JinjaEnvBuilder::new()
            .with_adapter(
                create_parse_adapter("postgres", DEFAULT_DBT_QUOTING, never_cancels()).unwrap(),
            )
            .with_root_package("empty_root".to_string())
            .try_with_macros(macro_units, None)
            .expect("Failed to register macros");

        let env = builder.build();

        // Get the non_internal_packages registry
        let non_internal_packages = env.get_global(NON_INTERNAL_PACKAGES).unwrap();
        // Verify that empty_root is in the keys
        let keys: Vec<_> = non_internal_packages
            .as_object()
            .unwrap()
            .try_iter_pairs()
            .unwrap()
            .map(|(k, _)| k.to_string())
            .collect();

        assert!(
            keys.contains(&"empty_root".to_string()),
            "Root package should be in non_internal_packages even with no macros"
        );
    }
}
