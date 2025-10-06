//! This module contains the functions for initializing the Jinja environment for the compile phase.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::sync::{Arc, OnceLock};

use crate::functions::build_flat_graph;
use crate::jinja_environment::JinjaEnv;
use dbt_common::once_cell_vars::DISPATCH_CONFIG;
use dbt_fusion_adapter::BaseAdapter;
use dbt_fusion_adapter::load_store::ResultStore;
use dbt_schemas::schemas::Nodes;
use dbt_schemas::state::{DbtRuntimeConfig, NodeResolverTracker};
use minijinja::arg_utils::{ArgParser, ArgsIter};
use minijinja::constants::MACRO_DISPATCH_ORDER;
use minijinja::dispatch_object::DispatchObject;
use minijinja::listener::RenderingEventListener;
use minijinja::value::Object;
use minijinja::{
    Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, Value as MinijinjaValue,
};
use minijinja::{State, UndefinedBehavior};
use std::rc::Rc;

/// Configure the Jinja environment for the compile phase.
pub fn configure_compile_and_run_jinja_environment(
    env: &mut JinjaEnv,
    adapter: Arc<dyn BaseAdapter>,
) {
    env.set_adapter(adapter);
    env.set_undefined_behavior(UndefinedBehavior::Lenient);
}

/// Configure the Jinja environment for the compile phase.
pub fn build_compile_and_run_base_context(
    node_resolver: Arc<dyn NodeResolverTracker>,
    package_name: &str,
    nodes: &Nodes,
    runtime_config: Arc<DbtRuntimeConfig>,
) -> BTreeMap<String, MinijinjaValue> {
    let mut ctx = BTreeMap::new();
    let config_macro = |_: &[MinijinjaValue]| -> Result<MinijinjaValue, MinijinjaError> {
        Ok(MinijinjaValue::from(""))
    };
    ctx.insert(
        "config".to_string(),
        MinijinjaValue::from_function(config_macro),
    );

    let macro_dispatch_order = DISPATCH_CONFIG
        .get()
        .map(|macro_dispatch_order| {
            macro_dispatch_order
                .read()
                .unwrap()
                .iter()
                .map(|(k, v)| (MinijinjaValue::from(k), MinijinjaValue::from(v.clone())))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();

    ctx.insert(
        MACRO_DISPATCH_ORDER.to_string(),
        MinijinjaValue::from_object(macro_dispatch_order),
    );

    // Create a BTreeMap for builtins
    let mut builtins = BTreeMap::new();

    // Create base ref function for macros (without validation)
    let ref_function = RefFunction::new_unvalidated(
        node_resolver.clone(),
        package_name.to_owned(),
        runtime_config.clone(),
    );
    let ref_value = MinijinjaValue::from_object(ref_function);
    ctx.insert("ref".to_string(), ref_value.clone());
    builtins.insert("ref".to_string(), ref_value);

    // Create source function
    let source_function = SourceFunction {
        node_resolver: node_resolver.clone(),
        package_name: package_name.to_owned(),
    };
    let source_value = MinijinjaValue::from_object(source_function);
    ctx.insert("source".to_string(), source_value.clone());
    builtins.insert("source".to_string(), source_value);

    // Create function function
    let function_function = FunctionFunction::new_unvalidated(
        node_resolver.clone(),
        package_name.to_owned(),
        runtime_config.clone(),
    );
    let function_value = MinijinjaValue::from_object(function_function);
    ctx.insert("function".to_string(), function_value.clone());
    builtins.insert("function".to_string(), function_value);

    // This is used in macros to gate the sql execution (set to true only after parse stage)
    // for example dbt_macro_assets/dbt-adapters/macros/etc/statement.sql
    ctx.insert("execute".to_string(), MinijinjaValue::from(true));

    // Register builtins as a global
    ctx.insert(
        "builtins".to_string(),
        MinijinjaValue::from_object(builtins),
    );

    let mut packages: BTreeSet<String> = runtime_config.dependencies.keys().cloned().collect();
    packages.insert(package_name.to_string());
    ctx.insert(
        "context".to_owned(),
        MinijinjaValue::from_object(MacroLookupContext {
            root_project_name: package_name.to_string(),
            current_project_name: None,
            packages,
        }),
    );

    // Register graph as a global
    ctx.insert(
        "graph".to_string(),
        MinijinjaValue::from_object(LazyFlatGraph::new(nodes)),
    );
    let result_store = ResultStore::default();
    ctx.insert(
        "store_result".to_owned(),
        MinijinjaValue::from_function(result_store.store_result()),
    );
    ctx.insert(
        "load_result".to_owned(),
        MinijinjaValue::from_function(result_store.load_result()),
    );

    ctx.insert("node".to_owned(), MinijinjaValue::NONE);
    ctx.insert("connection_name".to_owned(), MinijinjaValue::from(""));
    ctx
}

#[derive(Debug)]
pub struct RefFunction {
    node_resolver: Arc<dyn NodeResolverTracker>,
    package_name: String,
    runtime_config: Arc<DbtRuntimeConfig>,
    /// Optional validation configuration - None means no validation
    validation_config: Option<RefValidationConfig>,
    /// Only meaningful for node contexts; base context leaves this unset
    static_analysis_unsafe: Option<bool>,
}

#[derive(Debug)]
pub struct RefValidationConfig {
    /// The set of allowed node dependencies for this specific node
    pub allowed_dependencies: Arc<BTreeSet<String>>,
    /// Whether to skip dependency validation used for REPL and inline queries
    pub skip_validation: bool,
}

impl RefFunction {
    /// Create a new RefFunction without validation (for base context)
    pub fn new_unvalidated(
        node_resolver: Arc<dyn NodeResolverTracker>,
        package_name: String,
        runtime_config: Arc<DbtRuntimeConfig>,
    ) -> Self {
        Self {
            node_resolver,
            package_name,
            runtime_config,
            validation_config: None,
            static_analysis_unsafe: None,
        }
    }

    /// Create a new RefFunction with validation (for node context)
    pub fn new_with_validation(
        node_resolver: Arc<dyn NodeResolverTracker>,
        package_name: String,
        runtime_config: Arc<DbtRuntimeConfig>,
        allowed_dependencies: Arc<BTreeSet<String>>,
        skip_validation: bool,
        static_analysis_unsafe: bool,
    ) -> Self {
        Self {
            node_resolver,
            package_name,
            runtime_config,
            validation_config: Some(RefValidationConfig {
                allowed_dependencies,
                skip_validation,
            }),
            static_analysis_unsafe: Some(static_analysis_unsafe),
        }
    }

    fn should_use_deferred(&self) -> bool {
        if self.node_resolver.compile_or_test() {
            self.static_analysis_unsafe.unwrap_or(false)
        } else {
            true
        }
    }

    fn resolve_args(
        &self,
        args: &[MinijinjaValue],
    ) -> Result<(Option<String>, String, Option<String>), MinijinjaError> {
        if args.is_empty() || args.len() > 4 {
            return Err(MinijinjaError::new(
                MinijinjaErrorKind::InvalidOperation,
                "invalid number of arguments for ref macro",
            ));
        }
        let mut parser = ArgParser::new(args, None);
        // If there are two positional args, the first is the package name and the second is the model name
        let arg0 = parser.get::<String>("")?;
        let arg1 = parser.get_optional::<String>("");
        let (namespace, model_name) = match (arg0, arg1) {
            (namespace, Some(model_name)) => (Some(namespace), model_name),
            (model_name, None) => (None, model_name),
        };
        let version = parser.consume_optional_either_from_kwargs::<String>("version", "v");

        let package_name = namespace;

        if let Some(v) = version {
            Ok((package_name, model_name, Some(v)))
        } else {
            Ok((package_name, model_name, None))
        }
    }

    /// Validate that the referenced model is in the allowed dependencies
    fn validate_dependency(
        &self,
        unique_id: &str,
        package_name: &Option<String>,
        model_name: &str,
    ) -> Result<(), MinijinjaError> {
        let Some(validation_config) = &self.validation_config else {
            // No validation config means no validation needed
            return Ok(());
        };

        if validation_config.skip_validation {
            return Ok(());
        }

        if validation_config.allowed_dependencies.contains(unique_id) {
            Ok(())
        } else {
            // Construct the ref string for the error message
            let ref_string = if let Some(pkg) = package_name {
                format!("{{{{ ref('{pkg}', '{model_name}') }}}}")
            } else {
                format!("{{{{ ref('{model_name}') }}}}")
            };

            Err(MinijinjaError::new(
                MinijinjaErrorKind::InvalidOperation,
                format!(
                    "dbt was unable to infer all dependencies for the model \"{model_name}\". This typically happens when ref() is placed within a conditional block.
To fix this, add the following hint to the top of the model \"{model_name}\": 
-- depends_on: {ref_string}"
                ),
            ))
        }
    }
}

impl Object for RefFunction {
    fn call(
        self: &Arc<Self>,
        _state: &State<'_, '_>,
        args: &[MinijinjaValue],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<MinijinjaValue, MinijinjaError> {
        let (package_name, model_name, version) = self.resolve_args(args)?;

        match self.node_resolver.lookup_ref(
            &package_name,
            &model_name,
            &version,
            &Some(self.package_name.clone()),
        ) {
            Ok((unique_id, relation, _, deferred_relation)) => {
                // Validate that this ref is allowed (only if validation is configured)
                self.validate_dependency(&unique_id, &package_name, &model_name)?;
                // Here, we check if we should use the deferred relation or the normal relation
                // This is only relevant for the compile or test command
                // If we are compiling or testing, and the static analysis is unsafe, we use the deferred relation
                // Otherwise, we use the normal relation
                let resolved_relation = match (self.should_use_deferred(), deferred_relation) {
                    (true, Some(deferred)) => deferred,
                    _ => relation,
                };
                Ok(resolved_relation)
            }
            Err(_) => Err(MinijinjaError::new(
                MinijinjaErrorKind::NonKey,
                format!(
                    "ref not found for package: {}, model: {}, version: {:?}",
                    self.package_name, model_name, version
                ),
            )),
        }
    }

    fn call_method(
        self: &Arc<Self>,
        _state: &State<'_, '_>,
        method: &str,
        args: &[MinijinjaValue],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<MinijinjaValue, MinijinjaError> {
        match method {
            "id" => {
                let (package_name, model_name, version) = self.resolve_args(args)?;
                match self.node_resolver.lookup_ref(
                    &package_name,
                    &model_name,
                    &version,
                    &Some(self.package_name.clone()),
                ) {
                    Ok((unique_id, _, _, _)) => {
                        // Validate that this ref is allowed (only if validation is configured)
                        self.validate_dependency(&unique_id, &package_name, &model_name)?;
                        Ok(MinijinjaValue::from(unique_id))
                    }
                    Err(_) => Err(MinijinjaError::new(
                        MinijinjaErrorKind::NonKey,
                        format!(
                            "ref not found for package: {}, model: {}, version: {:?}",
                            self.package_name, model_name, version
                        ),
                    )),
                }
            }
            _ => Err(MinijinjaError::new(
                MinijinjaErrorKind::UnknownMethod,
                format!("No method named '{method}' on ref objects"),
            )),
        }
    }

    fn get_value(self: &Arc<Self>, key: &MinijinjaValue) -> Option<MinijinjaValue> {
        match key.as_str()? {
            "config" => Some(MinijinjaValue::from_dyn_object(self.runtime_config.clone())),
            "function_name" => Some(MinijinjaValue::from("ref")),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct SourceFunction {
    node_resolver: Arc<dyn NodeResolverTracker>,
    package_name: String,
}

impl Object for SourceFunction {
    fn call(
        self: &Arc<Self>,
        _state: &State<'_, '_>,
        args: &[MinijinjaValue],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<MinijinjaValue, MinijinjaError> {
        let parser = ArgParser::new(args, None);
        let num_args = parser.positional_len();
        let (source_name, table_name) = match num_args {
            0 | 1 => Err(MinijinjaError::new(
                MinijinjaErrorKind::MissingArgument,
                "source macro requires 2 arguments: source name and table name",
            )),
            2 => Ok((
                args[0].as_str().unwrap().to_string(), // source name (namespace)
                args[1].as_str().unwrap().to_string(), // name (relation name)
            )),
            _ => Err(MinijinjaError::new(
                MinijinjaErrorKind::TooManyArguments,
                "source",
            )),
        }?;
        match self
            .node_resolver
            .lookup_source(&self.package_name, &source_name, &table_name)
        {
            Ok((_, relation, _)) => Ok(relation),
            Err(_) => Err(MinijinjaError::new(
                MinijinjaErrorKind::NonKey,
                format!(
                    "Source not found for source name: {source_name}, table name: {table_name}"
                ),
            )),
        }
    }
}

#[derive(Debug)]
pub struct FunctionFunction {
    node_resolver: Arc<dyn NodeResolverTracker>,
    package_name: String,
    runtime_config: Arc<DbtRuntimeConfig>,
    /// Optional validation configuration - None means no validation
    validation_config: Option<FunctionValidationConfig>,
}

#[derive(Debug)]
pub struct FunctionValidationConfig {
    /// The set of allowed function dependencies for this specific node
    pub allowed_dependencies: Arc<BTreeSet<String>>,
    /// Whether to skip dependency validation used for REPL and inline queries
    pub skip_validation: bool,
}

impl FunctionFunction {
    /// Create a new FunctionFunction without validation (for base context)
    pub fn new_unvalidated(
        node_resolver: Arc<dyn NodeResolverTracker>,
        package_name: String,
        runtime_config: Arc<DbtRuntimeConfig>,
    ) -> Self {
        Self {
            node_resolver,
            package_name,
            runtime_config,
            validation_config: None,
        }
    }

    /// Create a new FunctionFunction with validation (for node context)
    pub fn new_with_validation(
        node_resolver: Arc<dyn NodeResolverTracker>,
        package_name: String,
        runtime_config: Arc<DbtRuntimeConfig>,
        allowed_dependencies: Arc<BTreeSet<String>>,
        skip_validation: bool,
    ) -> Self {
        Self {
            node_resolver,
            package_name,
            runtime_config,
            validation_config: Some(FunctionValidationConfig {
                allowed_dependencies,
                skip_validation,
            }),
        }
    }

    fn resolve_args(
        &self,
        args: &[MinijinjaValue],
    ) -> Result<(Option<String>, String), MinijinjaError> {
        if args.is_empty() || args.len() > 3 {
            return Err(MinijinjaError::new(
                MinijinjaErrorKind::InvalidOperation,
                "invalid number of arguments for function macro",
            ));
        }
        let mut parser = ArgParser::new(args, None);
        // If there are two positional args, the first is the package name and the second is the function name
        let arg0 = parser.get::<String>("")?;
        let arg1 = parser.get_optional::<String>("");
        let (namespace, function_name) = match (arg0, arg1) {
            (namespace, Some(function_name)) => (Some(namespace), function_name),
            (function_name, None) => (None, function_name),
        };

        let package_name = namespace;

        Ok((package_name, function_name))
    }

    /// Validate that the referenced function is in the allowed dependencies
    fn validate_dependency(
        &self,
        unique_id: &str,
        package_name: &Option<String>,
        function_name: &str,
    ) -> Result<(), MinijinjaError> {
        let Some(validation_config) = &self.validation_config else {
            // No validation config means no validation needed
            return Ok(());
        };

        if validation_config.skip_validation {
            return Ok(());
        }

        if validation_config.allowed_dependencies.contains(unique_id) {
            Ok(())
        } else {
            // Construct the function string for the error message
            let function_string = if let Some(pkg) = package_name {
                format!("{{{{ function('{pkg}', '{function_name}') }}}}")
            } else {
                format!("{{{{ function('{function_name}') }}}}")
            };

            Err(MinijinjaError::new(
                MinijinjaErrorKind::InvalidOperation,
                format!(
                    "dbt was unable to infer all dependencies for the function \"{function_name}\". This typically happens when function() is placed within a conditional block.
To fix this, add the following hint to the top of the model: 
-- depends_on: {function_string}"
                ),
            ))
        }
    }
}

impl Object for FunctionFunction {
    fn call(
        self: &Arc<Self>,
        _state: &State<'_, '_>,
        args: &[MinijinjaValue],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<MinijinjaValue, MinijinjaError> {
        let (package_name, function_name) = self.resolve_args(args)?;

        match self.node_resolver.lookup_function(
            &package_name,
            &function_name,
            &Some(self.package_name.clone()),
        ) {
            Ok((unique_id, function_call, _)) => {
                // Validate that this function is allowed (only if validation is configured)
                self.validate_dependency(&unique_id, &package_name, &function_name)?;
                Ok(function_call)
            }
            Err(_) => Err(MinijinjaError::new(
                MinijinjaErrorKind::NonKey,
                format!(
                    "function not found for package: {}, function: {}",
                    self.package_name, function_name
                ),
            )),
        }
    }

    fn call_method(
        self: &Arc<Self>,
        _state: &State<'_, '_>,
        method: &str,
        args: &[MinijinjaValue],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<MinijinjaValue, MinijinjaError> {
        match method {
            "id" => {
                let (package_name, function_name) = self.resolve_args(args)?;
                match self.node_resolver.lookup_function(
                    &package_name,
                    &function_name,
                    &Some(self.package_name.clone()),
                ) {
                    Ok((unique_id, _relation, _)) => {
                        // Validate that this function is allowed (only if validation is configured)
                        self.validate_dependency(&unique_id, &package_name, &function_name)?;
                        Ok(MinijinjaValue::from(unique_id.as_str()))
                    }
                    Err(_) => Err(MinijinjaError::new(
                        MinijinjaErrorKind::NonKey,
                        format!(
                            "function not found for package: {}, function: {}",
                            self.package_name, function_name
                        ),
                    )),
                }
            }
            _ => Err(MinijinjaError::new(
                MinijinjaErrorKind::UnknownMethod,
                format!("No method named '{method}' on function objects"),
            )),
        }
    }

    fn get_value(self: &Arc<Self>, key: &MinijinjaValue) -> Option<MinijinjaValue> {
        match key.as_str()? {
            "config" => Some(MinijinjaValue::from_dyn_object(self.runtime_config.clone())),
            "function_name" => Some(MinijinjaValue::from("function")),
            _ => None,
        }
    }
}

/// This is a special context object that is available during the compile or run phase.
/// It allows users to lookup macros by string and returns a DispatchObject, which when called
/// executes the macro. Users can also lookup macro namespaces by string, and this returns a Context
/// object, which when called with a macro name returns a DispatchObject.
#[derive(Debug)]
pub struct MacroLookupContext {
    /// The root project name.
    pub root_project_name: String,
    /// The current project name, when no current project specified, we search from the root project.
    pub current_project_name: Option<String>,
    /// The packages in the project.
    pub packages: BTreeSet<String>,
}

impl Object for MacroLookupContext {
    fn get_value(self: &Arc<Self>, key: &MinijinjaValue) -> Option<MinijinjaValue> {
        match key.as_str()? {
            // NOTE(serramatutu): In Core, the following non-macro keys are all members of `MacroLookupContext`.
            // They can all technically be used, though the usage is undocumented and not encouraged by dbt:
            // - dbt_version
            // - project_name
            // - schema
            // - run_started_at
            //
            // We added `project_name` because some naughty famous macro uses it and was
            // breaking lots of projects, but I prefer to avoid polluting this scope and sticking
            // as faithfully as possible to the "intended" behavior (only looking up macros)
            "project_name" => Some(MinijinjaValue::from(self.root_project_name.clone())),

            lookup_macro => {
                if self.packages.contains(lookup_macro) {
                    Some(MinijinjaValue::from_object(MacroLookupContext {
                        root_project_name: self.root_project_name.clone(),
                        current_project_name: Some(lookup_macro.to_string()),
                        packages: BTreeSet::new(),
                    }))
                } else {
                    Some(MinijinjaValue::from_object(DispatchObject {
                        macro_name: lookup_macro.to_string(),
                        package_name: self.current_project_name.clone(),
                        strict: self.current_project_name.is_some(),
                        auto_execute: false,
                        // TODO: If the macro uses a recursive context (i.e. context['self']) we will stack overflow
                        // but there is no way to conjure up a context object here without access to State
                        context: None,
                    }))
                }
            }
        }
    }

    fn render(self: &Arc<Self>, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    where
        Self: Sized + 'static,
    {
        self.fmt(f)
    }

    fn call_method(
        self: &Arc<Self>,
        state: &State<'_, '_>,
        method: &str,
        args: &[MinijinjaValue],
        listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<MinijinjaValue, MinijinjaError> {
        // TODO(serramatutu): should this behave fully like a dict, with values, keys, items,
        // enumerate etc?
        match method {
            "get" => {
                let iter = ArgsIter::new("MacroLookupContext.get", &["key"], args);
                let key = iter.next_arg::<&MinijinjaValue>()?;
                let default = iter.next_kwarg::<Option<&MinijinjaValue>>("default")?;
                iter.finish()?;

                Ok(self
                    .get_value(key)
                    .or_else(|| default.cloned())
                    .unwrap_or(MinijinjaValue::from(None::<MinijinjaValue>)))
            }
            _ => {
                if let Some(value) = self.get_value(&MinijinjaValue::from(method)) {
                    return value.call(state, args, listeners);
                }
                Err(MinijinjaError::new(
                    MinijinjaErrorKind::UnknownMethod,
                    format!("MacroLookupContext has no method named {method}"),
                ))
            }
        }
    }
}

/// This is a lazy-loaded flat graph object that builds the flat graph from
/// `nodes` on first access.
#[derive(Debug)]
struct LazyFlatGraph {
    nodes: Nodes,
    graph: OnceLock<MinijinjaValue>,
}

impl LazyFlatGraph {
    pub fn new(nodes: &Nodes) -> Self {
        // TODO: We don't want to clone the top level maps either -- make the
        // caller pass in Arc<Nodes> instead
        Self {
            nodes: nodes.clone(),
            graph: OnceLock::new(),
        }
    }

    fn get_graph(&self) -> &MinijinjaValue {
        self.graph
            .get_or_init(|| MinijinjaValue::from(build_flat_graph(&self.nodes)))
    }
}

impl Object for LazyFlatGraph {
    fn get_value(self: &Arc<Self>, key: &MinijinjaValue) -> Option<MinijinjaValue> {
        self.get_graph().as_object().unwrap().get_value(key)
    }

    fn repr(self: &Arc<Self>) -> minijinja::value::ObjectRepr {
        self.get_graph().as_object().unwrap().repr()
    }

    fn enumerate(self: &Arc<Self>) -> minijinja::value::Enumerator {
        self.get_graph().as_object().unwrap().enumerate()
    }

    fn enumerator_len(self: &Arc<Self>) -> Option<usize> {
        self.get_graph().as_object().unwrap().enumerator_len()
    }

    fn is_true(self: &Arc<Self>) -> bool {
        self.get_graph().as_object().unwrap().is_true()
    }

    fn is_mutable(self: &Arc<Self>) -> bool {
        self.get_graph().as_object().unwrap().is_mutable()
    }

    fn call(
        self: &Arc<Self>,
        state: &State<'_, '_>,
        args: &[MinijinjaValue],
        listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<MinijinjaValue, MinijinjaError> {
        self.get_graph()
            .as_object()
            .unwrap()
            .call(state, args, listeners)
    }

    fn call_method(
        self: &Arc<Self>,
        state: &State<'_, '_>,
        method: &str,
        args: &[MinijinjaValue],
        listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<MinijinjaValue, MinijinjaError> {
        self.get_graph()
            .as_object()
            .unwrap()
            .call_method(state, method, args, listeners)
    }

    fn render(self: &Arc<Self>, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    where
        Self: Sized + 'static,
    {
        self.get_graph().as_object().unwrap().render(f)
    }
}
