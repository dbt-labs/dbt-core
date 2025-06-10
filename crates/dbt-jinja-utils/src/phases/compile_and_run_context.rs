//! This module contains the functions for initializing the Jinja environment for the compile phase.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::functions::build_flat_graph;
use crate::jinja_environment::JinjaEnvironment;
use dbt_common::once_cell_vars::DISPATCH_CONFIG;
use dbt_fusion_adapter::adapters::BaseAdapter;
use dbt_schemas::schemas::manifest::Nodes;
use dbt_schemas::state::{DbtRuntimeConfig, RefsAndSourcesTracker};
use minijinja::arg_utils::ArgParser;
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
    env: &mut JinjaEnvironment<'static>,
    adapter: Arc<dyn BaseAdapter>,
) {
    env.set_adapter(adapter);
    env.set_undefined_behavior(UndefinedBehavior::Lenient);
}

/// Configure the Jinja environment for the compile phase.
pub fn build_compile_and_run_base_context(
    refs_and_sources: Arc<dyn RefsAndSourcesTracker>,
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

    // Create ref function
    let ref_function = RefFunction {
        refs_and_sources: refs_and_sources.clone(),
        package_name: package_name.to_owned(),
        runtime_config,
    };
    let ref_value = MinijinjaValue::from_object(ref_function);
    ctx.insert("ref".to_string(), ref_value.clone());
    builtins.insert("ref".to_string(), ref_value);

    // Create source function
    let source_function = SourceFunction {
        refs_and_sources: refs_and_sources.clone(),
        package_name: package_name.to_owned(),
    };
    let source_value = MinijinjaValue::from_object(source_function);
    ctx.insert("source".to_string(), source_value.clone());
    builtins.insert("source".to_string(), source_value);

    // This is used in macros to gate the sql execution (set to true only after parse stage)
    // for example dbt_macro_assets/dbt-adapters/macros/etc/statement.sql
    ctx.insert("execute".to_string(), MinijinjaValue::from(true));

    // Register builtins as a global
    ctx.insert(
        "builtins".to_string(),
        MinijinjaValue::from_object(builtins),
    );

    // Register graph as a global
    ctx.insert(
        "graph".to_string(),
        MinijinjaValue::from(build_flat_graph(nodes)),
    );
    ctx
}

#[derive(Debug)]
struct RefFunction {
    refs_and_sources: Arc<dyn RefsAndSourcesTracker>,
    package_name: String,
    runtime_config: Arc<DbtRuntimeConfig>,
}

impl RefFunction {
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
}

impl Object for RefFunction {
    fn call(
        self: &Arc<Self>,
        _state: &State<'_, '_>,
        args: &[MinijinjaValue],
        _listener: Rc<dyn RenderingEventListener>,
    ) -> Result<MinijinjaValue, MinijinjaError> {
        let (package_name, model_name, version) = self.resolve_args(args)?;

        match self.refs_and_sources.lookup_ref(
            &package_name,
            &model_name,
            &version,
            &Some(self.package_name.clone()),
        ) {
            Ok((_, relation, _)) => Ok(relation),
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
        _listener: Rc<dyn RenderingEventListener>,
    ) -> Result<MinijinjaValue, MinijinjaError> {
        match method {
            "id" => {
                let (package_name, model_name, version) = self.resolve_args(args)?;
                match self.refs_and_sources.lookup_ref(
                    &package_name,
                    &model_name,
                    &version,
                    &Some(self.package_name.clone()),
                ) {
                    Ok((unique_id, _, _)) => Ok(MinijinjaValue::from(unique_id)),
                    Err(_) => Err(MinijinjaError::new(
                        MinijinjaErrorKind::NonKey,
                        format!(
                            "ref not found for package: {}, model: {}, version: {:?}",
                            self.package_name, model_name, version
                        ),
                    )),
                }
            }
            _ => Err(MinijinjaError::from(MinijinjaErrorKind::UnknownMethod(
                "ref".to_string(),
                method.to_string(),
            ))),
        }
    }

    fn get_value(self: &Arc<Self>, key: &MinijinjaValue) -> Option<MinijinjaValue> {
        match key.as_str()? {
            "config" => Some(MinijinjaValue::from_object(
                self.runtime_config.to_minijinja_map(),
            )),
            "function_name" => Some(MinijinjaValue::from("ref")),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct SourceFunction {
    refs_and_sources: Arc<dyn RefsAndSourcesTracker>,
    package_name: String,
}

impl Object for SourceFunction {
    fn call(
        self: &Arc<Self>,
        _state: &State<'_, '_>,
        args: &[MinijinjaValue],
        _listener: Rc<dyn RenderingEventListener>,
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
            .refs_and_sources
            .lookup_source(&self.package_name, &source_name, &table_name)
        {
            Ok((_, relation, _)) => Ok(relation),
            Err(_) => Err(MinijinjaError::new(
                MinijinjaErrorKind::NonKey,
                format!(
                    "Source not found for source name: {}, table name: {}",
                    source_name, table_name
                ),
            )),
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
    /// The current project name.
    pub current_project_name: String,
    /// The packages in the project.
    pub packages: BTreeSet<String>,
}

impl Object for MacroLookupContext {
    fn get_value(self: &Arc<Self>, key: &MinijinjaValue) -> Option<MinijinjaValue> {
        match key.as_str()? {
            "project_name" => Some(MinijinjaValue::from(self.root_project_name.clone())),
            lookup_macro => {
                if self.packages.contains(lookup_macro) {
                    Some(MinijinjaValue::from_object(MacroLookupContext {
                        root_project_name: self.root_project_name.clone(),
                        current_project_name: lookup_macro.to_string(),
                        packages: self.packages.clone(),
                    }))
                } else {
                    Some(MinijinjaValue::from_object(DispatchObject {
                        macro_name: lookup_macro.to_string(),
                        package_name: Some(self.current_project_name.clone()),
                        strict: true,
                        auto_execute: false,
                        // TODO: If the macro uses a recursive context (i.e. context['self']) we will stack overflow
                        // but there is no way to conjure up a context object here without access to State
                        context: None,
                    }))
                }
            }
        }
    }
}
