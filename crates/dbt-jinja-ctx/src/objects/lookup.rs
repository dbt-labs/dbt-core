//! Macro / namespace lookup `Object` impls — std-only types moved here from
//! `dbt-jinja-utils` so the typed parse/compile/run-phase ctx structs can
//! hold them via `JinjaObject<T>` rather than opaque `MinijinjaValue`.
//!
//! Naming note: this module's [`DbtNamespace`] is the *dispatch-side*
//! namespace — it intercepts `{{ dbt.macro_name }}` lookups and produces
//! `DispatchObject`s for macro execution. There is a separate
//! `dbt_parser::dbt_namespace::DbtNamespace` (in the `dbt-parser` crate)
//! which is parse-phase adapter-call tracking (intercepts
//! `get_columns_in_relation` / `get_relation` to record introspection
//! calls). The two never collide at the type level (different crates,
//! different fully-qualified paths), but the name is reused. Renaming
//! either is a cross-cutting cleanup; leaving for now and flagging here.

use std::collections::BTreeSet;
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::Arc;

use minijinja::arg_utils::ArgsIter;
use minijinja::dispatch_object::DispatchObject;
use minijinja::listener::RenderingEventListener;
use minijinja::value::Object;
use minijinja::{Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, State, Value};

/// A namespace object for `{{ dbt.macro_name }}` (or
/// `{{ snowflake.macro_name }}` etc.) lookups during render. On
/// `get_property("name")` it consults the env's macro namespace registry
/// and produces a `DispatchObject` that, when called, executes the matching
/// macro.
#[derive(Debug, Clone)]
pub struct DbtNamespace {
    /// The namespace key (e.g. `"dbt"`, `"snowflake"`).
    pub name: String,
}

impl DbtNamespace {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}

impl Object for DbtNamespace {
    fn get_property(
        self: &Arc<Self>,
        state: &State<'_, '_>,
        name: &str,
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, MinijinjaError> {
        let ns_name = Value::from(self.name.clone());
        let namespace_registry = state
            .env()
            .get_macro_namespace_registry()
            .unwrap_or_default();
        let template_registry = state.env().get_macro_template_registry();
        // Could be a package name; check whether there's a macro in the namespace.
        if namespace_registry.get(&ns_name).is_some_and(|val| {
            val.try_iter()
                .map(|mut iter| iter.any(|v| v.as_str() == Some(name)))
                .unwrap_or(false)
        }) {
            let template_registry_entry = template_registry.get(&ns_name);
            let path = template_registry_entry
                .and_then(|entry| entry.get_attr("path").ok())
                .unwrap_or(ns_name);
            let span = template_registry_entry
                .and_then(|entry| entry.get_attr("span").ok())
                .unwrap_or_else(|| Value::from_serialize(minijinja::machinery::Span::default()));

            let context = state.get_base_context_with_path_and_span(&path, &span);
            Ok(Value::from_object(DispatchObject {
                macro_name: (*name).to_string(),
                package_name: Some(self.name.clone()),
                strict: true,
                auto_execute: false,
                context: Some(context),
            }))
        } else if self.name == "dbt" {
            let dbt_and_adapters = state.env().get_dbt_and_adapters_namespace();
            if let Some(package) = dbt_and_adapters.get(&Value::from(name)) {
                let package_name = package.as_str().map(|s| s.to_string());
                let template_registry_entry = template_registry.get(&ns_name);
                let path = template_registry_entry
                    .and_then(|entry| entry.get_attr("path").ok())
                    .unwrap_or(ns_name);
                let span = template_registry_entry
                    .and_then(|entry| entry.get_attr("span").ok())
                    .unwrap_or_else(
                        || Value::from_serialize(minijinja::machinery::Span::default()),
                    );

                let context = state.get_base_context_with_path_and_span(&path, &span);
                Ok(Value::from_object(DispatchObject {
                    macro_name: (*name).to_string(),
                    package_name,
                    strict: true,
                    auto_execute: false,
                    context: Some(context),
                }))
            } else {
                Ok(Value::UNDEFINED)
            }
        } else {
            Ok(Value::UNDEFINED)
        }
    }
}

/// Special context object available during the compile or run phase.
/// Allows users to look up macros by string and returns a `DispatchObject`,
/// which when called executes the macro. Users can also look up macro
/// namespaces by string, returning a nested `MacroLookupContext` that
/// itself, when called with a macro name, returns a `DispatchObject`.
#[derive(Debug, Clone)]
pub struct MacroLookupContext {
    /// The root project name.
    pub root_project_name: String,
    /// The current project name; when `None`, search begins from the root
    /// project.
    pub current_project_name: Option<String>,
    /// The packages available in the project.
    pub packages: BTreeSet<String>,
}

impl Object for MacroLookupContext {
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str()? {
            // NOTE(serramatutu): In Core, the following non-macro keys are
            // all members of `MacroLookupContext`. They can technically be
            // used (the usage is undocumented and not encouraged):
            // dbt_version, project_name, schema, run_started_at.
            //
            // We added `project_name` because some naughty famous macro
            // uses it and was breaking lots of projects, but I prefer to
            // avoid polluting this scope and sticking as faithfully as
            // possible to the "intended" behavior (only looking up macros).
            "project_name" => Some(Value::from(self.root_project_name.clone())),

            lookup_macro => {
                if self.packages.contains(lookup_macro) {
                    Some(Value::from_object(MacroLookupContext {
                        root_project_name: self.root_project_name.clone(),
                        current_project_name: Some(lookup_macro.to_string()),
                        packages: BTreeSet::new(),
                    }))
                } else {
                    Some(Value::from_object(DispatchObject {
                        macro_name: lookup_macro.to_string(),
                        package_name: self.current_project_name.clone(),
                        strict: self.current_project_name.is_some(),
                        auto_execute: false,
                        // TODO: If the macro uses a recursive context (i.e.
                        // context['self']) we will stack overflow but there
                        // is no way to conjure up a context object here
                        // without access to State.
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
        args: &[Value],
        listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, MinijinjaError> {
        // TODO(serramatutu): should this behave fully like a dict, with
        // values, keys, items, enumerate etc?
        match method {
            "get" => {
                let iter = ArgsIter::new("MacroLookupContext.get", &["key"], args);
                let key = iter.next_arg::<&Value>()?;
                let default = iter.next_kwarg::<Option<&Value>>("default")?;
                iter.finish()?;

                Ok(self
                    .get_value(key)
                    .or_else(|| default.cloned())
                    .unwrap_or(Value::from(None::<Value>)))
            }
            _ => {
                if let Some(value) = self.get_value(&Value::from(method)) {
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
