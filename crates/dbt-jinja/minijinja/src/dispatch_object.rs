use crate::constants::MACRO_DISPATCH_ORDER;
use crate::constants::TARGET_PACKAGE_NAME;
use crate::listener::RenderingEventListener;
use crate::value::Enumerator;
use crate::value::Object;
use crate::value::Value;
use crate::value::ValueMap;
use crate::vm::INCLUDE_RECURSION_COST;
use crate::{Error, ErrorKind, State};

use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;

/// all the package names in the project and their dependencies
/// - Contains a map of package_name -> Vec<dependencies>
/// - Also contains a special entry "__all__" -> Vec<all_package_names>
pub static THREAD_LOCAL_DEPENDENCIES: OnceLock<Mutex<BTreeSet<String>>> = OnceLock::new();

/// Dispatch object for Jinja templates
#[derive(Debug)]
pub struct DispatchObject {
    /// The name of the macro to dispatch
    pub macro_name: String,
    /// The user-specified package name to disptch to
    pub package_name: Option<String>,
    /// When true, only look up in the specified package, when false
    /// fallback to default lookup behavior
    pub strict: bool,
    /// Indicates if the object should be automatically invoked by the
    /// interpreter in case it is result of a method call
    pub auto_execute: bool,
    /// The context of the macro
    pub context: Option<Value>,
}

impl Object for DispatchObject {
    fn is_true(self: &Arc<Self>) -> bool {
        true
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        Enumerator::Str(&["macro_name", "package_name", "strict", "auto_execute"])
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str()? {
            "macro_name" => Some(Value::from(&self.macro_name)),
            "package_name" => self.package_name.as_ref().map(|s| Value::from(s.as_str())),
            "strict" => Some(Value::from(self.strict)),
            "auto_execute" => Some(Value::from(self.auto_execute)),
            _ => None,
        }
    }

    fn call(
        self: &Arc<Self>,
        state: &State<'_, '_>,
        args: &[Value],
        listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, Error> {
        // Check for "." in macro_name
        // TODO: delete this, this is already checked in the adapter.dispatch method
        if self.macro_name.contains('.') {
            let parts: Vec<&str> = self.macro_name.split('.').collect();
            if parts.len() == 2 {
                let (suggest_macro_namespace, suggest_macro_name) = (parts[0], parts[1]);
                let msg = format!(
                    "In adapter.dispatch, got a macro name of \"{}\", \
                    but \".\" is not a valid macro name component. Did you mean \
                    `adapter.dispatch(\"{}\", macro_namespace=\"{}\")`?",
                    self.macro_name, suggest_macro_name, suggest_macro_namespace
                );
                return Err(Error::new(ErrorKind::UnknownFunction, msg));
            }
        }

        let mut attempts = Vec::new();

        // Handle strict mode (used when we want to dispatch to a specific macro in a package with no prefixes)
        if self.strict {
            if let Some(pkg) = &self.package_name {
                let template_name = format!("{}.{}", pkg, self.macro_name);
                attempts.push(template_name.clone());

                // Try to execute the template, but catch any errors and convert to a strict mode error
                match self.execute_template(state, &template_name, args, listeners) {
                    Ok(rv) => return Ok(rv),
                    Err(_) => {
                        // In strict mode, we want a specific error message
                        return Err(Error::new(
                            ErrorKind::UnknownFunction,
                            format!(
                                "In strict mode: No macro named '{}' found in package '{}'",
                                self.macro_name, pkg
                            ),
                        ));
                    }
                }
            }
        }

        // Get search packages according to dbt's logic
        let search_packages = self.get_search_packages(state);

        // Get dialect from environment
        let dialect = state
            .env()
            .get_dialect()
            .map(|v| v.as_str().expect("dialect should be a string"))
            .unwrap_or("postgres");

        // Get adapter specific prefixes
        let adapter_prefixes = get_adapter_prefixes(dialect);
        // get not internal packages
        let non_internal_namespace = state.env().get_non_internal_packages();
        // get dbt and adapters namespace
        let dbt_and_adapters_namespace = state.env().get_dbt_and_adapters_namespace();

        // First try with specific packages if specified
        // The logic below comes from https://github.com/dbt-labs/dbt-core/blob/4aa5169212d8256002095d44dc5f2505dca1b07c/core/dbt/context/providers.py#L158
        for package_name_opt in &search_packages {
            if let Some(package_name) = package_name_opt {
                for prefix in &adapter_prefixes {
                    let search_name = format!("{}__{}", prefix, self.macro_name);
                    if package_name == "dbt" {
                        // For dbt package, check dbt_and_adapters namespace
                        let search_name_value = Value::from(&search_name);
                        if let Some(pkg) = dbt_and_adapters_namespace.get(&search_name_value) {
                            let template_name = format!("{}.{}", pkg, search_name);
                            attempts.push(template_name.clone());
                            let rv =
                                self.execute_template(state, &template_name, args, listeners)?;
                            return Ok(rv);
                        }
                    } else if non_internal_namespace.contains_key(&Value::from(package_name)) {
                        // For non-internal packages
                        let template_name = format!("{}.{}", package_name, search_name);
                        attempts.push(template_name.clone());
                        if template_exists(state, &template_name) {
                            let rv =
                                self.execute_template(state, &template_name, args, listeners)?;
                            return Ok(rv);
                        }
                    }
                }
            } else {
                // Iterate through adapter prefixes and try to find a template
                for prefix in &adapter_prefixes {
                    let search_name = format!("{}__{}", prefix, self.macro_name);

                    if let Some(template_name) =
                        macro_namespace_template_resolver(state, &search_name, &mut attempts)
                    {
                        let rv = self.execute_template(state, &template_name, args, listeners)?;
                        return Ok(rv);
                    }
                }
            }
        }

        // Format error message
        let searched = attempts
            .iter()
            .map(|a| format!("'{}'", a))
            .collect::<Vec<_>>()
            .join(", ");

        // Create error with the original file information preserved
        let mut err = Error::new(
            ErrorKind::UnknownFunction,
            format!(
                "In dispatch: No macro named '{}' found within namespace: '{}'\n    Searched for: {}",
                self.macro_name,
                self.package_name.clone().unwrap_or_else(|| "None".to_string()),
                searched
            ),
        );

        // Set the error to use the current instruction's file name
        err.insert_filename_and_line(state.instructions.name(), 1);

        Err(err)
    }
}

impl DispatchObject {
    // Update the get_search_packages method to better match Python's logic
    fn get_search_packages(&self, state: &State<'_, '_>) -> Vec<Option<String>> {
        let root_package = state.env().get_root_package_name();

        match &self.package_name {
            None => {
                // When no namespace is specified, return [None]
                vec![None]
            }
            Some(namespace) => {
                // First check macro_dispatch_order (custom search order)
                // TODO @venkaa28: I (@akbog) moved the lookup here from the environment
                // since there was only a single call site for it. However, I don't like that
                // this would default to empty in the previous implementation.
                let macro_dispatch_order = state
                    .lookup(MACRO_DISPATCH_ORDER)
                    .unwrap_or_default()
                    .downcast_object::<ValueMap>()
                    .unwrap_or(Arc::new(ValueMap::new()));

                if let Some(order) = macro_dispatch_order.get(&Value::from(namespace.as_str())) {
                    // Use configured order
                    order
                        .downcast_object::<Vec<String>>()
                        .unwrap_or_else(|| Arc::new(vec![]))
                        .as_ref()
                        .iter()
                        .map(|s| Some(s.clone()))
                        .collect()
                } else {
                    // namespaced dispatch looks if the namespace is a dependency of the root project (all packages)
                    #[allow(clippy::incompatible_msrv)]
                    let is_dependency = THREAD_LOCAL_DEPENDENCIES
                        .get()
                        .unwrap()
                        .lock()
                        .unwrap()
                        .contains(namespace);
                    if is_dependency {
                        // For dependencies without explicit order check the root package first, then the namespace
                        vec![Some(root_package), Some(namespace.clone())]
                    } else {
                        // Default case when namespace is specified, no search order, and the namespace
                        // is not a dependency of the current package
                        // Match dbt's behavior of returning [None]
                        vec![None]
                    }
                }
            }
        }
    }

    fn execute_template(
        &self,
        state: &State<'_, '_>,
        template_name: &str,
        args: &[Value],
        listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, Error> {
        let template = match state.env().get_template(template_name) {
            Ok(template) => template,
            Err(err) => {
                // If the template name was found in a namespace but the template itself doesn't exist,
                // this should be a hard error rather than a silent continuation
                return Err(Error::new(
                    ErrorKind::TemplateNotFound,
                    format!(
                        "Template '{}' was found in namespace but cannot be loaded: {}",
                        template_name, err
                    ),
                ));
            }
        };

        let template_state = template.eval_to_state_with_outer_stack_depth(
            self.context
                .clone()
                .unwrap_or_else(|| state.get_base_context()),
            listeners,
            state.ctx.depth() + INCLUDE_RECURSION_COST,
        )?;

        let func = template_state
            .lookup(
                template_name
                    .split('.')
                    .next_back()
                    .expect("template_name should have a dot"),
            )
            .expect("function should exist in template");

        match func.call(&template_state, args, listeners) {
            Ok(rv) => Ok(rv),
            Err(err) => match err.try_abrupt_return() {
                Some(rv) => Ok(rv.clone()),
                None => Err(err),
            },
        }
    }
}

/// Helper method to get adapter prefixes including parents
pub fn get_adapter_prefixes(dialect: &str) -> Vec<String> {
    let mut prefixes = Vec::new();

    // Current adapter
    prefixes.push(dialect.to_string());

    // Add parent adapters
    match dialect {
        "redshift" => prefixes.push("postgres".to_string()),
        "databricks" => prefixes.push("spark".to_string()),
        // Add other adapter hierarchies as needed
        _ => {}
    }

    // Always add default as last fallback
    prefixes.push("default".to_string());

    prefixes
}

/// Helper method to get internal packages
pub fn get_internal_packages(dialect: &str) -> Vec<String> {
    let mut internal_packages = Vec::new();

    internal_packages.push(format!("dbt_{}", dialect));

    // Add parent packages
    match dialect {
        "redshift" => internal_packages.push("dbt_postgres".to_string()),
        "databricks" => internal_packages.push("dbt_spark".to_string()),
        // Add other adapter hierarchies as needed
        _ => {}
    }
    internal_packages.push("dbt".to_string());

    internal_packages
}

/// Helper function to check if a template exists in the environment
fn template_exists(state: &State<'_, '_>, template_name: &str) -> bool {
    state.env().get_template(template_name).is_ok()
}

/// Finds a template in the namespace according to dbt's resolution rules
///
/// Follows dbt's search order:
/// 1. Local namespace (current package)
/// 2. Root package namespace
/// 3. Internal packages (dbt and adapters)
///
/// # Arguments
/// * `state` - The current state object containing environment info
///     - state must have the following attributes:
///         - root package name from env().get_root_package_name() -- Name of the root package
///         - non-internal packages from env().get_non_internal_packages() -- Map of non-internal packages
///         - dbt and adapters namespace from env().get_dbt_and_adapters_namespace() -- Map of internal packages (dbt and adapters)
/// * `search_name` - Name of the macro to resolve, including prefix (e.g., "postgres__get_test_value")
///
/// Thread local variables:
///     * `current_package_name` - Name of the current package
/// * `attempts` - A vector to track attempted template paths (for error reporting)
///
/// # Returns
/// * `Result<Option<String>, Error>` - The template path if found, None otherwise
///
/// Logic comes from https://github.com/dbt-labs/dbt-core/blob/4aa5169212d8256002095d44dc5f2505dca1b07c/core/dbt/context/macros.py#L83
/// and https://github.com/dbt-labs/dbt-core/blob/4aa5169212d8256002095d44dc5f2505dca1b07c/core/dbt/context/macros.py#L34
///
pub fn macro_namespace_template_resolver(
    state: &State<'_, '_>,
    search_name: &str,
    attempts: &mut Vec<String>,
) -> Option<String> {
    // Get necessary values from state
    let current_package_name = state
        .lookup(TARGET_PACKAGE_NAME)
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or("dbt".to_string());
    let root_package = state.env().get_root_package_name();
    let dbt_and_adapters = state.env().get_dbt_and_adapters_namespace();

    // 1. Local namespace (current package)
    let template_name = format!("{}.{}", current_package_name, search_name);
    attempts.push(template_name.clone());
    if template_exists(state, &template_name) {
        return Some(template_name);
    }

    // 2. Root package namespace
    let template_name = format!("{}.{}", root_package, search_name);
    attempts.push(template_name.clone());
    if template_exists(state, &template_name) {
        return Some(template_name);
    }

    // 3. Internal packages
    let search_name_value = Value::from(search_name);
    if let Some(pkg) = dbt_and_adapters.get(&search_name_value) {
        let template_name = format!("{}.{}", pkg, search_name);
        attempts.push(template_name.clone());
        if template_exists(state, &template_name) {
            return Some(template_name);
        }
    }

    // No template found
    None
}
