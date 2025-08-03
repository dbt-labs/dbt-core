use std::fmt;
use std::rc::Rc;
use std::sync::Arc;

use dbt_fusion_adapter::ParseAdapter;
use minijinja::listener::RenderingEventListener;
use minijinja::value::Object;
use minijinja::{
    Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, State, Value as MinijinjaValue,
};

/// A namespace object that intercepts specific dbt macro calls
/// to track them in the ParseAdapter before delegating to the original templates
#[derive(Debug, Clone)]
pub struct DbtNamespace {
    parse_adapter: Arc<ParseAdapter>,
}

impl DbtNamespace {
    /// Creates a new DbtNamespace that tracks calls in the ParseAdapter
    pub fn new(parse_adapter: Arc<ParseAdapter>) -> Self {
        Self { parse_adapter }
    }
}

impl fmt::Display for DbtNamespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DbtNamespace")
    }
}

impl Object for DbtNamespace {
    fn call_method(
        self: &Arc<Self>,
        state: &State,
        name: &str,
        args: &[MinijinjaValue],
        listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<MinijinjaValue, MinijinjaError> {
        // Intercept specific method calls to track them
        match name {
            "get_columns_in_relation" => {
                // Track the call in the parse adapter
                self.parse_adapter
                    .record_get_columns_in_relation_call(state, args)?;

                // Delegate to the original dbt.get_columns_in_relation template
                let template_name = "dbt.get_columns_in_relation";
                if let Ok(template) = state.env().get_template(template_name, listeners) {
                    let base_ctx = state.get_base_context();
                    let template_state = template.eval_to_state(base_ctx, listeners)?;
                    let func = template_state
                        .lookup("get_columns_in_relation")
                        .ok_or_else(|| {
                            MinijinjaError::new(
                                MinijinjaErrorKind::InvalidOperation,
                                "get_columns_in_relation macro not found",
                            )
                        })?;
                    func.call(&template_state, args, listeners)
                } else {
                    Err(MinijinjaError::new(
                        MinijinjaErrorKind::TemplateNotFound,
                        format!("Template {template_name} not found"),
                    ))
                }
            }
            "get_relation" => {
                // Track the call in the parse adapter if in execute mode
                self.parse_adapter.record_get_relation_call(state, args)?;

                // Delegate to the original dbt.get_relation template
                let template_name = "dbt.get_relation";
                if let Ok(template) = state.env().get_template(template_name, listeners) {
                    let base_ctx = state.get_base_context();
                    let template_state = template.eval_to_state(base_ctx, listeners)?;
                    let func = template_state.lookup("get_relation").ok_or_else(|| {
                        MinijinjaError::new(
                            MinijinjaErrorKind::InvalidOperation,
                            "get_relation macro not found",
                        )
                    })?;
                    func.call(&template_state, args, listeners)
                } else {
                    Err(MinijinjaError::new(
                        MinijinjaErrorKind::TemplateNotFound,
                        format!("Template {template_name} not found"),
                    ))
                }
            }
            _ => {
                // For all other methods, delegate to the original dbt template
                let template_name = format!("dbt.{name}");
                if let Ok(template) = state.env().get_template(&template_name, listeners) {
                    let base_ctx = state.get_base_context();
                    let template_state = template.eval_to_state(base_ctx, listeners)?;
                    let func = template_state.lookup(name).ok_or_else(|| {
                        MinijinjaError::new(
                            MinijinjaErrorKind::InvalidOperation,
                            format!("{name} macro not found"),
                        )
                    })?;
                    func.call(&template_state, args, listeners)
                } else {
                    Err(MinijinjaError::new(
                        MinijinjaErrorKind::TemplateNotFound,
                        format!("Template {template_name} not found"),
                    ))
                }
            }
        }
    }
}
