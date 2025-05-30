use minijinja::listener::RenderingEventListener;
use minijinja::value::Object;
use minijinja::Value;
use minijinja::{Error as MinijinjaError, State};

use std::rc::Rc;
use std::sync::Arc;

/// Configuration used to create a BehaviorFlagRendered instance
///
/// Args:
///     name: the name of the behavior flag
///     default: default setting, starts as False, becomes True after a bake-in period
///     description: an additional message to send when the flag evaluates to False
///     docs_url: the url to the relevant docs on docs.getdbt.com
/// *Note*:
///     While `description` and `docs_url` are both listed as `NotRequired`, at least one of them is required.
///     This is validated when the flag is rendered in `BehaviorFlagRendered` below.
///     The goal of this restriction is to provide the end user with context so they can make an informed decision
///     about if, and when, to enable the behavior flag.
#[derive(Debug, Clone)]
pub struct BehaviorFlag {
    name: &'static str,
    #[allow(dead_code)]
    default: bool,
    #[allow(dead_code)]
    source: Option<&'static str>,
    #[allow(dead_code)]
    description: Option<&'static str>,
    #[allow(dead_code)]
    docs_url: Option<&'static str>,
}

impl BehaviorFlag {
    /// Creates a new instance
    pub fn new(
        name: &'static str,
        default: bool,
        source: Option<&'static str>,
        description: Option<&'static str>,
        docs_url: Option<&'static str>,
    ) -> Self {
        Self {
            name,
            default,
            source,
            description,
            docs_url,
        }
    }

    fn no_warn(&self) -> bool {
        self.default
    }
}

impl Object for BehaviorFlag {
    fn call_method(
        self: &Arc<Self>,
        _state: &State,
        _name: &str,
        _args: &[Value],
        _listener: Rc<dyn RenderingEventListener>,
    ) -> Result<Value, MinijinjaError> {
        panic!("Cannot call any method on BehaviorFlag, such feature is not supported")
    }

    // this is invoked when a flag is used in an if statement
    fn is_true(self: &Arc<Self>) -> bool {
        self.default
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str() {
            Some("no_warn") => Some(Value::from(self.no_warn())),
            _ => unimplemented!("key {} is not supported on BehaviorFlag", key),
        }
    }
}

///  A collection of behavior flags
///
/// This is effectively a dictionary that supports dot notation for easy reference, e.g.:
///     ```jinja
///     {% if adapter.behavior.my_flag %}
///            ...
///     {% endif %}
///
///     {% if adapter.behavior.my_flag.no_warn %}  {# this will not fire the behavior change event #}
///         ...
///     {% endif %}
///     ```
///
/// Args:
///     flags: a list of configurations, one for each behavior flag
///     user_overrides: a set of user settings, which may include overrides on one or more of the behavior flags
#[derive(Debug, Clone)]
pub struct Behavior {
    flags: Vec<BehaviorFlag>,
}

impl Behavior {
    /// Creates an instance
    pub fn new(flags: &[BehaviorFlag]) -> Self {
        Self {
            flags: flags.to_vec(),
        }
    }
}

impl Object for Behavior {
    fn call_method(
        self: &Arc<Self>,
        _state: &State,
        _name: &str,
        _args: &[Value],
        _listener: Rc<dyn RenderingEventListener>,
    ) -> Result<Value, MinijinjaError> {
        panic!("Cannot call any method on Behavior, such feature is not supported")
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str() {
            Some(s) => self
                .flags
                .iter()
                .find(|flag| flag.name == s)
                .map(|flag| Value::from_object(flag.clone())),
            _ => None,
        }
    }
}
