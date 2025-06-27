use std::fmt::Debug;
use std::sync::Arc;
use std::{collections::BTreeMap, rc::Rc};

use minijinja::{
    listener::RenderingEventListener,
    value::{Object, ObjectRepr, Value},
    Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, State,
};

use crate::invocation_args::InvocationArgs;
use crate::utils::get_method;

/// Minijinja Value representing the dbt flags collection
#[derive(Debug, Clone)]
pub struct Flags {
    flags: BTreeMap<String, Value>,
}

impl Object for Flags {
    fn repr(self: &Arc<Self>) -> ObjectRepr {
        ObjectRepr::Plain
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        if let Some(s) = key.as_str() {
            if self.flags.contains_key(s) {
                return Some(self.flags[s].clone());
            }
        }
        None
    }

    fn call_method(
        self: &Arc<Self>,
        _state: &State<'_, '_>,
        name: &str,
        args: &[Value],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, MinijinjaError> {
        match name {
            "get" => get_method(args, &self.flags),
            _ => Err(MinijinjaError::new(
                MinijinjaErrorKind::UnknownMethod("Flags".to_string(), name.to_string()),
                format!("Unknown method on flags: {name}"),
            )),
        }
    }
}

impl Flags {
    /// Create a new flags object with default values filled in.
    pub fn new() -> Flags {
        let mut flags = Flags {
            flags: BTreeMap::new(),
        };
        flags.set_defaults();
        flags
    }

    /// Create a new flags object including project-level flags.
    ///
    /// TODO: this is an incomplete support to unblock this use case (see snowflake__list_relations_without_caching macro)
    /// the actual dbt flags needs to encompass not only project flags, but also env vars, and cli options
    /// https://docs.getdbt.com/reference/global-configs/about-global-configs
    pub fn from_project_flags(project_flags: BTreeMap<String, Value>) -> Flags {
        let mut flags = Flags {
            flags: project_flags,
        };
        flags.set_defaults();
        flags
    }

    fn set_defaults(&mut self) {
        self.flags
            .insert("INDIRECT_SELECTION".to_string(), Value::from("eager"));
        self.flags
            .insert("TARGET_PATH".to_string(), Value::UNDEFINED);
        self.flags
            .insert("DEFER_STATE".to_string(), Value::UNDEFINED);
        self.flags
            .insert("WARN_ERROR".to_string(), Value::UNDEFINED);
        self.flags
            .insert("FULL_REFRESH".to_string(), Value::from(false));
        self.flags
            .insert("STRICT_MODE".to_string(), Value::from(false));
        self.flags
            .insert("STORE_FAILURES".to_string(), Value::from(false));
        self.flags
            .insert("INTROSPECT".to_string(), Value::from(true));
        self.flags.insert(
            "STATE_MODIFIED_COMPARE_VARS".to_string(),
            Value::from(false),
        );
    }

    /// Create a new flags object from the invocation args
    pub fn from_invocation_args(invocation_args: BTreeMap<String, Value>) -> Flags {
        Flags {
            flags: invocation_args,
        }
    }

    /// Get dictionary that represents this set of flags
    pub fn to_dict(&self) -> BTreeMap<String, Value> {
        self.flags.clone()
    }

    /// Set the flag's according to https://github.com/dbt-labs/dbt-core/blob/HEAD/core/dbt/flags.py
    pub fn set_cli_flags(&mut self, invocation_args: &InvocationArgs) {
        self.flags.insert(
            "WARN_ERROR".to_string(),
            Value::from(invocation_args.warn_error),
        );
        self.flags.insert(
            "WARN_ERROR_OPTIONS".to_string(),
            Value::from(
                invocation_args
                    .warn_error_options
                    .clone()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_string()))
                    .collect::<BTreeMap<String, String>>(),
            ),
        );
        self.flags.insert(
            "VERSION_CHECK".to_string(),
            Value::from(invocation_args.version_check),
        );
        self.flags
            .insert("DEFER".to_string(), Value::from(invocation_args.defer));
        self.flags.insert(
            "DEFER_STATE".to_string(),
            Value::from(invocation_args.defer_state.clone()),
        );
        self.flags
            .insert("DEBUG".to_string(), Value::from(invocation_args.debug));
        self.flags.insert(
            "LOG_FORMAT_FILE".to_string(),
            Value::from(invocation_args.log_format_file.clone()),
        );
        self.flags.insert(
            "LOG_FORMAT".to_string(),
            Value::from(invocation_args.log_format.clone()),
        );
        self.flags.insert(
            "LOG_LEVEL_FILE".to_string(),
            Value::from(invocation_args.log_level_file.clone()),
        );
        self.flags.insert(
            "LOG_LEVEL".to_string(),
            Value::from(invocation_args.log_level.clone()),
        );
        self.flags.insert(
            "LOG_PATH".to_string(),
            Value::from(invocation_args.log_path.clone()),
        );
        self.flags.insert(
            "PROFILE".to_string(),
            Value::from(invocation_args.profile.clone()),
        );
        self.flags.insert(
            "PROFILES_DIR".to_string(),
            Value::from(invocation_args.profiles_dir.clone().unwrap_or_default()),
        );
        self.flags.insert(
            "PROJECT_DIR".to_string(),
            Value::from(invocation_args.project_dir.clone()),
        );
        self.flags
            .insert("QUIET".to_string(), Value::from(invocation_args.quiet));
        self.flags.insert(
            "RESOURCE_TYPE".to_string(),
            Value::from(invocation_args.resource_type.clone()),
        );
        self.flags.insert(
            "SEND_ANONYMOUS_USAGE_STATS".to_string(),
            Value::from(invocation_args.send_anonymous_usage_stats),
        );
        self.flags.insert(
            "WRITE_JSON".to_string(),
            Value::from(invocation_args.write_json),
        );
    }
    /// Override self with other flags
    pub fn join(&mut self, other: Flags) -> Self {
        for (key, value) in other.flags {
            self.flags.insert(key, value); // Insert or override existing keys
        }
        self.clone() // Return the updated Flags
    }
}

impl Default for Flags {
    fn default() -> Self {
        Self::new()
    }
}
