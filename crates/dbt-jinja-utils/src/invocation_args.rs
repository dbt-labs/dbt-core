use std::{collections::BTreeMap, env, rc::Rc, sync::Arc};

use dbt_common::io_args::EvalArgs;
use dbt_common::io_args::ReplayMode;
use itertools::Itertools;
use log::LevelFilter;
use minijinja::{
    Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, State,
    listener::RenderingEventListener,
    value::{Object, ObjectRepr, Value, mutable_map::MutableMap},
};

/// Invocation args is the dictionary of arguments passed into the jinja environment.
// TODO: this is not complete, we will add more as we go.
#[derive(Debug, Clone, Default)]
pub struct InvocationArgs {
    /// command
    pub invocation_command: String,
    /// vars
    pub vars: BTreeMap<String, Value>,
    /// select
    pub select: Option<String>,
    /// exclude
    pub exclude: Option<String>,
    /// profiles_dir
    pub profiles_dir: Option<String>,
    /// packages_install_path
    pub packages_install_path: Option<String>,
    /// target
    pub target: Option<String>,
    /// num_threads
    pub num_threads: Option<usize>,
    /// invocation_id
    pub invocation_id: uuid::Uuid,

    // and here are all flags
    /// Flags
    pub warn_error: bool,
    /// Warning error options
    pub warn_error_options: BTreeMap<String, Value>,
    /// Version check
    pub version_check: bool,
    /// Defer
    pub defer: bool,
    /// Defer state
    pub defer_state: String,
    /// Debug
    pub debug: bool,
    /// Log format file
    pub log_format_file: String,
    /// Log format
    pub log_format: String,
    /// Log level file
    pub log_level_file: String,
    /// Log level
    pub log_level: String,
    /// Log path
    pub log_path: String,
    /// Profile
    pub profile: String,
    /// Project dir
    pub project_dir: String,
    /// Quiet
    pub quiet: bool,
    /// Resource type
    pub resource_type: Vec<String>,
    /// Send anonymous usage stats
    pub send_anonymous_usage_stats: bool,
    /// Write json
    pub write_json: bool,
    /// Full refresh
    pub full_refresh: bool,

    /// Replay mode (when running against a recording)
    pub replay: Option<ReplayMode>,
}

impl InvocationArgs {
    /// Create an InvocationArgs from an EvalArgs.
    pub fn from_eval_args(arg: &EvalArgs) -> Self {
        let log_level = arg.log_level.unwrap_or(LevelFilter::Info);

        let log_level_file = arg.log_level_file.unwrap_or(log_level);

        let log_format = arg.log_format;
        let log_format_file = arg.log_format_file.unwrap_or(log_format);

        InvocationArgs {
            invocation_command: arg.command.as_str().to_string(),
            vars: arg
                .vars
                .iter()
                .map(|(k, v)| {
                    let value = Value::from_serialize(v);
                    (k.clone(), value)
                })
                .collect(),
            select: arg.select.clone().map(|select| select.to_string()),
            exclude: arg.exclude.clone().map(|exclude| exclude.to_string()),
            profiles_dir: arg
                .profiles_dir
                .clone()
                .map(|path| path.to_string_lossy().to_string()),
            packages_install_path: arg
                .packages_install_path
                .clone()
                .map(|path| path.to_string_lossy().to_string()),
            target: arg.target.clone(),
            // unrestricted multi-threading
            num_threads: arg.num_threads,
            invocation_id: arg.io.invocation_id,
            warn_error: arg.warn_error,
            warn_error_options: arg
                .warn_error_options
                .iter()
                .map(|(k, v)| {
                    let value = Value::from_serialize(v);
                    (k.clone(), value)
                })
                .collect(),
            version_check: arg.version_check,
            defer: arg.defer,
            defer_state: arg
                .defer_state
                .clone()
                .unwrap_or_default()
                .display()
                .to_string(),
            debug: arg.debug,
            log_format: log_format.to_string(),
            log_format_file: log_format_file.to_string(),
            log_level: log_level.to_string(),
            log_level_file: log_level_file.to_string(),
            log_path: arg
                .log_path
                .clone()
                .unwrap_or_default()
                .display()
                .to_string(),
            profile: arg.profile.clone().unwrap_or_default(),
            project_dir: arg
                .project_dir
                .clone()
                .unwrap_or_default()
                .display()
                .to_string(),
            quiet: arg.quiet,
            resource_type: arg.resource_types.iter().map(|rt| rt.to_string()).collect(),
            send_anonymous_usage_stats: arg.send_anonymous_usage_stats,
            write_json: arg.write_json,
            full_refresh: arg.full_refresh,
            replay: arg.replay.clone(),
        }
    }

    /// Convert the InvocationArgs to a dictionary.
    pub fn to_dict(&self) -> BTreeMap<String, Value> {
        let mut dict = BTreeMap::new();
        dict.insert(
            "which".to_string(),
            Value::from(self.invocation_command.clone()),
        );

        dict.insert(
            "invocation_command".to_string(),
            Value::from(env::args().join(" ")),
        );

        // Convert vars to a MutableMap so it can be copied in templates
        let vars_map = MutableMap::new();
        for (k, v) in &self.vars {
            vars_map.insert(Value::from(k.clone()), v.clone());
        }
        dict.insert("vars".to_string(), Value::from_object(vars_map));
        dict.insert("select".to_string(), Value::from(self.select.clone()));
        dict.insert("exclude".to_string(), Value::from(self.exclude.clone()));
        dict.insert(
            "profiles_dir".to_string(),
            Value::from(self.profiles_dir.clone()),
        );
        dict.insert(
            "packages_install_path".to_string(),
            Value::from(self.packages_install_path.clone()),
        );
        dict.insert("target".to_string(), Value::from(self.target.clone()));
        dict.insert("num_threads".to_string(), Value::from(self.num_threads));
        dict.insert(
            "invocation_id".to_string(),
            Value::from(self.invocation_id.to_string()),
        );
        dict.insert("warn_error".to_string(), Value::from(self.warn_error));
        dict.insert(
            "warn_error_options".to_string(),
            Value::from(
                self.warn_error_options
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_string()))
                    .collect::<BTreeMap<_, _>>(),
            ),
        );
        dict.insert("version_check".to_string(), Value::from(self.version_check));
        dict.insert("defer".to_string(), Value::from(self.defer));
        dict.insert(
            "defer_state".to_string(),
            Value::from(self.defer_state.clone()),
        );
        dict.insert("debug".to_string(), Value::from(self.debug));
        dict.insert(
            "log_format_file".to_string(),
            Value::from(self.log_format_file.clone()),
        );
        dict.insert(
            "log_format".to_string(),
            Value::from(self.log_format.clone()),
        );
        dict.insert(
            "log_level_file".to_string(),
            Value::from(self.log_level_file.clone()),
        );
        dict.insert("log_level".to_string(), Value::from(self.log_level.clone()));
        dict.insert("log_path".to_string(), Value::from(self.log_path.clone()));
        dict.insert("profile".to_string(), Value::from(self.profile.clone()));
        dict.insert(
            "project_dir".to_string(),
            Value::from(self.project_dir.clone()),
        );
        dict.insert("quiet".to_string(), Value::from(self.quiet));
        dict.insert(
            "resource_type".to_string(),
            Value::from(self.resource_type.clone()),
        );
        dict.insert(
            "send_anonymous_usage_stats".to_string(),
            Value::from(self.send_anonymous_usage_stats),
        );
        dict.insert("write_json".to_string(), Value::from(self.write_json));
        dict.insert("full_refresh".to_string(), Value::from(self.full_refresh));
        dict.insert("replay".to_string(), Value::from(self.replay.is_some()));
        // make all keys uppercase
        dict.into_iter()
            .map(|(key, value)| (key.to_uppercase(), value))
            .collect()
    }

    /// Set the number of threads to use.
    pub fn set_num_threads(&self, final_threads: Option<usize>) -> Self {
        Self {
            num_threads: final_threads,
            ..self.clone()
        }
    }
}

/// A wrapper around the invocation args dictionary that provides object methods
/// like `.vars` and `.copy()` for use in Jinja templates.
#[derive(Debug, Clone)]
pub struct InvocationArgsDict {
    dict: BTreeMap<String, Value>,
}

impl InvocationArgsDict {
    /// Create a new InvocationArgsDict from a dictionary
    pub fn new(dict: BTreeMap<String, Value>) -> Self {
        Self { dict }
    }
}

impl Object for InvocationArgsDict {
    fn repr(self: &Arc<Self>) -> ObjectRepr {
        ObjectRepr::Map
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        if let Some(s) = key.as_str() {
            // The dict keys are uppercase (see to_dict method), but we want to access them
            // with lowercase in templates for consistency with dbt
            let uppercase_key = s.to_uppercase();
            return self.dict.get(&uppercase_key).cloned();
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
            "copy" => {
                // Ensure no arguments are passed
                if !args.is_empty() {
                    return Err(MinijinjaError::new(
                        MinijinjaErrorKind::TooManyArguments,
                        "copy() takes no arguments",
                    ));
                }

                // Create a mutable copy
                let map = MutableMap::new();
                for (k, v) in &self.dict {
                    map.insert(Value::from(k.clone()), v.clone());
                }
                Ok(Value::from_object(map))
            }
            _ => Err(MinijinjaError::new(
                MinijinjaErrorKind::UnknownMethod,
                format!("InvocationArgsDict has no method named '{name}'"),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_common::io_args::ReplayMode;

    #[test]
    // Replay mode is detected during macro
    // registration (parse phase) via `invocation_args_dict.REPLAY`. If this key stops being
    // surfaced by `InvocationArgs::to_dict()`, replay-only macro overrides (e.g. to suppress
    // non-semantic package behavior) will silently stop applying.
    fn to_dict_includes_replay_flag() {
        let args = InvocationArgs {
            replay: Some(ReplayMode::FsReplay("some/path".into())),
            ..InvocationArgs::default()
        };
        let d = args.to_dict();
        let replay = d.get("REPLAY").expect("REPLAY should be present");
        assert!(
            replay.is_true(),
            "REPLAY should be present and truthy, got: {replay:?}"
        );

        let args2 = InvocationArgs {
            replay: None,
            ..InvocationArgs::default()
        };
        let d2 = args2.to_dict();
        let replay2 = d2.get("REPLAY").expect("REPLAY should be present");
        assert!(
            !replay2.is_true(),
            "REPLAY should be present and falsy, got: {replay2:?}"
        );
    }
}
