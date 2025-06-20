use std::collections::BTreeMap;

use dbt_common::io_args::EvalArgs;
use log::LevelFilter;
use minijinja::Value;

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
    pub defer: Option<bool>,
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
}

impl InvocationArgs {
    /// Create an InvocationArgs from an EvalArgs.
    pub fn from_eval_args(arg: &EvalArgs) -> Self {
        let log_level = arg.log_level.unwrap_or(LevelFilter::Info);

        let log_level_file = arg.log_level_file.unwrap_or(log_level);

        let log_format = arg.log_format;
        let log_format_file = arg.log_format_file.unwrap_or(log_format);

        InvocationArgs {
            invocation_command: arg.command.clone(),
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
        }
    }

    /// Convert the InvocationArgs to a dictionary.
    pub fn to_dict(&self) -> BTreeMap<String, Value> {
        let mut dict = BTreeMap::new();
        dict.insert(
            "invocation_command".to_string(),
            Value::from(self.invocation_command.clone()),
        );
        dict.insert("vars".to_string(), Value::from_object(self.vars.clone()));
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
