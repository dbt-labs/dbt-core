use crate::constants::EXECUTING;
use crate::io_args::IoArgs;
use crate::pretty_string::remove_ansi_codes;
use crate::FsResult;
use clap::ValueEnum;
use log::{LevelFilter, Metadata, Record};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::fmt::Display;
use std::io::{IsTerminal as _, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

const QUERY_LOG_SQL: &str = "query_log.sql";

/// Predicate to check if a key in a log [Record] is an internal logging key.
/// These keys are used internally by the logger for e.g. progress bar control
/// and stat tracking, and should not be propagated to the log output.
pub fn is_fusion_internal_key(key: &str) -> bool {
    key.starts_with("_") && key.ends_with("_")
}

// Logger configuration for individual loggers
#[derive(Clone)]
struct LoggerConfig {
    level_filter: LevelFilter,
    format: LogFormat,
    min_level: Option<LevelFilter>, // Minimum level to log (inclusive)
    max_level: Option<LevelFilter>, // Maximum level to log (inclusive)
    // targets to include
    includes: Option<Vec<String>>,
    // targets to exclude
    excludes: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, ValueEnum, Serialize, Copy)]
pub enum LogFormat {
    Text,
    Json,
    Fancy,
}

impl Default for LogFormat {
    fn default() -> Self {
        Self::Text
    }
}
impl Display for LogFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogFormat::Text => write!(f, "text"),
            LogFormat::Json => write!(f, "json"),
            LogFormat::Fancy => write!(f, "fancy"),
        }
    }
}

impl Default for LoggerConfig {
    fn default() -> Self {
        Self {
            level_filter: LevelFilter::Info,
            format: LogFormat::default(),
            min_level: None,
            max_level: None,
            includes: None,
            excludes: None,
        }
    }
}

struct KvVisitor<'a> {
    kvs: &'a mut HashMap<String, String>,
}

impl<'kvs> log::kv::VisitSource<'kvs> for KvVisitor<'_> {
    fn visit_pair(
        &mut self,
        key: log::kv::Key<'kvs>,
        value: log::kv::Value<'kvs>,
    ) -> Result<(), log::kv::Error> {
        if is_fusion_internal_key(key.as_str()) {
            // Skip special keys that are handled by the logger itself
            return Ok(());
        }

        self.kvs.insert(key.to_string(), value.to_string());
        Ok(())
    }
}

enum LogTarget {
    Stdout,
    Stderr,
    Writer(Arc<Mutex<Box<dyn Write + Send>>>),
}

// Individual logger that can be customized
struct Logger {
    target: LogTarget,
    config: LoggerConfig,
    name: String,
    invocation_id: uuid::Uuid,
    remove_ansi_codes: bool,
}

impl Display for Logger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Logger {{ name: {}, level: {}, format: {:?} }}",
            self.name, self.config.level_filter, self.config.format
        )
    }
}

macro_rules! locked_writeln {
    ($self:expr, $($arg:tt)*) => {
        match $self.target {
            LogTarget::Stdout => {
                let mut writer = std::io::stdout().lock();
                writeln!(writer, $($arg)*).ok();
            }
            LogTarget::Stderr => {
                let mut writer = std::io::stderr().lock();
                writeln!(writer, $($arg)*).ok();
            }
            LogTarget::Writer(ref path) => {
                if let Ok(mut writer) = path.lock() {
                    writeln!(writer, $($arg)*).ok();
                }
            }
        }
    };
}

impl Logger {
    fn new(
        name: impl Into<String>,
        writer: LogTarget,
        config: LoggerConfig,
        invocation_id: uuid::Uuid,
    ) -> Self {
        let remove_ansi_codes = match writer {
            LogTarget::Stdout => !std::io::stdout().is_terminal(),
            LogTarget::Stderr => !std::io::stderr().is_terminal(),
            LogTarget::Writer(_) => true, // Always remove ANSI codes for file writers
        };
        Self {
            target: writer,
            config,
            name: name.into(),
            invocation_id,
            remove_ansi_codes,
        }
    }

    fn enabled(&self, metadata: &Metadata) -> bool {
        let current_level = metadata.level();
        // Check if the level is within the configured level filter
        if current_level > self.config.level_filter {
            return false;
        }
        // Counter intuitively, the lower the level, the more verbose the logging
        if let Some(min_level) = self.config.min_level {
            if current_level < min_level {
                return false;
            }
        }
        if let Some(max_level) = self.config.max_level {
            if current_level > max_level {
                return false;
            }
        }

        // filter based on target
        if metadata.target().starts_with("datafusion") {
            return false;
        }

        // Reject if not in includes (when set)
        if let Some(ref includes) = self.config.includes {
            if !includes.contains(&metadata.target().to_string()) {
                return false;
            }
        }
        // Reject if in excludes (when set)
        if let Some(ref excludes) = self.config.excludes {
            if excludes.contains(&metadata.target().to_string()) {
                return false;
            }
        }
        true
    }

    fn format_json(record: &Record, invocation_id: &str, should_remove_ansi: bool) -> String {
        // Collect key-value pairs
        let mut kvs = HashMap::new();

        let key_values = record.key_values();
        let mut visitor = KvVisitor { kvs: &mut kvs };
        key_values
            .visit(&mut visitor)
            .expect("Failed to visit key-values for json format");

        // Build a JSON structure with all key-value pairs as direct attributes
        let mut msg = record.args().to_string();
        if should_remove_ansi {
            msg = remove_ansi_codes(&msg);
        }
        // Start with the base JSON structure
        let mut info_json = json!({
            "category": "",
            "code": kvs.get("code").unwrap_or(&"".to_string()).to_string(),
            "invocation_id": invocation_id,
            "name": kvs.get("name").unwrap_or(&"Generic".to_string()).to_string(),
            "pid": std::process::id(),
            "thread": std::thread::current().name().unwrap_or("main").to_string(),
            // drop the timezone offset and format as microseconds to conform to python logging timestamp parsing
            "ts": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            "msg": msg,
            "level": record.level().to_string().to_lowercase(),
            "extra": {},
        });

        // Add all key-value pairs directly to the info_json object
        if let serde_json::Value::Object(ref mut map) = info_json {
            for (key, value) in &kvs {
                // Skip keys that are already handled in the base structure
                if key != "name" && key != "code" && key != "invocation_id" && key != "data" {
                    map.insert(key.clone(), serde_json::Value::String(value.clone()));
                }
            }
        }
        let mut data = json!({ "log_version": 3, "version": env!("CARGO_PKG_VERSION")});
        if let Some(data_str) = kvs.get("data") {
            if let Ok(serde_json::Value::Object(ref data_obj)) =
                serde_json::from_str::<serde_json::Value>(data_str)
            {
                if let serde_json::Value::Object(ref mut map) = data {
                    for (key, value) in data_obj {
                        map.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        json!({ "info": info_json, "data": data}).to_string()
    }
}

impl log::Log for Logger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        let current_level = metadata.level();
        // Check if the level is within the configured level filter
        if current_level > self.config.level_filter {
            return false;
        }
        // Counter intuitively, the lower the level, the more verbose the logging
        if let Some(min_level) = self.config.min_level {
            if current_level < min_level {
                return false;
            }
        }
        if let Some(max_level) = self.config.max_level {
            if current_level > max_level {
                return false;
            }
        }
        // Reject if not in includes (when set)
        if let Some(ref includes) = self.config.includes {
            if !includes.contains(&metadata.target().to_string()) {
                return false;
            }
        }
        // Reject if in excludes (when set)
        if let Some(ref excludes) = self.config.excludes {
            if excludes.contains(&metadata.target().to_string()) {
                return false;
            }
        }
        true
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) && !super::term::is_term_control_only(record) {
            match self.config.format {
                LogFormat::Text | LogFormat::Fancy => {
                    let mut text = record.args().to_string();
                    if self.remove_ansi_codes {
                        text = remove_ansi_codes(&text);
                    }
                    locked_writeln!(self, "{}", text);
                }
                LogFormat::Json => {
                    let json = Self::format_json(
                        record,
                        &self.invocation_id.to_string(),
                        self.remove_ansi_codes,
                    );
                    locked_writeln!(self, "{}", json);
                }
            }
        }
    }

    fn flush(&self) {
        match self.target {
            LogTarget::Stdout => {
                let _ = std::io::stdout().flush();
            }
            LogTarget::Stderr => {
                let _ = std::io::stderr().flush();
            }
            LogTarget::Writer(ref path) => {
                if let Ok(mut writer) = path.lock() {
                    let _ = writer.flush();
                }
            }
        }
    }
}

// Main logger that manages multiple loggers
struct MultiLogger {
    loggers: Vec<Box<dyn log::Log>>,
}

impl log::Log for MultiLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        self.loggers.iter().any(|logger| logger.enabled(metadata))
    }

    fn log(&self, record: &Record) {
        for logger in &self.loggers {
            logger.log(record);
        }
    }

    fn flush(&self) {
        for logger in &self.loggers {
            logger.flush();
        }
    }
}

// Builder pattern for configuring loggers
#[derive(Default)]
struct MultiLoggerBuilder {
    loggers: Vec<Box<dyn log::Log>>,
    invocation_id: uuid::Uuid,
}

impl MultiLoggerBuilder {
    pub fn new(invocation_id: uuid::Uuid) -> Self {
        Self {
            loggers: Vec::new(),
            invocation_id,
        }
    }

    fn make_stdout_logger(&self, log_config: &FsLogConfig) -> Box<dyn log::Log> {
        let config = LoggerConfig {
            level_filter: log_config.log_level,
            format: log_config.log_format,
            min_level: Some(LevelFilter::Info),
            max_level: None,
            includes: None,
            excludes: None,
        };

        let logger = Logger::new("stdout", LogTarget::Stdout, config, self.invocation_id);

        Box::new(logger)
    }

    fn make_stderr_logger(&self, log_config: &FsLogConfig) -> Box<dyn log::Log> {
        let config = LoggerConfig {
            level_filter: log_config.log_level,
            format: log_config.log_format,
            min_level: None,
            max_level: Some(LevelFilter::Warn),
            includes: None,
            excludes: Some(vec![EXECUTING.to_string()]),
        };

        let logger = Logger::new("stderr", LogTarget::Stderr, config, self.invocation_id);

        Box::new(logger)
    }

    pub fn add_terminal_loggers(mut self, log_config: &FsLogConfig) -> Self {
        let stdout_logger = self.make_stdout_logger(log_config);
        let stderr_logger = self.make_stderr_logger(log_config);

        if log_config.log_format == LogFormat::Fancy {
            let mut fancy_logger =
                super::term::FancyLogger::new(vec![stdout_logger, stderr_logger]);
            fancy_logger.start_ticker();
            self.loggers.push(Box::new(fancy_logger));
        } else {
            // For text and json formats, we use the regular loggers
            self.loggers.push(stdout_logger);
            self.loggers.push(stderr_logger);
        }
        self
    }

    fn add_logger(
        mut self,
        name: impl Into<String>,
        writer: Arc<Mutex<Box<dyn Write + Send>>>,
        config: LoggerConfig,
    ) -> Self {
        self.loggers.push(Box::new(Logger::new(
            name,
            LogTarget::Writer(writer),
            config,
            self.invocation_id,
        )));
        self
    }

    fn build(self) -> MultiLogger {
        MultiLogger {
            loggers: self.loggers,
        }
    }
}

pub struct FsLogConfig {
    pub log_format: LogFormat,
    pub log_level: LevelFilter,
    pub file_log_path: PathBuf,
    pub file_log_level: LevelFilter,
    pub file_log_format: LogFormat,
    pub invocation_id: uuid::Uuid,
}

impl From<IoArgs> for FsLogConfig {
    fn from(args: IoArgs) -> Self {
        Self {
            log_format: args.log_format, // TODO support different log format for different loggers
            log_level: args.log_level.unwrap_or(LevelFilter::Info), // default log level
            file_log_path: args.log_path.map(|p| p.join("dbt.log")).unwrap_or_else(|| {
                if args.out_dir.starts_with(&args.in_dir) {
                    args.in_dir.join("logs/dbt.log")
                } else {
                    // This is because when we do test we do not want to modify in_dir
                    args.out_dir.join("dbt.log")
                }
            }),
            file_log_level: args.log_level.unwrap_or(LevelFilter::Info), // default file log level
            file_log_format: args.log_format,
            invocation_id: args.invocation_id,
        }
    }
}

impl std::fmt::Debug for FsLogConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FsLogConfig {{ log_format: {:?}, log_level: {:?}, file_log_path: {:?}, file_log_level: {:?}, file_log_format: {:?} }}",
            self.log_format, self.log_level, self.file_log_path, self.file_log_level, self.file_log_format
        )
    }
}

impl Default for FsLogConfig {
    fn default() -> Self {
        Self {
            log_format: LogFormat::Text,
            log_level: LevelFilter::Info,
            file_log_path: PathBuf::from("dbt.log"),
            file_log_level: LevelFilter::Info,
            file_log_format: LogFormat::Text,
            invocation_id: uuid::Uuid::new_v4(),
        }
    }
}

pub fn init_logger(log_config: FsLogConfig) -> FsResult<()> {
    static LOGGER: std::sync::OnceLock<Box<MultiLogger>> = std::sync::OnceLock::new();

    if LOGGER.get().is_some() {
        // We should probably error here, but it breaks the tests for some
        // reason
        return Ok(());
    }

    // Build the multi-logger
    let mut builder = MultiLoggerBuilder::new(log_config.invocation_id);

    builder = builder.add_terminal_loggers(&log_config);

    // Add file logger
    let file_config = LoggerConfig {
        level_filter: log_config.file_log_level,
        format: log_config.file_log_format,
        min_level: None,
        max_level: None,
        includes: None,
        excludes: None,
    };
    // Create parent directories if they don't exist
    if let Some(parent) = log_config.file_log_path.parent() {
        std::fs::create_dir_all(parent)
            .unwrap_or_else(|_| panic!("Failed to create log directory {parent:?}"));
    }
    let file = Arc::new(Mutex::new(Box::new(
        std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&log_config.file_log_path)
            .unwrap_or_else(|_| panic!("Failed to open log file {:?}", &log_config.file_log_path)),
    ) as Box<dyn Write + Send>));
    builder = builder.add_logger("file", file, file_config);

    // Add logger of sql queries executed through adapters
    let query_file_config = LoggerConfig {
        level_filter: LevelFilter::Debug,
        format: LogFormat::Text,
        min_level: None,
        max_level: None,
        includes: Some(vec![EXECUTING.to_string()]),
        excludes: None,
    };
    let query_log_path = log_config
        .file_log_path
        .parent()
        .unwrap_or_else(|| {
            panic!(
                "Failed to obtain parent from {:?}",
                log_config.file_log_path
            )
        })
        .join(QUERY_LOG_SQL);
    let file = Arc::new(Mutex::new(Box::new(
        std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&query_log_path)
            .unwrap_or_else(|_| panic!("Failed to open log file {query_log_path:?}")),
    ) as Box<dyn Write + Send>));
    builder = builder.add_logger("queries", file, query_file_config);

    // Build the logger
    let logger = builder.build();
    // Register the logger globally
    LOGGER
        .set(Box::new(logger))
        .map_err(|_| unexpected_fs_err!("Failed to set global logger"))?;

    // We have to raise the global max level here because we have downstream
    // systems depending on DEBUG level logs.
    // TODO: move all mission critical logs to INFO level and above
    log::set_max_level(LevelFilter::Trace);

    // Update the global logger
    log::set_logger(LOGGER.get().expect("Was just set"))
        .map_err(|e| unexpected_fs_err!("Failed to set global logger: {}", e))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use log::{Level, MetadataBuilder};

    use super::*;

    fn writer() -> Arc<Mutex<Box<dyn Write + Send>>> {
        let buffer: Vec<u8> = Vec::new();
        Arc::new(Mutex::new(Box::new(buffer)))
    }

    #[test]
    fn test_logger_target_not_in_includes() {
        let config = LoggerConfig {
            level_filter: LevelFilter::Info,
            format: LogFormat::Text,
            min_level: None,
            max_level: None,
            includes: Some(vec![EXECUTING.to_string()]),
            excludes: None,
        };

        let logger = Logger::new(
            "name",
            LogTarget::Writer(writer()),
            config,
            uuid::Uuid::new_v4(),
        );
        let metadata = MetadataBuilder::new().level(Level::Info).build();
        assert!(!logger.enabled(&metadata));
    }

    #[test]
    fn test_logger_target_in_includes() {
        let config = LoggerConfig {
            level_filter: LevelFilter::Info,
            format: LogFormat::Text,
            min_level: None,
            max_level: None,
            includes: Some(vec![EXECUTING.to_string()]),
            excludes: None,
        };

        let logger = Logger::new(
            "name",
            LogTarget::Writer(writer()),
            config,
            uuid::Uuid::new_v4(),
        );
        let metadata: Metadata<'_> = MetadataBuilder::new()
            .level(Level::Info)
            .target(EXECUTING)
            .build();
        assert!(logger.enabled(&metadata));
    }

    #[test]
    fn test_logger_exclude_non_dbt_or_fs_or_sdf_target() {
        let config = LoggerConfig {
            level_filter: LevelFilter::Info,
            format: LogFormat::Text,
            min_level: None,
            max_level: None,
            includes: None,
            excludes: None,
        };

        let logger = Logger::new(
            "name",
            LogTarget::Writer(writer()),
            config,
            uuid::Uuid::new_v4(),
        );
        let metadata = MetadataBuilder::new()
            .level(Level::Info)
            .target("datafusion_something")
            .build();
        assert!(!logger.enabled(&metadata));
    }

    #[test]
    fn test_logger_target_in_excludes() {
        let config = LoggerConfig {
            level_filter: LevelFilter::Info,
            format: LogFormat::Text,
            min_level: None,
            max_level: None,
            includes: None,
            excludes: Some(vec![EXECUTING.to_string()]),
        };

        let logger = Logger::new(
            "name",
            LogTarget::Writer(writer()),
            config,
            uuid::Uuid::new_v4(),
        );
        let metadata = MetadataBuilder::new()
            .level(Level::Info)
            .target(EXECUTING)
            .build();
        assert!(!logger.enabled(&metadata));
    }
}
