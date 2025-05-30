use crate::constants::EXECUTING;
use crate::io_args::IoArgs;
use crate::pretty_string::remove_ansi_codes;
use clap::ValueEnum;
use log::{LevelFilter, Metadata, Record, SetLoggerError};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::fmt::Display;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

const QUERY_LOG_SQL: &str = "query_log.sql";

// Logger configuration for individual loggers
#[derive(Clone)]
struct LoggerConfig {
    level_filter: LevelFilter,
    format: LogFormat,
    min_level: Option<LevelFilter>, // Minimum level to log (inclusive)
    max_level: Option<LevelFilter>, // Maximum level to log (inclusive)
    remove_ansi_codes: bool,
    // targets to include
    includes: Option<Vec<String>>,
    // targets to exclude
    excludes: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, ValueEnum, Serialize, Copy)]
pub enum LogFormat {
    Text,
    Json,
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
            remove_ansi_codes: false,
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
        self.kvs.insert(key.to_string(), value.to_string());
        Ok(())
    }
}

// Individual logger that can be customized
struct Logger {
    writer: Arc<Mutex<Box<dyn Write + Send>>>,
    config: LoggerConfig,
    name: String,
    invocation_id: uuid::Uuid,
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

impl Logger {
    fn new(
        name: impl Into<String>,
        writer: Arc<Mutex<Box<dyn Write + Send>>>,
        config: LoggerConfig,
        invocation_id: uuid::Uuid,
    ) -> Self {
        Self {
            writer,
            config,
            name: name.into(),
            invocation_id,
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

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            if let Ok(mut writer) = self.writer.lock() {
                match self.config.format {
                    LogFormat::Text => {
                        let mut text = record.args().to_string();
                        if self.config.remove_ansi_codes {
                            text = remove_ansi_codes(&text);
                        }
                        writeln!(writer, "{}", text).ok();
                    }
                    LogFormat::Json => {
                        let json = Self::format_json(
                            record,
                            &self.invocation_id.to_string(),
                            self.config.remove_ansi_codes,
                        );
                        writeln!(writer, "{}", json).ok();
                    }
                }
            }
        }
    }

    fn flush(&self) {
        if let Ok(mut writer) = self.writer.lock() {
            writer.flush().ok();
        }
    }
}

// Main logger that manages multiple loggers
struct MultiLogger {
    loggers: Vec<Logger>,
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
    loggers: Vec<Logger>,
    invocation_id: uuid::Uuid,
}

impl MultiLoggerBuilder {
    fn new(invocation_id: uuid::Uuid) -> Self {
        Self {
            loggers: Vec::new(),
            invocation_id,
        }
    }

    fn add_logger(
        mut self,
        name: impl Into<String>,
        writer: Arc<Mutex<Box<dyn Write + Send>>>,
        config: LoggerConfig,
    ) -> Self {
        self.loggers
            .push(Logger::new(name, writer, config, self.invocation_id));
        self
    }

    fn build(self) -> MultiLogger {
        MultiLogger {
            loggers: self.loggers,
        }
    }
}

pub struct FsLogConfig {
    pub stdout: Option<Arc<Mutex<Box<dyn Write + Send>>>>,
    pub stderr: Option<Arc<Mutex<Box<dyn Write + Send>>>>,
    pub log_format: LogFormat,
    pub log_level: LevelFilter,
    pub file_log_path: PathBuf,
    pub file_log_level: LevelFilter,
    pub file_log_format: LogFormat,
    pub remove_ansi_codes: bool,
    pub invocation_id: uuid::Uuid,
}

impl From<IoArgs> for FsLogConfig {
    fn from(args: IoArgs) -> Self {
        Self {
            stdout: args.stdout.clone(),
            stderr: args.stderr.clone(),
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
            remove_ansi_codes: (args.stdout.is_some() && args.stderr.is_some()),
            invocation_id: args.invocation_id,
        }
    }
}

impl std::fmt::Debug for FsLogConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FsLogConfig {{ stdout: {:?}, stderr: {:?}, log_format: {:?}, log_level: {:?}, file_log_path: {:?}, file_log_level: {:?}, file_log_format: {:?}, remove_ansi_codes: {:?} }}", self.stdout.is_some(), self.stderr.is_some(), self.log_format, self.log_level, self.file_log_path, self.file_log_level, self.file_log_format, self.remove_ansi_codes)
    }
}

impl Default for FsLogConfig {
    fn default() -> Self {
        Self {
            stdout: None,
            stderr: None,
            log_format: LogFormat::Text,
            log_level: LevelFilter::Info,
            file_log_path: PathBuf::from("dbt.log"),
            file_log_level: LevelFilter::Info,
            file_log_format: LogFormat::Text,
            remove_ansi_codes: false,
            invocation_id: uuid::Uuid::new_v4(),
        }
    }
}

// Add a new static for storing the current logger
static LOGGER: Mutex<Option<MultiLogger>> = Mutex::new(None);

pub fn init_logger(log_config: FsLogConfig) -> Result<(), SetLoggerError> {
    // Build the multi-logger
    let mut builder = MultiLoggerBuilder::new(log_config.invocation_id);

    // Add stdout logger
    let stdout_config = LoggerConfig {
        level_filter: log_config.log_level,
        format: log_config.log_format,
        min_level: Some(LevelFilter::Info),
        max_level: None,
        remove_ansi_codes: log_config.remove_ansi_codes,
        includes: None,
        excludes: None,
    };

    if let Some(stdout) = log_config.stdout {
        builder = builder.add_logger("stdout", stdout, stdout_config);
    } else {
        let stdout = Arc::new(Mutex::new(
            Box::new(std::io::stdout()) as Box<dyn Write + Send>
        ));
        builder = builder.add_logger("stdout", stdout, stdout_config);
    }

    // Add stderr logger
    let stderr_config = LoggerConfig {
        level_filter: log_config.log_level,
        format: log_config.log_format,
        min_level: None,
        max_level: Some(LevelFilter::Warn),
        remove_ansi_codes: log_config.remove_ansi_codes,
        includes: None,
        excludes: Some(vec![EXECUTING.to_string()]),
    };
    if let Some(stderr) = log_config.stderr {
        builder = builder.add_logger("stderr", stderr, stderr_config);
    } else {
        let stderr = Arc::new(Mutex::new(
            Box::new(std::io::stdout()) as Box<dyn Write + Send>
        ));
        builder = builder.add_logger("stderr", stderr, stderr_config);
    }

    // Add file logger
    let file_config = LoggerConfig {
        level_filter: log_config.file_log_level,
        format: log_config.file_log_format,
        min_level: None,
        max_level: None,
        remove_ansi_codes: true,
        includes: None,
        excludes: None,
    };
    // Create parent directories if they don't exist
    if let Some(parent) = log_config.file_log_path.parent() {
        std::fs::create_dir_all(parent)
            .unwrap_or_else(|_| panic!("Failed to create log directory {:?}", parent));
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
        remove_ansi_codes: true,
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
            .unwrap_or_else(|_| panic!("Failed to open log file {:?}", query_log_path)),
    ) as Box<dyn Write + Send>));
    builder = builder.add_logger("queries", file, query_file_config);

    // Build the logger
    let logger = builder.build();
    let max_level = logger
        .loggers
        .iter()
        .map(|l| l.config.level_filter)
        .max()
        .unwrap_or(LevelFilter::Error);
    log::set_max_level(max_level);

    // Update the global logger
    let mut global_logger = LOGGER.lock().unwrap();
    if global_logger.is_none() {
        // First initialization
        log::set_logger(Box::leak(Box::new(LoggerWrapper)))?;
    }
    *global_logger = Some(logger);

    Ok(())
}

// Add a wrapper struct that delegates to the current logger
struct LoggerWrapper;

impl log::Log for LoggerWrapper {
    fn enabled(&self, metadata: &Metadata) -> bool {
        if let Ok(logger) = LOGGER.lock() {
            if let Some(logger) = logger.as_ref() {
                return logger.enabled(metadata);
            }
        }
        false
    }

    fn log(&self, record: &Record) {
        if let Ok(logger) = LOGGER.lock() {
            if let Some(logger) = logger.as_ref() {
                logger.log(record);
            }
        }
    }

    fn flush(&self) {
        if let Ok(logger) = LOGGER.lock() {
            if let Some(logger) = logger.as_ref() {
                logger.flush();
            }
        }
    }
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
            remove_ansi_codes: true,
            includes: Some(vec![EXECUTING.to_string()]),
            excludes: None,
        };

        let logger = Logger::new("name", writer(), config, uuid::Uuid::new_v4());
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
            remove_ansi_codes: true,
            includes: Some(vec![EXECUTING.to_string()]),
            excludes: None,
        };

        let logger = Logger::new("name", writer(), config, uuid::Uuid::new_v4());
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
            remove_ansi_codes: true,
            includes: None,
            excludes: None,
        };

        let logger = Logger::new("name", writer(), config, uuid::Uuid::new_v4());
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
            remove_ansi_codes: true,
            includes: None,
            excludes: Some(vec![EXECUTING.to_string()]),
        };

        let logger = Logger::new("name", writer(), config, uuid::Uuid::new_v4());
        let metadata = MetadataBuilder::new()
            .level(Level::Info)
            .target(EXECUTING)
            .build();
        assert!(!logger.enabled(&metadata));
    }
}
