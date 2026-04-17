use dbt_telemetry::SeverityNumber;
use tracing::Level;

use crate::io_args::LogLevel;

pub fn log_level_filter_to_tracing(level_filter: &LogLevel) -> tracing::level_filters::LevelFilter {
    match *level_filter {
        LogLevel::Off => tracing::level_filters::LevelFilter::OFF,
        LogLevel::Error => tracing::level_filters::LevelFilter::ERROR,
        LogLevel::Warn => tracing::level_filters::LevelFilter::WARN,
        LogLevel::Info => tracing::level_filters::LevelFilter::INFO,
        LogLevel::Debug => tracing::level_filters::LevelFilter::DEBUG,
        LogLevel::Trace => tracing::level_filters::LevelFilter::TRACE,
    }
}

pub fn log_level_to_severity(level: &Level) -> (SeverityNumber, &'static str) {
    match *level {
        Level::ERROR => (SeverityNumber::Error, "ERROR"),
        Level::WARN => (SeverityNumber::Warn, "WARN"),
        Level::INFO => (SeverityNumber::Info, "INFO"),
        Level::DEBUG => (SeverityNumber::Debug, "DEBUG"),
        Level::TRACE => (SeverityNumber::Trace, "TRACE"),
    }
}
