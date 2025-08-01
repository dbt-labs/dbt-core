use std::time::{SystemTime, UNIX_EPOCH};

use dbt_telemetry::SeverityNumber;

pub fn tracing_level_to_severity(level: &tracing::Level) -> (SeverityNumber, Option<String>) {
    match *level {
        tracing::Level::ERROR => (SeverityNumber::Error, Some("ERROR".to_string())),
        tracing::Level::WARN => (SeverityNumber::Warn, Some("WARN".to_string())),
        tracing::Level::INFO => (SeverityNumber::Info, Some("INFO".to_string())),
        tracing::Level::DEBUG => (SeverityNumber::Debug, Some("DEBUG".to_string())),
        tracing::Level::TRACE => (SeverityNumber::Trace, Some("TRACE".to_string())),
    }
}

pub fn log_level_filter_to_tracing(
    level_filter: &log::LevelFilter,
) -> tracing::level_filters::LevelFilter {
    match *level_filter {
        log::LevelFilter::Off => tracing::level_filters::LevelFilter::OFF,
        log::LevelFilter::Error => tracing::level_filters::LevelFilter::ERROR,
        log::LevelFilter::Warn => tracing::level_filters::LevelFilter::WARN,
        log::LevelFilter::Info => tracing::level_filters::LevelFilter::INFO,
        log::LevelFilter::Debug => tracing::level_filters::LevelFilter::DEBUG,
        log::LevelFilter::Trace => tracing::level_filters::LevelFilter::TRACE,
    }
}

pub fn log_level_to_severity(level: &log::Level) -> (SeverityNumber, Option<String>) {
    match *level {
        log::Level::Error => (SeverityNumber::Error, Some("ERROR".to_string())),
        log::Level::Warn => (SeverityNumber::Warn, Some("WARN".to_string())),
        log::Level::Info => (SeverityNumber::Info, Some("INFO".to_string())),
        log::Level::Debug => (SeverityNumber::Debug, Some("DEBUG".to_string())),
        log::Level::Trace => (SeverityNumber::Trace, Some("TRACE".to_string())),
    }
}

pub fn current_time_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}
