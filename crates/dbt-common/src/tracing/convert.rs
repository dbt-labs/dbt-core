use dbt_telemetry::SeverityNumber;

pub fn tracing_level_to_severity(level: &tracing::Level) -> (SeverityNumber, &'static str) {
    match *level {
        tracing::Level::ERROR => (SeverityNumber::Error, "ERROR"),
        tracing::Level::WARN => (SeverityNumber::Warn, "WARN"),
        tracing::Level::INFO => (SeverityNumber::Info, "INFO"),
        tracing::Level::DEBUG => (SeverityNumber::Debug, "DEBUG"),
        tracing::Level::TRACE => (SeverityNumber::Trace, "TRACE"),
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

pub fn log_level_to_severity(level: &log::Level) -> (SeverityNumber, &'static str) {
    match *level {
        log::Level::Error => (SeverityNumber::Error, "ERROR"),
        log::Level::Warn => (SeverityNumber::Warn, "WARN"),
        log::Level::Info => (SeverityNumber::Info, "INFO"),
        log::Level::Debug => (SeverityNumber::Debug, "DEBUG"),
        log::Level::Trace => (SeverityNumber::Trace, "TRACE"),
    }
}
