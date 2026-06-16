//! Dbt-specific convenience helpers built on top of the generic tracing emit API.

use std::{ffi::OsStr, sync::Arc};

use dbt_error::{ErrorCode, FsError, fs_err};
use dbt_telemetry::{LogMessage, ProgressMessage};

use crate::{io_args::IoArgs, io_utils::StatusReporter};

use dbt_tracing::emit::{
    emit_debug_event, emit_error_event, emit_info_event, emit_trace_event, emit_warn_event,
};

#[derive(Default)]
struct LogMessageLocationFields {
    relative_path: Option<String>,
    line: Option<u32>,
    column: Option<u32>,
    expanded_relative_path: Option<String>,
    expanded_line: Option<u32>,
    expanded_column: Option<u32>,
}

fn log_message_location_fields(location: &crate::CodeLocationWithFile) -> LogMessageLocationFields {
    let expanded = location.expanded();

    LogMessageLocationFields {
        relative_path: Some(location.relative_path().to_string_lossy().to_string()),
        line: location.line_opt(),
        column: location.col_opt(),
        expanded_relative_path: expanded
            .map(|loc| loc.relative_path().to_string_lossy().to_string()),
        expanded_line: expanded.and_then(|loc| loc.line_opt()),
        expanded_column: expanded.and_then(|loc| loc.col_opt()),
    }
}

// Convenience shorthand's for common telemetry attributes

/// Emit a plain log message without error code at INFO level.
#[track_caller]
pub fn emit_info_log_message(message: impl AsRef<str>) {
    emit_info_event(
        LogMessage::new_from_level(tracing::Level::INFO),
        Some(message.as_ref()),
    )
}

/// Emit a plain log message without error code at DEBUG level.
#[track_caller]
pub fn emit_debug_log_message(message: impl AsRef<str>) {
    emit_debug_event(
        LogMessage::new_from_level(tracing::Level::DEBUG),
        Some(message.as_ref()),
    )
}

/// Emit a plain log message without error code at TRACE level.
///
/// NOTE: Trace level events are intended for fusion developer debugging and
/// turned off by default.
#[track_caller]
pub fn emit_trace_log_message(message: impl FnOnce() -> String) {
    emit_trace_event(|| {
        (
            LogMessage::new_from_level(tracing::Level::TRACE).into(),
            Some(message()),
        )
    })
}

#[track_caller]
fn emit_fs_error_log_message(
    error: &FsError,
    level: tracing::Level,
    status_reporter: Option<&Arc<dyn StatusReporter + 'static>>,
) {
    if let Some(status_reporter) = status_reporter {
        if matches!(level, tracing::Level::ERROR) && is_markdown_file(error.location.as_ref()) {
            // CLI log severity is downgraded via middleware, but the status reporter still needs
            // the same downgrade for LSP diagnostics.
            status_reporter.collect_warning(error);
        } else {
            match level {
                tracing::Level::WARN => status_reporter.collect_warning(error),
                _ => status_reporter.collect_error(error),
            };
        }
    };

    let mut log_message =
        LogMessage::new_from_level_and_code(error.code as u32, error.code.name(), level);
    if let Some(location) = error.location.as_ref() {
        let fields = log_message_location_fields(location);
        log_message.relative_path = fields.relative_path;
        log_message.code_line = fields.line;
        log_message.code_column = fields.column;
        log_message.expanded_relative_path = fields.expanded_relative_path;
        log_message.expanded_line = fields.expanded_line;
        log_message.expanded_column = fields.expanded_column;
    }

    match level {
        tracing::Level::WARN => emit_warn_event(log_message, Some(error.message().as_str())),
        _ => emit_error_event(log_message, Some(error.message().as_str())),
    }
}

fn is_markdown_file(location: Option<&crate::CodeLocationWithFile>) -> bool {
    location
        .and_then(|loc| loc.file.extension())
        .and_then(OsStr::to_str)
        .map(|ext| ext.eq_ignore_ascii_case("md"))
        .unwrap_or(false)
}

/// Emit a log message event at ERROR level with the given code and message.
///
/// This will also report the error to the provided status reporter, if any.
#[track_caller]
pub fn emit_error_log_message(
    code: ErrorCode,
    message: impl AsRef<str>,
    status_reporter: Option<&Arc<dyn StatusReporter + 'static>>,
) {
    if let Some(status_reporter) = status_reporter {
        status_reporter.collect_error(&fs_err!(code, "{}", message.as_ref()));
    };

    emit_error_event(
        LogMessage::new_from_level_and_code(code as u32, code.name(), tracing::Level::ERROR),
        Some(message.as_ref()),
    );
}

/// Emit a package-scoped (coming from a dependency) error log message.
#[track_caller]
pub fn emit_error_log_message_package_scoped(
    code: ErrorCode,
    message: impl AsRef<str>,
    package_name: &str,
    status_reporter: Option<&Arc<dyn StatusReporter + 'static>>,
) {
    if let Some(status_reporter) = status_reporter {
        status_reporter.collect_error(&fs_err!(code, "{}", message.as_ref()));
    };

    let mut log_message =
        LogMessage::new_from_level_and_code(code as u32, code.name(), tracing::Level::ERROR);
    log_message.package_name = Some(package_name.to_string());
    emit_error_event(log_message, Some(message.as_ref()));
}

/// Emit a log message event at ERROR level based on the given FsError.
///
/// This will also report the error to the provided status reporter, if any.
#[track_caller]
pub fn emit_error_log_from_fs_error(
    error: &FsError,
    status_reporter: Option<&Arc<dyn StatusReporter + 'static>>,
) {
    emit_fs_error_log_message(error, tracing::Level::ERROR, status_reporter);
}

/// Emit a log message event at WARN level with the given code and message.
///
/// This will also report the warning to the provided status reporter, if any.
#[track_caller]
pub fn emit_warn_log_message(
    code: ErrorCode,
    message: impl AsRef<str>,
    status_reporter: Option<&Arc<dyn StatusReporter + 'static>>,
) {
    if let Some(status_reporter) = status_reporter {
        status_reporter.collect_warning(&fs_err!(code, "{}", message.as_ref()));
    };

    emit_warn_event(
        LogMessage::new_from_level_and_code(code as u32, code.name(), tracing::Level::WARN),
        Some(message.as_ref()),
    );
}

/// Emit a package-scoped (coming from a dependency) warning log message.
#[track_caller]
pub fn emit_warn_log_message_package_scoped(
    code: ErrorCode,
    message: impl AsRef<str>,
    package_name: &str,
    status_reporter: Option<&Arc<dyn StatusReporter + 'static>>,
) {
    if let Some(status_reporter) = status_reporter {
        status_reporter.collect_warning(&fs_err!(code, "{}", message.as_ref()));
    };

    let mut log_message =
        LogMessage::new_from_level_and_code(code as u32, code.name(), tracing::Level::WARN);
    log_message.package_name = Some(package_name.to_string());
    emit_warn_event(log_message, Some(message.as_ref()));
}

/// Emit a log message event at WARN level based on the given FsError.
///
/// This will also report the warning to the provided status reporter, if any.
#[track_caller]
pub fn emit_warn_log_from_fs_error(
    warning: &FsError,
    status_reporter: Option<&Arc<dyn StatusReporter + 'static>>,
) {
    emit_fs_error_log_message(warning, tracing::Level::WARN, status_reporter);
}

/// Emit a log message related to parsing error based on the given FsError.
///
/// This will also report the error/warning to the provided status reporter, if any.
///
/// TODO: This should be removed when `ParsingErrorMessage` is no longer needed,
/// see `parse_error_filter` middleware docs why it is used currently.
#[track_caller]
pub fn emit_strict_parse_error(
    error: &FsError,
    package_name: Option<impl AsRef<str>>,
    io: &IoArgs, // TODO: remove when lsp will switch to tracing layer instead of status_reporter
) {
    use super::middlewares::parse_error_filter::ParsingErrorMessage;

    let mut log_message = LogMessage::new_from_level_and_code(
        error.code as u32,
        error.code.name(),
        tracing::Level::ERROR,
    );
    log_message.package_name = package_name.as_ref().map(|s| s.as_ref().to_string());
    if let Some(location) = error.location.as_ref() {
        let fields = log_message_location_fields(location);
        log_message.relative_path = fields.relative_path;
        log_message.code_line = fields.line;
        log_message.code_column = fields.column;
        log_message.expanded_relative_path = fields.expanded_relative_path;
        log_message.expanded_line = fields.expanded_line;
        log_message.expanded_column = fields.expanded_column;
    }
    emit_error_event(
        ParsingErrorMessage::new(log_message),
        Some(error.message().as_str()),
    );

    // Unfortunately, the logic for downgrading parsing errors to warnings, as well as filtering
    // repeated package compatibility diagnostics is fully replicated here.
    //
    // This is a consequence of LSP (status_reporter) not being a tracing layer and
    // thus not being able to leverage the existing `parse_error_filter` middleware.
    //
    // TODO: It is ugly, and inefficient, but as of time of writing the agreement was to keep
    // the existing architecture. This should be revisited in the future.
    use crate::collections::HashSet;
    use crate::dashmap::DashMap;
    use once_cell::sync::Lazy;

    static PACKAGE_WITH_ERRORS_OR_WARNING: Lazy<DashMap<String, HashSet<String>>> =
        Lazy::new(DashMap::default);

    /// Marks a package with an error or warning for the given key.
    fn mark_package_with_error_or_warning(key: &str, package_name: &str) {
        let mut package_set = PACKAGE_WITH_ERRORS_OR_WARNING
            .entry(key.to_string())
            .or_default();
        package_set.insert(package_name.to_string());
    }

    /// Returns true if the given package has an error or warning for the given key (invocation id).
    fn has_package_with_error_or_warning(key: &str, package_name: &str) -> bool {
        PACKAGE_WITH_ERRORS_OR_WARNING
            .get(key)
            .map(|set| set.contains(package_name))
            .unwrap_or(false)
    }

    static BETA_PARSING: Lazy<bool> = Lazy::new(|| {
        match std::env::var("DBT_ENGINE_BETA_PARSING") {
            Ok(val) => val == "1",
            Err(_) => false, // default to false (strict mode on)
        }
    });
    static BETA_PACKAGE_PARSING: Lazy<bool> = Lazy::new(|| {
        match std::env::var("DBT_ENGINE_BETA_PACKAGE_PARSING") {
            Ok(val) => val == "1",
            Err(_) => true, // default to true (strict mode off for packages)
        }
    });

    let Some(status_reporter) = io.status_reporter.as_ref() else {
        // No status reporter, nothing more to do
        return;
    };

    let downgrade_to_warn = if let Some(package_name) = package_name.as_ref() {
        // If we are filtering repeated compatibility diagnostics from packages, check if this
        // package has already emitted one.
        if !io.show_all_deprecations {
            let invocation_id = io.invocation_id.to_string();

            if has_package_with_error_or_warning(invocation_id.as_str(), package_name.as_ref()) {
                // We've seen this package compatibility diagnostic before, return
                return;
            }

            // Mark the package with an error or warning
            mark_package_with_error_or_warning(invocation_id.as_str(), package_name.as_ref());

            // Create a new FsError instead of the original one
            let err = fs_err!(
                ErrorCode::PackageParsingCompatibility,
                "Package `{}` issued one or more compatibility warnings. To display all warnings associated with this package, run with `--show-all-deprecations`.",
                package_name.as_ref()
            );

            if *BETA_PARSING || *BETA_PACKAGE_PARSING {
                status_reporter.collect_warning(&err);
            } else {
                status_reporter.collect_error(&err);
            }

            return;
        }

        // for package-related logs, two env vars control downgrading
        *BETA_PARSING || *BETA_PACKAGE_PARSING
    } else {
        // for local logs, only the main env var controls downgrading
        *BETA_PARSING
    };

    if downgrade_to_warn {
        status_reporter.collect_warning(error);
    } else {
        status_reporter.collect_error(error);
    }
}

// Progress messages
/// Emit a regular progress message at INFO level.
#[track_caller]
pub fn emit_info_progress_message(
    message: ProgressMessage,
    status_reporter: Option<&Arc<dyn StatusReporter + 'static>>,
) {
    if let Some(status_reporter) = status_reporter {
        status_reporter.show_progress(
            message.action.as_str(),
            message.target.as_str(),
            message.description.as_deref(),
        );
    };

    emit_info_event(message, None)
}

/// Print a message on a separate line to stdout only. This should be used instead of `println!`.
#[track_caller]
pub fn println(message: impl AsRef<str>) {
    use super::private_events::print_event::StdoutMessage;

    emit_info_event(
        StdoutMessage,
        Some(format!("{}\n", message.as_ref()).as_str()),
    );
}

/// Print a message to stdout only. This should be used instead of `print!`.
#[track_caller]
pub fn print(message: impl AsRef<str>) {
    use super::private_events::print_event::StdoutMessage;

    emit_info_event(StdoutMessage, Some(message.as_ref()));
}

/// Print an error to stderr only. This should be used instead of `eprintln!`.
///
/// Takes a mandatory error code. The message will be formatted similarly
/// to how error logs are formatted: `[error] [Name (dbt####)]: <message>`,
/// error colored in red.
#[track_caller]
pub fn print_err(error_code: ErrorCode, message: impl AsRef<str>) {
    use super::private_events::print_event::StderrMessage;

    emit_error_event(StderrMessage::new(Some(error_code)), Some(message.as_ref()));
}

/// Print an error to stderr only. This should be used instead of `eprintln!`.
#[track_caller]
pub fn print_err_from_fs_error(error: &FsError) {
    print_err(error.code, error.message().as_str());
}
