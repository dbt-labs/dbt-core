//! A module for emitting structured events.
//!
//! This module provides API's used for all span/event creation based on
//! our tracing infrastructre. They wrap `tracing::event!`/`tracing::span!` macros
//! and add functionality to capture file/line information of the callsite
//! (rather than the macro invocation site) and also efficiently pass telemetry attributes
//! into the tracing pipeline via thread-local storage.

use std::panic::Location;

use dbt_error::{ErrorCode, FsError, fs_err};
use dbt_telemetry::{LogMessage, TelemetryAttributes, TelemetryEventRecType};

use crate::io_args::IoArgs;

use super::{constants::ROOT_SPAN_NAME, event_info::store_event_attributes, shared::Recordable};

use tracing;

// Tracing's library built-in file/line detection is not based on panic module, and
// thus will always report the actual location of the macro call where it was invoke.
// We on the other hand, would like to use function, rather than macros to emit events
// and create spans to aide lsp/IDE's (and thus simplify debugging, refactoring etc.)
// To do that, we use functuns with `#[track_caller]` attribute that allow capturing
// file/line position of the callsite and then inject them as custom fields into
// tracing. Our data layer extracts these and prefers them over native location info
// privided by tracing, while still being compatible with direct tracing calls.
const FILE_FIELD: &str = "__file";
const LINE_FIELD: &str = "__line";

/// Helper that extracts file & line from fields if available
pub(super) fn get_file_and_line(values: Recordable<'_>) -> Option<(String, u32)> {
    struct SpanEventAttributesVisitor {
        file: Option<String>,
        line: Option<u32>,
    }

    impl tracing::field::Visit for SpanEventAttributesVisitor {
        fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            if field.name() == FILE_FIELD {
                self.file = Some(value.to_string());
            }
        }

        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            if field.name() == LINE_FIELD {
                self.line = Some(value as u32);
            }
        }
    }

    let mut visitor = SpanEventAttributesVisitor {
        file: None,
        line: None,
    };

    values.record(&mut visitor);

    visitor.file.map(|f| (f, visitor.line.unwrap_or(0)))
}

// The following repetetive functions have to be separate, as tracing requires
// a constant level for its macros and thus we cannot pass level as a parameter.
// They are also intentionally spelled out rather than using a macro, to ease
// debugging and IDE support.

/// Emit an error level event with the provided attributes and optional message.
#[track_caller]
pub fn emit_error_event(attrs: impl Into<TelemetryAttributes>, message: Option<&str>) {
    let attrs: TelemetryAttributes = attrs.into();

    debug_assert_eq!(
        attrs.record_category(),
        TelemetryEventRecType::Log,
        "Do not emit events of span type as logs!"
    );

    // Get the real code location
    let loc = Location::caller();

    // Save attributes to thread-local storage for the data layer to pick up
    store_event_attributes(attrs);

    // Emit event into tracing pipeline
    tracing::event!(
        tracing::Level::ERROR,
        message,
        { FILE_FIELD } = loc.file(),
        { LINE_FIELD } = loc.line()
    );
}

/// Emit a warning level event with the provided attributes and optional message.
#[track_caller]
pub fn emit_warn_event(attrs: impl Into<TelemetryAttributes>, message: Option<&str>) {
    let attrs: TelemetryAttributes = attrs.into();

    debug_assert_eq!(
        attrs.record_category(),
        TelemetryEventRecType::Log,
        "Do not emit events of span type as logs!"
    );

    // Get the real code location
    let loc = Location::caller();

    // Save attributes to thread-local storage for the data layer to pick up
    store_event_attributes(attrs);

    // Emit event into tracing pipeline
    tracing::event!(
        tracing::Level::WARN,
        message,
        { FILE_FIELD } = loc.file(),
        { LINE_FIELD } = loc.line()
    );
}

/// Emit an info level event with the provided attributes and optional message.
#[track_caller]
pub fn emit_info_event(attrs: impl Into<TelemetryAttributes>, message: Option<&str>) {
    let attrs: TelemetryAttributes = attrs.into();

    debug_assert_eq!(
        attrs.record_category(),
        TelemetryEventRecType::Log,
        "Do not emit events of span type as logs!"
    );

    // Get the real code location
    let loc = Location::caller();

    // Save attributes to thread-local storage for the data layer to pick up
    store_event_attributes(attrs);

    // Emit event into tracing pipeline
    tracing::event!(
        tracing::Level::INFO,
        message,
        { FILE_FIELD } = loc.file(),
        { LINE_FIELD } = loc.line()
    );
}

/// Emit a debug level event with the provided attributes and optional message.
#[track_caller]
pub fn emit_debug_event(attrs: impl Into<TelemetryAttributes>, message: Option<&str>) {
    let attrs: TelemetryAttributes = attrs.into();

    debug_assert_eq!(
        attrs.record_category(),
        TelemetryEventRecType::Log,
        "Do not emit events of span type as logs!"
    );

    // Get the real code location
    let loc = Location::caller();

    // Save attributes to thread-local storage for the data layer to pick up
    store_event_attributes(attrs);

    // Emit event into tracing pipeline
    tracing::event!(
        tracing::Level::DEBUG,
        message,
        { FILE_FIELD } = loc.file(),
        { LINE_FIELD } = loc.line()
    );
}

/// Emit a trace level event with the provided attributes and message.
///
/// NOTE: Trace level events are intended for fusion developer debugging and
/// turned off by default in production optional builds.
#[track_caller]
pub fn emit_trace_event(attrs: impl Into<TelemetryAttributes>, message: Option<&str>) {
    let attrs: TelemetryAttributes = attrs.into();

    debug_assert_eq!(
        attrs.record_category(),
        TelemetryEventRecType::Log,
        "Do not emit events of span type as logs!"
    );

    // Get the real code location
    let loc = Location::caller();

    // Save attributes to thread-local storage for the data layer to pick up
    store_event_attributes(attrs);

    // Emit event into tracing pipeline
    tracing::event!(
        tracing::Level::TRACE,
        message,
        { FILE_FIELD } = loc.file(),
        { LINE_FIELD } = loc.line()
    );
}

/// Create a root info-level span with no parent.
///
/// This function creates a new tracing span at the info level that explicitly
/// has no parent span (root of a trace tree). It tracks the caller's location
/// and injects file/line information into the span for better debugging.
///
/// # Arguments
/// * `attrs` - Telemetry attributes for the span. In production this is expected
///   to be an `Invocation` type
#[track_caller]
pub fn create_root_info_span(attrs: impl Into<TelemetryAttributes>) -> tracing::Span {
    let attrs: TelemetryAttributes = attrs.into();

    // Get the real code location
    let loc = Location::caller();

    // Save attributes to thread-local storage for the data layer to pick up
    store_event_attributes(attrs);

    // Create the span
    tracing::info_span!(
        parent: None,
        // In our structured tracing we do not care about the span name,
        // everything comes from the attributes. However, we give the name here
        // for debug assertions in some API's that assume the correct root span is used.
        ROOT_SPAN_NAME,
        { FILE_FIELD } = loc.file(),
        { LINE_FIELD } = loc.line()
    )
}

/// Create an info-level span with the current active span as parent.
///
/// This function creates a new tracing span at the info level. If there is an
/// active span, it will automatically become the parent. It tracks the caller's
/// location and injects file/line information into the span for better debugging.
///
/// # Arguments
/// * `attrs` - Telemetry attributes for the span
#[track_caller]
pub fn create_info_span(attrs: impl Into<TelemetryAttributes>) -> tracing::Span {
    let attrs: TelemetryAttributes = attrs.into();

    // Get the real code location
    let loc = Location::caller();

    // Save attributes to thread-local storage for the data layer to pick up
    store_event_attributes(attrs);

    // Create the span
    tracing::info_span!(
        // In our structured tracing we do not care about the span name,
        // everything comes from the attributes.
        "",
        { FILE_FIELD } = loc.file(),
        { LINE_FIELD } = loc.line()
    )
}

/// Create an info-level span with an explicit parent span.
///
/// This function creates a new tracing span at the info level with an explicitly
/// specified parent span (or None for no parent). It tracks the caller's location
/// and injects file/line information into the span for better debugging.
///
/// # Arguments
/// * `parent` - Optional parent span ID (obtain via `span.id()`)
/// * `attrs` - Telemetry attributes for the span
#[track_caller]
pub fn create_info_span_with_parent(
    parent: Option<tracing::span::Id>,
    attrs: impl Into<TelemetryAttributes>,
) -> tracing::Span {
    let attrs: TelemetryAttributes = attrs.into();

    // Get the real code location
    let loc = Location::caller();

    // Save attributes to thread-local storage for the data layer to pick up
    store_event_attributes(attrs);

    // Create the span
    tracing::info_span!(
        parent: parent,
        "",
        { FILE_FIELD } = loc.file(),
        { LINE_FIELD } = loc.line()
    )
}

/// Create a debug-level span with the current active span as parent.
///
/// This function creates a new tracing span at the debug level. If there is an
/// active span, it will automatically become the parent. It tracks the caller's
/// location and injects file/line information into the span for better debugging.
///
/// # Arguments
/// * `attrs` - Telemetry attributes for the span
#[track_caller]
pub fn create_debug_span(attrs: impl Into<TelemetryAttributes>) -> tracing::Span {
    let attrs: TelemetryAttributes = attrs.into();

    // Get the real code location
    let loc = Location::caller();

    // Save attributes to thread-local storage for the data layer to pick up
    store_event_attributes(attrs);

    // Create the span
    tracing::debug_span!(
        // In our structured tracing we do not care about the span name,
        // everything comes from the attributes.
        "",
        { FILE_FIELD } = loc.file(),
        { LINE_FIELD } = loc.line()
    )
}

/// Create a debug-level span with an explicit parent span.
///
/// This function creates a new tracing span at the debug level with an explicitly
/// specified parent span (or None for no parent). It tracks the caller's location
/// and injects file/line information into the span for better debugging.
///
/// # Arguments
/// * `parent` - Optional parent span ID (obtain via `span.id()`)
/// * `attrs` - Telemetry attributes for the span
#[track_caller]
pub fn create_debug_span_with_parent(
    parent: Option<tracing::span::Id>,
    attrs: impl Into<TelemetryAttributes>,
) -> tracing::Span {
    let attrs: TelemetryAttributes = attrs.into();

    // Get the real code location
    let loc = Location::caller();

    // Save attributes to thread-local storage for the data layer to pick up
    store_event_attributes(attrs);

    // Create the span
    tracing::debug_span!(
        parent: parent,
        "",
        { FILE_FIELD } = loc.file(),
        { LINE_FIELD } = loc.line()
    )
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

/// Emit a log message event at ERROR level with the given code and message.
#[track_caller]
pub fn emit_error_log_message(
    code: ErrorCode,
    message: impl AsRef<str>,
    io: &IoArgs, // TODO: remove when lsp will switch to tracing layer instead of status_reporter
) {
    emit_error_event(
        LogMessage::new_from_level_and_code(code as u32, tracing::Level::ERROR),
        Some(message.as_ref()),
    );

    // TODO: remove everything below this when lsp will switch to tracing layer instead of status_reporter
    let Some(status_reporter) = io.status_reporter.as_ref() else {
        // No status reporter, nothing more to do
        return;
    };

    status_reporter.collect_error(&fs_err!(code, "{}", message.as_ref()));
}

/// Emit a log message event at ERROR level based on the given FsError.
#[track_caller]
pub fn emit_error_log_from_fs_error(
    error: &FsError,
    io: &IoArgs, // TODO: remove when lsp will switch to tracing layer instead of status_reporter
) {
    emit_error_event(
        LogMessage::new_from_level_and_code(error.code as u32, tracing::Level::ERROR),
        Some(error.message().as_str()),
    );

    // TODO: remove everything below this when lsp will switch to tracing layer instead of status_reporter
    let Some(status_reporter) = io.status_reporter.as_ref() else {
        // No status reporter, nothing more to do
        return;
    };

    status_reporter.collect_error(error);
}

/// Emit a log message event at WARN level with the given code and message.
#[track_caller]
pub fn emit_warn_log_message(
    code: ErrorCode,
    message: impl AsRef<str>,
    io: &IoArgs, // TODO: remove when lsp will switch to tracing layer instead of status_reporter
) {
    emit_warn_event(
        LogMessage::new_from_level_and_code(code as u32, tracing::Level::WARN),
        Some(message.as_ref()),
    );

    // TODO: remove everything below this when lsp will switch to tracing layer instead of status_reporter
    let Some(status_reporter) = io.status_reporter.as_ref() else {
        // No status reporter, nothing more to do
        return;
    };

    status_reporter.collect_warning(&fs_err!(code, "{}", message.as_ref()));
}

/// Emit a log message event at WARN level based on the given FsError.
#[track_caller]
pub fn emit_warn_log_from_fs_error(
    warning: &FsError,
    io: &IoArgs, // TODO: remove when lsp will switch to tracing layer instead of status_reporter
) {
    emit_warn_event(
        LogMessage::new_from_level_and_code(warning.code as u32, tracing::Level::WARN),
        Some(warning.message().as_str()),
    );

    // TODO: remove everything below this when lsp will switch to tracing layer instead of status_reporter
    let Some(status_reporter) = io.status_reporter.as_ref() else {
        // No status reporter, nothing more to do
        return;
    };

    status_reporter.collect_warning(warning);
}

/// Emit a log message related to parsing error based on the given FsError.
/// TODO: This should be removed when `ParsingErrorMessage` is no longer needed,
/// see `parse_error_filter` middleware docs why it is used currently.
#[track_caller]
pub fn emit_strict_parse_error(
    error: &FsError,
    package_name: Option<impl AsRef<str>>,
    io: &IoArgs, // TODO: remove when lsp will switch to tracing layer instead of status_reporter
) {
    use super::middlewares::parse_error_filter::ParsingErrorMessage;

    let mut log_message =
        LogMessage::new_from_level_and_code(error.code as u32, tracing::Level::ERROR);
    log_message.package_name = package_name.as_ref().map(|s| s.as_ref().to_string());

    emit_error_event(
        ParsingErrorMessage::new(log_message),
        Some(error.message().as_str()),
    );

    // TODO: remove everything below this when lsp will switch to tracing layer instead of status_reporter
    use dashmap::DashMap;
    use once_cell::sync::Lazy;
    use std::collections::HashSet;

    static PACKAGE_WITH_ERRORS_OR_WARNING: Lazy<DashMap<String, HashSet<String>>> =
        Lazy::new(DashMap::new);

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
        // If we are filtering repeated deprecations from packages,
        // check if this is a deprecation message and if we've seen it before.
        if !io.show_all_deprecations {
            let invocation_id = io.invocation_id.to_string();

            if has_package_with_error_or_warning(invocation_id.as_str(), package_name.as_ref()) {
                // We've seen this deprecation message before, return
                return;
            }

            // Mark the package with an error or warning
            mark_package_with_error_or_warning(invocation_id.as_str(), package_name.as_ref());

            // Create a new FsError instead of the original one
            let err = fs_err!(
                ErrorCode::DependencyWarning,
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
