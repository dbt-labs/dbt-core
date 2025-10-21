use std::collections::HashSet;

use dbt_error::ErrorCode;
use dbt_telemetry::{AnyTelemetryEvent, LogMessage, LogRecordInfo, SeverityNumber};

use super::super::{
    data_provider::DataProvider,
    layer::TelemetryMiddleware,
    metrics::{InvocationMetricKey, MetricKey},
};

/// An unfortunate temporary wrapper used to mark log messages that are parsing errors.
/// At the time of writing there were no documented list of error codes that should be treated
/// as deprecation/parsing errors and thus handled by this middleware. Instead, existing call
/// sites just emitted all `FsErrors` as deprecation errors subject to the logic
/// in this module. Reverse-engineering all possible codes as part of migration seemed
/// a tall order, so instead we wrap the log message in this type that allows distinguishing
/// them from other log messages.
/// TODO: remove this wrapper and replace with a static list of fusion error codes that
/// should be treated as parsing/deprecation errors.
#[derive(Debug)]
pub(in crate::tracing) struct ParsingErrorMessage(LogMessage);

impl ParsingErrorMessage {
    pub fn new(log_message: LogMessage) -> Self {
        Self(log_message)
    }
}

impl AnyTelemetryEvent for ParsingErrorMessage {
    fn event_type(&self) -> &'static str {
        self.0.event_type()
    }

    fn event_display_name(&self) -> String {
        self.0.event_display_name()
    }

    fn record_category(&self) -> dbt_telemetry::TelemetryEventRecType {
        self.0.record_category()
    }

    fn output_flags(&self) -> dbt_telemetry::TelemetryOutputFlags {
        self.0.output_flags()
    }

    fn event_eq(&self, other: &dyn AnyTelemetryEvent) -> bool {
        if let Some(other) = other.as_any().downcast_ref::<Self>() {
            self.0 == other.0
        } else {
            false
        }
    }

    fn has_sensitive_data(&self) -> bool {
        self.0.has_sensitive_data()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn clone_box(&self) -> Box<dyn AnyTelemetryEvent> {
        Box::new(Self(self.0.clone()))
    }

    fn to_json(&self) -> Result<serde_json::Value, String> {
        self.0.to_json()
    }
}

/// Private type to store set of package names that have already emitted deprecation warnings
/// in data provider.
struct PackageWithLogsSet(HashSet<String>);

/// Middleware that adjust parsing error severity based on hiddne config set by env vars and
/// catches dependency-related errors and aggregates them into a single error message.
pub struct TelemetryParsingErrorFilter {
    /// If true, all schema parsing issues will be shown as warnings instead of errors.
    beta_parsing: bool,
    /// If true, dependency package parsing issues will be shown as warnings instead of errors.
    beta_package_parsing: bool,
    /// If true, all deprecation errors/warnings from dependency packages will be shown,
    /// not just the first one.
    show_all_deprecations: bool,
}

impl TelemetryParsingErrorFilter {
    pub fn new(show_all_deprecations: bool) -> Self {
        let beta_parsing = match std::env::var("DBT_ENGINE_BETA_PARSING") {
            Ok(val) => val == "1",
            Err(_) => false, // default to false (strict mode on)
        };
        let beta_package_parsing = match std::env::var("DBT_ENGINE_BETA_PACKAGE_PARSING") {
            Ok(val) => val == "1",
            Err(_) => true, // default to true (strict mode off for packages)
        };

        Self {
            beta_parsing,
            beta_package_parsing,
            show_all_deprecations,
        }
    }
}

impl TelemetryMiddleware for TelemetryParsingErrorFilter {
    fn on_span_start(
        &self,
        span: dbt_telemetry::SpanStartInfo,
        data_provider: &mut DataProvider<'_>,
    ) -> Option<dbt_telemetry::SpanStartInfo> {
        // If our configuration requires filtering repeated deprecations from packages,
        // initialize the set to track seen deprecations.
        if !self.show_all_deprecations && span.parent_span_id.is_none() {
            data_provider.init(PackageWithLogsSet(HashSet::new()));
        }

        Some(span)
    }
    fn on_log_record(
        &self,
        mut record: LogRecordInfo,
        data_provider: &mut DataProvider<'_>,
    ) -> Option<LogRecordInfo> {
        let Some(wrapped_log_message) = record.attributes.downcast_mut::<ParsingErrorMessage>()
        else {
            // Not a log message we know how to handle, return as is
            return Some(record);
        };

        let log_message = &mut wrapped_log_message.0;

        let (downgrade_to_warn, downgrade_message_suffix) = if let Some(package_name) =
            log_message.package_name.as_ref()
        {
            // If we are filtering repeated deprecations from packages,
            // check if this is a deprecation message and if we've seen it before.
            if !self.show_all_deprecations {
                let mut seen = false;
                data_provider.with_mut::<PackageWithLogsSet>(|seen_package_names| {
                    // This will check if we've seen this package name before and
                    // insert it into the set if not in one go.
                    seen = !seen_package_names.0.insert(package_name.to_string());
                });

                if seen {
                    // We've seen this deprecation message before, filter it out.
                    return None;
                }

                // Not seen before, replace with a general message
                log_message.code = Some(ErrorCode::DependencyWarning as u32);
                record.body = format!(
                    "Package `{package_name}` issued one or more compatibility warnings. To display all warnings associated with this package, run with `--show-all-deprecations`."
                );
            }

            // for package-related logs, two env vars control downgrading
            (
                self.beta_parsing || self.beta_package_parsing,
                "(will error post preview)",
            )
        } else {
            // for local logs, only the main env var controls downgrading
            (self.beta_parsing, "(will error post beta)")
        };

        debug_assert_eq!(
            record.severity_number,
            SeverityNumber::Error,
            "Do not emit deprecation messages as non-errors"
        );
        debug_assert_eq!(
            log_message.original_severity_number(),
            SeverityNumber::Error,
            "Do not emit deprecation messages as non-errors"
        );

        if downgrade_to_warn {
            record.severity_number = SeverityNumber::Warn;
            record.severity_text = SeverityNumber::Warn.as_str().to_string();
            // Append a note that this is a downgraded message
            record.body = format!("{} {}", record.body, downgrade_message_suffix);
        }

        // Increment autofix counter
        data_provider.increment_metric(
            MetricKey::InvocationMetric(InvocationMetricKey::AutoFixSuggestions),
            1,
        );

        // Finally, always replace wrapper with inner log message
        record.attributes = log_message.clone().into();
        Some(record)
    }
}
