use std::sync::{Arc, RwLock};

use dbt_error::ErrorCode;
use dbt_telemetry::{LogMessage, LogRecordInfo, SeverityNumber};

use crate::{
    tracing::{data_provider::DataProvider, layer::TelemetryMiddleware},
    warn_error_options::{ErrorCtx, WarnErrorDecision, WarnErrorOptions},
};

pub struct TelemetryWarnErrorOptionsMiddleware {
    warn_error_options: Arc<RwLock<WarnErrorOptions>>,
}

impl TelemetryWarnErrorOptionsMiddleware {
    pub fn new(warn_error_options: WarnErrorOptions) -> (Self, Arc<RwLock<WarnErrorOptions>>) {
        let warn_error_options = Arc::new(RwLock::new(warn_error_options));

        (
            Self {
                warn_error_options: Arc::clone(&warn_error_options),
            },
            warn_error_options,
        )
    }
}

impl TelemetryMiddleware for TelemetryWarnErrorOptionsMiddleware {
    fn on_log_record(
        &self,
        mut record: LogRecordInfo,
        _data_provider: &mut DataProvider<'_>,
    ) -> Option<LogRecordInfo> {
        let Some(log_message) = record.attributes.downcast_ref::<LogMessage>() else {
            return Some(record);
        };
        if record.severity_number != SeverityNumber::Warn {
            return Some(record);
        }
        let Some(code) = log_message
            .code
            .and_then(|code| u16::try_from(code).ok())
            .and_then(|code| ErrorCode::try_from(code).ok())
        else {
            return Some(record);
        };

        let decision = self
            .warn_error_options
            .read()
            .expect("warn_error_options lock should not be poisoned")
            .decision_for_error_code_with_context(
                code,
                ErrorCtx::from_dependency_package_name(log_message.package_name.as_deref()),
            );

        match decision {
            WarnErrorDecision::Silence => None,
            WarnErrorDecision::Retain => Some(record),
            WarnErrorDecision::UpgradeToError => {
                record.severity_number = SeverityNumber::Error;
                record.severity_text = SeverityNumber::Error.as_str().to_string();
                Some(record)
            }
        }
    }
}
