use std::borrow::Cow;

use super::super::{
    data_provider::DataProvider,
    layer::{ConsumerLayer, TelemetryConsumer},
    shutdown::{TelemetryShutdown, TelemetryShutdownItem},
};
use crate::constants::DBT_FUSION;

use dbt_error::{ErrorCode, FsResult};

use dbt_telemetry::{
    LogMessage,
    serialize::otlp::{export_log, export_span},
};
use dbt_telemetry::{LogRecordInfo, SpanEndInfo, SpanStartInfo, TelemetryOutputFlags};

use opentelemetry::{KeyValue, global, logs::LoggerProvider, trace::TracerProvider};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::{SdkLogger, SdkLoggerProvider};
use opentelemetry_sdk::resource::EnvResourceDetector;
use opentelemetry_sdk::trace::{SdkTracer, SdkTracerProvider};
use opentelemetry_sdk::{logs as sdk_logs, trace as sdk_trace};
use opentelemetry_semantic_conventions::resource::{SERVICE_NAME, SERVICE_VERSION};

/// Build an OTLP layer with HTTP exporters. If exporters cannot be built,
/// it will return None.
pub fn build_otlp_layer() -> Option<(ConsumerLayer, Vec<TelemetryShutdownItem>)> {
    let layer = OTLPExporterLayer::new_with_http_export()?;

    let shutdown_items: Vec<TelemetryShutdownItem> = vec![
        Box::new(layer.tracer_provider()),
        Box::new(layer.logger_provider()),
    ];

    Some((Box::new(layer), shutdown_items))
}

/// A tracing layer that reads telemetry data and sends it over HTTP to OTLP endpoint
pub struct OTLPExporterLayer {
    tracer_provider: SdkTracerProvider,
    logger_provider: SdkLoggerProvider,
    tracer: SdkTracer,
    logger: SdkLogger,
}

impl OTLPExporterLayer {
    /// Creates a new OTLPExporterLayer from provided exporters
    pub(crate) fn new(
        trace_exporter: impl sdk_trace::SpanExporter + 'static,
        log_exporter: impl sdk_logs::LogExporter + 'static,
    ) -> Self {
        // Set up resource with service information
        let resource = Resource::builder()
            .with_detectors(&[Box::new(EnvResourceDetector::new())])
            .with_attributes(vec![
                KeyValue::new(SERVICE_NAME, DBT_FUSION),
                KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
            ])
            .build();

        // Initialize a tracer provider.
        let tracer_provider = SdkTracerProvider::builder()
            .with_resource(resource.clone())
            .with_batch_exporter(trace_exporter)
            .build();

        // Initialize a logger provider.
        let logger_provider = SdkLoggerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(log_exporter)
            .build();

        // Set the global tracer provider. Clone is necessary but cheap, as it is a reference
        // to the same object.
        global::set_tracer_provider(tracer_provider.clone());

        // Get tracer
        let tracer = tracer_provider.tracer(DBT_FUSION);

        // Get root logger
        let logger = logger_provider.logger(DBT_FUSION);

        OTLPExporterLayer {
            tracer_provider,
            logger_provider,
            tracer,
            logger,
        }
    }

    /// Creates a new OTLPExporterLayer with HTTP exporters (binary protocol)
    ///
    /// If endpoint is not reachable or exporters fail to build, it will return None.
    ///
    /// Reads the OTLP endpoint from either:
    /// - the environment variable `OTEL_EXPORTER_OTLP_ENDPOINT` - works for logs & traces,
    ///   and assumes default routes: `/v1/logs` for logs and `/v1/traces` for traces.
    /// - the environment variable `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` - can
    ///   be used to specify a full endpoint for traces, with non-default routes.
    /// - the environment variable `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` - can
    ///   be used to specify a full endpoint for logs, with non-default routes.
    pub(crate) fn new_with_http_export() -> Option<Self> {
        // Add OTLP trace HTTP exporter
        let trace_exporter = match opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
            .build()
        {
            Ok(http_exporter) => http_exporter,
            Err(_) => return None,
        };

        // Create OTLP logger exporter
        let log_exporter = match opentelemetry_otlp::LogExporterBuilder::new()
            .with_http()
            .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
            .build()
        {
            Ok(http_exporter) => http_exporter,
            Err(_) => return None,
        };

        Some(Self::new(trace_exporter, log_exporter))
    }

    pub(crate) fn tracer_provider(&self) -> SdkTracerProvider {
        // Cheap, it's really an arc
        self.tracer_provider.clone()
    }

    pub(crate) fn logger_provider(&self) -> SdkLoggerProvider {
        // Cheap, it's really an arc
        self.logger_provider.clone()
    }
}

impl TelemetryShutdown for SdkTracerProvider {
    fn shutdown(&mut self) -> FsResult<()> {
        SdkTracerProvider::shutdown(self).map_err(|otel_error| {
            fs_err!(
                ErrorCode::IoError,
                "Failed to gracefully shutdown OTLP trace exporter: {otel_error}"
            )
        })
    }
}

impl TelemetryShutdown for SdkLoggerProvider {
    fn shutdown(&mut self) -> FsResult<()> {
        SdkLoggerProvider::shutdown(self).map_err(|otel_error| {
            fs_err!(
                ErrorCode::IoError,
                "Failed to gracefully shutdown OTLP log exporter: {otel_error}"
            )
        })
    }
}

impl TelemetryConsumer for OTLPExporterLayer {
    fn is_span_enabled(&self, span: &SpanStartInfo) -> bool {
        span.attributes
            .output_flags()
            .contains(TelemetryOutputFlags::EXPORT_OTLP)
    }

    fn is_log_enabled(&self, log_record: &LogRecordInfo) -> bool {
        log_record
            .attributes
            .output_flags()
            .contains(TelemetryOutputFlags::EXPORT_OTLP)
    }

    // We record spans to OTLP only when they are closed, so we don't need to do anything on new span
    fn on_span_end(&self, span: &SpanEndInfo, _: &mut DataProvider<'_>) {
        export_span(&self.tracer, span);
    }

    fn on_log_record(&self, record: &LogRecordInfo, _: &mut DataProvider<'_>) {
        // Unfortunately, we do not currently enforce log body to not contain ANSI codes,
        // so we need to make sure to strip them
        if record.attributes.is::<LogMessage>()
            && let Cow::Owned(stripped) = console::strip_ansi_codes(record.body.as_str())
        {
            let stripped_record = LogRecordInfo {
                body: stripped,
                ..record.clone()
            };
            export_log(&self.logger, &stripped_record);
            return;
        }

        export_log(&self.logger, record);
    }
}
