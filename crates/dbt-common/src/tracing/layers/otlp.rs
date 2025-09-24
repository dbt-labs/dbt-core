use super::super::{TelemetryShutdown, event_info::with_current_thread_log_record};
use crate::constants::DBT_FUSION;
use crate::{ErrorCode, FsResult};

use dbt_telemetry::serialize::otlp::{export_log, export_span};
use dbt_telemetry::{SpanEndInfo, TelemetryOutputFlags};

use opentelemetry::{KeyValue, global, logs::LoggerProvider, trace::TracerProvider};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::{SdkLogger, SdkLoggerProvider};
use opentelemetry_sdk::resource::EnvResourceDetector;
use opentelemetry_sdk::trace::{SdkTracer, SdkTracerProvider};
use opentelemetry_sdk::{logs as sdk_logs, trace as sdk_trace};
use opentelemetry_semantic_conventions::resource::{SERVICE_NAME, SERVICE_VERSION};
use tracing::{Subscriber, span};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

/// A tracing layer that reads telemetry data and sends it over HTTP to OTLP endpoint
pub struct OTLPExporterLayer<S>
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    tracer_provider: SdkTracerProvider,
    logger_provider: SdkLoggerProvider,
    tracer: SdkTracer,
    logger: SdkLogger,
    __phantom: std::marker::PhantomData<S>,
}

impl<S> OTLPExporterLayer<S>
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
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
            __phantom: std::marker::PhantomData,
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

impl<S> Layer<S> for OTLPExporterLayer<S>
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    // We record spans to OTLP only when they are closed, so we don't need to do anything on new span
    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        let span = ctx
            .span(&id)
            .expect("Span must exist for id in the current context");

        // Get the TelemetryRecord from extensions. It must be there unless we messed
        // up data layer / layer order.
        let extensions = span.extensions();

        let Some(span_data) = extensions.get::<SpanEndInfo>() else {
            unreachable!("Unexpectedly missing span end data!");
        };

        // Honor export flags: only export if OTLP export is enabled
        if !span_data
            .attributes
            .output_flags()
            .contains(TelemetryOutputFlags::EXPORT_OTLP)
        {
            return;
        }

        export_span(&self.tracer, span_data);
    }

    fn on_event(&self, _event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        // Add log record as events
        with_current_thread_log_record(|log_record| {
            // Honor export flags: only export if OTLP export is enabled
            if !log_record
                .attributes
                .output_flags()
                .contains(TelemetryOutputFlags::EXPORT_OTLP)
            {
                return;
            }
            export_log(&self.logger, log_record);
        });
    }
}
