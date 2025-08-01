use std::time::SystemTime;

use super::super::{TelemetryShutdown, init::process_span};
use crate::constants::DBT_FUSION;
use crate::{ErrorCode, FsResult};

use dbt_telemetry::{
    LogAttributes, LogRecordInfo, SeverityNumber, SpanEndInfo, SpanStatus, StatusCode,
};

use opentelemetry::logs::AnyValue;
use opentelemetry::trace::SamplingResult;
use opentelemetry::{
    KeyValue, TraceFlags,
    context::Context as OtelContext,
    global,
    logs::{LogRecord, Logger, LoggerProvider, Severity as OtelSeverity},
    trace::{
        Span as OtelSpanTrait, SpanContext, SpanKind, Status as OtelStatus, TraceContextExt,
        TraceState, Tracer, TracerProvider,
    },
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::{SdkLogger, SdkLoggerProvider};
use opentelemetry_sdk::resource::EnvResourceDetector;
use opentelemetry_sdk::trace::{SdkTracer, SdkTracerProvider};
use opentelemetry_semantic_conventions::attribute::{CODE_FILE_PATH, CODE_LINE_NUMBER};
use opentelemetry_semantic_conventions::resource::{SERVICE_NAME, SERVICE_VERSION};
use tracing::{Subscriber, span};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

// Type used to collect events for batch export on span close
type SpanEventVec = Vec<LogRecordInfo>;

const fn level_to_otel_severity(severity_number: &SeverityNumber) -> OtelSeverity {
    match severity_number {
        SeverityNumber::Trace => OtelSeverity::Trace,
        SeverityNumber::Trace2 => OtelSeverity::Trace2,
        SeverityNumber::Trace3 => OtelSeverity::Trace3,
        SeverityNumber::Trace4 => OtelSeverity::Trace4,
        SeverityNumber::Debug => OtelSeverity::Debug,
        SeverityNumber::Debug2 => OtelSeverity::Debug2,
        SeverityNumber::Debug3 => OtelSeverity::Debug3,
        SeverityNumber::Debug4 => OtelSeverity::Debug4,
        SeverityNumber::Info => OtelSeverity::Info,
        SeverityNumber::Info2 => OtelSeverity::Info2,
        SeverityNumber::Info3 => OtelSeverity::Info3,
        SeverityNumber::Info4 => OtelSeverity::Info4,
        SeverityNumber::Warn => OtelSeverity::Warn,
        SeverityNumber::Warn2 => OtelSeverity::Warn2,
        SeverityNumber::Warn3 => OtelSeverity::Warn3,
        SeverityNumber::Warn4 => OtelSeverity::Warn4,
        SeverityNumber::Error => OtelSeverity::Error,
        SeverityNumber::Error2 => OtelSeverity::Error2,
        SeverityNumber::Error3 => OtelSeverity::Error3,
        SeverityNumber::Error4 => OtelSeverity::Error4,
        SeverityNumber::Fatal => OtelSeverity::Fatal,
        SeverityNumber::Fatal2 => OtelSeverity::Fatal2,
        SeverityNumber::Fatal3 => OtelSeverity::Fatal3,
        SeverityNumber::Fatal4 => OtelSeverity::Fatal4,
    }
}

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
    /// Creates a new OTLPExporterLayer
    ///
    /// If endpoint is not reachable, it will return None.
    ///
    /// Reads the OTLP endpoint from either:
    /// - the environment variable `OTEL_EXPORTER_OTLP_ENDPOINT` - works for logs & traces,
    ///   and assumes default routes: `/v1/logs` for logs and `/v1/traces` for traces.
    /// - the environment variable `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` - works
    ///   can be used to specify a full endpoint for traces, with non-default routes.
    /// - the environment variable `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` - works
    ///   can be used to specify a full endpoint for logs, with non-default routes.
    pub(crate) fn new() -> Option<Self> {
        // Set up resource with service information
        let resource = Resource::builder()
            .with_detectors(&[Box::new(EnvResourceDetector::new())])
            .with_attributes(vec![
                KeyValue::new(SERVICE_NAME, DBT_FUSION),
                KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
            ])
            .build();

        // Add OTLP trace HTTP exporter
        let tracing_http_exporter = match opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
            .build()
        {
            Ok(http_exporter) => http_exporter,
            Err(_) => return None,
        };

        // Initialize a tracer provider.
        let tracer_provider = SdkTracerProvider::builder()
            .with_resource(resource.clone())
            .with_batch_exporter(tracing_http_exporter)
            .build();

        // Create OTLP logger exporter
        let logger_http_export = match opentelemetry_otlp::LogExporterBuilder::new()
            .with_http()
            .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
            .build()
        {
            Ok(http_exporter) => http_exporter,
            Err(_) => return None,
        };

        // Initialize a logger provider.
        let logger_provider = SdkLoggerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(logger_http_export)
            .build();

        // Set the global tracer provider. Clone is necessary but cheap, as it is a reference
        // to the same object.
        global::set_tracer_provider(tracer_provider.clone());

        // Get tracer
        let tracer = tracer_provider.tracer(DBT_FUSION);

        // Get root logger
        let logger = logger_provider.logger(DBT_FUSION);

        Some(OTLPExporterLayer {
            tracer_provider,
            logger_provider,
            tracer,
            logger,
            __phantom: std::marker::PhantomData,
        })
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
    fn on_new_span(&self, _attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let span = ctx
            .span(id)
            .expect("Span must exist for id in the current context");

        // Create an empty vector where we'll collect log records while within this span
        span.extensions_mut().insert::<SpanEventVec>(Vec::new());
    }

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

        let otel_trace_id = span_data.trace_id.into();
        let otel_span_id = span_data.span_id.into();

        // Get the vector of logs recorded within the span
        let Some(span_log_vector) = extensions.get::<SpanEventVec>() else {
            unreachable!("Unexpectedly missing span logs data!");
        };

        // OTEL sdk doesn't allow "just" specifying the parent span id, so we
        // use this faked remote context to achieve that...
        let otel_parent_cx = if let Some(parent_span_id) = span_data.parent_span_id {
            OtelContext::current().with_remote_span_context(SpanContext::new(
                otel_trace_id,
                parent_span_id.into(),
                TraceFlags::SAMPLED,
                false,
                TraceState::NONE,
            ))
        } else {
            OtelContext::current()
        };

        let span_attrs = serde_json::to_value(&span_data.attributes)
            .map(|val| val.as_object().cloned())
            .ok()
            .map_or(Vec::new(), |maybe_pairs| {
                maybe_pairs
                    .map(|pairs| {
                        pairs
                            .iter()
                            .map(|(k, v)| KeyValue::new(k.clone(), v.to_string()))
                            .collect()
                    })
                    .unwrap_or_default()
            });

        // Create OpenTelemetry span
        let mut otel_span = self
            .tracer
            .span_builder(span_data.name.clone())
            // This forces all spans to be exported
            .with_sampling_result(SamplingResult {
                attributes: Default::default(),
                decision: opentelemetry::trace::SamplingDecision::RecordAndSample,
                trace_state: Default::default(),
            })
            .with_kind(SpanKind::Internal)
            .with_trace_id(otel_trace_id)
            .with_span_id(otel_span_id)
            .with_start_time(
                // Yes, stupid. We have convert that back to SystemTime just to satisfy the SDK, which will
                // convert it back to a timestamp during export...
                SystemTime::UNIX_EPOCH
                    + std::time::Duration::from_nanos(span_data.start_time_unix_nano),
            )
            .with_attributes(span_attrs)
            .start_with_context(&self.tracer, &otel_parent_cx);

        // Add log records as events
        for log_record in span_log_vector {
            // Create a new log record
            let mut otel_log_record = self.logger.create_log_record();

            // Set the log basic attributes
            otel_log_record
                .set_severity_number(level_to_otel_severity(&log_record.severity_number));

            // Requires static lifetime...skipping for now
            // otel_log_record.set_severity_text(log_record.severity_text.unwrap_or_default());

            // Message
            otel_log_record.set_body(AnyValue::from(log_record.body.clone()));

            // Set timestamp ourselves, since sdk only sets the observed timestamp.
            let ts =
                SystemTime::UNIX_EPOCH + std::time::Duration::from_nanos(log_record.time_unix_nano);

            otel_log_record.set_timestamp(ts);
            otel_log_record.set_observed_timestamp(ts);

            // Set source code attributes
            if let LogAttributes::Log { location, .. } = &log_record.attributes {
                if let Some(file) = location.file.clone() {
                    otel_log_record.add_attribute(
                        opentelemetry::Key::new(CODE_FILE_PATH),
                        AnyValue::from(file),
                    );
                }

                if let Some(line) = location.line {
                    otel_log_record.add_attribute(
                        opentelemetry::Key::new(CODE_LINE_NUMBER),
                        AnyValue::from(line),
                    );
                }

                // if let Some(module) = location.module_path() {
                //     otel_log_record.add_attribute(
                //         opentelemetry::Key::new(CODE_FUNCTION_NAME),
                //         AnyValue::from(module.to_string()),
                //     );
                // }
            }

            let log_attrs = serde_json::to_value(&log_record.attributes)
                .map(|val| val.as_object().cloned())
                .ok()
                .flatten()
                .map_or(Vec::new(), |pairs| {
                    pairs
                        .iter()
                        // TODO filter out duplicates from code location
                        .map(|(k, v)| (opentelemetry::Key::from(k.clone()), v.to_string()))
                        .collect()
                });

            otel_log_record.set_event_name((&log_record.attributes).into());
            otel_log_record.add_attributes(log_attrs);
            // log_attrs.push(KeyValue::new("message", log_record.body.clone()));
            // log_attrs.push(KeyValue::new(
            //     "level",
            //     log_record.severity_number.clone() as i32 as i64,
            // ));

            otel_log_record.set_trace_context(
                otel_trace_id,
                otel_span_id,
                Some(TraceFlags::SAMPLED),
            );

            // otel_span.add_event_with_timestamp(
            //     otel_log_record.event_name().unwrap_or("log"),
            //     SystemTime::UNIX_EPOCH + std::time::Duration::from_nanos(log_record.time_unix_nano),
            //     otel_log_record
            //         .attributes_iter()
            //         .map(|(k, v)| KeyValue::new(k.clone(), v.into()))
            //         .collect(),
            // );

            self.logger.emit(otel_log_record);
        }

        // Set span status as OK
        if let Some(SpanStatus { code, message }) = &span_data.status {
            match code {
                StatusCode::Ok => otel_span.set_status(OtelStatus::Ok),
                StatusCode::Error => otel_span.set_status(OtelStatus::Error {
                    description: message.clone().unwrap_or_default().into(),
                }),
                _ => {}
            }
        };

        // End the span
        otel_span.end_with_timestamp(
            SystemTime::UNIX_EPOCH + std::time::Duration::from_nanos(span_data.end_time_unix_nano),
        );
    }

    fn on_event(&self, _event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        // Get the current span to extract span information
        let Some(current_span) = ctx.lookup_current().or_else(|| process_span(&ctx)) else {
            // If no current span is found, we can't get the event data
            // This may happen if tracing is not initialized (e.g. in tests)
            return;
        };

        // Extract & remove a LogRecord in the extensions (from TelemetryDataLayer)
        let log_record = {
            if let Some(log_record) = current_span.extensions().get::<LogRecordInfo>() {
                log_record.clone()
            } else {
                unreachable!("Unexpectedly missing log record data!");
            }
        }; // span_record is dropped here, releasing the immutable borrow

        // Get the log vector to store event data (we emit at the end of the span)
        if let Some(log_vector) = current_span.extensions_mut().get_mut::<SpanEventVec>() {
            log_vector.push(log_record);
        } else {
            unreachable!("Unexpectedly missing log record storage!");
        };
    }
}
