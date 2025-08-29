use std::collections::HashMap;

use super::super::{TelemetryShutdown, event_info::with_current_thread_event_data};
use crate::constants::DBT_FUSION;
use crate::{ErrorCode, FsResult};

use dbt_telemetry::{
    LogEventInfo, SeverityNumber, SpanEndInfo, SpanStatus, StatusCode, TelemetryAttributes,
};
use parquet::data_type::AsBytes;

use opentelemetry::{
    KeyValue, SpanId, TraceFlags, Value as OtelValue,
    context::Context as OtelContext,
    global,
    logs::{AnyValue, LogRecord, Logger, LoggerProvider, Severity as OtelSeverity},
    trace::{
        SamplingResult, Span as OtelSpanTrait, SpanContext, SpanKind, Status as OtelStatus,
        TraceContextExt, TraceState, Tracer, TracerProvider,
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

const fn level_to_otel_severity(severity_number: &SeverityNumber) -> OtelSeverity {
    match severity_number {
        SeverityNumber::Trace => OtelSeverity::Trace,
        SeverityNumber::Debug => OtelSeverity::Debug,
        SeverityNumber::Info => OtelSeverity::Info,
        SeverityNumber::Warn => OtelSeverity::Warn,
        SeverityNumber::Error => OtelSeverity::Error,
        SeverityNumber::Fatal => OtelSeverity::Fatal,
    }
}

const fn level_to_otel_severity_text(severity_number: &SeverityNumber) -> &'static str {
    match severity_number {
        SeverityNumber::Trace => "TRACE",
        SeverityNumber::Debug => "DEBUG",
        SeverityNumber::Info => "INFO",
        SeverityNumber::Warn => "WARN",
        SeverityNumber::Error => "ERROR",
        SeverityNumber::Fatal => "FATAL",
    }
}

fn serde_json_value_to_otel(value: &serde_json::Value) -> OtelValue {
    match value {
        serde_json::Value::Bool(b) => OtelValue::from(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                OtelValue::from(i)
            } else if let Some(u) = n.as_u64() {
                if u > i64::MAX as u64 {
                    // If the number is too large for i64, we convert it to a string
                    return OtelValue::from(u.to_string());
                } else {
                    // Otherwise, we can safely convert it to i64
                    OtelValue::from(u as i64)
                }
            } else if let Some(f) = n.as_f64() {
                OtelValue::from(f)
            } else {
                // Should not be reached
                OtelValue::from(n.to_string())
            }
        }
        serde_json::Value::String(s) => OtelValue::from(s.clone()),
        _ => value.to_string().into(),
    }
}

fn serde_json_value_to_otel_any_value(value: &serde_json::Value) -> AnyValue {
    match value {
        serde_json::Value::Bool(b) => AnyValue::from(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                AnyValue::from(i)
            } else if let Some(u) = n.as_u64() {
                if u > i64::MAX as u64 {
                    // If the number is too large for i64, we convert it to bytes
                    return AnyValue::from(u.as_bytes());
                } else {
                    // Otherwise, we can safely convert it to i64
                    AnyValue::from(u as i64)
                }
            } else if let Some(f) = n.as_f64() {
                AnyValue::from(f)
            } else {
                // Should not be reached
                AnyValue::from(n.to_string())
            }
        }
        serde_json::Value::String(s) => AnyValue::from(s.clone()),
        serde_json::Value::Array(arr) => AnyValue::ListAny(Box::new(
            arr.iter()
                .map(serde_json_value_to_otel_any_value)
                .collect::<Vec<_>>(),
        )),
        serde_json::Value::Object(obj) => AnyValue::Map(Box::new(
            obj.iter()
                .map(|(k, v)| {
                    (
                        opentelemetry::Key::from(k.clone()),
                        serde_json_value_to_otel_any_value(v),
                    )
                })
                .collect::<HashMap<_, _>>(),
        )),
        _ => AnyValue::from(value.to_string()),
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

        let otel_trace_id = span_data.trace_id.into();
        let otel_span_id = span_data.span_id.into();

        // OTEL sdk doesn't allow "just" specifying the parent span id, so we
        // use this faked remote context to achieve that...
        let otel_parent_cx = if let Some(parent_span_id) = span_data.parent_span_id {
            OtelContext::new().with_remote_span_context(SpanContext::new(
                otel_trace_id,
                parent_span_id.into(),
                TraceFlags::SAMPLED,
                false,
                TraceState::NONE,
            ))
        } else {
            OtelContext::new()
        };

        let span_attrs = serde_json::to_value(&span_data.attributes)
            .ok()
            .and_then(|val| {
                // We are using external tag for attributes enum, so value is a map with 2
                // keys: "attributes" and "eventName". We only care about the attributes.
                val.as_object().and_then(|top| {
                    top.get("attributes").and_then(|attrs| {
                        attrs.as_object().map(|pairs| {
                            pairs
                                .iter()
                                .map(|(k, v)| KeyValue::new(k.clone(), serde_json_value_to_otel(v)))
                                .collect::<Vec<_>>()
                        })
                    })
                })
            })
            .unwrap_or_default();

        // Create OpenTelemetry span
        let mut otel_span = self
            .tracer
            .span_builder(span_data.span_name.clone())
            // This forces all spans to be exported
            .with_sampling_result(SamplingResult {
                attributes: Default::default(),
                decision: opentelemetry::trace::SamplingDecision::RecordAndSample,
                trace_state: Default::default(),
            })
            .with_kind(SpanKind::Internal)
            .with_trace_id(otel_trace_id)
            .with_span_id(otel_span_id)
            .with_start_time(span_data.start_time_unix_nano)
            .with_attributes(span_attrs)
            .start_with_context(&self.tracer, &otel_parent_cx);

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
        otel_span.end_with_timestamp(span_data.end_time_unix_nano);
    }

    fn on_event(&self, _event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        // Add log record as events
        with_current_thread_event_data(|log_record| {
            // Create a new log record
            let mut otel_log_record = self.logger.create_log_record();

            // Set the log basic attributes
            otel_log_record
                .set_severity_number(level_to_otel_severity(&log_record.severity_number));

            otel_log_record
                .set_severity_text(level_to_otel_severity_text(&log_record.severity_number));

            // Message
            otel_log_record.set_body(AnyValue::from(log_record.body.clone()));

            // Set timestamp ourselves, since sdk only sets the observed timestamp.
            otel_log_record.set_timestamp(log_record.time_unix_nano);
            otel_log_record.set_observed_timestamp(log_record.time_unix_nano);

            // Set source code attributes
            if let TelemetryAttributes::Log(LogEventInfo { location, .. }) = &log_record.attributes
            {
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
                .ok()
                .and_then(|val| {
                    // We are using external tag for attributes enum, so value is a map with 2
                    // keys: "attributes" and "eventName". We only care about the attributes.
                    val.as_object().and_then(|top| {
                        top.get("attributes").and_then(|attrs| {
                            attrs.as_object().map(|pairs| {
                                pairs
                                    .iter()
                                    // TODO filter out duplicates from code location
                                    .map(|(k, v)| {
                                        (
                                            opentelemetry::Key::from(k.clone()),
                                            serde_json_value_to_otel_any_value(v),
                                        )
                                    })
                                    .collect::<Vec<_>>()
                            })
                        })
                    })
                })
                .unwrap_or_default();

            otel_log_record.set_event_name((&log_record.attributes).into());
            otel_log_record.add_attributes(log_attrs);

            otel_log_record.set_trace_context(
                log_record.trace_id.into(),
                log_record
                    .span_id
                    .map(|span_id| span_id.into())
                    .unwrap_or(SpanId::INVALID),
                Some(TraceFlags::SAMPLED),
            );

            self.logger.emit(otel_log_record);
        });
    }
}
