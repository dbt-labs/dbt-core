use dbt_telemetry::AnyTelemetryEvent;

/// Private type to wrap messages intended for stdout printing only (essentially alternative to `println!`).
#[derive(Debug)]
pub(in crate::tracing) struct StdoutMessage;

impl AnyTelemetryEvent for StdoutMessage {
    fn event_type(&self) -> &'static str {
        "v1.internal.events.fusion.log.StdoutMessage"
    }

    fn event_display_name(&self) -> String {
        "Stdout Message".to_string()
    }

    fn record_category(&self) -> dbt_telemetry::TelemetryEventRecType {
        dbt_telemetry::TelemetryEventRecType::Log
    }

    fn output_flags(&self) -> dbt_telemetry::TelemetryOutputFlags {
        dbt_telemetry::TelemetryOutputFlags::OUTPUT_CONSOLE
    }

    fn event_eq(&self, _: &dyn AnyTelemetryEvent) -> bool {
        false
    }

    fn has_sensitive_data(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn clone_box(&self) -> Box<dyn AnyTelemetryEvent> {
        Box::new(Self)
    }

    fn to_json(&self) -> Result<serde_json::Value, String> {
        Err("Unexpected attempt to serialize internal event".to_string())
    }
}
