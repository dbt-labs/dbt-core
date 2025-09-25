use chrono::{DateTime, Utc};
use dbt_telemetry::{QueryExecuted, QueryOutcome, TelemetryRecordRef};
use std::fmt::Write as _;

pub fn format_query_log_event(record: TelemetryRecordRef, _is_tty: bool) -> Option<String> {
    if let TelemetryRecordRef::SpanEnd(span_end) = record
        && let Some(query_data) = span_end.attributes.downcast_ref::<QueryExecuted>()
    {
        let mut buf = String::new();

        writeln!(
            &mut buf,
            "-- created_at: {}",
            DateTime::<Utc>::from(span_end.start_time_unix_nano).to_rfc3339()
        )
        .unwrap();
        writeln!(
            &mut buf,
            "-- finished_at: {}",
            DateTime::<Utc>::from(span_end.end_time_unix_nano).to_rfc3339()
        )
        .unwrap();
        writeln!(
            &mut buf,
            "-- outcome: {}",
            query_data.query_outcome().as_ref()
        )
        .unwrap();

        if query_data.query_outcome() == QueryOutcome::Error {
            if let Some(vendor_code) = query_data.query_error_vendor_code {
                writeln!(&mut buf, "-- error vendor code: {vendor_code}").unwrap();
            }

            if let Some(adapter_message) = query_data.query_error_adapter_message.as_deref() {
                writeln!(&mut buf, "-- error message: {adapter_message}").unwrap();
            }
        }
        writeln!(&mut buf, "-- dialect: {}", query_data.adapter_type.as_str()).unwrap();

        let node_id = query_data.unique_id.as_deref().unwrap_or("not available");
        writeln!(&mut buf, "-- node_id: {node_id}").unwrap();

        let query_id = query_data.query_id.as_deref().unwrap_or("not available");
        writeln!(&mut buf, "-- query_id: {query_id}").unwrap();

        match query_data.query_description.as_deref() {
            Some(desc) => writeln!(&mut buf, "-- desc: {desc}").unwrap(),
            None => writeln!(&mut buf, "-- desc: not provided").unwrap(),
        }

        write!(&mut buf, "{}", query_data.sql).unwrap();
        if !query_data.sql.ends_with(";") {
            write!(&mut buf, ";").unwrap();
        }

        return Some(buf);
    }

    None
}
