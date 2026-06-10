//! Custom serialization and deserialization functions for telemetry records.

pub mod arrow;
pub mod otlp;
pub mod traits;

// Generic id/timestamp serde helpers now live in dbt-tracing; reexported here so
// `serde(serialize_with = "...")` paths and the dbt-specific arrow/otlp serializers
// resolve them unchanged.
pub use dbt_tracing::serialize::envelope::{
    deserialize_optional_span_id, deserialize_span_id, deserialize_timestamp, deserialize_trace_id,
    serialize_optional_span_id, serialize_span_id, serialize_timestamp, serialize_trace_id,
    to_nanos,
};
