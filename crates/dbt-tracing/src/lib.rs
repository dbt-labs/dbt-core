//! A structured tracing library built on top of the [`tracing`] crate.
//!
//! `dbt-tracing` extends `tracing` with *fully structured* telemetry: spans and
//! events are described by typed, self-describing event structs instead of
//! ad-hoc key/value fields. On top of that it provides more ergonomic emit and
//! span APIs, a data layer that materializes events into trace/log record
//! envelopes, middleware that can filter and transform events in flight, and
//! consumer infrastructure for convenient storage and export. It owns the
//! generic telemetry API, the record envelopes, status/location metadata, and
//! the serialization registry traits.
//!
//! The library is independent of any concrete event taxonomy or output format —
//! it defines the traits and record types but ships no dbt-specific event
//! schemas, formatters, or exporters. Users provide those: Fusion and dbt-core
//! define their event types in `dbt-telemetry` (generated from protobuf), and
//! the dbt/Fusion integration layer — CLI config, user-facing formatting, and
//! export wiring — lives in `dbt-common::tracing`.
//!
//! [`tracing`]: https://docs.rs/tracing

pub mod attributes;
pub mod schemas;
pub mod serialize;
mod static_name;

pub use static_name::StaticName;

pub use attributes::{
    AnyTelemetryEvent, ArrowSerializableTelemetryEvent, StaticTelemetryEvent, TelemetryAttributes,
    TelemetryContext, TelemetryEventRecType, TelemetryOutputFlags,
};
pub use schemas::{
    LogRecordInfo, RecordCodeLocation, SeverityNumber, SpanEndInfo, SpanLinkInfo, SpanStartInfo,
    SpanStatus, StatusCode, TelemetryRecord, TelemetryRecordRef, TelemetryRecordType,
};
