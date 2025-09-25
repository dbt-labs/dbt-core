use crate::{
    SpanStatus, TelemetryOutputFlags,
    attributes::{
        ArrowSerializableTelemetryEvent, ProtoTelemetryEvent, TelemetryContext,
        TelemetryEventRecType,
    },
    serialize::arrow::ArrowAttributes,
};

use prost::Name;
use std::borrow::Cow;

pub use proto_rust::v1::public::events::fusion::node::{
    NodeCacheReason, NodeCancelReason, NodeErrorType, NodeEvaluated, NodeMaterialization,
    NodeOutcome, NodeSkipReason, NodeType, SourceFreshnessDetail, SourceFreshnessOutcome,
    TestEvaluationDetail, TestOutcome,
};

impl ProtoTelemetryEvent for NodeEvaluated {
    const RECORD_CATEGORY: TelemetryEventRecType = TelemetryEventRecType::Span;
    const OUTPUT_FLAGS: TelemetryOutputFlags = TelemetryOutputFlags::ALL;

    fn event_display_name(&self) -> String {
        format!("Node evaluated ({})", self.unique_id)
    }

    fn get_span_status(&self) -> Option<SpanStatus> {
        match self.node_outcome() {
            NodeOutcome::Success | NodeOutcome::Skipped => SpanStatus::succeeded().into(),
            NodeOutcome::Error => SpanStatus::failed("error").into(),
            NodeOutcome::Canceled => SpanStatus::failed("canceled").into(),
            NodeOutcome::Unspecified => None,
        }
    }

    fn has_sensitive_data(&self) -> bool {
        // TODO: theoretically we may want to use a consistent scrambling/hashing of
        // identifiers as some may consider this sensitive
        false
    }

    fn context(&self) -> Option<TelemetryContext> {
        Some(TelemetryContext {
            phase: Some(self.phase()),
            unique_id: Some(self.unique_id.clone()),
        })
    }
}

impl ArrowSerializableTelemetryEvent for NodeEvaluated {
    fn to_arrow_record(&self) -> ArrowAttributes<'_> {
        ArrowAttributes {
            // Well-known fields for easier querying
            phase: Some(self.phase()),
            name: Some(Cow::Borrowed(self.name.as_str())),
            database: self.database.as_deref().map(Cow::Borrowed),
            schema: self.schema.as_deref().map(Cow::Borrowed),
            identifier: self.identifier.as_deref().map(Cow::Borrowed),
            unique_id: Some(Cow::Borrowed(self.unique_id.as_str())),
            materialization: self.materialization.map(|_| self.materialization()),
            custom_materialization: self.custom_materialization.as_deref().map(Cow::Borrowed),
            node_type: Some(self.node_type()),
            node_outcome: Some(self.node_outcome()),
            node_error_type: self.node_error_type.map(|_| self.node_error_type()),
            node_cancel_reason: self.node_cancel_reason.map(|_| self.node_cancel_reason()),
            node_skip_reason: self.node_skip_reason.map(|_| self.node_skip_reason()),
            dbt_core_event_code: self.dbt_core_event_code.as_deref().map(Cow::Borrowed),
            // Serialize node_outcome_detail into JSON as it may grow with arbitrary data and less
            // likely to be queried directly
            json_payload: self.node_outcome_detail.map(|v| {
                serde_json::to_string(&v).unwrap_or_else(|_| {
                    panic!(
                        "Failed to serialize `node_outcome_detail` in event type \"{}\" to JSON",
                        Self::full_name()
                    )
                })
            }),
            ..Default::default()
        }
    }

    fn from_arrow_record(record: &ArrowAttributes) -> Result<Self, String> {
        Ok(
            Self {
                phase: record.phase.map(|v| v as i32).ok_or_else(
                    || format!("Missing `phase` for event type \"{}\"", Self::full_name())
                )?,
                name: record
                    .name
                    .as_deref()
                    .map(str::to_string)
                    .ok_or_else(|| format!("Missing `name` for event type \"{}\"", Self::full_name()))?,
                database: record.database.as_deref().map(str::to_string),
                schema: record.schema.as_deref().map(str::to_string),
                identifier: record.identifier.as_deref().map(str::to_string),
                unique_id: record.unique_id.as_deref().map(str::to_string).ok_or_else(|| {
                    format!("Missing `unique_id` for event type \"{}\"", Self::full_name())
                })?,
                materialization: record.materialization.map(|v| v as i32),
                custom_materialization: record.custom_materialization.as_deref().map(str::to_string),
                node_type: record.node_type.map(|v| v as i32).ok_or_else(|| {
                    format!("Missing `node_type` for event type \"{}\"", Self::full_name())
                })?,
                node_outcome: record.node_outcome.map(|v| v as i32).ok_or_else(|| {
                    format!("Missing `node_outcome` for event type \"{}\"", Self::full_name())
                })?,
                node_error_type: record.node_error_type.map(|v| v as i32),
                node_cancel_reason: record.node_cancel_reason.map(|v| v as i32),
                node_skip_reason: record.node_skip_reason.map(|v| v as i32),
                dbt_core_event_code: record.dbt_core_event_code.as_deref().map(str::to_string),
                node_outcome_detail: record.json_payload.as_ref().map(|v| serde_json::from_str(v).map_err(|e| {
                    format!(
                        "Failed to deserialize `node_outcome_detail` in event type \"{}\" from JSON: {}",
                        Self::full_name(),
                        e
                    )
                })).transpose()?,
            }
        )
    }
}
