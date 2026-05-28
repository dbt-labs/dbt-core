//! Helpers for converting Arrow record batches to JSON HTTP responses.
//!
//! All conversion goes through `arrow_json::ArrayWriter` exactly once,
//! writing UTF-8 directly into a `Vec<u8>` that becomes the HTTP body. No
//! intermediate `Vec<serde_json::Value>` is materialized at any point.
//!
//! For phase-2a queries (single rows, ≤1000 rows of node summaries) this
//! buffers the entire response in memory before sending. When endpoints
//! that return larger results land (column lineage, full edge dump), add a
//! streaming variant here that flushes batches as they arrive — the
//! `IndexBackend` trait should grow `query_arrow_stream` at the same time.

use std::io::Write as _;

use arrow_array::RecordBatch;
use axum::Json;
use axum::body::Body;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

use crate::providers::BackendError;

/// Write `batches` as a JSON array `[{...}, {...}]` directly into the
/// response body. Used by single-resource endpoints whose top-level shape
/// is the array itself.
pub fn batches_as_json_array(batches: &[RecordBatch]) -> Result<Vec<u8>, BackendError> {
    let mut buf = Vec::with_capacity(estimate_size(batches));
    let mut writer = arrow_json::ArrayWriter::new(&mut buf);
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        writer
            .write(batch)
            .map_err(|e| BackendError::Shape(e.to_string()))?;
    }
    writer
        .finish()
        .map_err(|e| BackendError::Shape(e.to_string()))?;
    if buf.is_empty() {
        // ArrayWriter writes nothing if no batches were appended; emit an
        // explicit empty array so callers get valid JSON.
        buf.extend_from_slice(b"[]");
    }
    Ok(buf)
}

/// Render `batches` as `{"<field>": <array>, ...extra}` directly. Streams
/// the array body via `ArrayWriter` then closes the object with `extra`
/// fields appended literally as JSON.
///
/// Used for endpoints like `/api/v1/nodes` whose shape wraps the list with
/// metadata (`truncated`). Keeps us from re-serializing the array.
pub fn wrapped_list_response(
    field: &str,
    batches: &[RecordBatch],
    extra_json: &[(&str, &str)],
) -> Response {
    let mut buf = Vec::with_capacity(estimate_size(batches) + 64);
    if let Err(e) = write!(&mut buf, r#"{{"{field}":"#) {
        return internal_error(e.to_string());
    }
    {
        let mut writer = arrow_json::ArrayWriter::new(&mut buf);
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            if let Err(e) = writer.write(batch) {
                return internal_error(format!("arrow→json: {e}"));
            }
        }
        if let Err(e) = writer.finish() {
            return internal_error(format!("arrow→json: {e}"));
        }
    }
    if buf.ends_with(b":") {
        // No batches written: ArrayWriter emitted nothing.
        buf.extend_from_slice(b"[]");
    }
    for (k, v) in extra_json {
        if let Err(e) = write!(&mut buf, r#","{k}":{v}"#) {
            return internal_error(e.to_string());
        }
    }
    buf.push(b'}');
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(buf))
        .expect("valid response")
}

/// For "expect ≤1 row" queries: convert the first row of the first
/// non-empty batch into a `serde_json::Value::Object`, or return `None`.
///
/// Materializes one row's worth of JSON — fine for `/project` and node
/// detail. Don't use this for list endpoints.
pub fn first_row_as_object(
    batches: &[RecordBatch],
) -> Result<Option<serde_json::Value>, BackendError> {
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let one = batch.slice(0, 1);
        let mut buf = Vec::with_capacity(256);
        let mut writer = arrow_json::ArrayWriter::new(&mut buf);
        writer
            .write(&one)
            .map_err(|e| BackendError::Shape(e.to_string()))?;
        writer
            .finish()
            .map_err(|e| BackendError::Shape(e.to_string()))?;
        let mut arr: serde_json::Value =
            serde_json::from_slice(&buf).map_err(|e| BackendError::Shape(e.to_string()))?;
        if let Some(arr_mut) = arr.as_array_mut() {
            return Ok(arr_mut.pop());
        }
        return Ok(None);
    }
    Ok(None)
}

/// Convert all batches into a single `serde_json::Value::Array`. Used by
/// the node detail handler to embed columns/edges as fields on the parent
/// object. Same per-row cost as `first_row_as_object`; only call for
/// bounded result sets.
pub fn batches_as_value_array(batches: &[RecordBatch]) -> Result<serde_json::Value, BackendError> {
    let buf = batches_as_json_array(batches)?;
    serde_json::from_slice(&buf).map_err(|e| BackendError::Shape(e.to_string()))
}

fn estimate_size(batches: &[RecordBatch]) -> usize {
    // Rough: ~96 bytes per cell. Tunable; just an initial allocation.
    batches
        .iter()
        .map(|b| b.num_rows() * b.num_columns() * 96)
        .sum::<usize>()
        .max(64)
}

pub fn internal_error(msg: String) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({"error": msg})),
    )
        .into_response()
}

pub fn bad_request(msg: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({"error": msg})),
    )
        .into_response()
}

/// 400 with a stable machine-readable `code` and a human-readable `message`.
/// Use for endpoints whose contracts document specific error codes (e.g. `GET
/// /api/v1/search`), so clients can branch on `code` without parsing `message`.
pub fn bad_request_coded(code: &str, message: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({"code": code, "message": message})),
    )
        .into_response()
}

pub fn not_found(msg: String) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({"error": msg})),
    )
        .into_response()
}

/// Parse a JSON-string parquet column into a `serde_json::Value`, falling
/// back to `Value::Null` on parse failure.
///
/// JSON-string parquet columns (`meta`, `config`, and other nested-shape
/// columns) are deserialized handler-side so the response surfaces nested
/// objects rather than escaped JSON strings. A malformed blob logs a
/// warning and emits `null` — never bubbles up to the client.
pub fn json_parse_or_null(s: Option<&str>) -> serde_json::Value {
    let Some(text) = s else {
        return serde_json::Value::Null;
    };
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return serde_json::Value::Null;
    }
    match serde_json::from_str::<serde_json::Value>(trimmed) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, raw = %text, "json_parse_or_null: parse failed");
            serde_json::Value::Null
        }
    }
}
