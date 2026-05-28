//! Shared base types for typed detail handlers (`GET /api/v1/<type>/:id`).
//!
//! Every typed `*Detail` struct composes `NodeBase` via `#[serde(flatten)]`
//! so the deferred generic `GET /api/v1/nodes/:id` dispatcher can route on
//! the `unique_id` prefix and delegate to a typed handler without
//! duplicating SQL queries across handlers.
//!
//! `ExecutionInfo` and `EdgeRef` live here so each new `*Detail` handler
//! reuses the same JSON shape without re-declaring them.

use arrow_array::{Array, BooleanArray, Float64Array, ListArray, RecordBatch, StringArray};
use serde::Serialize;

/// Fields common to every resource type's detail response.
#[derive(Serialize)]
pub struct NodeBase {
    pub unique_id: String,
    pub name: String,
    pub resource_type: String,
    pub package_name: Option<String>,
    pub description: Option<String>,
    pub original_file_path: Option<String>,
}

/// Per-resource last-run snapshot. `null` on the parent response when no
/// `dbt_rt.run_results` row exists for the resource.
#[derive(Serialize)]
pub struct ExecutionInfo {
    pub status: Option<String>,
    pub execution_time: Option<f64>,
    pub completed_at: Option<String>,
}

/// One inline edge in a detail response's `depends_on` / `referenced_by`.
#[derive(Serialize)]
pub struct EdgeRef {
    pub unique_id: String,
    pub edge_type: String,
}

// ---------------------------------------------------------------------------
// Arrow extraction helpers
// ---------------------------------------------------------------------------

/// Downcast a column to `StringArray` by name. Panics on schema mismatch —
/// a missing or mistyped column is a handler/schema bug, not a runtime
/// condition to swallow.
pub fn str_col<'a>(batch: &'a RecordBatch, name: &'static str) -> &'a StringArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("column '{name}' missing from batch"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("column '{name}' is not a StringArray"))
}

/// Downcast a column to `BooleanArray` by name. See [`str_col`].
pub fn bool_col<'a>(batch: &'a RecordBatch, name: &'static str) -> &'a BooleanArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("column '{name}' missing from batch"))
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap_or_else(|| panic!("column '{name}' is not a BooleanArray"))
}

/// Null-aware string read: `None` when the cell is null, otherwise the
/// owned `String`.
pub fn opt_str(col: &StringArray, i: usize) -> Option<String> {
    if col.is_null(i) {
        None
    } else {
        Some(col.value(i).to_owned())
    }
}

/// Extract a `List<Utf8>` column's first row into a `Vec<String>`. Returns
/// `[]` for any of: missing column, empty batch, null cell, or non-string
/// child array — a missing parquet column shouldn't 500 the endpoint.
pub fn extract_str_list(batch: &RecordBatch, col_name: &'static str) -> Vec<String> {
    let Some(col) = batch.column_by_name(col_name) else {
        return vec![];
    };
    if batch.num_rows() == 0 || col.is_null(0) {
        return vec![];
    }
    let Some(list) = col.as_any().downcast_ref::<ListArray>() else {
        return vec![];
    };
    let inner = list.value(0);
    let Some(strings) = inner.as_any().downcast_ref::<StringArray>() else {
        return vec![];
    };
    (0..strings.len())
        .filter(|&i| !strings.is_null(i))
        .map(|i| strings.value(i).to_owned())
        .collect()
}

/// Extract all rows of an edges query (`SELECT ... AS unique_id, edge_type`)
/// into `Vec<EdgeRef>`. Used for `depends_on` and `referenced_by` on every
/// `*Detail` response that exposes lineage.
pub fn extract_edge_refs(batches: &[RecordBatch]) -> Vec<EdgeRef> {
    let mut rows = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let uid = str_col(batch, "unique_id");
        let etype = str_col(batch, "edge_type");
        for i in 0..batch.num_rows() {
            rows.push(EdgeRef {
                unique_id: uid.value(i).to_owned(),
                edge_type: etype.value(i).to_owned(),
            });
        }
    }
    rows
}

/// Extract the first row of a run_results query into an `ExecutionInfo`.
/// Expects columns: `status: Utf8`, `execution_time: Float64`,
/// `completed_at: Utf8`. Returns `None` when no row is present; the handler
/// maps that to JSON `null` on the parent.
pub fn extract_execution_info(batches: &[RecordBatch]) -> Option<ExecutionInfo> {
    let batch = batches.iter().find(|b| b.num_rows() > 0)?;
    let status_col = batch
        .column_by_name("status")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let exec_time_col = batch
        .column_by_name("execution_time")
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
    let completed_at_col = batch
        .column_by_name("completed_at")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    Some(ExecutionInfo {
        status: status_col.and_then(|c| {
            if c.is_null(0) {
                None
            } else {
                Some(c.value(0).to_owned())
            }
        }),
        execution_time: exec_time_col
            .and_then(|c| if c.is_null(0) { None } else { Some(c.value(0)) }),
        completed_at: completed_at_col.and_then(|c| {
            if c.is_null(0) {
                None
            } else {
                Some(c.value(0).to_owned())
            }
        }),
    })
}
