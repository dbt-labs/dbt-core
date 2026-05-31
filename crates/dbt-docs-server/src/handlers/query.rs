//! Ad-hoc SQL query endpoint.
//!
//! `POST /api/v1/query` accepts `{"sql": "..."}`, runs it through the
//! [`crate::providers::Backend`], and returns `{columns, rows, row_count,
//! truncated, elapsed_ms}`. Rows are capped at [`MAX_ROWS`] to keep the
//! response bounded — anything beyond that is dropped and the
//! `truncated` flag is set.
//!
//! `GET /api/v1/tables` lists the registered parquet-backed views plus
//! their column types so the editor can populate autocomplete and a
//! sidebar without each keystroke round-tripping for schema info.
//!
//! Both endpoints are read-only and run synchronously against the shared
//! `Backend` (the DuckDB views are over read-only parquet files), so SQL
//! injection risk is bounded to "can read other parquet views this user
//! already has access to."

use std::collections::BTreeMap;
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::handlers::json::{batches_as_value_array, internal_error};
use crate::state::SharedState;

const MAX_ROWS: usize = 1000;

#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub sql: String,
}

#[derive(Debug, Serialize)]
pub struct QueryColumn {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub columns: Vec<QueryColumn>,
    pub rows: serde_json::Value,
    pub row_count: usize,
    pub truncated: bool,
    pub elapsed_ms: u128,
}

/// `POST /api/v1/query` — run arbitrary SQL against the parquet views.
///
/// Errors from DuckDB come back as `400 {"error": "..."}` so the editor can
/// surface them without treating them as a transport failure.
pub async fn run_query(
    State(state): State<SharedState>,
    Json(req): Json<QueryRequest>,
) -> Response {
    let sql = req.sql.trim().to_string();
    if sql.is_empty() {
        return query_error("empty SQL");
    }

    let backend = state.providers.backend.clone();
    let started = Instant::now();
    let result = tokio::task::spawn_blocking(move || backend.query_arrow(&sql)).await;
    let elapsed_ms = started.elapsed().as_millis();

    let batches = match result {
        Ok(Ok(batches)) => batches,
        Ok(Err(err)) => return query_error(&err.to_string()),
        Err(err) => return internal_error(err.to_string()),
    };

    match build_response(&batches, elapsed_ms) {
        Ok(resp) => Json(resp).into_response(),
        Err(err) => internal_error(err),
    }
}

fn build_response(batches: &[RecordBatch], elapsed_ms: u128) -> Result<QueryResponse, String> {
    let columns = batches
        .first()
        .map(|b| {
            b.schema()
                .fields()
                .iter()
                .map(|f| QueryColumn {
                    name: f.name().clone(),
                    data_type: f.data_type().to_string(),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let truncated = total_rows > MAX_ROWS;

    let mut selected: Vec<RecordBatch> = Vec::new();
    let mut remaining = MAX_ROWS;
    for batch in batches {
        if remaining == 0 {
            break;
        }
        if batch.num_rows() <= remaining {
            selected.push(batch.clone());
            remaining -= batch.num_rows();
        } else {
            selected.push(batch.slice(0, remaining));
            remaining = 0;
        }
    }

    let row_count = selected.iter().map(|b| b.num_rows()).sum();
    let rows = batches_as_value_array(&selected).map_err(|e| e.to_string())?;

    Ok(QueryResponse {
        columns,
        rows,
        row_count,
        truncated,
        elapsed_ms,
    })
}

fn query_error(msg: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({ "error": msg })),
    )
        .into_response()
}

#[derive(Debug, Serialize)]
pub struct TableInfo {
    pub schema: String,
    pub name: String,
    pub columns: Vec<QueryColumn>,
}

/// `GET /api/v1/tables` — list of registered parquet views plus their
/// columns. The editor uses this for autocomplete + the schema sidebar.
pub async fn list_tables(State(state): State<SharedState>) -> Response {
    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || {
        backend.query_arrow(
            "SELECT table_schema, table_name, column_name, data_type, ordinal_position \
             FROM information_schema.columns \
             WHERE table_schema IN ('dbt', 'dbt_rt') \
             ORDER BY table_schema, table_name, ordinal_position",
        )
    })
    .await;

    let batches = match result {
        Ok(Ok(b)) => b,
        Ok(Err(err)) => return internal_error(err.to_string()),
        Err(err) => return internal_error(err.to_string()),
    };

    match collect_tables(&batches) {
        Ok(tables) => Json(tables).into_response(),
        Err(err) => internal_error(err),
    }
}

fn collect_tables(batches: &[RecordBatch]) -> Result<Vec<TableInfo>, String> {
    let mut grouped: BTreeMap<(String, String), Vec<QueryColumn>> = BTreeMap::new();
    for batch in batches {
        let schema = batch
            .column(0)
            .as_string_opt::<i32>()
            .ok_or_else(|| "table_schema not Utf8".to_string())?;
        let name = batch
            .column(1)
            .as_string_opt::<i32>()
            .ok_or_else(|| "table_name not Utf8".to_string())?;
        let column = batch
            .column(2)
            .as_string_opt::<i32>()
            .ok_or_else(|| "column_name not Utf8".to_string())?;
        let data_type = batch
            .column(3)
            .as_string_opt::<i32>()
            .ok_or_else(|| "data_type not Utf8".to_string())?;
        for i in 0..batch.num_rows() {
            let key = (schema.value(i).to_string(), name.value(i).to_string());
            grouped.entry(key).or_default().push(QueryColumn {
                name: column.value(i).to_string(),
                data_type: data_type.value(i).to_string(),
            });
        }
    }
    Ok(grouped
        .into_iter()
        .map(|((schema, name), columns)| TableInfo {
            schema,
            name,
            columns,
        })
        .collect())
}
