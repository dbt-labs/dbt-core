use axum::extract::State;
use axum::response::Response;

use crate::handlers::json::{internal_error, wrapped_list_response};
use crate::state::SharedState;

/// Tables that contribute file-bearing resources to the navigation tree.
///
/// Each entry is `(view_name, resource_type_literal)`. For `dbt.nodes` the
/// literal is `None` because the table carries its own `resource_type`
/// column; for every other table we project a constant so the UNION yields
/// a single coherent shape.
///
/// `dbt.semantic_models.original_file_path` is nullable on disk, so the
/// projection filters those rows out — they'd never be renderable in a
/// file tree anyway.
const SOURCES: &[(&str, Option<&str>)] = &[
    ("dbt.nodes", None),
    ("dbt.exposures", Some("exposure")),
    ("dbt.metrics", Some("metric")),
    ("dbt.macros", Some("macro")),
    ("dbt.semantic_models", Some("semantic_model")),
    ("dbt.groups", Some("group")),
    ("dbt.unit_tests", Some("unit_test")),
    ("dbt.docs", Some("doc")),
    ("dbt.saved_queries", Some("saved_query")),
];

/// `GET /api/v1/files` — flat list of every file-bearing resource across
/// all registered parquet tables.
///
/// Response shape:
/// ```json
/// {
///   "files": [
///     { "unique_id": "...", "name": "...", "resource_type": "...",
///       "package_name": "...", "original_file_path": "...",
///       "patch_path": "..." | null }
///   ],
///   "total": 1234
/// }
/// ```
///
/// Powers the sourdough `PaginatedFileTree` in dbt-docs-v2's LocatePane.
/// No pagination on the wire — the tree slices per folder client-side.
/// Missing parquet tables are silently excluded from the UNION via
/// `Backend::table_has_rows` so partial indexes still render the slice
/// they have.
pub async fn list_files(State(state): State<SharedState>) -> Response {
    let backend = state.providers.backend.clone();

    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let mut selects: Vec<String> = Vec::with_capacity(SOURCES.len());
        for (table, literal) in SOURCES {
            if !backend.table_has_rows(table) {
                continue;
            }
            selects.push(projection_for(table, *literal));
        }
        if selects.is_empty() {
            return Ok((0u64, Vec::new()));
        }

        let union_sql = selects.join(" UNION ALL ");
        let rows_sql = format!(
            "SELECT unique_id, name, resource_type, package_name, \
                    original_file_path, patch_path \
             FROM ({union_sql}) \
             ORDER BY package_name, original_file_path, name"
        );
        let count_sql = format!("SELECT count(*) FROM ({union_sql})");

        let total = backend
            .query_scalar(&count_sql)
            .ok_or_else(|| "files count query returned no rows".to_string())?
            .parse::<u64>()
            .map_err(|e| format!("could not parse file count: {e}"))?;
        let batches = backend.query_arrow(&rows_sql).map_err(|e| e.to_string())?;
        Ok((total, batches))
    })
    .await;

    let (total, batches) = match result {
        Ok(Ok(t)) => t,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    wrapped_list_response("files", &batches, &[("total", &total.to_string())])
}

fn projection_for(table: &str, literal: Option<&str>) -> String {
    let resource_type_expr = match literal {
        Some(lit) => format!("'{lit}' AS resource_type"),
        None => "resource_type".to_string(),
    };
    // Only `dbt.nodes` and `dbt.macros` carry `patch_path`; coalesce to NULL elsewhere.
    let patch_path_expr = match table {
        "dbt.nodes" | "dbt.macros" => "patch_path".to_string(),
        _ => "CAST(NULL AS VARCHAR) AS patch_path".to_string(),
    };
    // `dbt.semantic_models.original_file_path` is nullable; skip those rows.
    let filter = if table == "dbt.semantic_models" {
        " WHERE original_file_path IS NOT NULL"
    } else {
        ""
    };
    format!(
        "SELECT unique_id, name, {resource_type_expr}, package_name, \
                original_file_path, {patch_path_expr} \
         FROM {table}{filter}"
    )
}

#[cfg(test)]
#[path = "files_tests.rs"]
mod tests;
