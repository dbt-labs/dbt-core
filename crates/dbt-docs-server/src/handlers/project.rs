use axum::Json;
use axum::extract::State;
use axum::response::{IntoResponse, Response};

use crate::handlers::json::{first_row_as_object, internal_error, not_found};
use crate::state::SharedState;

const PROJECT_SQL: &str = "SELECT \
    project_name AS name, \
    project_id, \
    description, \
    dbt_version, \
    adapter_type, \
    git_sha, \
    git_branch, \
    git_is_dirty \
FROM dbt.project LIMIT 1";

pub async fn get_project(State(state): State<SharedState>) -> Response {
    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || backend.query_arrow(PROJECT_SQL)).await;

    let batches = match result {
        Ok(Ok(b)) => b,
        Ok(Err(err)) => return internal_error(err.to_string()),
        Err(err) => return internal_error(err.to_string()),
    };
    match first_row_as_object(&batches) {
        Ok(Some(obj)) => Json(obj).into_response(),
        Ok(None) => not_found("no project metadata found in dbt.project".to_string()),
        Err(err) => internal_error(err.to_string()),
    }
}
