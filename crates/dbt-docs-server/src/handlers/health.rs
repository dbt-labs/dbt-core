use axum::body::Body;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::Response;
use serde::Serialize;
use utoipa::ToSchema;

use crate::state::SharedState;

#[derive(Serialize, ToSchema)]
pub struct HealthResponse {
    pub ok: bool,
    pub version: &'static str,
    /// Whether a dbt project (parquet index) is currently loaded.
    pub project_loaded: bool,
    /// Present only when a project is loaded; identifies the loaded index generation.
    pub generation: Option<String>,
}

/// Report server liveness and whether a project index is currently loaded.
#[utoipa::path(
    get,
    path = "/api/v1/health",
    responses((status = 200, description = "Server is healthy", body = HealthResponse)),
    tag = "health"
)]
pub async fn get_health(State(state): State<SharedState>) -> Response {
    let resp = HealthResponse {
        ok: true,
        version: state.server_version(),
        project_loaded: state.project_loaded,
        generation: state.generation.clone(),
    };

    let body = match serde_json::to_vec(&resp) {
        Ok(body) => body,
        Err(e) => {
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(e.to_string()))
                .expect("valid error response");
        }
    };

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(generation) = &resp.generation {
        builder = builder.header("X-Docs-Generation", generation);
    }
    builder.body(Body::from(body)).expect("valid response")
}

#[cfg(test)]
#[path = "health_tests.rs"]
mod health_tests;
