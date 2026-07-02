use axum::body::Body;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::Response;
use serde::Serialize;

use crate::state::SharedState;

#[derive(Serialize)]
pub struct HealthResponse {
    pub ok: bool,
    pub version: &'static str,
    pub project_loaded: bool,
    pub generation: Option<String>,
}

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
