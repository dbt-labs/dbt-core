use axum::Json;
use axum::extract::State;
use serde::Serialize;

use crate::state::SharedState;

#[derive(Serialize)]
pub struct HealthResponse {
    pub ok: bool,
    pub version: &'static str,
    pub index_dir: String,
}

pub async fn get_health(State(state): State<SharedState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        ok: true,
        version: state.server_version,
        index_dir: state.index_dir.display().to_string(),
    })
}
