use axum::Json;
use axum::extract::State;
use serde::Serialize;

use crate::state::SharedState;

#[derive(Serialize)]
pub struct IdentityResponse {
    pub is_logged_in: bool,
    pub analytics_enabled: bool,
}

pub async fn get_identity(State(state): State<SharedState>) -> Json<IdentityResponse> {
    Json(IdentityResponse {
        is_logged_in: false,
        analytics_enabled: !state.do_not_track,
    })
}

#[cfg(test)]
#[path = "identity_tests.rs"]
mod tests;
