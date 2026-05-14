use axum::Json;
use axum::extract::State;

use crate::state::{Capabilities, SharedState};

pub async fn get_capabilities(State(state): State<SharedState>) -> Json<Capabilities> {
    Json(state.capabilities.clone())
}
