use axum::Json;
use axum::extract::State;

use crate::state::{DistInfo, SharedState};

pub async fn get_distribution(State(state): State<SharedState>) -> Json<DistInfo> {
    Json(state.dist_info())
}
