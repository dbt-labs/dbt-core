use std::sync::Arc;

use axum::extract::State;

use super::get_capabilities;
use crate::providers::Providers;
use crate::state::AppState;

fn make_state(has_dbt_state: bool) -> Arc<AppState> {
    Arc::new(AppState::new(
        std::path::PathBuf::from("/tmp"),
        Providers::default(),
        has_dbt_state,
        true,
    ))
}

#[tokio::test]
async fn capabilities_state_off() {
    let state = make_state(false);
    let response = get_capabilities(State(state)).await;
    assert!(!response.has_dbt_state);
    assert!(!response.has_column_lineage);
}

#[tokio::test]
async fn capabilities_state_on() {
    let state = make_state(true);
    let response = get_capabilities(State(state)).await;
    assert!(response.has_dbt_state);
    assert!(!response.has_column_lineage);
}
