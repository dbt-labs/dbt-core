use std::sync::Arc;

use axum::extract::State;

use super::get_identity;
use crate::providers::Providers;
use crate::state::AppState;

fn make_state(do_not_track: bool) -> Arc<AppState> {
    Arc::new(
        AppState::new(
            std::path::PathBuf::from("/tmp"),
            Providers::default(),
            false,
        )
        .with_do_not_track(do_not_track),
    )
}

#[tokio::test]
async fn analytics_enabled_by_default() {
    let state = make_state(false);
    let response = get_identity(State(state)).await;
    assert!(response.analytics_enabled);
    assert!(!response.is_logged_in);
}

#[tokio::test]
async fn analytics_disabled_when_do_not_track() {
    let state = make_state(true);
    let response = get_identity(State(state)).await;
    assert!(!response.analytics_enabled);
    assert!(!response.is_logged_in);
}

#[tokio::test]
async fn is_logged_in_always_false() {
    let state = make_state(false);
    let response = get_identity(State(state)).await;
    assert!(!response.is_logged_in);
}
