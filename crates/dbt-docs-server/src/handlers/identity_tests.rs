use std::sync::Arc;

use axum::extract::State;

use super::get_identity;
use crate::providers::{DistInfo, DistInfoProvider, Providers, TelemetryHydration};
use crate::state::AppState;

/// A `DistInfoProvider` double with a fixed `is_logged_in`.
struct FakeDistInfo(bool);

impl DistInfoProvider for FakeDistInfo {
    fn dist_info(&self) -> DistInfo {
        DistInfo {
            name: "oss".to_string(),
            version: "unused",
            is_logged_in: self.0,
        }
    }
    fn telemetry_hydration(&self) -> TelemetryHydration {
        TelemetryHydration {
            is_logged_in: self.0,
            ..Default::default()
        }
    }
}

fn make_state(do_not_track: bool) -> Arc<AppState> {
    Arc::new(
        AppState::new(
            std::path::PathBuf::from("/tmp"),
            Providers::default(),
            false,
            true,
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
async fn is_logged_in_false_by_oss_default() {
    // OSS default provider reports not-logged-in.
    let state = make_state(false);
    let response = get_identity(State(state)).await;
    assert!(!response.is_logged_in);
}

#[tokio::test]
async fn is_logged_in_propagates_from_provider() {
    let providers = Providers {
        dist_info: Arc::new(FakeDistInfo(true)),
        ..Providers::default()
    };
    let state = Arc::new(
        AppState::new(std::path::PathBuf::from("/tmp"), providers, false, true)
            .with_do_not_track(false),
    );
    let response = get_identity(State(state)).await;
    assert!(response.is_logged_in);
}

#[tokio::test]
async fn yaml_opt_out_disables_analytics() {
    let state = Arc::new(
        AppState::new(
            std::path::PathBuf::from("/tmp"),
            Providers::default(),
            false,
            false,
        )
        .with_do_not_track(false),
    );
    let response = get_identity(State(state)).await;
    assert!(!response.analytics_enabled);
}

#[tokio::test]
async fn yaml_opt_in_keeps_analytics() {
    let state = Arc::new(
        AppState::new(
            std::path::PathBuf::from("/tmp"),
            Providers::default(),
            false,
            true,
        )
        .with_do_not_track(false),
    );
    let response = get_identity(State(state)).await;
    assert!(response.analytics_enabled);
}

#[tokio::test]
async fn do_not_track_overrides_yaml_consent() {
    let state = Arc::new(
        AppState::new(
            std::path::PathBuf::from("/tmp"),
            Providers::default(),
            false,
            true,
        )
        .with_do_not_track(true),
    );
    let response = get_identity(State(state)).await;
    assert!(!response.analytics_enabled);
}
