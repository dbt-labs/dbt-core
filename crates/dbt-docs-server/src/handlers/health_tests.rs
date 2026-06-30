use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::State;
use serde_json::Value;

use super::get_health;
use crate::providers::Providers;
use crate::state::AppState;

fn make_state(index_dir: PathBuf) -> Arc<AppState> {
    Arc::new(AppState::new(index_dir, Providers::default(), false, true))
}

/// Read the response body into a `serde_json::Value`, returning the
/// `X-Docs-Generation` header value alongside it (if present).
async fn read_response(response: axum::response::Response) -> (Option<String>, Value) {
    let header = response
        .headers()
        .get("X-Docs-Generation")
        .map(|v| v.to_str().unwrap().to_string());
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    (header, value)
}

#[tokio::test]
async fn health_empty_start() {
    // A directory with no parquet files: server is up but no index loaded.
    let dir = std::env::temp_dir();
    let state = make_state(dir);
    let response = get_health(State(state)).await;
    let (header, body) = read_response(response).await;

    assert_eq!(body["ok"], Value::Bool(true));
    assert!(body["version"].as_str().is_some_and(|v| !v.is_empty()));
    assert_eq!(body["project_loaded"], Value::Bool(false));
    assert_eq!(body["generation"], Value::Null);
    assert!(header.is_none(), "no X-Docs-Generation on empty-start");
    // Regression guard: the absolute index_dir path must not leak.
    assert!(body.get("index_dir").is_none());
}

#[tokio::test]
async fn health_loaded() {
    // A directory containing a parquet file: an index is loaded.
    let dir = std::env::temp_dir().join(format!("dbt-docs-health-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("nodes.parquet"), b"dummy").unwrap();

    let state = make_state(dir.clone());
    let response = get_health(State(state)).await;
    let (header, body) = read_response(response).await;

    std::fs::remove_dir_all(&dir).ok();

    assert_eq!(body["ok"], Value::Bool(true));
    assert!(body["version"].as_str().is_some_and(|v| !v.is_empty()));
    assert_eq!(body["project_loaded"], Value::Bool(true));

    let generation = body["generation"]
        .as_str()
        .expect("generation should be Some when loaded");
    assert!(
        chrono::DateTime::parse_from_rfc3339(generation).is_ok(),
        "generation must parse as RFC3339: {generation}"
    );
    assert_eq!(
        header.as_deref(),
        Some(generation),
        "X-Docs-Generation header must equal body generation"
    );
    assert!(body.get("index_dir").is_none());
}
