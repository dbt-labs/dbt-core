//! SPA asset delivery.
//!
//! Files are baked into the binary at build time via `rust-embed`,
//! gated on the `embed-ui` feature.
//!
//! The dbt-ui Vite build emits assets with `--base=./`, so `index.html`
//! references them as `./assets/*` and the browser resolves them relative
//! to the page URL. The server lookup just trims the leading `/`.

use axum::{
    body::Body,
    http::{StatusCode, header},
    response::Response,
};

/// Normalize an incoming request path to a bundle-relative path.
/// Leading `/` stripped; empty result -> `index.html`.
#[cfg(feature = "embed-ui")]
pub(crate) fn normalize_path(path: &str) -> String {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        "index.html".to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(feature = "embed-ui")]
pub(crate) fn asset_response(path: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Response {
    let mime: String = content_type.map(|s| s.to_string()).unwrap_or_else(|| {
        mime_guess::from_path(path)
            .first_or_octet_stream()
            .as_ref()
            .to_string()
    });
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, mime)
        .body(Body::from(bytes))
        .expect("valid asset response")
}

#[cfg(feature = "embed-ui")]
pub use crate::embed::serve_assets;

/// Stub used when the `embed-ui` feature is disabled. Returns 501.
#[cfg(not(feature = "embed-ui"))]
pub async fn serve_assets(_uri: axum::http::Uri) -> Response {
    Response::builder()
        .status(StatusCode::NOT_IMPLEMENTED)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(
            "dbt-docs-server built without a UI backend (enable `embed-ui`)",
        ))
        .expect("valid stub response")
}

#[cfg(all(test, feature = "embed-ui"))]
mod tests {
    use super::*;

    #[test]
    fn normalize_handles_root_and_bare_paths() {
        assert_eq!(normalize_path("/"), "index.html");
        assert_eq!(normalize_path(""), "index.html");
        assert_eq!(normalize_path("/assets/x.js"), "assets/x.js");
        assert_eq!(normalize_path("favicon.ico"), "favicon.ico");
    }
}
