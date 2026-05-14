#![allow(clippy::disallowed_methods)] // RustEmbed generates calls to std::path::Path::canonicalize

use axum::{
    body::Body,
    http::{StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use rust_embed::RustEmbed;

// Path resolved at build time by `build.rs`: `web/dist/` if `npm run build`
// has been run, otherwise the checked-in `web/placeholder/` directory.
#[derive(RustEmbed)]
#[folder = "$DOCS_SERVER_WEB_DIST/"]
struct Assets;

/// Fallback handler that serves the embedded SPA.
///
/// - `/` → `index.html`
/// - any other path that exists in the bundle → that file
/// - anything else → fall back to `index.html` (so SPA hash-routes work)
pub async fn serve_assets(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    match Assets::get(path) {
        Some(file) => asset_response(path, file.data.into_owned()),
        None => match Assets::get("index.html") {
            Some(file) => asset_response("index.html", file.data.into_owned()),
            None => (
                StatusCode::NOT_FOUND,
                "dbt docs SPA bundle not found. Build the web app: `npm install && npm run build` in fs/sa/crates/dbt-docs-server/web/.",
            )
                .into_response(),
        },
    }
}

fn asset_response(path: &str, bytes: Vec<u8>) -> Response {
    let mime = mime_guess::from_path(path).first_or_octet_stream();
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, mime.as_ref())
        .body(Body::from(bytes))
        .expect("valid asset response")
}
