#![allow(clippy::disallowed_methods)] // RustEmbed generates calls to std::path::Path::canonicalize

use axum::{
    http::{StatusCode, Uri},
    response::{IntoResponse, Response},
};
use rust_embed::RustEmbed;

use crate::assets::{asset_response, normalize_path};

// Path resolved at build time by `build.rs` from `web/dist/` (a
// developer-managed symlink to a built `dbt-ui/apps/metadata/dbt-docs-v2/dist`).
#[derive(RustEmbed)]
#[folder = "$DOCS_SERVER_WEB_DIST/"]
struct Assets;

/// Fallback handler for the embedded SPA.
///
/// Tries the requested file, then falls back to `index.html` so SPA hash
/// routes resolve client-side.
pub async fn serve_assets(uri: Uri) -> Response {
    let path = normalize_path(uri.path());

    if let Some(file) = Assets::get(&path) {
        return asset_response(&path, file.data.into_owned(), None);
    }
    if let Some(file) = Assets::get("index.html") {
        return asset_response("index.html", file.data.into_owned(), None);
    }
    (StatusCode::NOT_FOUND, "dbt docs SPA bundle is empty").into_response()
}
