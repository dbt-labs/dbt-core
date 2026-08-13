//! SPA asset delivery.
//!
//! Two sources, in priority order:
//!
//! 1. **A generated site directory on disk** ([`serve_site_dir`]). This is what
//!    `dbt docs serve` uses. Only this source has the `window.__DBT_DOCS__`
//!    bootstrap injected and the `data/*.parquet` artifacts alongside it, so it
//!    is the only one that produces a working app.
//! 2. **The bundle embedded via `rust-embed`** ([`serve_assets`]), gated on the
//!    `embed-ui` feature. A fallback for a server started with no generated site.
//!
//! Both answer unknown paths with `index.html` so client-side routes survive a
//! reload — belt and braces, since the SPA uses hash routes and a Vite base of
//! `./`, so the document path never changes as the user navigates and no host
//! rewrite is required. That is what lets the generated directory be served from
//! any subpath by anything, including a plain file server.

use std::path::{Component, Path, PathBuf};

use axum::{
    body::Body,
    http::{StatusCode, Uri, header},
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

/// Build a 200 with a guessed content type. Used by both asset sources, so it is
/// not gated on `embed-ui` — `serve_site_dir` needs it either way.
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

/// Join a request path onto `root`, refusing to escape it.
///
/// `--host` can be `0.0.0.0`, so this is reachable from off-box and a request for
/// `/../../etc/passwd` must not resolve. Rejects rather than sanitizes: anything
/// that is not a plain relative name is a request we have no reason to honor.
/// Returns `None` for a traversal attempt, and the root itself for an empty path.
pub(crate) fn resolve_within(root: &Path, request_path: &str) -> Option<PathBuf> {
    let trimmed = request_path.trim_start_matches('/');
    let mut resolved = root.to_path_buf();

    for segment in trimmed.split('/') {
        if segment.is_empty() || segment == "." {
            continue;
        }
        // Reject backslashes outright rather than leaving it to `Path`. Otherwise
        // the same request means different things per platform: on unix
        // `..\..\etc\passwd` is one odd-but-harmless filename, while on Windows it
        // is a traversal. Refusing it everywhere makes this function's guarantee
        // independent of where it runs.
        if segment.contains('\\') {
            return None;
        }
        // Then reject anything that is not a single plain name — `..`, absolute
        // segments, Windows path prefixes.
        let mut components = Path::new(segment).components();
        match (components.next(), components.next()) {
            (Some(Component::Normal(name)), None) => resolved.push(name),
            _ => return None,
        }
    }
    Some(resolved)
}

/// Serve a generated site directory, falling back to its `index.html`.
///
/// Falls through to the embedded bundle when the directory has no `index.html`,
/// so a stale or half-written site still shows the app rather than nothing.
pub async fn serve_site_dir(site_dir: &Path, uri: Uri) -> Response {
    if let Some(path) = resolve_within(site_dir, uri.path())
        && let Ok(bytes) = tokio::fs::read(&path).await
    {
        return asset_response(&path.to_string_lossy(), bytes, None);
    }

    let index = site_dir.join("index.html");
    if let Ok(bytes) = tokio::fs::read(&index).await {
        return asset_response("index.html", bytes, None);
    }

    // Without `embed-ui` this is the 501 stub, which is the honest answer when
    // there is neither a generated site nor an embedded bundle.
    serve_assets(uri).await
}

/// Stub used when the `embed-ui` feature is disabled. Returns 501.
#[cfg(not(feature = "embed-ui"))]
pub async fn serve_assets(_uri: Uri) -> Response {
    Response::builder()
        .status(StatusCode::NOT_IMPLEMENTED)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(
            "dbt-docs-server built without a UI backend (enable `embed-ui`)",
        ))
        .expect("valid stub response")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "embed-ui")]
    #[test]
    fn normalize_handles_root_and_bare_paths() {
        assert_eq!(normalize_path("/"), "index.html");
        assert_eq!(normalize_path(""), "index.html");
        assert_eq!(normalize_path("/assets/x.js"), "assets/x.js");
        assert_eq!(normalize_path("favicon.ico"), "favicon.ico");
    }

    fn resolved(request_path: &str) -> Option<String> {
        resolve_within(Path::new("/site"), request_path)
            .map(|p| p.to_string_lossy().replace('\\', "/"))
    }

    #[test]
    fn resolves_paths_inside_the_site_directory() {
        assert_eq!(
            resolved("/assets/index.js").as_deref(),
            Some("/site/assets/index.js")
        );
        assert_eq!(
            resolved("/data/dbt.nodes.parquet").as_deref(),
            Some("/site/data/dbt.nodes.parquet")
        );
        // Empty and redundant segments collapse to the root, which reads as a
        // directory and so falls through to index.html.
        assert_eq!(resolved("/").as_deref(), Some("/site"));
        assert_eq!(
            resolved("//assets//x.js").as_deref(),
            Some("/site/assets/x.js")
        );
        assert_eq!(
            resolved("/./assets/x.js").as_deref(),
            Some("/site/assets/x.js")
        );
    }

    #[test]
    fn refuses_to_escape_the_site_directory() {
        // `--host` can be 0.0.0.0, so these are reachable from off-box.
        for attempt in [
            "/../etc/passwd",
            "/assets/../../etc/passwd",
            "/..",
            "/assets/..",
            "/a/../../b",
        ] {
            assert_eq!(resolved(attempt), None, "{attempt} should be refused");
        }
    }

    #[test]
    fn refuses_backslashes_on_every_platform() {
        // These only traverse on Windows, where `\` is a separator. Refusing them
        // on unix too keeps one rule instead of a platform-dependent one, so this
        // test means the same thing wherever it runs.
        for attempt in [
            r"/..\..\etc\passwd",
            r"/\\server\share",
            r"/assets\..\..\secret",
        ] {
            assert_eq!(resolved(attempt), None, "{attempt} should be refused");
        }
    }
}
