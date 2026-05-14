//! Choose where `rust-embed` should read the SPA bundle from.
//!
//! - If `web/dist/index.html` exists (i.e. someone ran `npm run build`),
//!   point `rust-embed` at the real bundle.
//! - Otherwise, point at the checked-in `web/placeholder/` directory which
//!   contains a tiny page telling the user how to build the SPA. This lets
//!   `cargo build` produce a working binary (working API + placeholder UI)
//!   even without Node.js or `GITHUB_TOKEN`.
//!
//! `rust-embed` reads the `DOCS_SERVER_WEB_DIST` env var via its
//! `interpolate-folder-path` feature, so the path lives in `cargo:rustc-env`
//! and never in source.

use std::path::Path;

fn main() {
    let manifest_dir =
        std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR set by cargo");

    // Re-run when either the real dist or the placeholder changes. Cargo
    // watches the directories recursively for content changes.
    println!("cargo:rerun-if-changed=web/dist");
    println!("cargo:rerun-if-changed=web/placeholder");
    println!("cargo:rerun-if-changed=build.rs");

    let dist = Path::new(&manifest_dir).join("web").join("dist");
    let placeholder = Path::new(&manifest_dir).join("web").join("placeholder");

    let chosen = if dist.join("index.html").exists() {
        dist
    } else {
        println!(
            "cargo:warning=web/dist/index.html not found — embedding placeholder UI. Run `cd fs/sa/crates/dbt-docs-server/web && npm install && npm run build` to ship the real SPA."
        );
        placeholder
    };

    println!("cargo:rustc-env=DOCS_SERVER_WEB_DIST={}", chosen.display());
}
