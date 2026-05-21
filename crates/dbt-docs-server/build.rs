//! Build-time wiring for the SPA bundle.
//!
//! Points `rust-embed` at `web/dist/` (a committed build of the dbt-ui
//! `dbt-docs-v2` SPA) via the `DOCS_SERVER_WEB_DIST` env var.

use std::path::Path;

const WATCHED_WEB_INPUTS: &[&str] = &[
    "web/index.html",
    "web/package.json",
    "web/package-lock.json",
    "web/postcss.config.cjs",
    "web/tailwind.config.cjs",
    "web/tsconfig.json",
    "web/tsconfig.node.json",
    "web/vite.config.ts",
    "web/src",
    "web/placeholder",
];

fn main() {
    let manifest_dir =
        std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR set by cargo");

    // Re-run for checked-in frontend inputs. `web/dist` is gitignored, so only
    // watch it after it exists; otherwise Cargo treats the missing path as dirty
    // on every invocation.
    for path in WATCHED_WEB_INPUTS {
        println!("cargo:rerun-if-changed={path}");
    }
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=web/dist");

    if std::env::var_os("CARGO_FEATURE_EMBED_UI").is_none() {
        println!(
            "cargo:warning=dbt-docs-server: building with no UI backend; `serve_assets` will return 501. \
             Enable `embed-ui` (default) for a working server."
        );
        return;
    }

    let dist = Path::new(&manifest_dir).join("web").join("dist");
    println!("cargo:rustc-env=DOCS_SERVER_WEB_DIST={}", dist.display());
}
