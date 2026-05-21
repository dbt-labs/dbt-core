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

    let dist = Path::new(&manifest_dir).join("web").join("dist");
    let placeholder = Path::new(&manifest_dir).join("web").join("placeholder");

    let chosen = if dist.join("index.html").exists() {
        println!("cargo:rerun-if-changed=web/dist");
        dist
    } else {
        println!(
            "cargo:warning=web/dist/index.html not found — embedding placeholder UI. Run `cd fs/sa/crates/dbt-docs-server/web && npm install && npm run build` to ship the real SPA."
        );
        placeholder
    };

    println!("cargo:rustc-env=DOCS_SERVER_WEB_DIST={}", chosen.display());
}
