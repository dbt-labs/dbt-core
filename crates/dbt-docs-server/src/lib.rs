//! HTTP server that powers dbt docs v2.
//!
//! Serves an embedded SPA plus a JSON API backed by parquet artifacts
//! produced by `dbt --use-index`. The SA crate itself depends on no
//! proprietary code: all surfaces that interact with the artifact store or
//! perform proprietary analysis (column lineage, sample data, etc.) sit
//! behind dyn-compatible traits in [`providers`]. The proprietary
//! distribution wires in DuckDB-backed implementations via
//! `dbt-docs-server-impl`.
//!
//! The CLI entry is `dbt docs serve`; see `crates/dbt-cli/src/main.rs` for
//! how this crate is invoked.

use std::path::PathBuf;
use std::sync::Arc;

use dbt_clap_core::DocsServeArgs;

mod embed;
mod handlers;
pub mod providers;
mod server;
pub mod state;

pub use providers::Providers;
pub use server::run_with_args;
pub use state::Capabilities;

/// Resolve the directory containing parquet artifacts.
///
/// Order of resolution:
/// 1. `args.target_path` (if provided) → expects `<target_path>/index/` to exist.
/// 2. `./target/index/` in the current working directory.
pub fn resolve_index_dir(args: &DocsServeArgs) -> PathBuf {
    let target = args
        .target_path
        .clone()
        .unwrap_or_else(|| PathBuf::from("./target"));
    target.join("index")
}

/// Convenience entry that just wraps args in an `Arc`. Mostly useful for
/// tests; binaries should call [`run_with_args`] directly.
pub async fn run(args: DocsServeArgs, providers: Providers) -> std::io::Result<()> {
    run_with_args(Arc::new(args), providers).await
}
