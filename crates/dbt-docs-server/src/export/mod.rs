//! Static site export.
//!
//! Turns an index directory of parquet artifacts into a directory of plain files
//! that any file host can serve: no process, no API, no state. The browser loads
//! DuckDB-WASM from a CDN and queries the parquet directly, so everything the
//! former REST API computed server-side is computed client-side instead.
//!
//! Layout of `<out>`:
//!
//! ```text
//! index.html            SPA entry, with window.__DBT_DOCS__ injected
//! assets/               hashed JS/CSS from the committed Vite build
//! data/dbt.*.parquet    the artifact set (see `artifacts`)
//! ```
//!
//! Two properties the browser depends on:
//!
//! 1. **An artifact may be absent, and absence is data.** `--write-index` writes no
//!    file for a table with no rows, and the export copies the index rather than
//!    filling it in — so a project that has never run has no `dbt_rt.run_results`,
//!    one with no sources no `dbt.source_freshness`, and so on. The client declares
//!    an empty relation for those (`EMPTY_RELATION_DDL` in `duckdb/engine.ts`), which
//!    is why its queries need no missing-table variant. For column lineage the
//!    absence *is* the gating signal. What the client cannot absorb is a missing
//!    artifact answered with something other than nothing: see `is_navigation_path`
//!    in `assets.rs`.
//! 2. **Nothing outside `data/` describes the data.** There is no manifest to
//!    keep in sync; artifact names are a constant in the client, project
//!    identity comes from `dbt.project.parquet`, staleness from
//!    `dbt.generation.parquet`, and column-lineage availability from whether
//!    `dbt.column_lineage.parquet` loaded with rows.

mod bootstrap;

use std::path::{Path, PathBuf};

use crate::providers::Providers;

pub use bootstrap::{SiteBootstrap, SiteTelemetry};

/// Default CDN base the browser loads DuckDB-WASM from.
///
/// Pinned: `selectBundle` derives the `.wasm` and worker URLs from the package
/// version, so an unpinned base would silently change the engine under a site
/// that was tested against a specific one. Overridable via
/// `--duckdb-cdn-base` for hosts mirroring jsDelivr.
pub const DEFAULT_DUCKDB_CDN_BASE: &str = "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.32.0";

#[derive(Debug, thiserror::Error)]
pub enum ExportError {
    #[error("failed to write {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "no index to export\n\n\
         Expected parquet artifacts in {index_dir}\n\n\
         Run `dbt build` or `dbt docs generate` (without `--no-compile`), then retry.\n\
         `--static-analysis strict` on that build produces column-level lineage; \
         without it the site simply omits that feature."
    )]
    NoIndex { index_dir: PathBuf },
    #[error(
        "the index in {index_dir} has no nodes, so the site would be empty\n\n\
         The artifacts are there but hold no rows, which usually means the index \
         was written partially.\n\n\
         Run `dbt build` or `dbt docs generate` (without `--no-compile`), then retry."
    )]
    EmptyIndex { index_dir: PathBuf },
    #[error(
        "this build has no embedded docs UI (built without the `embed-ui` feature), \
         so there is no site to write"
    )]
    NoEmbeddedUi,
}

/// Inputs for [`export_site`].
#[derive(Debug, Clone)]
pub struct ExportOptions {
    /// Directory holding `dbt.*.parquet` / `dbt_rt.*.parquet`.
    pub index_dir: PathBuf,
    /// Directory to write the site into. Created if absent.
    pub output_dir: PathBuf,
    /// Override for [`DEFAULT_DUCKDB_CDN_BASE`].
    pub duckdb_cdn_base: Option<String>,
    /// Whether the browser may emit analytics. Resolved here rather than in the
    /// browser because consent lives in the project and profile, which only the
    /// machine running the export can read.
    pub analytics_enabled: bool,
}

/// What an export produced. Reported to the user and asserted on in tests.
#[derive(Debug, Clone)]
pub struct ExportSummary {
    pub output_dir: PathBuf,
    /// Where the site reads its parquet from, relative to `index.html`.
    pub data_dir: String,
    /// Whether the index carries column-level lineage. Derived from the artifact,
    /// which is also how the browser decides — one signal, not two.
    pub has_column_lineage: bool,
    /// How many parquet files were copied. Zero in the common case: the site reads
    /// the index where it already lies.
    pub copied_artifacts: usize,
}

/// The directory, relative to the site root, that the browser reads parquet from.
///
/// The site reads the index artifacts as written — there is no exported copy, so this
/// is the index directory itself and its contract is the index's contract.
pub const DATA_DIR: &str = "index";

/// Write the site to `options.output_dir`.
///
/// Writes the SPA and its bootstrap, and nothing else in the common case: the browser
/// reads `<output_dir>/index/*.parquet`, which is where `--write-index` already put
/// them. No artifact is derived, projected, or copied, so the index is the only
/// artifact contract the site depends on.
pub fn export_site(
    providers: &Providers,
    options: &ExportOptions,
) -> Result<ExportSummary, ExportError> {
    if !index_dir_has_artifacts(&options.index_dir) {
        return Err(ExportError::NoIndex {
            index_dir: options.index_dir.clone(),
        });
    }

    // Refuse to write a site with no nodes in it. A partially written index yields a
    // full-looking set of files and a site that renders nothing; `dbt.nodes` being
    // empty always means the input was wrong rather than that the project lacks a
    // resource type, so it is the honest place to fail.
    let node_count = providers
        .backend
        .query_scalar("SELECT COUNT(*) FROM dbt.nodes")
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or(0);
    if node_count == 0 {
        return Err(ExportError::EmptyIndex {
            index_dir: options.index_dir.clone(),
        });
    }

    // The site addresses its data at `index/` relative to itself. That is already true
    // when writing into the target directory, which is the default. Writing anywhere
    // else means the index has to come along, or the site has no data to read.
    let in_place = options.index_dir == options.output_dir.join(DATA_DIR);
    let copied_artifacts = if in_place {
        0
    } else {
        copy_index(&options.index_dir, &options.output_dir.join(DATA_DIR))?
    };

    write_spa(options, providers)?;

    Ok(ExportSummary {
        output_dir: options.output_dir.clone(),
        data_dir: format!("{DATA_DIR}/"),
        has_column_lineage: index_has_column_lineage(&options.index_dir),
        copied_artifacts,
    })
}

/// Copy the index's parquet into the site, byte for byte.
///
/// Deliberately a copy and not a projection: the files keep their names, columns, and
/// contents, so a self-contained site and an in-place one read the same contract.
fn copy_index(index_dir: &Path, dest_dir: &Path) -> Result<usize, ExportError> {
    create_dir_all(dest_dir)?;
    let entries = std::fs::read_dir(index_dir).map_err(|source| ExportError::Io {
        path: index_dir.to_path_buf(),
        source,
    })?;

    let mut copied = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("parquet") {
            continue;
        }
        let Some(name) = path.file_name() else {
            continue;
        };
        let dest = dest_dir.join(name);
        std::fs::copy(&path, &dest).map_err(|source| ExportError::Io { path: dest, source })?;
        copied += 1;
    }
    Ok(copied)
}

/// Whether the index carries column-level lineage.
///
/// Presence of the artifact is the whole signal, matching what the browser checks.
/// A row-less file counts as absent: `--write-index` without static analysis can leave
/// the table empty, and an empty table is not a feature.
fn index_has_column_lineage(index_dir: &Path) -> bool {
    let path = index_dir.join("dbt.column_lineage.parquet");
    // A parquet holding only a schema is a few hundred bytes; one holding edges is
    // not. Cheaper than opening the file, and only drives a progress message.
    std::fs::metadata(&path).is_ok_and(|m| m.len() > 2_048)
}

/// Copy the embedded SPA out, injecting the bootstrap into `index.html`.
#[cfg(feature = "embed-ui")]
fn write_spa(options: &ExportOptions, providers: &Providers) -> Result<(), ExportError> {
    let bootstrap = SiteBootstrap::new(providers, options);
    let mut wrote_index = false;

    for (path, bytes) in crate::embed::iter_assets() {
        let dest = options.output_dir.join(&path);
        if let Some(parent) = dest.parent() {
            create_dir_all(parent)?;
        }
        // Written straight through, one file at a time. Only `index.html` is
        // materialized, because injecting the bootstrap rewrites it.
        let result = if path == "index.html" {
            wrote_index = true;
            let injected = bootstrap.inject(&String::from_utf8_lossy(&bytes));
            std::fs::write(&dest, injected)
        } else {
            std::fs::write(&dest, &bytes)
        };
        result.map_err(|source| ExportError::Io { path: dest, source })?;
    }

    if !wrote_index {
        return Err(ExportError::NoEmbeddedUi);
    }
    Ok(())
}

#[cfg(not(feature = "embed-ui"))]
fn write_spa(_options: &ExportOptions, _providers: &Providers) -> Result<(), ExportError> {
    Err(ExportError::NoEmbeddedUi)
}

/// Whether `dir` holds at least one parquet file.
///
/// Mirrors `AppState::compute_project_loaded` — presence of any artifact is the
/// signal that there is something to export. Public so callers can check before
/// paying to open a backend, and report [`ExportError::NoIndex`] themselves.
pub fn index_dir_has_artifacts(dir: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    entries.flatten().any(|entry| {
        entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.ends_with(".parquet"))
    })
}

fn create_dir_all(path: &Path) -> Result<(), ExportError> {
    std::fs::create_dir_all(path).map_err(|source| ExportError::Io {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(test)]
#[path = "export_tests.rs"]
mod tests;
