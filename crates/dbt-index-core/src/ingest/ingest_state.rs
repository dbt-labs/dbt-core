use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::SystemTime;

// ---------------------------------------------------------------------------
// Directory layout under target/metadata/
// ---------------------------------------------------------------------------

pub const PARSE_NODES_SUBDIR: &str = "parse/nodes";
pub const PARSE_COLUMNS_SUBDIR: &str = "parse/columns";
pub const PARSE_ALIVE: &str = "parse/alive.parquet";
pub const PARSE_PROJECT: &str = "parse/project.parquet";
pub const PARSE_RESOLVER_STATE: &str = "parse/resolver_state.parquet";
/// Written only on cold start. Its mtime signals the last full parse; compile rows
/// with an older `ingested_at` are stale and should be ignored.
pub const PARSE_GENERATION: &str = "parse/generation.parquet";
pub const COMPILE_NODES_SUBDIR: &str = "compile/nodes";
pub const COMPILE_COLUMNS_SUBDIR: &str = "compile/columns";
pub const COMPILE_CLL_SUBDIR: &str = "compile/column_lineage";
pub const CATALOG_COLUMNS_SUBDIR: &str = "catalog/columns";
pub const RUN_INVOCATIONS_SUBDIR: &str = "run/invocations";
pub const RUN_RESULTS_SUBDIR: &str = "run/results";
pub const RUN_FRESHNESS_SUBDIR: &str = "run/freshness";

// ---------------------------------------------------------------------------
// IngestState — tracks what epochs have been applied
// ---------------------------------------------------------------------------

/// Tracks what has been applied from `target/metadata/` into DuckDB.
///
/// ## Delta decision logic (per epoch directory)
///
/// Let `last` = stored last epoch number, `curr_max` = max epoch on disk.
///
/// | Condition              | Meaning                          | Action          |
/// |------------------------|----------------------------------|-----------------|
/// | `last == u32::MAX`     | First run, no state yet          | Full reload     |
/// | `curr_max < last`      | Compaction reset epoch numbering | Full reload     |
/// | `curr_max == last`     | No new epochs (may still have deletions) | Deletions only |
/// | `curr_max > last`      | New incremental epochs available | Delta load      |
///
/// Deletions are always computed via `alive_ids` diff regardless of epoch state:
/// a node removed from `alive.parquet` is deleted even if no epoch was written.
///
/// ## Trigger
/// `apply_delta` is a no-op unless `parse/alive.parquet` mtime changes — every
/// parse (cold start, incremental, compaction) rewrites alive.parquet last.
#[derive(Debug, Default)]
pub struct IngestState {
    /// mtime of `parse/alive.parquet` at last apply — the primary freshness signal.
    pub alive_mtime: Option<SystemTime>,
    /// Last epoch number applied per subdirectory. Defaults to `u32::MAX` (never seen).
    pub last_epoch: HashMap<&'static str, u32>,
    /// Directory where crack_epochs writes flat delta parquet files.
    pub index_dir: Option<PathBuf>,
    /// Alive unique_ids after the last apply. Diff against current alive.parquet
    /// gives the deleted set — no DuckDB table scan required.
    pub alive_ids: HashSet<String>,
}

impl IngestState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn last_epoch_for(&self, subdir: &'static str) -> u32 {
        self.last_epoch.get(subdir).copied().unwrap_or(u32::MAX)
    }

    pub fn set_epoch(&mut self, subdir: &'static str, epoch: u32) {
        self.last_epoch.insert(subdir, epoch);
    }
}
