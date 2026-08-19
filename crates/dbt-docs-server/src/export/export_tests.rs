use std::sync::Arc;

use arrow_array::RecordBatch;
use dbt_index_core::{Backend, BackendError};

use super::*;

/// Backend that answers only the probe the export makes.
///
/// The export issues no SQL now — it copies files at most — so the only thing a
/// backend is asked is whether the index has any nodes.
struct MockBackend {
    node_count: u64,
}

impl MockBackend {
    fn new() -> Self {
        Self { node_count: 6 }
    }

    /// An index whose artifacts exist but hold no nodes.
    fn empty_index() -> Self {
        Self { node_count: 0 }
    }
}

impl Backend for MockBackend {
    fn is_available(&self) -> bool {
        true
    }

    fn query_scalar(&self, sql: &str) -> Option<String> {
        if sql.contains("COUNT(*) FROM dbt.nodes") {
            return Some(self.node_count.to_string());
        }
        None
    }

    fn query_arrow(&self, _sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
        Ok(vec![])
    }
}

struct Harness {
    dir: tempfile::TempDir,
    index_dir: PathBuf,
    output_dir: PathBuf,
}

impl Harness {
    /// An index directory holding one parquet per named table.
    ///
    /// `output_dir` is the index's parent, mirroring the real layout: the site is
    /// written to the target directory and reads `index/` beside itself.
    fn in_place(tables: &[&str]) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let output_dir = dir.path().join("target");
        let index_dir = output_dir.join(DATA_DIR);
        std::fs::create_dir_all(&index_dir).unwrap();
        for table in tables {
            std::fs::write(index_dir.join(format!("{table}.parquet")), b"parquet-stub").unwrap();
        }
        Self {
            dir,
            index_dir,
            output_dir,
        }
    }

    /// The same index, but writing the site somewhere else entirely.
    fn out_of_place(tables: &[&str]) -> Self {
        let mut harness = Self::in_place(tables);
        harness.output_dir = harness.dir.path().join("elsewhere");
        harness
    }

    fn options(&self) -> ExportOptions {
        ExportOptions {
            index_dir: self.index_dir.clone(),
            output_dir: self.output_dir.clone(),
            duckdb_cdn_base: None,
            analytics_enabled: false,
        }
    }
}

fn providers(backend: Arc<MockBackend>) -> Providers {
    Providers {
        backend,
        ..Providers::default()
    }
}

/// Run an export and hand back the result.
///
/// Under `cargo test` the SPA step fails — `web/dist/` is a build artifact, not a
/// fixture — so tests assert on what happens before it, or on guards that reject
/// before anything is written at all.
fn export(harness: &Harness, backend: Arc<MockBackend>) -> Result<ExportSummary, ExportError> {
    export_site(&providers(backend), &harness.options())
}

#[test]
fn missing_index_is_reported_before_anything_is_written() {
    let harness = Harness::in_place(&[]);
    std::fs::remove_dir_all(&harness.index_dir).unwrap();

    let err = export(&harness, Arc::new(MockBackend::new())).unwrap_err();
    assert!(matches!(err, ExportError::NoIndex { .. }), "{err:?}");
    assert!(
        !harness.output_dir.join("index.html").exists(),
        "nothing should be written when there is no index"
    );
}

#[test]
fn an_empty_index_dir_is_not_an_index() {
    // The directory exists but holds no parquet — the state after a run that never
    // wrote one.
    let harness = Harness::in_place(&[]);

    let err = export(&harness, Arc::new(MockBackend::new())).unwrap_err();
    assert!(matches!(err, ExportError::NoIndex { .. }), "{err:?}");
}

#[test]
fn an_index_with_no_nodes_is_refused() {
    // Artifacts present but holding no rows: a partially written index. Shipping a
    // site that renders nothing is worse than failing.
    let harness = Harness::in_place(&["dbt.nodes"]);

    let err = export(&harness, Arc::new(MockBackend::empty_index())).unwrap_err();
    assert!(matches!(err, ExportError::EmptyIndex { .. }), "{err:?}");
}

#[test]
fn an_in_place_index_is_read_where_it_lies() {
    let harness = Harness::in_place(&["dbt.nodes", "dbt.edges"]);
    let before = std::fs::read_dir(&harness.index_dir).unwrap().count();

    let _ = export(&harness, Arc::new(MockBackend::new()));

    assert_eq!(
        std::fs::read_dir(&harness.index_dir).unwrap().count(),
        before,
        "the index must not gain files"
    );
    assert!(
        !harness.output_dir.join("docs_data").exists(),
        "no derived data directory should be created"
    );
}

#[test]
fn writing_elsewhere_copies_the_index_verbatim() {
    // A self-contained site still needs the data, and it travels as an exact copy —
    // same names, same bytes — so both layouts read one contract.
    let tables = ["dbt.nodes", "dbt.edges", "dbt_rt.run_results"];
    let harness = Harness::out_of_place(&tables);

    let _ = export(&harness, Arc::new(MockBackend::new()));

    let dest = harness.output_dir.join(DATA_DIR);
    for table in tables {
        let copied = dest.join(format!("{table}.parquet"));
        assert!(copied.exists(), "{table} was not copied");
        assert_eq!(
            std::fs::read(&copied).unwrap(),
            std::fs::read(harness.index_dir.join(format!("{table}.parquet"))).unwrap(),
            "{table} was not copied byte for byte"
        );
    }
}

#[test]
fn non_parquet_files_are_left_behind_when_copying() {
    // `views.sql` and friends are ingest bookkeeping; the browser has no use for them.
    let harness = Harness::out_of_place(&["dbt.nodes"]);
    std::fs::write(harness.index_dir.join("views.sql"), b"CREATE VIEW x").unwrap();

    let _ = export(&harness, Arc::new(MockBackend::new()));

    assert!(
        !harness.output_dir.join(DATA_DIR).join("views.sql").exists(),
        "only parquet should travel"
    );
}

#[test]
fn column_lineage_is_reported_from_the_artifact_alone() {
    // Presence is the whole signal, and the same one the browser reads. A row-less
    // file counts as absent: an empty table is not a feature.
    let harness = Harness::in_place(&["dbt.nodes"]);
    let lineage = harness.index_dir.join("dbt.column_lineage.parquet");

    assert!(!index_has_column_lineage(&harness.index_dir));

    std::fs::write(&lineage, vec![0u8; 512]).unwrap();
    assert!(
        !index_has_column_lineage(&harness.index_dir),
        "a schema-only file is not lineage"
    );

    std::fs::write(&lineage, vec![0u8; 8_192]).unwrap();
    assert!(index_has_column_lineage(&harness.index_dir));
}
