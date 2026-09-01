//! COPY-path coverage that needs a DuckDB driver.
//!
//! Empty-metadata tests in `tests.rs` stay on Arrow so they pass without a
//! driver. This module checks the COPY writer when one is present.

use super::schema::INFO_SCHEMA;

/// The COPY path, when a DuckDB driver is present, writes the same versioned
/// tree as Arrow: every table, `views.sql`, the parquet KV, and not
/// `epoch_views.sql`.
#[test]
fn copy_path_writes_versioned_tables_when_duckdb_available() {
    if crate::db::Db::open_memory().is_err() {
        eprintln!("skipping: DuckDB driver unavailable");
        return;
    }
    let metadata = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    let staging = tempfile::tempdir().unwrap();
    super::write_info_schema_with(
        super::Materializer::Copy,
        metadata.path(),
        root.path(),
        staging.path(),
    )
    .unwrap();

    let versioned = super::versioned_dir(root.path());
    assert!(versioned.join("views.sql").exists());
    assert!(
        !versioned.join("epoch_views.sql").exists(),
        "epoch_views.sql reads the private metadata layout and must not ship"
    );
    let want = super::INFO_SCHEMA_VERSION.to_string();
    for table in INFO_SCHEMA {
        assert!(
            versioned.join(table.file_name()).exists(),
            "{} was not written",
            table.qualified_name()
        );
        let kv = out_kv(&versioned, &table.file_name());
        assert!(
            kv.iter()
                .any(|(k, v)| k == super::INFO_SCHEMA_VERSION_KEY && v.as_deref() == Some(&want)),
            "{} is missing {}",
            table.qualified_name(),
            super::INFO_SCHEMA_VERSION_KEY
        );
    }
    // COPY does not stage.
    assert!(
        std::fs::read_dir(staging.path())
            .map(|mut e| e.next().is_none())
            .unwrap_or(true)
    );
}

fn out_kv(dir: &std::path::Path, file: &str) -> Vec<(String, Option<String>)> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let f = std::fs::File::open(dir.join(file)).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
    builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .map(|kvs| {
            kvs.iter()
                .map(|kv| (kv.key.clone(), kv.value.clone()))
                .collect()
        })
        .unwrap_or_default()
}
