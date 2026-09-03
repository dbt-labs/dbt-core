//! The dbt information schema: a queryable parquet layer over dbt metadata.
//!
//! # Shape
//!
//! One flat, schema-prefixed parquet file per table (`dbt.models.parquet`,
//! `dbt_rt.run_results.parquet`, ...) plus a generated `views.sql`. Three
//! namespaces: `dbt` for project metadata, `dbt_rt` for runtime results, and
//! `dbt_internal` for tables that are not part of the public contract.
//!
//! # How it is produced
//!
//! Two materializers, selected by [`Materializer`]. Production uses
//! [`Materializer::Auto`]: DuckDB `COPY` from the epoch-view layer when a
//! driver is available, Arrow ingest-and-project otherwise (air-gapped CI).
//!
//! The Arrow path is two steps. The first is the existing metadata ingest, run
//! into a staging directory: it walks the epoch layers, applies the merge and
//! carry-forward rules, and produces one file per source table. The second
//! projects those files into the information schema shape — renaming, dropping
//! and splitting columns per [`schema::INFO_SCHEMA`]. The staging directory is
//! the flat index at `target/private/index` when one is already present: the
//! metadata ingest that builds the index and the one that feeds this projection
//! are the same code, so an index built by `--write-index` (or a prior run) is
//! reused via the delta path instead of re-ingesting the epochs. When there is
//! no index to reuse — `--no-write-index`, or a `parse` that never builds one —
//! the ingest runs into a private staging directory instead, so requesting the
//! information schema never materialises an index the caller opted out of.
//! Reusing the ingest verbatim either way keeps a single implementation of the
//! epoch and merge semantics. The projection step runs unconditionally on every
//! invocation, so removing the output directory is enough to force a rebuild
//! even when staging is up to date.
//!
//! The COPY path skips staging. It executes [`epoch_views::generate`] on one
//! in-memory connection and `COPY (SELECT * FROM <view>) TO '<file>'` per
//! table. Cold and steady cost the same: every run re-reads the epochs.
//!
//! Tables are described declaratively so the schema can be reviewed as data
//! rather than as code, and so output column types are derived from their
//! source rather than restated. See [`spec`].

// Gated: a measurement tool, not shipped library code. See info_schema/bench.rs.
#[cfg(any(test, feature = "bench"))]
pub mod bench;
mod copy;
pub mod epoch;
pub mod epoch_views;
pub mod parse_safe;
pub mod project;
pub mod schema;
pub mod spec;
pub mod views;

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::SchemaRef;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::IndexError;
use crate::db::{DBT_RT_TABLES, DBT_TABLES};
use crate::ingest::metadata_to_parquet::{bool_col, str_col, timestamp_micros_col};
use crate::ingest::{IngestState, ingest_from_metadata_direct, timings};

use schema::{DAG_RESOURCE_TYPES, INFO_SCHEMA};
use spec::{Src, TableSpec};

/// Directory name under the target directory.
pub const INFO_SCHEMA_DIR_NAME: &str = "info_schema";

/// Version of the information schema contract.
///
/// Carried three ways, so a consumer can find it wherever it is looking:
/// in the output path (`info_schema/v1/`), in `dbt.project.schema_version`,
/// and in every file's parquet key-value metadata under
/// [`INFO_SCHEMA_VERSION_KEY`]. Bumped when a column is removed or retyped;
/// adding a nullable column does not require a bump.
///
/// Versions coexist rather than replace: the writer only ever creates its own
/// version's directory and never removes another's.
pub const INFO_SCHEMA_VERSION: u32 = 1;

/// Parquet key-value metadata key holding [`INFO_SCHEMA_VERSION`].
pub const INFO_SCHEMA_VERSION_KEY: &str = "dbt:info-schema-version";

/// The directory this version's tables are written to.
pub fn versioned_dir(info_schema_dir: &Path) -> std::path::PathBuf {
    info_schema_dir.join(format!("v{INFO_SCHEMA_VERSION}"))
}

/// Fallback staging directory name, used when there is no flat index to reuse as
/// the intermediate. Also the tests' and benchmark's choice, which always build
/// into a throwaway workdir rather than a project's `target/private/index`.
///
/// Deliberately outside the output directory: it holds files in the source shape,
/// and a caller globbing the information schema must never pick them up.
pub const STAGING_DIR_NAME: &str = ".info_schema_staging";

/// How [`write_info_schema`] materializes parquet.
///
/// Production uses [`Materializer::Auto`]: DuckDB `COPY` from the epoch-view
/// layer when a driver loads, Arrow ingest-and-project otherwise. The bench
/// pins [`Materializer::Arrow`] and [`Materializer::Copy`] so the two can be
/// timed on the same corpus.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Materializer {
    /// DuckDB `COPY` from epoch views, falling back to Arrow if the driver is
    /// missing or `COPY` fails.
    #[default]
    Auto,
    /// Ingest epochs into staging parquet, then project. No DuckDB.
    Arrow,
    /// DuckDB `COPY` only. Errors if the driver is unavailable or a statement
    /// fails, so a measurement cannot silently time the fallback.
    Copy,
}

impl Materializer {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Arrow => "arrow",
            Self::Copy => "copy",
        }
    }
}

/// Build the information schema at `info_schema_dir` from the metadata at
/// `metadata_dir`, staging the intermediate source tables in `staging_dir`.
///
/// `staging_dir` holds the intermediate source-shaped tables (not the information
/// schema shape). Used only by the Arrow path: the caller passes the flat index
/// directory (`target/private/index`) when one already exists, so its ingest is
/// reused via the delta path rather than re-run; otherwise it passes a private
/// fallback directory. See
/// [`has_persisted_state`](crate::ingest::metadata_to_parquet::has_persisted_state).
/// The COPY path ignores `staging_dir`.
///
/// Tables land in this version's subdirectory, not in `info_schema_dir`
/// itself, so several versions can coexist under one root.
///
/// Returns the number of tables written.
pub fn write_info_schema(
    metadata_dir: &Path,
    info_schema_dir: &Path,
    staging_dir: &Path,
) -> Result<usize, IndexError> {
    write_info_schema_with(
        Materializer::Auto,
        metadata_dir,
        info_schema_dir,
        staging_dir,
    )
}

/// [`write_info_schema`] with an explicit materializer. The bench pins Arrow
/// vs Copy; production goes through [`write_info_schema`].
pub fn write_info_schema_with(
    how: Materializer,
    metadata_dir: &Path,
    info_schema_dir: &Path,
    staging_dir: &Path,
) -> Result<usize, IndexError> {
    match how {
        Materializer::Arrow => write_via_arrow(metadata_dir, info_schema_dir, staging_dir),
        Materializer::Copy => copy::write_via_copy(metadata_dir, info_schema_dir),
        Materializer::Auto => match copy::write_via_copy(metadata_dir, info_schema_dir) {
            Ok(n) => Ok(n),
            Err(_) => write_via_arrow(metadata_dir, info_schema_dir, staging_dir),
        },
    }
}

fn write_via_arrow(
    metadata_dir: &Path,
    info_schema_dir: &Path,
    staging_dir: &Path,
) -> Result<usize, IndexError> {
    let mut state = IngestState::default();
    ingest_from_metadata_direct(metadata_dir, staging_dir, &mut state)?;
    let out_dir = versioned_dir(info_schema_dir);
    let written = timings::time(timings::Stage::Projection, || {
        project_all(staging_dir, &out_dir)
    })?;
    timings::time(timings::Stage::ViewsSql, || {
        views::write_views_sql(&out_dir)
    })?;
    // `epoch_views.sql` — the same schema as views straight over the epoch files —
    // is deliberately not written here: it reads the private `target/private/metadata/`
    // layout and the `dbt_internal` epoch relations, which are not part of the
    // public contract, so it does not belong in the shipped info-schema directory.
    // The generator (`epoch_views::generate`) stays for the differential test and a
    // future serve path that wants to query metadata live without a conversion.
    Ok(written)
}

/// Source-table file name, picking the prefix the ingest step wrote it under.
fn source_file_name(table: &str) -> String {
    if DBT_RT_TABLES.contains(&table) && !DBT_TABLES.contains(&table) {
        format!("dbt_rt.{table}.parquet")
    } else {
        format!("dbt.{table}.parquet")
    }
}

/// Read a source table. A missing file is not an error: the corresponding
/// output table is simply empty.
fn read_source(staging_dir: &Path, table: &str) -> Vec<RecordBatch> {
    let path = staging_dir.join(source_file_name(table));
    let Ok(file) = std::fs::File::open(&path) else {
        return Vec::new();
    };
    let Ok(builder) = ParquetRecordBatchReaderBuilder::try_new(file) else {
        return Vec::new();
    };
    let Ok(reader) = builder.build() else {
        return Vec::new();
    };
    reader.flatten().collect()
}

/// Write one output table. Always writes the file, even with no rows, because
/// `views.sql` creates a view per table and a view over a missing file fails
/// every query against the database.
pub(super) fn write_output(
    dir: &Path,
    spec: &TableSpec,
    out: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<(), IndexError> {
    std::fs::create_dir_all(dir)?;
    write_parquet(&dir.join(spec.file_name()), out, batches)
}

/// Write `path` as zstd parquet with [`INFO_SCHEMA_VERSION_KEY`] in the file
/// metadata. Used by the Arrow writer and by the COPY path when DuckDB did not
/// stamp the KV itself.
pub(super) fn write_parquet(
    path: &Path,
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<(), IndexError> {
    let tmp = path.with_extension("parquet.tmp");
    // Stamp the version into the file itself, so a parquet file that has been
    // copied out of its versioned directory is still self-describing.
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(1).unwrap()))
        .set_key_value_metadata(Some(vec![parquet::file::metadata::KeyValue::new(
            INFO_SCHEMA_VERSION_KEY.to_string(),
            INFO_SCHEMA_VERSION.to_string(),
        )]))
        .build();
    let file = std::fs::File::create(&tmp)?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(schema), Some(props))
        .map_err(|e| IndexError::Other(format!("info schema writer: {e}")))?;
    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| IndexError::Other(format!("info schema write: {e}")))?;
    }
    writer
        .close()
        .map_err(|e| IndexError::Other(format!("info schema close: {e}")))?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Project every table in the schema.
fn project_all(staging_dir: &Path, out_dir: &Path) -> Result<usize, IndexError> {
    let mut count = 0;
    for spec in INFO_SCHEMA {
        let out = project::out_schema(spec)?;
        let batches = match spec.src {
            Src::Table(table) => {
                let src = read_source(staging_dir, table);
                let mut projected = project::project_table(spec, &src, &out)?;
                if spec.name == "project" {
                    fill_last_full_parse_at(staging_dir, &out, &mut projected)?;
                    fill_schema_version(&out, &mut projected)?;
                }
                projected
            }
            Src::Join { left, right, .. } => {
                let l = read_source(staging_dir, left);
                let r = read_source(staging_dir, right);
                project::project_join(spec, &l, &r, &out)?
            }
            Src::Own => build_own(staging_dir, spec, &out)?,
        };
        write_output(out_dir, spec, &out, &batches)?;
        count += 1;
    }
    Ok(count)
}

/// Rows for the tables that are assembled rather than projected.
fn build_own(
    staging_dir: &Path,
    spec: &TableSpec,
    out: &SchemaRef,
) -> Result<Vec<RecordBatch>, IndexError> {
    match spec.name {
        "project_vars" => build_project_vars(staging_dir, out),
        "dag_nodes" => build_dag_nodes(staging_dir, out),
        // No source of taxonomy data yet; the shape is published so it can be
        // filled without a schema change.
        "classifiers" => Ok(Vec::new()),
        other => Err(IndexError::Other(format!(
            "info schema: no builder for assembled table '{other}'"
        ))),
    }
}

/// The adapter's built-in packages, whose vars are not the user's.
///
/// Mirrors the package resolution order used when building the macro
/// namespace: the adapter package, its parent where one exists, then `dbt`.
fn internal_packages(adapter_type: Option<&str>) -> Vec<String> {
    let mut v = Vec::new();
    if let Some(adapter) = adapter_type {
        v.push(format!("dbt_{adapter}"));
        match adapter {
            "redshift" => v.push("dbt_postgres".to_string()),
            "databricks" => v.push("dbt_spark".to_string()),
            _ => {}
        }
    }
    v.push("dbt".to_string());
    v
}

type VarMap = BTreeMap<String, serde_json::Value>;

/// Render a var value the way `serde_json::Value` displays: a plain string
/// bare, anything else as JSON.
fn render_var_value(value: serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s,
        other => other.to_string(),
    }
}

/// Split a package's vars map into scalar variables and package-scoped nested
/// maps. A nested object is a scope only when its key is an installed package;
/// otherwise it stays an object-valued variable on the parent.
fn split_scoped_vars(map: VarMap, installed: &HashSet<String>) -> (VarMap, Vec<(String, VarMap)>) {
    let mut scalars = BTreeMap::new();
    let mut scopes = Vec::new();
    for (key, value) in map {
        if let serde_json::Value::Object(obj) = value {
            if installed.contains(&key) {
                scopes.push((key, obj.into_iter().collect()));
                continue;
            }
            scalars.insert(key, serde_json::Value::Object(obj));
        } else {
            scalars.insert(key, value);
        }
    }
    (scalars, scopes)
}

/// `dbt.project_vars`, one row per (project, variable).
///
/// The source table holds one row per package, with the package name in
/// `var_name` and that package's whole variable map encoded in `var_value`.
/// Un-nest it so each variable is its own row and the package it applies to is
/// its own column. Nested object keys that name an installed package become
/// their own `project_name` rows, inheriting the parent's scalar vars and
/// overlaying the nested map.
fn build_project_vars(staging_dir: &Path, out: &SchemaRef) -> Result<Vec<RecordBatch>, IndexError> {
    let adapter = read_source(staging_dir, "project")
        .first()
        .and_then(|b| str_col(b, "adapter_type").map(|c| c.value(0).to_string()));
    let skip = internal_packages(adapter.as_deref());

    let mut installed: HashSet<String> = HashSet::new();
    for batch in read_source(staging_dir, "packages") {
        let Some(col) = str_col(&batch, "package_name") else {
            continue;
        };
        for row in 0..batch.num_rows() {
            if !col.is_null(row) {
                installed.insert(col.value(row).to_string());
            }
        }
    }

    let mut projects: Vec<String> = Vec::new();
    let mut names: Vec<String> = Vec::new();
    let mut values: Vec<String> = Vec::new();
    let mut stamps: Vec<i64> = Vec::new();

    let mut emit = |package: &str, map: BTreeMap<String, serde_json::Value>, stamp: i64| {
        for (name, value) in map {
            projects.push(package.to_string());
            names.push(name);
            values.push(render_var_value(value));
            stamps.push(stamp);
        }
    };

    for batch in read_source(staging_dir, "project_vars") {
        let Some(pkg_col) = str_col(&batch, "var_name") else {
            continue;
        };
        let Some(val_col) = str_col(&batch, "var_value") else {
            continue;
        };
        let stamp_col = timestamp_micros_col(&batch, "ingested_at");

        for row in 0..batch.num_rows() {
            if pkg_col.is_null(row) || val_col.is_null(row) {
                continue;
            }
            let package = pkg_col.value(row);
            if skip.iter().any(|s| s == package) {
                continue;
            }
            let Ok(map) =
                serde_json::from_str::<BTreeMap<String, serde_json::Value>>(val_col.value(row))
            else {
                continue;
            };
            let stamp = stamp_col
                .filter(|c| !c.is_null(row))
                .map(|c| c.value(row))
                .unwrap_or(0);
            let (scalars, scopes) = split_scoped_vars(map, &installed);
            emit(package, scalars.clone(), stamp);
            for (scope_pkg, nested) in scopes {
                if skip.iter().any(|s| s == &scope_pkg) {
                    continue;
                }
                let mut inherited = scalars.clone();
                inherited.extend(nested);
                emit(&scope_pkg, inherited, stamp);
            }
        }
    }

    if projects.is_empty() {
        return Ok(Vec::new());
    }
    let cols: Vec<arrow_array::ArrayRef> = vec![
        Arc::new(StringArray::from(projects)),
        Arc::new(StringArray::from(names)),
        Arc::new(StringArray::from(values)),
        Arc::new(timestamps(stamps, out, 3)),
    ];
    Ok(vec![RecordBatch::try_new(Arc::clone(out), cols).map_err(
        |e| IndexError::Other(format!("info schema project_vars: {e}")),
    )?])
}

/// `dbt.dag_nodes`: every enabled resource that participates in the DAG.
///
/// Most resource types come from the node set, which carries `resource_type`
/// and `enabled`. Exposures, metrics and unit tests are only ever kept in their
/// own tables, so they are unioned in with their type supplied literally; those
/// tables carry `enabled` too, and a disabled one is dropped here just as a
/// disabled node is.
fn build_dag_nodes(staging_dir: &Path, out: &SchemaRef) -> Result<Vec<RecordBatch>, IndexError> {
    /// Resource types that live outside the node set, and the table holding each.
    const SIDE_TABLES: &[(&str, &str)] = &[
        ("exposures", "exposure"),
        ("metrics", "metric"),
        ("unit_tests", "unit_test"),
    ];

    let mut ids: Vec<String> = Vec::new();
    let mut types: Vec<String> = Vec::new();
    let mut stamps: Vec<i64> = Vec::new();

    /// `enabled` for one row, defaulting to true: a missing column or value
    /// means enabled, matching the config default.
    fn enabled_at(col: Option<&arrow_array::BooleanArray>, row: usize) -> bool {
        col.map(|c| c.is_null(row) || c.value(row)).unwrap_or(true)
    }

    /// `ingested_at` for one row, or 0 where the column or value is absent.
    fn stamp_at(col: Option<&arrow_array::TimestampMicrosecondArray>, row: usize) -> i64 {
        col.filter(|c| !c.is_null(row))
            .map(|c| c.value(row))
            .unwrap_or(0)
    }

    for batch in read_source(staging_dir, "nodes") {
        let Some(id_col) = str_col(&batch, "unique_id") else {
            continue;
        };
        let Some(rt_col) = str_col(&batch, "resource_type") else {
            continue;
        };
        let en_col = bool_col(&batch, "enabled");
        let ts_col = timestamp_micros_col(&batch, "ingested_at");
        for row in 0..batch.num_rows() {
            if id_col.is_null(row) || rt_col.is_null(row) {
                continue;
            }
            if !DAG_RESOURCE_TYPES.contains(&rt_col.value(row)) {
                continue;
            }
            if !enabled_at(en_col, row) {
                continue;
            }
            ids.push(id_col.value(row).to_string());
            types.push(rt_col.value(row).to_string());
            stamps.push(stamp_at(ts_col, row));
        }
    }

    for (table, resource_type) in SIDE_TABLES {
        if !DAG_RESOURCE_TYPES.contains(resource_type) {
            continue;
        }
        for batch in read_source(staging_dir, table) {
            let Some(id_col) = str_col(&batch, "unique_id") else {
                continue;
            };
            let en_col = bool_col(&batch, "enabled");
            let ts_col = timestamp_micros_col(&batch, "ingested_at");
            for row in 0..batch.num_rows() {
                if id_col.is_null(row) {
                    continue;
                }
                if !enabled_at(en_col, row) {
                    continue;
                }
                ids.push(id_col.value(row).to_string());
                types.push((*resource_type).to_string());
                stamps.push(stamp_at(ts_col, row));
            }
        }
    }

    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let cols: Vec<arrow_array::ArrayRef> = vec![
        Arc::new(StringArray::from(ids)),
        Arc::new(StringArray::from(types)),
        Arc::new(timestamps(stamps, out, 2)),
    ];
    Ok(vec![RecordBatch::try_new(Arc::clone(out), cols).map_err(
        |e| IndexError::Other(format!("info schema dag_nodes: {e}")),
    )?])
}

/// Build a timestamp array carrying the timezone declared for output column
/// `idx`, so it matches the schema exactly.
fn timestamps(
    values: Vec<i64>,
    out: &SchemaRef,
    idx: usize,
) -> arrow_array::TimestampMicrosecondArray {
    use arrow_schema::DataType;
    let arr = arrow_array::TimestampMicrosecondArray::from(values);
    match out.field(idx).data_type() {
        DataType::Timestamp(_, Some(tz)) => arr.with_timezone(tz.to_string()),
        _ => arr,
    }
}

/// Fill `dbt.project.schema_version` with [`INFO_SCHEMA_VERSION`].
fn fill_schema_version(out: &SchemaRef, batches: &mut [RecordBatch]) -> Result<(), IndexError> {
    let Ok(idx) = out.index_of("schema_version") else {
        return Ok(());
    };
    for batch in batches.iter_mut() {
        let mut cols: Vec<arrow_array::ArrayRef> = batch.columns().to_vec();
        cols[idx] = Arc::new(arrow_array::Int64Array::from(vec![
            i64::from(
                INFO_SCHEMA_VERSION
            );
            batch.num_rows()
        ]));
        *batch = RecordBatch::try_new(Arc::clone(out), cols)
            .map_err(|e| IndexError::Other(format!("info schema schema_version: {e}")))?;
    }
    Ok(())
}

/// Replace `dbt.project.last_full_parse_at` with the timestamp of the last
/// full parse, which the source keeps in its own single-row table.
fn fill_last_full_parse_at(
    staging_dir: &Path,
    out: &SchemaRef,
    batches: &mut [RecordBatch],
) -> Result<(), IndexError> {
    let Some(idx) = out.index_of("last_full_parse_at").ok() else {
        return Ok(());
    };
    let stamp = read_source(staging_dir, "generation")
        .first()
        .and_then(|b| {
            let i = b.schema().index_of("ingested_at").ok()?;
            let col = b
                .column(i)
                .as_any()
                .downcast_ref::<arrow_array::TimestampMicrosecondArray>()?;
            (b.num_rows() > 0 && !col.is_null(0)).then(|| col.value(0))
        });
    let Some(stamp) = stamp else {
        return Ok(());
    };

    for batch in batches.iter_mut() {
        let mut cols: Vec<arrow_array::ArrayRef> = batch.columns().to_vec();
        cols[idx] = Arc::new(timestamps(vec![stamp; batch.num_rows()], out, idx));
        *batch = RecordBatch::try_new(Arc::clone(out), cols)
            .map_err(|e| IndexError::Other(format!("info schema project: {e}")))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_copy;
#[cfg(test)]
mod tests_epoch;
#[cfg(test)]
mod tests_parse_safe;
