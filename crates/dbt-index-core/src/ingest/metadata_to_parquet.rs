//! High-performance fusion ingest: reads target/metadata/ parquet with Arrow,
//! writes the index parquet directly with IndexWriter — no DuckDB involved.
//!
//! This is the CLI path for `dbt-index ingest --fusion`. The serve path
//! (`dbt-index serve --fusion`) uses `metadata_to_duckdb::apply_delta` instead,
//! which keeps an in-memory DuckDB so every query gets a live queryable DB.
//!
//! Speed: parquet→parquet with Arrow = no SQL parse/exec overhead.
//! Expected: ~200ms cold for 6k nodes vs 4.5s through DuckDB ADBC.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::{Duration, UNIX_EPOCH};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{Value, json};

use crate::IndexError;
use crate::epoch_layers;
use crate::ingest::ingest_state::{
    CATALOG_COLUMNS_SUBDIR, COMPILE_CLL_SUBDIR, COMPILE_COLUMNS_SUBDIR, COMPILE_NODES_SUBDIR,
    IngestState, PARSE_ALIVE, PARSE_COLUMNS_SUBDIR, PARSE_GENERATION, PARSE_NODES_SUBDIR,
    PARSE_PROJECT, PARSE_RESOLVER_STATE, RUN_CATALOG_STATS_SUBDIR, RUN_FRESHNESS_SUBDIR,
    RUN_INVOCATIONS_SUBDIR, RUN_RESULTS_SUBDIR,
};
use crate::ingest::payload::{
    ParsedDoc, ParsedExposure, ParsedGroup, ParsedMacro, ParsedMetric, ParsedSavedQuery,
    ParsedSemanticModel, ParsedTimeSpine, ParsedUnitTest,
};
use crate::parquet::{IndexWriter, WriteMode};

/// Columns projected when reading parse/nodes epoch parquet files.
/// Skips large unused columns (compiled_code etc.) for faster reads.
pub const PARSE_NODES_COLS: &[&str] = &[
    "unique_id",
    "resource_type",
    "name",
    "package_name",
    "original_path",
    "description",
    "database",
    "schema",
    "alias",
    "relation_name",
    "identifier",
    "materialization",
    "access",
    "group_name",
    "source_name",
    "tags",
    "fqn",
    "depends_on",
    "ingested_at",
    "is_disabled",
    "payload",
];

// ---------------------------------------------------------------------------
// IngestState persistence — saved to index_dir/.fusion_state.json
// ---------------------------------------------------------------------------

pub const STATE_FILE: &str = ".fusion_state.json";

/// Serializable mirror of IngestState for disk persistence.
#[derive(serde::Serialize, serde::Deserialize, Default)]
pub struct PersistedState {
    /// micros-since-epoch for alive.parquet mtime
    pub alive_mtime_us: Option<u64>,
    /// last epoch number per subdir
    pub last_epoch: HashMap<String, u32>,
    // base_mtime_us removed: compaction detection now uses curr_max < last_epoch comparison.
    // Old persisted state files may still have this field — ignored via #[serde(default)].
    #[serde(default)]
    pub base_mtime_us: HashMap<String, u64>,
}

pub fn save_state(index_dir: &Path, state: &IngestState) {
    let alive_mtime_us = state.alive_mtime.and_then(|t| {
        t.duration_since(UNIX_EPOCH)
            .ok()
            .map(|d| d.as_micros() as u64)
    });
    let last_epoch = state
        .last_epoch
        .iter()
        .map(|(k, v)| ((*k).to_string(), *v))
        .collect();
    let ps = PersistedState {
        alive_mtime_us,
        last_epoch,
        base_mtime_us: Default::default(),
    };
    if let Ok(json) = serde_json::to_string(&ps) {
        let _ = std::fs::write(index_dir.join(STATE_FILE), json);
    }
}

pub fn load_state(index_dir: &Path) -> Option<IngestState> {
    let bytes = std::fs::read(index_dir.join(STATE_FILE)).ok()?;
    let ps: PersistedState = serde_json::from_slice(&bytes).ok()?;

    let alive_mtime = ps
        .alive_mtime_us
        .map(|us| UNIX_EPOCH + Duration::from_micros(us));

    const ALL_SUBDIRS: &[&str] = &[
        PARSE_NODES_SUBDIR,
        PARSE_COLUMNS_SUBDIR,
        COMPILE_NODES_SUBDIR,
        COMPILE_COLUMNS_SUBDIR,
        COMPILE_CLL_SUBDIR,
        RUN_INVOCATIONS_SUBDIR,
        RUN_RESULTS_SUBDIR,
        RUN_FRESHNESS_SUBDIR,
    ];
    let mut last_epoch = HashMap::new();
    for &subdir in ALL_SUBDIRS {
        if let Some(&epoch) = ps.last_epoch.get(subdir) {
            last_epoch.insert(subdir, epoch);
        }
    }

    Some(IngestState {
        alive_mtime,
        last_epoch,
        index_dir: None,
        alive_ids: HashSet::new(),
    })
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Smart entry point: loads persisted IngestState from index_dir if present
/// (delta path), otherwise does a full cold ingest. Saves updated state on
/// success so the next call only processes new epochs.
pub fn ingest_from_metadata_direct(
    metadata_dir: &Path,
    index_dir: &Path,
    _state: &mut IngestState,
) -> Result<usize, IndexError> {
    // Auto-detect cold vs delta based on persisted state.
    match load_state(index_dir) {
        Some(mut state) => {
            let n = apply_delta_direct(metadata_dir, index_dir, &mut state)?;
            save_state(index_dir, &state);
            Ok(n)
        }
        None => {
            let mut state = IngestState::default();
            let n = cold_ingest(metadata_dir, index_dir, &mut state)?;
            save_state(index_dir, &state);
            Ok(n)
        }
    }
}

fn cold_ingest(
    metadata_dir: &Path,
    index_dir: &Path,
    state: &mut IngestState,
) -> Result<usize, IndexError> {
    let mut writer = IndexWriter::new(index_dir)?;
    let now = writer.now().to_string();
    let mut total = 0;

    let alive_ids = load_alive_ids(metadata_dir);
    let compile_map = load_compile_nodes_map(metadata_dir, state, true)?;
    total += write_parse_nodes(
        &mut writer,
        metadata_dir,
        state,
        &now,
        true,
        &compile_map,
        alive_ids.as_ref(),
    )?;
    total += write_parse_columns(
        &mut writer,
        metadata_dir,
        state,
        &now,
        true,
        alive_ids.as_ref(),
    )?;
    total += write_parse_project(&mut writer, metadata_dir, &now)?;
    total += write_parse_generation(&mut writer, metadata_dir, &now)?;
    total += write_compile_columns(
        &mut writer,
        metadata_dir,
        state,
        &now,
        true,
        alive_ids.as_ref(),
    )?;
    total += write_catalog_columns(
        &mut writer,
        metadata_dir,
        state,
        &now,
        true,
        alive_ids.as_ref(),
    )?;
    total += write_compile_cll(
        &mut writer,
        metadata_dir,
        state,
        &now,
        true,
        alive_ids.as_ref(),
    )?;
    total += write_run_invocations(&mut writer, metadata_dir, state, &now, true)?;
    total += write_run_results(&mut writer, metadata_dir, state, &now, true)?;
    total += write_column_stats(&mut writer, index_dir, &now)?;
    total += write_seed_catalog_stats(&mut writer, index_dir, metadata_dir, &now)?;
    total += write_warehouse_catalog_stats(&mut writer, metadata_dir, state, &now, true)?;
    total += write_run_freshness(&mut writer, metadata_dir, state, &now, true)?;
    writer.finish_for_ingest()?;

    // Store at µs precision to match what save_state serializes.
    let alive_us: Option<u64> = std::fs::metadata(metadata_dir.join(PARSE_ALIVE))
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_micros() as u64);
    state.alive_mtime = alive_us.map(|us| UNIX_EPOCH + Duration::from_micros(us));

    Ok(total)
}

/// Incremental delta ingest: reads only new epochs since last run,
/// merges into existing index_dir parquet files.
pub fn apply_delta_direct(
    metadata_dir: &Path,
    index_dir: &Path,
    state: &mut IngestState,
) -> Result<usize, IndexError> {
    // Compare at microsecond precision — state is persisted as µs, OS mtime is ns.
    let current_us: Option<u64> = std::fs::metadata(metadata_dir.join(PARSE_ALIVE))
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_micros() as u64);
    let stored_us: Option<u64> = state
        .alive_mtime
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_micros() as u64);
    if current_us == stored_us {
        return Ok(0);
    }
    state.alive_mtime = current_us.map(|us| UNIX_EPOCH + Duration::from_micros(us));

    let mut writer = IndexWriter::new(index_dir)?;
    let now = writer.now().to_string();
    let mut total = 0;

    let alive_ids = load_alive_ids(metadata_dir);
    let compile_map = load_compile_nodes_map(metadata_dir, state, false)?;
    total += write_parse_nodes(
        &mut writer,
        metadata_dir,
        state,
        &now,
        false,
        &compile_map,
        alive_ids.as_ref(),
    )?;
    total += write_parse_columns(
        &mut writer,
        metadata_dir,
        state,
        &now,
        false,
        alive_ids.as_ref(),
    )?;
    total += write_parse_project(&mut writer, metadata_dir, &now)?;
    total += write_parse_generation(&mut writer, metadata_dir, &now)?;
    total += write_compile_columns(
        &mut writer,
        metadata_dir,
        state,
        &now,
        false,
        alive_ids.as_ref(),
    )?;
    total += write_catalog_columns(
        &mut writer,
        metadata_dir,
        state,
        &now,
        false,
        alive_ids.as_ref(),
    )?;
    total += write_compile_cll(
        &mut writer,
        metadata_dir,
        state,
        &now,
        false,
        alive_ids.as_ref(),
    )?;
    total += write_run_invocations(&mut writer, metadata_dir, state, &now, false)?;
    total += write_run_results(&mut writer, metadata_dir, state, &now, false)?;
    total += write_column_stats(&mut writer, index_dir, &now)?;
    total += write_seed_catalog_stats(&mut writer, index_dir, metadata_dir, &now)?;
    total += write_warehouse_catalog_stats(&mut writer, metadata_dir, state, &now, false)?;
    total += write_run_freshness(&mut writer, metadata_dir, state, &now, false)?;
    writer.finish_for_ingest()?;

    Ok(total)
}

// ---------------------------------------------------------------------------
// Helpers — read a parquet file into RecordBatches
// ---------------------------------------------------------------------------

fn read_parquet_batches(path: &Path) -> Result<Vec<arrow_array::RecordBatch>, IndexError> {
    read_parquet_batches_proj(path, &[])
}

/// Read parquet, projecting only the named columns (empty = all columns).
pub fn read_parquet_batches_proj(
    path: &Path,
    cols: &[&str],
) -> Result<Vec<arrow_array::RecordBatch>, IndexError> {
    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| IndexError::Other(format!("parquet open {}: {e}", path.display())))?;
    let builder = builder.with_batch_size(65536);
    let builder = if cols.is_empty() {
        builder
    } else {
        let schema = builder.schema();
        let indices: Vec<usize> = cols
            .iter()
            .filter_map(|name| schema.index_of(name).ok())
            .collect();
        let mask = parquet::arrow::ProjectionMask::roots(builder.parquet_schema(), indices);
        builder.with_projection(mask)
    };
    let reader = builder
        .build()
        .map_err(|e| IndexError::Other(format!("parquet reader {}: {e}", path.display())))?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(|e| IndexError::Other(format!("parquet read: {e}")))?);
    }
    Ok(batches)
}

// ---------------------------------------------------------------------------
// Arrow column extraction helpers
// ---------------------------------------------------------------------------

pub fn str_col<'a>(
    batch: &'a arrow_array::RecordBatch,
    name: &str,
) -> Option<&'a arrow_array::StringArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<arrow_array::StringArray>())
}

fn i64_col<'a>(
    batch: &'a arrow_array::RecordBatch,
    name: &str,
) -> Option<&'a arrow_array::Int64Array> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<arrow_array::Int64Array>())
}

fn timestamp_micros_col<'a>(
    batch: &'a arrow_array::RecordBatch,
    name: &str,
) -> Option<&'a arrow_array::TimestampMicrosecondArray> {
    batch.column_by_name(name).and_then(|c| {
        c.as_any()
            .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
    })
}

fn i32_col<'a>(
    batch: &'a arrow_array::RecordBatch,
    name: &str,
) -> Option<&'a arrow_array::Int32Array> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<arrow_array::Int32Array>())
}

fn f64_col<'a>(
    batch: &'a arrow_array::RecordBatch,
    name: &str,
) -> Option<&'a arrow_array::Float64Array> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<arrow_array::Float64Array>())
}

fn list_col_batch<'a>(
    batch: &'a arrow_array::RecordBatch,
    name: &str,
) -> Option<&'a arrow_array::ListArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<arrow_array::ListArray>())
}

fn get_str(col: Option<&arrow_array::StringArray>, i: usize) -> Option<String> {
    crate::ingest::payload::str_col(col, i)
}

fn get_i64(col: Option<&arrow_array::Int64Array>, i: usize) -> Option<i64> {
    use arrow_array::Array;
    col.filter(|c| !c.is_null(i)).map(|c| c.value(i))
}

fn get_timestamp_micros(
    col: Option<&arrow_array::TimestampMicrosecondArray>,
    i: usize,
) -> Option<i64> {
    use arrow_array::Array;
    col.filter(|c| !c.is_null(i)).map(|c| c.value(i))
}

fn get_i32(col: Option<&arrow_array::Int32Array>, i: usize) -> Option<i32> {
    use arrow_array::Array;
    col.filter(|c| !c.is_null(i)).map(|c| c.value(i))
}

fn get_f64(col: Option<&arrow_array::Float64Array>, i: usize) -> Option<f64> {
    use arrow_array::Array;
    col.filter(|c| !c.is_null(i)).map(|c| c.value(i))
}

fn get_list(col: Option<&arrow_array::ListArray>, i: usize) -> Vec<String> {
    crate::ingest::payload::list_col(col, i)
}

pub fn extract_json_field_raw<'a>(json: &'a str, key: &str) -> Option<&'a str> {
    let needle = format!("\"{key}\":");
    let key_pos = json.find(needle.as_str())?;
    let value_start_offset = key_pos + needle.len();
    // Skip leading whitespace
    let trimmed = json[value_start_offset..].trim_start();
    if trimmed.is_empty() {
        return None;
    }
    let abs_start = json.len() - trimmed.len();
    let end = find_json_value_end(json, abs_start)?;
    Some(&json[abs_start..end])
}

/// Find the end offset of a JSON value starting at `start` in `s`.
fn find_json_value_end(s: &str, start: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    let b = *bytes.get(start)?;
    match b {
        b'{' => find_matching(bytes, start, b'{', b'}'),
        b'[' => find_matching(bytes, start, b'[', b']'),
        b'"' => {
            let mut i = start + 1;
            while i < bytes.len() {
                match bytes[i] {
                    b'\\' => i += 2,
                    b'"' => return Some(i + 1),
                    _ => i += 1,
                }
            }
            None
        }
        _ => {
            // number, boolean, null — scan until delimiter
            let end = bytes[start..]
                .iter()
                .position(|&c| matches!(c, b',' | b'}' | b']' | b' ' | b'\n' | b'\r' | b'\t'))?;
            Some(start + end)
        }
    }
}

fn find_matching(bytes: &[u8], start: usize, open: u8, close: u8) -> Option<usize> {
    let mut depth = 0i32;
    let mut i = start;
    let mut in_string = false;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' if in_string => i += 2,
            b'"' => {
                in_string = !in_string;
                i += 1;
            }
            c if !in_string && c == open => {
                depth += 1;
                i += 1;
            }
            c if !in_string && c == close => {
                depth -= 1;
                i += 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => i += 1,
        }
    }
    None
}

/// micros since epoch → RFC3339 timestamp string
fn micros_to_ts(micros: i64) -> String {
    use std::time::{Duration, UNIX_EPOCH};
    let d = UNIX_EPOCH + Duration::from_micros(micros as u64);
    let secs = d.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    let nanos = (micros % 1_000_000) * 1000;
    chrono::DateTime::<chrono::Utc>::from(UNIX_EPOCH + Duration::new(secs, nanos as u32))
        .to_rfc3339()
}

fn micros_to_datetime(micros: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp_micros(micros).unwrap_or_else(chrono::Utc::now)
}

// ---------------------------------------------------------------------------
// Compile fields — loaded ahead of parse so they can be merged in one pass
// ---------------------------------------------------------------------------

pub struct CompileFields {
    pub compiled_code: Option<String>,
    pub compiled_code_hash: Option<String>,
    pub compiled_path: Option<String>,
    pub grain: Vec<String>,
    pub grain_declared: Vec<String>,
    pub grain_tested: Vec<String>,
    pub table_role: Option<String>,
}

pub type CompileMap = HashMap<String, CompileFields>;

/// Load alive.parquet → set of live unique_ids (None if file absent).
pub fn load_alive_ids(metadata_dir: &Path) -> Option<HashSet<String>> {
    let path = metadata_dir.join(PARSE_ALIVE);
    if !path.exists() {
        return None;
    }
    let batches = read_parquet_batches_proj(&path, &["unique_id"]).ok()?;
    let mut ids = HashSet::new();
    for b in &batches {
        if let Some(col) = str_col(b, "unique_id") {
            use arrow_array::Array;
            for i in 0..b.num_rows() {
                if !col.is_null(i) {
                    ids.insert(col.value(i).to_string());
                }
            }
        }
    }
    Some(ids)
}

/// Prune rows whose `unique_id` is not in `alive_ids`. No-op when `alive_ids` is None.
pub fn prune_by_alive(rows: &mut Vec<Value>, alive_ids: Option<&HashSet<String>>) {
    let Some(ids) = alive_ids else { return };
    rows.retain(|r| {
        r.get("unique_id")
            .and_then(|v| v.as_str())
            .map(|id| ids.contains(id))
            .unwrap_or(false)
    });
}

/// Read all compile/nodes epochs into a uid→CompileFields map.
/// Returns an empty map (not an error) if the directory doesn't exist.
/// `full`: when true, detects base compaction and reloads all epochs if needed.
pub fn load_compile_nodes_map(
    metadata_dir: &Path,
    state: &mut IngestState,
    full: bool,
) -> Result<CompileMap, IndexError> {
    let dir = metadata_dir.join(COMPILE_NODES_SUBDIR);
    if !dir.exists() {
        return Ok(CompileMap::new());
    }

    let epochs = epoch_layers::existing_epochs(&dir);
    if epochs.is_empty() {
        return Ok(CompileMap::new());
    }

    let last = state.last_epoch_for(COMPILE_NODES_SUBDIR);
    let curr_max_ep = epochs.iter().map(|(n, _)| *n).max().unwrap_or(0);
    let need_full = full || last == u32::MAX || curr_max_ep < last;
    let new_epochs: Vec<_> = if need_full {
        epochs
    } else {
        epochs.into_iter().filter(|(n, _)| *n > last).collect()
    };

    if new_epochs.is_empty() {
        return Ok(CompileMap::new());
    }

    const COMPILE_COLS: &[&str] = &[
        "unique_id",
        "compiled_code",
        "compiled_code_hash",
        "compiled_path",
        "grain",
        "grain_declared",
        "grain_tested",
        "table_role",
    ];

    let mut map = CompileMap::new();
    let mut last_epoch = 0u32;
    for (epoch_num, path) in &new_epochs {
        let batches = read_parquet_batches_proj(path, COMPILE_COLS)?;
        for batch in &batches {
            let uid_col = str_col(batch, "unique_id");
            let ccode_col = str_col(batch, "compiled_code");
            let chash_col = str_col(batch, "compiled_code_hash");
            let cpath_col = str_col(batch, "compiled_path");
            let grain_col = str_col(batch, "grain");
            let gdecl_col = str_col(batch, "grain_declared");
            let gtest_col = str_col(batch, "grain_tested");
            let trole_col = str_col(batch, "table_role");
            for i in 0..batch.num_rows() {
                let Some(uid) = get_str(uid_col, i) else {
                    continue;
                };
                let grain = get_str(grain_col, i)
                    .and_then(|s| serde_json::from_str::<Vec<String>>(&s).ok())
                    .unwrap_or_default();
                let grain_declared = get_str(gdecl_col, i)
                    .and_then(|s| serde_json::from_str::<Vec<String>>(&s).ok())
                    .unwrap_or_default();
                let grain_tested = get_str(gtest_col, i)
                    .and_then(|s| serde_json::from_str::<Vec<String>>(&s).ok())
                    .unwrap_or_default();
                map.insert(
                    uid,
                    CompileFields {
                        compiled_code: get_str(ccode_col, i),
                        compiled_code_hash: get_str(chash_col, i),
                        compiled_path: get_str(cpath_col, i),
                        grain,
                        grain_declared,
                        grain_tested,
                        table_role: get_str(trole_col, i),
                    },
                );
            }
        }
        last_epoch = *epoch_num;
    }
    state.set_epoch(COMPILE_NODES_SUBDIR, last_epoch);
    Ok(map)
}

// ---------------------------------------------------------------------------
// Parse nodes → dbt.nodes + dbt.edges + dbt.test_metadata + special tables
// ---------------------------------------------------------------------------

pub struct OwnedNodeRow {
    pub unique_id: String,
    pub name: String,
    pub resource_type: String,
    pub package_name: String,
    pub file_path: String,
    pub original_file_path: String,
    pub fqn: Vec<String>,
    pub alias: Option<String>,
    pub checksum: Option<String>,
    pub description: Option<String>,
    pub raw_code: Option<String>,
    pub database_name: String,
    pub schema_name: String,
    pub relation_name: Option<String>,
    pub identifier: Option<String>,
    pub enabled: Option<bool>,
    pub materialized: Option<String>,
    pub access_level: Option<String>,
    pub group_name: Option<String>,
    pub contract_enforced: bool,
    pub primary_key: Vec<String>,
    pub tags: Vec<String>,
    pub source_name: Option<String>,
    pub source_description: Option<String>,
    pub loader: Option<String>,
    pub loaded_at_field: Option<String>,
    pub patch_path: Option<String>,
    pub meta: Option<String>,
    pub config: Option<String>,
    pub deprecation_date: Option<String>,
    pub version: Option<String>,
    pub latest_version: Option<String>,
    pub compiled_code: Option<String>,
    pub compiled_code_hash: Option<String>,
    pub compiled_path: Option<String>,
    pub table_role: Option<String>,
    pub grain: Vec<String>,
    pub grain_declared: Vec<String>,
    pub grain_tested: Vec<String>,
    pub ingested_at: String,
}

fn write_parse_nodes(
    writer: &mut IndexWriter,
    metadata_dir: &Path,
    state: &mut IngestState,
    now: &str,
    full: bool,
    compile_map: &CompileMap,
    alive_ids: Option<&HashSet<String>>,
) -> Result<usize, IndexError> {
    let dir = metadata_dir.join(PARSE_NODES_SUBDIR);
    if !dir.exists() {
        return Ok(0);
    }

    let epochs = epoch_layers::existing_epochs(&dir);
    let last_saved = state.last_epoch_for(PARSE_NODES_SUBDIR);
    let curr_max_ep = epochs.iter().map(|(n, _)| *n).max().unwrap_or(0);
    let need_full = full || last_saved == u32::MAX || curr_max_ep < last_saved;
    if epochs.is_empty() {
        return Ok(0);
    }

    let new_epochs: Vec<_> = if need_full {
        epochs
    } else {
        let last = state.last_epoch_for(PARSE_NODES_SUBDIR);
        epochs.into_iter().filter(|(n, _)| *n > last).collect()
    };
    if new_epochs.is_empty() {
        return Ok(0);
    }

    // Accumulate rows across all new epochs
    let mut node_rows: Vec<OwnedNodeRow> = Vec::new();
    let mut edge_rows: Vec<Value> = Vec::new();
    let mut test_meta_rows: Vec<Value> = Vec::new();
    // Special table rows
    let mut metric_rows: Vec<Value> = Vec::new();
    let mut semantic_model_rows: Vec<Value> = Vec::new();
    let mut semantic_entity_rows: Vec<Value> = Vec::new();
    let mut semantic_measure_rows: Vec<Value> = Vec::new();
    let mut semantic_dimension_rows: Vec<Value> = Vec::new();
    let mut saved_query_rows: Vec<Value> = Vec::new();
    let mut exposure_rows: Vec<Value> = Vec::new();
    let mut group_rows: Vec<Value> = Vec::new();
    let mut macro_rows: Vec<Value> = Vec::new();
    let mut doc_rows: Vec<Value> = Vec::new();
    let mut unit_test_rows: Vec<Value> = Vec::new();
    let mut time_spine_rows: Vec<Value> = Vec::new();

    // Columns projected from parse/nodes epochs — see module-level PARSE_NODES_COLS.

    // Process epochs newest-first: for full ingest the latest epoch wins per unique_id.
    // seen_node_ids tracks which nodes have already been emitted (from a newer epoch).
    let mut seen_node_ids: HashSet<String> = HashSet::new();
    let iterate_newest_first = need_full;

    let mut last_epoch_num = 0u32;
    let epoch_iter: Vec<_> = if iterate_newest_first {
        new_epochs.iter().rev().collect()
    } else {
        new_epochs.iter().collect()
    };
    for (epoch_num, path) in &epoch_iter {
        let batches = read_parquet_batches_proj(path, PARSE_NODES_COLS)?;
        for batch in &batches {
            // Parse payloads selectively: models only have `config` in their payload
            // (no raw_code, meta, patch_path, etc.), so we avoid the expensive full
            // simd_json parse for them and extract config as a raw JSON string instead.
            let payloads: Vec<Value> = {
                let payload_col = str_col(batch, "payload");
                let rt_col_inner = str_col(batch, "resource_type");
                use arrow_array::Array;
                (0..batch.num_rows())
                    .map(|i| {
                        let is_model = rt_col_inner
                            .filter(|c| !c.is_null(i))
                            .map(|c| c.value(i) == "model")
                            .unwrap_or(false);
                        if is_model {
                            // For models: extract only `config` as a raw string — avoid
                            // full JSON parse of the ~800-byte payload.
                            let config_raw = payload_col
                                .filter(|c| !c.is_null(i))
                                .and_then(|c| extract_json_field_raw(c.value(i), "config"))
                                .map(|s| Value::String(s.to_string()));
                            if let Some(cfg) = config_raw {
                                let mut obj = serde_json::Map::new();
                                obj.insert("config".to_string(), cfg);
                                Value::Object(obj)
                            } else {
                                Value::Object(serde_json::Map::new())
                            }
                        } else {
                            // Non-model nodes (macros, docs, sources, etc.): full parse needed.
                            payload_col
                                .filter(|c| !c.is_null(i))
                                .and_then(|c| {
                                    let mut buf = c.value(i).as_bytes().to_vec();
                                    simd_json::serde::from_slice::<Value>(&mut buf).ok()
                                })
                                .unwrap_or(Value::Null)
                        }
                    })
                    .collect()
            };
            extract_parse_nodes_batch(
                batch,
                now,
                &payloads,
                compile_map,
                if iterate_newest_first {
                    Some(&mut seen_node_ids)
                } else {
                    None
                },
                alive_ids,
                &mut node_rows,
                &mut edge_rows,
                &mut test_meta_rows,
                &mut metric_rows,
                &mut semantic_model_rows,
                &mut semantic_entity_rows,
                &mut semantic_measure_rows,
                &mut semantic_dimension_rows,
                &mut saved_query_rows,
                &mut exposure_rows,
                &mut group_rows,
                &mut macro_rows,
                &mut doc_rows,
                &mut unit_test_rows,
                &mut time_spine_rows,
            );
        }
        last_epoch_num = last_epoch_num.max(*epoch_num);
    }

    // On delta ingest (oldest-first, no inline dedup): prune against alive_ids after.
    if !iterate_newest_first {
        if let Some(ids) = alive_ids {
            node_rows.retain(|r| ids.contains(&r.unique_id));
            edge_rows.retain(|r| {
                r.get("parent_unique_id")
                    .and_then(|v| v.as_str())
                    .map(|id| ids.contains(id))
                    .unwrap_or(false)
                    && r.get("child_unique_id")
                        .and_then(|v| v.as_str())
                        .map(|id| ids.contains(id))
                        .unwrap_or(false)
            });
        }
        prune_by_alive(&mut test_meta_rows, alive_ids);
        prune_by_alive(&mut metric_rows, alive_ids);
        prune_by_alive(&mut semantic_model_rows, alive_ids);
        prune_by_alive(&mut semantic_entity_rows, alive_ids);
        prune_by_alive(&mut semantic_measure_rows, alive_ids);
        prune_by_alive(&mut semantic_dimension_rows, alive_ids);
        prune_by_alive(&mut saved_query_rows, alive_ids);
        prune_by_alive(&mut exposure_rows, alive_ids);
        prune_by_alive(&mut group_rows, alive_ids);
        prune_by_alive(&mut macro_rows, alive_ids);
        prune_by_alive(&mut doc_rows, alive_ids);
        prune_by_alive(&mut unit_test_rows, alive_ids);
        prune_by_alive(&mut time_spine_rows, alive_ids);
    }

    let count = node_rows.len();

    // Convert owned node rows to typed NodeRow<'_> for fast serde_arrow serialization.
    use crate::parquet::NodeRow;
    let typed_nodes: Vec<NodeRow<'_>> = node_rows
        .iter()
        .map(|r| NodeRow {
            unique_id: &r.unique_id,
            name: &r.name,
            resource_type: &r.resource_type,
            package_name: &r.package_name,
            file_path: r.file_path.clone(),
            original_file_path: r.original_file_path.clone(),
            fqn: r.fqn.clone(),
            alias: r.alias.as_deref(),
            checksum: r.checksum.as_deref(),
            description: r.description.as_deref(),
            node_language: None,
            raw_code: r.raw_code.as_deref(),
            database_name: r.database_name.as_str(),
            schema_name: r.schema_name.as_str(),
            relation_name: r.relation_name.as_deref(),
            identifier: r.identifier.as_deref(),
            enabled: r.enabled,
            materialized: r.materialized.clone(),
            incremental_strategy: None,
            on_schema_change: None,
            unique_key: None,
            full_refresh: None,
            persist_docs: None,
            pre_hook: None,
            post_hook: None,
            grants: None,
            config: r.config.clone(),
            access_level: r.access_level.clone(),
            group_name: r.group_name.as_deref(),
            contract_enforced: r.contract_enforced,
            version: r.version.clone(),
            latest_version: r.latest_version.clone(),
            deprecation_date: r.deprecation_date.as_deref(),
            node_constraints: None,
            primary_key: r.primary_key.clone(),
            docs_show: true,
            patch_path: r.patch_path.clone(),
            time_spine: None,
            tags: r.tags.clone(),
            meta: r.meta.clone(),
            ai_context: None,
            source_name: r.source_name.as_deref(),
            source_description: r.source_description.as_deref(),
            loader: r.loader.as_deref(),
            loaded_at_field: r.loaded_at_field.as_deref(),
            loaded_at_query: None,
            freshness: None,
            external_config: None,
            source_meta: None,
            quoting: None,
            compiled_code: r.compiled_code.as_deref(),
            compiled_code_hash: r.compiled_code_hash.as_deref(),
            compiled_path: r.compiled_path.as_deref(),
            extra_ctes: None,
            compiled_at: None,
            raw_code_hash: None,
            search_text: None,
            grain: r.grain.clone(),
            grain_declared: r.grain_declared.clone(),
            grain_tested: r.grain_tested.clone(),
            grain_inferred: vec![],
            table_role: r.table_role.as_deref(),
            ingested_at: &r.ingested_at,
        })
        .collect();

    if need_full {
        writer.write_dbt_items("nodes", &typed_nodes)?;
        drop(typed_nodes);
        writer.write_dbt_table("edges", edge_rows)?;
        writer.write_dbt_table("test_metadata", test_meta_rows)?;
        writer.write_dbt_table("metrics", metric_rows)?;
        writer.write_dbt_table("semantic_models", semantic_model_rows)?;
        writer.write_dbt_table("semantic_entities", semantic_entity_rows)?;
        writer.write_dbt_table("semantic_measures", semantic_measure_rows)?;
        writer.write_dbt_table("semantic_dimensions", semantic_dimension_rows)?;
        writer.write_dbt_table("saved_queries", saved_query_rows)?;
        writer.write_dbt_table("exposures", exposure_rows)?;
        writer.write_dbt_table("groups", group_rows)?;
        writer.write_dbt_table("macros", macro_rows)?;
        writer.write_dbt_table("docs", doc_rows)?;
        writer.write_dbt_table("unit_tests", unit_test_rows)?;
        writer.write_dbt_table("time_spines", time_spine_rows)?;
    } else {
        // Delta: merge new rows into existing index (latest wins).
        // alive_ids determines which old rows to preserve — if unavailable, keep all.
        let all_valid: HashSet<String>;
        let valid = match alive_ids {
            Some(ids) => ids,
            None => {
                all_valid = node_rows.iter().map(|r| r.unique_id.clone()).collect();
                &all_valid
            }
        };

        // nodes always written — even an empty delta must carry forward compiled fields.
        writer.write_dbt_items_merged(
            "nodes",
            &typed_nodes,
            WriteMode::CarryForwardMerge {
                key_cols: &["unique_id"],
                carry_cols: &[
                    "compiled_code",
                    "compiled_code_hash",
                    "compiled_path",
                    "grain",
                    "grain_declared",
                    "grain_tested",
                    "table_role",
                ],
                valid_ids: valid,
            },
        )?;
        // Edges: replace edges for changed child nodes; preserve edges for alive unchanged nodes.
        if !edge_rows.is_empty() {
            writer.write_dbt_table_merged(
                "edges",
                edge_rows,
                WriteMode::MergePrune {
                    key_col: "child_unique_id",
                    valid_ids: valid,
                },
            )?;
        }
        // For snapshot tables: merge new rows, keep old rows for alive nodes.
        macro_rules! merge_if_nonempty {
            ($rows:expr, $table:literal) => {
                if !$rows.is_empty() {
                    writer.write_dbt_table_merged(
                        $table,
                        $rows,
                        WriteMode::MergePrune {
                            key_col: "unique_id",
                            valid_ids: valid,
                        },
                    )?;
                }
            };
        }
        merge_if_nonempty!(test_meta_rows, "test_metadata");
        merge_if_nonempty!(metric_rows, "metrics");
        merge_if_nonempty!(semantic_model_rows, "semantic_models");
        merge_if_nonempty!(semantic_entity_rows, "semantic_entities");
        merge_if_nonempty!(semantic_measure_rows, "semantic_measures");
        merge_if_nonempty!(semantic_dimension_rows, "semantic_dimensions");
        merge_if_nonempty!(saved_query_rows, "saved_queries");
        merge_if_nonempty!(exposure_rows, "exposures");
        merge_if_nonempty!(group_rows, "groups");
        merge_if_nonempty!(macro_rows, "macros");
        merge_if_nonempty!(doc_rows, "docs");
        merge_if_nonempty!(unit_test_rows, "unit_tests");
        merge_if_nonempty!(time_spine_rows, "time_spines");
    }

    state.set_epoch(PARSE_NODES_SUBDIR, last_epoch_num);
    Ok(count)
}

#[allow(clippy::too_many_arguments)]
pub fn extract_parse_nodes_batch(
    batch: &arrow_array::RecordBatch,
    now: &str,
    payloads: &[Value],
    compile_map: &CompileMap,
    mut seen_ids: Option<&mut HashSet<String>>,
    alive_ids: Option<&HashSet<String>>,
    node_rows: &mut Vec<OwnedNodeRow>,
    edge_rows: &mut Vec<Value>,
    test_meta_rows: &mut Vec<Value>,
    metric_rows: &mut Vec<Value>,
    semantic_model_rows: &mut Vec<Value>,
    semantic_entity_rows: &mut Vec<Value>,
    semantic_measure_rows: &mut Vec<Value>,
    semantic_dimension_rows: &mut Vec<Value>,
    saved_query_rows: &mut Vec<Value>,
    exposure_rows: &mut Vec<Value>,
    group_rows: &mut Vec<Value>,
    macro_rows: &mut Vec<Value>,
    doc_rows: &mut Vec<Value>,
    unit_test_rows: &mut Vec<Value>,
    time_spine_rows: &mut Vec<Value>,
) {
    let uid_col = str_col(batch, "unique_id");
    let rt_col = str_col(batch, "resource_type");
    let name_col = str_col(batch, "name");
    let pkg_col = str_col(batch, "package_name");
    let orig_col = str_col(batch, "original_path");
    let desc_col = str_col(batch, "description");
    let db_col = str_col(batch, "database");
    let schema_col = str_col(batch, "schema");
    let alias_col = str_col(batch, "alias");
    let rel_col = str_col(batch, "relation_name");
    let ident_col = str_col(batch, "identifier");
    let mat_col = str_col(batch, "materialization");
    let access_col = str_col(batch, "access");
    let grp_col = str_col(batch, "group_name");
    let src_col = str_col(batch, "source_name");
    let tags_col = list_col_batch(batch, "tags");
    let fqn_col = list_col_batch(batch, "fqn");
    let deps_col = list_col_batch(batch, "depends_on");
    let ts_col = i64_col(batch, "ingested_at");
    let dis_col = i32_col(batch, "is_disabled");

    for i in 0..batch.num_rows() {
        let Some(uid) = get_str(uid_col, i) else {
            continue;
        };

        // Alive check — skip nodes not in the live set.
        if let Some(ids) = alive_ids {
            if !ids.contains(&uid) {
                continue;
            }
        }
        // Dedup check — skip nodes already emitted from a newer epoch.
        if let Some(s) = seen_ids.as_mut() {
            if !s.insert(uid.clone()) {
                continue;
            }
        }

        let rt = get_str(rt_col, i).unwrap_or_default();

        let ts = get_i64(ts_col, i)
            .map(micros_to_ts)
            .unwrap_or_else(|| now.to_string());
        let tags = get_list(tags_col, i);
        let fqn = get_list(fqn_col, i);
        let deps = get_list(deps_col, i);

        // Use pre-parsed payload (parsed in parallel before this loop)
        let payload: &Value = payloads.get(i).unwrap_or(&Value::Null);

        let common = payload
            .get("__common_attr__")
            .cloned()
            .unwrap_or(Value::Null);
        let file_path: Option<String> = common
            .get("path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let checksum = payload
            .get("checksum")
            .and_then(|c| c.get("checksum"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let raw_code = payload
            .get("raw_code")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let contract_enforced = payload
            .get("contract")
            .and_then(|c| c.get("enforced"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let primary_key: Vec<String> = payload
            .get("__model_attr__")
            .and_then(|m| m.get("primary_key"))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|s| s.as_str())
                    .map(|s| s.to_string())
                    .collect()
            })
            .unwrap_or_default();

        let enabled = get_i32(dis_col, i).map(|d| d == 0).unwrap_or(true);

        let source_name = get_str(src_col, i).or_else(|| {
            payload
                .get("source_name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        });
        let source_description = payload
            .get("source_description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let loader = payload
            .get("loader")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let loaded_at_field = payload
            .get("loaded_at_field")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let patch_path = payload
            .get("patch_path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let meta = payload.get("meta").cloned();
        let config = payload.get("config").cloned();
        let deprecation_date = payload
            .get("deprecation_date")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let version = payload.get("version").cloned();
        let latest_version = payload.get("latest_version").cloned();

        // Nodes that go into dbt.nodes
        let write_to_nodes = matches!(
            rt.as_str(),
            "model"
                | "source"
                | "seed"
                | "snapshot"
                | "analysis"
                | "test"
                | "operation"
                | "function"
        );

        if write_to_nodes {
            let (
                compiled_code,
                compiled_code_hash,
                compiled_path,
                table_role,
                grain,
                grain_declared,
                grain_tested,
            ) = if let Some(c) = compile_map.get(&uid) {
                (
                    c.compiled_code.clone(),
                    c.compiled_code_hash.clone(),
                    c.compiled_path.clone(),
                    c.table_role.clone(),
                    c.grain.clone(),
                    c.grain_declared.clone(),
                    c.grain_tested.clone(),
                )
            } else {
                (None, None, None, None, vec![], vec![], vec![])
            };
            node_rows.push(OwnedNodeRow {
                unique_id: uid.clone(),
                name: get_str(name_col, i).unwrap_or_default(),
                resource_type: rt.clone(),
                package_name: get_str(pkg_col, i).unwrap_or_default(),
                file_path: file_path.unwrap_or_default(),
                original_file_path: get_str(orig_col, i).unwrap_or_default(),
                fqn,
                alias: get_str(alias_col, i),
                checksum,
                description: get_str(desc_col, i),
                raw_code,
                database_name: get_str(db_col, i).unwrap_or_default(),
                schema_name: get_str(schema_col, i).unwrap_or_default(),
                relation_name: get_str(rel_col, i),
                identifier: get_str(ident_col, i),
                enabled: Some(enabled),
                materialized: get_str(mat_col, i),
                access_level: get_str(access_col, i),
                group_name: get_str(grp_col, i),
                contract_enforced,
                primary_key,
                tags: tags.clone(),
                source_name,
                source_description,
                loader,
                loaded_at_field,
                patch_path,
                meta: meta.as_ref().map(|v| v.to_string()),
                config: config.as_ref().map(|v| v.to_string()),
                deprecation_date,
                version: version.as_ref().map(|v| v.to_string()),
                latest_version: latest_version.as_ref().map(|v| v.to_string()),
                compiled_code,
                compiled_code_hash,
                compiled_path,
                table_role,
                grain,
                grain_declared,
                grain_tested,
                ingested_at: ts.clone(),
            });
        }

        // Edges from depends_on
        for parent in &deps {
            edge_rows.push(json!({
                "parent_unique_id": parent,
                "child_unique_id": uid,
                "edge_type": "ref",
                "ingested_at": ts,
            }));
        }

        // test_metadata for test nodes (from __test_attr__ in payload)
        if rt == "test" {
            let test_attr = payload.get("__test_attr__");
            let tm = test_attr
                .and_then(|a| a.get("test_metadata"))
                .or_else(|| payload.get("test_metadata"));
            if let Some(tm) = tm {
                let attached = test_attr
                    .and_then(|a| a.get("attached_node"))
                    .and_then(|v| v.as_str())
                    .or_else(|| payload.get("attached_node").and_then(|v| v.as_str()))
                    .or_else(|| payload.get("file_key_name").and_then(|v| v.as_str()))
                    .map(|s| s.to_string());
                // severity and fail/warn thresholds live in deprecated_config or config
                let config = payload.get("config");
                let dep_config = payload.get("deprecated_config");
                let severity = dep_config
                    .and_then(|c| c.get("severity"))
                    .or_else(|| config.and_then(|c| c.get("severity")))
                    .and_then(|v| v.as_str());
                let warn_if = dep_config
                    .and_then(|c| c.get("warn_if"))
                    .and_then(|v| v.as_str());
                let error_if = dep_config
                    .and_then(|c| c.get("error_if"))
                    .and_then(|v| v.as_str());
                let fail_calc = dep_config
                    .and_then(|c| c.get("fail_calc"))
                    .and_then(|v| v.as_str());
                let store_failures = dep_config
                    .and_then(|c| c.get("store_failures"))
                    .and_then(|v| v.as_bool());
                let store_failures_as = dep_config
                    .and_then(|c| c.get("store_failures_as"))
                    .and_then(|v| v.as_str());
                test_meta_rows.push(json!({
                    "unique_id": uid,
                    "test_name": tm.get("name").and_then(|v| v.as_str()),
                    "test_namespace": tm.get("namespace").and_then(|v| v.as_str()),
                    "kwargs": tm.get("kwargs").map(|v| v.to_string()),
                    "column_name": test_attr.and_then(|a| a.get("column_name")).and_then(|v| v.as_str()),
                    "attached_node": attached,
                    "severity": severity,
                    "warn_if": warn_if,
                    "error_if": error_if,
                    "fail_calc": fail_calc,
                    "store_failures": store_failures,
                    "store_failures_as": store_failures_as,
                    "ingested_at": ts,
                }));
            }
        }

        // Special tables by resource_type
        match rt.as_str() {
            "metric" => {
                let node_name = get_str(name_col, i).unwrap_or_default();
                build_metric_from_payload(&uid, &node_name, payload, &tags, &ts, metric_rows);
            }
            "semantic_model" => {
                build_semantic_model_from_payload(
                    &uid,
                    payload,
                    &ts,
                    semantic_model_rows,
                    semantic_entity_rows,
                    semantic_measure_rows,
                    semantic_dimension_rows,
                );
            }
            "saved_query" => {
                build_saved_query_from_payload(&uid, payload, &ts, saved_query_rows);
            }
            "exposure" => {
                build_exposure_from_payload(&uid, payload, &tags, &ts, exposure_rows);
            }
            "group" => {
                build_group_from_payload(&uid, payload, &ts, group_rows);
            }
            "macro" => {
                build_macro_from_payload(&uid, payload, &ts, macro_rows);
            }
            "docs_macro" => {
                build_doc_from_payload(&uid, payload, &ts, doc_rows);
            }
            "unit_test" => {
                build_unit_test_from_payload(&uid, payload, &ts, unit_test_rows);
            }
            "model" => {
                // time_spine: embedded in __model_attr__
                if let Some(ts_val) = payload
                    .get("__model_attr__")
                    .and_then(|m| m.get("time_spine"))
                    .filter(|v| !v.is_null())
                {
                    build_time_spine_from_payload(&uid, ts_val, &ts, time_spine_rows);
                }
            }
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Special table builders — translate payload to output row format
// ---------------------------------------------------------------------------

fn build_metric_from_payload(
    uid: &str,
    node_name: &str,
    payload: &Value,
    tags: &[String],
    now: &str,
    rows: &mut Vec<Value>,
) {
    let m = ParsedMetric::from_payload(uid, payload, tags);
    // m.name comes from __metric_attr__.name which may be absent in the payload;
    // fall back to the name column from parse/nodes (always populated).
    let name = if m.name.is_empty() {
        node_name
    } else {
        &m.name
    };
    rows.push(json!({
        "unique_id": uid,
        "name": name,
        "label": m.label,
        "metric_type": m.metric_type,
        "description": m.description,
        "package_name": m.package_name,
        "file_path": m.file_path,
        "original_file_path": m.original_file_path,
        "fqn": m.fqn,
        "type_params": m.type_params.as_ref().map(|v| v.to_string()),
        "metric_filter": m.metric_filter.as_ref().map(|v| v.to_string()),
        "time_granularity": m.time_granularity,
        "input_metric_names": m.input_metric_names,
        "depends_on_nodes": m.depends_on_nodes,
        "depends_on_macros": m.depends_on_macros,
        "group_name": m.group_name,
        "tags": m.tags,
        "meta": m.meta.as_ref().map(|v| v.to_string()),
        "config": m.config.as_ref().map(|v| v.to_string()),
        "created_at": m.created_at,
        "ingested_at": now,
    }));
}

fn build_semantic_model_from_payload(
    uid: &str,
    payload: &Value,
    now: &str,
    sm_rows: &mut Vec<Value>,
    ent_rows: &mut Vec<Value>,
    meas_rows: &mut Vec<Value>,
    dim_rows: &mut Vec<Value>,
) {
    let m = ParsedSemanticModel::from_payload(payload);
    sm_rows.push(json!({
        "unique_id": uid,
        "name": m.name,
        "model": m.model_ref,
        "label": m.label,
        "description": m.description,
        "fqn": m.fqn,
        "node_relation": m.node_relation.as_ref().map(|v| v.to_string()),
        "primary_entity": m.primary_entity,
        "defaults": m.defaults.as_ref().map(|v| v.to_string()),
        "depends_on_nodes": m.depends_on_nodes,
        "depends_on_macros": m.depends_on_macros,
        "group_name": m.group_name,
        "created_at": m.created_at,
        "ingested_at": now,
    }));
    for ent in &m.entities {
        ent_rows.push(json!({
            "unique_id": uid,
            "name": ent.name,
            "entity_type": ent.entity_type,
            "description": ent.description,
            "label": ent.label,
            "entity_role": ent.entity_role,
            "expr": ent.expr,
            "ingested_at": now,
        }));
    }
    for meas in &m.measures {
        meas_rows.push(json!({
            "unique_id": uid,
            "name": meas.name,
            "agg": meas.agg,
            "description": meas.description,
            "label": meas.label,
            "expr": meas.expr,
            "create_metric": meas.create_metric,
            "agg_time_dimension": meas.agg_time_dimension,
            "agg_params": meas.agg_params.as_ref().map(|v| v.to_string()),
            "non_additive_dimension": meas.non_additive_dimension.as_ref().map(|v| v.to_string()),
            "ingested_at": now,
        }));
    }
    for dim in &m.dimensions {
        dim_rows.push(json!({
            "unique_id": uid,
            "name": dim.name,
            "dimension_type": dim.dimension_type,
            "description": dim.description,
            "label": dim.label,
            "expr": dim.expr,
            "is_partition": dim.is_partition,
            "time_granularity": dim.time_granularity,
            "validity_params": dim.validity_params.as_ref().map(|v| v.to_string()),
            "ingested_at": now,
        }));
    }
}

fn build_saved_query_from_payload(uid: &str, payload: &Value, now: &str, rows: &mut Vec<Value>) {
    let m = ParsedSavedQuery::from_payload(payload);
    rows.push(json!({
        "unique_id": uid,
        "name": m.name,
        "label": m.label,
        "description": m.description,
        "package_name": m.package_name,
        "file_path": m.file_path,
        "original_file_path": m.original_file_path,
        "fqn": m.fqn,
        "query_params": m.query_params.as_ref().map(|v| v.to_string()),
        "exports": m.exports.as_ref().map(|v| v.to_string()),
        "depends_on_nodes": m.depends_on_nodes,
        "depends_on_macros": m.depends_on_macros,
        "tags": m.tags,
        "group_name": m.group_name,
        "created_at": m.created_at,
        "ingested_at": now,
    }));
}

fn build_exposure_from_payload(
    uid: &str,
    payload: &Value,
    tags: &[String],
    now: &str,
    rows: &mut Vec<Value>,
) {
    let m = ParsedExposure::from_payload(payload, tags);
    rows.push(json!({
        "unique_id": uid,
        "name": m.name,
        "exposure_type": m.exposure_type,
        "label": m.label,
        "owner_name": m.owner_name,
        "owner_email": m.owner_email,
        "url": m.url,
        "maturity": m.maturity,
        "description": m.description,
        "package_name": m.package_name,
        "file_path": m.file_path,
        "original_file_path": m.original_file_path,
        "fqn": m.fqn,
        "depends_on_nodes": m.depends_on_nodes,
        "depends_on_macros": m.depends_on_macros,
        "tags": m.tags,
        "created_at": m.created_at,
        "ingested_at": now,
    }));
}

fn build_group_from_payload(uid: &str, payload: &Value, now: &str, rows: &mut Vec<Value>) {
    let m = ParsedGroup::from_payload(payload);
    rows.push(json!({
        "unique_id": uid,
        "name": m.name,
        "description": m.description,
        "owner_name": m.owner_name,
        "owner_email": m.owner_email,
        "ingested_at": now,
    }));
}

fn build_macro_from_payload(uid: &str, payload: &Value, now: &str, rows: &mut Vec<Value>) {
    let m = ParsedMacro::from_payload(payload);
    rows.push(json!({
        "unique_id": uid,
        "name": m.name,
        "package_name": m.package_name,
        "file_path": m.file_path,
        "original_file_path": m.original_file_path,
        "macro_sql": m.macro_sql.as_deref().unwrap_or(""),
        "description": m.description.as_deref().unwrap_or(""),
        "depends_on_macros": m.depends_on_macros,
        "supported_languages": m.supported_languages,
        "arguments": m.arguments.as_ref().map(|v| v.to_string()),
        "docs_show": m.docs_show,
        "patch_path": m.patch_path,
        "meta": m.meta.as_ref().map(|v| v.to_string()),
        "created_at": m.created_at,
        "ingested_at": now,
    }));
}

fn build_doc_from_payload(uid: &str, payload: &Value, now: &str, rows: &mut Vec<Value>) {
    let m = ParsedDoc::from_payload(payload);
    rows.push(json!({
        "unique_id": uid,
        "name": m.name,
        "block_contents": m.block_contents,
        "ingested_at": now,
    }));
}

fn build_unit_test_from_payload(uid: &str, payload: &Value, now: &str, rows: &mut Vec<Value>) {
    let m = ParsedUnitTest::from_payload(payload);
    rows.push(json!({
        "unique_id": uid,
        "name": m.name,
        "model": m.model,
        "description": m.description,
        "package_name": m.package_name,
        "file_path": m.file_path,
        "original_file_path": m.original_file_path,
        "fqn": m.fqn,
        "given": m.given.as_ref().map(|v| v.to_string()),
        "expect": m.expect.as_ref().map(|v| v.to_string()),
        "overrides": m.overrides.as_ref().map(|v| v.to_string()),
        "versions": m.versions.as_ref().map(|v| v.to_string()),
        "version": m.version,
        "schema_name": m.schema_name,
        "depends_on_nodes": m.depends_on_nodes,
        "depends_on_macros": m.depends_on_macros,
        "ingested_at": now,
    }));
}

fn build_time_spine_from_payload(uid: &str, ts_val: &Value, now: &str, rows: &mut Vec<Value>) {
    let m = ParsedTimeSpine::from_payload(ts_val);
    rows.push(json!({
        "unique_id": uid,
        "primary_column": m.primary_column,
        "primary_granularity": m.primary_granularity,
        "custom_granularities": m.custom_granularities.as_ref().map(|v| v.to_string()),
        "node_relation": m.node_relation.as_ref().map(|v| v.to_string()),
        "ingested_at": now,
    }));
}

// ---------------------------------------------------------------------------
// Parse columns → dbt.node_columns (declared_type, description)
// ---------------------------------------------------------------------------

fn write_parse_columns(
    writer: &mut IndexWriter,
    metadata_dir: &Path,
    state: &mut IngestState,
    now: &str,
    full: bool,
    alive_ids: Option<&HashSet<String>>,
) -> Result<usize, IndexError> {
    let dir = metadata_dir.join(PARSE_COLUMNS_SUBDIR);
    if !dir.exists() {
        return Ok(0);
    }

    let epochs = epoch_layers::existing_epochs(&dir);
    let last_saved = state.last_epoch_for(PARSE_COLUMNS_SUBDIR);
    let curr_max_ep = epochs.iter().map(|(n, _)| *n).max().unwrap_or(0);
    let need_full = full || last_saved == u32::MAX || curr_max_ep < last_saved;
    let new_epochs: Vec<_> = if need_full {
        epochs
    } else {
        let last = state.last_epoch_for(PARSE_COLUMNS_SUBDIR);
        epochs.into_iter().filter(|(n, _)| *n > last).collect()
    };
    if new_epochs.is_empty() {
        return Ok(0);
    }

    let mut rows: Vec<Value> = Vec::new();
    let mut last_epoch = 0u32;
    for (epoch_num, path) in &new_epochs {
        let batches = read_parquet_batches(path)?;
        for batch in &batches {
            let uid_col = str_col(batch, "unique_id");
            let col_col = str_col(batch, "column_name");
            let type_col = str_col(batch, "declared_type");
            let desc_col = str_col(batch, "description");
            let tags_col = list_col_batch(batch, "tags");
            let ts_col = i64_col(batch, "ingested_at");
            for i in 0..batch.num_rows() {
                let Some(uid) = get_str(uid_col, i) else {
                    continue;
                };
                let ts = get_i64(ts_col, i)
                    .map(micros_to_ts)
                    .unwrap_or_else(|| now.to_string());
                let tags = get_list(tags_col, i);
                rows.push(json!({
                    "unique_id": uid,
                    "column_name": get_str(col_col, i).unwrap_or_default(),
                    "declared_type": get_str(type_col, i),
                    "description": get_str(desc_col, i),
                    "tags": tags,
                    "ingested_at": ts,
                }));
            }
        }
        last_epoch = *epoch_num;
    }

    if need_full {
        prune_by_alive(&mut rows, alive_ids);
    }
    let count = rows.len();
    if need_full {
        writer.write_dbt_table("node_columns", rows)?;
    } else {
        // Latest-wins per node: replace all columns for affected unique_ids.
        let changed_ids: HashSet<String> = rows
            .iter()
            .filter_map(|r| {
                r.get("unique_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();
        writer.write_dbt_table_merged(
            "node_columns",
            rows,
            WriteMode::MergePrune {
                key_col: "unique_id",
                valid_ids: &changed_ids,
            },
        )?;
    }
    state.set_epoch(PARSE_COLUMNS_SUBDIR, last_epoch);
    Ok(count)
}

// ---------------------------------------------------------------------------
// Parse project → dbt.project + dbt.project_vars + dbt.project_env_vars
// ---------------------------------------------------------------------------

#[allow(clippy::cognitive_complexity)]
fn write_parse_project(
    writer: &mut IndexWriter,
    metadata_dir: &Path,
    now: &str,
) -> Result<usize, IndexError> {
    use arrow_array::Array;

    // ── Primary: generation.parquet — cold-start-stable project metadata (#10623) ──
    // project_name, adapter_type, dbt_version, vars_json, env_vars_json are written
    // only on cold start (their corresponding hash fields in GenerationRow all gate
    // cache invalidation), so they belong here rather than in resolver_state.parquet.
    let mut project_name = String::new();
    let mut adapter_type = String::new();
    let mut dbt_version = String::new();
    let mut vars_json: Option<String> = None;
    let mut env_vars_json: Option<String> = None;

    macro_rules! read_str_opt {
        ($batch:expr, $field:expr, $target:expr) => {
            if $target.is_none() {
                if let Some(col) = str_col($batch, $field) {
                    if !col.is_empty() && !col.is_null(0) && !col.value(0).is_empty() {
                        $target = Some(col.value(0).to_string());
                    }
                }
            }
        };
    }

    let gen_path = metadata_dir.join(PARSE_GENERATION);
    if gen_path.exists() {
        let batches = read_parquet_batches(&gen_path)?;
        for batch in &batches {
            if project_name.is_empty() {
                if let Some(col) = str_col(batch, "project_name") {
                    if !col.is_empty() && !col.is_null(0) && !col.value(0).is_empty() {
                        project_name = col.value(0).to_string();
                    }
                }
            }
            if adapter_type.is_empty() {
                if let Some(col) = str_col(batch, "adapter_type") {
                    if !col.is_empty() && !col.is_null(0) && !col.value(0).is_empty() {
                        adapter_type = col.value(0).to_string();
                    }
                }
            }
            if dbt_version.is_empty() {
                if let Some(col) = str_col(batch, "dbt_version") {
                    if !col.is_empty() && !col.is_null(0) && !col.value(0).is_empty() {
                        dbt_version = col.value(0).to_string();
                    }
                }
            }
            read_str_opt!(batch, "vars_json", vars_json);
            read_str_opt!(batch, "env_vars_json", env_vars_json);
        }
    }

    // ── Legacy fallback: parse/project.parquet (retired KV store, pre-#10600) ──
    if project_name.is_empty() {
        let legacy_path = metadata_dir.join(PARSE_PROJECT);
        if legacy_path.exists() {
            let batches = read_parquet_batches(&legacy_path)?;
            let mut kv: HashMap<String, String> = HashMap::new();
            for batch in &batches {
                let key_col = str_col(batch, "key");
                let val_col = str_col(batch, "value");
                if let (Some(keys), Some(vals)) = (key_col, val_col) {
                    for i in 0..batch.num_rows() {
                        if !keys.is_null(i) && !vals.is_null(i) {
                            kv.insert(keys.value(i).to_string(), vals.value(i).to_string());
                        }
                    }
                }
            }
            project_name = kv.get("project_name").cloned().unwrap_or_default();
            if adapter_type.is_empty() {
                adapter_type = kv.get("adapter_type").cloned().unwrap_or_default();
            }
            if dbt_version.is_empty() {
                dbt_version = kv.get("dbt_version").cloned().unwrap_or_default();
            }
        }
    }

    if project_name.is_empty() {
        return Ok(0);
    }

    // ── resolver_state.parquet: git fields (updated every parse) ─────────────
    let mut git_sha: Option<String> = None;
    let mut git_branch: Option<String> = None;
    let mut git_is_dirty: Option<bool> = None;

    let rs_path = metadata_dir.join(PARSE_RESOLVER_STATE);
    if rs_path.exists() {
        let batches = read_parquet_batches(&rs_path)?;
        for batch in &batches {
            // vars_json / env_vars_json: fall back to resolver_state for metadata
            // dirs written before generation.parquet gained these fields (#10623).
            read_str_opt!(batch, "vars_json", vars_json);
            read_str_opt!(batch, "env_vars_json", env_vars_json);
            read_str_opt!(batch, "git_sha", git_sha);
            read_str_opt!(batch, "git_branch", git_branch);
            if git_is_dirty.is_none() {
                if let Some(col) = i32_col(batch, "git_is_dirty") {
                    if !col.is_empty() && !col.is_null(0) {
                        git_is_dirty = Some(col.value(0) != 0);
                    }
                }
            }
        }
    }

    writer.write_dbt_table(
        "project",
        vec![json!({
            "project_name": project_name,
            "dbt_version": dbt_version,
            "adapter_type": adapter_type,
            "git_sha": git_sha,
            "git_branch": git_branch,
            "git_is_dirty": git_is_dirty,
            "ingested_at": now,
        })],
    )?;

    // project_vars
    let var_rows: Vec<Value> = vars_json
        .as_deref()
        .and_then(|s| serde_json::from_str::<serde_json::Map<String, Value>>(s).ok())
        .map(|obj| {
            obj.into_iter()
                .map(|(k, v)| {
                    json!({
                        "var_name": k,
                        "var_value": v.to_string(),
                        "ingested_at": now,
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    writer.write_dbt_table("project_vars", var_rows)?;

    // project_env_vars
    let env_rows: Vec<Value> = env_vars_json
        .as_deref()
        .and_then(|s| serde_json::from_str::<serde_json::Map<String, Value>>(s).ok())
        .map(|obj| {
            obj.into_iter()
                .map(|(k, v)| {
                    let used_in: Vec<String> = v
                        .as_array()
                        .map(|a| {
                            a.iter()
                                .filter_map(|s| s.as_str())
                                .map(|s| s.to_string())
                                .collect()
                        })
                        .unwrap_or_default();
                    json!({
                        "env_var_name": k,
                        "used_in": used_in,
                        "ingested_at": now,
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    writer.write_dbt_table("project_env_vars", env_rows)?;

    Ok(1)
}

// ---------------------------------------------------------------------------
// Parse generation → dbt.generation
// ---------------------------------------------------------------------------

fn write_parse_generation(
    writer: &mut IndexWriter,
    metadata_dir: &Path,
    now: &str,
) -> Result<usize, IndexError> {
    let path = metadata_dir.join(PARSE_GENERATION);
    if !path.exists() {
        return Ok(0);
    }

    let batches = read_parquet_batches(&path)?;
    for batch in &batches {
        let ts_col = i64_col(batch, "ingested_at");
        if let Some(col) = ts_col {
            use arrow_array::Array;
            if !col.is_empty() && !col.is_null(0) {
                let ts = micros_to_ts(col.value(0));
                writer.write_dbt_table("generation", vec![json!({ "ingested_at": ts })])?;
                return Ok(1);
            }
        }
    }

    // generation.parquet exists but has no rows — treat as absent.
    // Write a sentinel using now so dbt.generation is never empty.
    writer.write_dbt_table("generation", vec![json!({ "ingested_at": now })])?;
    Ok(1)
}

// ---------------------------------------------------------------------------
// Compile columns → dbt.node_columns (inferred_type, column_index)
// Fast path: Arrow→Arrow projection, no serde_json round-trip.
// ---------------------------------------------------------------------------

fn write_compile_columns(
    writer: &mut IndexWriter,
    metadata_dir: &Path,
    state: &mut IngestState,
    _now: &str,
    full: bool,
    alive_ids: Option<&HashSet<String>>,
) -> Result<usize, IndexError> {
    let dir = metadata_dir.join(COMPILE_COLUMNS_SUBDIR);
    if !dir.exists() {
        return Ok(0);
    }

    let epochs = epoch_layers::existing_epochs(&dir);
    let last_saved = state.last_epoch_for(COMPILE_COLUMNS_SUBDIR);
    let curr_max_ep = epochs.iter().map(|(n, _)| *n).max().unwrap_or(0);
    let need_full = full || last_saved == u32::MAX || curr_max_ep < last_saved;
    let new_epochs: Vec<_> = if need_full {
        epochs
    } else {
        let last = state.last_epoch_for(COMPILE_COLUMNS_SUBDIR);
        epochs.into_iter().filter(|(n, _)| *n > last).collect()
    };
    if new_epochs.is_empty() {
        return Ok(0);
    }

    // Invariant: all columns for a given unique_id come from the same epoch (a compile
    // epoch writes all columns for all compiled nodes atomically). Later epochs supersede
    // earlier ones for the same node.
    //
    // Strategy: read epochs newest-first, track which unique_ids have been seen.
    // For each batch keep only rows whose unique_id is NOT yet seen, then mark them seen.
    // Combined with alive pruning inline — one HashSet lookup per row total.
    let out_schema = crate::parquet::schema_for("node_columns");
    let mut seen_ids: HashSet<String> = HashSet::new();
    let mut batches: Vec<arrow_array::RecordBatch> = Vec::new();

    for (_epoch_num, path) in new_epochs.iter().rev() {
        for batch in read_parquet_batches(path)? {
            use arrow_array::Array;
            let uid_col = str_col(&batch, "unique_id");
            // Single pass: build mask and update seen_ids simultaneously.
            // For each row: alive? not yet seen? → keep, mark seen.
            let mut keep = Vec::with_capacity(batch.num_rows());
            let mut any_keep = false;
            for i in 0..batch.num_rows() {
                let Some(uid) = uid_col.filter(|c| !c.is_null(i)).map(|c| c.value(i)) else {
                    keep.push(false);
                    continue;
                };
                if alive_ids.map(|ids| !ids.contains(uid)).unwrap_or(false) {
                    keep.push(false);
                    continue;
                }
                if seen_ids.contains(uid) {
                    keep.push(false);
                    continue;
                }
                seen_ids.insert(uid.to_string());
                keep.push(true);
                any_keep = true;
            }
            if !any_keep {
                continue;
            }
            let mask = arrow_array::BooleanArray::from(keep);
            if let Ok(filtered) = arrow_select::filter::filter_record_batch(&batch, &mask) {
                batches.push(filtered);
            }
        }
    }

    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    if total == 0 {
        let last = new_epochs.last().map(|(n, _)| *n).unwrap_or(0);
        state.set_epoch(COMPILE_COLUMNS_SUBDIR, last);
        return Ok(0);
    }

    let projected = project_compile_columns_batches(batches, &out_schema)?;

    writer.merge_compile_columns_into_node_columns(projected)?;

    let last = new_epochs.last().map(|(n, _)| *n).unwrap_or(0);
    state.set_epoch(COMPILE_COLUMNS_SUBDIR, last);
    Ok(total)
}

// ---------------------------------------------------------------------------
// catalog/columns — warehouse-fetched column types and comments
// ---------------------------------------------------------------------------

fn write_catalog_columns(
    writer: &mut IndexWriter,
    metadata_dir: &Path,
    state: &mut IngestState,
    _now: &str,
    full: bool,
    alive_ids: Option<&HashSet<String>>,
) -> Result<usize, IndexError> {
    let dir = metadata_dir.join(CATALOG_COLUMNS_SUBDIR);
    if !dir.exists() {
        return Ok(0);
    }

    let epochs = epoch_layers::existing_epochs(&dir);
    let last_saved = state.last_epoch_for(CATALOG_COLUMNS_SUBDIR);
    let curr_max_ep = epochs.iter().map(|(n, _)| *n).max().unwrap_or(0);
    let need_full = full || last_saved == u32::MAX || curr_max_ep < last_saved;
    let new_epochs: Vec<_> = if need_full {
        epochs
    } else {
        epochs
            .into_iter()
            .filter(|(n, _)| *n > last_saved)
            .collect()
    };
    if new_epochs.is_empty() {
        return Ok(0);
    }

    let out_schema = crate::parquet::schema_for("node_columns");
    let mut seen_ids: HashSet<String> = HashSet::new();
    let mut batches: Vec<arrow_array::RecordBatch> = Vec::new();

    for (_epoch_num, path) in new_epochs.iter().rev() {
        for batch in read_parquet_batches(path)? {
            use arrow_array::Array;
            let uid_col = str_col(&batch, "unique_id");
            let mut keep = Vec::with_capacity(batch.num_rows());
            let mut any_keep = false;
            for i in 0..batch.num_rows() {
                let Some(uid) = uid_col.filter(|c| !c.is_null(i)).map(|c| c.value(i)) else {
                    keep.push(false);
                    continue;
                };
                if alive_ids.map(|ids| !ids.contains(uid)).unwrap_or(false) {
                    keep.push(false);
                    continue;
                }
                if seen_ids.contains(uid) {
                    keep.push(false);
                    continue;
                }
                seen_ids.insert(uid.to_string());
                keep.push(true);
                any_keep = true;
            }
            if !any_keep {
                continue;
            }
            let mask = arrow_array::BooleanArray::from(keep);
            if let Ok(filtered) = arrow_select::filter::filter_record_batch(&batch, &mask) {
                batches.push(filtered);
            }
        }
    }

    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    if total == 0 {
        let last = new_epochs.last().map(|(n, _)| *n).unwrap_or(0);
        state.set_epoch(CATALOG_COLUMNS_SUBDIR, last);
        return Ok(0);
    }

    let projected = project_catalog_columns_batches(batches, &out_schema)?;

    writer.merge_catalog_columns_into_node_columns(projected)?;

    let last = new_epochs.last().map(|(n, _)| *n).unwrap_or(0);
    state.set_epoch(CATALOG_COLUMNS_SUBDIR, last);
    Ok(total)
}

fn project_catalog_columns_batches(
    batches: Vec<arrow_array::RecordBatch>,
    out_schema: &arrow_schema::SchemaRef,
) -> Result<Vec<arrow_array::RecordBatch>, IndexError> {
    use arrow_array::{Int64Array, new_null_array};
    use arrow_schema::DataType;
    use std::sync::Arc;

    batches
        .into_iter()
        .map(|batch| {
            let nrows = batch.num_rows();

            let uid_out = col_or_null(&batch, "unique_id", &DataType::Utf8, nrows);
            let cname_out = col_or_null(&batch, "column_name", &DataType::Utf8, nrows);
            let catalog_type_out = col_or_null(&batch, "catalog_type", &DataType::Utf8, nrows);
            let catalog_comment_out =
                col_or_null(&batch, "catalog_comment", &DataType::Utf8, nrows);

            // column_index: cast i32 → i64
            let idx_out: arrow_array::ArrayRef =
                if let Ok(ki) = batch.schema().index_of("column_index") {
                    let col = batch.column(ki);
                    arrow_cast::cast_with_options(
                        col,
                        &DataType::Int64,
                        &arrow_cast::CastOptions::default(),
                    )
                    .map(|a| a as arrow_array::ArrayRef)
                    .unwrap_or_else(|_| Arc::new(Int64Array::from(vec![None::<i64>; nrows])))
                } else {
                    Arc::new(Int64Array::from(vec![None::<i64>; nrows]))
                };

            // ingested_at: pass through from input
            let ingested_at: arrow_array::ArrayRef = batch
                .schema()
                .index_of("ingested_at")
                .ok()
                .map(|i| batch.column(i).clone())
                .unwrap_or_else(|| {
                    use arrow_array::TimestampMicrosecondArray;
                    let now_micros = chrono::Utc::now().timestamp_micros();
                    Arc::new(
                        TimestampMicrosecondArray::from(vec![now_micros; nrows])
                            .with_timezone("UTC"),
                    )
                });

            let null_str = new_null_array(&DataType::Utf8, nrows);
            let null_bool = new_null_array(&DataType::Boolean, nrows);
            let null_list = empty_list_array(nrows);

            let columns: Vec<arrow_array::ArrayRef> = vec![
                uid_out,                  // unique_id
                cname_out,                // column_name
                idx_out,                  // column_index
                null_str.clone(),         // declared_type
                null_str.clone(),         // inferred_type
                catalog_type_out.clone(), // catalog_type
                catalog_type_out, // data_type (initially set to catalog_type, merge refines)
                null_str.clone(), // description
                null_str.clone(), // label
                null_str.clone(), // expression
                null_bool,        // quote
                null_str.clone(), // granularity
                null_list,        // tags
                null_str.clone(), // meta
                null_str.clone(), // column_constraints
                null_str.clone(), // tests
                catalog_comment_out, // catalog_comment
                ingested_at,      // ingested_at
            ];

            arrow_array::RecordBatch::try_new(out_schema.clone(), columns)
                .map_err(|e| IndexError::Other(format!("project_catalog_columns: {e}")))
        })
        .collect()
}

/// Collect RecordBatches from a list of epoch (num, path) pairs.
fn collect_epoch_batches(
    epochs: &[(u32, std::path::PathBuf)],
) -> Result<Vec<arrow_array::RecordBatch>, IndexError> {
    let mut out = Vec::new();
    for (_n, path) in epochs {
        out.extend(read_parquet_batches(path)?);
    }
    Ok(out)
}

/// Filter batches: keep only rows whose `col_name` column value is in `alive_ids`.
/// Uses a boolean filter kernel — faster than index-based take for dense keep sets.
/// Filter batches: keep only rows whose `col_name` is in `alive_ids`.
/// Uses boolean filter kernel (faster than index take for dense keep sets).
fn filter_batches_by_alive_col(
    batches: Vec<arrow_array::RecordBatch>,
    alive_ids: Option<&HashSet<String>>,
    col_name: &str,
) -> Vec<arrow_array::RecordBatch> {
    let Some(ids) = alive_ids else { return batches };
    batches
        .into_iter()
        .filter_map(|batch| {
            let col = str_col(&batch, col_name);
            use arrow_array::Array;
            let mask: arrow_array::BooleanArray = (0..batch.num_rows())
                .map(|i| {
                    Some(
                        col.filter(|c| !c.is_null(i))
                            .map(|c| ids.contains(c.value(i)))
                            .unwrap_or(false),
                    )
                })
                .collect();
            if mask.true_count() == 0 {
                return None;
            }
            arrow_select::filter::filter_record_batch(&batch, &mask).ok()
        })
        .collect()
}

/// Project CLL input batches to the column_lineage output schema.
/// Input columns map 1:1 — just reorder/rename and fill ingested_at.
fn project_cll_batches(
    batches: Vec<arrow_array::RecordBatch>,
    out_schema: &arrow_schema::SchemaRef,
    now: &str,
) -> Result<Vec<arrow_array::RecordBatch>, IndexError> {
    use arrow_array::{StringArray, new_null_array};
    use arrow_schema::DataType;
    use std::sync::Arc;

    batches
        .into_iter()
        .map(|batch| {
            let nrows = batch.num_rows();

            let from_n = col_or_null(&batch, "from_node_unique_id", &DataType::Utf8, nrows);
            let from_c = col_or_null(&batch, "from_column_name", &DataType::Utf8, nrows);
            let to_n = col_or_null(&batch, "to_node_unique_id", &DataType::Utf8, nrows);
            let to_c = col_or_null(&batch, "to_column_name", &DataType::Utf8, nrows);
            let kind = col_or_null(&batch, "lineage_kind", &DataType::Utf8, nrows);

            // ingested_at: use existing timestamp column if present, else fill with now.
            let ingested_at: arrow_array::ArrayRef = {
                let ts_field = out_schema.field_with_name("ingested_at").ok();
                let ts_type = ts_field
                    .map(|f| f.data_type().clone())
                    .unwrap_or_else(|| DataType::Utf8);
                batch
                    .schema()
                    .index_of("ingested_at")
                    .ok()
                    .map(|i| {
                        let col = batch.column(i);
                        if col.data_type() == &ts_type {
                            col.clone()
                        } else {
                            new_null_array(&ts_type, nrows)
                        }
                    })
                    .unwrap_or_else(|| {
                        // Fill with current timestamp string — write_dbt_table will overwrite it.
                        Arc::new(StringArray::from(vec![now; nrows])) as arrow_array::ArrayRef
                    })
            };

            // Output column order must match schema_column_lineage field order:
            //   from_node_unique_id, from_column_name, to_node_unique_id, to_column_name,
            //   lineage_kind, ingested_at
            let columns: Vec<arrow_array::ArrayRef> =
                vec![from_n, from_c, to_n, to_c, kind, ingested_at];

            arrow_array::RecordBatch::try_new(out_schema.clone(), columns)
                .map_err(|e| IndexError::Other(format!("project_cll_batches: {e}")))
        })
        .collect()
}

/// Project compile/columns input batches to the node_columns output schema.
/// Maps: unique_id, column_name, column_index(i32→i64 cast), column_type→inferred_type,
/// description; all other output columns are null-filled.
fn project_compile_columns_batches(
    batches: Vec<arrow_array::RecordBatch>,
    out_schema: &arrow_schema::SchemaRef,
) -> Result<Vec<arrow_array::RecordBatch>, IndexError> {
    use arrow_array::{Int64Array, new_null_array};
    use arrow_schema::DataType;
    use std::sync::Arc;

    batches
        .into_iter()
        .map(|batch| {
            let nrows = batch.num_rows();

            // Reuse input columns zero-copy when types match.
            let uid_out = col_or_null(&batch, "unique_id", &DataType::Utf8, nrows);
            let cname_out = col_or_null(&batch, "column_name", &DataType::Utf8, nrows);
            let itype_out = col_or_null(&batch, "column_type", &DataType::Utf8, nrows);
            let desc_out = col_or_null(&batch, "description", &DataType::Utf8, nrows);

            // column_index: cast i32 → i64 (avoids per-row alloc).
            let idx_out: arrow_array::ArrayRef =
                if let Ok(ki) = batch.schema().index_of("column_index") {
                    let col = batch.column(ki);
                    arrow_cast::cast_with_options(
                        col,
                        &DataType::Int64,
                        &arrow_cast::CastOptions::default(),
                    )
                    .map(|a| a as arrow_array::ArrayRef)
                    .unwrap_or_else(|_| Arc::new(Int64Array::from(vec![None::<i64>; nrows])))
                } else {
                    Arc::new(Int64Array::from(vec![None::<i64>; nrows]))
                };

            // ingested_at: pass through from input (already TimestampMicrosecond UTC).
            let ingested_at: arrow_array::ArrayRef = batch
                .schema()
                .index_of("ingested_at")
                .ok()
                .map(|i| batch.column(i).clone())
                .unwrap_or_else(|| {
                    use arrow_array::TimestampMicrosecondArray;
                    let now_micros = chrono::Utc::now().timestamp_micros();
                    Arc::new(
                        TimestampMicrosecondArray::from(vec![now_micros; nrows])
                            .with_timezone("UTC"),
                    )
                });

            // All-null columns: use new_null_array — O(1) allocation, just a validity bitmap.
            let null_str = new_null_array(&DataType::Utf8, nrows);
            let null_bool = new_null_array(&DataType::Boolean, nrows);
            // tags: List<Utf8> non-nullable — all-zero offsets, empty values buffer.
            let null_list = empty_list_array(nrows);

            let columns: Vec<arrow_array::ArrayRef> = vec![
                uid_out,          // unique_id
                cname_out,        // column_name
                idx_out,          // column_index
                null_str.clone(), // declared_type
                itype_out,        // inferred_type
                null_str.clone(), // catalog_type
                null_str.clone(), // data_type
                desc_out,         // description
                null_str.clone(), // label
                null_str.clone(), // expression
                null_bool,        // quote
                null_str.clone(), // granularity
                null_list,        // tags
                null_str.clone(), // meta
                null_str.clone(), // column_constraints
                null_str.clone(), // tests
                null_str.clone(), // catalog_comment
                ingested_at,      // ingested_at
            ];

            arrow_array::RecordBatch::try_new(out_schema.clone(), columns)
                .map_err(|e| IndexError::Other(format!("project_compile_columns: {e}")))
        })
        .collect()
}

/// Return a column from `batch` by name if it matches `expected_type`, else a null array.
fn col_or_null(
    batch: &arrow_array::RecordBatch,
    name: &str,
    expected_type: &arrow_schema::DataType,
    nrows: usize,
) -> arrow_array::ArrayRef {
    use arrow_array::new_null_array;
    if let Ok(idx) = batch.schema().index_of(name) {
        let col = batch.column(idx);
        if col.data_type() == expected_type {
            return col.clone();
        }
    }
    new_null_array(expected_type, nrows)
}

/// Build an all-empty (non-null) List<Utf8> array of length `n`.
/// Offsets are all-zero (every list has length 0); values buffer is empty.
/// O(n) for the offsets allocation but zero per-row work — much faster than ListBuilder.
fn empty_list_array(n: usize) -> arrow_array::ArrayRef {
    use arrow_array::{ListArray, StringArray};
    use arrow_buffer::OffsetBuffer;
    use arrow_schema::{DataType, Field};
    use std::sync::Arc;

    // n+1 zero offsets: every list starts and ends at position 0 (empty).
    let offsets: arrow_buffer::ScalarBuffer<i32> = vec![0i32; n + 1].into();
    let offsets = OffsetBuffer::new(offsets);
    let values = Arc::new(StringArray::from(Vec::<Option<&str>>::new())) as arrow_array::ArrayRef;
    let field = Arc::new(Field::new("item", DataType::Utf8, true));
    Arc::new(ListArray::new(field, offsets, values, None))
}

// ---------------------------------------------------------------------------
// Compile column lineage → dbt.column_lineage
// ---------------------------------------------------------------------------

fn write_compile_cll(
    writer: &mut IndexWriter,
    metadata_dir: &Path,
    state: &mut IngestState,
    now: &str,
    full: bool,
    alive_ids: Option<&HashSet<String>>,
) -> Result<usize, IndexError> {
    let dir = metadata_dir.join(COMPILE_CLL_SUBDIR);
    if !dir.exists() {
        return Ok(0);
    }

    let epochs = epoch_layers::existing_epochs(&dir);
    let last_saved = state.last_epoch_for(COMPILE_CLL_SUBDIR);
    let curr_max_ep = epochs.iter().map(|(n, _)| *n).max().unwrap_or(0);
    let need_full = full || last_saved == u32::MAX || curr_max_ep < last_saved;
    let new_epochs: Vec<_> = if need_full {
        epochs
    } else {
        let last = state.last_epoch_for(COMPILE_CLL_SUBDIR);
        epochs.into_iter().filter(|(n, _)| *n > last).collect()
    };
    if new_epochs.is_empty() {
        return Ok(0);
    }

    let batches = collect_epoch_batches(&new_epochs)?;

    // Alive pruning on full ingest.
    let batches = if need_full && alive_ids.is_some() {
        filter_batches_by_alive_col(batches, alive_ids, "to_node_unique_id")
    } else {
        batches
    };

    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    let last = new_epochs.last().map(|(n, _)| *n).unwrap_or(0);

    if total == 0 {
        state.set_epoch(COMPILE_CLL_SUBDIR, last);
        return Ok(0);
    }

    // Project to output schema and write as Arrow — avoids JSON round-trip.
    let out_schema = crate::parquet::schema_for("column_lineage");
    let projected = project_cll_batches(batches, &out_schema, now)?;

    if need_full {
        writer.write_arrow_batches("column_lineage", projected)?;
    } else {
        // Delta: collect recomputed target ids for merge.
        let mut recomputed_targets: HashSet<String> = HashSet::new();
        for b in &projected {
            if let Some(col) = str_col(b, "to_node_unique_id") {
                use arrow_array::Array;
                for i in 0..b.num_rows() {
                    if !col.is_null(i) {
                        recomputed_targets.insert(col.value(i).to_string());
                    }
                }
            }
        }
        // Fall back to JSON merge for delta (rare path, low row count).
        let mut rows: Vec<Value> = Vec::new();
        for b in projected {
            let from_n_col = str_col(&b, "from_node_unique_id");
            let from_c_col = str_col(&b, "from_column_name");
            let to_n_col = str_col(&b, "to_node_unique_id");
            let to_c_col = str_col(&b, "to_column_name");
            let kind_col = str_col(&b, "lineage_kind");
            let ts_col_b = str_col(&b, "ingested_at");
            for i in 0..b.num_rows() {
                let Some(from_n) = get_str(from_n_col, i) else {
                    continue;
                };
                let Some(to_n) = get_str(to_n_col, i) else {
                    continue;
                };
                rows.push(json!({
                    "from_node_unique_id": from_n,
                    "from_column_name":    get_str(from_c_col, i).unwrap_or_default(),
                    "to_node_unique_id":   to_n,
                    "to_column_name":      get_str(to_c_col, i).unwrap_or_default(),
                    "lineage_kind":        get_str(kind_col, i),
                    "ingested_at":         get_str(ts_col_b, i).unwrap_or_else(|| now.to_string()),
                }));
            }
        }
        writer.write_dbt_table_merged(
            "column_lineage",
            rows,
            WriteMode::ReplaceColumnLineage {
                recomputed_targets: &recomputed_targets,
                valid_node_ids: alive_ids,
            },
        )?;
    }
    state.set_epoch(COMPILE_CLL_SUBDIR, last);
    Ok(total)
}

// ---------------------------------------------------------------------------
// Run invocations → dbt_rt.invocations
// ---------------------------------------------------------------------------

fn write_run_invocations(
    writer: &mut IndexWriter,
    metadata_dir: &Path,
    state: &mut IngestState,
    _now: &str,
    full: bool,
) -> Result<usize, IndexError> {
    #[derive(serde::Serialize)]
    struct InvocationOutputRow {
        invocation_id: String,
        command: Option<String>,
        selector: Option<String>,
        dbt_version: String,
        generated_at: Option<chrono::DateTime<chrono::Utc>>,
        elapsed_time: Option<f64>,
        args: Option<String>,
        node_count: Option<i64>,
        target_name: Option<String>,
        target_type: Option<String>,
        target_database: Option<String>,
        target_schema: Option<String>,
        target_threads: Option<i64>,
        vars_override: Option<String>,
        git_sha: Option<String>,
        git_branch: Option<String>,
        git_is_dirty: Option<bool>,
    }

    let dir = metadata_dir.join(RUN_INVOCATIONS_SUBDIR);
    if !dir.exists() {
        return Ok(0);
    }

    let epochs = epoch_layers::existing_epochs(&dir);
    let last_saved = state.last_epoch_for(RUN_INVOCATIONS_SUBDIR);
    let curr_max_ep = epochs.iter().map(|(n, _)| *n).max().unwrap_or(0);
    let need_full = full || last_saved == u32::MAX || curr_max_ep < last_saved;
    let has_new_epochs = need_full || epochs.iter().any(|(n, _)| *n > last_saved);
    if !has_new_epochs {
        return Ok(0);
    }

    let mut rows = Vec::new();
    let mut last_epoch = 0u32;
    for (epoch_num, path) in &epochs {
        let batches = read_parquet_batches(path)?;
        for batch in &batches {
            let inv_col = str_col(batch, "invocation_id");
            let cmd_col = str_col(batch, "command");
            let sel_col = str_col(batch, "selector");
            let ver_col = str_col(batch, "dbt_version");
            let nc_col = i64_col(batch, "node_count");
            let tname_col = str_col(batch, "target_name");
            let atype_col = str_col(batch, "adapter_type");
            let sha_col = str_col(batch, "git_sha");
            let branch_col = str_col(batch, "git_branch");
            let dirty_col = i32_col(batch, "git_is_dirty");
            let elapsed_col = f64_col(batch, "elapsed_secs");
            let ts_col = timestamp_micros_col(batch, "ingested_at");
            for i in 0..batch.num_rows() {
                let Some(inv) = get_str(inv_col, i) else {
                    continue;
                };
                let generated_at = get_timestamp_micros(ts_col, i).map(micros_to_datetime);
                let git_dirty = get_i32(dirty_col, i).map(|v| v == 1);
                rows.push(InvocationOutputRow {
                    invocation_id: inv,
                    command: get_str(cmd_col, i),
                    selector: get_str(sel_col, i),
                    dbt_version: get_str(ver_col, i).unwrap_or_default(),
                    generated_at,
                    elapsed_time: get_f64(elapsed_col, i),
                    args: None,
                    node_count: get_i64(nc_col, i),
                    target_name: get_str(tname_col, i),
                    target_type: get_str(atype_col, i),
                    target_database: None,
                    target_schema: None,
                    target_threads: None,
                    vars_override: None,
                    git_sha: get_str(sha_col, i),
                    git_branch: get_str(branch_col, i),
                    git_is_dirty: git_dirty,
                });
            }
        }
        last_epoch = *epoch_num;
    }

    let count = rows.len();
    writer.write_rt_items("invocations", &rows)?;
    state.set_epoch(RUN_INVOCATIONS_SUBDIR, last_epoch);
    Ok(count)
}

// ---------------------------------------------------------------------------
// Run results → dbt_rt.run_results
// ---------------------------------------------------------------------------

fn write_run_results(
    writer: &mut IndexWriter,
    metadata_dir: &Path,
    state: &mut IngestState,
    _now: &str,
    full: bool,
) -> Result<usize, IndexError> {
    #[derive(serde::Serialize)]
    struct RunResultOutputRow {
        unique_id: String,
        invocation_id: String,
        status: Option<String>,
        execution_time: Option<f64>,
        thread_id: Option<String>,
        message: Option<String>,
        failures: Option<i64>,
        compiled: Option<bool>,
        compiled_code_hash: Option<String>,
        relation_name: Option<String>,
        adapter_response: Option<String>,
        timing: Option<String>,
        batch_results: Option<String>,
        rows_affected: Option<i64>,
        created_at: Option<chrono::DateTime<chrono::Utc>>,
    }

    let dir = metadata_dir.join(RUN_RESULTS_SUBDIR);
    if !dir.exists() {
        return Ok(0);
    }

    let epochs = epoch_layers::existing_epochs(&dir);
    let last_saved = state.last_epoch_for(RUN_RESULTS_SUBDIR);
    let curr_max_ep = epochs.iter().map(|(n, _)| *n).max().unwrap_or(0);
    let need_full = full || last_saved == u32::MAX || curr_max_ep < last_saved;
    let has_new_epochs = need_full || epochs.iter().any(|(n, _)| *n > last_saved);
    if !has_new_epochs {
        return Ok(0);
    }

    let mut rows = Vec::new();
    let mut last_epoch = 0u32;
    for (epoch_num, path) in &epochs {
        let batches = read_parquet_batches(path)?;
        for batch in &batches {
            let uid_col = str_col(batch, "unique_id");
            let inv_col = str_col(batch, "invocation_id");
            let stat_col = str_col(batch, "status");
            let exec_col = f64_col(batch, "execution_time");
            let thr_col = str_col(batch, "thread_id");
            let msg_col = str_col(batch, "message");
            let fail_col = i64_col(batch, "failures");
            let chash_col = str_col(batch, "compiled_code_hash");
            let rel_col = str_col(batch, "relation_name");
            let resp_col = str_col(batch, "adapter_response");
            let tim_col = str_col(batch, "timing");
            let ts_col = timestamp_micros_col(batch, "ingested_at");
            for i in 0..batch.num_rows() {
                let Some(uid) = get_str(uid_col, i) else {
                    continue;
                };
                let Some(inv) = get_str(inv_col, i) else {
                    continue;
                };
                let created_at = get_timestamp_micros(ts_col, i).map(micros_to_datetime);
                rows.push(RunResultOutputRow {
                    unique_id: uid,
                    invocation_id: inv,
                    status: get_str(stat_col, i),
                    execution_time: get_f64(exec_col, i),
                    thread_id: get_str(thr_col, i),
                    message: get_str(msg_col, i),
                    failures: get_i64(fail_col, i),
                    compiled: None,
                    compiled_code_hash: get_str(chash_col, i),
                    relation_name: get_str(rel_col, i),
                    adapter_response: get_str(resp_col, i),
                    timing: get_str(tim_col, i),
                    batch_results: None,
                    rows_affected: None,
                    created_at,
                });
            }
        }
        last_epoch = *epoch_num;
    }

    let count = rows.len();
    writer.write_rt_items("run_results", &rows)?;
    state.set_epoch(RUN_RESULTS_SUBDIR, last_epoch);
    Ok(count)
}

// ---------------------------------------------------------------------------
// Run freshness → dbt.source_freshness
// ---------------------------------------------------------------------------

fn write_run_freshness(
    writer: &mut IndexWriter,
    metadata_dir: &Path,
    state: &mut IngestState,
    now: &str,
    full: bool,
) -> Result<usize, IndexError> {
    let dir = metadata_dir.join(RUN_FRESHNESS_SUBDIR);
    if !dir.exists() {
        return Ok(0);
    }

    let epochs = epoch_layers::existing_epochs(&dir);
    let last_saved = state.last_epoch_for(RUN_FRESHNESS_SUBDIR);
    let curr_max_ep = epochs.iter().map(|(n, _)| *n).max().unwrap_or(0);
    let need_full = full || last_saved == u32::MAX || curr_max_ep < last_saved;
    let new_epochs: Vec<_> = if need_full {
        epochs
    } else {
        let last = state.last_epoch_for(RUN_FRESHNESS_SUBDIR);
        epochs.into_iter().filter(|(n, _)| *n > last).collect()
    };
    if new_epochs.is_empty() {
        return Ok(0);
    }

    let mut rows: Vec<Value> = Vec::new();
    let mut last_epoch = 0u32;
    for (epoch_num, path) in &new_epochs {
        let batches = read_parquet_batches(path)?;
        for batch in &batches {
            let uid_col = str_col(batch, "unique_id");
            let inv_col = str_col(batch, "invocation_id");
            let stat_col = str_col(batch, "status");
            let mla_col = str_col(batch, "max_loaded_at");
            let snap_col = str_col(batch, "snapshotted_at");
            let ago_col = f64_col(batch, "max_loaded_at_time_ago");
            let exec_col = f64_col(batch, "execution_time");
            let wc_col = i64_col(batch, "warn_after_count");
            let wp_col = str_col(batch, "warn_after_period");
            let ec_col = i64_col(batch, "error_after_count");
            let ep_col = str_col(batch, "error_after_period");
            let ts_col = i64_col(batch, "ingested_at");
            for i in 0..batch.num_rows() {
                let Some(uid) = get_str(uid_col, i) else {
                    continue;
                };
                let ts = get_i64(ts_col, i)
                    .map(micros_to_ts)
                    .unwrap_or_else(|| now.to_string());
                rows.push(json!({
                    "unique_id": uid,
                    "invocation_id": get_str(inv_col, i),
                    "status": get_str(stat_col, i),
                    "max_loaded_at": get_str(mla_col, i),
                    "snapshotted_at": get_str(snap_col, i),
                    "max_loaded_at_time_ago": get_f64(ago_col, i),
                    "execution_time": get_f64(exec_col, i),
                    "warn_after_count": get_i64(wc_col, i),
                    "warn_after_period": get_str(wp_col, i),
                    "error_after_count": get_i64(ec_col, i),
                    "error_after_period": get_str(ep_col, i),
                    "ingested_at": ts,
                }));
            }
        }
        last_epoch = *epoch_num;
    }

    let count = rows.len();
    if need_full {
        writer.write_dbt_table("source_freshness", rows)?;
    } else {
        writer.write_dbt_table_append("source_freshness", rows, "unique_id")?;
    }
    state.set_epoch(RUN_FRESHNESS_SUBDIR, last_epoch);
    Ok(count)
}

// ---------------------------------------------------------------------------
// Column stats → dbt.column_stats
// ---------------------------------------------------------------------------

fn write_column_stats(
    writer: &mut IndexWriter,
    index_dir: &Path,
    now: &str,
) -> Result<usize, IndexError> {
    let rr_path = index_dir.join("dbt_rt.run_results.parquet");
    let nc_path = index_dir.join("dbt.node_columns.parquet");
    if !rr_path.exists() || !nc_path.exists() {
        return Ok(0);
    }

    let mut row_counts: HashMap<String, i64> = HashMap::new();
    for batch in read_parquet_batches(&rr_path)? {
        let uid_col = str_col(&batch, "unique_id");
        let ra_col = i64_col(&batch, "rows_affected");
        for i in 0..batch.num_rows() {
            let Some(uid) = get_str(uid_col, i) else {
                continue;
            };
            let Some(ra) = get_i64(ra_col, i) else {
                continue;
            };
            row_counts.insert(uid, ra);
        }
    }
    if row_counts.is_empty() {
        return Ok(0);
    }

    let mut entries: Vec<(String, String)> = Vec::new();
    for batch in read_parquet_batches(&nc_path)? {
        let uid_col = str_col(&batch, "unique_id");
        let col_name_col = str_col(&batch, "column_name");
        for i in 0..batch.num_rows() {
            let Some(uid) = get_str(uid_col, i) else {
                continue;
            };
            let Some(col) = get_str(col_name_col, i) else {
                continue;
            };
            if row_counts.contains_key(&uid) {
                entries.push((uid, col));
            }
        }
    }

    let typed_rows: Vec<crate::parquet::ColumnStatRow<'_>> = entries
        .iter()
        .map(|(uid, col)| crate::parquet::ColumnStatRow {
            unique_id: uid.as_str(),
            column_name: Some(col.as_str()),
            column_type: None,
            row_count: row_counts.get(uid.as_str()).copied(),
            distinct_count: None,
            null_pct: None,
            min_value: None,
            max_value: None,
            avg_value: None,
            std_value: None,
            q25: None,
            q50: None,
            q75: None,
            top_values: None,
            ingested_at: now,
        })
        .collect();

    let count = typed_rows.len();
    writer.write_dbt_items("column_stats", &typed_rows)?;
    Ok(count)
}

// ---------------------------------------------------------------------------
// Seed catalog stats → dbt.catalog_tables + dbt.catalog_stats
// ---------------------------------------------------------------------------

fn write_seed_catalog_stats(
    writer: &mut IndexWriter,
    index_dir: &Path,
    metadata_dir: &Path,
    now: &str,
) -> Result<usize, IndexError> {
    let nodes_path = index_dir.join("dbt.nodes.parquet");
    if !nodes_path.exists() {
        return Ok(0);
    }
    let target_dir = metadata_dir.parent().unwrap_or(metadata_dir);
    let data_dir = target_dir.join("data");
    if !data_dir.exists() {
        return Ok(0);
    }

    let mut catalog_table_rows: Vec<Value> = Vec::new();
    let mut catalog_stat_rows: Vec<Value> = Vec::new();

    for batch in read_parquet_batches(&nodes_path)? {
        let uid_col = str_col(&batch, "unique_id");
        let rt_col = str_col(&batch, "resource_type");
        let rn_col = str_col(&batch, "relation_name");

        for i in 0..batch.num_rows() {
            let Some(resource_type) = get_str(rt_col, i) else {
                continue;
            };
            if resource_type != "seed" {
                continue;
            }
            let Some(uid) = get_str(uid_col, i) else {
                continue;
            };
            let Some(relation_name) = get_str(rn_col, i) else {
                continue;
            };

            let parts: Vec<&str> = relation_name.splitn(3, '.').collect();
            if parts.len() != 3 {
                continue;
            }
            let data_path = data_dir
                .join(parts[0])
                .join(parts[1])
                .join(parts[2])
                .join("output.parquet");
            if !data_path.exists() {
                continue;
            }

            let row_count = read_parquet_batches(&data_path)?
                .iter()
                .map(|b| b.num_rows() as i64)
                .sum::<i64>();

            catalog_table_rows.push(json!({
                "unique_id": uid,
                "table_type": "TABLE",
                "database_name": parts[0],
                "schema_name": parts[1],
                "table_name": parts[2],
                "table_owner": null,
                "table_comment": null,
                "ingested_at": now,
            }));
            catalog_stat_rows.push(json!({
                "unique_id": uid,
                "stat_id": "row_count",
                "stat_label": "Row Count",
                "stat_value": row_count.to_string(),
                "description": "Row count derived from seed data parquet",
                "include_in_stats": true,
                "ingested_at": now,
            }));
        }
    }

    let count = catalog_stat_rows.len();
    if count > 0 {
        writer.write_dbt_table("catalog_tables", catalog_table_rows)?;
        writer.write_dbt_table("catalog_stats", catalog_stat_rows)?;
    }
    Ok(count)
}

// ---------------------------------------------------------------------------
// Warehouse catalog stats → dbt.catalog_tables + dbt.catalog_stats
// ---------------------------------------------------------------------------

fn write_warehouse_catalog_stats(
    writer: &mut IndexWriter,
    metadata_dir: &Path,
    state: &mut IngestState,
    now: &str,
    full: bool,
) -> Result<usize, IndexError> {
    let dir = metadata_dir.join(RUN_CATALOG_STATS_SUBDIR);
    if !dir.exists() {
        return Ok(0);
    }

    let epochs = epoch_layers::existing_epochs(&dir);
    let last_saved = state.last_epoch_for(RUN_CATALOG_STATS_SUBDIR);
    let curr_max_ep = epochs.iter().map(|(n, _)| *n).max().unwrap_or(0);
    let need_full = full || last_saved == u32::MAX || curr_max_ep < last_saved;
    let new_epochs: Vec<_> = if need_full {
        epochs
    } else {
        let last = state.last_epoch_for(RUN_CATALOG_STATS_SUBDIR);
        epochs.into_iter().filter(|(n, _)| *n > last).collect()
    };
    if new_epochs.is_empty() {
        return Ok(0);
    }

    let mut catalog_table_rows: Vec<Value> = Vec::new();
    let mut catalog_stat_rows: Vec<Value> = Vec::new();
    let mut last_epoch = 0u32;

    for (epoch_num, path) in &new_epochs {
        let batches = read_parquet_batches(path)?;
        for batch in &batches {
            let uid_col = str_col(batch, "unique_id");
            let tt_col = str_col(batch, "table_type");
            let owner_col = str_col(batch, "table_owner");
            let db_col = str_col(batch, "database_name");
            let schema_col = str_col(batch, "schema_name");
            let tname_col = str_col(batch, "table_name");
            let rc_col = i64_col(batch, "row_count");
            let bytes_col = i64_col(batch, "bytes");
            let lm_col = str_col(batch, "last_modified");

            for i in 0..batch.num_rows() {
                let Some(uid) = get_str(uid_col, i) else {
                    continue;
                };

                let table_type = get_str(tt_col, i);
                let table_owner = get_str(owner_col, i);
                let database_name = get_str(db_col, i);
                let schema_name = get_str(schema_col, i);
                let table_name = get_str(tname_col, i);
                let row_count = get_i64(rc_col, i);
                let bytes = get_i64(bytes_col, i);
                let last_modified = get_str(lm_col, i);

                catalog_table_rows.push(json!({
                    "unique_id": uid,
                    "table_type": table_type,
                    "database_name": database_name,
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "table_owner": table_owner,
                    "table_comment": null,
                    "ingested_at": now,
                }));

                if let Some(rc) = row_count {
                    catalog_stat_rows.push(json!({
                        "unique_id": uid,
                        "stat_id": "row_count",
                        "stat_label": "Row Count",
                        "stat_value": rc.to_string(),
                        "description": "Row count from warehouse catalog",
                        "include_in_stats": true,
                        "ingested_at": now,
                    }));
                }
                if let Some(b) = bytes {
                    catalog_stat_rows.push(json!({
                        "unique_id": uid,
                        "stat_id": "bytes",
                        "stat_label": "Bytes",
                        "stat_value": b.to_string(),
                        "description": "Table size in bytes from warehouse catalog",
                        "include_in_stats": true,
                        "ingested_at": now,
                    }));
                }
                if let Some(lm) = last_modified {
                    catalog_stat_rows.push(json!({
                        "unique_id": uid,
                        "stat_id": "last_modified",
                        "stat_label": "Last Modified",
                        "stat_value": lm,
                        "description": "Last modified timestamp from warehouse catalog",
                        "include_in_stats": true,
                        "ingested_at": now,
                    }));
                }
            }
        }
        last_epoch = *epoch_num;
    }

    let count = catalog_stat_rows.len();
    if need_full {
        writer.write_dbt_table("catalog_tables", catalog_table_rows)?;
        writer.write_dbt_table("catalog_stats", catalog_stat_rows)?;
    } else {
        writer.write_dbt_table_append("catalog_tables", catalog_table_rows, "unique_id")?;
        writer.write_dbt_table_append("catalog_stats", catalog_stat_rows, "unique_id")?;
    }
    state.set_epoch(RUN_CATALOG_STATS_SUBDIR, last_epoch);
    Ok(count)
}
