//! Pure-Rust parquet-backed incremental parse cache.
//!
//! Replaces `duckdb_incremental.rs` with zero runtime dependencies on DuckDB / xdbc.
//!
//! # Layout
//!
//! ```text
//! target/parse_state/
//!   project.parquet              — project-level key/value pairs (profile, vars, deps, kinds, …)
//!   filestamps.parquet           — per-package file timestamps
//!   alive.parquet                — snapshot of all currently-alive unique_ids
//!   nodes/v1_0.parquet           — base epoch (or compacted)
//!   nodes/v1_1.parquet           — delta epoch
//!   nodes/v1_2.parquet           …
//! ```
//!
//! # Epoch append strategy for nodes
//!
//! - Cold start (`changed_nodes = None`) or full-rewrite: write `nodes/0.parquet`,
//!   delete all other epoch files.
//! - Incremental (`changed_nodes = Some(set)`): write a new `nodes/N.parquet`
//!   with only the changed rows.
//! - Read: collect all epoch files in order, latest epoch wins by `unique_id`.
//! - Compact: when epoch count > `COMPACT_THRESHOLD`, merge all into `nodes/0.parquet`
//!   and delete the rest.
//!
//! # Liveness invariant
//!
//! A node is alive if and only if its `unique_id` appears in the latest-epoch-wins
//! merge of all epoch files. There are no tombstones. This is safe because any event
//! that can delete a node (file deleted, `.yml` changed, macro changed, config changed)
//! triggers a FullParse, which rewrites `nodes/0.parquet` from scratch containing only
//! the currently-alive nodes. Absence from the merged view means the node was deleted.
//! The incremental path (delta epochs) is only taken when a model/analysis `.sql` file
//! changes — a 1:1 mapping to a single node — so no deletion can occur on that path.
//!
//! # No SQL, no DuckDB
//!
//! All filtering and graph traversal that used SQL is done in Rust over the
//! deserialized row vectors.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

// ── timing helper (enabled via DBT_PP_TIMING=1) ───────────────────────────────
fn t(label: &str, start: Instant) {
    if std::env::var_os("DBT_PP_TIMING").is_some() {
        eprintln!(
            "[pp] {:>7.2}ms  {label}",
            start.elapsed().as_secs_f64() * 1000.0
        );
    }
}

use arrow::datatypes::{DataType, Field, FieldRef};
use parquet::arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};
use serde::{Deserialize, Serialize};

use dbt_schemas::{
    schemas::{
        DbtAnalysis, DbtExposure, DbtFunction, DbtModel, DbtSeed, DbtSnapshot, DbtSource, DbtTest,
        DbtUnitTest, Nodes,
        macros::{DbtDocsMacro, DbtMacro, MacroArgument},
        manifest::{DbtMetric, DbtSavedQuery, DbtSemanticModel},
        nodes::DbtGroup,
    },
    state::{DbtPackage, Macros, ResourcePathKind},
};

use crate::partial_parse::PackageSnapshot;

// ── constants ─────────────────────────────────────────────────────────────────

pub const CACHE_DIR_NAME: &str = "parse_state";

/// Compact epoch files into one when this many delta files exist.
const COMPACT_THRESHOLD: u32 = 8;

const SCHEMA_VERSION: u32 = 1;

// ── path helpers ──────────────────────────────────────────────────────────────

pub fn cache_dir(out_dir: &Path) -> PathBuf {
    out_dir.join(CACHE_DIR_NAME)
}

pub(crate) fn project_path(dir: &Path) -> PathBuf {
    dir.join("project.parquet")
}

fn filestamps_path(dir: &Path) -> PathBuf {
    dir.join("filestamps.parquet")
}

pub(crate) fn nodes_dir(dir: &Path) -> PathBuf {
    dir.join("nodes")
}

fn version_prefix() -> String {
    format!("v{}_", SCHEMA_VERSION)
}

fn node_epoch_path(dir: &Path, epoch: u32) -> PathBuf {
    nodes_dir(dir).join(format!("{}{epoch}.parquet", version_prefix()))
}

/// Return sorted list of existing epoch numbers from the nodes/ subdirectory.
fn existing_epochs(dir: &Path) -> Vec<u32> {
    let prefix = version_prefix();
    dbt_metadata_parquet::epoch_io::existing_epochs(&nodes_dir(dir), &prefix)
        .into_iter()
        .map(|(n, _)| n)
        .collect()
}

/// Returns `(epoch_number, path)` pairs for the nodes directory.
pub(crate) fn existing_epoch_paths(dir: &Path) -> Vec<(u32, PathBuf)> {
    let prefix = version_prefix();
    dbt_metadata_parquet::epoch_io::existing_epochs(&nodes_dir(dir), &prefix)
}

fn next_epoch(dir: &Path) -> u32 {
    let prefix = version_prefix();
    dbt_metadata_parquet::epoch_io::next_epoch(&nodes_dir(dir), &prefix)
}

pub(crate) fn system_time_to_nanos(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_nanos() as u64
}

// ── row types (Serialize + Deserialize → serde_arrow) ────────────────────────

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct KvRow {
    pub key: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct FilestampRow {
    package_name: String,
    package_root: String,
    path_kind: String,
    path: String,
    mtime_ns: i64,
    #[serde(default)]
    ingested_at: i64,
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub(crate) struct NodeRow {
    pub unique_id: String,
    pub is_disabled: i32,
    pub kind: String,
    pub name: String,
    pub package_name: String,
    pub original_path: String,
    pub fqn: String,
    pub tags: String,
    pub depends_on: String,
    pub uses_graph: i32,
    pub materialization: String,
    pub payload: String,
    pub ingested_at: i64,
    pub extended_model: i32,
    // Macro-only columns: fields that have #[serde(skip*)] so they are absent from the
    // JSON payload. Non-macro rows carry empty string defaults and ignore these on read.
    pub macro_arguments_json: String, // JSON array of MacroArgument — "" for non-macros
    pub macro_span_json: String,      // JSON-encoded Span (all 6 fields) — "" if absent
    pub macro_name_span_json: String, // JSON-encoded macro_name_span — "" if absent
    // Promoted fields: redundant indexes into payload for DuckDB filter pushdown.
    // Nullable (Option) — backward-compatible with old epoch files (serde_arrow reads NULL).
    pub description: Option<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub alias: Option<String>,
    pub relation_name: Option<String>,
    pub access: Option<String>,
    pub group_name: Option<String>,
    pub source_name: Option<String>,
    pub identifier: Option<String>,
}

/// Index-only view of NodeRow — columns needed for selector evaluation, excluding `payload`.
/// Used by resolve_unique_ids_from_index with column projection so the
/// payload column bytes are never read from disk.
#[derive(Deserialize)]
pub(crate) struct NodeIndexRow {
    pub unique_id: String,
    pub original_path: String,
    pub kind: String,
    pub name: String,
    pub package_name: String,
    pub fqn: String,
    pub tags: String,
    pub depends_on: String,
    /// Kept for future lazy-load logic that distinguishes ephemeral / view materializations.
    #[allow(dead_code)]
    pub materialization: String,
}

// ── parquet read/write helpers ─────────────────────────────────────────────────

fn str_field(name: &str) -> FieldRef {
    Arc::new(Field::new(name, DataType::Utf8, false))
}

fn i32_field(name: &str) -> FieldRef {
    Arc::new(Field::new(name, DataType::Int32, false))
}

fn i64_field(name: &str) -> FieldRef {
    Arc::new(Field::new(name, DataType::Int64, false))
}

fn write_rows_with_fields<T: Serialize>(path: &Path, fields: &[FieldRef], rows: &[T]) -> bool {
    let plain_fields: Vec<Field> = fields.iter().map(|f| f.as_ref().clone()).collect();
    dbt_metadata_parquet::epoch_io::write_rows(path, &plain_fields, rows).is_ok()
}

fn read_rows<T: serde::de::DeserializeOwned>(path: &Path) -> Vec<T> {
    dbt_metadata_parquet::epoch_io::read_rows(path)
}

fn kv_fields() -> Vec<FieldRef> {
    vec![str_field("key"), str_field("value")]
}

fn filestamp_fields() -> Vec<FieldRef> {
    vec![
        str_field("package_name"),
        str_field("package_root"),
        str_field("path_kind"),
        str_field("path"),
        i64_field("mtime_ns"),
        i64_field("ingested_at"),
    ]
}

fn nullable_str_field(name: &str) -> FieldRef {
    Arc::new(Field::new(name, DataType::Utf8, true))
}

fn node_fields() -> Vec<FieldRef> {
    vec![
        str_field("unique_id"),
        i32_field("is_disabled"),
        str_field("kind"),
        str_field("name"),
        str_field("package_name"),
        str_field("original_path"),
        str_field("fqn"),
        str_field("tags"),
        str_field("depends_on"),
        i32_field("uses_graph"),
        str_field("materialization"),
        str_field("payload"),
        i64_field("ingested_at"),
        i32_field("extended_model"),
        str_field("macro_arguments_json"),
        str_field("macro_span_json"),
        str_field("macro_name_span_json"),
        nullable_str_field("description"),
        nullable_str_field("database"),
        nullable_str_field("schema"),
        nullable_str_field("alias"),
        nullable_str_field("relation_name"),
        nullable_str_field("access"),
        nullable_str_field("group_name"),
        nullable_str_field("source_name"),
        nullable_str_field("identifier"),
    ]
}

// ── node row builders ─────────────────────────────────────────────────────────

fn node_row_from_trait<N>(uid: &str, node: &N, kind: &str, is_disabled: i32) -> NodeRow
where
    N: dbt_schemas::schemas::nodes::InternalDbtNode + Serialize,
{
    let c = node.common();
    let b = node.base();
    let fqn = serde_json::to_string(&c.fqn).unwrap_or_else(|_| "[]".into());
    let tags = serde_json::to_string(&c.tags).unwrap_or_else(|_| "[]".into());
    let deps: Vec<&String> = b.depends_on.nodes.iter().collect();
    let depends_on = serde_json::to_string(&deps).unwrap_or_else(|_| "[]".into());
    let uses_graph = if c.raw_code.as_deref().unwrap_or("").contains("graph") {
        1
    } else {
        0
    };
    let materialization = b.materialized.to_string();
    let payload = serde_json::to_string(node).unwrap_or_else(|_| "{}".into());
    let description = c.description.clone();
    let database = if b.database.is_empty() {
        None
    } else {
        Some(b.database.clone())
    };
    let schema = if b.schema.is_empty() {
        None
    } else {
        Some(b.schema.clone())
    };
    let alias = if b.alias.is_empty() {
        None
    } else {
        Some(b.alias.clone())
    };
    let group_name = node.get_group();
    NodeRow {
        unique_id: uid.to_string(),
        is_disabled,
        kind: kind.to_string(),
        name: c.name.clone(),
        package_name: c.package_name.clone(),
        original_path: c.original_file_path.display().to_string(),
        fqn,
        tags,
        depends_on,
        uses_graph,
        materialization,
        payload,
        extended_model: node.is_extended_model() as i32,
        description,
        database,
        schema,
        alias,
        relation_name: b.relation_name.clone(),
        group_name,
        ..Default::default()
    }
}

fn node_row_from_group(uid: &str, node: &DbtGroup, is_disabled: i32) -> NodeRow {
    let c = &node.__common_attr__;
    let fqn = serde_json::to_string(&c.fqn).unwrap_or_else(|_| "[]".into());
    let tags = serde_json::to_string(&c.tags).unwrap_or_else(|_| "[]".into());
    let deps: Vec<&String> = node.__base_attr__.depends_on.nodes.iter().collect();
    let depends_on = serde_json::to_string(&deps).unwrap_or_else(|_| "[]".into());
    let uses_graph = if c.raw_code.as_deref().unwrap_or("").contains("graph") {
        1
    } else {
        0
    };
    let payload = serde_json::to_string(node).unwrap_or_else(|_| "{}".into());
    NodeRow {
        unique_id: uid.to_string(),
        is_disabled,
        kind: "group".into(),
        name: c.name.clone(),
        package_name: c.package_name.clone(),
        original_path: c.original_file_path.display().to_string(),
        fqn,
        tags,
        depends_on,
        uses_graph,
        payload,
        ..Default::default()
    }
}

fn node_row_from_macro(uid: &str, node: &DbtMacro, is_disabled: i32) -> NodeRow {
    let deps: Vec<&String> = node.depends_on.macros.iter().collect();
    let depends_on = serde_json::to_string(&deps).unwrap_or_else(|_| "[]".into());
    let uses_graph = if node.macro_sql.contains("graph") {
        1
    } else {
        0
    };
    let payload = serde_json::to_string(node).unwrap_or_else(|_| "{}".into());
    let macro_arguments_json =
        serde_json::to_string(&node.arguments).unwrap_or_else(|_| "[]".into());
    let macro_span_json = node
        .span
        .as_ref()
        .and_then(|s| serde_json::to_string(s).ok())
        .unwrap_or_default();
    let macro_name_span_json = node
        .macro_name_span
        .as_ref()
        .and_then(|s| serde_json::to_string(s).ok())
        .unwrap_or_default();
    NodeRow {
        unique_id: uid.to_string(),
        is_disabled,
        kind: "macro".into(),
        name: node.name.clone(),
        package_name: node.package_name.clone(),
        original_path: node.original_file_path.display().to_string(),
        fqn: "[]".into(),
        tags: "[]".into(),
        depends_on,
        uses_graph,
        payload,
        macro_arguments_json,
        macro_span_json,
        macro_name_span_json,
        ..Default::default()
    }
}

fn node_row_from_docs_macro(uid: &str, node: &DbtDocsMacro, is_disabled: i32) -> NodeRow {
    let payload = serde_json::to_string(node).unwrap_or_else(|_| "{}".into());
    NodeRow {
        unique_id: uid.to_string(),
        is_disabled,
        kind: "docs_macro".into(),
        name: node.name.clone(),
        package_name: node.package_name.clone(),
        original_path: node.original_file_path.display().to_string(),
        fqn: "[]".into(),
        tags: "[]".into(),
        depends_on: "[]".into(),
        payload,
        ..Default::default()
    }
}

#[allow(clippy::cognitive_complexity)]
fn collect_all_rows(nodes: &Nodes, disabled_nodes: &Nodes, macros: &Macros) -> Vec<NodeRow> {
    let mut rows = Vec::new();
    macro_rules! push_trait {
        ($map:expr, $kind:literal, $dis:expr) => {
            for (uid, node) in $map.iter() {
                rows.push(node_row_from_trait(uid, node.as_ref(), $kind, $dis));
            }
        };
    }
    push_trait!(&nodes.models, "model", 0);
    push_trait!(&nodes.seeds, "seed", 0);
    push_trait!(&nodes.tests, "test", 0);
    push_trait!(&nodes.unit_tests, "unit_test", 0);
    push_trait!(&nodes.sources, "source", 0);
    push_trait!(&nodes.snapshots, "snapshot", 0);
    push_trait!(&nodes.analyses, "analysis", 0);
    push_trait!(&nodes.exposures, "exposure", 0);
    push_trait!(&nodes.semantic_models, "semantic_model", 0);
    push_trait!(&nodes.metrics, "metric", 0);
    push_trait!(&nodes.saved_queries, "saved_query", 0);
    push_trait!(&nodes.functions, "function", 0);
    for (uid, node) in nodes.groups.iter() {
        rows.push(node_row_from_group(uid, node.as_ref(), 0));
    }
    for (uid, node) in nodes.macros.iter() {
        rows.push(node_row_from_macro(uid, node.as_ref(), 0));
    }
    push_trait!(&disabled_nodes.models, "model", 1);
    push_trait!(&disabled_nodes.seeds, "seed", 1);
    push_trait!(&disabled_nodes.tests, "test", 1);
    push_trait!(&disabled_nodes.unit_tests, "unit_test", 1);
    push_trait!(&disabled_nodes.sources, "source", 1);
    push_trait!(&disabled_nodes.snapshots, "snapshot", 1);
    push_trait!(&disabled_nodes.analyses, "analysis", 1);
    push_trait!(&disabled_nodes.exposures, "exposure", 1);
    push_trait!(&disabled_nodes.semantic_models, "semantic_model", 1);
    push_trait!(&disabled_nodes.metrics, "metric", 1);
    push_trait!(&disabled_nodes.saved_queries, "saved_query", 1);
    push_trait!(&disabled_nodes.functions, "function", 1);
    for (uid, node) in disabled_nodes.groups.iter() {
        rows.push(node_row_from_group(uid, node.as_ref(), 1));
    }
    for (uid, node) in disabled_nodes.macros.iter() {
        rows.push(node_row_from_macro(uid, node.as_ref(), 1));
    }
    for (uid, node) in macros.docs_macros.iter() {
        rows.push(node_row_from_docs_macro(uid, node, 0));
    }
    rows
}

#[allow(clippy::cognitive_complexity)]
fn collect_delta_rows(
    nodes: &Nodes,
    disabled_nodes: &Nodes,
    macros: &Macros,
    changed: &HashSet<String>,
) -> Vec<NodeRow> {
    let mut rows = Vec::new();
    macro_rules! push_if_changed {
        ($map:expr, $kind:literal, $dis:expr) => {
            for (uid, node) in $map.iter() {
                if changed.contains(uid.as_str()) {
                    rows.push(node_row_from_trait(uid, node.as_ref(), $kind, $dis));
                }
            }
        };
    }
    push_if_changed!(&nodes.models, "model", 0);
    push_if_changed!(&nodes.seeds, "seed", 0);
    push_if_changed!(&nodes.tests, "test", 0);
    push_if_changed!(&nodes.unit_tests, "unit_test", 0);
    push_if_changed!(&nodes.sources, "source", 0);
    push_if_changed!(&nodes.snapshots, "snapshot", 0);
    push_if_changed!(&nodes.analyses, "analysis", 0);
    push_if_changed!(&nodes.exposures, "exposure", 0);
    push_if_changed!(&nodes.semantic_models, "semantic_model", 0);
    push_if_changed!(&nodes.metrics, "metric", 0);
    push_if_changed!(&nodes.saved_queries, "saved_query", 0);
    push_if_changed!(&nodes.functions, "function", 0);
    for (uid, node) in nodes.groups.iter() {
        if changed.contains(uid.as_str()) {
            rows.push(node_row_from_group(uid, node.as_ref(), 0));
        }
    }
    for (uid, node) in nodes.macros.iter() {
        if changed.contains(uid.as_str()) {
            rows.push(node_row_from_macro(uid, node.as_ref(), 0));
        }
    }
    push_if_changed!(&disabled_nodes.models, "model", 1);
    push_if_changed!(&disabled_nodes.seeds, "seed", 1);
    push_if_changed!(&disabled_nodes.tests, "test", 1);
    push_if_changed!(&disabled_nodes.unit_tests, "unit_test", 1);
    push_if_changed!(&disabled_nodes.sources, "source", 1);
    push_if_changed!(&disabled_nodes.snapshots, "snapshot", 1);
    push_if_changed!(&disabled_nodes.analyses, "analysis", 1);
    push_if_changed!(&disabled_nodes.exposures, "exposure", 1);
    push_if_changed!(&disabled_nodes.semantic_models, "semantic_model", 1);
    push_if_changed!(&disabled_nodes.metrics, "metric", 1);
    push_if_changed!(&disabled_nodes.saved_queries, "saved_query", 1);
    push_if_changed!(&disabled_nodes.functions, "function", 1);
    for (uid, node) in disabled_nodes.groups.iter() {
        if changed.contains(uid.as_str()) {
            rows.push(node_row_from_group(uid, node.as_ref(), 1));
        }
    }
    for (uid, node) in disabled_nodes.macros.iter() {
        if changed.contains(uid.as_str()) {
            rows.push(node_row_from_macro(uid, node.as_ref(), 1));
        }
    }
    for (uid, node) in macros.docs_macros.iter() {
        if changed.contains(uid.as_str()) {
            rows.push(node_row_from_docs_macro(uid, node, 0));
        }
    }
    rows
}

// ── alive collection ─────────────────────────────────────────────────────────

#[allow(clippy::cognitive_complexity)]
fn collect_alive_rows(
    nodes: &Nodes,
    disabled_nodes: &Nodes,
    macros: &Macros,
    ingested_at: i64,
) -> Vec<dbt_metadata_parquet::parse_alive::AliveRow> {
    use dbt_metadata_parquet::parse_alive::AliveRow;
    let mut rows = Vec::new();
    macro_rules! push_alive {
        ($map:expr, $kind:literal) => {
            for uid in $map.keys() {
                rows.push(AliveRow {
                    unique_id: uid.clone(),
                    resource_type: $kind.to_string(),
                    ingested_at,
                });
            }
        };
    }
    push_alive!(&nodes.models, "model");
    push_alive!(&nodes.seeds, "seed");
    push_alive!(&nodes.tests, "test");
    push_alive!(&nodes.unit_tests, "unit_test");
    push_alive!(&nodes.sources, "source");
    push_alive!(&nodes.snapshots, "snapshot");
    push_alive!(&nodes.analyses, "analysis");
    push_alive!(&nodes.exposures, "exposure");
    push_alive!(&nodes.semantic_models, "semantic_model");
    push_alive!(&nodes.metrics, "metric");
    push_alive!(&nodes.saved_queries, "saved_query");
    push_alive!(&nodes.groups, "group");
    push_alive!(&nodes.functions, "function");
    push_alive!(&nodes.macros, "macro");
    push_alive!(&disabled_nodes.models, "model");
    push_alive!(&disabled_nodes.seeds, "seed");
    push_alive!(&disabled_nodes.tests, "test");
    push_alive!(&disabled_nodes.unit_tests, "unit_test");
    push_alive!(&disabled_nodes.sources, "source");
    push_alive!(&disabled_nodes.snapshots, "snapshot");
    push_alive!(&disabled_nodes.analyses, "analysis");
    push_alive!(&disabled_nodes.exposures, "exposure");
    push_alive!(&disabled_nodes.semantic_models, "semantic_model");
    push_alive!(&disabled_nodes.metrics, "metric");
    push_alive!(&disabled_nodes.saved_queries, "saved_query");
    push_alive!(&disabled_nodes.functions, "function");
    push_alive!(&disabled_nodes.groups, "group");
    push_alive!(&disabled_nodes.macros, "macro");
    for uid in macros.docs_macros.keys() {
        rows.push(AliveRow {
            unique_id: uid.clone(),
            resource_type: "doc".to_string(),
            ingested_at,
        });
    }
    rows
}

// ── epoch compaction ──────────────────────────────────────────────────────────

/// Merge all epoch files into `nodes/0.parquet`, delete the rest.
fn compact(dir: &Path) {
    let epochs = existing_epochs(dir);
    if epochs.len() <= 1 {
        return;
    }
    // Read all epochs in order; latest epoch wins by unique_id.
    // We build a HashMap keyed by unique_id; later epochs overwrite earlier ones.
    let mut by_id: HashMap<String, NodeRow> = HashMap::new();
    for epoch in &epochs {
        for row in read_rows::<NodeRow>(&node_epoch_path(dir, *epoch)) {
            by_id.insert(row.unique_id.clone(), row);
        }
    }
    let merged: Vec<NodeRow> = by_id.into_values().collect();
    // Write to epoch 0. ingested_at of each winning row is preserved naturally.
    if write_rows_with_fields(&node_epoch_path(dir, 0), &node_fields(), &merged) {
        // Delete all other epoch files.
        for epoch in epochs.iter().filter(|&&e| e != 0) {
            fs::remove_file(node_epoch_path(dir, *epoch)).ok();
        }
    }
}

// ── read all node rows (latest-epoch wins) ────────────────────────────────────

fn read_node_rows(dir: &Path) -> Vec<NodeRow> {
    let epochs = existing_epochs(dir);
    if epochs.is_empty() {
        return vec![];
    }
    if epochs.len() == 1 {
        return read_rows(&node_epoch_path(dir, epochs[0]));
    }
    // Multiple epochs: latest wins.
    let mut by_id: HashMap<String, NodeRow> = HashMap::new();
    for epoch in &epochs {
        for row in read_rows::<NodeRow>(&node_epoch_path(dir, *epoch)) {
            by_id.insert(row.unique_id.clone(), row);
        }
    }
    by_id.into_values().collect()
}

/// Minimal projected read: just unique_id + ingested_at, no payload.
fn read_ingested_at(dir: &Path) -> HashMap<String, i64> {
    #[derive(Deserialize)]
    struct Row {
        unique_id: String,
        ingested_at: i64,
    }
    let mut out: HashMap<String, i64> = HashMap::new();
    for epoch in existing_epochs(dir) {
        let path = node_epoch_path(dir, epoch);
        let Ok(file) = fs::File::open(&path) else {
            continue;
        };
        let Ok(builder) = ParquetRecordBatchReaderBuilder::try_new(file) else {
            continue;
        };
        let schema_desc = builder.parquet_schema();
        let col_indices: Vec<usize> = ["unique_id", "ingested_at"]
            .iter()
            .filter_map(|name| {
                (0..schema_desc.num_columns()).find(|&i| schema_desc.column(i).name() == *name)
            })
            .collect();
        let mask = ProjectionMask::leaves(schema_desc, col_indices);
        let Ok(reader) = builder.with_projection(mask).build() else {
            continue;
        };
        for batch in reader {
            let Ok(batch) = batch else { break };
            if let Ok(rows) = serde_arrow::from_record_batch::<Vec<Row>>(&batch) {
                for r in rows {
                    out.insert(r.unique_id, r.ingested_at);
                }
            }
        }
    }
    out
}

// ── save ──────────────────────────────────────────────────────────────────────

pub struct SaveArgs<'a> {
    pub out_dir: &'a Path,
    pub version: u32,
    pub dbt_version: &'a str,
    pub profile_hash: &'a str,
    pub profile_file_hash: &'a str,
    pub project_file_hash: &'a str,
    pub cli_vars_hash: &'a str,
    pub dbt_profile_json: &'a str,
    pub vars_json: &'a str,
    pub env_vars_json: &'a str,
    pub get_relation_calls_json: &'a str,
    pub get_columns_in_relation_calls_json: &'a str,
    pub patterned_dangling_sources_json: &'a str,
    pub nodes_with_resolution_errors_json: &'a str,
    pub nodes_with_access_errors_json: &'a str,
    pub operations_json: &'a str,
    pub packages: &'a [PackageSnapshot],
    pub nodes: &'a Nodes,
    pub disabled_nodes: &'a Nodes,
    pub macros: &'a Macros,
    pub changed_nodes: Option<&'a HashSet<String>>,
    pub ingested_at: i64,
    pub selectors_json: &'a str,
    pub git_sha: &'a str,
    pub git_branch: &'a str,
    pub git_is_dirty: bool,
}

pub fn save(args: &SaveArgs<'_>) -> Result<(), String> {
    // Nothing changed — skip all I/O.
    if let Some(set) = args.changed_nodes {
        if set.is_empty() {
            return Ok(());
        }
    }

    let t0 = Instant::now();
    let dir = cache_dir(args.out_dir);
    fs::create_dir_all(&dir).map_err(|e| e.to_string())?;

    // ── filestamps + pkg_deps/pkg_kinds ─────────────────────────────────────────
    let mut filestamp_rows: Vec<FilestampRow> = Vec::new();
    let mut pkg_deps_map: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut pkg_kinds_map: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for pkg in args.packages {
        for dep in &pkg.dependencies {
            pkg_deps_map
                .entry(pkg.package_name.clone())
                .or_default()
                .insert(dep.clone());
        }
        for (path_kind, entries) in &pkg.all_paths {
            let kind_str = serde_json::to_string(path_kind)
                .unwrap_or_default()
                .trim_matches('"')
                .to_string();
            pkg_kinds_map
                .entry(pkg.package_name.clone())
                .or_default()
                .insert(kind_str.clone());
            for (path, mtime_ns) in entries {
                filestamp_rows.push(FilestampRow {
                    package_name: pkg.package_name.clone(),
                    package_root: pkg.package_root_path.clone(),
                    path_kind: kind_str.clone(),
                    path: path.clone(),
                    mtime_ns: *mtime_ns as i64,
                    ingested_at: args.ingested_at,
                });
            }
        }
    }

    let tfs = Instant::now();
    write_rows_with_fields(&filestamps_path(&dir), &filestamp_fields(), &filestamp_rows);
    t(
        &format!("write filestamps.parquet ({} rows)", filestamp_rows.len()),
        tfs,
    );

    // ── project (includes pkg_deps_json and pkg_kinds_json) ──────────────────
    let pkg_deps_json = serde_json::to_string(&pkg_deps_map).unwrap_or_else(|_| "{}".into());
    let pkg_kinds_json = serde_json::to_string(&pkg_kinds_map).unwrap_or_else(|_| "{}".into());
    let kv_rows: Vec<KvRow> = vec![
        ("version", args.version.to_string()),
        ("dbt_version", args.dbt_version.to_string()),
        ("profile_hash", args.profile_hash.to_string()),
        ("profile_file_hash", args.profile_file_hash.to_string()),
        ("project_file_hash", args.project_file_hash.to_string()),
        ("cli_vars_hash", args.cli_vars_hash.to_string()),
        ("dbt_profile_json", args.dbt_profile_json.to_string()),
        ("vars_json", args.vars_json.to_string()),
        ("env_vars_json", args.env_vars_json.to_string()),
        (
            "get_relation_calls_json",
            args.get_relation_calls_json.to_string(),
        ),
        (
            "get_columns_in_relation_calls_json",
            args.get_columns_in_relation_calls_json.to_string(),
        ),
        (
            "patterned_dangling_sources_json",
            args.patterned_dangling_sources_json.to_string(),
        ),
        (
            "nodes_with_resolution_errors_json",
            args.nodes_with_resolution_errors_json.to_string(),
        ),
        (
            "nodes_with_access_errors_json",
            args.nodes_with_access_errors_json.to_string(),
        ),
        ("operations_json", args.operations_json.to_string()),
        ("selectors_json", args.selectors_json.to_string()),
        ("git_sha", args.git_sha.to_string()),
        ("git_branch", args.git_branch.to_string()),
        (
            "git_is_dirty",
            if args.git_is_dirty { "1" } else { "0" }.to_string(),
        ),
        ("pkg_deps_json", pkg_deps_json),
        ("pkg_kinds_json", pkg_kinds_json),
    ]
    .into_iter()
    .map(|(k, v)| KvRow {
        key: k.to_string(),
        value: v,
    })
    .collect();

    // ── project ───────────────────────────────────────────────────────────────
    let tm = Instant::now();
    write_rows_with_fields(&project_path(&dir), &kv_fields(), &kv_rows);
    t("write project.parquet", tm);

    // ── nodes ─────────────────────────────────────────────────────────────────
    let nf_fields = node_fields();

    let any_uses_graph = match args.changed_nodes {
        None => {
            // Cold start: write epoch 0, remove any delta epochs.
            let tc = Instant::now();
            let mut all_rows = collect_all_rows(args.nodes, args.disabled_nodes, args.macros);
            for r in &mut all_rows {
                r.ingested_at = args.ingested_at;
            }
            t(&format!("collect_all_rows ({} rows)", all_rows.len()), tc);
            let any_uses_graph = all_rows.iter().any(|r| r.uses_graph != 0);
            let total = all_rows.len();
            let tw = Instant::now();
            write_rows_with_fields(&node_epoch_path(&dir, 0), &nf_fields, &all_rows);
            t(&format!("write nodes/0.parquet ({total} rows, cold)"), tw);
            for epoch in existing_epochs(&dir).into_iter().filter(|&e| e != 0) {
                fs::remove_file(node_epoch_path(&dir, epoch)).ok();
            }
            any_uses_graph
        }
        Some(changed) => {
            let exceeds_threshold = changed.len() > 100;

            if exceeds_threshold {
                // Full rewrite as epoch 0. Read back existing ingested_at values (projected,
                // no payload) so unchanged rows keep their original timestamp; only rows in
                // `changed` get the current invocation timestamp.
                let prior_ingested = read_ingested_at(&dir);
                let tc = Instant::now();
                let mut all_rows = collect_all_rows(args.nodes, args.disabled_nodes, args.macros);
                for r in &mut all_rows {
                    r.ingested_at = if changed.contains(&r.unique_id) {
                        args.ingested_at
                    } else {
                        prior_ingested
                            .get(&r.unique_id)
                            .copied()
                            .unwrap_or(args.ingested_at)
                    };
                }
                t(&format!("collect_all_rows ({} rows)", all_rows.len()), tc);
                let any_uses_graph = all_rows.iter().any(|r| r.uses_graph != 0);
                let total = all_rows.len();
                let tw = Instant::now();
                write_rows_with_fields(&node_epoch_path(&dir, 0), &nf_fields, &all_rows);
                t(
                    &format!("write nodes/0.parquet ({total} rows, full-rewrite)"),
                    tw,
                );
                for epoch in existing_epochs(&dir).into_iter().filter(|&e| e != 0) {
                    fs::remove_file(node_epoch_path(&dir, epoch)).ok();
                }
                any_uses_graph
            } else {
                let tc = Instant::now();
                let mut delta =
                    collect_delta_rows(args.nodes, args.disabled_nodes, args.macros, changed);
                let epoch = next_epoch(&dir);
                for r in &mut delta {
                    r.ingested_at = args.ingested_at;
                }
                t(&format!("collect_delta_rows ({} rows)", delta.len()), tc);
                let any_uses_graph = delta.iter().any(|r| r.uses_graph != 0);
                let tw = Instant::now();
                write_rows_with_fields(&node_epoch_path(&dir, epoch), &nf_fields, &delta);
                t(
                    &format!("write nodes/{epoch}.parquet ({} rows, delta)", delta.len()),
                    tw,
                );

                // Periodic compaction.
                if existing_epochs(&dir).len() as u32 > COMPACT_THRESHOLD {
                    compact(&dir);
                }
                any_uses_graph
            }
        }
    };
    let mut project: Vec<KvRow> = read_rows(&project_path(&dir));
    if let Some(row) = project.iter_mut().find(|r| r.key == "any_uses_graph") {
        row.value = if any_uses_graph {
            "1".into()
        } else {
            "0".into()
        };
    } else {
        project.push(KvRow {
            key: "any_uses_graph".into(),
            value: if any_uses_graph {
                "1".into()
            } else {
                "0".into()
            },
        });
    }
    write_rows_with_fields(&project_path(&dir), &kv_fields(), &project);

    // ── alive ────────────────────────────────────────────────────────────────
    let ta = Instant::now();
    let alive_rows = collect_alive_rows(
        args.nodes,
        args.disabled_nodes,
        args.macros,
        args.ingested_at,
    );
    let alive_path = dir.join("alive.parquet");
    if let Err(e) = dbt_metadata_parquet::parse_alive::write_alive(&alive_path, &alive_rows) {
        eprintln!("[warning] Failed to write alive.parquet: {e}");
    }
    t(
        &format!("write alive.parquet ({} rows)", alive_rows.len()),
        ta,
    );

    // Clean up legacy pkg_deps.parquet / pkg_kinds.parquet if still present.
    let _ = fs::remove_file(dir.join("pkg_deps.parquet"));
    let _ = fs::remove_file(dir.join("pkg_kinds.parquet"));

    t("save total", t0);
    Ok(())
}

// ── load ──────────────────────────────────────────────────────────────────────

pub struct LoadedState {
    pub version: u32,
    pub dbt_version: String,
    pub profile_hash: String,
    pub profile_file_hash: String,
    pub project_file_hash: String,
    pub cli_vars_hash: String,
    pub dbt_profile_json: String,
    pub vars_json: String,
    pub env_vars_json: String,
    pub get_relation_calls_json: String,
    pub get_columns_in_relation_calls_json: String,
    pub patterned_dangling_sources_json: String,
    pub nodes_with_resolution_errors_json: String,
    pub nodes_with_access_errors_json: String,
    pub operations_json: String,
    pub packages: Vec<PackageSnapshot>,
    pub nodes: Nodes,
    pub disabled_nodes: Nodes,
    pub docs_macros: BTreeMap<String, DbtDocsMacro>,
    pub any_uses_graph: bool,
    pub selectors_json: String,
    pub git_sha: String,
    pub git_branch: String,
    pub git_is_dirty: bool,
}

pub fn peek_any_uses_graph(out_dir: &Path) -> bool {
    let dir = cache_dir(out_dir);
    let project: Vec<KvRow> = read_rows(&project_path(&dir));
    project
        .iter()
        .find(|r| r.key == "any_uses_graph")
        .map(|r| r.value == "1")
        .unwrap_or(false)
}

pub fn load(out_dir: &Path) -> Option<LoadedState> {
    load_filtered(out_dir, None)
}

pub fn load_filtered(
    out_dir: &Path,
    allowed_kinds: Option<&HashSet<&'static str>>,
) -> Option<LoadedState> {
    load_filtered_with_unique_ids(out_dir, allowed_kinds, None)
}

pub fn load_filtered_with_unique_ids(
    out_dir: &Path,
    allowed_kinds: Option<&HashSet<&'static str>>,
    allowed_unique_ids: Option<&HashSet<String>>,
) -> Option<LoadedState> {
    let t0 = Instant::now();
    let dir = cache_dir(out_dir);
    if !dir.exists() {
        return None;
    }
    let project_file = project_path(&dir);
    if !project_file.exists() {
        return None;
    }
    t("filecheck (dir+project exists)", t0);

    let t1 = Instant::now();
    let project: Vec<KvRow> = read_rows(&project_file);
    t(
        &format!("read project.parquet ({} kv rows)", project.len()),
        t1,
    );

    let get_kv = |key: &str| -> String {
        project
            .iter()
            .find(|r| r.key == key)
            .map(|r| r.value.clone())
            .unwrap_or_default()
    };

    let version: u32 = get_kv("version").parse().ok()?;
    let dbt_version = get_kv("dbt_version");
    if dbt_version.is_empty() {
        return None;
    }
    let profile_hash = get_kv("profile_hash");
    let profile_file_hash = get_kv("profile_file_hash");
    let project_file_hash = get_kv("project_file_hash");
    let cli_vars_hash = get_kv("cli_vars_hash");
    let dbt_profile_json = get_kv("dbt_profile_json");
    let vars_json = get_kv("vars_json");
    let env_vars_json = get_kv("env_vars_json");
    let get_relation_calls_json = get_kv("get_relation_calls_json");
    let get_columns_in_relation_calls_json = get_kv("get_columns_in_relation_calls_json");
    let patterned_dangling_sources_json = get_kv("patterned_dangling_sources_json");
    let nodes_with_resolution_errors_json = get_kv("nodes_with_resolution_errors_json");
    let nodes_with_access_errors_json = get_kv("nodes_with_access_errors_json");
    let operations_json = get_kv("operations_json");
    let selectors_json = get_kv("selectors_json");
    let git_sha = get_kv("git_sha");
    let git_branch = get_kv("git_branch");
    let git_is_dirty = get_kv("git_is_dirty") == "1";
    let any_uses_graph = get_kv("any_uses_graph") == "1";

    let t2 = Instant::now();
    let packages = load_packages(&dir, &project)?;
    t("load_packages (filestamps + pkg from KV)", t2);

    let t3 = Instant::now();
    let (nodes, disabled_nodes, docs_macros) = load_nodes(&dir, allowed_kinds, allowed_unique_ids)?;
    t(
        &format!(
            "load_nodes (nodes, {} + {} nodes)",
            nodes.iter().count(),
            disabled_nodes.iter().count()
        ),
        t3,
    );

    t("load total", t0);

    Some(LoadedState {
        version,
        dbt_version,
        profile_hash,
        profile_file_hash,
        project_file_hash,
        cli_vars_hash,
        dbt_profile_json,
        vars_json,
        env_vars_json,
        get_relation_calls_json,
        get_columns_in_relation_calls_json,
        patterned_dangling_sources_json,
        nodes_with_resolution_errors_json,
        nodes_with_access_errors_json,
        operations_json,
        packages,
        nodes,
        disabled_nodes,
        docs_macros,
        any_uses_graph,
        selectors_json,
        git_sha,
        git_branch,
        git_is_dirty,
    })
}

pub(crate) fn load_packages(dir: &Path, project: &[KvRow]) -> Option<Vec<PackageSnapshot>> {
    let get_kv = |key: &str| -> String {
        project
            .iter()
            .find(|r| r.key == key)
            .map(|r| r.value.clone())
            .unwrap_or_default()
    };

    let mut pkg_roots: HashMap<String, String> = HashMap::new();
    let mut pkg_paths: HashMap<String, HashMap<ResourcePathKind, Vec<(String, u64)>>> =
        HashMap::new();

    for row in read_rows::<FilestampRow>(&filestamps_path(dir)) {
        pkg_roots.insert(row.package_name.clone(), row.package_root.clone());
        let kind: ResourcePathKind = serde_json::from_str(&format!("\"{}\"", row.path_kind))
            .unwrap_or(ResourcePathKind::ModelPaths);
        pkg_paths
            .entry(row.package_name)
            .or_default()
            .entry(kind)
            .or_default()
            .push((row.path, row.mtime_ns as u64));
    }

    // Read pkg_deps and pkg_kinds from project KV (folded in from separate files).
    let mut pkg_deps: HashMap<String, BTreeSet<String>> =
        serde_json::from_str(&get_kv("pkg_deps_json")).unwrap_or_default();

    let pkg_kinds_raw: BTreeMap<String, BTreeSet<String>> =
        serde_json::from_str(&get_kv("pkg_kinds_json")).unwrap_or_default();
    for (pkg_name, kinds) in &pkg_kinds_raw {
        for kind_str in kinds {
            let kind: ResourcePathKind = serde_json::from_str(&format!("\"{kind_str}\""))
                .unwrap_or(ResourcePathKind::ModelPaths);
            pkg_paths
                .entry(pkg_name.clone())
                .or_default()
                .entry(kind)
                .or_default();
        }
    }

    // Fallback: read legacy separate files if KV entries are empty (first run after upgrade).
    if pkg_deps.is_empty() {
        #[derive(Deserialize)]
        struct PkgDepRow {
            package_name: String,
            dependency: String,
        }
        for row in read_rows::<PkgDepRow>(&dir.join("pkg_deps.parquet")) {
            pkg_deps
                .entry(row.package_name)
                .or_default()
                .insert(row.dependency);
        }
    }
    if pkg_kinds_raw.is_empty() {
        #[derive(Deserialize)]
        struct PkgKindRow {
            package_name: String,
            path_kind: String,
        }
        for row in read_rows::<PkgKindRow>(&dir.join("pkg_kinds.parquet")) {
            let kind: ResourcePathKind = serde_json::from_str(&format!("\"{}\"", row.path_kind))
                .unwrap_or(ResourcePathKind::ModelPaths);
            pkg_paths
                .entry(row.package_name)
                .or_default()
                .entry(kind)
                .or_default();
        }
    }

    let mut snapshots: Vec<PackageSnapshot> = pkg_roots
        .into_iter()
        .map(|(name, root)| PackageSnapshot {
            package_root_path: root,
            package_name: name.clone(),
            all_paths: pkg_paths.remove(&name).unwrap_or_default(),
            dependencies: pkg_deps.remove(&name).unwrap_or_default(),
        })
        .collect();

    // Ensure root package (the one not listed as a dependency of any other) is at index 0.
    // HashMap iteration order is non-deterministic; without this, validate() would use a
    // random package's root path to resolve profiles.yml, causing spurious cache invalidation.
    let all_deps: HashSet<&str> = snapshots
        .iter()
        .flat_map(|p| p.dependencies.iter().map(|d| d.as_str()))
        .collect();
    if let Some(root_idx) = snapshots
        .iter()
        .position(|p| !all_deps.contains(p.package_name.as_str()))
    {
        snapshots.swap(0, root_idx);
    }

    Some(snapshots)
}

fn load_nodes(
    dir: &Path,
    allowed_kinds: Option<&HashSet<&'static str>>,
    allowed_unique_ids: Option<&HashSet<String>>,
) -> Option<(Nodes, Nodes, BTreeMap<String, DbtDocsMacro>)> {
    let mut nodes = Nodes::default();
    let mut disabled_nodes = Nodes::default();
    let mut docs_macros: BTreeMap<String, DbtDocsMacro> = BTreeMap::new();

    let all_rows = read_node_rows(dir);
    for row in all_rows {
        if row.kind == "docs_macro" {
            if let Ok(v) = serde_json::from_str::<DbtDocsMacro>(&row.payload) {
                docs_macros.insert(row.unique_id, v);
            }
            continue;
        }
        if let Some(kinds) = allowed_kinds {
            if !kinds.contains(row.kind.as_str()) {
                continue;
            }
        }
        if let Some(ids) = allowed_unique_ids {
            if !ids.contains(&row.unique_id) {
                continue;
            }
        }
        let target = if row.is_disabled != 0 {
            &mut disabled_nodes
        } else {
            &mut nodes
        };
        deserialize_into(
            &row.unique_id,
            &row.kind,
            &row.payload,
            row.extended_model != 0,
            &row.macro_arguments_json,
            &row.macro_span_json,
            &row.macro_name_span_json,
            target,
        );
    }

    Some((nodes, disabled_nodes, docs_macros))
}

#[allow(clippy::too_many_arguments)]
fn deserialize_into(
    uid: &str,
    kind: &str,
    payload: &str,
    extended_model: bool,
    macro_arguments_json: &str,
    macro_span_json: &str,
    macro_name_span_json: &str,
    nodes: &mut Nodes,
) {
    macro_rules! deser {
        ($map:expr, $T:ty) => {
            if let Ok(v) = serde_json::from_str::<$T>(payload) {
                $map.insert(uid.to_string(), Arc::new(v));
            }
        };
    }
    match kind {
        "model" => {
            if let Ok(mut v) = serde_json::from_str::<DbtModel>(payload) {
                v.__base_attr__.extended_model = extended_model;
                nodes.models.insert(uid.to_string(), Arc::new(v));
            }
        }
        "macro" => {
            if let Ok(mut v) = serde_json::from_str::<DbtMacro>(payload) {
                if !macro_arguments_json.is_empty() {
                    if let Ok(args) =
                        serde_json::from_str::<Vec<MacroArgument>>(macro_arguments_json)
                    {
                        v.arguments = args;
                    }
                }
                if !macro_span_json.is_empty() {
                    v.span = serde_json::from_str(macro_span_json).ok();
                }
                if !macro_name_span_json.is_empty() {
                    v.macro_name_span = serde_json::from_str(macro_name_span_json).ok();
                }
                nodes.macros.insert(uid.to_string(), Arc::new(v));
            }
        }
        "seed" => deser!(nodes.seeds, DbtSeed),
        "test" => deser!(nodes.tests, DbtTest),
        "unit_test" => deser!(nodes.unit_tests, DbtUnitTest),
        "source" => deser!(nodes.sources, DbtSource),
        "snapshot" => deser!(nodes.snapshots, DbtSnapshot),
        "analysis" => deser!(nodes.analyses, DbtAnalysis),
        "exposure" => deser!(nodes.exposures, DbtExposure),
        "semantic_model" => deser!(nodes.semantic_models, DbtSemanticModel),
        "metric" => deser!(nodes.metrics, DbtMetric),
        "saved_query" => deser!(nodes.saved_queries, DbtSavedQuery),
        "group" => deser!(nodes.groups, DbtGroup),
        "function" => deser!(nodes.functions, DbtFunction),
        _ => {}
    }
}

// ── PackageSnapshot ↔ DbtPackage ─────────────────────────────────────────────

pub fn snapshot_packages(packages: &[DbtPackage]) -> Vec<PackageSnapshot> {
    packages
        .iter()
        .map(|pkg| {
            let all_paths: HashMap<ResourcePathKind, Vec<(String, u64)>> = pkg
                .all_paths
                .iter()
                .map(|(kind, entries)| {
                    let rows = entries
                        .iter()
                        .map(|(path, mtime)| {
                            (path.display().to_string(), system_time_to_nanos(*mtime))
                        })
                        .collect();
                    (kind.clone(), rows)
                })
                .collect();
            PackageSnapshot {
                package_root_path: pkg.package_root_path.display().to_string(),
                package_name: pkg.dbt_project.name.clone(),
                all_paths,
                dependencies: pkg.dependencies.clone(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index_resolution::resolve_dirty_unique_ids_from_index;
    use minijinja::machinery::Span;
    use tempfile::TempDir;

    fn make_row(uid: &str, payload: &str) -> NodeRow {
        NodeRow {
            unique_id: uid.to_string(),
            kind: "model".to_string(),
            name: uid.to_string(),
            package_name: "pkg".to_string(),
            original_path: format!("models/{uid}.sql"),
            fqn: format!(r#"["pkg","{}"]"#, uid),
            tags: "[]".to_string(),
            depends_on: "[]".to_string(),
            materialization: "table".to_string(),
            payload: payload.to_string(),
            ..Default::default()
        }
    }

    /// Write epoch 0 with 3 rows, then 9 delta epochs each updating row "a".
    /// After the 9th delta save() triggers compaction; only epoch 0 must remain,
    /// and "a" must carry the payload from the last delta.
    #[test]
    fn compaction_merges_epochs_and_latest_wins() {
        let tmp = TempDir::new().unwrap();
        let dir = cache_dir(tmp.path());
        fs::create_dir_all(nodes_dir(&dir)).unwrap();

        // Write base epoch: rows a, b, c.
        let base = vec![
            make_row("a", r#"{"v":0}"#),
            make_row("b", r#"{"v":0}"#),
            make_row("c", r#"{"v":0}"#),
        ];
        write_rows_with_fields(&node_epoch_path(&dir, 0), &node_fields(), &base);

        // Write 9 delta epochs, each updating "a" with an incrementing payload.
        // The 9th delta triggers compact() (existing_epochs().len() > COMPACT_THRESHOLD=8).
        for i in 1u32..=9 {
            let delta = vec![make_row("a", &format!(r#"{{"v":{i}}}"#))];
            write_rows_with_fields(&node_epoch_path(&dir, i), &node_fields(), &delta);
        }
        // Manually trigger compact (save() does this internally; here we call directly).
        compact(&dir);

        // Only epoch 0 must remain.
        let epochs = existing_epochs(&dir);
        assert_eq!(
            epochs,
            vec![0],
            "expected only epoch 0 after compaction, got {epochs:?}"
        );

        // Read back and verify latest-wins: "a" has v=9, "b" and "c" have v=0.
        let rows = read_node_rows(&dir);
        let mut by_id: HashMap<String, String> =
            rows.into_iter().map(|r| (r.unique_id, r.payload)).collect();
        assert_eq!(
            by_id.remove("a").unwrap(),
            r#"{"v":9}"#,
            "a should carry last delta"
        );
        assert_eq!(
            by_id.remove("b").unwrap(),
            r#"{"v":0}"#,
            "b should be unchanged"
        );
        assert_eq!(
            by_id.remove("c").unwrap(),
            r#"{"v":0}"#,
            "c should be unchanged"
        );
        assert!(by_id.is_empty(), "unexpected extra rows: {by_id:?}");
    }

    /// Verify that all new NodeRow columns survive a write→read round-trip through parquet.
    #[test]
    fn node_fact_row_new_columns_round_trip() {
        let tmp = TempDir::new().unwrap();
        let dir = cache_dir(tmp.path());
        fs::create_dir_all(nodes_dir(&dir)).unwrap();

        let span = Span {
            start_line: 3,
            start_col: 5,
            start_offset: 42,
            end_line: 3,
            end_col: 20,
            end_offset: 57,
        };
        let name_span = Span {
            start_line: 3,
            start_col: 9,
            start_offset: 46,
            end_line: 3,
            end_col: 15,
            end_offset: 52,
        };
        let args = vec![
            MacroArgument {
                name: "amount".into(),
                type_: Some("number".into()),
                description: "the amount".into(),
            },
            MacroArgument {
                name: "currency".into(),
                type_: None,
                description: String::new(),
            },
        ];

        let row = NodeRow {
            unique_id: "macro.pkg.my_macro".to_string(),
            kind: "macro".to_string(),
            name: "my_macro".to_string(),
            package_name: "pkg".to_string(),
            original_path: "macros/my_macro.sql".to_string(),
            fqn: "[]".to_string(),
            tags: "[]".to_string(),
            depends_on: "[]".to_string(),
            payload: r#"{"name":"my_macro","package_name":"pkg"}"#.to_string(),
            ingested_at: 1_700_000_000_000,
            macro_arguments_json: serde_json::to_string(&args).unwrap(),
            macro_span_json: serde_json::to_string(&span).unwrap(),
            macro_name_span_json: serde_json::to_string(&name_span).unwrap(),
            ..Default::default()
        };

        write_rows_with_fields(
            &node_epoch_path(&dir, 0),
            &node_fields(),
            std::slice::from_ref(&row),
        );
        let rows: Vec<NodeRow> = read_rows(&node_epoch_path(&dir, 0));
        assert_eq!(rows.len(), 1);
        let got = &rows[0];

        assert_eq!(got.ingested_at, 1_700_000_000_000);
        assert_eq!(got.extended_model, 0);
        assert_eq!(got.macro_arguments_json, row.macro_arguments_json);
        assert_eq!(got.macro_span_json, row.macro_span_json);
        assert_eq!(got.macro_name_span_json, row.macro_name_span_json);

        // Verify span JSON round-trips correctly.
        let got_span: Span = serde_json::from_str(&got.macro_span_json).unwrap();
        assert_eq!(got_span.start_line, 3);
        assert_eq!(got_span.start_col, 5);
        assert_eq!(got_span.start_offset, 42);
        assert_eq!(got_span.end_line, 3);
        assert_eq!(got_span.end_col, 20);
        assert_eq!(got_span.end_offset, 57);

        let got_args: Vec<MacroArgument> = serde_json::from_str(&got.macro_arguments_json).unwrap();
        assert_eq!(got_args.len(), 2);
        assert_eq!(got_args[0].name, "amount");
        assert_eq!(got_args[0].type_, Some("number".into()));
        assert_eq!(got_args[1].name, "currency");
        assert_eq!(got_args[1].type_, None);
    }

    /// Verify that extended_model=true round-trips through parquet + deserialize_into.
    #[test]
    fn extended_model_survives_deserialize_into() {
        use dbt_schemas::schemas::nodes::DbtModel;

        let tmp = TempDir::new().unwrap();
        let dir = cache_dir(tmp.path());
        fs::create_dir_all(nodes_dir(&dir)).unwrap();

        // Build a minimal DbtModel and serialize it as payload.
        let mut model = DbtModel::default();
        model.__base_attr__.extended_model = true;
        // serde_json will skip extended_model (it has #[serde(skip_serializing)]).
        let payload = serde_json::to_string(&model).unwrap();

        // Confirm extended_model is absent from the JSON payload.
        assert!(
            !payload.contains("extended_model"),
            "extended_model should be skip_serializing"
        );

        let row = NodeRow {
            unique_id: "model.pkg.orders".to_string(),
            kind: "model".to_string(),
            extended_model: 1,
            payload,
            ..make_row("model.pkg.orders", "{}")
        };
        write_rows_with_fields(&node_epoch_path(&dir, 0), &node_fields(), &[row]);

        let mut nodes = Nodes::default();
        let rows: Vec<NodeRow> = read_rows(&node_epoch_path(&dir, 0));
        let r = &rows[0];
        deserialize_into(
            &r.unique_id,
            &r.kind,
            &r.payload,
            r.extended_model != 0,
            &r.macro_arguments_json,
            &r.macro_span_json,
            &r.macro_name_span_json,
            &mut nodes,
        );

        let loaded = nodes
            .models
            .get("model.pkg.orders")
            .expect("model should be loaded");
        assert!(
            loaded.__base_attr__.extended_model,
            "extended_model must survive the round-trip"
        );
    }

    /// Verify that macro span, name_span, and arguments survive parquet + deserialize_into.
    #[test]
    fn macro_fields_survive_deserialize_into() {
        use dbt_schemas::schemas::macros::DbtMacro;

        let tmp = TempDir::new().unwrap();
        let dir = cache_dir(tmp.path());
        fs::create_dir_all(nodes_dir(&dir)).unwrap();

        let span = Span {
            start_line: 10,
            start_col: 1,
            start_offset: 200,
            end_line: 10,
            end_col: 30,
            end_offset: 229,
        };
        let name_span = Span {
            start_line: 10,
            start_col: 10,
            start_offset: 209,
            end_line: 10,
            end_col: 20,
            end_offset: 219,
        };
        let args = vec![MacroArgument {
            name: "col".into(),
            type_: Some("string".into()),
            description: "a column".into(),
        }];

        let macro_node = DbtMacro {
            name: "cents_to_dollars".into(),
            package_name: "pkg".into(),
            span: Some(span),
            macro_name_span: Some(name_span),
            arguments: args.clone(),
            ..DbtMacro::default()
        };

        // payload is produced by serde_json which skips span/macro_name_span/arguments.
        let payload = serde_json::to_string(&macro_node).unwrap();
        assert!(
            !payload.contains("start_line"),
            "span should be skip_serializing in payload"
        );

        let row = NodeRow {
            unique_id: "macro.pkg.cents_to_dollars".to_string(),
            kind: "macro".to_string(),
            name: "cents_to_dollars".to_string(),
            package_name: "pkg".to_string(),
            payload,
            macro_arguments_json: serde_json::to_string(&args).unwrap(),
            macro_span_json: serde_json::to_string(&span).unwrap(),
            macro_name_span_json: serde_json::to_string(&name_span).unwrap(),
            ..make_row("macro.pkg.cents_to_dollars", "{}")
        };

        write_rows_with_fields(&node_epoch_path(&dir, 0), &node_fields(), &[row]);

        let mut nodes = Nodes::default();
        let rows: Vec<NodeRow> = read_rows(&node_epoch_path(&dir, 0));
        let r = &rows[0];
        deserialize_into(
            &r.unique_id,
            &r.kind,
            &r.payload,
            r.extended_model != 0,
            &r.macro_arguments_json,
            &r.macro_span_json,
            &r.macro_name_span_json,
            &mut nodes,
        );

        let loaded = nodes
            .macros
            .get("macro.pkg.cents_to_dollars")
            .expect("macro should be loaded");

        let loaded_span = loaded.span.expect("span must be restored");
        assert_eq!(loaded_span.start_line, 10);
        assert_eq!(loaded_span.start_col, 1);
        assert_eq!(loaded_span.start_offset, 200);
        assert_eq!(loaded_span.end_line, 10);
        assert_eq!(loaded_span.end_col, 30);
        assert_eq!(loaded_span.end_offset, 229);

        let loaded_name_span = loaded
            .macro_name_span
            .expect("macro_name_span must be restored");
        assert_eq!(loaded_name_span.start_offset, 209);

        assert_eq!(loaded.arguments.len(), 1);
        assert_eq!(loaded.arguments[0].name, "col");
        assert_eq!(loaded.arguments[0].type_, Some("string".into()));
    }

    // ── state:dirty tests ────────────────────────────────────────────────────────

    /// Build a minimal filestamps.parquet + node index in a tempdir.
    /// Returns (TempDir, root_path_for_package).
    fn write_dirty_fixture(
        nodes: &[(&str, &str, &[&str])], // (uid, rel_path, deps)
    ) -> (TempDir, PathBuf) {
        let tmp = TempDir::new().unwrap();
        let out_dir = tmp.path().to_path_buf();
        let pkg_root = out_dir.join("project");
        fs::create_dir_all(pkg_root.join("models")).unwrap();

        // Write actual .sql files so mtime checks work.
        for (uid, rel_path, _) in nodes {
            let full = pkg_root.join(rel_path);
            if let Some(p) = full.parent() {
                fs::create_dir_all(p).unwrap();
            }
            fs::write(&full, uid.as_bytes()).unwrap();
        }

        // Build NodeRow vec.
        let node_rows: Vec<NodeRow> = nodes
            .iter()
            .map(|&(uid, rel_path, deps)| {
                let deps_json = serde_json::to_string(
                    &deps.iter().copied().map(str::to_string).collect::<Vec<_>>(),
                )
                .unwrap();
                NodeRow {
                    unique_id: uid.to_string(),
                    kind: "model".to_string(),
                    name: uid.to_string(),
                    package_name: "pkg".to_string(),
                    original_path: rel_path.to_string(),
                    fqn: format!(r#"["pkg","{}"]"#, uid),
                    tags: "[]".to_string(),
                    depends_on: deps_json,
                    materialization: "table".to_string(),
                    payload: "{}".to_string(),
                    ..Default::default()
                }
            })
            .collect();

        // Write to parquet cache.
        let dir = cache_dir(&out_dir);
        fs::create_dir_all(nodes_dir(&dir)).unwrap();
        write_rows_with_fields(&node_epoch_path(&dir, 0), &node_fields(), &node_rows);

        // Write filestamps.parquet.
        let filestamp_rows: Vec<FilestampRow> = nodes
            .iter()
            .map(|&(_, rel_path, _)| {
                let mtime = fs::metadata(pkg_root.join(rel_path))
                    .and_then(|m| m.modified())
                    .map(system_time_to_nanos)
                    .unwrap_or(0);
                FilestampRow {
                    package_name: "pkg".to_string(),
                    package_root: pkg_root.display().to_string(),
                    path_kind: "ModelPaths".to_string(),
                    path: rel_path.to_string(),
                    mtime_ns: mtime as i64,
                    ingested_at: 1_700_000_000_000,
                }
            })
            .collect();
        write_rows_with_fields(&filestamps_path(&dir), &filestamp_fields(), &filestamp_rows);

        // Write project.parquet with pkg_deps_json and pkg_kinds_json KV entries.
        let mut pkg_kinds_map: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        pkg_kinds_map
            .entry("pkg".to_string())
            .or_default()
            .insert("ModelPaths".to_string());
        let project_rows: Vec<KvRow> = vec![
            KvRow {
                key: "pkg_deps_json".to_string(),
                value: "{}".to_string(),
            },
            KvRow {
                key: "pkg_kinds_json".to_string(),
                value: serde_json::to_string(&pkg_kinds_map).unwrap(),
            },
        ];
        write_rows_with_fields(&project_path(&dir), &kv_fields(), &project_rows);

        (tmp, pkg_root)
    }

    /// Touch a file by sleeping 15ms then rewriting it (advances mtime).
    fn touch_file(path: &Path) {
        std::thread::sleep(Duration::from_millis(15));
        let content = fs::read(path).unwrap_or_default();
        fs::write(path, content).unwrap();
    }

    /// Graph: a←b←c (c depends on b, b depends on a), plus independent d.
    #[test]
    fn test_resolve_dirty_basic() {
        let nodes = &[
            ("model.pkg.a", "models/a.sql", &[][..]),
            ("model.pkg.b", "models/b.sql", &["model.pkg.a"][..]),
            ("model.pkg.c", "models/c.sql", &["model.pkg.b"][..]),
            ("model.pkg.d", "models/d.sql", &[][..]),
        ];
        let (tmp, pkg_root) = write_dirty_fixture(nodes);

        // Touch b — b is dirty; a is pulled in as its ancestor (so all_deps_present passes);
        // c and d are not included (no children walk, d is independent).
        touch_file(&pkg_root.join("models/b.sql"));

        let result = resolve_dirty_unique_ids_from_index(tmp.path(), None, None, false).unwrap();
        assert!(result.contains("model.pkg.b"), "b is dirty");
        assert!(
            result.contains("model.pkg.a"),
            "a pulled in as ancestor of b"
        );
        assert!(
            !result.contains("model.pkg.c"),
            "c not included (no + walk)"
        );
        assert!(
            !result.contains("model.pkg.d"),
            "d not included (independent)"
        );
    }

    /// state:dirty+ (children walk): touching a should include a, b, c.
    #[test]
    fn test_resolve_dirty_children_walk() {
        let nodes = &[
            ("model.pkg.a", "models/a.sql", &[][..]),
            ("model.pkg.b", "models/b.sql", &["model.pkg.a"][..]),
            ("model.pkg.c", "models/c.sql", &["model.pkg.b"][..]),
        ];

        let (tmp, pkg_root) = write_dirty_fixture(nodes);
        touch_file(&pkg_root.join("models/a.sql"));

        let result =
            resolve_dirty_unique_ids_from_index(tmp.path(), None, Some(u32::MAX), false).unwrap();
        assert!(result.contains("model.pkg.a"), "a is dirty seed");
        assert!(result.contains("model.pkg.b"), "b is downstream of a");
        assert!(result.contains("model.pkg.c"), "c is downstream of b");
    }

    /// +state:dirty (parents walk): touching c should include a, b, c via ancestor closure.
    #[test]
    fn test_resolve_dirty_parent_walk() {
        let nodes = &[
            ("model.pkg.a", "models/a.sql", &[][..]),
            ("model.pkg.b", "models/b.sql", &["model.pkg.a"][..]),
            ("model.pkg.c", "models/c.sql", &["model.pkg.b"][..]),
        ];
        let (tmp, pkg_root) = write_dirty_fixture(nodes);
        touch_file(&pkg_root.join("models/c.sql"));

        let result =
            resolve_dirty_unique_ids_from_index(tmp.path(), Some(u32::MAX), None, false).unwrap();
        assert!(result.contains("model.pkg.a"), "a is ancestor of c");
        assert!(result.contains("model.pkg.b"), "b is ancestor of c");
        assert!(result.contains("model.pkg.c"), "c is dirty seed");
    }

    /// Nothing touched → Some(empty set) not None (cache exists, just clean).
    #[test]
    fn test_resolve_dirty_nothing_touched() {
        let nodes = &[
            ("model.pkg.a", "models/a.sql", &[][..]),
            ("model.pkg.b", "models/b.sql", &["model.pkg.a"][..]),
        ];
        let (tmp, _pkg_root) = write_dirty_fixture(nodes);

        let result = resolve_dirty_unique_ids_from_index(tmp.path(), None, None, false).unwrap();
        // No model nodes — only macros (none in this fixture).
        assert!(!result.contains("model.pkg.a"));
        assert!(!result.contains("model.pkg.b"));
    }

    /// No cache → None.
    #[test]
    fn test_resolve_dirty_no_cache() {
        let tmp = TempDir::new().unwrap();
        let result = resolve_dirty_unique_ids_from_index(tmp.path(), None, None, false);
        assert!(result.is_none(), "no cache should return None");
    }

    /// Diamond: b depends on a, c depends on b AND e. Touching a with + walk
    /// should include a, b, c, and e (e is ancestor of c, pulled in by ancestor closure).
    #[test]
    fn test_resolve_dirty_diamond() {
        let nodes = &[
            ("model.pkg.a", "models/a.sql", &[][..]),
            ("model.pkg.e", "models/e.sql", &[][..]),
            ("model.pkg.b", "models/b.sql", &["model.pkg.a"][..]),
            (
                "model.pkg.c",
                "models/c.sql",
                &["model.pkg.b", "model.pkg.e"][..],
            ),
        ];
        let (tmp, pkg_root) = write_dirty_fixture(nodes);
        touch_file(&pkg_root.join("models/a.sql"));

        let result =
            resolve_dirty_unique_ids_from_index(tmp.path(), None, Some(u32::MAX), false).unwrap();
        assert!(result.contains("model.pkg.a"), "a is dirty seed");
        assert!(result.contains("model.pkg.b"), "b downstream of a");
        assert!(result.contains("model.pkg.c"), "c downstream of b");
        // e is a parent of c (which is in the result), so ancestor closure adds it.
        assert!(
            result.contains("model.pkg.e"),
            "e pulled in as ancestor of c"
        );
    }
}
