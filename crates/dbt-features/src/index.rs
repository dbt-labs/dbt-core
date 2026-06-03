use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use dbt_common::io_args::EvalArgs;
use dbt_common::tracing::emit::emit_warn_log_message;
use dbt_common::{ErrorCode, FsResult};
use dbt_docs_server::Providers;
use dbt_docs_server::providers::{Backend, DefaultDistInfoProvider};
use dbt_index_core::column_impact::UnavailableColumnImpact;
use dbt_index_core::column_lineage::UnavailableColumnLineage;
use dbt_lineage_core::{CllEdge, PlanGrainInfo};
use dbt_schema_store::SchemaStoreTrait;
use dbt_schemas::schemas::manifest::{DbtManifest, DbtNode};
use dbt_schemas::state::ResolverState;
use dbt_tasks_core::RunTaskResults;

#[async_trait]
pub trait IndexHooks: Send + Sync {
    async fn lineage_grain_infos(
        &self,
        _run_task_results: &RunTaskResults,
    ) -> FsResult<HashMap<String, PlanGrainInfo>> {
        Ok(HashMap::new())
    }

    async fn column_lineage(
        &self,
        _resolved_state: &ResolverState,
        _run_task_results: &RunTaskResults,
    ) -> FsResult<Vec<CllEdge>> {
        Ok(Vec::new())
    }
}

pub struct NoOpIndexHooks;

#[async_trait]
impl IndexHooks for NoOpIndexHooks {}

pub struct IndexFeature {
    pub hooks: Box<dyn IndexHooks>,
    pub providers_factory: fn(Arc<dyn Backend>) -> Providers,
}

pub fn default_providers_factory(backend: Arc<dyn Backend>) -> Providers {
    Providers {
        backend: backend.clone(),
        column_lineage: Arc::new(UnavailableColumnLineage::new()),
        column_impact: Arc::new(UnavailableColumnImpact::new()),
        dist_info: Arc::new(DefaultDistInfoProvider),
    }
}

fn unique_key_to_grain(uk: &Option<dbt_schemas::schemas::common::DbtUniqueKey>) -> Vec<String> {
    use dbt_schemas::schemas::common::DbtUniqueKey;
    match uk {
        Some(DbtUniqueKey::Single(s)) => vec![s.clone()],
        Some(DbtUniqueKey::Multiple(v)) => v.clone(),
        None => vec![],
    }
}

/// Write metadata parquet epoch files from in-memory typed structs.
///
/// Writes only `target/metadata/` epoch files — no DuckDB, no `target/index/`.
/// Independent of `write_json`. Errors are non-fatal — logged as warnings.
#[allow(clippy::cognitive_complexity)]
pub fn write_metadata_parquet(
    arg: &EvalArgs,
    manifest: &DbtManifest,
    resolved_state: Option<&ResolverState>,
    schema_store: Option<&dyn SchemaStoreTrait>,
    column_lineage: Option<&[CllEdge]>,
    recomputed_column_lineage_targets: &HashSet<String>,
    grain_infos: &HashMap<String, PlanGrainInfo>,
) {
    write_metadata_parquet_impl(
        arg,
        manifest,
        resolved_state,
        schema_store,
        column_lineage,
        recomputed_column_lineage_targets,
        grain_infos,
    );
}

#[allow(clippy::cognitive_complexity)]
fn write_metadata_parquet_impl(
    arg: &EvalArgs,
    manifest: &DbtManifest,
    resolved_state: Option<&ResolverState>,
    schema_store: Option<&dyn SchemaStoreTrait>,
    column_lineage: Option<&[CllEdge]>,
    recomputed_column_lineage_targets: &HashSet<String>,
    grain_infos: &HashMap<String, PlanGrainInfo>,
) {
    use dbt_common::static_analysis::is_strict_static_analysis;
    use dbt_index_core::hash_str;

    let ingested_at: i64 = resolved_state
        .map(|rs| rs.run_started_at.timestamp_micros())
        .unwrap_or_else(|| Utc::now().timestamp_micros());

    let mut compiled_node_rows: Vec<dbt_metadata_parquet::compiled_node::CompiledNodeRow> =
        Vec::new();
    let mut compile_column_rows: Vec<dbt_metadata_parquet::compile_columns::CompileColumnRow> =
        Vec::new();

    // grain tracking
    let mut grain_tested: HashMap<&str, Vec<String>> = HashMap::new();
    let mut unique_key_grain: HashMap<&str, Vec<String>> = HashMap::new();

    for (uid, node) in &manifest.nodes {
        match node {
            DbtNode::Test(t) => {
                if let Some(tm) = &t.test_metadata {
                    if let Some(attached) = t.attached_node.as_deref() {
                        match tm.name.as_str() {
                            "unique_combination_of_columns" => {
                                if let Some(cols) =
                                    tm.kwargs.get("combination_of_columns").and_then(|v| {
                                        v.as_sequence().map(|seq| {
                                            seq.iter()
                                                .filter_map(|i| i.as_str().map(str::to_string))
                                                .collect::<Vec<_>>()
                                        })
                                    })
                                {
                                    if !cols.is_empty() {
                                        grain_tested.insert(attached, cols);
                                    }
                                }
                            }
                            "unique" => {
                                let col = t.column_name.as_deref().or_else(|| {
                                    tm.kwargs.get("column_name").and_then(|v| v.as_str())
                                });
                                if let Some(col) = col {
                                    grain_tested
                                        .entry(attached)
                                        .or_insert_with(|| vec![col.to_string()]);
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            DbtNode::Model(m) => {
                let cols = unique_key_to_grain(&m.config.unique_key);
                if !cols.is_empty() {
                    unique_key_grain.insert(uid, cols);
                }
            }
            _ => {}
        }
    }
    for (uid, cols) in unique_key_grain {
        grain_tested.entry(uid).or_insert(cols);
    }

    for (uid, node) in &manifest.nodes {
        let b = match node {
            DbtNode::Model(x) => &x.__base_attr__,
            DbtNode::Seed(x) => &x.__base_attr__,
            DbtNode::Snapshot(x) => &x.__base_attr__,
            DbtNode::Analysis(x) => &x.__base_attr__,
            DbtNode::Test(x) => &x.__base_attr__,
            DbtNode::Operation(x) => &x.__base_attr__,
            DbtNode::Function(x) => &x.__base_attr__,
        };

        // compile/nodes requires --write-metadata --static-analysis strict.
        // Without strict SA there is no grain_infos from LP and no schema inference;
        // compile/nodes would only duplicate what parse/nodes already contains.
        let write_compile_nodes =
            arg.write_metadata && arg.static_analysis.is_some_and(is_strict_static_analysis);
        if write_compile_nodes {
            let grain_declared: Vec<String> = match node {
                DbtNode::Model(m) => m.primary_key.as_ref().cloned().unwrap_or_default(),
                _ => vec![],
            };
            let grain_tested_val = grain_tested.get(uid.as_str()).cloned().unwrap_or_default();
            let grain_inferred = grain_infos
                .get(uid.as_str())
                .map(|info| info.columns.clone())
                .unwrap_or_default();
            let grain = if !grain_declared.is_empty() {
                grain_declared.clone()
            } else if !grain_tested_val.is_empty() {
                grain_tested_val.clone()
            } else {
                grain_inferred.clone()
            };

            compiled_node_rows.push(dbt_metadata_parquet::compiled_node::CompiledNodeRow {
                unique_id: uid.to_string(),
                compiled_code: b.compiled_code.clone(),
                compiled_code_hash: b.compiled_code.as_deref().map(hash_str),
                compiled_path: b.compiled_path.clone(),
                grain: serde_json::to_string(&grain).unwrap_or_default(),
                grain_declared: serde_json::to_string(&grain_declared).unwrap_or_default(),
                grain_tested: serde_json::to_string(&grain_tested_val).unwrap_or_default(),
                table_role: None,
                ingested_at,
            });
        }

        // compile/columns: inferred types — requires --write-metadata --static-analysis strict.
        if write_compile_nodes {
            if let Some(store) = schema_store {
                if let Some(entry) = store.get_schema_by_unique_id(uid) {
                    for (idx, field) in entry.inner().fields().iter().enumerate() {
                        compile_column_rows.push(
                            dbt_metadata_parquet::compile_columns::CompileColumnRow {
                                unique_id: uid.to_string(),
                                column_name: field.name().clone(),
                                column_index: idx as i32,
                                column_type: Some(field.data_type().to_string()),
                                description: None,
                                ingested_at,
                            },
                        );
                    }
                }
            }
        }
    }

    // sources: compile/columns (parse/columns now written by parse_state::save)
    for (uid, src) in &manifest.sources {
        // compile/columns — src.columns is already merged with schema types by update_manifest
        if !src.columns.is_empty() {
            for (idx, col) in src.columns.iter().enumerate() {
                compile_column_rows.push(dbt_metadata_parquet::compile_columns::CompileColumnRow {
                    unique_id: uid.to_string(),
                    column_name: col.name.clone(),
                    column_index: idx as i32,
                    column_type: col.data_type.clone(),
                    description: col.description.clone(),
                    ingested_at,
                });
            }
        }
    }

    // CLL edges arrive pre-deduped from cll_edges_from_lineage_results.
    let cll_ingested_at = ingested_at;
    let cll_edges: &[CllEdge] = column_lineage.unwrap_or(&[]);

    let targets_opt = if recomputed_column_lineage_targets.is_empty() {
        None
    } else {
        Some(recomputed_column_lineage_targets)
    };
    let compiled_nodes_targets = targets_opt;
    let compile_columns_targets = targets_opt;

    let cll_dir = arg.metadata_dir().join("compile").join("column_lineage");
    let compiled_nodes_dir = arg.metadata_dir().join("compile").join("nodes");
    let compile_columns_dir = arg.metadata_dir().join("compile").join("columns");

    let alive_node_count = manifest.nodes.len();

    let t_write = std::time::Instant::now();
    let errors: Vec<String> = std::thread::scope(|s| {
        let t_cll = s.spawn(|| {
            let cll_rows: Vec<dbt_metadata_parquet::cll_epoch::CllRow> = cll_edges
                .iter()
                .map(|e| dbt_metadata_parquet::cll_epoch::CllRow {
                    from_node_unique_id: e.from_node.clone(),
                    from_column_name: e.from_col.clone(),
                    to_node_unique_id: e.to_node.clone(),
                    to_column_name: e.to_col.clone(),
                    lineage_kind: e.op.to_string(),
                    ingested_at: cll_ingested_at,
                })
                .collect();
            dbt_metadata_parquet::cll_epoch::write_cll_epoch(
                &cll_dir,
                cll_rows,
                cll_ingested_at,
                targets_opt,
                Some(alive_node_count),
                None,
            )
            .err()
            .map(|e| format!("metadata: cll_epoch: {e}"))
        });
        let t_compiled = s.spawn(|| {
            dbt_metadata_parquet::compiled_node::write_compiled_nodes(
                &compiled_nodes_dir,
                compiled_node_rows,
                compiled_nodes_targets,
                Some(alive_node_count),
                None,
            )
            .err()
            .map(|e| format!("metadata: compiled_nodes: {e}"))
        });
        let t_compile_cols = s.spawn(|| {
            dbt_metadata_parquet::compile_columns::write_compile_columns(
                &compile_columns_dir,
                compile_column_rows,
                compile_columns_targets,
                Some(alive_node_count),
                None,
            )
            .err()
            .map(|e| format!("metadata: compile_columns: {e}"))
        });
        [
            t_cll.join().unwrap_or(None),
            t_compiled.join().unwrap_or(None),
            t_compile_cols.join().unwrap_or(None),
        ]
        .into_iter()
        .flatten()
        .collect()
    });

    if std::env::var_os("DBT_LINEAGE_TIMING").is_some() {
        eprintln!(
            "[lineage] {:>8.1}ms  thread::scope write parquet (3 parallel)",
            t_write.elapsed().as_secs_f64() * 1000.0
        );
    }

    for e in errors {
        emit_warn_log_message(ErrorCode::Generic, e, arg.io.status_reporter.as_ref());
    }
}
