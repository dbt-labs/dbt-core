use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;

use async_trait::async_trait;
use chrono::Utc;
use dbt_common::io_args::EvalArgs;
use dbt_common::tracing::emit::emit_warn_log_message;
use dbt_common::{ErrorCode, FsResult};
use dbt_lineage_core::{ColIdWithOp, PlanGrainInfo};
use dbt_scheduler::node_selector::ColId;
use dbt_schema_store::SchemaStoreTrait;
use dbt_schemas::schemas::manifest::{DbtManifest, DbtNode};
use dbt_schemas::state::ResolverState;
use dbt_tasks_core::RunTaskResults;

#[async_trait]
pub trait IndexHooks: Send + Sync {
    fn create_docs_providers(
        &self,
        _index_dir: &Path,
        _metadata_dir: Option<&Path>,
    ) -> FsResult<Option<dbt_docs_server::Providers>> {
        Ok(None)
    }

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
    ) -> FsResult<BTreeMap<ColId, BTreeSet<ColIdWithOp>>> {
        Ok(BTreeMap::new())
    }
}

pub struct IndexFeature {
    pub hooks: Box<dyn IndexHooks>,
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
    column_lineage: Option<&BTreeMap<ColId, BTreeSet<ColIdWithOp>>>,
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
    column_lineage: Option<&BTreeMap<ColId, BTreeSet<ColIdWithOp>>>,
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

    // CLL epoch rows
    let cll_ingested_at = ingested_at;
    let mut cll_epoch_rows: Vec<dbt_metadata_parquet::cll_epoch::CllRow> = Vec::new();
    if let Some(lineage) = column_lineage {
        // Deduplicate per (from_node, from_col, to_node, to_col): keep strongest kind.
        // Priority: copy=0 > mod=1 > scan=2. One pass with a HashMap, O(n).
        fn kind_rank(k: &str) -> u8 {
            match k {
                "copy" => 0,
                "mod" => 1,
                _ => 2, // scan and anything unknown
            }
        }
        type BestKey<'a> = (&'a str, &'a str, &'a str, &'a str);
        type BestVal<'a> = (&'a str, Option<&'a str>);
        let mut best: HashMap<BestKey<'_>, BestVal<'_>> = HashMap::new();
        for (to_col, from_cols) in lineage {
            let to_node = to_col.table.as_str();
            let to_col_name = if to_col.column.is_empty() {
                None
            } else {
                Some(to_col.column.as_str())
            };
            for from_col in from_cols {
                let key = (
                    from_col.col_id.table.as_str(),
                    from_col.col_id.column.as_str(),
                    to_node,
                    to_col_name.unwrap_or(""),
                );
                let rank = kind_rank(&from_col.op);
                let entry = best.entry(key).or_insert((&from_col.op, to_col_name));
                if rank < kind_rank(entry.0) {
                    *entry = (&from_col.op, to_col_name);
                }
            }
        }
        for ((from_node, from_col_name, to_node, to_col_str), (kind, to_col_name)) in &best {
            cll_epoch_rows.push(dbt_metadata_parquet::cll_epoch::CllRow {
                from_node_unique_id: (*from_node).to_string(),
                from_column_name: (*from_col_name).to_string(),
                to_node_unique_id: (*to_node).to_string(),
                to_column_name: to_col_name.map(|s| s.to_string()),
                lineage_kind: (*kind).to_string(),
                ingested_at: 0, // overwritten by write_cll_epoch
            });
            let _ = to_col_str; // key stores "" for None; to_col_name is authoritative
        }
    }

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

    let errors: Vec<String> = std::thread::scope(|s| {
        let t_cll = s.spawn(|| {
            dbt_metadata_parquet::cll_epoch::write_cll_epoch(
                &cll_dir,
                cll_epoch_rows,
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

    for e in errors {
        emit_warn_log_message(ErrorCode::Generic, e, arg.io.status_reporter.as_ref());
    }
}
