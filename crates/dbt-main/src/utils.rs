use std::collections::{BTreeMap, BTreeSet};

use chrono::Utc;
use dbt_common::ErrorCode;
use dbt_common::io_args::EvalArgs;
use dbt_common::tracing::emit::emit_warn_log_message;
use dbt_schemas::schemas::RunResultOutput;
use dbt_schemas::schemas::manifest::{DbtManifest, DbtNode};
use dbt_schemas::stats::Stats;
use dbt_tasks_core::stats_to_results;

/// Minimal context captured before `SystemArgs` is moved into the async runtime.
/// Passed back to `run_cli` so it can write the invocation record unconditionally at exit,
/// regardless of which exit path (success / error / NoFilesChanged / early checkpoint) was taken.
pub struct InvocationContext {
    invocations_dir: std::path::PathBuf,
    invocation_id: String,
    command: String,
    selector: Option<String>,
    cli_args: Vec<String>,
    target_name: Option<String>,
    profile_name: Option<String>,
    start: std::time::Instant,
}

impl InvocationContext {
    pub fn new(
        metadata_dir: std::path::PathBuf,
        io: &dbt_common::io_args::IoArgs,
        command: dbt_common::io_args::FsCommand,
        common_args: &dbt_clap_core::CommonArgs,
    ) -> Self {
        let selector = common_args.select.as_ref().map(|v| v.join(" "));
        Self {
            invocations_dir: metadata_dir.join("run").join("invocations"),
            invocation_id: io.invocation_id.to_string(),
            command: command.as_str().to_string(),
            selector,
            cli_args: std::env::args().collect(),
            target_name: common_args.target.clone(),
            profile_name: common_args.profile.clone(),
            start: std::time::Instant::now(),
        }
    }

    pub fn write(self, status: &str) {
        use dbt_metadata_parquet::invocations::{InvocationRow, write_invocation};
        let elapsed_secs = self.start.elapsed().as_secs_f64();
        let ingested_at: i64 = Utc::now().timestamp_micros();
        let row = InvocationRow {
            invocation_id: self.invocation_id,
            command: self.command,
            status: status.to_string(),
            selector: self.selector,
            cli_args: self.cli_args,
            project_name: None,
            adapter_type: None,
            target_name: self.target_name,
            profile_name: self.profile_name,
            environment_id: None,
            environment_name: None,
            account_identifier: None,
            defer_env_id: None,
            user_id: None,
            user_name: None,
            dbt_version: env!("CARGO_PKG_VERSION").to_string(),
            git_sha: None,
            git_branch: None,
            git_is_dirty: None,
            elapsed_secs: Some(elapsed_secs),
            // TODO: populate from resolved_state.nodes.len() after compilation.
            // Currently NULL because the single write site (execute_fs_and_shutdown) has
            // no access to the node count. Nobody queries this column from parquet yet —
            // dbt-index timings reads from dbt_rt.invocations (DuckDB), not this file.
            node_count: None,
            ingested_at,
        };
        if let Err(e) = write_invocation(&self.invocations_dir, row) {
            eprintln!("warning: failed to write invocation parquet: {e}");
        }
    }
}

/// Write runtime result rows to the metadata/runtime/results parquet directory.
/// Called alongside `write_run_results_json` at end of run/test/build.
pub(crate) fn write_runtime_results_parquet(stats: &Stats, arg: &EvalArgs) {
    use dbt_metadata_parquet::runtime_results::{RuntimeResultRow, write_runtime_results};

    let results_dir = arg.metadata_dir().join("run").join("results");

    let ingested_at: i64 = Utc::now().timestamp_micros();

    let nodes = match stats.nodes.as_ref() {
        Some(n) => n,
        None => return,
    };

    let rows: Vec<RuntimeResultRow> = stats
        .stats
        .iter()
        .map(|stat| {
            let result: RunResultOutput = stats_to_results(stat, nodes).into();
            RuntimeResultRow {
                invocation_id: arg.io.invocation_id.to_string(),
                unique_id: result.unique_id.clone(),
                status: result.status.clone(),
                message: result.message.clone(),
                execution_time: Some(result.execution_time),
                thread_id: Some(result.thread_id.clone()),
                failures: result.failures,
                compiled_code_hash: None,
                relation_name: result.relation_name.clone(),
                adapter_response: Some(
                    serde_json::to_string(&result.adapter_response).unwrap_or_default(),
                ),
                timing: Some(serde_json::to_string(&result.timing).unwrap_or_default()),
                ingested_at,
            }
        })
        .collect();

    if let Err(e) = write_runtime_results(&results_dir, &rows) {
        emit_warn_log_message(
            ErrorCode::IoError,
            format!("Failed to write runtime results parquet: {e}"),
            arg.io.status_reporter.as_ref(),
        );
    }
}

pub(crate) fn update_manifest_with_macro_depends_on(
    dbt_manifest: &mut DbtManifest,
    macro_depends_on: &BTreeMap<String, BTreeSet<String>>,
) {
    if macro_depends_on.is_empty() {
        return;
    }

    for (unique_id, node) in dbt_manifest.nodes.iter_mut() {
        if let DbtNode::Model(model_node) = node
            && let Some(macros) = macro_depends_on.get(unique_id)
        {
            model_node.__base_attr__.depends_on.macros = macros.iter().cloned().collect();
        }
        if let DbtNode::Test(test_node) = node
            && let Some(macros) = macro_depends_on.get(unique_id)
        {
            test_node.__base_attr__.depends_on.macros = macros.iter().cloned().collect();
        }
        if let DbtNode::Snapshot(snapshot_node) = node
            && let Some(macros) = macro_depends_on.get(unique_id)
        {
            snapshot_node.__base_attr__.depends_on.macros = macros.iter().cloned().collect();
        }
        if let DbtNode::Seed(seed_node) = node
            && let Some(macros) = macro_depends_on.get(unique_id)
        {
            seed_node.__base_attr__.depends_on.macros = macros.iter().cloned().collect();
        }
        if let DbtNode::Analysis(analysis_node) = node
            && let Some(macros) = macro_depends_on.get(unique_id)
        {
            analysis_node.__base_attr__.depends_on.macros = macros.iter().cloned().collect();
        }
        if let DbtNode::Operation(operation_node) = node
            && let Some(macros) = macro_depends_on.get(unique_id)
        {
            operation_node.__base_attr__.depends_on.macros = macros.iter().cloned().collect();
        }
    }
    for (unique_id, macro_node) in dbt_manifest.macros.iter_mut() {
        if let Some(macros) = macro_depends_on.get(unique_id) {
            macro_node.depends_on.macros = macros.iter().cloned().collect();
        }
    }
}
