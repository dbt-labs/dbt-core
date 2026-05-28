use std::{collections::BTreeMap, time::SystemTime};

use chrono::{DateTime, Utc};
use dbt_common::{
    ErrorCode, FsResult, io_args::EvalArgs, stdfs::File, tracing::emit::emit_warn_log_message,
};
use dbt_schemas::{
    schemas::{RunResultOutput, RunResultsArgs, RunResultsArtifact, RunResultsMetadata},
    stats::Stats,
};

use crate::stats_to_results;

/// Build a `RunResultsArtifact` from run stats.
fn build_run_results_artifact(stats: &Stats, arg: &EvalArgs) -> RunResultsArtifact {
    let now = SystemTime::now();
    let generated_at: DateTime<Utc> = DateTime::from(now);

    let results: Vec<RunResultOutput> = stats
        .stats
        .iter()
        .map(|stat| {
            stats_to_results(
                stat,
                stats
                    .nodes
                    .as_ref()
                    .expect("stats should have nodes for results generation"),
            )
            .into()
        })
        .collect();

    let total_elapsed_time: f64 = stats
        .stats
        .iter()
        .map(|stat| stat.get_duration().as_secs_f64())
        .sum();

    let metadata = RunResultsMetadata {
        dbt_schema_version: "https://schemas.getdbt.com/dbt/run-results/v6.json".to_string(),
        dbt_version: env!("CARGO_PKG_VERSION").to_string(),
        generated_at,
        invocation_id: arg.io.invocation_id.to_string(),
        invocation_started_at: None,
        env: dbt_common::constants::collect_dbt_custom_envs(),
    };

    let mut args_map = BTreeMap::new();
    let command_str = arg.command.as_str();

    args_map.insert(
        "command".to_string(),
        dbt_yaml::Value::string(command_str.to_string()),
    );
    args_map.insert(
        "which".to_string(),
        dbt_yaml::Value::string(command_str.to_string()),
    );
    args_map.insert(
        "static_analysis".to_string(),
        if let Some(sa) = arg.static_analysis {
            dbt_yaml::Value::string(sa.to_string())
        } else {
            dbt_yaml::Value::null()
        },
    );

    let args = RunResultsArgs {
        command: command_str.to_string(),
        which: command_str.to_string(),
        __other__: args_map,
    };

    RunResultsArtifact {
        metadata,
        results,
        elapsed_time: total_elapsed_time,
        args,
    }
}

// TODO: We need to add more information to the run_results.json file
pub fn write_run_results_json(stats: &Stats, arg: &EvalArgs) -> FsResult<()> {
    let run_results_path = arg.io.out_dir.join("run_results.json");
    let run_results_file = File::create(run_results_path)?;
    let run_results_artifact = build_run_results_artifact(stats, arg);
    serde_json::to_writer(run_results_file, &run_results_artifact)?;
    Ok(())
}

/// Write `run_results.json`, emitting a warning on failure instead of propagating the error.
pub fn write_run_results_json_or_warn(stats: &Stats, arg: &EvalArgs) {
    if let Err(e) = write_run_results_json(stats, arg) {
        emit_warn_log_message(
            ErrorCode::IoError,
            format!("Failed to write run_results.json: {e}"),
            arg.io.status_reporter.as_ref(),
        );
    }
}
