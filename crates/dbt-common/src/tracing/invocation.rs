use dbt_telemetry::{
    InvocationCloudAttributes, InvocationEvalArgs, InvocationInfo, ProcessInfo, TelemetryAttributes,
};

use crate::io_args::{EvalArgs, ReplayMode};

fn create_invocation_eval_args(eval_arg: &EvalArgs) -> InvocationEvalArgs {
    let (replay_mode, replay_path) = match &eval_arg.replay {
        Some(ReplayMode::DbtReplay(path)) => (
            Some("dbt".to_string()),
            Some(path.to_string_lossy().to_string()),
        ),
        Some(ReplayMode::FsReplay(path)) => (
            Some("fs".to_string()),
            Some(path.to_string_lossy().to_string()),
        ),
        Some(ReplayMode::FsRecord(path)) => (
            Some("fs".to_string()),
            Some(path.to_string_lossy().to_string()),
        ),
        None => (None, None),
    };

    InvocationEvalArgs {
        command: eval_arg.command.clone(),
        profiles_dir: eval_arg
            .profiles_dir
            .as_ref()
            .map(|p| p.to_string_lossy().to_string()),
        packages_install_path: eval_arg
            .packages_install_path
            .as_ref()
            .map(|p| p.to_string_lossy().to_string()),
        target: eval_arg.target.clone(),
        profile: eval_arg.profile.clone(),
        vars: eval_arg.vars.clone(),
        limit: eval_arg.limit.map(|l| l as u64),
        num_threads: eval_arg.num_threads.map(|l| l as u64),
        selector: eval_arg.selector.clone(),
        select: eval_arg.select.iter().map(|s| s.to_string()).collect(),
        exclude: eval_arg.exclude.iter().map(|s| s.to_string()).collect(),
        indirect_selection: eval_arg.indirect_selection.map(|s| s.to_string()),
        output_keys: eval_arg.output_keys.iter().map(|s| s.to_string()).collect(),
        resource_types: eval_arg
            .resource_types
            .iter()
            .map(|s| s.to_string())
            .collect(),
        exclude_resource_types: eval_arg
            .exclude_resource_types
            .iter()
            .map(|s| s.to_string())
            .collect(),
        debug: eval_arg.debug,
        log_format: eval_arg.log_format.to_string(),
        log_level: eval_arg.log_level.map(|s| s.to_string()),
        log_path: eval_arg
            .log_path
            .as_ref()
            .map(|p| p.to_string_lossy().to_string()),
        target_path: eval_arg
            .target_path
            .as_ref()
            .map(|p| p.to_string_lossy().to_string()),
        project_dir: eval_arg
            .project_dir
            .as_ref()
            .map(|p| p.to_string_lossy().to_string()),
        quiet: eval_arg.quiet,
        write_json: eval_arg.write_json,
        write_catalog: eval_arg.write_catalog,
        update_deps: eval_arg.update_deps,
        replay_mode,
        replay_path,
        static_analysis: eval_arg.static_analysis.to_string(),
        interactive: eval_arg.interactive,
        task_cache_url: eval_arg.task_cache_url.clone(),
        run_cache_mode: eval_arg.run_cache_mode.to_string(),
        show_scans: eval_arg.show_scans,
        max_depth: eval_arg.max_depth as u64,
        use_fqtn: eval_arg.use_fqtn,
        skip_unreferenced_table_check: eval_arg.skip_unreferenced_table_check,
        state: eval_arg
            .state
            .as_ref()
            .map(|p| p.to_string_lossy().to_string()),
        defer_state: eval_arg
            .defer_state
            .as_ref()
            .map(|p| p.to_string_lossy().to_string()),
        connection: eval_arg.connection,
        warn_error: eval_arg.warn_error,
        warn_error_options: eval_arg.warn_error_options.clone(),
        version_check: eval_arg.version_check,
        defer: eval_arg.defer,
        fail_fast: eval_arg.fail_fast,
        empty: eval_arg.empty,
        sample: eval_arg.sample.clone(),
        full_refresh: eval_arg.full_refresh,
        favor_state: eval_arg.favor_state,
        refresh_sources: eval_arg.refresh_sources,
        send_anonymous_usage_stats: eval_arg.send_anonymous_usage_stats,
        check_all: eval_arg.check_all,
    }
}

/// Creates telemetry attributes for the Invocation span by extracting environment variables,
/// CLI flags, and other relevant information.
pub fn create_invocation_attributes(package: &str, eval_arg: &EvalArgs) -> TelemetryAttributes {
    // Capture raw command string
    let raw_command = std::env::args().collect::<Vec<_>>().join(" ");

    TelemetryAttributes::Invocation(Box::new(InvocationInfo {
        invocation_id: eval_arg.io.invocation_id.to_string(),
        raw_command,
        eval_args: create_invocation_eval_args(eval_arg),
        process_info: ProcessInfo::new(package),
        cloud_args: InvocationCloudAttributes::from_env_lossy(),
        metrics: Default::default(),
    }))
}
