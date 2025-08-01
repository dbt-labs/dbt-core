use crate::dbt_sa_clap::{Cli, Commands};
use dbt_common::cancellation::CancellationToken;
use dbt_jinja_utils::invocation_args::InvocationArgs;
use dbt_loader::clean::execute_clean_command;
use dbt_schemas::man::execute_man_command;

use dbt_common::io_args::EvalArgs;
use dbt_common::{
    ErrorCode, FsResult, checkpoint_maybe_exit,
    constants::{DBT_MANIFEST_JSON, DBT_PROJECT_YML, DBT_TARGET_DIR_NAME, INSTALLING, VALIDATING},
    fs_err, fsinfo,
    io_args::{Phases, SystemArgs},
    io_utils::determine_project_dir,
    logging::init_logger,
    pretty_string::GREEN,
    show_error, show_progress, show_progress_exit, show_result_with_default_title, stdfs,
    tracing::{ToTracingValue, constants::TRACING_ATTR_FIELD, span_info::record_span_status},
};

use dbt_schemas::schemas::{Nodes, telemetry::SpanAttributes};
use dbt_schemas::state::Macros;
#[allow(unused_imports)]
use git_version::git_version;

use dbt_schemas::schemas::manifest::build_manifest;
use tracing::instrument;

use std::{path::Path, sync::Arc, time::SystemTime};

use dbt_loader::{args::LoadArgs, load};
use dbt_parser::{args::ResolveArgs, resolver::resolve};

use serde_json::to_string_pretty;

// ------------------------------------------------------------------------------------------------

#[instrument(skip_all, level = "trace")]
pub async fn execute_fs(arg: SystemArgs, cli: Cli, token: CancellationToken) -> FsResult<i32> {
    init_logger((&arg.io).into()).expect("Failed to initialize logger");

    // Create the Invocation span as a new root
    let invocation_span = tracing::info_span!(
        parent: None,
        "Invocation",
        { TRACING_ATTR_FIELD } = SpanAttributes::Invocation {
            invocation_id: arg.io.invocation_id.to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            host_os: std::env::consts::OS.to_string(),
            host_arch: std::env::consts::ARCH.to_string(),
            target: arg.target.clone(),
            metrics: None,
        }
        .to_tracing_value(),
    );

    let result = invocation_span
        .in_scope(|| async { do_execute_fs(arg, cli, token).await })
        .await;

    // Record span run result
    match result {
        Ok(0) => record_span_status(&invocation_span, None),
        Ok(_) => record_span_status(&invocation_span, Some("Executed with errors")),
        Err(ref e) => record_span_status(&invocation_span, Some(format!("Error: {e}").as_str())),
    };

    result
}

#[allow(clippy::cognitive_complexity)]
#[instrument(skip_all, level = "trace")]
async fn do_execute_fs(arg: SystemArgs, cli: Cli, token: CancellationToken) -> FsResult<i32> {
    let start = SystemTime::now();

    if let Commands::Man(cmd) = &cli.command {
        let arg = cmd.to_eval_args(arg.clone(), Path::new("."), Path::new("."));
        return match execute_man_command(&arg).await {
            Ok(code) => Ok(code),
            Err(e) => {
                show_error!(&arg.io, e);
                Ok(1)
            }
        };
    } else if let Commands::Init(init_args) = &cli.command {
        // Handle init command
        use dbt_common::init::run_init_workflow;

        show_progress!(
            &arg.io,
            fsinfo!(
                INSTALLING.into(),
                "dbt project and profile setup".to_string()
            )
        );

        let project_name = if init_args.project_name == "jaffle_shop" {
            None // Use default
        } else {
            Some(init_args.project_name.clone())
        };

        match run_init_workflow(
            project_name,
            init_args.skip_profile_setup,
            init_args.common_args.profile.clone(), // Get profile from common args
        ) {
            Ok(()) => {
                // If profile setup was not skipped, run debug to validate credentials
                if init_args.skip_profile_setup {
                    return Ok(0);
                }

                log::info!(
                    "{} profile inputs, adapters, and connection",
                    GREEN.apply_to(VALIDATING)
                );
                log::info!(""); // Add empty line for spacing
            }
            Err(e) => {
                show_error!(&arg.io, e);
                return Ok(1);
            }
        }
    }

    // Handle project specific commands
    match execute_setup_and_all_phases(arg.clone(), cli.clone(), &start, &token).await {
        Ok(code) => Ok(code),
        Err(e) => {
            show_error!(&arg.io, e);
            show_progress_exit!(arg, start)
        }
    }
}

#[allow(clippy::cognitive_complexity)]
#[instrument(skip_all, level = "trace")]
async fn execute_setup_and_all_phases(
    system_arg: SystemArgs,
    cli: Cli,
    start: &SystemTime,
    token: &CancellationToken,
) -> FsResult<i32> {
    let from_main = system_arg.from_main;

    // Process cli arguments, determine in_dir, out_dir, create eval_args
    let node_targets = vec![];
    let maybe_project_dir = cli.project_dir();
    let in_dir = if let Some(project_dir) = maybe_project_dir {
        project_dir
    } else {
        determine_project_dir(node_targets.as_slice(), DBT_PROJECT_YML)
            .map_err(|e| fs_err!(ErrorCode::IoError, "{}", e))?
    };
    let out_dir = cli
        .target_path()
        .unwrap_or(in_dir.join(DBT_TARGET_DIR_NAME));
    let arg = cli.to_eval_args(system_arg.clone(), &in_dir, &out_dir, from_main);

    // Header ..
    // current_exe errors when running in dbt-cloud
    // https://github.com/rust-lang/rust/issues/46090
    #[cfg(debug_assertions)]
    {
        use chrono::{DateTime, Local};
        use dbt_common::constants::DBT_SA_CLI;
        use std::env;
        let exe_path = env::current_exe()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get current exe path: {}", e))?;
        let modified_time = stdfs::last_modified(&exe_path)?;

        // Convert SystemTime to DateTime<Local>
        let datetime: DateTime<Local> = DateTime::from(modified_time);
        let formatted_time = datetime.format("%Y-%m-%d %H:%M:%S").to_string();
        let build_time = if from_main {
            let git_hash = git_version!(fallback = "unknown");
            format!(
                "{} ({} {})",
                env!("CARGO_PKG_VERSION"),
                git_hash,
                formatted_time
            )
        } else {
            "".to_string()
        };
        let info = fsinfo!(DBT_SA_CLI.into(), build_time);
        show_progress!(&arg.io, info);
    }

    // Check if the command is `Clean`
    if let Commands::Clean(ref clean_args) = cli.command {
        match execute_clean_command(&arg, &clean_args.files, token).await {
            Ok(code) => Ok(code),
            Err(e) => {
                show_error!(&arg.io, e);
                show_progress_exit!(&arg, start)
            }
        }
    } else {
        // Execute all steps of all other commands, if any throws an error we stop
        match execute_all_phases(&arg, &cli, token).await {
            Ok(code) => Ok(code),
            Err(e) => {
                show_error!(&arg.io, e);
                show_progress_exit!(&arg, start)
            }
        }
    }
}

#[allow(clippy::cognitive_complexity)]
#[instrument(skip_all, level = "trace")]
async fn execute_all_phases(
    arg: &EvalArgs,
    _cli: &Cli,
    token: &CancellationToken,
) -> FsResult<i32> {
    let start = SystemTime::now();

    // Loads all .yml files + collects all included files
    let load_args = LoadArgs::from_eval_args(arg);
    let invocation_args = InvocationArgs::from_eval_args(arg);
    let (dbt_state, num_threads, _dbt_cloud) = load(&load_args, &invocation_args, token).await?;

    let arg = arg
        .with_target(dbt_state.dbt_profile.target.to_string())
        .with_threads(num_threads);
    show_result_with_default_title!(&arg.io, ShowOptions::InputFiles, &dbt_state.to_string());

    // This also exits the init command b/c init `to_eval_args` sets the phase to debug
    checkpoint_maybe_exit!(Phases::Debug, &arg, start);
    checkpoint_maybe_exit!(Phases::Deps, &arg, start);

    // Parses (dbt parses) all .sql files with execute == false
    let resolve_args = ResolveArgs::try_from_eval_args(&arg)?;
    let invocation_args = InvocationArgs::from_eval_args(&arg);
    let arc_dbt_state = Arc::new(dbt_state);
    let (resolved_state, _jinja_env) = resolve(
        &resolve_args,
        &invocation_args,
        arc_dbt_state,
        Macros::default(),
        Nodes::default(),
        Some(Arc::new(
            dbt_jinja_utils::listener::DefaultListenerFactory::default(),
        )),
        token,
    )
    .await?;

    let dbt_manifest = build_manifest(&arg.io.invocation_id.to_string(), &resolved_state);

    if arg.write_json {
        let dbt_manifest_path = arg.io.out_dir.join(DBT_MANIFEST_JSON);
        stdfs::create_dir_all(dbt_manifest_path.parent().unwrap())?;
        stdfs::write(dbt_manifest_path, serde_json::to_string(&dbt_manifest)?)?;
    }

    show_result_with_default_title!(
        &arg.io,
        ShowOptions::Manifest,
        to_string_pretty(&dbt_manifest)?
    );

    show_progress_exit!(&arg, start)
}
