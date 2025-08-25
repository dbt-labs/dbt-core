use crate::dbt_sa_clap::{Cli, Commands};
use dbt_common::cancellation::CancellationToken;
use dbt_common::create_root_info_span;
use dbt_common::tracing::create_invocation_attributes;
use dbt_jinja_utils::invocation_args::InvocationArgs;
use dbt_loader::clean::execute_clean_command;
use dbt_schemas::man::execute_man_command;

use dbt_common::io_args::EvalArgs;
use dbt_common::{
    ErrorCode, FsResult, checkpoint_maybe_exit,
    constants::{DBT_MANIFEST_JSON, INSTALLING, VALIDATING},
    fs_err, fsinfo,
    io_args::{Phases, SystemArgs},
    logging::init_logger,
    pretty_string::GREEN,
    show_error, show_progress, show_progress_exit, show_result_with_default_title, stdfs,
    tracing::span_info::record_span_status,
};

use dbt_schemas::schemas::Nodes;
use dbt_schemas::state::Macros;
#[allow(unused_imports)]
use git_version::git_version;

use dbt_schemas::schemas::manifest::build_manifest;
use tracing::{Instrument, instrument};

use std::{sync::Arc, time::SystemTime};

use dbt_loader::{args::LoadArgs, load};
use dbt_parser::{args::ResolveArgs, resolver::resolve};

use serde_json::to_string_pretty;

// ------------------------------------------------------------------------------------------------

#[instrument(skip_all, level = "trace")]
pub async fn execute_fs(
    system_arg: SystemArgs,
    cli: Cli,
    token: CancellationToken,
) -> FsResult<i32> {
    // Resolve EvalArgs from SystemArgs and Cli. This will create out folders,
    // for commands that need it and canonicalize the paths. May error on invalid paths.
    let eval_arg = cli.to_eval_args(system_arg)?;

    init_logger((&eval_arg.io).into()).expect("Failed to initialize logger");

    // Create the Invocation span as a new root
    let invocation_span = create_root_info_span!(create_invocation_attributes("dbt-sa", &eval_arg));

    let result = do_execute_fs(&eval_arg, cli, token)
        .instrument(invocation_span.clone())
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
async fn do_execute_fs(eval_arg: &EvalArgs, cli: Cli, token: CancellationToken) -> FsResult<i32> {
    let start = SystemTime::now();

    if let Commands::Man(_) = &cli.command {
        return match execute_man_command(eval_arg).await {
            Ok(code) => Ok(code),
            Err(e) => {
                show_error!(&eval_arg.io, e);
                Ok(1)
            }
        };
    } else if let Commands::Init(init_args) = &cli.command {
        // Handle init command
        use dbt_common::init::run_init_workflow;

        show_progress!(
            &eval_arg.io,
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
        )
        .await
        {
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
                show_error!(&eval_arg.io, e);
                return Ok(1);
            }
        }
    }

    // Handle project specific commands
    match execute_setup_and_all_phases(eval_arg, cli, &start, &token).await {
        Ok(code) => Ok(code),
        Err(e) => {
            show_error!(&eval_arg.io, e);
            show_progress_exit!(eval_arg, start)
        }
    }
}

#[allow(clippy::cognitive_complexity)]
#[instrument(skip_all, level = "trace")]
async fn execute_setup_and_all_phases(
    eval_arg: &EvalArgs,
    cli: Cli,
    start: &SystemTime,
    token: &CancellationToken,
) -> FsResult<i32> {
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
        let build_time = if eval_arg.from_main {
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
        show_progress!(&eval_arg.io, info);
    }

    // Check if the command is `Clean`
    if let Commands::Clean(ref clean_args) = cli.command {
        match execute_clean_command(eval_arg, &clean_args.files, token).await {
            Ok(code) => Ok(code),
            Err(e) => {
                show_error!(&eval_arg.io, e);
                show_progress_exit!(eval_arg, start)
            }
        }
    } else {
        // Execute all steps of all other commands, if any throws an error we stop
        match execute_all_phases(eval_arg, &cli, token).await {
            Ok(code) => Ok(code),
            Err(e) => {
                show_error!(&eval_arg.io, e);
                show_progress_exit!(eval_arg, start)
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
            dbt_jinja_utils::listener::DefaultRenderingEventListenerFactory::default(),
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
