use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use dbt_adapter::load_store::ResultStore;
use dbt_adapter::{
    Adapter, AdapterType, convert_macro_result_to_record_batch,
    relation::{RelationObject, create_relation},
};
use dbt_clap_core::{
    Cli, Command, CompileArgs, CoreCommand, DocsServeArgs as ClapDocsServeArgs, DocsSubcommand,
    LoginSubcommand, ProjectTemplate, ShowArgs, SystemCommand,
};
use dbt_common::cancellation::CancellationToken;
use dbt_common::io_utils::StatusReporter;
use dbt_common::{
    ErrorCode, FsResult,
    artifact_io::write_artifact_to_file,
    constants::{
        DBT_CATALOG_JSON, DBT_COMPILED_DIR_NAME, DBT_MANIFEST_JSON, ERROR, INSTALLING, VALIDATING,
    },
    create_root_info_span, fs_err,
    io_args::{DisplayFormat, EvalArgs, IoArgs, Phases, ShowOptions, SystemArgs},
    path::get_target_write_path,
    pretty_string::{GREEN, RED, color_quotes},
    stdfs,
    tracing::{
        dbt_metrics::{
            FusionMetricKey, NodeSubOutcome, OutcomeCountsKey, OutcomeKind, error_count_checkpoint,
            get_error_count, return_exit_code_from_error_counter,
        },
        emit::{
            emit_error_log_from_fs_error, emit_error_log_message, emit_info_event,
            emit_info_log_message, emit_info_progress_message, emit_warn_log_message,
        },
        invocation::create_invocation_attributes,
        metrics::get_metric,
        span_info::record_span_status,
    },
    warn_error_options::{SupportedLegacyWarnError, WarnErrorDecision},
};
use dbt_common::{FsError, io_args::FsCommand};
use dbt_dag::schedule::Schedule;
use dbt_docs_server::providers::Backend;
use dbt_features::feature_stack::FeatureStack;
use dbt_features::index::write_metadata_parquet;
use dbt_index_core::backend::DuckDbViewsBackend;
use dbt_index_core::ingest::ingest_state::IngestState;
use dbt_index_core::ingest::metadata_to_parquet::{
    apply_delta_direct, ingest_from_metadata_direct,
};
use dbt_index_core::{WriteSource, save_artifact_meta};
use dbt_init::init;
use dbt_jinja_utils::{
    jinja_environment::JinjaEnv,
    listener::{DefaultJinjaTypeCheckEventListenerFactory, JinjaTypeCheckingEventListenerFactory},
    utils::get_catalog_by_relations,
};
use dbt_lineage_core::ColIdWithOp;
use dbt_loader::{
    clean::execute_clean_command, execute_deps_command, upload_artifacts_ingest_if_enabled,
};
use dbt_login::{execute_login, execute_login_status};
use dbt_scheduler::node_selector::ColId;
use dbt_schema_store::{DataStoreTrait, SchemaStoreTrait};
use dbt_schemas::{
    man::execute_man_command,
    schemas::legacy_catalog::{DbtCatalog, build_catalog},
};
use dbt_schemas::{
    schemas::{
        DbtModel, InternalDbtNodeAttributes,
        common::{DbtMaterialization, ResolvedQuoting},
        relations::base::BaseRelation,
    },
    state::ResolverState,
};
use dbt_tasks_core::{
    RunTaskResults, task_runner_hooks::TaskRunnerHooksFactory,
    utils::write_run_results_json_or_warn,
};
use dbt_tasks_sa::base_context::build_base_context;
use dbt_telemetry::ArtifactType;
use dbt_telemetry::{
    CompiledCodeInline, NodeOutcome, NodeSkipReason, ProgressMessage, ShowDataOutput,
    ShowDataOutputFormat, ShowResult,
};

#[cfg(debug_assertions)]
use git_version::git_version;
use minijinja::Value;
use serde_json::{json, to_string_pretty};
use tracing::{Instrument, Span};
use vortex_client::client::vortex_producer_is_running;
use vortex_events::{build_result_string, invocation_end_event};

use crate::{
    compilation::{
        DbtCustomScheduleDescription, DbtProjectCompilation, DbtProjectCompilationCacheChanges,
        DbtRunTasksResult, DbtScheduleDescription, update_manifest,
    },
    retry::{RETRIABLE_COMMANDS, RetryState},
    uninstall::exec_uninstall,
    update::exec_update,
    utils::{InvocationContext, write_catalog_stats_parquet, write_runtime_results_parquet},
    vars::validate_engine_env_vars,
};

// ------------------------------------------------------------------------------------------------

pub async fn execute_fs(
    system_arg: SystemArgs,
    cli: Box<Cli>,
    feature_stack: Arc<FeatureStack>,
    token: CancellationToken,
) -> FsResult<()> {
    execute_fs_and_shutdown(system_arg, cli, false, feature_stack, token).await
}

pub async fn execute_fs_and_shutdown(
    system_arg: SystemArgs,
    cli: Box<Cli>,
    shutdown: bool,
    feature_stack: Arc<FeatureStack>,
    token: CancellationToken,
) -> FsResult<()> {
    // Resolve EvalArgs from SystemArgs and Cli. This will create out folders,
    // for commands that need it and canonicalize the paths. May error on invalid paths.
    // If this fails (e.g., not in a dbt project directory), print a concise error and exit 1.
    let mut eval_arg = cli.to_eval_args(system_arg.clone()).map_err(|e| {
        // before the logger is initialized, so we print directly to stderr
        eprintln!(
            "{} {}",
            RED.apply_to(format!("[{ERROR}]")),
            color_quotes(e.pretty().as_str())
        );
        FsError::exit_with_status(1)
    })?;

    // --dirty without --select: synthesize a `seed_id+ seed_id+ ...` selector so the
    // scheduler runs only the dirty nodes and their descendants — ancestors are loaded
    // for dep closure but not scheduled.  Falls back to select=None (run all) when the
    // cache doesn't exist yet or nothing is dirty.
    if cli.common_args().dirty && eval_arg.select.is_none() {
        use dbt_metadata::partial_parse::dirty_select_expression;
        if let Some(expr) = dirty_select_expression(&eval_arg.io) {
            eval_arg.select = Some(expr);
        }
    }

    let invocation_id = eval_arg.io.invocation_id.to_string();
    let send_anonymous_usage_stats = eval_arg.io.send_anonymous_usage_stats;
    let dbt_distribution = feature_stack
        .instrumentation
        .event_emitter
        .dbt_distribution();

    // Capture invocation context now — eval_arg.metadata_dir() is correctly resolved here.
    // Written at exit so every path (success / error / warm-parse / Ctrl+C) is covered.
    let invocation_ctx = if eval_arg.write_metadata {
        let common = cli.common_args();
        Some(InvocationContext::new(
            eval_arg.metadata_dir(),
            &eval_arg.io,
            eval_arg.command,
            &common,
        ))
    } else {
        None
    };

    // Create the Invocation span as a new root
    let invocation_span = create_root_info_span(create_invocation_attributes("dbt", &eval_arg));
    let result = do_execute_fs(&eval_arg, cli, feature_stack, &token)
        .instrument(invocation_span.clone())
        .await;

    // Record span run result
    let span_status = match &result {
        Ok(()) => None,
        Err(err) => match err.exit_status() {
            Some(0) => None,
            Some(_) => Some("Executed with errors".to_string()),
            None => Some(format!("Error: {}", err)),
        },
    };
    record_span_status(&invocation_span, span_status.as_deref());

    // Write invocation record — one place covers every exit path.
    if let Some(ctx) = invocation_ctx {
        // NoFilesChanged (warm parse, no-op) propagates via `?` as Err rather than Ok —
        // it's a "successful sentinel", not a real failure. exit_status() == Some(0)
        // catches it and ExitRepl; everything else with exit_status() == None is a real error.
        let status = match &result {
            Ok(()) => "success",
            Err(e) if e.exit_status() == Some(0) => "success",
            Err(_) => "error",
        };
        ctx.write(status);
    }

    // Shutdown must be called to ensure vortex flushes all events.
    // If any event is sent after this, it will be dropped.
    //
    // TODO: this part currently accounts for by far the largest portion of the
    // shutdown (aka "post-exec stall") time. We should optimize this by either:
    // 1) investigate whether vortex itself can be further optimized; or
    // 2) move vortex telemetry into a separate subprocess so it can run fully
    //    async without blocking the main process.
    if send_anonymous_usage_stats || (shutdown && vortex_producer_is_running()) {
        debug_assert!(
            send_anonymous_usage_stats,
            "Vortex producer is running, but send_anonymous_usage_stats \
is false. This should not happen."
        );
        let result_string = build_result_string(&result);
        tokio::task::spawn_blocking(move || {
            // This blocks on the worker thread until the final batch(es)
            // are sent, so we run it as a blocking tokio task.
            invocation_end_event(invocation_id, result_string, dbt_distribution, shutdown);
        })
        .instrument(invocation_span)
        .await
        .map_err(|e| {
            if e.is_cancelled() {
                Ok(()) // ignore cancellation
            } else {
                Err(e) // let JoinError::Panic cause a panic
            }
        })
        .unwrap();
    }

    result
}

#[allow(clippy::cognitive_complexity)]
async fn do_execute_fs(
    eval_arg: &EvalArgs,
    cli: Box<Cli>,
    feature_stack: Arc<FeatureStack>,
    token: &CancellationToken,
) -> FsResult<()> {
    use CoreCommand::*;
    // Current versions of rustls require us to explicitly install a default provider.
    // The default provider can only be installed once per process, so
    // be defensive here (tests may use the same process)
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("Failed to install crypto provider");
    }

    feature_stack
        .cli
        .hooks
        .will_execute(&cli, eval_arg, &feature_stack)
        .await?;

    if let Command::Core(System(cmd)) = cli.command {
        return match &cmd.command {
            SystemCommand::Update(system_update_args) => {
                exec_update(system_update_args).await.inspect_err(|e| {
                    // Without this, the FsError returned by exec_update is silently
                    // swallowed: the invocation summary reports "Finished 'system'
                    // successfully" (errors==0) while the process exits non-zero,
                    // leaving wrappers like orc/dispatch with no diagnostic. Logging
                    // here writes the message to stderr and bumps the error counter
                    // so the summary reflects reality.
                    emit_error_log_from_fs_error(e.as_ref(), eval_arg.io.status_reporter.as_ref());
                })
            }
            SystemCommand::Uninstall(_) => exec_uninstall().await.inspect_err(|e| {
                emit_error_log_from_fs_error(e.as_ref(), eval_arg.io.status_reporter.as_ref());
            }),
            SystemCommand::InstallDrivers => {
                dbt_xdbc::pre_install_all_drivers().map_err(|install_err| {
                    emit_error_log_message(
                        ErrorCode::Generic,
                        format!("Failed to install drivers: {}", install_err).as_str(),
                        eval_arg.io.status_reporter.as_ref(),
                    );
                    FsError::exit_with_status(1)
                })
            }
        };
    } else if let Command::Core(Man(_)) = &cli.command {
        return execute_man_command(eval_arg).await;
    } else if let Command::Core(Login(login_args)) = &cli.command {
        return match login_args.subcommand {
            Some(LoginSubcommand::Status) => execute_login_status().await,
            None => execute_login(Arc::clone(&feature_stack.login_hooks), token).await,
        };
    } else if let Command::Core(Docs(docs_args)) = cli.command {
        return match docs_args.subcommand {
            Some(DocsSubcommand::Serve(serve_args)) => {
                run_docs_serve(
                    serve_args,
                    &feature_stack,
                    eval_arg.io.status_reporter.as_ref(),
                )
                .await
            }
            _ => {
                emit_warn_log_message(
                    ErrorCode::DocsGenerateWarning,
                    "`dbt docs generate` is not supported. Use `dbt compile --write-catalog` to write \
                    catalog.json. To host docs locally, use the dbt Core index.html with catalog.json \
                    and manifest.json in the same directory: \
                    https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/task/docs/index.html",
                    eval_arg.io.status_reporter.as_ref(),
                );
                Ok(())
            }
        };
    } else if let Command::Core(Init(init_args)) = &cli.command {
        // Handle init command
        use dbt_init::init::run_init_workflow;

        // Notify the CLI extension hooks that we're initializing a project
        feature_stack
            .cli
            .hooks
            .will_init_project(eval_arg.io.invocation_id, &cli, init_args)
            .await?;

        emit_info_progress_message(
            ProgressMessage::new_from_action_and_target(
                INSTALLING.to_string(),
                "dbt project and profile setup".to_string(),
            ),
            eval_arg.io.status_reporter.as_ref(),
        );

        let project_name = if init_args.project_name == "jaffle_shop" {
            None // Use default
        } else {
            Some(init_args.project_name.clone())
        };

        let project_template = match init_args.sample {
            ProjectTemplate::JaffleShop => init::assets::ProjectTemplateAsset::JaffleShop,
            ProjectTemplate::MomsFlowerShop => init::assets::ProjectTemplateAsset::MomsFlowerShop,
        };

        match run_init_workflow(
            project_name,
            init_args.skip_profile_setup,
            init_args.common_args.profile.clone(), // Get profile from common args
            &project_template,
        )
        .await
        {
            Ok(()) => {
                // If profile setup was not skipped, run debug to validate credentials
                if init_args.skip_profile_setup {
                    return Err(FsError::exit_with_status(0));
                }

                emit_info_log_message(format!(
                    "{} profile inputs, adapters, and connection\n", // Add empty line for spacing
                    GREEN.apply_to(VALIDATING)
                ));
            }
            Err(e) => {
                emit_error_log_from_fs_error(e.as_ref(), eval_arg.io.status_reporter.as_ref());
                let code = e.exit_status().unwrap_or(1);
                return Err(FsError::exit_with_status(code));
            }
        }
    } else if let Command::Core(Deps(deps_args)) = &cli.command {
        let command_name = feature_stack.tracing.config_provider.get_command_name();
        emit_info_progress_message(
            ProgressMessage::new_from_action_and_target(
                command_name.to_string(),
                env!("CARGO_PKG_VERSION").to_string(),
            ),
            eval_arg.io.status_reporter.as_ref(),
        );

        return match execute_deps_command(
            eval_arg,
            deps_args.common_args.get_warn_error(),
            deps_args.common_args.warn_error_options.clone(),
            Some(feature_stack.tracing.config_provider.as_ref()),
            token,
        )
        .await
        {
            Ok(()) => Ok(()),
            Err(e) if e.exit_status().is_some() => Err(e),
            Err(e) => {
                emit_error_log_from_fs_error(e.as_ref(), eval_arg.io.status_reporter.as_ref());
                Err(FsError::exit_with_status(1))
            }
        };
    } else if let Command::Core(Clean(clean_args)) = &cli.command {
        let command_name = feature_stack.tracing.config_provider.get_command_name();
        emit_info_progress_message(
            ProgressMessage::new_from_action_and_target(
                command_name.to_string(),
                env!("CARGO_PKG_VERSION").to_string(),
            ),
            eval_arg.io.status_reporter.as_ref(),
        );

        return execute_clean_command(eval_arg, &clean_args.files, token).await;
    }
    // Handle project specific commands
    let hooks_factory = Arc::clone(&feature_stack.task_runner.hooks_factory);
    execute_setup_and_all_phases(eval_arg, &cli, feature_stack, hooks_factory, token).await
}

#[allow(clippy::cognitive_complexity)]
pub async fn execute_setup_and_all_phases(
    eval_arg: &EvalArgs,
    cli: &Cli,
    feature_stack: Arc<FeatureStack>,
    task_runner_hooks_factory: Arc<dyn TaskRunnerHooksFactory>,
    token: &CancellationToken,
) -> FsResult<()> {
    emit_version_info(
        eval_arg,
        feature_stack.tracing.config_provider.get_command_name(),
    )?;

    check_options(&eval_arg.io, cli);
    if let Err(e) = validate_engine_env_vars() {
        emit_error_log_from_fs_error(e.as_ref(), eval_arg.io.status_reporter.as_ref());
        return Err(FsError::exit_with_status(1));
    }

    let mut executor = {
        let arg = Cow::Borrowed(eval_arg);
        let cli = Cow::Borrowed(cli);
        AllPhasesExecutor::new(arg, cli, feature_stack, task_runner_hooks_factory)
    };

    let result = match executor.execute_all_phases(token).await {
        Ok(()) => Ok(()),
        Err(e) if e.exit_status().is_some() => Err(e),
        Err(e) => {
            emit_error_log_from_fs_error(e.as_ref(), eval_arg.io.status_reporter.as_ref());
            Err(FsError::exit_with_status(1))
        }
    };

    // Surface an "update available" hint if the background version check
    // produced one. Shared between `dbt` and `dbt-repl` — for the REPL it
    // appears just before the prompt.
    let version_check_handle = executor.version_check_handle_mut().take();
    if let Some(handle) = version_check_handle
        && let Ok(Some(latest_version)) = handle.await
    {
        let hint = match crate::install_method::InstallMethod::detect()
            .upgrade_command(Some(&latest_version))
        {
            Some(command) => format!("{latest_version} (run `{command}`)"),
            None => {
                format!("{latest_version} (upgrade via the package manager you installed dbt with)")
            }
        };
        emit_info_progress_message(
            ProgressMessage::new_from_action_and_target("New version available".to_string(), hint),
            eval_arg.io.status_reporter.as_ref(),
        );
    }

    result
}

/// Emits version information as a progress message.
/// In debug builds, includes additional details like git hash and build time.
fn emit_version_info(eval_arg: &EvalArgs, command_name: &str) -> FsResult<()> {
    // current_exe errors when running in dbt-cloud
    // https://github.com/rust-lang/rust/issues/46090
    #[cfg(debug_assertions)]
    {
        use chrono::{DateTime, Local};
        use std::env;
        let exe_path = env::current_exe()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get current exe path: {}", e))?;
        let modified_time = stdfs::last_modified(&exe_path)?;

        // Convert SystemTime to DateTime<Local>
        let datetime: DateTime<Local> = DateTime::from(modified_time);
        let formatted_time = datetime.format("%Y-%m-%d %H:%M:%S").to_string();
        if eval_arg.from_main {
            let git_hash = git_version!(fallback = "unknown");
            let build_time = format!(
                "{} ({} {})",
                env!("CARGO_PKG_VERSION"),
                git_hash,
                formatted_time
            );
            emit_info_progress_message(
                ProgressMessage::new_from_action_and_target(command_name.to_string(), build_time),
                eval_arg.io.status_reporter.as_ref(),
            );
            return Ok(());
        };
    }

    // Show version (always shown in release builds, or in debug builds when not from_main)
    let current_version = env!("CARGO_PKG_VERSION");
    emit_info_progress_message(
        ProgressMessage::new_from_action_and_target(
            command_name.to_string(),
            current_version.to_string(),
        ),
        eval_arg.io.status_reporter.as_ref(),
    );

    Ok(())
}

struct AllPhasesExecutor<'a> {
    arg: Cow<'a, EvalArgs>,
    cli: Cow<'a, Cli>,
    feature_stack: Arc<FeatureStack>,
    start: SystemTime,
    // simple support objects
    jinja_type_checking_event_listener_factory: Arc<DefaultJinjaTypeCheckEventListenerFactory>,
    task_runner_hooks_factory: Arc<dyn TaskRunnerHooksFactory>,
    version_check_handle: Option<tokio::task::JoinHandle<Option<String>>>,
}

impl<'a> AllPhasesExecutor<'a> {
    pub fn new(
        arg: Cow<'a, EvalArgs>,
        cli: Cow<'a, Cli>,
        feature_stack: Arc<FeatureStack>,
        task_runner_hooks_factory: Arc<dyn TaskRunnerHooksFactory>,
    ) -> Self {
        let start = SystemTime::now();

        Self {
            arg,
            cli,
            feature_stack,
            start,
            jinja_type_checking_event_listener_factory: Arc::new(
                DefaultJinjaTypeCheckEventListenerFactory::default(),
            ),
            task_runner_hooks_factory,
            version_check_handle: None,
        }
    }

    fn version_check_handle_mut(&mut self) -> &mut Option<tokio::task::JoinHandle<Option<String>>> {
        &mut self.version_check_handle
    }

    pub fn prepare_for_potential_retry(
        &mut self,
    ) -> FsResult<Option<DbtCustomScheduleDescription>> {
        use CoreCommand::*;

        // Handle retry command: load retry state and create custom schedule
        if let Command::Core(Retry(retry_args)) = &self.cli.command {
            debug_assert!(matches!(self.arg.command, FsCommand::Retry));

            // Load retry state
            let retry_state = {
                let run_results_path = self
                    .arg
                    .state
                    .clone()
                    .unwrap_or_else(|| self.arg.io.out_dir.clone())
                    .join("run_results.json");
                RetryState::from_run_results(&run_results_path)
            }?;

            // Get the original command to execute
            let command_for_retry = retry_state.to_command(retry_args).map_err(|other| {
                let mut message = format!("Cannot retry command '{other}' - only ");
                message.push_str(RETRIABLE_COMMANDS.join("/").as_str());
                message += " supported";
                fs_err!(ErrorCode::InvalidArgument, "{message}")
            })?;
            let effective_sa = command_for_retry.static_analysis();

            // Emit info message when preserving non-default SA from original run
            if retry_args.static_analysis.is_none()
                && retry_state.original_static_analysis.is_some()
                && let Some(effective_sa) = effective_sa
            {
                emit_info_log_message(format!(
                    "Using static_analysis={} from original run (override with --static-analysis)",
                    effective_sa
                ));
            }

            emit_info_progress_message(
                ProgressMessage::new_from_action_and_target(
                    "Retrying".to_string(),
                    format!(
                        "{} failed nodes from previous {} command",
                        retry_state.retryable_node_ids.len(),
                        retry_state.original_command
                    ),
                ),
                self.arg.io.status_reporter.as_ref(),
            );

            // Modify command in-place eval args with the original command and effective SA
            let arg_for_retry = self.arg.to_mut();
            arg_for_retry.command = command_for_retry.as_command();
            arg_for_retry.static_analysis = effective_sa;

            // Create a custom schedule for the retryable nodes from the original run.
            // This keeps retry bounded to nodes recorded in run_results.json instead
            // of broadening the selection by expanding descendants.
            let custom_schedule = Some(DbtCustomScheduleDescription {
                unique_ids: retry_state.retryable_node_ids,
                include_parents: false,
                include_children: false,
            });

            let common_args = command_for_retry.common_args().clone();
            let cli_for_retry = Cli {
                command: Command::Core(command_for_retry),
                common_args,
            };
            self.cli = Cow::Owned(cli_for_retry);
            Ok(custom_schedule)
        } else {
            Ok(None)
        }
    }

    /// Initializes a new compilation.
    /// The resulting compilation is based on the state of the file system.
    pub async fn load_and_resolve_state(
        &mut self,
        token: &CancellationToken,
    ) -> FsResult<(
        DbtProjectCompilation,
        JinjaEnv,
        Option<DbtProjectCompilationCacheChanges>,
    )> {
        let event_emitter = self
            .feature_stack
            .as_ref()
            .instrumentation
            .event_emitter
            .as_ref();
        DbtProjectCompilation::initialize_cli(
            &self.feature_stack,
            self.arg.as_ref(),
            self.cli.as_ref(),
            Some(event_emitter),
            Arc::clone(&self.jinja_type_checking_event_listener_factory)
                as Arc<dyn JinjaTypeCheckingEventListenerFactory>,
            token,
            &mut self.version_check_handle,
        )
        .await
    }

    /// Run tasks based on the arguments.
    /// This can be called multiple times on the same compilation.
    async fn run_tasks(
        &self,
        compilation: &mut DbtProjectCompilation,
        jinja_env: JinjaEnv,
        compilation_cache_changes: Option<&DbtProjectCompilationCacheChanges>,
        schedule: Schedule<String>,
        token: &CancellationToken,
    ) -> FsResult<DbtRunTasksResult> {
        compilation
            .run_tasks(
                self.arg.as_ref(),
                self.cli.as_ref(),
                self.start,
                jinja_env,
                Arc::clone(&self.feature_stack),
                schedule,
                compilation_cache_changes,
                None,
                Arc::clone(&self.jinja_type_checking_event_listener_factory)
                    as Arc<dyn JinjaTypeCheckingEventListenerFactory>,
                self.task_runner_hooks_factory.as_ref(),
                token,
            )
            .await
    }

    /// Handle inline compile - print the compiled SQL
    async fn run_inline_compile(&self, resolved_state: &ResolverState) -> FsResult<()> {
        debug_assert!(matches!(
            &self.cli.command,
            Command::Core(CoreCommand::Compile(CompileArgs {
                inline: Some(_),
                ..
            })),
        ));
        // Find the inline model in the compiled nodes
        let inline_model = resolved_state
            .nodes
            .models
            .values()
            .find(|model| model.materialized() == DbtMaterialization::Inline)
            .ok_or_else(|| {
                fs_err!(
                    ErrorCode::Unexpected,
                    "Failed to find inline model after compilation"
                )
            })?;

        Self::emit_inline_compiled_sql(inline_model, self.arg.as_ref()).await
    }

    /// Read the compiled inline SQL from the target directory and emit a
    /// `CompiledCodeInline` telemetry event.
    async fn emit_inline_compiled_sql(
        inline_model: &Arc<DbtModel>,
        arg: &EvalArgs,
    ) -> FsResult<()> {
        let absolute_compiled_path = get_target_write_path(
            &arg.io.in_dir,
            &arg.io.out_dir.join(DBT_COMPILED_DIR_NAME),
            &inline_model.__common_attr__.package_name,
            &inline_model.__common_attr__.path,
            &inline_model.__common_attr__.original_file_path,
        );

        let compiled_sql = dbt_common::tokiofs::read_to_string(&absolute_compiled_path)
            .await
            .map_err(|_| {
                fs_err!(
                    ErrorCode::Unexpected,
                    "Failed to read compiled inline SQL at {}",
                    absolute_compiled_path.display()
                )
            })?;

        emit_info_event(CompiledCodeInline { sql: compiled_sql }, None);

        Ok(())
    }

    fn emit_selected_compile_output(
        &self,
        resolved_state: &ResolverState,
        schedule: &Schedule<String>,
        map_compiled_sql: &HashMap<String, Option<String>>,
    ) -> FsResult<()> {
        if !matches!(
            &self.cli.command,
            Command::Core(CoreCommand::Compile(CompileArgs { inline: None, .. }))
        ) || self.arg.select.is_none()
            || schedule.all_selected_nodes.len() != 1
        {
            return Ok(());
        }

        let unique_id = schedule
            .all_selected_nodes
            .iter()
            .next()
            .expect("all_selected_nodes has exactly one entry");
        if !schedule.selected_nodes.contains(unique_id) {
            return Ok(());
        }

        let Some(model) = resolved_state.nodes.models.get(unique_id) else {
            return Ok(());
        };

        let has_only_progress_show_options = !self.arg.io.show.is_empty()
            && self.arg.io.show.iter().all(|option| {
                matches!(
                    option,
                    ShowOptions::Progress
                        | ShowOptions::ProgressParse
                        | ShowOptions::ProgressHydrate
                        | ShowOptions::ProgressRender
                        | ShowOptions::ProgressAnalyze
                        | ShowOptions::ProgressRun
                        | ShowOptions::Completed
                )
            });
        if get_error_count() > 0
            || !has_only_progress_show_options
            || !model
                .__common_attr__
                .language
                .as_deref()
                .is_some_and(|language| language.eq_ignore_ascii_case("sql"))
        {
            return Ok(());
        }

        let compiled_sql = map_compiled_sql
            .get(unique_id)
            .and_then(Option::as_deref)
            .map(|s| s.to_string())
            .or_else(|| {
                let path = get_target_write_path(
                    &self.arg.io.in_dir,
                    &self.arg.io.out_dir.join(DBT_COMPILED_DIR_NAME),
                    &model.__common_attr__.package_name,
                    &model.__common_attr__.path,
                    &model.__common_attr__.original_file_path,
                );
                stdfs::read_to_string(&path).ok()
            })
            .ok_or_else(|| {
                fs_err!(
                    ErrorCode::Unexpected,
                    "Failed to find compiled SQL for {}",
                    unique_id
                )
            })?;

        let node_name = model.__common_attr__.name.clone();
        let (output_format, content) = if self.arg.format == DisplayFormat::Json {
            (
                ShowDataOutputFormat::Json,
                to_string_pretty(&json!({
                    "node": node_name,
                    "compiled": compiled_sql,
                }))?,
            )
        } else {
            (
                ShowDataOutputFormat::Text,
                format!("Compiled node '{node_name}' is:\n{compiled_sql}"),
            )
        };

        emit_info_event(
            ShowDataOutput::new_with_default_code(
                output_format,
                content,
                node_name,
                false,
                Some(unique_id.clone()),
                vec![],
            ),
            None,
        );

        Ok(())
    }

    fn should_exit_with_run_result_warning(&self) -> bool {
        let should_upgrade_warning = [
            SupportedLegacyWarnError::RunResultWarning,
            SupportedLegacyWarnError::RunResultWarningMessage,
        ]
        .into_iter()
        .any(|legacy| {
            self.arg
                .warn_error_options
                .decision_for_supported_legacy(legacy)
                == WarnErrorDecision::UpgradeToError
        });

        should_upgrade_warning
            && get_error_count() == 0
            && [
                NodeSubOutcome::TestWarned,
                NodeSubOutcome::FreshnessWarned,
                NodeSubOutcome::NodeWarned,
            ]
            .into_iter()
            .any(|sub_outcome| {
                get_metric(FusionMetricKey::OutcomeCounts(OutcomeCountsKey::new(
                    OutcomeKind::Node(NodeOutcome::Success),
                    NodeSkipReason::Unspecified,
                    Some(sub_outcome),
                ))) > 0
            })
    }

    async fn write_json(
        &self,
        run_task_results: &RunTaskResults,
        compilation: &DbtProjectCompilation,
        jinja_env: &Arc<JinjaEnv>,
        base_context: &BTreeMap<String, Value>,
        resolved_state: &ResolverState,
        adapter: &Arc<Adapter>,
    ) -> FsResult<()> {
        debug_assert!(self.arg.write_json);

        if self.arg.write_catalog && !self.arg.write_metadata {
            let metadata_adapter = adapter
                .metadata_adapter()
                .expect("Expected implements MetadataAdapter");
            let relations = metadata_adapter
                .create_relations_from_executed_nodes(resolved_state, &run_task_results.stats.run);
            let _ = write_catalog_json(
                adapter,
                resolved_state,
                relations,
                jinja_env,
                compilation.root_project_name(),
                base_context,
                self.arg.as_ref(),
                20,
            )
            .await?;
        }

        Ok(())
    }

    #[allow(clippy::cognitive_complexity)]
    pub async fn execute_all_phases(&mut self, token: &CancellationToken) -> FsResult<()> {
        use CoreCommand::*;

        let type_ops_factory = Arc::clone(&self.feature_stack.adapter.type_ops_factory);
        // Inform the user that schemas require --static-analysis strict, and CLL requires
        // --write-lineage in addition.
        if self.arg.write_metadata
            && matches!(
                self.arg.command,
                FsCommand::Compile | FsCommand::Build | FsCommand::Run
            )
            && !self
                .arg
                .static_analysis
                .is_some_and(dbt_common::static_analysis::is_strict_static_analysis)
        {
            emit_warn_log_message(
                ErrorCode::Generic,
                "--write-metadata: column schemas will not be populated without `--static-analysis strict`; add `--write-lineage` to also write column-level lineage.",
                self.arg.io.status_reporter.as_ref(),
            );
        } else if self.arg.write_metadata
            && matches!(
                self.arg.command,
                FsCommand::Compile | FsCommand::Build | FsCommand::Run
            )
            && self
                .arg
                .static_analysis
                .is_some_and(dbt_common::static_analysis::is_strict_static_analysis)
            && !self.arg.write_lineage
        {
            emit_warn_log_message(
                ErrorCode::Generic,
                "--write-metadata: add `--write-lineage` to write column-level lineage into compile/cll parquet.",
                self.arg.io.status_reporter.as_ref(),
            );
        }

        let retry_schedule = self.prepare_for_potential_retry()?;

        let (mut compilation, jinja_env, compilation_cache_changes) =
            self.load_and_resolve_state(token).await?;

        let schedule_desc = retry_schedule
            .as_ref()
            .map(DbtScheduleDescription::Custom)
            .unwrap_or(DbtScheduleDescription::Default);

        let schedule = compilation
            .create_schedule(
                self.cli.as_ref(),
                self.arg.as_ref(),
                schedule_desc,
                Default::default(),
                token,
            )
            .await?;

        // Validate show command selection before running any tasks
        if let Command::Core(Show(ShowArgs { inline: None, .. })) = &self.cli.command
            && schedule.selected_nodes.len() > 1
        {
            return Err(fs_err!(
                ErrorCode::InvalidArgument,
                "Only one node can be selected for show: {}, {}, and {} more",
                schedule.selected_nodes.iter().next().unwrap().to_string(),
                schedule.selected_nodes.iter().nth(1).unwrap().to_string(),
                schedule.selected_nodes.len() - 1
            ));
        }

        let (run_task_args, run_task_results, jinja_env, adapter, compilation_cache_state) =
            match self
                .run_tasks(
                    &mut compilation,
                    jinja_env,
                    compilation_cache_changes.as_ref(),
                    schedule.clone(),
                    token,
                )
                .await
            {
                Ok(result) => result,
                Err(err) => {
                    // The manifest represents the parsed project state and is always valid after
                    // load/resolve completes. Writing it unconditionally ensures downstream
                    // systems (codex ingestion, job.run.completed webhooks) can process failed
                    // runs regardless of failure mode — matching dbt-core behaviour.
                    // Parse already writes it during load/resolve.
                    if self.arg.write_json && self.arg.command != FsCommand::Parse {
                        // Write run_results.json with error status for all selected nodes
                        // so that `dbt retry` can pick them up after compilation failures.
                        // Only write for real errors (no exit_status); normal phase-checkpoint
                        // exits (list, format, lint, schedule, source freshness) carry an
                        // exit_status and must not produce spurious "Compilation Error" results.
                        if err.exit_status().is_none() {
                            let now = SystemTime::now();
                            let error_stats = dbt_schemas::stats::Stats {
                                stats: schedule
                                    .selected_nodes
                                    .iter()
                                    .map(|uid| dbt_common::stats::Stat {
                                        unique_id: uid.clone(),
                                        num_rows: None,
                                        rows_affected: None,
                                        start_time: now,
                                        end_time: now,
                                        status: dbt_common::stats::NodeStatus::Errored,
                                        thread_id: "main".to_string(),
                                        message: Some("Compilation Error".to_string()),
                                    })
                                    .collect(),
                                nodes: Some(compilation.nodes().clone()),
                            };
                            write_run_results_json_or_warn(&error_stats, self.arg.as_ref());
                            if self.arg.write_metadata {
                                write_runtime_results_parquet(&error_stats, self.arg.as_ref());
                            }
                        }

                        let dbt_manifest = compilation.take_dbt_manifest();
                        write_artifact_to_file(
                            &dbt_manifest,
                            ArtifactType::Manifest,
                            &self.arg.io.out_dir,
                            DBT_MANIFEST_JSON,
                            &self.arg.io.in_dir,
                        )?;
                    }
                    return Err(err);
                }
            };

        let resolved_state = Arc::clone(&run_task_results.resolved_state);

        // Write run_results.json eagerly from real stats so that it persists
        // even if post-execution steps (did_run_tasks, update_manifest,
        // save_build_cache, did_compile, etc.) fail before the late write_json() call.
        if self.arg.write_json && self.arg.command != FsCommand::Parse {
            write_run_results_json_or_warn(&run_task_results.stats.run, self.arg.as_ref());
        }
        if self.arg.write_metadata {
            write_runtime_results_parquet(&run_task_results.stats.run, self.arg.as_ref());
        }

        // When writing metadata epochs (--write-index), fetch catalog stats from the
        // warehouse for executed TABLE nodes and write to run/catalog_stats parquet.
        // This is intentionally separate from the write_json / write_catalog path:
        // --write-index sets write_json=false so write_catalog_json is never called.
        if self.arg.write_metadata && matches!(self.arg.command, FsCommand::Run | FsCommand::Build)
        {
            let metadata_adapter = adapter.metadata_adapter();
            if let Some(metadata_adapter) = metadata_adapter {
                let relations = metadata_adapter.create_relations_from_executed_nodes(
                    &resolved_state,
                    &run_task_results.stats.run,
                );
                if !relations.is_empty() {
                    let base_context = build_base_context(&resolved_state, &jinja_env);
                    match fetch_catalog_data(
                        &adapter,
                        &resolved_state,
                        relations,
                        &jinja_env,
                        compilation.root_project_name(),
                        &base_context,
                        self.arg.as_ref(),
                        20,
                    )
                    .await
                    {
                        Ok(catalog) => {
                            write_catalog_stats_parquet(&catalog, self.arg.as_ref()).await;
                        }
                        Err(e) => {
                            emit_warn_log_message(
                                ErrorCode::Generic,
                                format!("Failed to fetch catalog data for metadata epoch: {e}"),
                                self.arg.io.status_reporter.as_ref(),
                            );
                        }
                    }
                }
            }
        }

        for s in &run_task_results.storeables {
            let path = self.arg.io.out_dir.join(s.out_dir_relpath());
            let mut output = stdfs::File::create(&path)?;
            s.write_results(resolved_state.as_ref(), &mut output)?;
        }

        self.feature_stack
            .cli
            .hooks
            .did_schedule_and_run_tasks(
                self.arg.as_ref(),
                self.cli.as_ref(),
                compilation.previous_state.as_deref(),
                &run_task_results,
                resolved_state.as_ref(),
                token,
            )
            .await?;

        let mut dbt_manifest = compilation.take_dbt_manifest();
        // update_manifest clones the full ResolverState (~3GB for 6k nodes) to merge
        // compiled SQL + inferred schemas into manifest nodes. Only needed for --write-json.
        // For --write-metadata we keep the Arc and borrow — no clone.
        let resolved_state: Arc<ResolverState> = if self.arg.write_json {
            let schema_store =
                Arc::clone(&compilation_cache_state.schema_store) as Arc<dyn SchemaStoreTrait>;

            let macro_depends_on = self
                .jinja_type_checking_event_listener_factory
                .depends_on()
                .clone();

            Arc::new(update_manifest(
                &run_task_args,
                &type_ops_factory,
                &schema_store,
                resolved_state,
                &macro_depends_on,
                &mut dbt_manifest,
            )?)
        } else {
            resolved_state
        };

        // Produce parquet metadata epoch files (compile/nodes, compile/columns, cll, etc.).
        // Must happen before into_map_compiled_sql() consumes the manifest.
        if self.arg.write_metadata && self.arg.command != FsCommand::Show {
            let schema_store =
                Arc::clone(&compilation_cache_state.schema_store) as Arc<dyn SchemaStoreTrait>;

            let grain_infos = self
                .feature_stack
                .index
                .hooks
                .lineage_grain_infos(&run_task_results)
                .await?;

            let recomputed_targets: HashSet<String> = if matches!(
                self.arg.command,
                FsCommand::Compile | FsCommand::Build | FsCommand::Run
            ) {
                run_task_results
                    .stats
                    .compile
                    .stats
                    .iter()
                    .map(|s| s.unique_id.clone())
                    .collect()
            } else {
                HashSet::new()
            };

            if recomputed_targets.is_empty() {
                write_metadata_parquet(
                    self.arg.as_ref(),
                    &dbt_manifest,
                    Some(resolved_state.as_ref()),
                    Some(schema_store.as_ref()),
                    None,
                    &recomputed_targets,
                    &grain_infos,
                );
            } else if !self.arg.write_lineage {
                let empty_lineage: BTreeMap<ColId, BTreeSet<ColIdWithOp>> = BTreeMap::new();
                write_metadata_parquet(
                    self.arg.as_ref(),
                    &dbt_manifest,
                    Some(resolved_state.as_ref()),
                    Some(schema_store.as_ref()),
                    Some(&empty_lineage),
                    &recomputed_targets,
                    &grain_infos,
                );
            } else {
                match self
                    .feature_stack
                    .index
                    .hooks
                    .column_lineage(resolved_state.as_ref(), &run_task_results)
                    .await
                {
                    Ok(column_lineage) => {
                        write_metadata_parquet(
                            self.arg.as_ref(),
                            &dbt_manifest,
                            Some(resolved_state.as_ref()),
                            Some(schema_store.as_ref()),
                            Some(&column_lineage),
                            &recomputed_targets,
                            &grain_infos,
                        );
                    }
                    Err(e) => {
                        emit_warn_log_message(
                            ErrorCode::Generic,
                            format!("dbt-index: column_lineage: {e}"),
                            self.arg.io.status_reporter.as_ref(),
                        );
                        let empty_lineage: BTreeMap<ColId, BTreeSet<ColIdWithOp>> = BTreeMap::new();
                        let empty_targets: HashSet<String> = HashSet::new();
                        write_metadata_parquet(
                            self.arg.as_ref(),
                            &dbt_manifest,
                            Some(resolved_state.as_ref()),
                            Some(schema_store.as_ref()),
                            Some(&empty_lineage),
                            &empty_targets,
                            &grain_infos,
                        );
                    }
                }
            }

            // Fetch catalog and write catalog/columns epoch BEFORE write_index so the
            // index ingestion can consume it. Only for Run/Build (need executed nodes).
            if self.arg.write_catalog
                && matches!(self.arg.command, FsCommand::Run | FsCommand::Build)
            {
                let base_context = build_base_context(&resolved_state, &jinja_env);
                let metadata_adapter = adapter
                    .metadata_adapter()
                    .expect("Expected implements MetadataAdapter");
                let relations = metadata_adapter.create_relations_from_executed_nodes(
                    &resolved_state,
                    &run_task_results.stats.run,
                );
                match write_catalog_json(
                    &adapter,
                    &resolved_state,
                    relations,
                    &jinja_env,
                    compilation.root_project_name(),
                    &base_context,
                    self.arg.as_ref(),
                    20,
                )
                .await
                {
                    Ok(catalog) => {
                        write_catalog_columns_epoch(&catalog, self.arg.as_ref());
                    }
                    Err(e) => {
                        emit_warn_log_message(
                            ErrorCode::Generic,
                            format!("catalog: {e}"),
                            self.arg.io.status_reporter.as_ref(),
                        );
                    }
                }
            }

            // When --write-index is active, convert metadata epochs → snapshot index parquet.
            if self.arg.write_index {
                let metadata_dir = self.arg.metadata_dir();
                let index_dir = self.arg.index_dir();
                let mut state = IngestState::default();
                match ingest_from_metadata_direct(&metadata_dir, &index_dir, &mut state) {
                    Ok(_) => {
                        if let Err(e) = save_artifact_meta(
                            &index_dir,
                            &self.arg.io.out_dir,
                            WriteSource::DirectWrite,
                            None,
                        ) {
                            emit_warn_log_message(
                                ErrorCode::Generic,
                                format!("dbt-index: save_artifact_meta: {e}"),
                                self.arg.io.status_reporter.as_ref(),
                            );
                        }
                    }
                    Err(e) => emit_warn_log_message(
                        ErrorCode::Generic,
                        format!("dbt-index: write-index: {e}"),
                        self.arg.io.status_reporter.as_ref(),
                    ),
                }
            }
        }

        // todo: here we clone lots of stuff, but this could also be just the CAS
        let map_compiled_sql = dbt_manifest.into_map_compiled_sql();

        if self.arg.io.should_show(ShowOptions::Stats) {
            emit_info_event(
                ShowResult::new_text(
                    run_task_results.stats.compile.to_string(),
                    "stats",
                    "Compile time stats",
                ),
                None,
            );
        }

        if let Command::Core(Compile(CompileArgs {
            inline: Some(_), ..
        })) = &self.cli.command
        {
            return self.run_inline_compile(&resolved_state).await;
        }

        self.emit_selected_compile_output(&resolved_state, &schedule, &map_compiled_sql)?;

        let schema_store =
            Arc::clone(&compilation_cache_state.schema_store) as Arc<dyn SchemaStoreTrait>;
        let data_store = Arc::clone(&compilation_cache_state.data_store) as Arc<dyn DataStoreTrait>;
        self.feature_stack
            .cli
            .hooks
            .did_emit_selected_compile_output(
                self.arg.as_ref(),
                &resolved_state,
                &jinja_env,
                run_task_results.task_runner_ctx.as_ref(),
                &schema_store,
                &data_store,
                &map_compiled_sql,
                &self.feature_stack,
                token,
            )
            .await?;

        // Phase-only checkpoint at Compile: exit if the requested phase ends
        // at or before Compile, but otherwise continue regardless of the
        // current error count. (We deliberately do NOT consult the error
        // counter here — that's what `checkpoint_maybe_exit` does for phase
        // boundaries that are themselves the unit of work. Subsequent steps,
        // including runtime stats output, still need to run on test/run
        // failures.)
        if !self.arg.skip_checkpoints && self.arg.phase <= Phases::Compile {
            return Err(return_exit_code_from_error_counter());
        }

        self.feature_stack
            .cli
            .hooks
            .did_compile(
                self.arg.as_ref(),
                self.cli.as_ref(),
                &resolved_state,
                &schedule,
                token,
            )
            .await?;

        for showable in &run_task_results.showables {
            showable.show(self.arg.as_ref(), &resolved_state, &schedule, token)?;
        }

        if !self.arg.skip_checkpoints && self.arg.phase <= Phases::Lineage {
            return Err(return_exit_code_from_error_counter());
        }

        assert!(self.arg.phase == Phases::All);

        let should_exit_with_warning = self.should_exit_with_run_result_warning();

        // Write run_results.json
        if self.arg.write_json {
            let base_context = build_base_context(&resolved_state, &jinja_env);
            self.write_json(
                &run_task_results,
                &compilation,
                &jinja_env,
                &base_context,
                &resolved_state,
                &adapter,
            )
            .await?;

            if matches!(self.arg.command, FsCommand::Run | FsCommand::Build) {
                upload_artifacts_ingest_if_enabled(
                    &compilation.dbt_cloud_config().cloned(),
                    &self.arg.io,
                    self.arg.write_catalog,
                )
                .await?;
            }
        }
        if self.arg.io.should_show(ShowOptions::Stats) {
            emit_info_event(
                ShowResult::new_text(
                    run_task_results.stats.run.to_string(),
                    "stats",
                    "Runtime stats",
                ),
                None,
            );
        }

        match error_count_checkpoint() {
            Ok(()) if should_exit_with_warning => Err(FsError::exit_with_status(2)),
            result => result,
        }
    }
}

async fn run_docs_serve(
    serve_args: ClapDocsServeArgs,
    feature_stack: &Arc<FeatureStack>,
    status_reporter: Option<&Arc<dyn StatusReporter + 'static>>,
) -> FsResult<()> {
    let args = dbt_docs_server::DocsServeArgs {
        target_path: serve_args.target_path,
        host: serve_args.host,
        port: serve_args.port,
        no_open: serve_args.no_open,
    };
    let index_dir = dbt_docs_server::resolve_index_dir(&args);

    let target = args
        .target_path
        .clone()
        .unwrap_or_else(|| std::path::PathBuf::from("./target"));
    let metadata_dir = target.join("metadata");

    if let Some(md) = metadata_dir.exists().then_some(metadata_dir.as_path()) {
        if md.exists() {
            let mut state = IngestState::default();
            if let Err(err) = apply_delta_direct(md, &index_dir, &mut state) {
                emit_warn_log_message(
                    ErrorCode::Generic,
                    format!("dbt docs serve: failed to ingest metadata: {err}"),
                    None,
                );
            }
        }
    }

    let backend: Arc<dyn Backend> = Arc::new(
        DuckDbViewsBackend::open(&index_dir)
            .map_err(|e| dbt_common::fs_err!(ErrorCode::Generic, "{e}"))?,
    );
    let providers = (feature_stack.index.providers_factory)(backend);

    dbt_docs_server::run_with_args(Arc::new(args), providers)
        .await
        .map_err(|err| {
            emit_error_log_message(ErrorCode::Generic, err.to_string(), status_reporter);
            FsError::exit_with_status(1)
        })
}

#[allow(clippy::cognitive_complexity)]
pub fn check_options(io_args: &IoArgs, cli: &Cli) {
    let common_args = cli.common_args();

    if common_args.no_debug {
        emit_warn_log_message(
            ErrorCode::NotYetSupportedOption,
            "--no-debug is no longer supported",
            io_args.status_reporter.as_ref(),
        );
    }
    if common_args.cache_selected_only || common_args.no_cache_selected_only {
        emit_warn_log_message(
            ErrorCode::NoLongerSupportedOption,
            "--cache-selected is no longer supported",
            io_args.status_reporter.as_ref(),
        );
    }

    if common_args.skip_write_msgpack_if_exist {
        emit_warn_log_message(
            ErrorCode::NoLongerSupportedOption,
            "--skip-write-msgpack-if-exist is no longer supported",
            io_args.status_reporter.as_ref(),
        );
    }

    if common_args.log_cache_events || common_args.no_log_cache_events {
        emit_warn_log_message(
            ErrorCode::NoLongerSupportedOption,
            "--log-cache-events is no longer supported",
            io_args.status_reporter.as_ref(),
        );
    }
    if common_args.macro_debugging || common_args.no_macro_debugging {
        emit_warn_log_message(
            ErrorCode::NoLongerSupportedOption,
            "--macro-debugging is no longer supported",
            io_args.status_reporter.as_ref(),
        );
    }

    if common_args.partial_parse_file_diff || common_args.no_partial_parse_file_diff {
        emit_warn_log_message(
            ErrorCode::NoLongerSupportedOption,
            "--partial-parse-file-diff is no longer supported",
            io_args.status_reporter.as_ref(),
        );
    }
    if common_args.partial_parse_file_path.is_some() {
        emit_warn_log_message(
            ErrorCode::NoLongerSupportedOption,
            "--partial-parse-file-path is no longer supported",
            io_args.status_reporter.as_ref(),
        );
    }
    if common_args.populate_cache || common_args.no_populate_cache {
        emit_warn_log_message(
            ErrorCode::NoLongerSupportedOption,
            "--populate-cache is no longer supported",
            io_args.status_reporter.as_ref(),
        );
    }
    if common_args.print || common_args.no_print {
        emit_warn_log_message(
            ErrorCode::NotYetSupportedOption,
            "--print is not supported yet",
            io_args.status_reporter.as_ref(),
        );
    }
    if common_args.printer_width != 120 {
        emit_warn_log_message(
            ErrorCode::NoLongerSupportedOption,
            "--printer-width is no longer supported",
            io_args.status_reporter.as_ref(),
        );
    }
    if common_args.record_timing_info.is_some() {
        emit_warn_log_message(
            ErrorCode::NoLongerSupportedOption,
            "--record-timing-info is no longer supported",
            io_args.status_reporter.as_ref(),
        );
    }
    if common_args.static_parser || common_args.no_static_parser {
        emit_warn_log_message(
            ErrorCode::NoLongerSupportedOption,
            "--static_parser is no longer supported",
            io_args.status_reporter.as_ref(),
        );
    }
    if common_args.use_colors || common_args.no_use_colors {
        emit_warn_log_message(
            ErrorCode::NoLongerSupportedOption,
            "--use-colors is no longer supported",
            io_args.status_reporter.as_ref(),
        );
    }
    if common_args.use_colors_file || common_args.no_use_colors_file {
        emit_warn_log_message(
            ErrorCode::NoLongerSupportedOption,
            "--use-colors-file is no longer supported",
            io_args.status_reporter.as_ref(),
        );
    }
    if common_args.use_experimental_parser || common_args.no_use_experimental_parser {
        emit_warn_log_message(
            ErrorCode::NoLongerSupportedOption,
            "--use-experimental-parser is no longer supported",
            io_args.status_reporter.as_ref(),
        );
    }
    if common_args.use_fast_test_edges || common_args.no_use_fast_test_edges {
        emit_warn_log_message(
            ErrorCode::NoLongerSupportedOption,
            "--use-fast-test-edges is no longer supported",
            io_args.status_reporter.as_ref(),
        );
    }
}

/// Fetches catalog data from the warehouse without writing any artifact.
/// Returns the populated `DbtCatalog` (nodes keyed by unique_id with stats).
///
/// Extracted from `write_catalog_json` so both the JSON path and the parquet
/// metadata epoch path can share the same warehouse query logic.
#[allow(clippy::too_many_arguments)]
async fn fetch_catalog_data(
    adapter: &Arc<Adapter>,
    resolved_state: &ResolverState,
    relations: Vec<Arc<dyn BaseRelation>>,
    jinja_env: &JinjaEnv,
    project_name: &str,
    context: &BTreeMap<String, Value>,
    arg: &EvalArgs,
    batches: usize,
) -> FsResult<DbtCatalog> {
    emit_info_log_message("Preparing to write catalog.json");
    let metadata_adapter = adapter
        .metadata_adapter()
        .expect("Expected implements MetadataAdapter");
    let maybe_region = (adapter.adapter_type() == AdapterType::Bigquery).then(|| {
        adapter
            .engine()
            .config("location")
            .map_or_else(|| "us".to_string(), |cfg| cfg.to_lowercase())
    });
    // Build relations_by_schema map and task queue for worker pool
    let mut relations_by_schema = BTreeMap::new();
    let mut tasks = Vec::new();

    for rel in relations {
        let database = rel.database().unwrap_or_default();
        let schema = rel.schema().unwrap_or_default().to_string();
        let key = (database.to_string(), schema.clone());

        relations_by_schema
            .entry(key.clone())
            .or_insert_with(Vec::new)
            .push(rel);

        // Add (database, schema) task if not already present
        if !tasks.contains(&key) {
            tasks.push(key);
        }
    }

    // Create shared, thread-safe structures for worker coordination
    let task_queue = Arc::new(Mutex::new(tasks));
    let relations_map = Arc::new(relations_by_schema);

    // Shared collection for macro results
    let shared_results: Arc<Mutex<Vec<Arc<arrow::array::RecordBatch>>>> =
        Arc::new(Mutex::new(Vec::new()));

    // Shared collection for catalog errors
    let shared_errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    // Progress tracker: worker_id -> (schema_name, task_start_time)
    // Entries are updated whenever a worker picks up a task
    // Entries are removed when a worker has no remaining work and exits
    let progress_tracker: Arc<Mutex<HashMap<usize, (String, Instant)>>> =
        Arc::new(Mutex::new(HashMap::new()));

    // Timeout threshold for detecting hung workers (1 minute)
    const WORKER_TIMEOUT: Duration = Duration::from_secs(60);
    // Polling interval for checking worker progress
    const POLL_INTERVAL: Duration = Duration::from_millis(500);

    let mut node_stats_and_stuff = BTreeMap::new();
    let mut node_columns = BTreeMap::new();

    // Spawn worker pool
    let concurrency = batches.max(1); // this means max of (1 or batches)
    let mut handles = Vec::new();

    for worker_id in 0..concurrency {
        let task_queue_clone = task_queue.clone();
        let relations_map_clone = relations_map.clone();
        // Clone the JinjaEnv for each worker to create isolated execution environment
        let jinja_env_clone = jinja_env.clone();
        let adapter_type = resolved_state.adapter_type;
        let maybe_region_clone = maybe_region.clone();
        let project_name_owned = project_name.to_string();
        // Clone the base context - we'll create fresh ResultStore for each schema iteration
        let base_context = context.clone();
        // Clone shared structures for progress tracking, result collection, and error accumulation
        let shared_results_clone = shared_results.clone();
        let shared_errors_clone = shared_errors.clone();
        let progress_tracker_clone = progress_tracker.clone();

        let cur_span = Span::current();
        let handle = std::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(move || -> FsResult<()> {
                let _sp = cur_span.enter();
                // Worker loop: process tasks until queue is empty
                loop {
                    let task = task_queue_clone.lock().unwrap().pop();
                    let Some((database, schema)) = task else {
                        // No available work - remove from progress tracker and exit
                        progress_tracker_clone.lock().unwrap().remove(&worker_id);
                        break;
                    };
                    let schema_clone = schema.clone();

                    // Update progress tracker with current schema and timestamp
                    progress_tracker_clone
                        .lock()
                        .unwrap()
                        .insert(worker_id, (schema_clone.clone(), Instant::now()));

                    // CRITICAL: Create a fresh ResultStore for EACH schema iteration to ensure
                    // complete isolation. The `run_query` macro uses a hardcoded name "run_query_statement"
                    // for store_result/load_result. By creating a fresh ResultStore for each schema,
                    // we ensure that:
                    // 1. No state leaks between different schemas processed by the same worker
                    // 2. No possibility of race conditions with other workers
                    // 3. Each macro invocation has a completely clean ResultStore
                    let iteration_result_store = ResultStore::default();
                    let mut iteration_context = base_context.clone();
                    iteration_context.insert(
                        "store_result".to_owned(),
                        Value::from_function(iteration_result_store.store_result()),
                    );
                    iteration_context.insert(
                        "load_result".to_owned(),
                        Value::from_function(iteration_result_store.load_result()),
                    );
                    iteration_context.insert(
                        "store_raw_result".to_owned(),
                        Value::from_function(iteration_result_store.store_raw_result()),
                    );

                    // Lookup relations for this schema
                    let rels = relations_map_clone
                        .get(&(database.clone(), schema.clone()))
                        .expect("schema must exist in relations map");
                    let relation_as_values = rels
                        .iter()
                        .map(|r| RelationObject::new(Arc::clone(r)).into_value())
                        .collect::<Vec<Value>>();

                    let db_schema = RelationObject::new(Arc::from(
                        create_relation(
                            adapter_type,
                            database.to_string(),
                            schema.clone(),
                            maybe_region_clone.clone(), // hack for BQ
                            None,
                            ResolvedQuoting::default(),
                        )?,
                    ))
                    .into_value();

                    // To avoid blowing up the query, we use the get_catalog macro for batches with more than 50 relations
                    let jinja_result: FsResult<Value> = if relation_as_values.len() > 50 {
                        let args = vec![
                            Value::from_serialize(db_schema),
                            Value::from_serialize(vec![schema.clone()]),
                        ];
                        get_catalog_by_relations(
                            &jinja_env_clone,
                            "get_catalog",
                            &project_name_owned,
                            &project_name_owned,
                            &iteration_context,
                            &args,
                        )
                    } else {
                        let args = vec![
                            Value::from_serialize(db_schema),
                            Value::from_serialize(relation_as_values.clone()),
                        ];
                        get_catalog_by_relations(
                            &jinja_env_clone,
                            "get_catalog_relations",
                            &project_name_owned,
                            &project_name_owned,
                            &iteration_context,
                            &args,
                        )
                    };
                    match jinja_result {
                        Ok(v) => match convert_macro_result_to_record_batch(&v) {
                            Ok(record_batch) => {
                                shared_results_clone.lock().unwrap().push(record_batch);
                            }
                            Err(e) => {
                                let msg = format!("[Non-critical] Issue processing catalog for schema '{database}.{schema}': {e}");
                                emit_info_log_message(&msg);
                                shared_errors_clone.lock().unwrap().push(msg);
                            }
                        },
                        Err(e) => {
                            let msg = format!("[Non-critical] Issue fetching catalog for schema '{database}.{schema}': {e}");
                            emit_info_log_message(&msg);
                            shared_errors_clone.lock().unwrap().push(msg);
                        }
                    }
                }
                Ok(())
            })
            .expect("failed to spawn worker thread");
        handles.push(handle);
    }

    // Do this so that handles are not abandoned immediately while we poll
    let _handles = handles;

    // Do not await workers directly as they may lock due to ADBC issues
    loop {
        tokio::time::sleep(POLL_INTERVAL).await;

        let tracker_snapshot = progress_tracker.lock().unwrap().clone();

        // All workers finished normally
        if tracker_snapshot.is_empty() {
            emit_info_log_message("Fetched full catalog.json results");
            break;
        }

        let now = Instant::now();
        let all_stale = tracker_snapshot
            .iter()
            .all(|(_, (_, start_time))| now.duration_since(*start_time) > WORKER_TIMEOUT);

        if all_stale {
            emit_info_log_message("Fetched partial catalog.json results");

            // Record errors for hung workers
            for (schema, start_time) in tracker_snapshot.values() {
                let elapsed = now.duration_since(*start_time).as_secs();
                shared_errors
                    .lock()
                    .unwrap()
                    .push(format!("Timed out on schema '{schema}' after {elapsed}s"));
            }

            // Record errors for any remaining unprocessed tasks in the queue
            let remaining_tasks = std::mem::take(&mut *task_queue.lock().unwrap());
            for (database, schema) in remaining_tasks {
                shared_errors
                    .lock()
                    .unwrap()
                    .push(format!("Schema '{database}.{schema}' was never processed"));
            }

            break;
        }
    }

    // Collect results from shared structure
    let record_batches = std::mem::take(&mut *shared_results.lock().unwrap());

    for record_batch in record_batches.iter() {
        node_stats_and_stuff
            .extend(metadata_adapter.build_schemas_from_stats_sql(Arc::clone(record_batch))?);
        node_columns
            .extend(metadata_adapter.build_columns_from_get_columns(Arc::clone(record_batch))?);
    }
    let catalog_errors = std::mem::take(&mut *shared_errors.lock().unwrap());
    let mut catalog = build_catalog(
        &arg.io.invocation_id.to_string(),
        resolved_state,
        node_stats_and_stuff,
        node_columns,
    );
    if !catalog_errors.is_empty() {
        emit_info_log_message(
            "Encountered some issues building catalog.json, these did not affect job execution",
        );
        catalog.errors = Some(catalog_errors);
    }
    Ok(catalog)
}

/// Fetches catalog data from the warehouse and writes `catalog.json`.
#[allow(clippy::too_many_arguments)]
pub async fn write_catalog_json(
    adapter: &Arc<Adapter>,
    resolved_state: &ResolverState,
    relations: Vec<Arc<dyn BaseRelation>>,
    jinja_env: &JinjaEnv,
    project_name: &str,
    context: &BTreeMap<String, Value>,
    arg: &EvalArgs,
    batches: usize,
) -> FsResult<DbtCatalog> {
    let catalog = fetch_catalog_data(
        adapter,
        resolved_state,
        relations,
        jinja_env,
        project_name,
        context,
        arg,
        batches,
    )
    .await?;
    write_artifact_to_file(
        &catalog,
        ArtifactType::Catalog,
        &arg.io.out_dir,
        DBT_CATALOG_JSON,
        &arg.io.in_dir,
    )?;
    emit_info_log_message("Successfully wrote catalog.json");
    Ok(catalog)
}

fn write_catalog_columns_epoch(catalog: &DbtCatalog, arg: &EvalArgs) {
    use chrono::Utc;
    use dbt_metadata_parquet::catalog_columns::CatalogColumnRow;

    let ingested_at = Utc::now().timestamp_micros();
    let mut rows = Vec::new();

    for (unique_id, table) in catalog.nodes.iter().chain(catalog.sources.iter()) {
        for (idx, (_col_name, col)) in table.columns.iter().enumerate() {
            rows.push(CatalogColumnRow {
                unique_id: unique_id.clone(),
                column_name: col.name.clone(),
                column_index: idx as i32,
                catalog_type: Some(col.data_type.clone()),
                catalog_comment: col.comment.clone(),
                ingested_at,
            });
        }
    }

    let dir = arg.metadata_dir().join("catalog").join("columns");
    if let Err(e) =
        dbt_metadata_parquet::catalog_columns::write_catalog_columns(&dir, rows, None, None, None)
    {
        emit_warn_log_message(
            ErrorCode::Generic,
            format!("metadata: catalog_columns: {e}"),
            arg.io.status_reporter.as_ref(),
        );
    }
}
