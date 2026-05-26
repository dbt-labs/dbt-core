use std::process::{Command, Output};
use std::sync::Arc;

use dbt_agate::MappedSequence;
use dbt_common::cancellation::CancellationToken;
use dbt_common::io_args::{EvalArgs, LocalExecutionBackendKind};
use dbt_common::io_utils::StatusReporter;
use dbt_common::tracing::emit::emit_info_progress_message;
use dbt_common::{ErrorCode, FsResult, fs_err};
use dbt_compilation::core::DbtLoadedProject;
use dbt_schemas::schemas::profiles::{DbConfig, Execute};
use dbt_telemetry::ProgressMessage;
use dbt_xdbc::QueryCtx;

// Action labels for debug command progress messages (without padding - formatter handles padding)
const ACTION_DEBUGGING: &str = "Debugging";
const ACTION_DEBUGGED: &str = "Debugged";
const ACTION_SKIPPED: &str = "Skipped";

// dbt-core event codes for JSON compatibility
const DBT_CORE_DEBUG_CMD_OUT: &str = "Z047";
const DBT_CORE_DEBUG_CMD_RESULT: &str = "Z048";

struct DependencyStatus {
    installed: bool,
    detail: Option<String>,
}

/// Helper to create progress message
fn create_progress_msg(action: &str, target: &str) -> ProgressMessage {
    let dbt_core_event_code = if action == ACTION_DEBUGGED {
        DBT_CORE_DEBUG_CMD_RESULT.to_string()
    } else {
        DBT_CORE_DEBUG_CMD_OUT.to_string()
    };

    ProgressMessage::new_with_code(
        action.to_string(),
        target.to_string(),
        None,
        dbt_core_event_code,
    )
}

pub struct DebugArgs {
    pub status_reporter: Option<Arc<dyn StatusReporter>>,
    pub target: Option<String>,
    pub connection: bool,
    pub local_execution_backend: LocalExecutionBackendKind,
}

impl DebugArgs {
    pub fn from_eval_args(arg: &EvalArgs) -> Self {
        Self {
            status_reporter: arg.io.status_reporter.clone(),
            target: arg.target.clone(),
            connection: arg.connection,
            local_execution_backend: arg.local_execution_backend,
        }
    }
}

#[allow(clippy::cognitive_complexity)]
pub async fn debug(
    arg: &DebugArgs,
    loaded_project: &DbtLoadedProject,
    token: CancellationToken,
) -> FsResult<()> {
    let db_config = loaded_project.dbt_state().dbt_profile.db_config.clone();

    let mut all_debug_checks_passed = true;

    // profile info
    let profile_display = format!("profile: {}", arg.target.clone().unwrap_or_default());
    emit_info_progress_message(
        create_progress_msg(ACTION_DEBUGGING, &profile_display),
        arg.status_reporter.as_ref(),
    );

    // dbt version
    let dbt_version_display = format!("dbt version: {}", env!("CARGO_PKG_VERSION"));
    emit_info_progress_message(
        create_progress_msg(ACTION_DEBUGGING, &dbt_version_display),
        arg.status_reporter.as_ref(),
    );

    // platform info
    let platform_info_display = format!(
        "platform: {} {} ({})",
        std::env::consts::OS,
        std::env::consts::ARCH,
        std::env::consts::FAMILY
    );
    emit_info_progress_message(
        create_progress_msg(ACTION_DEBUGGING, &platform_info_display),
        arg.status_reporter.as_ref(),
    );

    let adapter_type = db_config.adapter_type();
    let execute = Execute::from_compute_flag(arg.local_execution_backend);
    let adapter_info_display = format!("adapter type: {} ({})", adapter_type, execute);
    emit_info_progress_message(
        create_progress_msg(ACTION_DEBUGGING, &adapter_info_display),
        arg.status_reporter.as_ref(),
    );

    // Skip dependency info if --connection is set
    if arg.connection {
        emit_info_progress_message(
            create_progress_msg(ACTION_SKIPPED, "steps before connection testing"),
            arg.status_reporter.as_ref(),
        );
    } else {
        // dependency info
        let dependencies = ["git"];
        let mut dependency_displays = Vec::new();
        for dep in dependencies {
            let dependency_status = dependency_installed(dep);
            let status = if dependency_status.installed {
                format!("{dep}: OK")
            } else {
                all_debug_checks_passed = false;
                let detail = dependency_status
                    .detail
                    .map(|detail| format!("\n    {}", detail.replace('\n', "\n    ")))
                    .unwrap_or_default();
                format!("{dep}: ERROR{detail}")
            };
            dependency_displays.push(status);
        }

        emit_info_progress_message(
            create_progress_msg(
                ACTION_DEBUGGING,
                &format!("dependencies:\n  {}", dependency_displays.join("\n  ")),
            ),
            arg.status_reporter.as_ref(),
        );
    }

    // Format connection details, omitting any secrets via into_connection_mapping().
    let mapping = db_config.to_connection_mapping().unwrap();
    let connection_details = serde_json::to_string_pretty(&mapping)?
        .trim_matches('{')
        .trim_matches('}')
        .trim()
        .to_string();

    emit_info_progress_message(
        create_progress_msg(
            ACTION_DEBUGGING,
            &format!("connection:\n  {}", connection_details),
        ),
        arg.status_reporter.as_ref(),
    );

    if execute == Execute::Local {
        emit_info_progress_message(
            create_progress_msg(ACTION_SKIPPED, "local connection test"),
            arg.status_reporter.as_ref(),
        );
    } else {
        let mut config_as_mapping = db_config.to_mapping().unwrap();
        // set a short timeout for the connection test to fail fast if there are issues
        config_as_mapping
            .entry("connect_timeout".into())
            .or_insert("1s".into());

        // Attempt connection using 'select 1 as id'
        let base_adapter =
            loaded_project.init_base_adapter(adapter_type, config_as_mapping, token.clone())?;

        let sql = "select 1 as id";
        let ctx = QueryCtx::default();
        base_adapter
            .execute_without_state(Some(&ctx), sql, false)
            .map_err(|e| fs_err!(ErrorCode::AuthenticationFailed, "dbt was unable to connect to the specified database.\nThe following error was returned:\n\n{}\n\nCheck your database credentials and try again. For more information, visit:\nhttps://docs.getdbt.com/docs/core/connect-data-platform/connection-profiles", e))?;

        // Check for allow_id_token parameter when using Snowflake with externalbrowser
        if let DbConfig::Snowflake(db_config_inner) = &db_config
            && db_config_inner.authenticator == Some("externalbrowser".to_string())
        {
            let sql = "SHOW PARAMETERS LIKE 'ALLOW_ID_TOKEN' IN ACCOUNT";

            let allow_token_id = match base_adapter
                .execute_without_state(Some(&ctx), sql, true)
                .map_err(|e| fs_err!(ErrorCode::AuthenticationFailed, "{}", e))
            {
                Ok((_result, agate_table)) => {
                    let columns = agate_table.columns().values();

                    if let Some(value_column) = columns.get(1) {
                        if let Ok(value) = value_column.get_item_by_index(0) {
                            let value_str = value.as_str().unwrap_or("");
                            Some(value_str.eq_ignore_ascii_case("true"))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                Err(_e) => None,
            };

            // The LSP relies on the contents of this debug line to determine whether to show a tip.
            let allow_token_id_result = match allow_token_id {
                    Some(true) => "Enabled".to_string(),
                    Some(false) => "Disabled. Consider enabling the Snowflake system parameter allow_id_token, to open fewer browser tabs during authentication. See https://docs.getdbt.com/docs/local/connect-data-platform/snowflake-setup?version=2.0#supported-authentication-types for more info.".to_string(),
                    None => "Unable to confirm. Consider enabling the Snowflake system parameter allow_id_token, to open fewer browser tabs during authentication. See https://docs.getdbt.com/docs/local/connect-data-platform/snowflake-setup?version=2.0#supported-authentication-types for more info.".to_string(),
                };

            emit_info_progress_message(
                create_progress_msg(
                    ACTION_DEBUGGING,
                    &format!(
                        "externalbrowser connection caching: {}",
                        allow_token_id_result
                    ),
                ),
                arg.status_reporter.as_ref(),
            );
        }

        emit_info_progress_message(
            create_progress_msg(ACTION_DEBUGGING, "connection test: OK"),
            arg.status_reporter.as_ref(),
        );
    }

    if all_debug_checks_passed {
        emit_info_progress_message(
            create_progress_msg(ACTION_DEBUGGED, "All checks passed!"),
            arg.status_reporter.as_ref(),
        );
    }

    Ok(())
}

fn dependency_installed(dependency: &str) -> DependencyStatus {
    match Command::new(dependency).arg("--help").output() {
        Ok(output) if output.status.success() => DependencyStatus {
            installed: true,
            detail: None,
        },
        Ok(output) => DependencyStatus {
            installed: false,
            detail: Some(format_dependency_command_failure(dependency, &output)),
        },
        Err(error) => DependencyStatus {
            installed: false,
            detail: Some(format_dependency_spawn_failure(dependency, &error)),
        },
    }
}

fn format_dependency_command_failure(dependency: &str, output: &Output) -> String {
    let output_text = String::from_utf8_lossy(if output.stderr.is_empty() {
        &output.stdout
    } else {
        &output.stderr
    })
    .trim()
    .to_string();

    let mut lines = Vec::new();
    if !output_text.is_empty() {
        lines.push(format!("Error from {dependency} --help: {output_text}"));
    } else if let Some(code) = output.status.code() {
        lines.push(format!("`{dependency} --help` exited with status code {code}."));
    } else {
        lines.push(format!("`{dependency} --help` failed."));
    }
    lines.push(dependency_help_hint(dependency));
    lines.join("\n")
}

fn format_dependency_spawn_failure(dependency: &str, error: &std::io::Error) -> String {
    format!(
        "Error from {dependency} --help: {error}\n{}",
        dependency_help_hint(dependency)
    )
}

fn dependency_help_hint(dependency: &str) -> String {
    format!(
        "Make sure that `{dependency}` is installed in your shell and that `{dependency} --help` can execute successfully."
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::process::ExitStatusExt;

    #[test]
    fn test_dependency_not_installed() {
        let result = dependency_installed("not_installed");
        assert!(!result.installed);
        let detail = result.detail.as_deref().unwrap_or_default();
        assert!(detail.contains("Make sure that `not_installed` is installed"));
    }

    #[cfg(unix)]
    #[test]
    fn test_dependency_command_failure_includes_stderr_and_hint() {
        let output = Output {
            status: std::process::ExitStatus::from_raw(256),
            stdout: Vec::new(),
            stderr: b"xcrun: error: invalid active developer path".to_vec(),
        };

        let message = format_dependency_command_failure("git", &output);

        assert!(
            message.contains("Error from git --help: xcrun: error: invalid active developer path")
        );
        assert!(message.contains("Make sure that `git` is installed in your shell"));
    }

    #[cfg(unix)]
    #[test]
    fn test_dependency_command_failure_falls_back_to_stdout() {
        let output = Output {
            status: std::process::ExitStatus::from_raw(256),
            stdout: b"git help failed".to_vec(),
            stderr: Vec::new(),
        };

        let message = format_dependency_command_failure("git", &output);

        assert!(message.contains("Error from git --help: git help failed"));
    }
}
