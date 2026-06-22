use std::io;
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

// dbt-core event codes for JSON compatibility
const DBT_CORE_DEBUG_CMD_OUT: &str = "Z047";
const DBT_CORE_DEBUG_CMD_RESULT: &str = "Z048";

#[derive(Clone, Copy)]
enum DebugAction {
    Debugging,
    Debugged,
    Skipped,
}

impl DebugAction {
    fn label(self) -> &'static str {
        match self {
            Self::Debugging => "Debugging",
            Self::Debugged => "Debugged",
            Self::Skipped => "Skipped",
        }
    }

    fn event_code(self) -> &'static str {
        match self {
            Self::Debugged => DBT_CORE_DEBUG_CMD_RESULT,
            Self::Debugging | Self::Skipped => DBT_CORE_DEBUG_CMD_OUT,
        }
    }
}

#[derive(Clone, Copy)]
struct Dependency {
    program: &'static str,
}

impl Dependency {
    const GIT: Self = Self { program: "git" };

    fn check(self) -> DependencyStatus {
        match Command::new(self.program).arg("--help").output() {
            Ok(output) if output.status.success() => DependencyStatus::Installed,
            Ok(output) => DependencyStatus::Error(self.command_failure_detail(&output)),
            Err(error) => DependencyStatus::Error(self.spawn_failure_detail(&error)),
        }
    }

    fn command_failure_detail(self, output: &Output) -> String {
        let output_text = String::from_utf8_lossy(if output.stderr.is_empty() {
            &output.stdout
        } else {
            &output.stderr
        })
        .trim()
        .to_string();

        let mut lines = Vec::new();
        if !output_text.is_empty() {
            lines.push(format!("Error from {} --help: {output_text}", self.program));
        } else if let Some(code) = output.status.code() {
            lines.push(format!(
                "`{} --help` exited with status code {code}.",
                self.program
            ));
        } else {
            lines.push(format!("`{} --help` failed.", self.program));
        }
        lines.push(self.help_hint());
        lines.join("\n")
    }

    fn spawn_failure_detail(self, error: &io::Error) -> String {
        format!(
            "Error from {} --help: {error}\n{}",
            self.program,
            self.help_hint()
        )
    }

    fn help_hint(self) -> String {
        format!(
            "Make sure that `{}` is installed in your shell and that `{} --help` can execute successfully.",
            self.program, self.program
        )
    }
}

enum DependencyStatus {
    Installed,
    Error(String),
}

impl DependencyStatus {
    fn into_display(self, dependency: Dependency) -> (bool, String) {
        match self {
            Self::Installed => (true, format!("{}: OK", dependency.program)),
            Self::Error(detail) => (
                false,
                format!(
                    "{}: ERROR\n    {}",
                    dependency.program,
                    detail.replace('\n', "\n    ")
                ),
            ),
        }
    }
}

/// Helper to create progress message
fn create_progress_msg(action: DebugAction, target: impl Into<String>) -> ProgressMessage {
    ProgressMessage::new_with_code(
        action.label().to_string(),
        target.into(),
        None,
        action.event_code().to_string(),
    )
}

fn emit_debug_progress(
    status_reporter: Option<&Arc<dyn StatusReporter>>,
    action: DebugAction,
    target: impl Into<String>,
) {
    emit_info_progress_message(create_progress_msg(action, target), status_reporter);
}

fn emit_dependency_debug_progress(status_reporter: Option<&Arc<dyn StatusReporter>>) -> bool {
    let (all_checks_passed, dependency_displays) = [Dependency::GIT].into_iter().fold(
        (true, Vec::new()),
        |(all_checks_passed, mut dependency_displays), dependency| {
            let (dependency_ok, dependency_display) = dependency.check().into_display(dependency);
            dependency_displays.push(dependency_display);
            (all_checks_passed && dependency_ok, dependency_displays)
        },
    );

    emit_debug_progress(
        status_reporter,
        DebugAction::Debugging,
        format!("dependencies:\n  {}", dependency_displays.join("\n  ")),
    );

    all_checks_passed
}

fn connection_details_display(db_config: &DbConfig) -> FsResult<String> {
    let mapping = db_config.to_connection_mapping().unwrap();
    Ok(serde_json::to_string_pretty(&mapping)?
        .trim_matches('{')
        .trim_matches('}')
        .trim()
        .to_string())
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
    emit_debug_progress(
        arg.status_reporter.as_ref(),
        DebugAction::Debugging,
        profile_display,
    );

    // dbt version
    let dbt_version_display = format!("dbt version: {}", env!("CARGO_PKG_VERSION"));
    emit_debug_progress(
        arg.status_reporter.as_ref(),
        DebugAction::Debugging,
        dbt_version_display,
    );

    // platform info
    let platform_info_display = format!(
        "platform: {} {} ({})",
        std::env::consts::OS,
        std::env::consts::ARCH,
        std::env::consts::FAMILY
    );
    emit_debug_progress(
        arg.status_reporter.as_ref(),
        DebugAction::Debugging,
        platform_info_display,
    );

    let adapter_type = db_config.adapter_type();
    let execute = Execute::from_compute_flag(arg.local_execution_backend);
    let adapter_info_display = format!("adapter type: {} ({})", adapter_type, execute);
    emit_debug_progress(
        arg.status_reporter.as_ref(),
        DebugAction::Debugging,
        adapter_info_display,
    );

    // Skip dependency info if --connection is set
    if arg.connection {
        emit_debug_progress(
            arg.status_reporter.as_ref(),
            DebugAction::Skipped,
            "steps before connection testing",
        );
    } else if !emit_dependency_debug_progress(arg.status_reporter.as_ref()) {
        all_debug_checks_passed = false;
    }

    // Format connection details, omitting any secrets via into_connection_mapping().
    emit_debug_progress(
        arg.status_reporter.as_ref(),
        DebugAction::Debugging,
        format!("connection:\n  {}", connection_details_display(&db_config)?),
    );

    if execute == Execute::Local {
        emit_debug_progress(
            arg.status_reporter.as_ref(),
            DebugAction::Skipped,
            "local connection test",
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

            emit_debug_progress(
                arg.status_reporter.as_ref(),
                DebugAction::Debugging,
                format!(
                    "externalbrowser connection caching: {}",
                    allow_token_id_result
                ),
            );
        }

        emit_debug_progress(
            arg.status_reporter.as_ref(),
            DebugAction::Debugging,
            "connection test: OK",
        );
    }

    if all_debug_checks_passed {
        emit_debug_progress(
            arg.status_reporter.as_ref(),
            DebugAction::Debugged,
            "All checks passed!",
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::process::ExitStatusExt;

    #[test]
    fn test_dependency_not_installed() {
        let dependency = Dependency {
            program: "not_installed",
        };
        let result = dependency.check();
        let detail = match result {
            DependencyStatus::Installed => panic!("expected a missing dependency error"),
            DependencyStatus::Error(detail) => detail,
        };
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

        let message = Dependency::GIT.command_failure_detail(&output);

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

        let message = Dependency::GIT.command_failure_detail(&output);

        assert!(message.contains("Error from git --help: git help failed"));
    }
}
