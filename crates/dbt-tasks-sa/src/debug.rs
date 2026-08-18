use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dbt_adbc::QueryCtx;
use dbt_agate::MappedSequence;
use dbt_common::cancellation::CancellationToken;
use dbt_common::io_args::{EvalArgs, LocalExecutionBackendKind};
use dbt_common::tracing::dbt_emit::emit_info_progress_message;
use dbt_common::{ErrorCode, FsResult, fs_err};
use dbt_compilation::core::DbtLoadedProject;
use dbt_schemas::schemas::profiles::{DbConfig, Execute};
use dbt_tasks_core::alt_catalog_attach::{AltCatalogAttachChecker, AltCatalogAttachOutcome};
use dbt_tasks_core::alt_propagation::{AltPropagationChecker, AltPropagationOutcome};
use dbt_telemetry::ProgressMessage;

// Action labels for debug command progress messages (without padding - formatter handles padding)
const ACTION_DEBUGGING: &str = "Debugging";
const ACTION_DEBUGGED: &str = "Debugged";
const ACTION_SKIPPED: &str = "Skipped";

// dbt-core event codes for JSON compatibility
const DBT_CORE_DEBUG_CMD_OUT: &str = "Z047";
const DBT_CORE_DEBUG_CMD_RESULT: &str = "Z048";

/// Renders `" (N.Ns)"` for a step's elapsed time, or nothing if it took
/// under a second -- fast steps don't need their timing called out.
fn duration_suffix(elapsed: Duration) -> String {
    if elapsed > Duration::from_secs(1) {
        format!(" ({:.1}s)", elapsed.as_secs_f64())
    } else {
        String::new()
    }
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
    pub target: Option<String>,
    pub connection: bool,
    pub local_execution_backend: LocalExecutionBackendKind,
    /// Checker for verifying alt-compute-to-native propagation, if this
    /// build has one registered. `None` means the check is skipped.
    pub alt_propagation_checker: Option<Arc<dyn AltPropagationChecker>>,
    /// Checker for verifying the catalogs declared in `catalogs.yml` are
    /// reachable from the alt compute target, if this build has one
    /// registered. `None` means the check is skipped.
    pub alt_catalog_attach_checker: Option<Arc<dyn AltCatalogAttachChecker>>,
}

impl DebugArgs {
    pub fn from_eval_args(arg: &EvalArgs) -> Self {
        Self {
            target: arg.target.clone(),
            connection: arg.connection,
            local_execution_backend: arg.local_execution_backend,
            alt_propagation_checker: None,
            alt_catalog_attach_checker: None,
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
    emit_info_progress_message(create_progress_msg(ACTION_DEBUGGING, &profile_display));

    // dbt version
    let dbt_version_display = format!("dbt version: {}", env!("CARGO_PKG_VERSION"));
    emit_info_progress_message(create_progress_msg(ACTION_DEBUGGING, &dbt_version_display));

    // platform info
    let platform_info_display = format!(
        "platform: {} {} ({})",
        std::env::consts::OS,
        std::env::consts::ARCH,
        std::env::consts::FAMILY
    );
    emit_info_progress_message(create_progress_msg(
        ACTION_DEBUGGING,
        &platform_info_display,
    ));

    let adapter_type = db_config.adapter_type();
    let execute = Execute::from_compute_flag(arg.local_execution_backend);
    let adapter_info_display = format!("adapter type: {} ({})", adapter_type, execute);
    emit_info_progress_message(create_progress_msg(ACTION_DEBUGGING, &adapter_info_display));

    // Skip dependency info if --connection is set
    if arg.connection {
        emit_info_progress_message(create_progress_msg(
            ACTION_SKIPPED,
            "steps before connection testing",
        ));
    } else {
        // dependency info
        let dependencies = ["git"];
        let mut dependency_displays = Vec::new();
        for dep in dependencies {
            let status = if dependency_installed(dep).await? {
                format!("{dep}: OK")
            } else {
                all_debug_checks_passed = false;
                format!("{dep}: ERROR")
            };
            dependency_displays.push(status);
        }

        emit_info_progress_message(create_progress_msg(
            ACTION_DEBUGGING,
            &format!("dependencies:\n  {}", dependency_displays.join("\n  ")),
        ));
    }

    // Format connection details, omitting any secrets via into_connection_mapping().
    let mapping = db_config.to_connection_mapping().unwrap();
    let connection_details = serde_json::to_string_pretty(&mapping)?
        .trim_matches('{')
        .trim_matches('}')
        .trim()
        .to_string();

    emit_info_progress_message(create_progress_msg(
        ACTION_DEBUGGING,
        &format!("connection:\n  {}", connection_details),
    ));

    // Sidecar/DuckDB mode doesn't connect to a remote warehouse; skip the connection test.
    if execute == Execute::Sidecar {
        emit_info_progress_message(create_progress_msg(ACTION_SKIPPED, "local connection test"));
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
        let connection_test_started = Instant::now();
        base_adapter
            .execute_without_state(Some(&ctx), sql, false, None)
            .map_err(|e| fs_err!(ErrorCode::AuthenticationFailed, "dbt was unable to connect to the specified database.\nThe following error was returned:\n\n{}\n\nCheck your database credentials and try again. For more information, visit:\nhttps://docs.getdbt.com/docs/core/connect-data-platform/connection-profiles", e))?;
        let connection_test_elapsed = connection_test_started.elapsed();

        // Check for allow_id_token parameter when using Snowflake with externalbrowser
        if let DbConfig::Snowflake(db_config_inner) = &db_config
            && db_config_inner.authenticator == Some("externalbrowser".to_string())
        {
            let sql = "SHOW PARAMETERS LIKE 'ALLOW_ID_TOKEN' IN ACCOUNT";

            let allow_token_id = match base_adapter
                .execute_without_state(Some(&ctx), sql, true, None)
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

            emit_info_progress_message(create_progress_msg(
                ACTION_DEBUGGING,
                &format!(
                    "externalbrowser connection caching: {}",
                    allow_token_id_result
                ),
            ));
        }

        emit_info_progress_message(create_progress_msg(
            ACTION_DEBUGGING,
            &format!(
                "connection test: OK{}",
                duration_suffix(connection_test_elapsed)
            ),
        ));
    }

    // Lake Compute (dbt-compute / MDLS) checks: only when the profile
    // declares an alt/remote compute target via `x_alt_target`. Independent
    // of whichever target is currently active/selected.
    if let Some(alt_db_config) = loaded_project
        .dbt_state()
        .dbt_profile
        .alt_target_db_config
        .clone()
    {
        debug_lake_compute(arg, &db_config, &alt_db_config, loaded_project, &token).await?;
    }

    if all_debug_checks_passed {
        emit_info_progress_message(create_progress_msg(ACTION_DEBUGGED, "All checks passed!"));
    }

    Ok(())
}

/// Runs the Lake Compute connectivity checks: dbt-compute auth, MDLS
/// write/read-back, and (if a checker is registered and the project
/// declares a catalog-linked database) native-connection propagation.
async fn debug_lake_compute(
    arg: &DebugArgs,
    native_db_config: &DbConfig,
    alt_db_config: &DbConfig,
    loaded_project: &DbtLoadedProject,
    token: &CancellationToken,
) -> FsResult<()> {
    let mut alt_mapping = alt_db_config.to_mapping().unwrap();
    alt_mapping
        .entry("connect_timeout".into())
        .or_insert("1s".into());

    let alt_adapter_type = alt_db_config.adapter_type();
    let alt_adapter =
        loaded_project.init_base_adapter(alt_adapter_type, alt_mapping, token.clone())?;
    let ctx = QueryCtx::default();

    // 1. dbt-compute auth: a cheap round trip through the alt connection.
    let alt_connection_started = Instant::now();
    alt_adapter
        .execute_without_state(Some(&ctx), "select 1 as id", false, None)
        .map_err(|e| {
            fs_err!(
                ErrorCode::AuthenticationFailed,
                "dbt was unable to connect to dbt Compute using the configured `x_alt_target`.\n\
                 The following error was returned:\n\n{}",
                e
            )
        })?;
    emit_info_progress_message(create_progress_msg(
        ACTION_DEBUGGING,
        &format!(
            "dbt Compute connection test: OK{}",
            duration_suffix(alt_connection_started.elapsed())
        ),
    ));

    // 2. Declared-catalog attach. Runs before the write tests below because it
    // is the cheapest check that can fail on a misconfigured catalog, and a
    // catalog that cannot be attached makes everything after it moot.
    match &arg.alt_catalog_attach_checker {
        None => {
            emit_info_progress_message(create_progress_msg(
                ACTION_SKIPPED,
                "catalog attach test (unavailable in this build)",
            ));
        }
        Some(checker) => {
            let attach_started = Instant::now();
            let outcome = checker
                .check_catalog_attach(native_db_config, alt_db_config, token.clone())
                .await?;
            emit_info_progress_message(create_progress_msg(
                ACTION_DEBUGGING,
                &format!(
                    "{}{}",
                    format_catalog_attach_outcome(&outcome),
                    duration_suffix(attach_started.elapsed())
                ),
            ));
        }
    }

    // 3. MDLS write + read-back, in an already-authorized namespace (the alt
    // target's configured database/schema). Namespace-level DDL is
    // deliberately avoided: creating a new namespace is denied for the
    // Polaris principal used here and (confirmed empirically) can hang
    // rather than fail fast, so this only exercises table create/drop
    // within a namespace that must already exist.
    let probe_table = format!(
        "__dbt_debug_probe_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos()
    );
    let qualified_probe = qualify_probe_name(
        alt_db_config.get_database().map(String::as_str),
        alt_db_config.get_schema().map(String::as_str),
        &probe_table,
    );

    let write_started = Instant::now();
    let write_result = alt_adapter.execute_without_state(
        Some(&ctx),
        &format!("create table {qualified_probe} (id integer)"),
        false,
        None,
    );
    let write_elapsed = write_started.elapsed();
    let write_ok = write_result.is_ok();
    if let Err(e) = write_result {
        // Clean up is unnecessary: the create failed, so there's nothing to drop.
        return Err(fs_err!(
            ErrorCode::AuthenticationFailed,
            "dbt was able to authenticate to dbt Compute, but failed to write to MDLS.\n\
             The following error was returned:\n\n{}\n\n\
             This commonly means the configured schema/namespace does not exist yet: dbt debug \
             does not create one, since new-namespace creation is not supported for this \
             credential. Check that the configured schema/namespace already exists and that the \
             credentials are authorized to write to it.",
            e
        ));
    }
    emit_info_progress_message(create_progress_msg(
        ACTION_DEBUGGING,
        &format!("MDLS write test: OK{}", duration_suffix(write_elapsed)),
    ));

    let read_started = Instant::now();
    let read_result = alt_adapter.execute_without_state(
        Some(&ctx),
        &format!("select count(*) as c from {qualified_probe}"),
        true,
        None,
    );
    let read_elapsed = read_started.elapsed();

    // Cleanup is unconditional and best-effort: don't let a drop failure mask
    // the read-back result, but don't leave the probe table behind either.
    let _ = alt_adapter.execute_without_state(
        Some(&ctx),
        &format!("drop table if exists {qualified_probe}"),
        false,
        None,
    );

    if write_ok {
        read_result.map_err(|e| {
            fs_err!(
                ErrorCode::AuthenticationFailed,
                "dbt was able to write to MDLS, but failed to read the object back.\n\
                 The following error was returned:\n\n{}",
                e
            )
        })?;
    }
    emit_info_progress_message(create_progress_msg(
        ACTION_DEBUGGING,
        &format!("MDLS read-back test: OK{}", duration_suffix(read_elapsed)),
    ));

    // 4. Snowflake propagation / catalog-linking: only if the project
    // declares a catalog-linked database, and a checker is registered.
    let linked_database = loaded_project
        .dbt_state()
        .catalogs
        .as_ref()
        .and_then(|catalogs| catalogs.iceberg_rest_catalog_databases().ok())
        .and_then(|dbs| dbs.into_iter().next())
        .map(|(_, db)| db);

    match (&linked_database, &arg.alt_propagation_checker) {
        (None, _) => {
            emit_info_progress_message(create_progress_msg(
                ACTION_SKIPPED,
                "Snowflake propagation test (no catalog-linked database configured)",
            ));
        }
        (Some(_), None) => {
            emit_info_progress_message(create_progress_msg(
                ACTION_SKIPPED,
                "Snowflake propagation test (unavailable in this build)",
            ));
        }
        (Some(linked_database), Some(checker)) => {
            emit_info_progress_message(create_progress_msg(
                ACTION_DEBUGGING,
                "Snowflake propagation test (this mints a short-lived credential and waits \
                 for dbt Compute to confirm the write is visible in Snowflake; can take up \
                 to a minute)...",
            ));
            let propagation_started = Instant::now();
            let outcome = checker
                .check_alt_propagation(
                    native_db_config,
                    alt_db_config,
                    linked_database,
                    token.clone(),
                )
                .await?;
            let propagation_elapsed = propagation_started.elapsed();
            emit_info_progress_message(create_progress_msg(
                ACTION_DEBUGGING,
                &format!(
                    "{}{}",
                    format_propagation_outcome(&outcome),
                    duration_suffix(propagation_elapsed)
                ),
            ));
        }
    }

    Ok(())
}

/// Builds a schema/database-qualified name for the MDLS probe object from
/// the alt target's own configured database/schema.
fn qualify_probe_name(database: Option<&str>, schema: Option<&str>, table: &str) -> String {
    match (database, schema) {
        (Some(db), Some(schema)) => format!("{db}.{schema}.{table}"),
        (None, Some(schema)) => format!("{schema}.{table}"),
        _ => table.to_string(),
    }
}

/// Renders an [`AltCatalogAttachOutcome`] as the `dbt debug` progress line.
/// A failed attach never reaches here -- it comes back as an error, since a
/// catalog that cannot be attached is a setup problem the user must fix.
fn format_catalog_attach_outcome(outcome: &AltCatalogAttachOutcome) -> String {
    match outcome {
        AltCatalogAttachOutcome::NothingToCheck => {
            "catalog attach test: skipped (no declared catalogs to check)".to_string()
        }
        AltCatalogAttachOutcome::Attached { catalogs } => {
            format!("catalog attach test: OK ({})", catalogs.join(", "))
        }
    }
}

/// Renders an [`AltPropagationOutcome`] as the `dbt debug` progress line.
/// `NotYetVisible` is reported informationally, not as a failure, since
/// catalog-integration propagation is inherently asynchronous.
fn format_propagation_outcome(outcome: &AltPropagationOutcome) -> String {
    match outcome {
        AltPropagationOutcome::Verified => "Snowflake propagation test: OK".to_string(),
        AltPropagationOutcome::NotYetVisible {
            waited_secs,
            configured_refresh_secs,
        } => {
            let refresh_note = configured_refresh_secs
                .map(|s| {
                    format!(" your catalog integration's refresh interval is configured at {s}s;")
                })
                .unwrap_or_default();
            format!(
                "Snowflake propagation test: not yet visible after {waited_secs}s.{refresh_note} this may just need more time, not necessarily a failure."
            )
        }
    }
}

async fn dependency_installed(dependency: &str) -> FsResult<bool> {
    Ok(Command::new(dependency)
        .arg("--help")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_dependency_not_installed() {
        let result = dependency_installed("not_installed").await.unwrap();
        assert!(!result);
    }

    #[test]
    fn duration_suffix_hides_fast_steps() {
        assert_eq!(duration_suffix(Duration::from_millis(500)), "");
        assert_eq!(duration_suffix(Duration::from_secs(1)), "");
    }

    #[test]
    fn duration_suffix_shows_slow_steps() {
        assert_eq!(duration_suffix(Duration::from_millis(1500)), " (1.5s)");
        assert_eq!(duration_suffix(Duration::from_secs(90)), " (90.0s)");
    }

    #[test]
    fn qualify_probe_name_with_database_and_schema() {
        assert_eq!(
            qualify_probe_name(Some("db"), Some("schema"), "t"),
            "db.schema.t"
        );
    }

    #[test]
    fn qualify_probe_name_with_schema_only() {
        assert_eq!(qualify_probe_name(None, Some("schema"), "t"), "schema.t");
    }

    #[test]
    fn qualify_probe_name_with_neither() {
        assert_eq!(qualify_probe_name(None, None, "t"), "t");
    }

    #[test]
    fn format_catalog_attach_outcome_lists_checked_catalogs() {
        let msg = format_catalog_attach_outcome(&AltCatalogAttachOutcome::Attached {
            catalogs: vec!["mdls_horizon".to_string(), "native_db".to_string()],
        });
        assert_eq!(msg, "catalog attach test: OK (mdls_horizon, native_db)");
    }

    #[test]
    fn format_catalog_attach_outcome_nothing_to_check() {
        let msg = format_catalog_attach_outcome(&AltCatalogAttachOutcome::NothingToCheck);
        assert!(msg.contains("no declared catalogs to check"));
    }

    #[test]
    fn format_propagation_outcome_verified() {
        assert_eq!(
            format_propagation_outcome(&AltPropagationOutcome::Verified),
            "Snowflake propagation test: OK"
        );
    }

    #[test]
    fn format_propagation_outcome_not_yet_visible_with_refresh_interval() {
        let msg = format_propagation_outcome(&AltPropagationOutcome::NotYetVisible {
            waited_secs: 90,
            configured_refresh_secs: Some(3600),
        });
        assert!(msg.contains("not yet visible after 90s"));
        assert!(msg.contains("refresh interval is configured at 3600s"));
        assert!(msg.contains("not necessarily a failure"));
    }

    #[test]
    fn format_propagation_outcome_not_yet_visible_without_refresh_interval() {
        let msg = format_propagation_outcome(&AltPropagationOutcome::NotYetVisible {
            waited_secs: 90,
            configured_refresh_secs: None,
        });
        assert!(msg.contains("not yet visible after 90s"));
        assert!(!msg.contains("refresh interval"));
    }
}
