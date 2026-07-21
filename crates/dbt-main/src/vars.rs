use dbt_common::{io_utils::StatusReporter, tracing::dbt_emit::emit_warn_log_message};
use dbt_init::{ErrorCode, FsResult, fs_err};
use std::sync::Arc;

const ENGINE_ENV_PREFIX: &str = "DBT_ENGINE_";

/// Environment variables from dbt-clap-core that can be aliased with DBT_ENGINE_ prefix.
/// For each entry, if DBT_ENGINE_<SUFFIX> is set and DBT_<SUFFIX> is not,
/// we copy the value to DBT_<SUFFIX> before CLI parsing.
const ALIASABLE_ENV_VARS: &[&str] = &[
    "DBT_BETA_USE_QUERY_CACHE",
    "DBT_BUILD_CACHE_CAS_URL",
    "DBT_BUILD_CACHE_MODE",
    "DBT_BUILD_CACHE_NODES_URL",
    "DBT_CACHE_ALL_SCHEMAS",
    "DBT_CACHE_SELECTED_ONLY",
    "DBT_COMPUTE",
    "DBT_DEBUG",
    "DBT_DEFER",
    "DBT_DEFER_STATE",
    "DBT_DISABLE_VERSION_CHECK",
    "DBT_EVENT_TIME_END",
    "DBT_EVENT_TIME_START",
    "DBT_EXPORT_SAVED_QUERIES",
    "DBT_EXPORT_TO_OTLP",
    "DBT_FAIL_FAST",
    "DBT_FAVOR_STATE",
    "DBT_FS_INTERNAL_PACKAGES_INSTALL_PATH",
    "DBT_FULL_REFRESH",
    "DBT_INDIRECT_SELECTION",
    "DBT_INTERNAL_PACKAGE_MODE",
    "DBT_INTROSPECT",
    "DBT_INVOCATION_ID",
    "DBT_LOG_CACHE_EVENTS",
    "DBT_LOG_FILE_MAX_BYTES",
    "DBT_LOG_FORMAT",
    "DBT_LOG_FORMAT_FILE",
    "DBT_LOG_LEVEL",
    "DBT_LOG_LEVEL_FILE",
    "DBT_LOG_PATH",
    "DBT_MACRO_DEBUGGING",
    "DBT_MAXIMUM_SEED_SIZE_MIB",
    "DBT_NO_FAIL_FAST",
    "DBT_NO_FAVOR_STATE",
    "DBT_NO_LOG_CACHE_EVENTS",
    "DBT_NO_PARALLEL",
    "DBT_OTEL_FILE_NAME",
    "DBT_OTEL_PARQUET_FILE_NAME",
    "DBT_PACKAGES_INSTALL_PATH",
    "DBT_PARENT_SPAN_ID",
    "DBT_PARTIAL_PARSE",
    "DBT_PARTIAL_PARSE_FILE_DIFF",
    "DBT_PARTIAL_PARSE_FILE_PATH",
    "DBT_POPULATE_CACHE",
    "DBT_PRINT",
    "DBT_PRINTER_WIDTH",
    "DBT_PROFILE",
    "DBT_PROFILES_DIR",
    "DBT_PROJECT_DIR",
    "DBT_QUIET",
    "DBT_SAMPLE",
    "DBT_SEND_ANONYMOUS_USAGE_STATS",
    "DBT_SHOW_ALL_DEPRECATIONS",
    "DBT_SKIP_SEMANTIC_MANIFEST_VALIDATION",
    "DBT_STATE",
    "DBT_STATIC_ANALYSIS",
    "DBT_STATIC_PARSER",
    "DBT_STORE_FAILURES",
    "DBT_TARGET",
    "DBT_TARGET_PATH",
    "DBT_TASK_CACHE_URL",
    "DBT_TIME_MACHINE_MODE",
    "DBT_TIME_MACHINE_ORDERING",
    "DBT_TIME_MACHINE_PATH",
    "DBT_USE_COLORS",
    "DBT_USE_COLORS_FILE",
    "DBT_USE_EXPERIMENTAL_PARSER",
    "DBT_USE_FAST_TEST_EDGES",
    "DBT_VERSION_CHECK",
    "DBT_WARN_ERROR",
    "DBT_WARN_ERROR_OPTIONS",
    "DBT_WRITE_CATALOG",
    "DBT_WRITE_JSON",
];

/// Environment variables that are recognized by dbt-core but NOT supported by fusion.
/// When these are set, we issue a warning that they will have no effect.
const KNOWN_UNUSED_ENGINE_ENV_VARS: &[&str] = &[
    "DBT_ENGINE_ARTIFACT_STATE_PATH",
    "DBT_ENGINE_CLEAN_PROJECT_FILES_ONLY",
    "DBT_ENGINE_DEFER_TO_STATE",
    "DBT_ENGINE_DOWNLOAD_DIR",
    "DBT_ENGINE_EMPTY",
    "DBT_ENGINE_EXCLUDE_RESOURCE_TYPES",
    "DBT_ENGINE_FAVOR_STATE_MODE",
    "DBT_ENGINE_HOST",
    "DBT_ENGINE_INCLUDE_SAVED_QUERY",
    "DBT_ENGINE_INVOCATION_ENV",
    "DBT_ENGINE_NO_PRINT",
    "DBT_ENGINE_PACKAGE_HUB_URL",
    "DBT_ENGINE_PP_FILE_DIFF_TEST",
    "DBT_ENGINE_PP_TEST",
    "DBT_ENGINE_RECORDED_FILE_PATH",
    "DBT_ENGINE_RESOURCE_TYPES",
    "DBT_ENGINE_SHOW_RESOURCE_REPORT",
    "DBT_ENGINE_SQLPARSE",
    "DBT_ENGINE_TEST_STATE_MODIFIED",
    "DBT_ENGINE_UPLOAD_TO_ARTIFACTS_INGEST_API",
    "DBT_ENGINE_USE_EXPERIMENTAL_JOB_HEALTH_MONITOR",
    "DBT_ENGINE_USE_EXPERIMENTAL_SKIP_NODES_SYNCHRONOUSLY",
    "DBT_ENGINE_USE_V2_PARSER",
    "DBT_ENGINE_VORTEX_EVENT_FORWARDING_ENABLED",
    "DBT_ENGINE_WRITE_SQL_QUERY_DATA",
];

/// Engine-specific environment variables that ARE used by fusion.
/// These are NOT aliases of DBT_* vars - they are unique to the engine.
const USED_ENGINE_ENV_VARS: &[&str] = &[
    "DBT_ENGINE_BETA_PACKAGE_PARSING",
    "DBT_ENGINE_BETA_PARSING",
    "DBT_ENGINE_EXPERIMENTAL_LIST_UDFS",
    "DBT_ENGINE_EXPERIMENTAL_SNAPSHOT_COLUMNS",
    "DBT_ENGINE_MANAGE_STATE",
    "DBT_ENGINE_NO_WARN_SEMANTIC_MANIFEST_VALIDATION",
    "DBT_ENGINE_RECORDER_FILE_PATH",
    "DBT_ENGINE_RECORDER_MODE",
    "DBT_ENGINE_RECORDER_ROW_LIMIT",
    "DBT_ENGINE_RECORDER_TYPES",
    "DBT_ENGINE_STATE_API_URL",
    "DBT_ENGINE_STATE_AUTH_URL",
    "DBT_ENGINE_STATE_EMIT_REUSED_STATUS",
    "DBT_ENGINE_STATE_OAUTH_CLIENT_ID",
    "DBT_ENGINE_STATE_TOKEN_URL",
];

// The full set of known DBT_ENGINE_* env vars is constructed from:
// 1. USED_ENGINE_ENV_VARS - engine-specific vars used by fusion
// 2. KNOWN_UNUSED_ENGINE_ENV_VARS - dbt-core vars not supported by fusion
// 3. Aliases derived from ALIASABLE_ENV_VARS (DBT_* -> DBT_ENGINE_*)
static KNOWN_ENGINE_ENV_VARS: std::sync::LazyLock<std::collections::HashSet<String>> =
    std::sync::LazyLock::new(|| {
        let mut set = std::collections::HashSet::new();

        // Add used engine-specific vars
        for var in USED_ENGINE_ENV_VARS {
            set.insert((*var).to_string());
        }

        // Add unused engine-specific vars (for backwards compatibility)
        for var in KNOWN_UNUSED_ENGINE_ENV_VARS {
            set.insert((*var).to_string());
        }

        // Add aliased vars: DBT_<SUFFIX> -> DBT_ENGINE_<SUFFIX>
        for dbt_var in ALIASABLE_ENV_VARS {
            if let Some(suffix) = dbt_var.strip_prefix("DBT_") {
                set.insert(format!("DBT_ENGINE_{}", suffix));
            }
        }

        set
    });

/// Applies DBT_ENGINE_* environment variable aliases.
///
/// For each variable in `ALIASABLE_ENV_VARS`, if `DBT_ENGINE_<SUFFIX>` is set
/// and `DBT_<SUFFIX>` is not, copies the value to `DBT_<SUFFIX>`.
///
/// This allows users to use `DBT_ENGINE_FAIL_FAST=true` instead of `DBT_FAIL_FAST=true`,
/// which is useful in environments where the `DBT_` prefix conflicts with dbt-core.
///
/// # Safety
///
/// This function modifies the process environment. It must be called before
/// spawning any threads and before CLI parsing.
pub fn apply_engine_env_var_aliases() {
    for dbt_var in ALIASABLE_ENV_VARS {
        // Extract the suffix after "DBT_"
        let Some(suffix) = dbt_var.strip_prefix("DBT_") else {
            continue;
        };

        let engine_var = format!("DBT_ENGINE_{}", suffix);

        // Only set DBT_* if DBT_ENGINE_* is set and DBT_* is not
        if std::env::var(dbt_var).is_err() {
            if let Ok(value) = std::env::var(&engine_var) {
                // SAFETY: Called before any threads are spawned
                #[allow(clippy::disallowed_methods)]
                unsafe {
                    std::env::set_var(dbt_var, value);
                }
            }
        }
    }
}

/// Warns about environment variables that are recognized but not supported by fusion.
///
/// These are typically dbt-core specific variables that have no effect in fusion.
/// Returns a list of the unused variables that were set (for testing purposes).
pub fn warn_unused_engine_env_vars(
    status_reporter: Option<&Arc<dyn StatusReporter + 'static>>,
) -> Vec<String> {
    let unused: Vec<String> = std::env::vars()
        .map(|(k, _)| k)
        .filter(|k| KNOWN_UNUSED_ENGINE_ENV_VARS.contains(&k.as_str()))
        .collect();

    for var in &unused {
        emit_warn_log_message(
            ErrorCode::UnsupportedFusionFeature,
            format!("{var} is not supported by fusion and will have no effect."),
            status_reporter,
        );
    }

    unused
}

/// Validates that no unknown environment variables use the reserved `DBT_ENGINE_` prefix.
///
/// The `DBT_ENGINE_` prefix is reserved for dbt engine use. User-authored environment
/// variables must not use this prefix to avoid conflicts with current or future
/// engine-defined variables.
pub fn validate_engine_env_vars() -> FsResult<()> {
    let unknown: Vec<String> = std::env::vars()
        .map(|(k, _)| k)
        .filter(|k| k.starts_with(ENGINE_ENV_PREFIX) && !KNOWN_ENGINE_ENV_VARS.contains(k))
        .collect();

    if unknown.is_empty() {
        return Ok(());
    }

    Err(fs_err!(
        ErrorCode::InvalidConfig,
        "The `{}` prefix is reserved for the dbt engine. \
         The following environment variable(s) use this reserved prefix: {}. \
         Please rename them to avoid conflicts.",
        ENGINE_ENV_PREFIX,
        unknown.join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    #[test]
    fn validate_engine_env_vars_rejects_unknown() {
        let _lock = ENV_MUTEX.lock().unwrap();
        unsafe {
            #[allow(clippy::disallowed_methods)]
            std::env::set_var("DBT_ENGINE_MY_CUSTOM_VAR", "1");
        }
        let result = validate_engine_env_vars();
        unsafe {
            #[allow(clippy::disallowed_methods)]
            std::env::remove_var("DBT_ENGINE_MY_CUSTOM_VAR");
        }
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("DBT_ENGINE_MY_CUSTOM_VAR"),
            "error should mention the offending var: {}",
            err
        );
    }

    #[test]
    fn validate_engine_env_vars_allows_known() {
        let _lock = ENV_MUTEX.lock().unwrap();
        unsafe {
            #[allow(clippy::disallowed_methods)]
            std::env::set_var("DBT_ENGINE_BETA_PARSING", "1");
        }
        let result = validate_engine_env_vars();
        unsafe {
            #[allow(clippy::disallowed_methods)]
            std::env::remove_var("DBT_ENGINE_BETA_PARSING");
        }
        assert!(result.is_ok(), "known engine env var should not error");
    }

    #[test]
    fn validate_engine_env_vars_allows_dbt_state_vars() {
        let _lock = ENV_MUTEX.lock().unwrap();
        unsafe {
            #[allow(clippy::disallowed_methods)]
            std::env::set_var("DBT_ENGINE_MANAGE_STATE", "1");
            #[allow(clippy::disallowed_methods)]
            std::env::set_var("DBT_ENGINE_STATE_OAUTH_CLIENT_ID", "client-id");
        }
        let result = validate_engine_env_vars();
        unsafe {
            #[allow(clippy::disallowed_methods)]
            std::env::remove_var("DBT_ENGINE_MANAGE_STATE");
            #[allow(clippy::disallowed_methods)]
            std::env::remove_var("DBT_ENGINE_STATE_OAUTH_CLIENT_ID");
        }
        assert!(result.is_ok(), "dbt State engine env vars should not error");
    }

    #[test]
    fn apply_engine_env_var_aliases_sets_dbt_var_from_engine_var() {
        let _lock = ENV_MUTEX.lock().unwrap();
        // Clean up any existing vars first
        unsafe {
            #[allow(clippy::disallowed_methods)]
            std::env::remove_var("DBT_FAIL_FAST");
            #[allow(clippy::disallowed_methods)]
            std::env::remove_var("DBT_ENGINE_FAIL_FAST");
        }

        // Set the DBT_ENGINE_ variant
        unsafe {
            #[allow(clippy::disallowed_methods)]
            std::env::set_var("DBT_ENGINE_FAIL_FAST", "true");
        }

        // Apply aliases
        apply_engine_env_var_aliases();

        // Verify DBT_FAIL_FAST is now set
        let result = std::env::var("DBT_FAIL_FAST");
        assert_eq!(result.ok(), Some("true".to_string()));

        // Clean up
        unsafe {
            #[allow(clippy::disallowed_methods)]
            std::env::remove_var("DBT_FAIL_FAST");
            #[allow(clippy::disallowed_methods)]
            std::env::remove_var("DBT_ENGINE_FAIL_FAST");
        }
    }

    #[test]
    fn apply_engine_env_var_aliases_does_not_override_existing() {
        let _lock = ENV_MUTEX.lock().unwrap();
        // Clean up any existing vars first
        unsafe {
            #[allow(clippy::disallowed_methods)]
            std::env::remove_var("DBT_QUIET");
            #[allow(clippy::disallowed_methods)]
            std::env::remove_var("DBT_ENGINE_QUIET");
        }

        // Set both variants - DBT_ should take precedence
        unsafe {
            #[allow(clippy::disallowed_methods)]
            std::env::set_var("DBT_QUIET", "original");
            #[allow(clippy::disallowed_methods)]
            std::env::set_var("DBT_ENGINE_QUIET", "should_not_override");
        }

        // Apply aliases
        apply_engine_env_var_aliases();

        // Verify DBT_QUIET retains original value
        let result = std::env::var("DBT_QUIET");
        assert_eq!(result.ok(), Some("original".to_string()));

        // Clean up
        unsafe {
            #[allow(clippy::disallowed_methods)]
            std::env::remove_var("DBT_QUIET");
            #[allow(clippy::disallowed_methods)]
            std::env::remove_var("DBT_ENGINE_QUIET");
        }
    }

    #[test]
    fn warn_unused_engine_env_vars_detects_unused() {
        let _lock = ENV_MUTEX.lock().unwrap();
        // Clean up first
        unsafe {
            #[allow(clippy::disallowed_methods)]
            std::env::remove_var("DBT_ENGINE_SQLPARSE");
        }

        // Set an unused engine env var
        unsafe {
            #[allow(clippy::disallowed_methods)]
            std::env::set_var("DBT_ENGINE_SQLPARSE", "true");
        }

        // Check that the warning function detects it
        let unused = warn_unused_engine_env_vars(None);
        assert!(
            unused.contains(&"DBT_ENGINE_SQLPARSE".to_string()),
            "should detect DBT_ENGINE_SQLPARSE as unused"
        );

        // Clean up
        unsafe {
            #[allow(clippy::disallowed_methods)]
            std::env::remove_var("DBT_ENGINE_SQLPARSE");
        }
    }

    #[test]
    fn use_v2_parser_is_a_recognized_no_op() {
        let _lock = ENV_MUTEX.lock().unwrap();
        // Clean up first
        unsafe {
            #[allow(clippy::disallowed_methods)]
            std::env::remove_var("DBT_ENGINE_USE_V2_PARSER");
        }

        // Set the no-op engine env var
        unsafe {
            #[allow(clippy::disallowed_methods)]
            std::env::set_var("DBT_ENGINE_USE_V2_PARSER", "1");
        }

        // It must be recognized (not rejected as an unknown reserved-prefix var)...
        let validate = validate_engine_env_vars();
        // ...and reported as unused (it is a no-op in fusion).
        let unused = warn_unused_engine_env_vars(None);

        // Clean up before asserting so a failure doesn't leak the var
        unsafe {
            #[allow(clippy::disallowed_methods)]
            std::env::remove_var("DBT_ENGINE_USE_V2_PARSER");
        }

        assert!(
            validate.is_ok(),
            "DBT_ENGINE_USE_V2_PARSER should be a recognized engine env var"
        );
        assert!(
            unused.contains(&"DBT_ENGINE_USE_V2_PARSER".to_string()),
            "DBT_ENGINE_USE_V2_PARSER should be treated as a no-op (unused) var"
        );
    }

    #[test]
    fn warn_unused_engine_env_vars_ignores_supported() {
        let _lock = ENV_MUTEX.lock().unwrap();
        // Clean up first
        unsafe {
            #[allow(clippy::disallowed_methods)]
            std::env::remove_var("DBT_ENGINE_FAIL_FAST");
        }

        // Set a supported (aliased) engine env var
        unsafe {
            #[allow(clippy::disallowed_methods)]
            std::env::set_var("DBT_ENGINE_FAIL_FAST", "true");
        }

        // Check that the warning function does NOT include it
        let unused = warn_unused_engine_env_vars(None);
        assert!(
            !unused.contains(&"DBT_ENGINE_FAIL_FAST".to_string()),
            "should NOT report DBT_ENGINE_FAIL_FAST as unused (it's aliased)"
        );

        // Clean up
        unsafe {
            #[allow(clippy::disallowed_methods)]
            std::env::remove_var("DBT_ENGINE_FAIL_FAST");
        }
    }
}
