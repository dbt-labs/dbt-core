// ----------------------------------------------------------------------------------------------
// DBT FUSION
pub const DBT_FUSION: &str = "dbt-fusion";
pub const DBT_SA_CLI: &str = "dbt-sa-cli";

// ----------------------------------------------------------------------------------------------
// dbt inputs
pub const DBT_MIN_SUPPORTED_VERSION: &str = "1.8.0";
pub const DBT_PROJECT_YML: &str = "dbt_project.yml";
pub const DBT_PROFILES_YML: &str = "profiles.yml";

// ----------------------------------------------------------------------------------------------
// dbt outputs

//   target/
//   ├── compiled/
//   │   ├── model_fs_example.sql
//   │   └── other_files.sql
//   ├── run/
//   │   ├── model_fs_example.sql
//   │   └── other_files.sql
//   ├── generic_tests/
//   │   ├── test.sql
//   ├── manifest.json
//   ├── catalog.json
//   └── logs/
//       └── fs_run_log.txt
//   └── db/
//       └── database/schema/table.parquet
pub const DBT_TARGET_DIR_NAME: &str = "target";
pub const DBT_PACKAGES_DIR_NAME: &str = "dbt_packages";
pub const DBT_INTERNAL_PACKAGES_DIR_NAME: &str = "dbt_internal_packages";
pub const DBT_MANIFEST_JSON: &str = "manifest.json";
pub const DBT_CATALOG_JSON: &str = "catalog.json";
pub const DBT_COMPILED_DIR_NAME: &str = "compiled";
pub const DBT_METADATA_DIR_NAME: &str = "metadata";
pub const DBT_EPHEMERAL_DIR_NAME: &str = "ephemeral";
pub const DBT_HOOKS_DIR_NAME: &str = "hooks";
pub const DBT_CTE_PREFIX: &str = "__dbt__cte__";
pub const DBT_RUN_DIR_NAME: &str = "run";
pub const DBT_DB_DIR_NAME: &str = "db";
pub const DBT_LOG_DIR_NAME: &str = "logs";
pub const DBT_ROOT_PACKAGE_VAR_PREFIX: &str = "__root__";
pub const DBT_GENERIC_TESTS_DIR_NAME: &str = "generic_tests";
pub const DBT_SNAPSHOTS_DIR_NAME: &str = "snapshots";
// ----------------------------------------------------------------------------------------------
pub const DBT_MODELS_DIR_NAME: &str = "models";

// ----------------------------------------------------------------------------------------------
// dbt packages
pub const DBT_PACKAGES_LOCK_FILE: &str = "package-lock.yml";
pub const DBT_PACKAGES_YML: &str = "packages.yml";
pub const DBT_DEPENDENCIES_YML: &str = "dependencies.yml";

// ----------------------------------------------------------------------------------------------
// dbt console output
pub const ERROR: &str = "error:";
pub const WARNING: &str = "warning:";
pub const PANIC: &str = "panic:";

// ----------------------------------------------------------------------------------------------
// test verdicts
pub const VERDICT_PASS: &str = "[Pass]";
pub const VERDICT_FAIL: &str = "[Fail]";
pub const VERDICT_WARN: &str = "[Warn]";
// ----------------------------------------------------------------------------------------------
// actions in order of appearance

pub const ANALYZING: &str = " Analyzing";
pub const LOADING: &str = "   Loading";
pub const FETCHING: &str = "  Fetching";
pub const INSTALLING: &str = "Installing";
pub const EXTENDING: &str = " Extending";
pub const RESOLVING: &str = " Resolving";
pub const PARSING: &str = "   Parsing";
pub const REMOVING: &str = "  Removing";
pub const CACHING: &str = "   Caching";
// not being issued right now
pub const SCHEDULING: &str = "Scheduling";
//
pub const CLEANING: &str = "  Cleaning";
pub const FORMATTING: &str = " Formatting";
pub const LINTING: &str = "   Linting";
pub const DOWNLOADING: &str = " Downloading";
pub const DOWNLOADED: &str = " Downloaded";
pub const COMPILING: &str = " Compiling";
pub const RENDERING: &str = " Rendering";
pub const SEEDING: &str = "   Seeding";
pub const HYDRATING: &str = "  Hydrating";
pub const TESTING: &str = "   Testing";
pub const RUNNING: &str = "   Running";
pub const DEFERRING: &str = "Deferring State";
pub const WAITING: &str = "   Waiting";
pub const CLONING: &str = "   Cloning";
pub const ABORTED: &str = "   Aborted";
pub const SUCCEEDED: &str = " Succeeded";
pub const PASSED: &str = "    Passed";
pub const WARNED: &str = "    Warned";
pub const FAILED: &str = "    Failed";
pub const REUSED: &str = "    Reused";
pub const STALE: &str = "     Stale";
pub const SKIPPED: &str = "   Skipped";
pub const ANALYZED: &str = "  Analyzed";
pub const RENDERED: &str = "  Rendered";
pub const FRESHNESS: &str = " Freshness";

// debug command
pub const VALIDATING: &str = "Validating";
pub const DEBUGGING: &str = " Debugging";
pub const DEBUGGED: &str = "  Debugged";

// done
pub const FINISHED: &str = "  Finished";

// other
pub const PREVIEWING: &str = "Previewing";
pub const INLINE_NODE: &str = "sql_operation.inline";
pub const NOOP: &str = "noop";

// log targets
pub const EXECUTING: &str = " ExecutingSql";
pub const CACHE_LOG: &str = "CacheLogging";

// cas/node read/write

pub const CAS_RD: &str = "   Reading";
pub const CAS_WR: &str = "   Writing";
pub const NODES_RD: &str = "   Reading";
pub const NODES_WR: &str = "   Writing";
pub const COLUMNS_RD: &str = "   Reading";
pub const COLUMNS_WR: &str = "   Writing";
pub const COLUMN_LINEAGE_WR: &str = "   Writing";

pub const DBT_CDN_URL: &str = "https://public.cdn.getdbt.com/fs";
