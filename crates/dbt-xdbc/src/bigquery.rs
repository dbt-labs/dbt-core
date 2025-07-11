// from https://github.com/apache/arrow-adbc/blob/9a10e6791db6d54b813fde4df3925c354822192e/go/adbc/driver/bigquery/driver.go#L31

pub const AUTH_TYPE: &str = "adbc.bigquery.sql.auth_type";
pub const PROJECT_ID: &str = "adbc.bigquery.sql.project_id";
pub const DATASET_ID: &str = "adbc.bigquery.sql.dataset_id";
pub const TABLE_ID: &str = "adbc.bigquery.sql.table_id";

// values
pub mod auth_type {
    pub const DEFAULT: &str = "adbc.bigquery.sql.auth_type.auth_bigquery";
    pub const USER_AUTHENTICATION: &str = "adbc.bigquery.sql.auth_type.user_authentication";
    pub const TEMPORARY_ACCESS_TOKEN: &str = "adbc.bigquery.sql.auth_type.temporary_access_token";
    pub const JSON_CREDENTIAL_FILE: &str = "adbc.bigquery.sql.auth_type.json_credential_file";
    pub const JSON_CREDENTIAL_STRING: &str = "adbc.bigquery.sql.auth_type.json_credential_string";
}

pub const AUTH_CREDENTIALS: &str = "adbc.bigquery.sql.auth_credentials";
// one-time access token, the kind that refresh token will generate incidentally
pub const AUTH_ACCESS_TOKEN: &str = "adbc.bigquery.sql.auth.access_token";
pub const AUTH_CLIENT_ID: &str = "adbc.bigquery.sql.auth.client_id";
pub const AUTH_CLIENT_SECRET: &str = "adbc.bigquery.sql.auth.client_secret";
pub const AUTH_REFRESH_TOKEN: &str = "adbc.bigquery.sql.auth.refresh_token";
pub const AUTH_ACCESS_TOKEN_ENDPOINT: &str = "adbc.bigquery.sql.auth.access_token_endpoint";
pub const AUTH_ACCESS_TOKEN_SERVER_NAME: &str = "adbc.bigquery.sql.auth.access_token_server_name";

// The parameter mode specifies if the query uses positional syntax ("?")
// or the named syntax ("@p"). It is illegal to mix positional and named syntax.
// Default is QUERY_PARAMETER_MODE_POSITIONAL.
pub const QUERY_PARAMETER_MODE: &str = "adbc.bigquery.sql.query.parameter_mode";
// values
pub const QUERY_PARAMETER_MODE_NAMED: &str = "adbc.bigquery.sql.query.parameter_mode_named";
pub const QUERY_PARAMETER_MODE_POSITIONAL: &str =
    "adbc.bigquery.sql.query.parameter_mode_positional";

pub const QUERY_DESTINATION_TABLE: &str = "adbc.bigquery.sql.query.destination_table";
pub const QUERY_DEFAULT_PROJECT_ID: &str = "adbc.bigquery.sql.query.default_project_id";
pub const QUERY_DEFAULT_DATASET_ID: &str = "adbc.bigquery.sql.query.default_dataset_id";
pub const QUERY_CREATE_DISPOSITION: &str = "adbc.bigquery.sql.query.create_disposition";
pub const QUERY_WRITE_DISPOSITION: &str = "adbc.bigquery.sql.query.write_disposition";
pub const QUERY_DISABLE_QUERY_CACHE: &str = "adbc.bigquery.sql.query.disable_query_cache"; // bool
pub const DISABLE_FLATTENED_RESULTS: &str = "adbc.bigquery.sql.query.disable_flattened_results"; // bool
pub const QUERY_ALLOW_LARGE_RESULTS: &str = "adbc.bigquery.sql.query.allow_large_results"; // bool

pub const QUERY_PRIORITY: &str = "adbc.bigquery.sql.query.priority"; // string
pub const QUERY_MAX_BILLING_TIER: &str = "adbc.bigquery.sql.query.max_billing_tier"; // i64
pub const QUERY_MAX_BYTES_BILLED: &str = "adbc.bigquery.sql.query.max_bytes_billed"; // i64
pub const QUERY_USE_LEGACY_SQL: &str = "adbc.bigquery.sql.query.use_legacy_sql"; // bool
pub const QUERY_DRY_RUN: &str = "adbc.bigquery.sql.query.dry_run"; // bool
pub const QUERY_CREATE_SESSION: &str = "adbc.bigquery.sql.query.create_session"; // bool
pub const QUERY_JOB_TIMEOUT: &str = "adbc.bigquery.sql.query.job_timeout"; // i64

pub const QUERY_RESULT_BUFFER_SIZE: &str = "adbc.bigquery.sql.query.result_buffer_size"; // i64
pub const QUERY_PREFETCH_CONCURRENCY: &str = "adbc.bigquery.sql.query.prefetch_concurrency"; // i64

// values
pub const DEFAULT_QUERY_RESULT_BUFFER_SIZE: i64 = 200;
pub const DEFAULT_QUERY_PREFETCH_CONCURRENCY: i64 = 10;

pub const DEFAULT_CCESS_TOKEN_ENDPOINT: &str = "https://accounts.google.com/o/oauth2/token";
pub const DEFAULT_ACCESS_TOKEN_SERVER_NAME: &str = "google.com";
pub const INGEST_FILE_DELIMITER: &str = "adbc.bigquery.ingest.csv_delimiter";
pub const INGEST_PATH: &str = "adbc.bigquery.ingest.csv_filepath";
pub const INGEST_SCHEMA: &str = "adbc.bigquery.ingest.csv_schema";
pub const UPDATE_TABLE_COLUMNS_DESCRIPTION: &str = "adbc.bigquery.table.update_columns_description";
pub const UPDATE_DATASET_AUTHORIZE_VIEW_TO_DATASETS: &str =
    "adbc.bigquery.dataset.authorize_view_to_datasets";
