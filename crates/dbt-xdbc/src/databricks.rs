/// Databricks ADBC Connection Options
/// Referenced from: github.com/dbt-labs/arrow-adbc/go/driver/databricks/driver.go
/// Authentication type options
pub const AUTH_TYPE: &str = "databricks.auth_type";

pub mod auth_type {
    /// OAuth M2M authentication
    pub const OAUTH_M2M: &str = "oauth-m2m";
    /// Personal Access Token authentication
    pub const PAT: &str = "pat";
    /// External Browser authentication
    pub const EXTERNAL_BROWSER: &str = "external-browser";
}

/// HTTP Path to connect
pub const HTTP_PATH: &str = "databricks.http_path";

/// Optional default catalog to use when executing SQL statements
pub const CATALOG: &str = "databricks.catalog";
/// Optional default schema to use when executing SQL statements
pub const SCHEMA: &str = "databricks.schema";

/// Databricks host (either of workspace endpoint or Accounts API endpoint)
pub const HOST: &str = "databricks.server_hostname";

/// Databricks token
pub const TOKEN: &str = "databricks.access_token";

/// The Databricks service principal's client ID
pub const CLIENT_ID: &str = "databricks.oauth.client_id";
/// The Databricks service principal's client secret
pub const CLIENT_SECRET: &str = "databricks.oauth.client_secret";
/// Timeout for U2M OAuth
pub const OAUTH_TIMEOUT: &str = "databricks.oauth.external_browser.timeout";

/// TLS/SSL options
pub const SSL_MODE: &str = "databricks.ssl_mode";
pub const SSL_ROOT_CERT: &str = "databricks.ssl_root_cert";

/// User agent string for dbt attribution by databricks
pub const USER_AGENT: &str = "databricks.user_agent";
