pub mod odbc {
    use std::env;

    // combination of:
    //      https://docs.databricks.com/aws/en/integrations/odbc/authentication
    //      https://docs.databricks.com/aws/en/integrations/odbc/compute

    /// The suggested value for the DRIVER key in the ODBC connection string.
    pub fn odbc_driver_path() -> String {
        if let Ok(driver_path_override) = env::var(DATABRICKS_DRIVER_PATH_ENV_VAR_NAME) {
            return driver_path_override;
        }
        // Locations of the Databricks ODBC driver (64-bit) on different platforms (based on the
        // installers downloaded from https://www.databricks.com/spark/odbc-drivers-download).
        // If standard locations change, we can start dynamically probing different locations.
        #[cfg(target_os = "linux")]
        {
            // $ dpkg-deb --contents ~/simbaspark_2.9.1.1001-2_amd64.deb | egrep "\.so$"
            // -rwxrwxrwx root/root  94462112 2024-12-19 18:02 ./opt/simba/spark/lib/64/libsparkodbc_sb64.so
            "/opt/simba/spark/lib/64/libsparkodbc_sb64.so".to_string()
        }
        #[cfg(target_os = "macos")]
        {
            "/Library/simba/spark/lib/libsparkodbc_sb64-universal.dylib".to_string()
        }
        #[cfg(target_os = "windows")]
        {
            "C:\\Program Files\\Simba Spark ODBC Driver\\lib\\64\\SparkODBC_sb64.dll".to_string()
        }
    }

    pub const DRIVER: &str = "Driver";
    pub const HOST: &str = "Host";
    pub const PORT: &str = "Port";
    pub const HTTP_PATH: &str = "HTTPPath";
    pub const SSL: &str = "SSL";
    pub const THRIFT_TRANSPORT: &str = "ThriftTransport";
    pub const TOKEN_FIELD: &str = "PWD";

    // Optional
    pub const SCHEMA: &str = "Schema";
    pub const CATALOG: &str = "Catalog";

    pub const AUTH_MECHANISM: &str = "AuthMech";
    pub mod auth_mechanism_options {
        pub const TOKEN: &str = "3"; // UID + PWD
        pub const OAUTH: &str = "11"; // token pass-through & U2M M2M OAUTH
    }

    pub mod auth_flow_options {
        pub const OAUTH_TOKEN: &str = "0"; // use databricks CLI or ***
        pub const CLIENT_CREDENTIALS: &str = "1"; // M2M OAUTH
        pub const BROWSER: &str = "2"; // U2M OAUTH
    }
    // *** https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m#manually-generate-and-use-access-tokens-for-oauth-service-principal-authentication

    pub const AUTH_FLOW: &str = "Auth_Flow";
    pub const AUTH_ACCESS_TOKEN: &str = "Auth_AccessToken";

    pub const AUTH_CLIENT_ID: &str = "Auth_Client_ID";
    pub const AUTH_CLIENT_SECRET: &str = "Auth_Client_Secret";
    pub const AUTH_SCOPE: &str = "Auth_Scope";
    pub const OAUTH2_REDIRECT_URL_PORT: &str = "OAuth2RedirectUrlPort";

    // Default values

    pub const DEFAULT_PORT: &str = "443";
    pub const DEFAULT_TOKEN_UID: &str = "token";

    // Environment variables
    pub(crate) const DATABRICKS_DRIVER_PATH_ENV_VAR_NAME: &str = "DATABRICKS_DRIVER_PATH";
}

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
