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
pub const AUTH_TYPE: &str = "adbc.databricks.auth_type";

pub mod auth_type {
    /// OAuth M2M authentication
    pub const OAUTH_M2M: &str = "oauth-m2m";
    /// Personal Access Token authentication
    pub const PAT: &str = "pat";
    /// Google ID authentication
    pub const GOOGLE_ID: &str = "google-id";
    /// Google Credentials authentication
    pub const GOOGLE_CREDENTIALS: &str = "google-credentials";
    /// Azure CLI authentication
    pub const AZURE_CLI: &str = "azure-cli";
    /// Azure MSI authentication
    pub const AZURE_MSI: &str = "azure-msi";
    /// Azure Client Secret authentication
    pub const AZURE_CLIENT_SECRET: &str = "azure-client-secret";
    /// External Browser authentication
    pub const EXTERNAL_BROWSER: &str = "external-browser";
}

/// Cluster option
pub const CLUSTER: &str = "adbc.databricks.cluster";
/// Warehouse option
pub const WAREHOUSE: &str = "adbc.databricks.warehouse";
/// Serverless Compute ID option
pub const SERVERLESS_COMPUTE_ID: &str = "adbc.databricks.serverless_compute_id";

/// Optional default catalog to use when executing SQL statements
pub const CATALOG: &str = "adbc.databricks.catalog";
/// Optional default schema to use when executing SQL statements
pub const SCHEMA: &str = "adbc.databricks.schema";

/// URL of the metadata service that provides authentication credentials
pub const METADATA_SERVICE_URL: &str = "adbc.databricks.metadata_service_url";

/// Databricks host (either of workspace endpoint or Accounts API endpoint)
pub const HOST: &str = "adbc.databricks.host";
/// Databricks token
pub const TOKEN: &str = "adbc.databricks.token";
/// The Databricks account ID for the Databricks account endpoint
pub const ACCOUNT_ID: &str = "adbc.databricks.account_id";
/// Username
pub const USERNAME: &str = "username";
/// Password
pub const PASSWORD: &str = "password";

/// The Databricks service principal's client ID
pub const CLIENT_ID: &str = "adbc.databricks.client_id";
/// The Databricks service principal's client secret
pub const CLIENT_SECRET: &str = "adbc.databricks.client_secret";

/// Location of the Databricks CLI credentials file
pub const CONFIG_FILE: &str = "adbc.databricks.config_file";
/// The default named profile to use, other than DEFAULT
pub const PROFILE: &str = "adbc.databricks.profile";

/// Google Service Account option
pub const GOOGLE_SERVICE_ACCOUNT: &str = "adbc.databricks.google_service_account";
/// Google Credentials option
pub const GOOGLE_CREDENTIALS: &str = "adbc.databricks.google_credentials";

/// Azure Resource Manager ID for Azure Databricks workspace
pub const AZURE_RESOURCE_ID: &str = "adbc.databricks.azure_workspace_resource_id";

/// Azure Use MSI option
pub const AZURE_USE_MSI: &str = "adbc.databricks.azure_use_msi";
/// Azure Client Secret option
pub const AZURE_CLIENT_SECRET: &str = "adbc.databricks.azure_client_secret";
/// Azure Client ID option
pub const AZURE_CLIENT_ID: &str = "adbc.databricks.azure_client_id";
/// Azure Tenant ID option
pub const AZURE_TENANT_ID: &str = "adbc.databricks.azure_tenant_id";

/// Parameters to request Azure OIDC token on behalf of Github Actions
pub const ACTIONS_ID_TOKEN_REQUEST_URL: &str = "adbc.databricks.actions_id_token_request_url";
/// Parameters to request Azure OIDC token on behalf of Github Actions
pub const ACTIONS_ID_TOKEN_REQUEST_TOKEN: &str = "adbc.databricks.actions_id_token_request_token";

/// AzureEnvironment (PUBLIC, USGOVERNMENT, CHINA) has specific set of API endpoints
pub const AZURE_ENVIRONMENT: &str = "adbc.databricks.azure_environment";

/// Skip SSL certificate verification for HTTP calls
pub const INSECURE_SKIP_VERIFY: &str = "adbc.databricks.insecure_skip_verify";

/// Number of seconds for HTTP timeout. Default is 60 (1 minute)
pub const HTTP_TIMEOUT_SECONDS: &str = "adbc.databricks.http_timeout_seconds";

/// Truncate JSON fields in JSON above this limit. Default is 96
pub const DEBUG_TRUNCATE_BYTES: &str = "adbc.databricks.debug_truncate_bytes";

/// Debug HTTP headers of requests made by the provider. Default is false
pub const DEBUG_HEADERS: &str = "adbc.databricks.debug_headers";

/// Maximum number of requests per second made to Databricks REST API. Default is 15 RPS
pub const RATE_LIMIT_PER_SECOND: &str = "adbc.databricks.rate_limit_per_second";

/// Number of seconds to keep retrying HTTP requests. Default is 300 (5 minutes)
pub const RETRY_TIMEOUT_SECONDS: &str = "adbc.databricks.retry_timeout_seconds";
