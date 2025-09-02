pub mod odbc {
    // https://docs.aws.amazon.com/pdfs/redshift/latest/mgmt/redshift-mgmt.pdf#odbc20-configuration-options
    pub const DRIVER: &str = "Driver";
    pub const AUTH_TYPE: &str = "AuthType";

    /// The suggested value for the DRIVER key in the ODBC connection string.
    pub fn odbc_driver_path() -> String {
        if let Ok(driver_path_override) = std::env::var(REDSHIFT_DRIVER_PATH_ENV_VAR_NAME) {
            return driver_path_override;
        }
        // Locations of the Databricks ODBC driver (64-bit) on different platforms (based on the
        // installers downloaded from
        // https://docs.aws.amazon.com/redshift/latest/mgmt/odbc-driver-windows-how-to-install.html,
        // https://docs.aws.amazon.com/redshift/latest/mgmt/odbc-driver-mac-how-to-install.html, or
        // https://docs.aws.amazon.com/redshift/latest/mgmt/odbc-driver-linux-how-to-install.html).
        // If standard locations change, we can start dynamically probing different locations.
        #[cfg(target_os = "linux")]
        {
            "/opt/amazon/redshiftodbc/lib/64/libamazonredshiftodbc64.so".to_string()
        }
        #[cfg(target_os = "macos")]
        {
            "/opt/amazon/redshift/lib/libamazonredshiftodbc.dylib".to_string()
        }
        #[cfg(target_os = "windows")]
        {
            "REPLACE_WITH_A_VALID_WINDOWS_PATH".to_string()
        }
    }

    pub const ACCESS_KEY_ID: &str = "AccessKeyID";
    pub const APP_ID: &str = "app_id";
    pub const APP_NAME: &str = "app_name";
    pub const AUTH_PROFILE: &str = "AuthProfile"; // if you set this parameter, you must also set AccessKeyID and SecretAccessKey

    pub mod auth_type_options {
        pub const STANDARD: &str = "Standard";
        pub const AWS_PROFILE: &str = "AWS Profile";
        pub const AWS_IAM_CREDENTIALS: &str = "AWS IAM Credentials";
        // To use an identity provider, you must set the plugin_name property.
        pub const IDP_AD_FS: &str = "Identity Provider: AD FS";
        pub const IDP_AUTH_PLUGIN: &str = "Identity Provider: Auth Plugin";
        pub const IDP_AZURE_AD: &str = "Identity Provider: Azure AD";
        pub const IDP_JWT: &str = "Identity Provider: JWT";
        pub const IDP_OKTA: &str = "Identity Provider: Okta";
        pub const IDP_PINGFEDERATE: &str = "Identity Provider: PingFederate";
    }

    pub const AUTO_CREATE_USER: &str = "AutoCreate";
    pub mod auto_crete_user_options {
        pub const CREATE_NEW_USER: bool = true;
        pub const FAIL_ON_NO_USER: bool = false;
    }
    pub const CA_FILE: &str = "CaFile"; // Linux only
    pub const CLIENT_ID: &str = "client_id"; // Azure
    pub const CLIENT_SECRET: &str = "client_secret"; // Azure
    pub const CLUSTER_ID: &str = "ClusterId";
    pub const COMPRESSION: &str = "compression";
    pub mod compression_options {
        pub const LZ4: &str = "lz4";
        pub const ZSTD: &str = "zstd";
        pub const OFF: &str = "off";
    }

    pub const DATABASE: &str = "Database";
    pub const DATABASE_METADATA_CURRENT_DB_ONLY: &str = "DatabaseMetadataCurrentDbOnly";
    pub const DBGROUPS_FILTER: &str = "dbgroups_filter";
    pub const DEFAULT_DRIVER_NAME: &str = "Amazon Redshift ODBC Driver (x64)"; // also an x32 version
    pub const DSN: &str = "DSN";
    pub const ENDPOINT_URL: &str = "EndpointUrl";
    pub const FORCE_LOWERCASE: &str = "ForceLowercase";
    pub mod force_lowercase_options {
        pub const LOWERCASE_ALL_DB_GROUPS: bool = true;
        pub const DO_NOT_ALTER_USER_GROUPS: bool = false;
    }
    pub const GROUP_FEDERATION: &str = "group_federation";
    pub mod group_federation_options {
        pub const USE_IAM: bool = true; // getClusterCredentialsWithIAM
        pub const DO_NOT_USE_IAM: bool = false; // getClusterCredentials
    }

    pub const HTTPS_PROXY_HOST: &str = "https_proxy_host";
    pub const HTTPS_PROXY_PASSWORD: &str = "https_proxy_password";
    pub const HTTPS_PROXY_PORT: &str = "https_proxy_port";
    pub const HTTPS_PROXY_USERNAME: &str = "https_proxy_username";
    pub const IAM: &str = "IAM"; // set true for IAM auth methods
    pub const IDC_CLIENT_DISPLAY_NAME: &str = "idc_client_display_name";
    pub const IDC_REGION: &str = "idc_region";
    pub const IDP_HOST: &str = "idp_host";
    pub const IDP_PORT: &str = "idp_port";
    pub const IDP_RESPONSE_TIMEOUT: &str = "idp_response_timeout";
    pub const IDP_TENANT: &str = "idp_tenant";
    pub const IDP_USE_HTTPS_PROXY: &str = "idp_use_https_proxy";
    pub const INSTANCE_PROFILE: &str = "InstanceProfile";
    pub const ISSUER_URL: &str = "issuer_url";
    pub const KEEP_ALIVE: &str = "KeepAlive";
    pub const KEEP_ALIVE_COUNT: &str = "KeepAliveCount";
    pub const KEEP_ALIVE_INTERVAL: &str = "KeepAliveInterval";
    pub const KEEP_ALIVE_TIME: &str = "KeepAliveTime";
    pub const LISTEN_PORT: &str = "listen_port";
    pub const LOGIN_URL: &str = "login_url";
    pub const LOGIN_TO_RP: &str = "loginToRp";
    pub const LOG_LEVEL: &str = "LogLevel";
    pub mod log_level_options {
        /// From docs: We recommend you only enable logging long enough to capture an issue, as
        /// logging decreases performance and can consume a large quantity of disk space.
        pub const OFF: i32 = 0;
        pub const ERROR: i32 = 1;
        pub const API_CALL: i32 = 0; // log all ODBC function calls
        pub const INFO: i32 = 0;
        pub const MSG_PROTOCOL: i32 = 0; // log all info about driver protocols
        pub const DEBUG: i32 = 0; // logs all driver activity
        pub const DEBUG_APPEND: i32 = 0; // keep appending logs for all driver activities
    }
    pub const LOG_PATH: &str = "LogPath";
    pub const MIN_TLS: &str = "Min_TLS";
    pub const PARTNER_SPID: &str = "partner_spid";
    pub const PASSWORD: &str = "Password";
    pub const PLUGIN_NAME: &str = "plugin_name";
    pub mod auth_plugin_options {
        pub const ACTIVE_DIRECTORY_FEDERATION_SERVICE: &str = "ADFS";
        pub const AZURE_ACTIVE_DIRECTORY: &str = "AzureAD";
        pub const BROWSER_AZURE_ACTIVE_DIRECTORY: &str = "BrowserAzureAD";
        pub const AWS_IAM_IDC_BROWSER: &str = "BrowserIdcAuthPlugin";
        pub const BROWSER_SAML: &str = "BrowserSAML";
        pub const AWS_IAM_IDC_TOKEN: &str = "IdpTokenAuthPlugin";
        pub const JWT: &str = "JWT";
        pub const PING: &str = "Ping";
        pub const OKTA: &str = "Okta";
    }
    pub const PORT_NUMBER: &str = "PortNumber";
    pub const PREFERRED_ROLE: &str = "preferred_role";
    pub const PROFILE: &str = "Profile";
    pub const PROVIDER_NAME: &str = "provider_name";
    pub const PROXY_HOST: &str = "ProxyHost";
    pub const PROXY_PORT: &str = "ProxyPort";
    pub const PROXY_PWD: &str = "ProxyPwd";
    pub const PROXY_UID: &str = "ProxyUid";
    pub const READ_ONLY: &str = "ReadOnly";
    pub const REGION: &str = "region";
    pub const SECRET_ACCESS_KEY: &str = "SecretAccessKey";
    pub const SESSION_TOKEN: &str = "SessionToken";
    pub const SERVER: &str = "Server"; // or Host
    pub const SSL_INSECURE: &str = "ssl_insecure";
    pub const SSL_MODE: &str = "SSLMode";
    pub mod ssl_mode_options {
        pub const VERIFY_FULL: &str = "verify-full";
        pub const VERIFY_CA: &str = "verify-ca";
        pub const REQUIRE: &str = "require";
        pub const PREFER: &str = "prefer";
        pub const ALLOW: &str = "allow";
        pub const DISABLE: &str = "disable";
    }
    pub const STS_CONNECTION_TIMEOUT: &str = "StsConnectionTimeout";
    pub const STS_ENDPOINT_URL: &str = "StsEndpointUrl";
    pub const TOKEN: &str = "token";
    pub const TOKEN_TYPE: &str = "token_type"; // usually EXT_JWT, as ACCESS_TOKEN is not well supported
    pub const UID: &str = "UID";
    pub const WEB_IDENTITY_TOKEN: &str = "web_identity_token";

    // Environment variables
    pub(crate) const REDSHIFT_DRIVER_PATH_ENV_VAR_NAME: &str = "REDSHIFT_DRIVER_PATH";
}

/// The auth provider.
pub const AUTH_PROVIDER: &str = "redshift.auth.provider";
/// If specified, driver uses AWS SDK to fetch credentials.
pub const CLUSTER_TYPE: &str = "redshift.cluster_type";

pub mod cluster_type {
    pub const REDSHIFT: &str = "redshift";
    pub const REDSHIFT_IAM: &str = "redshift-iam";
    pub const SERVERLESS: &str = "redshift-serverless";
}

/// Option to automatically create user if it does not exist.
pub const AUTO_CREATE: &str = "redshift.auto_create_user";

/// Name of cluster containing the database.
pub const CLUSTER_IDENTIFIER: &str = "redshift.cluster_identifier";

/// Custom domain name associated with workgroup
pub const CUSTOM_DOMAIN_NAME: &str = "redshift.custom_domain_name";
/// Workgroup name associated with database
pub const WORK_GROUP_NAME: &str = "redshift.workgroup_name";

/// JSON string list of groups the user should join
pub const DB_GROUPS: &str = "redshift.db_groups";

pub const DB_NAME: &str = "redshift.db_name";
pub const CONNECTION_URI: &str = "redshift.connection_uri";

pub const AUTH_IDC_REGION: &str = "redshift.auth.idc_region";
pub const AUTH_ISSUER_URL: &str = "redshift.auth.issuer_url";

/// Whether TLS encryption is required
pub const SSL_MODE: &str = "redshift.ssl_mode";
pub const SSL_CERT: &str = "redshift.ssl_cert";
pub const SSL_KEY: &str = "redshift.ssl_key";
pub const SSL_ROOT_CERT: &str = "redshift.ssl_root_key";

pub const CONNECT_TIMEOUT_MS: &str = "redshift.connect_timeout_ms";
pub const CONNECT_TIMEOUT: &str = "redshift.connect_timeout";

pub const APPLICATION_NAME: &str = "redshift.application_name";

pub const AWS_REGION: &str = "redshift.aws.region";
pub const AWS_PROFILE: &str = "redshift.aws.profile";
pub const AWS_ACCESS_KEY_ID: &str = "redshift.aws.access_key_id";
pub const AWS_SECRET_ACCESS_KEY: &str = "redshift.aws.secret_access_key";
pub const AWS_SESSION_TOKEN: &str = "redshift.aws.session_token";

/// S3 bucket to use when ingesting data
pub const INGEST_BUCKET: &str = "redshift.ingest.bucket";

pub const AUTH_PROVIDER_USER_PASS: &str = "userpass";
pub const AUTH_PROVIDER_BROWSER_IDC: &str = "BrowserIdcAuthPlugin";
