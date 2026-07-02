//! Option keys for the FDCS ADBC driver.
//!
//! These mirror the keys understood by the `quack_adbc` driver crate (its
//! `options` module). They are kept in sync by convention — the same way the
//! other backend modules (e.g. [`crate::snowflake`]) mirror the keys expected by
//! their respective drivers — so the `dbt-auth` `fdcs` module can configure a
//! [`crate::database::Builder`] without depending on the driver crate.

// Names of Database options --------------------------------------------

/// Quack API base URL (required).
pub const BASE_URL: &str = "adbc.quack.base_url";
/// Organization to scope requests to.
pub const ORGANIZATION: &str = "adbc.quack.organization";
/// Default warehouse alias for queries.
pub const WAREHOUSE: &str = "adbc.quack.warehouse";
/// Default worker-pool affinity tag.
pub const AFFINITY: &str = "adbc.quack.affinity";
/// Default per-query timeout, in seconds.
pub const TIMEOUT_SECONDS: &str = "adbc.quack.timeout_seconds";
/// Default source SQL dialect for transpilation.
pub const DIALECT: &str = "adbc.quack.dialect";

/// Authentication method: one of [`auth_type`].
pub const AUTH_TYPE: &str = "adbc.quack.auth.type";
/// API key, sent as `X-API-Key`.
pub const AUTH_API_KEY: &str = "adbc.quack.auth.api_key";
/// Static bearer token.
pub const AUTH_TOKEN: &str = "adbc.quack.auth.token";

/// Okta authorization endpoint.
pub const OKTA_AUTH_URL: &str = "adbc.quack.auth.okta.auth_url";
/// Okta token endpoint.
pub const OKTA_TOKEN_URL: &str = "adbc.quack.auth.okta.token_url";
/// Okta OAuth client id.
pub const OKTA_CLIENT_ID: &str = "adbc.quack.auth.okta.client_id";

/// Accepted values for [`AUTH_TYPE`].
pub mod auth_type {
    /// Send an `X-API-Key` header.
    pub const API_KEY: &str = "api_key";
    /// Send a static bearer token.
    pub const TOKEN: &str = "token";
    /// Run the interactive Okta PKCE browser flow.
    pub const OKTA_BROWSER: &str = "okta_browser";
}
