//! Option keys for the dbt Compute ADBC driver.
//!
//! These mirror the keys understood by the `adbc_driver_dbt` driver crate (its
//! `options` module). They are kept in sync by convention — the same way the
//! other backend modules (e.g. [`crate::snowflake`]) mirror the keys expected by
//! their respective drivers — so the `dbt-auth` `fdcs` module can configure a
//! [`crate::database::Builder`] without depending on the driver crate.

// Names of Database options --------------------------------------------

/// dbt Compute API base URL (required).
pub const BASE_URL: &str = "adbc.dbt.base_url";
/// Organization to scope requests to.
pub const ORGANIZATION: &str = "adbc.dbt.organization";
/// Default warehouse alias for queries.
pub const WAREHOUSE: &str = "adbc.dbt.warehouse";
/// Default worker-pool affinity tag.
pub const AFFINITY: &str = "adbc.dbt.affinity";
/// Default per-query timeout, in seconds.
pub const TIMEOUT_SECONDS: &str = "adbc.dbt.timeout_seconds";
/// Default source SQL dialect for transpilation.
pub const DIALECT: &str = "adbc.dbt.dialect";

/// Authentication method: one of [`auth_type`].
pub const AUTH_TYPE: &str = "adbc.dbt.auth.type";
/// API key, sent as `X-API-Key`.
pub const AUTH_API_KEY: &str = "adbc.dbt.auth.api_key";
/// Static bearer token.
pub const AUTH_TOKEN: &str = "adbc.dbt.auth.token";

/// Okta authorization endpoint.
pub const OKTA_AUTH_URL: &str = "adbc.dbt.auth.okta.auth_url";
/// Okta token endpoint.
pub const OKTA_TOKEN_URL: &str = "adbc.dbt.auth.okta.token_url";
/// Okta OAuth client id.
pub const OKTA_CLIENT_ID: &str = "adbc.dbt.auth.okta.client_id";

/// Accepted values for [`AUTH_TYPE`].
pub mod auth_type {
    /// Send an `X-API-Key` header.
    pub const API_KEY: &str = "api_key";
    /// Send a static bearer token.
    pub const TOKEN: &str = "token";
    /// Run the interactive Okta PKCE browser flow.
    pub const OKTA_BROWSER: &str = "okta_browser";
}
