use std::time::Duration;

use dbt_adapter_core::AdapterType;
use dbt_auth::AdapterConfig;
use dbt_xdbc::Connection;
use dbt_xdbc::duration::parse_duration;

#[derive(Debug)]
pub(crate) enum BackoffStrategy {
    /// Quadratic backoff: `attempt * attempt seconds`.
    ///
    /// Matches dbt-snowflake (Python)'s `exponential_backoff(attempt) = attempt * attempt`
    /// (the Python name is misleading; the formula is quadratic).
    Quadratic,
}

impl BackoffStrategy {
    /// Defines how long to sleep before the `attempt`-th retry (1-indexed).
    ///
    /// `attempt=1` is the first retry. May return [`Duration::ZERO`] for instant retry.
    fn delay_before_next_attempt(&self, attempt: u32) -> Duration {
        match self {
            BackoffStrategy::Quadratic => {
                let attempt = u64::from(attempt);
                Duration::from_secs(attempt.saturating_mul(attempt))
            }
        }
    }
}

/// Policy for retrying a connection-establishment failure.
#[derive(Debug)]
pub(crate) struct ConnectionRetryPolicy {
    adapter_type: AdapterType,
    /// Retries on top of the initial attempt. `0` is effectively no-retry.
    pub max_retries: u32,
    /// Maximum cumulative time spent *sleeping between* retry attempts.
    ///
    /// This is a sleep budget, not a wall-clock cap on the whole loop —
    /// per-attempt time is bounded separately by gosnowflake's
    /// `LOGIN_TIMEOUT` (also sourced from `connect_timeout`). Naming and
    /// semantics match the dbt-core convention for `connect_timeout`.
    pub max_retry_wait: Duration,
    /// See [`BackoffStrategy`].
    pub backoff: BackoffStrategy,
}

impl ConnectionRetryPolicy {
    /// Build a connection-establishment retry policy from a profile ([AdapterConfig]).
    ///
    /// No retries will happen if `max_retries` is zero or [ConnectionRetryPolicy::is_retryable]
    /// always returns `false`.
    ///
    /// Profile fields (all optional):
    /// - `connect_retries: int` — retries on top of the initial attempt
    ///   (default per [`default_connect_retries`])
    /// - `connect_timeout: int (seconds) | duration string` — drives BOTH
    ///   the per-attempt gosnowflake `LOGIN_TIMEOUT` (in `dbt-auth`) and
    ///   the retry-loop's cumulative-sleep budget [`max_retry_wait`] (here).
    ///   Matches dbt-core's interpretation.
    /// - `retry_all: bool = false` — catch every error class
    /// - `retry_on_database_errors: bool = false` — broaden the retryable set
    ///   to include database-level / unknown errors
    pub fn new(adapter_type: AdapterType, config: &AdapterConfig) -> ConnectionRetryPolicy {
        let max_retries = config
            .get_string("connect_retries")
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or_else(|| Self::default_connect_retries(adapter_type));

        // `connect_timeout` is typed as `Option<i64>` in the profile schema
        // (see `dbt-schemas/src/schemas/profiles.rs`), so customer profiles
        // set a bare integer (seconds). `dbt-auth::apply_connection_args`
        // already reads it via `get_string` and treats it as seconds for
        // gosnowflake's per-attempt `LOGIN_TIMEOUT`; mirror that here so the
        // retry-loop's sleep budget honors the same field. Also accept a
        // duration string (e.g. "60s") for symmetry with `parse_duration`.
        let max_retry_wait = config
            .get_string("connect_timeout")
            .and_then(|v| {
                let s = v.as_ref();
                s.parse::<u64>()
                    .ok()
                    .map(Duration::from_secs)
                    .or_else(|| parse_duration(s).ok())
            })
            .unwrap_or_else(|| Self::default_max_retry_wait(adapter_type));

        ConnectionRetryPolicy {
            adapter_type,
            max_retries,
            max_retry_wait,
            backoff: BackoffStrategy::Quadratic,
        }
    }

    /// Execute a connection attempt with retry according to the policy.
    ///
    /// `connect_fn` is called for each attempt. On retryable failures the
    /// policy's `backoff` is applied before the next attempt. The final error
    /// after exhausting retries is the error from the last attempt.
    ///
    /// `max_retry_wait` bounds the *sleep* time across retries — per-attempt
    /// time is bounded separately by `LOGIN_TIMEOUT`. Worst-case total
    /// wallclock is `(max_retries + 1) * LOGIN_TIMEOUT + max_retry_wait`.
    pub fn execute(
        &self,
        config: &AdapterConfig,
        mut connect_fn: impl FnMut() -> adbc_core::error::Result<Box<dyn Connection>>,
    ) -> adbc_core::error::Result<Box<dyn Connection>> {
        let mut sleep_elapsed = Duration::ZERO;
        let mut attempt: u32 = 0;
        loop {
            match connect_fn() {
                Ok(conn) => return Ok(conn),
                Err(err) => {
                    if attempt >= self.max_retries
                        || sleep_elapsed >= self.max_retry_wait
                        || !self.is_retryable(config, &err)
                    {
                        return Err(err);
                    }
                    // XXX: consider printing the error as a warning before hanging on the user
                    let next = attempt + 1;
                    let want = self.backoff.delay_before_next_attempt(next);
                    // Cap the delay so the sleep budget is respected.
                    let remaining = self.max_retry_wait.saturating_sub(sleep_elapsed);
                    let delay = want.min(remaining);
                    if !delay.is_zero() {
                        std::thread::sleep(delay);
                        sleep_elapsed += delay;
                    }
                    attempt = next;
                }
            }
        }
    }

    /// Per-adapter default for the `connect_retries` profile field.
    fn default_connect_retries(adapter_type: AdapterType) -> u32 {
        use AdapterType::*;
        match adapter_type {
            Snowflake => 4,
            // XXX: expand if customization is required
            _ => 1,
        }
    }

    /// Per-adapter default for [`max_retry_wait`](ConnectionRetryPolicy::max_retry_wait).
    /// Used when `connect_timeout` is not set on the profile.
    fn default_max_retry_wait(adapter_type: AdapterType) -> Duration {
        use AdapterType::*;
        match adapter_type {
            Snowflake => Duration::from_secs(60),
            // XXX: expand if customization is required
            _ => Duration::from_secs(60),
        }
    }

    fn is_retryable(&self, config: &AdapterConfig, err: &adbc_core::error::Error) -> bool {
        use AdapterType::*;
        match self.adapter_type {
            Snowflake => is_retryable_snowflake_login_error(config, err),
            // Other adapters don't have a retryable-error criteria implemented here yet.
            _ => false,
        }
    }
}

/// Mirrors the Python dbt-snowflake retryable exception list:
///
/// ```text
/// InternalError, InternalServerError, ServiceUnavailableError,
/// GatewayTimeoutError, RequestTimeoutError, BadGatewayError,
/// OtherHTTPRetryableError, BindUploadError
/// ```
///
/// We don't have those exception types directly in ADBC; the same failures
/// surface as `Status::IO` (network-level) or `Status::Internal` with the
/// gosnowflake error string preserved. We match on both the status and
/// well-known substrings of the underlying Go error.
///
/// - `retry_all`: catch every error class (Python's `retry_all=True` →
///   `retryable_exceptions = [Error]`).
/// - `retry_on_database_errors`: broaden to internal/unknown statuses
///   (Python's `retry_on_database_errors=True` → adds `DatabaseError`).
fn is_retryable_snowflake_login_error(
    config: &AdapterConfig,
    err: &adbc_core::error::Error,
) -> bool {
    use adbc_core::error::Status;

    let retry_all = config.get_bool("retry_all").unwrap_or(false);
    if retry_all {
        return true;
    }

    // Known-permanent: do not retry real auth failures.
    if matches!(err.status, Status::Unauthenticated | Status::Unauthorized) {
        return false;
    }

    let msg = err.message.to_lowercase();
    const TRANSIENT_PATTERNS: &[&str] = &[
        // Go net/http + context fired
        "client.timeout exceeded while awaiting headers",
        "context deadline exceeded",
        // Go net dial / DNS
        "i/o timeout",
        "connection reset",
        "connection refused",
        "no such host",
        // Snowflake / HTTP gateway responses
        "internal server error",
        "bad gateway",
        "service unavailable",
        "gateway timeout",
        "request timeout",
        "bindupload",
    ];
    if TRANSIENT_PATTERNS.iter().any(|p| msg.contains(p)) {
        return true;
    }

    // Network-level failures.
    if matches!(err.status, Status::IO) {
        return true;
    }

    // `retry_on_database_errors=true` broadens to internal/unknown statuses,
    // mirroring Python's inclusion of `DatabaseError` in the retryable list.
    let retry_on_database_errors = config.get_bool("retry_on_database_errors").unwrap_or(false);
    if retry_on_database_errors && matches!(err.status, Status::Internal | Status::Unknown) {
        return true;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::error::{Error as AdbcError, Status};

    fn adbc_err(status: Status, msg: &str) -> AdbcError {
        AdbcError::with_message_and_status(msg, status)
    }

    #[test]
    fn quadratic_backoff_matches_python_formula() {
        use BackoffStrategy::Quadratic;
        assert_eq!(
            Quadratic.delay_before_next_attempt(0),
            Duration::from_secs(0)
        );
        assert_eq!(
            Quadratic.delay_before_next_attempt(1),
            Duration::from_secs(1)
        );
        assert_eq!(
            Quadratic.delay_before_next_attempt(2),
            Duration::from_secs(4)
        );
        assert_eq!(
            Quadratic.delay_before_next_attempt(3),
            Duration::from_secs(9)
        );
        assert_eq!(
            Quadratic.delay_before_next_attempt(10),
            Duration::from_secs(100)
        );
    }

    #[test]
    fn connect_retry_policy_default_retries_for_non_snowflake() {
        let cfg = AdapterConfig::new(dbt_yaml::Mapping::new());
        assert_eq!(
            ConnectionRetryPolicy::new(AdapterType::Bigquery, &cfg).max_retries,
            1
        );
        assert_eq!(
            ConnectionRetryPolicy::new(AdapterType::Databricks, &cfg).max_retries,
            1
        );
        assert_eq!(
            ConnectionRetryPolicy::new(AdapterType::Redshift, &cfg).max_retries,
            1
        );
        assert_eq!(
            ConnectionRetryPolicy::new(AdapterType::DuckDB, &cfg).max_retries,
            1
        );
    }

    #[test]
    fn connect_retry_policy_snowflake_defaults_match_python() {
        let cfg = AdapterConfig::new(dbt_yaml::Mapping::new());
        let policy = ConnectionRetryPolicy::new(AdapterType::Snowflake, &cfg);
        assert_eq!(policy.max_retries, 4);
        // Quadratic backoff: attempt=1 → 1s, attempt=2 → 4s.
        assert_eq!(
            policy.backoff.delay_before_next_attempt(1),
            Duration::from_secs(1)
        );
        assert_eq!(
            policy.backoff.delay_before_next_attempt(2),
            Duration::from_secs(4)
        );
        // Default predicate: retry on Status::IO, not on auth failures.
        assert!(policy.is_retryable(&cfg, &adbc_err(Status::IO, "dial tcp: i/o timeout")));
        assert!(!policy.is_retryable(&cfg, &adbc_err(Status::Unauthenticated, "bad creds")));
    }

    #[test]
    fn connect_retry_policy_snowflake_reads_profile_fields() {
        let mapping = dbt_yaml::Mapping::from_iter([
            ("connect_retries".into(), 3.into()),
            ("retry_all".into(), true.into()),
        ]);
        let cfg = AdapterConfig::new(mapping);
        let policy = ConnectionRetryPolicy::new(AdapterType::Snowflake, &cfg);
        assert_eq!(policy.max_retries, 3);
        // retry_all=true matches Python's `retryable_exceptions = [Error]` →
        // catches every error class including auth.
        assert!(policy.is_retryable(&cfg, &adbc_err(Status::Unauthenticated, "bad creds")));
    }

    #[test]
    fn connect_timeout_defaults_to_60s() {
        let cfg = AdapterConfig::new(dbt_yaml::Mapping::new());
        let policy = ConnectionRetryPolicy::new(AdapterType::Snowflake, &cfg);
        assert_eq!(policy.max_retry_wait, Duration::from_secs(60));
    }

    #[test]
    fn connect_timeout_parses_seconds_string() {
        let mapping = dbt_yaml::Mapping::from_iter([("connect_timeout".into(), "30s".into())]);
        let cfg = AdapterConfig::new(mapping);
        let policy = ConnectionRetryPolicy::new(AdapterType::Snowflake, &cfg);
        assert_eq!(policy.max_retry_wait, Duration::from_secs(30));
    }

    #[test]
    fn connect_timeout_parses_minutes_string() {
        let mapping = dbt_yaml::Mapping::from_iter([("connect_timeout".into(), "2m".into())]);
        let cfg = AdapterConfig::new(mapping);
        let policy = ConnectionRetryPolicy::new(AdapterType::Snowflake, &cfg);
        assert_eq!(policy.max_retry_wait, Duration::from_secs(120));
    }

    #[test]
    fn connect_timeout_invalid_falls_back_to_default() {
        let mapping =
            dbt_yaml::Mapping::from_iter([("connect_timeout".into(), "not_a_duration".into())]);
        let cfg = AdapterConfig::new(mapping);
        let policy = ConnectionRetryPolicy::new(AdapterType::Snowflake, &cfg);
        assert_eq!(policy.max_retry_wait, Duration::from_secs(60));
    }

    // -- Snowflake retryable-error classifier ---------------------------------

    fn cfg_default() -> AdapterConfig {
        AdapterConfig::new(dbt_yaml::Mapping::new())
    }

    fn cfg_retry_all() -> AdapterConfig {
        AdapterConfig::new(dbt_yaml::Mapping::from_iter([(
            "retry_all".into(),
            true.into(),
        )]))
    }

    fn cfg_retry_on_database_errors() -> AdapterConfig {
        AdapterConfig::new(dbt_yaml::Mapping::from_iter([(
            "retry_on_database_errors".into(),
            true.into(),
        )]))
    }

    #[test]
    fn snowflake_matches_gosnowflake_client_timeout() {
        let e = adbc_err(
            Status::Internal,
            "Post \"https://acct.snowflakecomputing.com:443/session/v1/login-request\": \
             context deadline exceeded (Client.Timeout exceeded while awaiting headers)",
        );
        assert!(is_retryable_snowflake_login_error(&cfg_default(), &e));
    }

    #[test]
    fn snowflake_matches_io_status() {
        let e = adbc_err(Status::IO, "dial tcp 1.2.3.4:443: i/o timeout");
        assert!(is_retryable_snowflake_login_error(&cfg_default(), &e));
    }

    #[test]
    fn snowflake_matches_http_5xx_substring() {
        let cfg = cfg_default();
        for substr in [
            "Internal Server Error",
            "Bad Gateway",
            "Service Unavailable",
            "Gateway Timeout",
            "Request Timeout",
        ] {
            let e = adbc_err(Status::Internal, &format!("response: 5xx {substr}"));
            assert!(
                is_retryable_snowflake_login_error(&cfg, &e),
                "expected retry for substring {substr:?}"
            );
        }
    }

    #[test]
    fn snowflake_skips_real_auth_failures_by_default() {
        let e = adbc_err(
            Status::Unauthenticated,
            "Snowflake authentication failed: bad password",
        );
        assert!(!is_retryable_snowflake_login_error(&cfg_default(), &e));
        // retry_all matches Python: catches every Error class, including auth.
        assert!(is_retryable_snowflake_login_error(&cfg_retry_all(), &e));
    }

    #[test]
    fn snowflake_respects_retry_on_database_errors_flag() {
        let e = adbc_err(Status::Unknown, "some snowflake DB-level failure");
        assert!(!is_retryable_snowflake_login_error(&cfg_default(), &e));
        assert!(is_retryable_snowflake_login_error(
            &cfg_retry_on_database_errors(),
            &e
        ));
    }

    #[test]
    fn snowflake_skips_truly_unrelated_failure() {
        let e = adbc_err(Status::InvalidArguments, "bad config option");
        assert!(!is_retryable_snowflake_login_error(&cfg_default(), &e));
        assert!(!is_retryable_snowflake_login_error(
            &cfg_retry_on_database_errors(),
            &e
        ));
    }
}
