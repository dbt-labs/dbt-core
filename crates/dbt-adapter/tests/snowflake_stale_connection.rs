//! Stale-connection repro for live Snowflake. Holds a Snowflake xdbc connection
//! in a thread-local across a long idle interval, then attempts to reuse it.
//!
//! Validates the theory that dbt-fusion's per-thread + recycling-pool connection
//! reuse hands out HTTP/TCP connections silently killed by intermediate idle
//! timeouts (AWS NLB defaults to 350s) while a long-running model bottleneck
//! holds another worker thread busy. The user-visible symptom is
//! `context deadline exceeded. (Client.Timeout exceeded while awaiting headers)`
//! emitted by the gosnowflake HTTP client embedded in the ADBC Snowflake driver.
//!
//! Run:
//! ```sh
//! STALE_CONN_IDLE_SECS=1800 STALE_CONN_CLIENT_TIMEOUT_SECS=60 \
//!   caffeinate -dimsu cargo xtask test --llm --no-external-deps -p dbt-xdbc \
//!     stale_thread_local_snowflake_connection -- --ignored --nocapture
//! ```
//!
//! Auth: reads `~/.snowsql/config` directly and uses ADBC's `EXTERNAL_BROWSER`
//! flow (`from_snowsql_config()` would require a `password` field, which SSO
//! configs don't have). On first connect a browser window opens for SAML; with
//! `CLIENT_STORE_TEMP_CREDS=true` the resulting token is cached so the
//! mid-test reuse query does not re-prompt.

use std::cell::RefCell;
use std::env;
use std::time::{Duration, Instant};

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::AdbcVersion;
use arrow_array::cast::AsArray;
use dbt_xdbc::{
    Backend, Connection, Database, connection,
    database::{self, LogLevel},
    driver, snowflake,
};
use ini::ini;

const ADBC_VERSION: AdbcVersion = AdbcVersion::V110;

thread_local! {
    static CONN: RefCell<Option<Box<dyn Connection>>> = const { RefCell::new(None) };
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn snowsql_field(
    map: &std::collections::HashMap<String, std::collections::HashMap<String, Option<String>>>,
    key: &str,
) -> Result<String> {
    map.get("connections")
        .and_then(|c| c.get(key).cloned().flatten())
        .ok_or_else(|| {
            Error::with_message_and_status(
                format!("missing connections.{key} in ~/.snowsql/config"),
                Status::Internal,
            )
        })
}

fn open_database(client_timeout_secs: u64) -> Result<Box<dyn Database>> {
    let mut driver = driver::Builder::new(Backend::Snowflake, driver::LoadStrategy::CdnCache)
        .with_adbc_version(ADBC_VERSION)
        .try_load()?;

    let home = dirs::home_dir().ok_or_else(|| {
        Error::with_message_and_status("failed to get home directory", Status::Internal)
    })?;
    let config_path = home.join(".snowsql").join("config");
    let map = ini!(config_path.to_str().expect("snowsql config path"));

    let mut builder = database::Builder::new(Backend::Snowflake);
    builder.with_named_option(snowflake::ACCOUNT, snowsql_field(&map, "accountname")?)?;
    builder.with_named_option(snowflake::WAREHOUSE, snowsql_field(&map, "warehousename")?)?;
    builder.with_named_option(snowflake::ROLE, snowsql_field(&map, "rolename")?)?;
    builder.with_username(snowsql_field(&map, "username")?);

    // SSO / browser auth: AUTH_TYPE drives the flow; STORE_TEMP_CREDS caches
    // the SAML token to avoid a second browser prompt during phase 3 reuse.
    builder.with_named_option(snowflake::AUTH_TYPE, snowflake::auth_type::EXTERNAL_BROWSER)?;
    builder.with_named_option(snowflake::CLIENT_STORE_TEMP_CREDS, "true")?;

    builder.with_named_option(snowflake::LOG_TRACING, LogLevel::Warn.to_string())?;
    builder.with_named_option(snowflake::LOGIN_TIMEOUT, "60s")?;
    builder.with_named_option(snowflake::REQUEST_TIMEOUT, "600s")?;
    builder.with_named_option(snowflake::CLIENT_TIMEOUT, format!("{client_timeout_secs}s"))?;
    builder.build(&mut driver)
}

fn query_current_session(conn: &mut dyn Connection) -> Result<String> {
    let mut stmt = conn.new_statement()?;
    stmt.set_sql_query("SELECT CURRENT_SESSION()::STRING")?;
    let batch = stmt
        .execute()?
        .next()
        .expect("a record batch")
        .map_err(Error::from)?;
    Ok(batch.column(0).as_string::<i32>().value(0).to_string())
}

#[ignore = "live Snowflake; up to 30 min wallclock; opt-in via -- --ignored"]
#[tokio::test]
async fn stale_thread_local_snowflake_connection() {
    let idle_secs = env_u64("STALE_CONN_IDLE_SECS", 1800);
    let client_timeout_secs = env_u64("STALE_CONN_CLIENT_TIMEOUT_SECS", 60);
    let idle = Duration::from_secs(idle_secs);

    eprintln!(
        "[repro] idle={idle:?} client_timeout={client_timeout_secs}s \
         (set STALE_CONN_IDLE_SECS / STALE_CONN_CLIENT_TIMEOUT_SECS to override)"
    );

    // Single spawn_blocking so phase 1 (open + park) and phase 3 (reuse) run on
    // the *same* tokio blocking-pool worker — preserving the thread-local slot.
    // The closure body uses std::thread::sleep so the connection sees no traffic
    // for the whole `idle` window; this mirrors a connection sitting in dbt's
    // RECYCLING_POOL while a peer worker thread runs a long bottleneck model.
    let outcome = tokio::task::spawn_blocking(move || -> (String, Duration, Result<String>) {
        let thread_id = std::thread::current().id();

        let mut database = open_database(client_timeout_secs).expect("open database");
        let mut conn = connection::Builder::default()
            .build(&mut database)
            .expect("open connection");
        let session_id = query_current_session(&mut *conn).expect("warm-up query");
        eprintln!("[t=    0s] thread={thread_id:?} session={session_id} (parking in TLS)");
        CONN.with(|c| c.replace(Some(conn)));

        let park_start = Instant::now();
        let log_every = Duration::from_secs(60).min(idle);
        loop {
            let elapsed = park_start.elapsed();
            if elapsed >= idle {
                break;
            }
            std::thread::sleep((idle - elapsed).min(log_every));
            eprintln!("[t={:>5}s] still parked...", park_start.elapsed().as_secs());
        }

        let mut conn = CONN
            .with(|c| c.borrow_mut().take())
            .expect("connection should still be in TLS on the same blocking thread");
        let reuse_start = Instant::now();
        let result = query_current_session(&mut *conn);
        let reuse_elapsed = reuse_start.elapsed();
        eprintln!(
            "[t={:>5}s] reuse attempt finished in {reuse_elapsed:?}: {:?}",
            park_start.elapsed().as_secs(),
            result.as_ref().map(|s| s.as_str()).map_err(|e| &e.message)
        );

        (session_id, reuse_elapsed, result)
    })
    .await
    .expect("spawn_blocking join");

    let (session_id_before, reuse_elapsed, reuse_result) = outcome;

    match reuse_result {
        Ok(session_id_after) => {
            assert_eq!(
                session_id_before, session_id_after,
                "TLS handoff returned a different Snowflake session"
            );
            panic!(
                "connection survived {idle:?} idle (reuse took {reuse_elapsed:?}); \
                 theory not reproduced at this duration on this network. \
                 Try a longer STALE_CONN_IDLE_SECS or run from a network with \
                 stricter idle timeouts."
            );
        }
        Err(e) => {
            let msg = e.message;
            eprintln!(
                "FAILED AS EXPECTED:\n  session_before = {session_id_before}\n  \
                 reuse_elapsed  = {reuse_elapsed:?}\n  error          = {msg}"
            );
            assert!(
                msg.contains("context deadline exceeded")
                    || msg.contains("Client.Timeout exceeded while awaiting headers"),
                "expected gosnowflake HTTP timeout in error, got: {msg}"
            );
        }
    }
}
