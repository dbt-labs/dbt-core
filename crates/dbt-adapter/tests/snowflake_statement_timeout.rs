//! STATEMENT_TIMEOUT_IN_SECONDS cancellation repro for live Snowflake.
//!
//! Server-side cancellation is a different failure mode than the HTTP/TCP
//! staleness covered by `snowflake_stale_connection.rs`: when a query exceeds
//! `STATEMENT_TIMEOUT_IN_SECONDS`, Snowflake itself cancels the running
//! statement and returns "Statement reached its statement or warehouse timeout
//! of N second(s) and was canceled". This test:
//!
//! 1. Sets the session timeout to 30s with `ALTER SESSION`.
//! 2. Submits `CALL SYSTEM$WAIT(60)` — Snowflake should cancel at ~30s.
//! 3. Asserts the error string and that cancellation fires near 30s, not 60s.
//! 4. Reuses the same xdbc connection — does it remain healthy after a
//!    canceled statement, or does the next query fail / land in a new session?
//!    Phase 3 is the production-relevant question: dbt-fusion's recycling pool
//!    will hand a just-canceled connection to the next node with no liveness
//!    check.
//!
//! Run:
//! ```sh
//! caffeinate -dimsu cargo xtask test --no-external-deps -p dbt-xdbc \
//!   statement_timeout_cancellation -- --ignored --nocapture
//! ```
//!
//! Auth: same as `snowflake_stale_connection.rs` — reads `~/.snowsql/config`
//! and uses ADBC `EXTERNAL_BROWSER` (`CLIENT_STORE_TEMP_CREDS=true` caches the
//! SAML token across the cancellation/reuse boundary).

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

    builder.with_named_option(snowflake::AUTH_TYPE, snowflake::auth_type::EXTERNAL_BROWSER)?;
    builder.with_named_option(snowflake::CLIENT_STORE_TEMP_CREDS, "true")?;

    builder.with_named_option(snowflake::LOG_TRACING, LogLevel::Warn.to_string())?;
    builder.with_named_option(snowflake::LOGIN_TIMEOUT, "60s")?;
    builder.with_named_option(snowflake::REQUEST_TIMEOUT, "600s")?;
    builder.with_named_option(snowflake::CLIENT_TIMEOUT, format!("{client_timeout_secs}s"))?;
    builder.build(&mut driver)
}

fn run(conn: &mut dyn Connection, sql: &str) -> Result<()> {
    let mut stmt = conn.new_statement()?;
    stmt.set_sql_query(sql)?;
    for batch in stmt.execute()? {
        batch.map_err(Error::from)?;
    }
    Ok(())
}

fn query_string(conn: &mut dyn Connection, sql: &str) -> Result<String> {
    let mut stmt = conn.new_statement()?;
    stmt.set_sql_query(sql)?;
    let batch = stmt
        .execute()?
        .next()
        .expect("a record batch")
        .map_err(Error::from)?;
    Ok(batch.column(0).as_string::<i32>().value(0).to_string())
}

#[ignore = "live Snowflake; ~60s wallclock; opt-in via -- --ignored"]
#[tokio::test]
async fn statement_timeout_cancellation() {
    tokio::task::spawn_blocking(move || {
        // CLIENT_TIMEOUT generously above the 30s server-side cancellation
        // window so we can be sure any client-side timeout is incidental.
        let mut database = open_database(120).expect("open database");
        let mut conn = connection::Builder::default()
            .build(&mut database)
            .expect("open connection");

        // Phase 1: lock session statement timeout to 30s and capture session ID.
        run(&mut *conn, "ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 30")
            .expect("alter session statement_timeout");
        let session_id_before =
            query_string(&mut *conn, "SELECT CURRENT_SESSION()::STRING").expect("session id");
        eprintln!("[setup]  session={session_id_before} STATEMENT_TIMEOUT_IN_SECONDS=30");

        // Phase 2: submit a 60s wait — Snowflake should cancel at ~30s.
        let wait_started = Instant::now();
        let cancel_result = run(&mut *conn, "CALL SYSTEM$WAIT(60)");
        let wait_elapsed = wait_started.elapsed();

        match cancel_result {
            Err(e) => {
                eprintln!("[cancel] elapsed={wait_elapsed:?} err={}", e.message);
                let lower = e.message.to_lowercase();
                // Be specific: Snowflake's STATEMENT_TIMEOUT message starts
                // with "Statement reached its statement or warehouse timeout".
                // Avoid the generic "was canceled" — that matches user-cancel,
                // gateway-cancel, and other unrelated paths.
                assert!(
                    lower.contains("statement reached")
                        && (lower.contains("statement timeout")
                            || lower.contains("warehouse timeout")),
                    "expected Snowflake statement-timeout cancellation, got: {}",
                    e.message
                );
                // Sandwich the elapsed time around the configured 30s.
                // Lower bound proves SYSTEM$WAIT actually ran to the timeout
                // boundary (gosnowflake polls every ~1–5s, so the cancel
                // should surface near 30s). An early return here means we
                // were passing for the wrong reason.
                assert!(
                    wait_elapsed >= Duration::from_secs(25),
                    "cancellation fired too early ({wait_elapsed:?}); SYSTEM$WAIT \
                     did not run to the timeout boundary. err={}",
                    e.message
                );
                assert!(
                    wait_elapsed < Duration::from_secs(45),
                    "cancellation took too long ({wait_elapsed:?}); did the timeout actually fire?"
                );
            }
            Ok(()) => panic!("expected cancellation, got success after {wait_elapsed:?}"),
        }

        // Phase 3: production-relevant question. Is the connection still
        // healthy after a server-canceled statement, or has gosnowflake / ADBC
        // left it in a state that breaks the next reuse?
        let session_id_after = match query_string(&mut *conn, "SELECT CURRENT_SESSION()::STRING") {
            Ok(s) => s,
            Err(e) => panic!(
                "post-cancellation reuse FAILED — this is the bug class. err={}",
                e.message
            ),
        };
        assert_eq!(
            session_id_before, session_id_after,
            "post-cancellation reuse landed in a different session: before={session_id_before} after={session_id_after}"
        );
        eprintln!("[reuse]  same session, connection healthy after cancellation");
    })
    .await
    .expect("spawn_blocking join");
}
