//! BigQuery compatibility tests for the MetricFlow semantic query compiler.
//!
//! Two modes:
//!
//! - **Replay** (default): loads pre-recorded Arrow IPC results from
//!   `tests/data/bigquery_recordings/`.  No BigQuery connection needed.
//!   ```sh
//!   cargo test -p dbt-metricflow --test bigquery_compat
//!   ```
//!
//! - **Record** (`#[ignore]`, requires `fusion_tests/bigquery` profile):
//!   executes compiled SQL on a live BigQuery instance and writes results to
//!   Arrow IPC files so replay tests stay up to date.
//!   ```sh
//!   cargo test -p dbt-metricflow --test bigquery_compat -- --ignored --nocapture
//!   ```

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use dbt_auth::{AdapterConfig, auth_for_backend};
use dbt_metricflow::Dialect;
use dbt_profile::ProfileEnvironment;
use dbt_xdbc::{Backend, LoadStrategy, connection, driver};

const PROFILE: &str = "fusion_tests";
const TARGET: &str = "bigquery";

// ═══════════════════════════════════════════════════════════════════════════
// BigQuery connection wrapper
// ═══════════════════════════════════════════════════════════════════════════

struct BigQueryConn {
    _drv: Box<dyn dbt_xdbc::Driver>,
    _database: Box<dyn dbt_xdbc::Database>,
    conn: Box<dyn dbt_xdbc::Connection>,
    dataset: Option<String>,
}

impl Drop for BigQueryConn {
    fn drop(&mut self) {
        if let Some(dataset) = self.dataset.take() {
            eprintln!("bigquery_compat: dropping dataset {dataset}");
            let _ = self.try_execute_update(&format!("DROP SCHEMA IF EXISTS {dataset} CASCADE"));
        }
    }
}

impl BigQueryConn {
    fn connect() -> Self {
        let home = dirs::home_dir().expect("home dir exists");
        let profile_path = home.join(".dbt").join("profiles.yml");
        let penv = ProfileEnvironment::new(BTreeMap::new());
        let resolved = dbt_profile::resolve_with_env(&penv, &profile_path, PROFILE, Some(TARGET))
            .expect("failed to resolve fusion_tests/bigquery profile");

        eprintln!(
            "bigquery_compat: connecting to {} ({}) target={}",
            resolved.adapter_type, resolved.profile_name, resolved.target_name,
        );

        assert_eq!(
            resolved.adapter_type, "bigquery",
            "profile must be bigquery, got: {}",
            resolved.adapter_type
        );

        let backend = Backend::BigQuery;
        let adapter_config = AdapterConfig::new(resolved.credentials);
        let auth = auth_for_backend(backend);
        let db_builder = auth
            .configure(&adapter_config)
            .expect("auth configuration failed")
            .builder;

        let mut drv = driver::Builder::new(backend, LoadStrategy::CdnCache)
            .try_load()
            .expect("failed to load BigQuery driver");

        let mut database = db_builder
            .build(&mut drv)
            .expect("failed to open BigQuery database");

        let conn = connection::Builder::default()
            .build(&mut database)
            .expect("failed to create BigQuery connection");

        BigQueryConn {
            _drv: drv,
            _database: database,
            conn,
            dataset: None,
        }
    }

    fn set_dataset(&mut self, dataset: String) {
        self.dataset = Some(dataset);
    }

    fn try_execute_update(&mut self, sql: &str) -> Result<Option<i64>, String> {
        let mut stmt = self.conn.new_statement().map_err(|e| e.to_string())?;
        stmt.set_sql_query(sql).map_err(|e| e.to_string())?;
        stmt.execute_update().map_err(|e| e.to_string())
    }

    fn execute_update(&mut self, sql: &str) {
        let mut stmt = self
            .conn
            .new_statement()
            .expect("failed to create statement");
        stmt.set_sql_query(sql).expect("failed to set SQL");
        stmt.execute_update().unwrap_or_else(|e| {
            panic!("DDL/DML failed: {e}\n  SQL: {}", &sql[..sql.len().min(200)])
        });
    }

    fn execute_query(&mut self, sql: &str) -> Vec<RecordBatch> {
        let mut stmt = self
            .conn
            .new_statement()
            .expect("failed to create statement");
        stmt.set_sql_query(sql).expect("failed to set SQL");
        let reader = stmt
            .execute()
            .unwrap_or_else(|e| panic!("query failed: {e}\n  SQL: {}", &sql[..sql.len().min(200)]));
        reader
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("failed to read results")
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Arrow IPC recording
// ═══════════════════════════════════════════════════════════════════════════

fn recordings_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join("bigquery_recordings")
}

fn write_recording(name: &str, batches: &[RecordBatch]) {
    let dir = recordings_dir();
    fs::create_dir_all(&dir).expect("failed to create recordings dir");
    let path = dir.join(format!("{name}.arrow"));
    let schema = if let Some(b) = batches.first() {
        b.schema()
    } else {
        return;
    };
    let file = fs::File::create(&path).expect("failed to create recording file");
    let mut writer = StreamWriter::try_new(file, &schema).expect("failed to create IPC writer");
    for batch in batches {
        writer.write(batch).expect("failed to write batch");
    }
    writer.finish().expect("failed to finish IPC stream");
}

fn read_recording(name: &str) -> Option<Vec<RecordBatch>> {
    let path = recordings_dir().join(format!("{name}.arrow"));
    if !path.exists() {
        return None;
    }
    let data = fs::read(&path).expect("failed to read recording");
    let reader = StreamReader::try_new(Cursor::new(data), None).expect("failed to open IPC stream");
    Some(
        reader
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("failed to read batches"),
    )
}

// ═══════════════════════════════════════════════════════════════════════════
// Record test — live BigQuery, writes Arrow IPC recordings
// ═══════════════════════════════════════════════════════════════════════════

#[test]
#[ignore = "requires fusion_tests/bigquery profile in ~/.dbt/profiles.yml"]
fn bigquery_compat_record() {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let dataset = format!("mf_test_{ts}");
    eprintln!("bigquery_compat [record]: using dataset {dataset}");

    let mut bq = BigQueryConn::connect();
    bq.execute_update(&format!("CREATE SCHEMA IF NOT EXISTS {dataset}"));
    bq.set_dataset(dataset.clone());
    eprintln!("bigquery_compat [record]: dataset {dataset} created");

    for stmt in common::bigquery_data_ddl(&dataset) {
        bq.execute_update(&stmt);
    }
    eprintln!("bigquery_compat [record]: test data loaded");

    let database = "dbt-test-env";

    let mut store = common::setup_metric_store(database, &dataset);

    common::run_scorecard(
        &mut store,
        Dialect::BigQuery,
        "BigQuery",
        &mut |name, sql| match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            bq.execute_query(sql)
        })) {
            Ok(batches) => {
                write_recording(name, &batches);
                Ok(Some(batches))
            }
            Err(_) => Err(format!("execution panicked\n  SQL: {sql}")),
        },
    );

    eprintln!(
        "bigquery_compat [record]: recordings written to {:?}",
        recordings_dir()
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Replay test — reads Arrow IPC recordings, no BigQuery needed
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn bigquery_compat_replay() {
    let dir = recordings_dir();
    if !dir.exists() || fs::read_dir(&dir).map_or(true, |mut d| d.next().is_none()) {
        eprintln!(
            "bigquery_compat [replay]: no recordings at {dir:?} — \
             run bigquery_compat_record to generate them"
        );
        return;
    }

    let mut store = common::setup_metric_store("RECORDED_DB", "RECORDED_DATASET");

    common::run_scorecard(
        &mut store,
        Dialect::BigQuery,
        "BigQuery",
        &mut |name, _sql| Ok(read_recording(name)),
    );
}
