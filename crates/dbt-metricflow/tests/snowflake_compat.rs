//! Snowflake compatibility tests for the MetricFlow semantic query compiler.
//!
//! Two modes:
//!
//! - **Replay** (default): loads pre-recorded Arrow IPC results from
//!   `tests/data/snowflake_recordings/`.  No Snowflake connection needed.
//!   ```sh
//!   cargo test -p dbt-metricflow --test snowflake_compat
//!   ```
//!
//! - **Record** (`#[ignore]`, requires `fusion_tests/snowflake` profile):
//!   executes compiled SQL on a live Snowflake cluster and writes results to
//!   Arrow IPC files so replay tests stay up to date.
//!   ```sh
//!   cargo test -p dbt-metricflow --test snowflake_compat -- --ignored --nocapture
//!   ```

use std::collections::BTreeMap;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use dbt_auth::{AdapterConfig, auth_for_backend};
use dbt_profile::ProfileEnvironment;
use dbt_xdbc::{Backend, LoadStrategy, connection, driver};

use dbt_metricflow::{Dialect, InMemoryMetricStore, compile, parse_query_spec};
use serde_json::json;

// ═══════════════════════════════════════════════════════════════════════════
// Lightweight Arrow IPC recorder
// FIXME: Replace with the shared record/replay crate once it lands.
// ═══════════════════════════════════════════════════════════════════════════

fn recordings_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join("snowflake_recordings")
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
    let data = fs::read(&path).expect("failed to read recording file");
    let reader = StreamReader::try_new(Cursor::new(data), None).expect("failed to open IPC stream");
    let batches: Vec<RecordBatch> = reader
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("failed to read IPC batches");
    Some(batches)
}

// ═══════════════════════════════════════════════════════════════════════════
// Snowflake connection helper
// ═══════════════════════════════════════════════════════════════════════════

const PROFILE: &str = "fusion_tests";
const TARGET: &str = "snowflake";

struct SnowflakeConn {
    _drv: Box<dyn dbt_xdbc::Driver>,
    _database: Box<dyn dbt_xdbc::Database>,
    conn: Box<dyn dbt_xdbc::Connection>,
    schema: Option<String>,
}

impl Drop for SnowflakeConn {
    fn drop(&mut self) {
        if let Some(schema) = self.schema.take() {
            eprintln!("snowflake_compat: dropping schema {schema}");
            let _ = self.try_execute_update(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"));
        }
    }
}

impl SnowflakeConn {
    fn connect() -> Self {
        let home = dirs::home_dir().expect("home dir exists");
        let profile_path = home.join(".dbt").join("profiles.yml");
        let penv = ProfileEnvironment::new(BTreeMap::new());
        let resolved = dbt_profile::resolve_with_env(&penv, &profile_path, PROFILE, Some(TARGET))
            .expect("failed to resolve fusion_tests/snowflake profile");

        eprintln!(
            "snowflake_compat: connecting to {} ({}) target={}",
            resolved.adapter_type, resolved.profile_name, resolved.target_name,
        );

        assert_eq!(
            resolved.adapter_type, "snowflake",
            "profile must be snowflake, got: {}",
            resolved.adapter_type
        );

        let backend = Backend::Snowflake;
        let adapter_config = AdapterConfig::new(resolved.credentials);
        let auth = auth_for_backend(backend);
        let db_builder = auth
            .configure(&adapter_config)
            .expect("auth configuration failed")
            .builder;

        let mut drv = driver::Builder::new(backend, LoadStrategy::CdnCache)
            .try_load()
            .expect("failed to load Snowflake driver");

        let mut database = db_builder
            .build(&mut drv)
            .expect("failed to open Snowflake database");

        let conn = connection::Builder::default()
            .build(&mut database)
            .expect("failed to create Snowflake connection");

        SnowflakeConn {
            _drv: drv,
            _database: database,
            conn,
            schema: None,
        }
    }

    fn set_schema(&mut self, schema: String) {
        self.schema = Some(schema);
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
        stmt.execute_update()
            .unwrap_or_else(|e| panic!("SQL failed: {e}\n  SQL: {}", &sql[..sql.len().min(200)]));
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
// Manifest builder — reuses the same structure as metricflow_compat.rs
// but with Snowflake-qualified relation names
// ═══════════════════════════════════════════════════════════════════════════

#[allow(clippy::too_many_arguments)]
fn sm(
    name: &str,
    alias: &str,
    database: &str,
    schema: &str,
    primary_entity: Option<&str>,
    default_agg_time_dim: Option<&str>,
    measures: serde_json::Value,
    dimensions: serde_json::Value,
    entities: serde_json::Value,
) -> serde_json::Value {
    let relation_name = format!("\"{database}\".\"{schema}\".\"{alias}\"");
    json!({
        "name": name,
        "description": name,
        "node_relation": {
            "alias": alias,
            "schema_name": schema,
            "database": database,
            "relation_name": relation_name,
        },
        "defaults": default_agg_time_dim.map(|d| json!({"agg_time_dimension": d})),
        "primary_entity": primary_entity,
        "entities": entities,
        "measures": measures,
        "dimensions": dimensions,
        "label": null,
        "metadata": null,
        "config": null,
    })
}

// ── Measure / dimension / entity / metric helpers ────────────────────────
// (Identical to metricflow_compat.rs — copied for test isolation.)

fn measure(name: &str, agg: &str, expr: Option<&str>) -> serde_json::Value {
    json!({
        "name": name, "agg": agg, "expr": expr,
        "description": null, "label": null, "create_metric": false,
        "agg_time_dimension": null, "agg_params": null,
        "non_additive_dimension": null, "config": null,
    })
}

fn measure_non_additive(
    name: &str,
    agg: &str,
    expr: Option<&str>,
    nad_name: &str,
    window_choice: &str,
    window_groupings: Vec<&str>,
) -> serde_json::Value {
    let mut m = measure(name, agg, expr);
    m["non_additive_dimension"] = json!({
        "name": nad_name, "window_choice": window_choice, "window_groupings": window_groupings,
    });
    m
}

fn dim_categorical(name: &str, expr: Option<&str>) -> serde_json::Value {
    json!({
        "name": name, "type": "categorical", "description": null, "label": null,
        "expr": expr, "is_partition": false, "type_params": null,
        "metadata": null, "config": null,
    })
}

fn dim_time(name: &str, expr: Option<&str>, granularity: &str) -> serde_json::Value {
    json!({
        "name": name, "type": "time", "description": null, "label": null,
        "expr": expr, "is_partition": false,
        "type_params": { "time_granularity": granularity, "validity_params": null },
        "metadata": null, "config": null,
    })
}

fn dim_time_partition(name: &str, granularity: &str) -> serde_json::Value {
    json!({
        "name": name, "type": "time", "description": null, "label": null,
        "expr": null, "is_partition": true,
        "type_params": { "time_granularity": granularity, "validity_params": null },
        "metadata": null, "config": null,
    })
}

fn entity(name: &str, etype: &str, expr: &str) -> serde_json::Value {
    json!({
        "name": name, "type": etype, "description": null, "label": null,
        "role": null, "expr": expr, "config": null, "metadata": null,
    })
}

fn simple_metric(
    name: &str,
    _measure_name: &str,
    model: &str,
    agg: &str,
    expr: &str,
    agg_time_dim: &str,
) -> serde_json::Value {
    json!({
        "name": name, "description": name, "type": "simple",
        "label": null, "time_granularity": null, "filter": null,
        "metadata": null, "config": null,
        "type_params": {
            "measure": null, "input_measures": [], "numerator": null, "denominator": null,
            "expr": expr, "window": null, "grain_to_date": null, "metrics": null,
            "conversion_type_params": null, "cumulative_type_params": null,
            "join_to_timespine": false, "fill_nulls_with": null, "is_private": false,
            "metric_aggregation_params": {
                "semantic_model": model, "agg": agg, "agg_params": null,
                "agg_time_dimension": agg_time_dim, "non_additive_dimension": null, "expr": expr,
            },
        },
    })
}

fn simple_metric_with_filter(
    name: &str,
    measure_name: &str,
    model: &str,
    agg: &str,
    expr: &str,
    agg_time_dim: &str,
    filter: serde_json::Value,
) -> serde_json::Value {
    let mut m = simple_metric(name, measure_name, model, agg, expr, agg_time_dim);
    m["filter"] = filter;
    m
}

fn derived_metric(name: &str, expr: &str, metrics: Vec<serde_json::Value>) -> serde_json::Value {
    json!({
        "name": name, "description": name, "type": "derived",
        "label": null, "time_granularity": null, "filter": null,
        "metadata": null, "config": null,
        "type_params": {
            "measure": null, "input_measures": [], "numerator": null, "denominator": null,
            "expr": expr, "window": null, "grain_to_date": null, "metrics": metrics,
            "conversion_type_params": null, "cumulative_type_params": null,
            "join_to_timespine": false, "fill_nulls_with": null, "is_private": false,
            "metric_aggregation_params": null,
        },
    })
}

fn ratio_metric(name: &str, numerator: &str, denominator: &str) -> serde_json::Value {
    json!({
        "name": name, "description": name, "type": "ratio",
        "label": null, "time_granularity": null, "filter": null,
        "metadata": null, "config": null,
        "type_params": {
            "measure": null, "input_measures": [],
            "numerator": { "name": numerator, "filter": null, "alias": null, "offset_window": null, "offset_to_grain": null },
            "denominator": { "name": denominator, "filter": null, "alias": null, "offset_window": null, "offset_to_grain": null },
            "expr": null, "window": null, "grain_to_date": null, "metrics": null,
            "conversion_type_params": null, "cumulative_type_params": null,
            "join_to_timespine": false, "fill_nulls_with": null, "is_private": false,
            "metric_aggregation_params": null,
        },
    })
}

fn cumulative_metric(
    name: &str,
    model: &str,
    agg: &str,
    expr: &str,
    agg_time_dim: &str,
    window: Option<serde_json::Value>,
    grain_to_date: Option<&str>,
) -> serde_json::Value {
    json!({
        "name": name, "description": name, "type": "cumulative",
        "label": null, "time_granularity": null, "filter": null,
        "metadata": null, "config": null,
        "type_params": {
            "measure": null, "input_measures": [], "numerator": null, "denominator": null,
            "expr": null, "window": null, "grain_to_date": null, "metrics": null,
            "conversion_type_params": null,
            "cumulative_type_params": {
                "window": window, "grain_to_date": grain_to_date,
            },
            "join_to_timespine": false, "fill_nulls_with": null, "is_private": false,
            "metric_aggregation_params": {
                "semantic_model": model, "agg": agg, "agg_params": null,
                "agg_time_dimension": agg_time_dim, "non_additive_dimension": null, "expr": expr,
            },
        },
    })
}

fn conversion_metric(
    name: &str,
    base_metric: &str,
    conversion_metric_name: &str,
    entity: &str,
    calc: &str,
    window: Option<serde_json::Value>,
) -> serde_json::Value {
    json!({
        "name": name, "description": name, "type": "conversion",
        "label": null, "time_granularity": null, "filter": null,
        "metadata": null, "config": null,
        "type_params": {
            "measure": null, "input_measures": [], "numerator": null, "denominator": null,
            "expr": null, "window": null, "grain_to_date": null, "metrics": null,
            "conversion_type_params": {
                "base_measure": { "name": base_metric, "filter": null, "alias": null, "offset_window": null, "offset_to_grain": null },
                "conversion_measure": { "name": conversion_metric_name, "filter": null, "alias": null, "offset_window": null, "offset_to_grain": null },
                "entity": entity, "calculation": calc, "window": window,
                "constant_properties": null,
            },
            "cumulative_type_params": null,
            "join_to_timespine": false, "fill_nulls_with": null, "is_private": false,
            "metric_aggregation_params": null,
        },
    })
}

fn metric_input(name: &str) -> serde_json::Value {
    json!({ "name": name, "filter": null, "alias": null, "offset_window": null, "offset_to_grain": null })
}

fn metric_input_alias(name: &str, alias: &str) -> serde_json::Value {
    json!({ "name": name, "filter": null, "alias": alias, "offset_window": null, "offset_to_grain": null })
}

fn metric_input_offset_window(name: &str, alias: &str, offset_window: &str) -> serde_json::Value {
    json!({
        "name": name, "filter": null, "alias": alias,
        "offset_window": offset_window, "offset_to_grain": null,
    })
}

fn metric_input_offset_to_grain(
    name: &str,
    alias: &str,
    offset_to_grain: &str,
) -> serde_json::Value {
    json!({
        "name": name, "filter": null, "alias": alias,
        "offset_window": null, "offset_to_grain": offset_to_grain,
    })
}

fn measure_with_agg_time(
    name: &str,
    agg: &str,
    expr: Option<&str>,
    agg_time_dim: &str,
) -> serde_json::Value {
    let mut m = measure(name, agg, expr);
    m["agg_time_dimension"] = json!(agg_time_dim);
    m
}

fn metric_input_with_filter(name: &str, alias: &str, filter: &str) -> serde_json::Value {
    json!({
        "name": name,
        "filter": { "where_filters": [{"where_sql_template": filter}] },
        "alias": alias,
        "offset_window": null,
        "offset_to_grain": null,
    })
}

fn simple_metric_filtered(
    name: &str,
    model: &str,
    agg: &str,
    expr: &str,
    agg_time_dim: &str,
    filter: &str,
) -> serde_json::Value {
    let mut m = simple_metric(name, name, model, agg, expr, agg_time_dim);
    m["filter"] = json!({"where_filters": [{"where_sql_template": filter}]});
    m
}

fn ratio_metric_with_filter(
    name: &str,
    numerator: &str,
    denominator: &str,
    filter: &str,
) -> serde_json::Value {
    let mut m = ratio_metric(name, numerator, denominator);
    m["filter"] = json!({"where_filters": [{"where_sql_template": filter}]});
    m
}

fn simple_metric_with_spine(
    name: &str,
    model: &str,
    agg: &str,
    expr: &str,
    agg_time_dim: &str,
    join_to_timespine: bool,
    fill_nulls_with: Option<i64>,
) -> serde_json::Value {
    json!({
        "name": name, "description": name, "type": "simple",
        "label": null, "time_granularity": null, "filter": null,
        "metadata": null, "config": null,
        "type_params": {
            "measure": null, "input_measures": [], "numerator": null, "denominator": null,
            "expr": expr, "window": null, "grain_to_date": null, "metrics": null,
            "conversion_type_params": null, "cumulative_type_params": null,
            "join_to_timespine": join_to_timespine, "fill_nulls_with": fill_nulls_with,
            "is_private": false,
            "metric_aggregation_params": {
                "semantic_model": model, "agg": agg, "agg_params": null,
                "agg_time_dimension": agg_time_dim, "non_additive_dimension": null, "expr": expr,
            },
        },
    })
}

fn conversion_metric_with_const_props(
    name: &str,
    base_measure: &str,
    conversion_measure: &str,
    entity: &str,
    calculation: &str,
    window: Option<serde_json::Value>,
    constant_properties: Vec<(&str, &str)>,
) -> serde_json::Value {
    let props: Vec<serde_json::Value> = constant_properties
        .into_iter()
        .map(|(base, conv)| json!({"base_property": base, "conversion_property": conv}))
        .collect();
    json!({
        "name": name, "description": name, "type": "conversion",
        "label": null, "time_granularity": null, "filter": null,
        "metadata": null, "config": null,
        "type_params": {
            "measure": null, "input_measures": [], "numerator": null, "denominator": null,
            "expr": null, "window": null, "grain_to_date": null, "metrics": null,
            "conversion_type_params": {
                "base_measure": { "name": base_measure },
                "conversion_measure": { "name": conversion_measure },
                "entity": entity, "calculation": calculation, "window": window,
                "constant_properties": props,
            },
            "cumulative_type_params": null,
            "join_to_timespine": false, "fill_nulls_with": null, "is_private": false,
            "metric_aggregation_params": null,
        },
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// Manifest + DDL builders (parameterized by database + schema)
// ═══════════════════════════════════════════════════════════════════════════

fn build_semantic_manifest(database: &str, schema: &str) -> serde_json::Value {
    let bookings_source = sm(
        "bookings_source",
        "FCT_BOOKINGS",
        database,
        schema,
        Some("booking"),
        Some("ds"),
        json!([
            measure("bookings", "sum", Some("1")),
            measure("instant_bookings", "sum_boolean", Some("is_instant")),
            measure("booking_value", "sum", Some("booking_value")),
            measure("max_booking_value", "max", Some("booking_value")),
            measure("min_booking_value", "min", Some("booking_value")),
            measure("bookers", "count_distinct", Some("guest_id")),
            measure("average_booking_value", "average", Some("booking_value")),
            measure_with_agg_time("booking_payments", "sum", Some("booking_value"), "paid_at"),
            measure("referred_bookings", "count", Some("referrer_id")),
            measure("median_booking_value", "median", Some("booking_value")),
        ]),
        json!([
            dim_time("ds", None, "day"),
            dim_time("paid_at", None, "day"),
            dim_time_partition("ds_partitioned", "day"),
            dim_categorical("is_instant", Some("is_instant")),
        ]),
        json!([
            entity("booking", "primary", "1"),
            entity("listing", "foreign", "listing_id"),
            entity("guest", "foreign", "guest_id"),
            entity("host", "foreign", "host_id"),
        ]),
    );

    let listings_latest = sm(
        "listings_latest",
        "DIM_LISTINGS_LATEST",
        database,
        schema,
        None,
        Some("ds"),
        json!([
            measure("listings", "sum", Some("1")),
            measure("largest_listing", "max", Some("capacity")),
            measure("smallest_listing", "min", Some("capacity")),
        ]),
        json!([
            dim_time("ds", Some("created_at"), "day"),
            dim_time("created_at", None, "day"),
            dim_categorical("country_latest", Some("country")),
            dim_categorical("is_lux_latest", Some("is_lux")),
            dim_categorical("capacity_latest", Some("capacity")),
        ]),
        json!([
            entity("listing", "primary", "listing_id"),
            entity("user", "foreign", "user_id"),
        ]),
    );

    let views_source = sm(
        "views_source",
        "FCT_VIEWS",
        database,
        schema,
        Some("view"),
        Some("ds"),
        json!([measure("views", "sum", Some("1")),]),
        json!([
            dim_time("ds", None, "day"),
            dim_time_partition("ds_partitioned", "day"),
        ]),
        json!([
            entity("listing", "foreign", "listing_id"),
            entity("user", "foreign", "user_id"),
        ]),
    );

    let users_latest = sm(
        "users_latest",
        "DIM_USERS_LATEST",
        database,
        schema,
        None,
        None,
        json!([]),
        json!([
            dim_time("ds_latest", Some("ds"), "day"),
            dim_categorical("home_state_latest", None),
        ]),
        json!([entity("user", "primary", "user_id"),]),
    );

    let users_ds_source = sm(
        "users_ds_source",
        "DIM_USERS",
        database,
        schema,
        None,
        Some("created_at"),
        json!([
            measure("new_users", "sum", Some("1")),
            measure_with_agg_time("archived_users", "sum", Some("1"), "archived_at"),
        ]),
        json!([
            dim_time("ds", None, "day"),
            dim_time("created_at", None, "day"),
            dim_time_partition("ds_partitioned", "day"),
            dim_categorical("home_state", None),
            dim_time("archived_at", None, "hour"),
            dim_time("last_profile_edit_ts", None, "millisecond"),
            dim_time("last_login_ts", None, "minute"),
        ]),
        json!([entity("user", "primary", "user_id"),]),
    );

    let revenue = sm(
        "revenue",
        "FCT_REVENUE",
        database,
        schema,
        Some("revenue_instance"),
        Some("ds"),
        json!([measure("txn_revenue", "sum", Some("revenue")),]),
        json!([dim_time("ds", Some("created_at"), "day"),]),
        json!([entity("user", "foreign", "user_id"),]),
    );

    let accounts_source = sm(
        "accounts_source",
        "FCT_ACCOUNTS",
        database,
        schema,
        Some("account"),
        Some("ds"),
        json!([
            measure("account_balance", "sum", None),
            measure_non_additive(
                "total_account_balance_first_day",
                "sum",
                Some("account_balance"),
                "ds",
                "min",
                vec![]
            ),
            measure_non_additive(
                "current_account_balance_by_user",
                "sum",
                Some("account_balance"),
                "ds",
                "max",
                vec!["user"]
            ),
        ]),
        json!([
            dim_time("ds", None, "day"),
            dim_time("ds_month", Some("ds_month"), "month"),
            dim_categorical("account_type", None),
        ]),
        json!([entity("user", "foreign", "user_id"),]),
    );

    let id_verifications = sm(
        "id_verifications",
        "FCT_ID_VERIFICATIONS",
        database,
        schema,
        None,
        Some("ds"),
        json!([measure("identity_verifications", "sum", Some("1")),]),
        json!([
            dim_time("ds", None, "day"),
            dim_time_partition("ds_partitioned", "day"),
            dim_categorical("verification_type", None),
        ]),
        json!([
            entity("verification", "primary", "verification_id"),
            entity("user", "foreign", "user_id"),
        ]),
    );

    let visits_source = sm(
        "visits_source",
        "FCT_VISITS",
        database,
        schema,
        Some("visit"),
        Some("ds"),
        json!([measure("visits", "count", Some("1")),]),
        json!([
            dim_time("ds", None, "day"),
            dim_categorical("referrer_id", None),
        ]),
        json!([
            entity("user", "foreign", "user_id"),
            entity("session", "foreign", "session_id"),
        ]),
    );

    let buys_source = sm(
        "buys_source",
        "FCT_BUYS",
        database,
        schema,
        Some("buy"),
        Some("ds"),
        json!([measure("buys", "count", Some("1")),]),
        json!([dim_time("ds", None, "day"),]),
        json!([entity("user", "foreign", "user_id"),]),
    );

    let companies = sm(
        "companies",
        "DIM_COMPANIES",
        database,
        schema,
        None,
        None,
        json!([]),
        json!([dim_categorical("company_name", None),]),
        json!([
            entity("company", "primary", "company_id"),
            entity("user", "unique", "user_id"),
        ]),
    );

    let lux_listing_mapping = sm(
        "lux_listing_mapping",
        "DIM_LUX_LISTING_ID_MAPPING",
        database,
        schema,
        None,
        None,
        json!([]),
        json!([]),
        json!([
            entity("listing", "primary", "listing_id"),
            entity("lux_listing", "foreign", "lux_listing_id"),
        ]),
    );

    let company_regions = sm(
        "company_regions",
        "DIM_COMPANY_REGIONS",
        database,
        schema,
        None,
        None,
        json!([]),
        json!([dim_categorical("region_name", None),]),
        json!([
            entity("region", "primary", "region_id"),
            entity("company", "foreign", "company_id"),
        ]),
    );

    let conversions_source = sm(
        "conversions_source",
        "FCT_CONVERSIONS",
        database,
        schema,
        Some("converted_account"),
        Some("created_at"),
        json!([measure("conversions", "sum", Some("1")),]),
        json!([
            dim_time("created_at", None, "day"),
            dim_categorical("channel", None),
        ]),
        json!([]),
    );

    let signups_source = sm(
        "signups_source",
        "FCT_SIGNUPS",
        database,
        schema,
        Some("signup_account"),
        Some("created_at"),
        json!([measure("signups", "sum", Some("1")),]),
        json!([
            dim_time("created_at", None, "day"),
            dim_categorical("source", None),
        ]),
        json!([]),
    );

    // Semi-additive metrics
    let mut semi_additive_first_day = simple_metric(
        "total_account_balance_first_day",
        "total_account_balance_first_day",
        "accounts_source",
        "sum",
        "account_balance",
        "ds",
    );
    semi_additive_first_day["type_params"]["metric_aggregation_params"]["non_additive_dimension"] = json!({
        "name": "ds", "window_choice": "min", "window_groupings": []
    });
    let mut semi_additive_current_by_user = simple_metric(
        "current_account_balance_by_user",
        "current_account_balance_by_user",
        "accounts_source",
        "sum",
        "account_balance",
        "ds",
    );
    semi_additive_current_by_user["type_params"]["metric_aggregation_params"]["non_additive_dimension"] = json!({
        "name": "ds", "window_choice": "max", "window_groupings": ["user"]
    });

    // Cumulative metric with fill_nulls (needs mutation, so create before json! macro)
    let mut cumul_fill_nulls = cumulative_metric(
        "every_two_days_bookers_fill_nulls_with_0",
        "bookings_source",
        "count_distinct",
        "guest_id",
        "ds",
        Some(json!({"count": 2, "granularity": "day"})),
        None,
    );
    cumul_fill_nulls["type_params"]["join_to_timespine"] = json!(true);
    cumul_fill_nulls["type_params"]["fill_nulls_with"] = json!(0);

    let metrics = json!([
        // Simple metrics from bookings_source
        simple_metric("bookings", "bookings", "bookings_source", "sum", "1", "ds"),
        simple_metric(
            "instant_bookings",
            "instant_bookings",
            "bookings_source",
            "sum_boolean",
            "is_instant",
            "ds"
        ),
        simple_metric(
            "booking_value",
            "booking_value",
            "bookings_source",
            "sum",
            "booking_value",
            "ds"
        ),
        simple_metric(
            "max_booking_value",
            "max_booking_value",
            "bookings_source",
            "max",
            "booking_value",
            "ds"
        ),
        simple_metric(
            "min_booking_value",
            "min_booking_value",
            "bookings_source",
            "min",
            "booking_value",
            "ds"
        ),
        simple_metric(
            "bookers",
            "bookers",
            "bookings_source",
            "count_distinct",
            "guest_id",
            "ds"
        ),
        simple_metric(
            "average_booking_value",
            "average_booking_value",
            "bookings_source",
            "average",
            "booking_value",
            "ds"
        ),
        simple_metric(
            "referred_bookings",
            "referred_bookings",
            "bookings_source",
            "count",
            "referrer_id",
            "ds"
        ),
        // Simple metrics with filters
        simple_metric_with_filter(
            "instant_booking_value",
            "booking_value",
            "bookings_source",
            "sum",
            "booking_value",
            "ds",
            json!({"where_filters": [{"where_sql_template": "{{ Dimension('booking__is_instant') }}"}]}),
        ),
        // Simple metrics from other models
        simple_metric("views", "views", "views_source", "sum", "1", "ds"),
        simple_metric("listings", "listings", "listings_latest", "sum", "1", "ds"),
        simple_metric(
            "largest_listing",
            "largest_listing",
            "listings_latest",
            "max",
            "capacity",
            "ds"
        ),
        simple_metric(
            "smallest_listing",
            "smallest_listing",
            "listings_latest",
            "min",
            "capacity",
            "ds"
        ),
        simple_metric(
            "identity_verifications",
            "identity_verifications",
            "id_verifications",
            "sum",
            "1",
            "ds"
        ),
        simple_metric("revenue", "txn_revenue", "revenue", "sum", "revenue", "ds"),
        simple_metric(
            "account_balance",
            "account_balance",
            "accounts_source",
            "sum",
            "account_balance",
            "ds"
        ),
        simple_metric(
            "booking_payments",
            "booking_payments",
            "bookings_source",
            "sum",
            "booking_value",
            "paid_at"
        ),
        simple_metric(
            "new_users",
            "new_users",
            "users_ds_source",
            "sum",
            "1",
            "created_at"
        ),
        simple_metric_with_filter(
            "bookings_where_listing_high_value",
            "bookings",
            "bookings_source",
            "sum",
            "1",
            "ds",
            json!({"where_filters": [{"where_sql_template": "{{ Metric('booking_value', ['listing']) }} > 1000"}]}),
        ),
        // Derived metrics
        derived_metric(
            "booking_fees",
            "booking_value * 0.05",
            vec![metric_input("booking_value")]
        ),
        derived_metric(
            "booking_fees_per_booker",
            "booking_value * 0.05 / bookers",
            vec![metric_input("booking_value"), metric_input("bookers")]
        ),
        derived_metric(
            "views_times_booking_value",
            "booking_value * views",
            vec![metric_input("booking_value"), metric_input("views")]
        ),
        derived_metric(
            "non_referred_bookings_pct",
            "(bookings - ref_bookings) * 1.0 / bookings",
            vec![
                metric_input_alias("referred_bookings", "ref_bookings"),
                metric_input("bookings")
            ]
        ),
        derived_metric(
            "booking_value_sub_instant",
            "booking_value - instant_booking_value",
            vec![
                metric_input("instant_booking_value"),
                metric_input("booking_value")
            ]
        ),
        derived_metric(
            "booking_value_sub_instant_add_10",
            "booking_value_sub_instant + 10",
            vec![metric_input("booking_value_sub_instant")]
        ),
        // Offset window derived metrics
        derived_metric(
            "bookings_growth_1_day",
            "bookings - bookings_1_day_ago",
            vec![
                metric_input("bookings"),
                metric_input_offset_window("bookings", "bookings_1_day_ago", "1 day")
            ]
        ),
        // Offset to grain derived metrics
        derived_metric(
            "bookings_growth_since_start_of_month",
            "bookings - bookings_at_start_of_month",
            vec![
                metric_input("bookings"),
                metric_input_offset_to_grain("bookings", "bookings_at_start_of_month", "month")
            ]
        ),
        // Ratio metrics
        ratio_metric("bookings_per_booker", "bookings", "bookers"),
        ratio_metric("bookings_per_view", "bookings", "views"),
        ratio_metric("bookings_per_listing", "bookings", "listings"),
        ratio_metric("bookings_per_dollar", "bookings", "booking_value"),
        // Cumulative metrics
        cumulative_metric(
            "trailing_2_months_revenue",
            "revenue",
            "sum",
            "revenue",
            "ds",
            Some(json!({"count": 2, "granularity": "month"})),
            None
        ),
        cumulative_metric(
            "revenue_all_time",
            "revenue",
            "sum",
            "revenue",
            "ds",
            None,
            None
        ),
        cumulative_metric(
            "revenue_mtd",
            "revenue",
            "sum",
            "revenue",
            "ds",
            None,
            Some("month")
        ),
        cumulative_metric(
            "cumulative_bookings",
            "bookings_source",
            "sum",
            "1",
            "ds",
            None,
            None
        ),
        cumulative_metric(
            "every_2_days_bookers",
            "bookings_source",
            "count_distinct",
            "guest_id",
            "ds",
            Some(json!({"count": 2, "granularity": "day"})),
            None
        ),
        // Time spine / fill_nulls metrics
        simple_metric_with_spine(
            "bookings_fill_nulls_no_spine",
            "bookings_source",
            "sum",
            "1",
            "ds",
            false,
            Some(0)
        ),
        simple_metric_with_spine(
            "bookings_join_to_time_spine",
            "bookings_source",
            "sum",
            "1",
            "ds",
            true,
            None
        ),
        simple_metric_with_spine(
            "bookings_fill_nulls_with_spine",
            "bookings_source",
            "sum",
            "1",
            "ds",
            true,
            Some(0)
        ),
        simple_metric_with_spine(
            "revenue_fill_nulls_with_spine",
            "revenue",
            "sum",
            "revenue",
            "ds",
            true,
            Some(0)
        ),
        // Base measures as simple metrics (needed for conversion metric CTEs)
        simple_metric("visits", "visits", "visits_source", "count", "1", "ds"),
        simple_metric("buys", "buys", "buys_source", "count", "1", "ds"),
        // Conversion metrics
        conversion_metric(
            "visit_buy_conversion_rate_7days",
            "visits",
            "buys",
            "user",
            "conversion_rate",
            Some(json!({"count": 7, "granularity": "day"}))
        ),
        conversion_metric(
            "visit_buy_conversions_7days",
            "visits",
            "buys",
            "user",
            "conversions",
            Some(json!({"count": 7, "granularity": "day"}))
        ),
        conversion_metric(
            "visit_buy_conversion_rate_unbounded",
            "visits",
            "buys",
            "user",
            "conversion_rate",
            None
        ),
        conversion_metric_with_const_props(
            "visit_buy_conversion_rate_by_session",
            "visits",
            "buys",
            "user",
            "conversion_rate",
            Some(json!({"count": 7, "granularity": "day"})),
            vec![("session_id", "session_id")],
        ),
        // Semi-additive metrics
        semi_additive_first_day,
        semi_additive_current_by_user,
        // Derived metrics with shared alias names
        derived_metric(
            "derived_shared_alias_1a",
            "shared_alias - 10",
            vec![metric_input_alias("bookings", "shared_alias")]
        ),
        derived_metric(
            "derived_shared_alias_1b",
            "shared_alias - 100",
            vec![metric_input_alias("bookings", "shared_alias")]
        ),
        derived_metric(
            "derived_shared_alias_2",
            "shared_alias + 10",
            vec![metric_input_alias("instant_bookings", "shared_alias")]
        ),
        // ── Additional metrics for single-metric test porting ────────
        simple_metric_filtered(
            "booking_value_for_non_null_listing_id",
            "bookings_source",
            "sum",
            "booking_value",
            "ds",
            "{{ Entity('listing') }} IS NOT NULL",
        ),
        simple_metric_filtered(
            "lux_listings",
            "listings_latest",
            "count",
            "1",
            "ds",
            "{{ Dimension('listing__is_lux_latest') }}",
        ),
        derived_metric(
            "booking_value_per_view",
            "booking_value / NULLIF(views, 0)",
            vec![metric_input("booking_value"), metric_input("views")]
        ),
        derived_metric(
            "bookings_growth_2_weeks",
            "bookings - bookings_2_weeks_ago",
            vec![
                metric_input("bookings"),
                metric_input_offset_window("bookings", "bookings_2_weeks_ago", "14 days"),
            ]
        ),
        derived_metric(
            "bookings_5_day_lag",
            "bookings_5_days_ago",
            vec![metric_input_offset_window(
                "bookings",
                "bookings_5_days_ago",
                "5 days"
            ),]
        ),
        derived_metric(
            "bookings_month_start_compared_to_1_month_prior",
            "month_start_bookings - bookings_1_month_ago",
            vec![
                metric_input_offset_to_grain("bookings", "month_start_bookings", "month"),
                metric_input_offset_window("bookings", "bookings_1_month_ago", "1 month"),
            ]
        ),
        derived_metric(
            "instant_plus_non_referred_bookings_pct",
            "non_referred + (instant * 1.0 / bookings)",
            vec![
                metric_input_alias("non_referred_bookings_pct", "non_referred"),
                metric_input_alias("instant_bookings", "instant"),
                metric_input("bookings"),
            ]
        ),
        derived_metric(
            "bookings_per_lux_listing_derived",
            "bookings * 1.0 / NULLIF(lux_listing, 0)",
            vec![
                metric_input("bookings"),
                metric_input_with_filter(
                    "listings",
                    "lux_listing",
                    "{{ Dimension('listing__is_lux_latest') }}"
                ),
            ]
        ),
        derived_metric(
            "every_2_days_bookers_2_days_ago",
            "every_2_days_bookers_2_days_ago",
            vec![metric_input_offset_window(
                "every_2_days_bookers",
                "every_2_days_bookers_2_days_ago",
                "2 days"
            ),]
        ),
        simple_metric_with_spine(
            "bookings_fill_nulls_with_0",
            "bookings_source",
            "sum",
            "1",
            "ds",
            true,
            Some(0),
        ),
        simple_metric_with_spine(
            "bookings_fill_nulls_with_0_without_time_spine",
            "bookings_source",
            "sum",
            "1",
            "ds",
            false,
            Some(0),
        ),
        cumul_fill_nulls,
        derived_metric(
            "bookings_growth_2_weeks_fill_nulls_with_0_for_non_offset",
            "bookings_fill_nulls_with_0 - bookings_2_weeks_ago",
            vec![
                metric_input("bookings_fill_nulls_with_0"),
                metric_input_offset_window("bookings", "bookings_2_weeks_ago", "14 days"),
            ]
        ),
        derived_metric(
            "bookings_offset_once",
            "bookings_1_day_ago",
            vec![metric_input_offset_window(
                "bookings",
                "bookings_1_day_ago",
                "5 days"
            ),]
        ),
        derived_metric(
            "bookings_offset_twice",
            "2 * bookings_offset_once",
            vec![metric_input_offset_window(
                "bookings_offset_once",
                "bookings_offset_once",
                "2 days"
            ),]
        ),
        derived_metric(
            "booking_fees_since_start_of_month",
            "booking_fees - booking_fees_start_of_month",
            vec![
                metric_input_offset_to_grain(
                    "booking_fees",
                    "booking_fees_start_of_month",
                    "month"
                ),
                metric_input("booking_fees"),
            ]
        ),
        simple_metric_with_filter(
            "active_listings",
            "listings",
            "listings_latest",
            "count",
            "1",
            "ds",
            json!({"where_filters": [{"where_sql_template": "{{ Metric('bookings', ['listing']) }} > 2"}]}),
        ),
        ratio_metric_with_filter(
            "popular_listing_bookings_per_booker",
            "listings",
            "listings",
            "{{ Metric('views', ['listing']) }} > 10",
        ),
        // Metrics for bare dimension name collision test
        simple_metric(
            "account_conversions",
            "conversions",
            "conversions_source",
            "sum",
            "1",
            "created_at"
        ),
        simple_metric(
            "account_signups",
            "signups",
            "signups_source",
            "sum",
            "1",
            "created_at"
        ),
        // ── Subdaily metrics ─────────────────────────────────────────
        simple_metric(
            "archived_users",
            "archived_users",
            "users_ds_source",
            "sum",
            "1",
            "archived_at"
        ),
        simple_metric_with_spine(
            "subdaily_join_to_time_spine_metric",
            "users_ds_source",
            "sum",
            "1",
            "archived_at",
            true,
            None,
        ),
        derived_metric(
            "subdaily_offset_window_metric",
            "archived_users - archived_users_1_day_ago",
            vec![
                metric_input("archived_users"),
                metric_input_offset_window("archived_users", "archived_users_1_day_ago", "1 day"),
            ]
        ),
        derived_metric(
            "subdaily_offset_grain_to_date_metric",
            "archived_users - archived_users_at_start_of_month",
            vec![
                metric_input("archived_users"),
                metric_input_offset_to_grain(
                    "archived_users",
                    "archived_users_at_start_of_month",
                    "month"
                ),
            ]
        ),
        // ── 3-input derived metric (COALESCE join test) ─────────────
        derived_metric(
            "bookings_plus_views_plus_listings",
            "bookings + views + listings",
            vec![
                metric_input("bookings"),
                metric_input("views"),
                metric_input("listings"),
            ]
        ),
        // ── fill_nulls_with in derived expression test ──────────────
        derived_metric(
            "fill_nulls_derived_test",
            "bookings_fill_nulls_with_0 + views",
            vec![
                metric_input("bookings_fill_nulls_with_0"),
                metric_input("views"),
            ]
        ),
    ]);

    let project_configuration = json!({
        "time_spine_table_configurations": [],
        "metadata": null,
        "dsi_package_version": { "major_version": "0", "minor_version": "0", "patch_version": "0" },
        "time_spines": [
            {
                "node_relation": { "alias": "MF_TIME_SPINE", "schema_name": schema, "database": database, "relation_name": null },
                "primary_column": { "name": "ds", "time_granularity": "day" },
                "custom_granularities": [],
            },
        ],
    });

    let saved_queries = json!([
        {
            "name": "p0_booking",
            "description": "Booking-related metrics.",
            "label": null, "metadata": null, "tags": [],
            "query_params": {
                "metrics": ["bookings", "instant_bookings"],
                "group_by": ["TimeDimension('metric_time', 'day')", "Dimension('listing__capacity_latest')"],
                "where": ["{{ Dimension('listing__capacity_latest') }} > 3"],
                "order_by": [], "limit": null,
            },
            "exports": [],
        },
    ]);

    json!({
        "semantic_models": [
            bookings_source, listings_latest, views_source, users_latest,
            users_ds_source, revenue, accounts_source, id_verifications,
            visits_source, buys_source, companies, company_regions,
            lux_listing_mapping, conversions_source, signups_source,
        ],
        "metrics": metrics,
        "project_configuration": project_configuration,
        "saved_queries": saved_queries,
    })
}

/// DDL statements to create and populate all MetricFlow test tables in Snowflake.
/// All table names are schema-qualified.
fn snowflake_data_ddl(schema: &str) -> Vec<String> {
    let mut stmts = Vec::new();

    stmts.push(format!(
        "CREATE TABLE {schema}.fct_bookings (
        ds DATE, ds_partitioned DATE, paid_at DATE,
        guest_id VARCHAR, host_id VARCHAR, listing_id VARCHAR,
        booking_value DOUBLE, is_instant BOOLEAN, referrer_id VARCHAR
    )"
    ));

    stmts.push(format!("INSERT INTO {schema}.fct_bookings VALUES
        ('2019-12-01','2019-12-01','2019-12-01','u0003452','u0004114','l5948301',951.23,false,'u0003141'),
        ('2019-12-18','2019-12-18','2019-12-19','u0004114','u0003141','l3141592',797.42,false,null),
        ('2019-12-18','2019-12-18','2019-12-19','u0004114','u0003141','l3141592',808.14,true,'u0003141'),
        ('2019-12-18','2019-12-18','2019-12-19','u0003141','u0003154','l2718281',531.99,true,null),
        ('2019-12-18','2019-12-18','2019-12-19','u0003154','u0003141','l2718281',285.89,false,null),
        ('2019-12-18','2019-12-18','2019-12-19','u0004114','u0003141','l3141592',714.85,true,'u0005432'),
        ('2019-12-18','2019-12-18','2019-12-19','u0004114','u0003141','l3141592',352.42,false,null),
        ('2019-12-18','2019-12-18','2019-12-19','u0004114','u0003141','l3141592',444.14,true,'u0003141'),
        ('2019-12-18','2019-12-18','2019-12-19','u0003141','u0003154','l2718281',415.99,true,null),
        ('2019-12-18','2019-12-18','2019-12-19','u0003154','u0003141','l2718281',578.89,false,null),
        ('2019-12-18','2019-12-18','2019-12-19','u0004114','u0003141','l3141592',768.85,true,'u0005432'),
        ('2019-12-19','2019-12-19','2019-12-20','u0004114','u0003141','l2718281',368.42,true,null),
        ('2019-12-19','2019-12-19','2019-12-20','u0004114','u0003141','l2718281',521.14,true,null),
        ('2019-12-19','2019-12-19','2019-12-20','u0004114','u0003141','l3141592',581.99,false,null),
        ('2019-12-19','2019-12-19','2019-12-20','u0005432','u0003452','l2718281',753.85,true,'u0003452'),
        ('2019-12-19','2019-12-19','2019-12-20','u0004114','u1004114','l9658588-incomplete',0.0,false,null),
        ('2019-12-19','2019-12-19','2019-12-20','u0004114','u1004114','l8912456-incomplete',0.0,false,null),
        ('2019-12-19','2019-12-19','2019-12-20','u0003452','u0005432','l3141592',754.89,false,'u0003141'),
        ('2019-12-19','2019-12-19','2019-12-20','u0003452','u0004114','l5948301',152.23,false,'u0003141'),
        ('2019-12-19','2019-12-19','2019-12-20','u1003452','u1004114','no_such_listing',0.0,false,null),
        ('2019-12-19','2019-12-19','2019-12-20','u0004114','u0003141','l2718281',532.42,true,null),
        ('2019-12-19','2019-12-19','2019-12-20','u0004114','u0003141','l2718281',258.14,true,null),
        ('2019-12-19','2019-12-19','2019-12-20','u0004114','u0003141','l3141592',753.99,false,null),
        ('2019-12-19','2019-12-19','2019-12-20','u0005432','u0003452','l2718281',852.85,true,'u0003452'),
        ('2019-12-19','2019-12-19','2019-12-20','u0004114','u1004114','l9658588-incomplete',0.0,false,null),
        ('2019-12-19','2019-12-19','2019-12-20','u0004114','u1004114','l8912456-incomplete',0.0,false,null),
        ('2019-12-19','2019-12-19','2019-12-20','u0003452','u0005432','l3141592',728.89,false,'u0003141'),
        ('2019-12-19','2019-12-19','2019-12-20','u0003452','u0004114','l5948301',951.23,false,'u0003141')
    "));

    stmts.push(format!(
        "CREATE TABLE {schema}.dim_listings_latest (
        created_at DATE, listing_id VARCHAR, country VARCHAR,
        capacity INTEGER, is_lux BOOLEAN, user_id VARCHAR
    )"
    ));
    stmts.push(format!(
        "INSERT INTO {schema}.dim_listings_latest VALUES
        ('2020-01-01','l3141592','us',3,true,'u0004114'),
        ('2020-01-02','l5948301','us',5,true,'u0004114'),
        ('2020-01-02','l2718281','cote d''ivoire',4,false,'u0005432'),
        ('2020-01-02','l9658588-incomplete','us',null,null,'u1004114'),
        ('2020-01-02','l8912456-incomplete',null,null,null,'u1004114'),
        ('2020-01-02','l7891283-incomplete','ca',null,false,'u1004114')
    "
    ));

    stmts.push(format!(
        "CREATE TABLE {schema}.fct_views (
        ds DATE, ds_partitioned DATE, listing_id VARCHAR, user_id VARCHAR
    )"
    ));
    stmts.push(format!(
        "INSERT INTO {schema}.fct_views VALUES
        ('2019-12-01','2019-12-01','l3141592','u0004114'),
        ('2019-12-01','2019-12-01','l2718281','u0005432'),
        ('2019-12-01','2019-12-01','l5948301','u0003452'),
        ('2019-12-19','2019-12-19','l3141592','u0004114'),
        ('2019-12-19','2019-12-19','l2718281','u0005432'),
        ('2019-12-19','2019-12-19','l3141592','u0003452'),
        ('2019-12-19','2019-12-19','l5948301','u0003141'),
        ('2019-12-19','2019-12-19','l5948301','u0004114'),
        ('2019-12-19','2019-12-19','l2718281','u0003141'),
        ('2019-12-19','2019-12-19','l2718281','u0003452'),
        ('2019-12-19','2019-12-19','l2718281','u0004114'),
        ('2019-12-20','2019-12-20','l3141592','u0003452'),
        ('2019-12-20','2019-12-20','l5948301','u0003141'),
        ('2019-12-20','2019-12-20','l2718281','u0005432')
    "
    ));

    stmts.push(format!(
        "CREATE TABLE {schema}.dim_users_latest (ds DATE, user_id VARCHAR, home_state_latest VARCHAR)"
    ));
    stmts.push(format!(
        "INSERT INTO {schema}.dim_users_latest VALUES
        ('2020-01-01','u0003141','MD'),('2020-01-01','u0003154','CA'),
        ('2020-01-01','u0003452','HI'),('2020-01-01','u0004114','NY'),
        ('2020-01-01','u0005432','WA'),('2020-01-01','u1004114','TX'),
        ('2020-01-01','u1003452','CA')
    "
    ));

    stmts.push(format!(
        "CREATE TABLE {schema}.dim_users (
        ds DATE, ds_partitioned DATE, created_at DATE, user_id VARCHAR,
        home_state VARCHAR, last_profile_edit_ts TIMESTAMP_NTZ,
        bio_added_ts TIMESTAMP_NTZ, last_login_ts TIMESTAMP_NTZ, archived_at TIMESTAMP_NTZ
    )"
    ));
    stmts.push(format!(
        "INSERT INTO {schema}.dim_users VALUES
        ('2019-12-19','2019-12-19','2019-12-19','u0003452','HI','2019-12-19 10:30:00','2019-12-19 09:00:00','2019-12-19 08:15:00','2019-12-19 10:00:00'),
        ('2019-12-19','2019-12-19','2019-12-19','u0004114','NY','2019-12-19 11:00:00','2019-12-19 10:00:00','2019-12-19 09:30:00','2019-12-19 10:00:00'),
        ('2019-12-19','2019-12-19','2019-12-19','u0003141','MD','2019-12-19 12:30:00','2019-12-19 11:00:00','2019-12-19 10:45:00','2019-12-19 11:00:00'),
        ('2019-12-19','2019-12-19','2019-12-19','u0003154','CA','2019-12-19 14:00:00','2019-12-19 13:00:00','2019-12-19 12:00:00','2019-12-19 11:00:00'),
        ('2019-12-19','2019-12-19','2019-12-19','u0005432','WA','2019-12-19 15:30:00','2019-12-19 14:00:00','2019-12-19 13:15:00','2019-12-19 12:00:00'),
        ('2019-12-19','2019-12-19','2019-12-19','u1004114','TX','2019-12-19 16:00:00','2019-12-19 15:00:00','2019-12-19 14:30:00','2019-12-19 12:00:00'),
        ('2019-12-19','2019-12-19','2019-12-19','u1003452','CA','2019-12-19 17:30:00','2019-12-19 16:00:00','2019-12-19 15:45:00','2019-12-19 13:00:00')
    "
    ));

    stmts.push(format!(
        "CREATE TABLE {schema}.fct_revenue (created_at DATE, user_id VARCHAR, revenue DOUBLE)"
    ));
    stmts.push(format!(
        "INSERT INTO {schema}.fct_revenue VALUES
        ('2019-12-19','u0004114',100.0),('2019-12-19','u0003141',200.0),
        ('2019-12-20','u0004114',150.0),('2019-12-20','u0003452',50.0),
        ('2020-01-01','u0004114',300.0),('2020-01-02','u0005432',75.0)
    "
    ));

    stmts.push(format!(
        "CREATE TABLE {schema}.fct_accounts (
        ds DATE, user_id VARCHAR, account_balance DOUBLE,
        account_type VARCHAR, ds_month DATE
    )"
    ));
    stmts.push(format!(
        "INSERT INTO {schema}.fct_accounts VALUES
        ('2019-12-19','u0003141',1000.0,'checking','2019-12-01'),
        ('2019-12-19','u0004114',2000.0,'savings','2019-12-01'),
        ('2019-12-20','u0003141',1100.0,'checking','2019-12-01'),
        ('2019-12-20','u0004114',1800.0,'savings','2019-12-01'),
        ('2020-01-01','u0003141',1200.0,'checking','2020-01-01'),
        ('2020-01-01','u0004114',1900.0,'savings','2020-01-01')
    "
    ));

    stmts.push(format!(
        "CREATE TABLE {schema}.fct_id_verifications (
        ds DATE, ds_partitioned DATE, verification_id VARCHAR,
        user_id VARCHAR, verification_type VARCHAR
    )"
    ));
    stmts.push(format!(
        "INSERT INTO {schema}.fct_id_verifications VALUES
        ('2019-12-19','2019-12-19','v001','u0003141','phone'),
        ('2019-12-19','2019-12-19','v002','u0004114','id_card'),
        ('2019-12-20','2019-12-20','v003','u0003452','phone'),
        ('2020-01-01','2020-01-01','v004','u0005432','passport')
    "
    ));

    stmts.push(format!(
        "CREATE TABLE {schema}.fct_visits (ds DATE, user_id VARCHAR, session_id VARCHAR, referrer_id VARCHAR)"
    ));
    stmts.push(format!(
        "INSERT INTO {schema}.fct_visits VALUES
        ('2019-12-19','u0003141','s001','fb_ad_1'),
        ('2019-12-19','u0004114','s002','google'),
        ('2019-12-19','u0003452','s003',null),
        ('2019-12-20','u0003141','s004','fb_ad_1'),
        ('2019-12-20','u0005432','s005','email')
    "
    ));

    stmts.push(format!(
        "CREATE TABLE {schema}.fct_buys (ds DATE, user_id VARCHAR, session_id VARCHAR, ds_month DATE)"
    ));
    stmts.push(format!(
        "INSERT INTO {schema}.fct_buys VALUES
        ('2019-12-19','u0003141','s001','2019-12-01'),
        ('2019-12-20','u0003141','s004','2019-12-01'),
        ('2019-12-20','u0005432','s005','2019-12-01')
    "
    ));

    stmts.push(format!(
        "CREATE TABLE {schema}.dim_companies (company_id VARCHAR, user_id VARCHAR, company_name VARCHAR)"
    ));
    stmts.push(format!(
        "INSERT INTO {schema}.dim_companies VALUES ('c001','u0003141','Acme Corp'),('c002','u0004114','Globex Inc')"
    ));

    stmts.push(format!(
        "CREATE TABLE {schema}.dim_lux_listing_id_mapping (listing_id VARCHAR, lux_listing_id VARCHAR)"
    ));
    stmts.push(format!(
        "INSERT INTO {schema}.dim_lux_listing_id_mapping VALUES ('l3141592','lux_001'),('l5948301','lux_002')"
    ));

    stmts.push(format!(
        "CREATE TABLE {schema}.dim_company_regions (region_id VARCHAR, company_id VARCHAR, region_name VARCHAR)"
    ));
    stmts.push(format!(
        "INSERT INTO {schema}.dim_company_regions VALUES ('r001','c001','Northeast'),('r002','c002','West Coast')"
    ));

    stmts.push(format!(
        "CREATE TABLE {schema}.fct_conversions (created_at DATE, channel VARCHAR)"
    ));
    stmts.push(format!(
        "INSERT INTO {schema}.fct_conversions VALUES ('2024-01-01','organic'),('2024-01-02','paid')"
    ));

    stmts.push(format!(
        "CREATE TABLE {schema}.fct_signups (created_at DATE, source VARCHAR)"
    ));
    stmts.push(format!(
        "INSERT INTO {schema}.fct_signups VALUES ('2024-01-01','web'),('2024-01-02','mobile')"
    ));

    stmts
}

// ═══════════════════════════════════════════════════════════════════════════
// Test cases (same as metricflow_compat.rs)
// ═══════════════════════════════════════════════════════════════════════════

struct TestCase {
    name: &'static str,
    spec: &'static str,
    min_rows: usize,
    expected_value: Option<f64>,
}

const TEST_CASES: &[TestCase] = &[
    // ── Simple metrics ───────────────────────────────────────────────
    TestCase {
        name: "simple_bookings_no_dimensions",
        spec: r#"{"metrics": ["bookings"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "simple_bookings_by_day",
        spec: r#"{"metrics": ["bookings"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "simple_bookings_by_listing",
        spec: r#"{"metrics": ["bookings"], "group_by": ["Entity('listing')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "simple_booking_value",
        spec: r#"{"metrics": ["booking_value"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "simple_max_booking_value",
        spec: r#"{"metrics": ["max_booking_value"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "simple_min_booking_value",
        spec: r#"{"metrics": ["min_booking_value"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "simple_bookers_count_distinct",
        spec: r#"{"metrics": ["bookers"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "simple_average_booking_value",
        spec: r#"{"metrics": ["average_booking_value"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "simple_referred_bookings_count",
        spec: r#"{"metrics": ["referred_bookings"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "simple_views",
        spec: r#"{"metrics": ["views"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "simple_listings",
        spec: r#"{"metrics": ["listings"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "simple_identity_verifications",
        spec: r#"{"metrics": ["identity_verifications"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "simple_revenue",
        spec: r#"{"metrics": ["revenue"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "simple_account_balance",
        spec: r#"{"metrics": ["account_balance"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── Granularity variations ───────────────────────────────────────
    TestCase {
        name: "bookings_by_week",
        spec: r#"{"metrics": ["bookings"], "group_by": ["TimeDimension('metric_time', 'week')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "bookings_by_month",
        spec: r#"{"metrics": ["bookings"], "group_by": ["TimeDimension('metric_time', 'month')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "bookings_by_year",
        spec: r#"{"metrics": ["bookings"], "group_by": ["TimeDimension('metric_time', 'year')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── Cross-model dimension joins ──────────────────────────────────
    TestCase {
        name: "bookings_by_listing_country",
        spec: r#"{"metrics": ["bookings"], "group_by": ["Dimension('listing__country_latest')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "bookings_by_listing_capacity",
        spec: r#"{"metrics": ["bookings"], "group_by": ["Dimension('listing__capacity_latest')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "revenue_by_user_home_state",
        spec: r#"{"metrics": ["revenue"], "group_by": ["Dimension('user__home_state_latest')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── Multiple simple metrics (same model) ─────────────────────────
    TestCase {
        name: "bookings_and_booking_value",
        spec: r#"{"metrics": ["bookings", "booking_value"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "bookings_and_instant_bookings",
        spec: r#"{"metrics": ["bookings", "instant_bookings"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── Multiple simple metrics (different models) ───────────────────
    TestCase {
        name: "bookings_and_views_no_dims",
        spec: r#"{"metrics": ["bookings", "views"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "bookings_and_views_by_day",
        spec: r#"{"metrics": ["bookings", "views"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "bookings_and_listings_by_listing",
        spec: r#"{"metrics": ["bookings", "listings"], "group_by": ["Entity('listing')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── Derived metrics ──────────────────────────────────────────────
    TestCase {
        name: "derived_booking_fees",
        spec: r#"{"metrics": ["booking_fees"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "derived_booking_fees_by_day",
        spec: r#"{"metrics": ["booking_fees"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "derived_booking_fees_per_booker",
        spec: r#"{"metrics": ["booking_fees_per_booker"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "derived_views_times_booking_value",
        spec: r#"{"metrics": ["views_times_booking_value"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "derived_non_referred_bookings_pct",
        spec: r#"{"metrics": ["non_referred_bookings_pct"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "nested_derived_booking_value_sub_instant_add_10",
        spec: r#"{"metrics": ["booking_value_sub_instant_add_10"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── Offset derived metrics ──────────────────────────────────────
    TestCase {
        name: "offset_window_bookings_growth_by_day",
        spec: r#"{"metrics": ["bookings_growth_1_day"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "offset_to_grain_bookings_growth_by_day",
        spec: r#"{"metrics": ["bookings_growth_since_start_of_month"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── Ratio metrics ────────────────────────────────────────────────
    TestCase {
        name: "ratio_bookings_per_booker",
        spec: r#"{"metrics": ["bookings_per_booker"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "ratio_bookings_per_view",
        spec: r#"{"metrics": ["bookings_per_view"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "ratio_bookings_per_listing",
        spec: r#"{"metrics": ["bookings_per_listing"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "ratio_bookings_per_dollar",
        spec: r#"{"metrics": ["bookings_per_dollar"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "ratio_bookings_per_booker_by_day",
        spec: r#"{"metrics": ["bookings_per_booker"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── Where filters ────────────────────────────────────────────────
    TestCase {
        name: "bookings_with_where_filter",
        spec: r#"{"metrics": ["bookings"], "where": ["{{ Dimension('booking__is_instant') }}"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "bookings_where_listing_country",
        spec: r#"{"metrics": ["bookings"], "where": ["{{ Dimension('listing__country_latest') }} = 'us'"], "group_by": ["Dimension('booking__is_instant')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── Order by + limit ─────────────────────────────────────────────
    TestCase {
        name: "bookings_order_by_metric_time",
        spec: r#"{"metrics": ["bookings"], "group_by": ["TimeDimension('metric_time', 'day')"], "order_by": ["+metric_time"], "limit": 5}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── Metric with filter ───────────────────────────────────────────
    TestCase {
        name: "metric_with_filter_instant_booking_value",
        spec: r#"{"metrics": ["instant_booking_value"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "metric_filter_bookings_by_listing_value",
        spec: r#"{"metrics": ["bookings_where_listing_high_value"]}"#,
        min_rows: 1,
        expected_value: Some(23.0),
    },
    // ── Cumulative metrics ──────────────────────────────────────────
    TestCase {
        name: "cumulative_revenue_all_time",
        spec: r#"{"metrics": ["revenue_all_time"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "cumulative_trailing_2_months_revenue",
        spec: r#"{"metrics": ["trailing_2_months_revenue"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "cumulative_revenue_mtd",
        spec: r#"{"metrics": ["revenue_mtd"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "cumulative_bookings_all_time",
        spec: r#"{"metrics": ["cumulative_bookings"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "cumulative_every_2_days_bookers",
        spec: r#"{"metrics": ["every_2_days_bookers"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "cumulative_revenue_all_time_no_time_dim",
        spec: r#"{"metrics": ["revenue_all_time"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "cumulative_trailing_2_months_by_user",
        spec: r#"{"metrics": ["trailing_2_months_revenue"], "group_by": ["TimeDimension('metric_time', 'day')", "Dimension('user__home_state_latest')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── Time spine / fill_nulls metrics ─────────────────────────────
    TestCase {
        name: "fill_nulls_no_spine_by_day",
        spec: r#"{"metrics": ["bookings_fill_nulls_no_spine"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "join_to_time_spine_by_day",
        spec: r#"{"metrics": ["bookings_join_to_time_spine"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "fill_nulls_with_spine_by_day",
        spec: r#"{"metrics": ["bookings_fill_nulls_with_spine"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "fill_nulls_with_spine_by_month",
        spec: r#"{"metrics": ["bookings_fill_nulls_with_spine"], "group_by": ["TimeDimension('metric_time', 'month')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "revenue_fill_nulls_with_spine_by_day",
        spec: r#"{"metrics": ["revenue_fill_nulls_with_spine"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "join_to_time_spine_no_group_by",
        spec: r#"{"metrics": ["bookings_join_to_time_spine"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── Semi-additive measures ──────────────────────────────────────
    TestCase {
        name: "semi_additive_first_day_balance",
        spec: r#"{"metrics": ["total_account_balance_first_day"]}"#,
        min_rows: 1,
        expected_value: Some(3000.0),
    },
    TestCase {
        name: "semi_additive_current_balance_by_user",
        spec: r#"{"metrics": ["current_account_balance_by_user"]}"#,
        min_rows: 1,
        expected_value: Some(3100.0),
    },
    TestCase {
        name: "semi_additive_current_balance_by_user_grouped",
        spec: r#"{"metrics": ["current_account_balance_by_user"], "group_by": ["Entity('user')"]}"#,
        min_rows: 2,
        expected_value: None,
    },
    TestCase {
        name: "semi_additive_first_day_by_account_type",
        spec: r#"{"metrics": ["total_account_balance_first_day"], "group_by": ["Dimension('account__account_type')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "semi_additive_current_balance_with_where",
        spec: r#"{"metrics": ["current_account_balance_by_user"], "where": ["{{ Dimension('account__account_type') }} = 'savings'"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── Multi-hop joins ─────────────────────────────────────────────
    TestCase {
        name: "multi_hop_bookings_by_company",
        spec: r#"{"metrics": ["bookings"], "group_by": ["Dimension('company__company_name')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "multi_hop_bookings_by_lux_listing",
        spec: r#"{"metrics": ["bookings"], "group_by": ["Entity('lux_listing')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "multi_hop_revenue_by_company",
        spec: r#"{"metrics": ["revenue"], "group_by": ["Dimension('company__company_name')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── Conversion metrics ─────────────────────────────────────────
    TestCase {
        name: "conversion_rate_7day_window",
        spec: r#"{"metrics": ["visit_buy_conversion_rate_7days"]}"#,
        min_rows: 1,
        expected_value: Some(0.6),
    },
    TestCase {
        name: "conversion_count_7day_window",
        spec: r#"{"metrics": ["visit_buy_conversions_7days"]}"#,
        min_rows: 1,
        expected_value: Some(3.0),
    },
    TestCase {
        name: "conversion_rate_unbounded",
        spec: r#"{"metrics": ["visit_buy_conversion_rate_unbounded"]}"#,
        min_rows: 1,
        expected_value: Some(0.6),
    },
    TestCase {
        name: "conversion_rate_with_constant_property",
        spec: r#"{"metrics": ["visit_buy_conversion_rate_by_session"]}"#,
        min_rows: 1,
        expected_value: Some(0.6),
    },
    // ── WHERE filter with TimeDimension Jinja syntax ────────────────
    TestCase {
        name: "where_time_dimension_jinja",
        spec: r#"{"metrics": ["bookings"], "group_by": ["TimeDimension('metric_time', 'day')"], "where": ["{{ TimeDimension('metric_time', 'day') }} >= '2019-12-19'"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── WHERE filter with time range (two conditions) ───────────────
    TestCase {
        name: "where_time_range",
        spec: r#"{"metrics": ["bookings"], "group_by": ["TimeDimension('metric_time', 'day')"], "where": ["{{ TimeDimension('metric_time', 'day') }} >= '2019-12-18'", "{{ TimeDimension('metric_time', 'day') }} <= '2019-12-19'"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── WHERE filter combined with cross-model join ─────────────────
    TestCase {
        name: "where_combined_time_and_dimension",
        spec: r#"{"metrics": ["bookings"], "group_by": ["Dimension('listing__country_latest')"], "where": ["{{ TimeDimension('metric_time', 'day') }} >= '2019-12-19'", "{{ Dimension('listing__country_latest') }} = 'us'"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── Saved query execution ───────────────────────────────────────
    TestCase {
        name: "saved_query_p0_booking",
        spec: r#"{"metrics": ["bookings", "instant_bookings"], "group_by": ["TimeDimension('metric_time', 'day')", "Dimension('listing__capacity_latest')"], "where": ["{{ Dimension('listing__capacity_latest') }} > 3"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── Multiple dimension joins from same model ──────────────────────
    TestCase {
        name: "bookings_by_listing_country_and_capacity",
        spec: r#"{"metrics": ["bookings"], "group_by": ["Dimension('listing__country_latest')", "Dimension('listing__capacity_latest')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "revenue_by_user_state_and_day",
        spec: r#"{"metrics": ["revenue"], "group_by": ["Dimension('user__home_state_latest')", "TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── Order-by descending + multi-column ──────────────────────────
    TestCase {
        name: "bookings_order_by_desc",
        spec: r#"{"metrics": ["bookings"], "group_by": ["TimeDimension('metric_time', 'day')"], "order_by": ["-bookings"], "limit": 5}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "bookings_order_by_multi_column",
        spec: r#"{"metrics": ["bookings", "booking_value"], "group_by": ["TimeDimension('metric_time', 'day')"], "order_by": ["-bookings", "+metric_time"], "limit": 10}"#,
        min_rows: 1,
        expected_value: None,
    },
    // NOTE: conversion metrics with group-by are not yet supported
    // (the compiler emits a scalar subquery regardless of group_by).
    // ── Cumulative with non-day granularity ─────────────────────────
    TestCase {
        name: "cumulative_revenue_all_time_by_month",
        spec: r#"{"metrics": ["revenue_all_time"], "group_by": ["TimeDimension('metric_time', 'month')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── Metric filter + group-by combination ────────────────────────
    TestCase {
        name: "metric_filter_instant_by_day",
        spec: r#"{"metrics": ["instant_booking_value"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── Quarter granularity ─────────────────────────────────────────
    TestCase {
        name: "bookings_by_quarter",
        spec: r#"{"metrics": ["bookings"], "group_by": ["TimeDimension('metric_time', 'quarter')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── Double-underscore granularity syntax (JSON notation) ────────
    TestCase {
        name: "bookings_double_underscore_day",
        spec: r#"{"metrics": ["bookings"], "group_by": ["metric_time__day"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── Fill nulls with spine + where filter ────────────────────────
    TestCase {
        name: "fill_nulls_spine_with_where",
        spec: r#"{"metrics": ["bookings_fill_nulls_with_spine"], "group_by": ["TimeDimension('metric_time', 'day')"], "where": ["{{ TimeDimension('metric_time', 'day') }} >= '2019-12-18'"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── Cumulative + where filter ───────────────────────────────────
    TestCase {
        name: "cumulative_revenue_with_where",
        spec: r#"{"metrics": ["revenue_all_time"], "group_by": ["TimeDimension('metric_time', 'day')"], "where": ["{{ TimeDimension('metric_time', 'day') }} >= '2019-12-19'"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── Ratio metric + group-by dimension ───────────────────────────
    TestCase {
        name: "ratio_bookings_per_booker_by_listing_country",
        spec: r#"{"metrics": ["bookings_per_booker"], "group_by": ["Dimension('listing__country_latest')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── Derived metric + where filter ───────────────────────────────
    TestCase {
        name: "derived_booking_fees_with_where",
        spec: r#"{"metrics": ["booking_fees"], "group_by": ["TimeDimension('metric_time', 'day')"], "where": ["{{ Dimension('booking__is_instant') }}"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ── Multi-model join (PK→FK direction) ─────────────────────────
    // listings is the primary model (listing = primary entity), bookings
    // is the target (listing = foreign entity). Before the bidirectional
    // edge fix this produced a CROSS JOIN.
    TestCase {
        name: "listings_and_bookings_pk_fk_join",
        spec: r#"{"metrics": ["listings", "bookings"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "listings_and_bookings_by_listing",
        spec: r#"{"metrics": ["listings", "bookings"], "group_by": ["Entity('listing')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ═══════════════════════════════════════════════════════════════════
    // Multi-metric: constrained + non-constrained (same model)
    // ═══════════════════════════════════════════════════════════════════
    TestCase {
        name: "constrained_with_non_constrained_same_src",
        spec: r#"{"metrics": ["booking_value", "instant_booking_value"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "constrained_with_non_constrained_diff_src",
        spec: r#"{"metrics": ["instant_booking_value", "views"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "ratio_with_non_ratio",
        spec: r#"{"metrics": ["bookings", "bookings_per_view"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "query_with_3_metrics",
        spec: r#"{"metrics": ["bookings", "bookings_per_booker", "booking_value"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "metrics_with_different_agg_time_dims",
        spec: r#"{"metrics": ["booking_value", "booking_payments"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "shared_alias_derived_same",
        spec: r#"{"metrics": ["derived_shared_alias_1a", "derived_shared_alias_1b"], "group_by": ["Dimension('booking__is_instant')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "shared_alias_derived_different",
        spec: r#"{"metrics": ["derived_shared_alias_1a", "derived_shared_alias_2"], "group_by": ["Dimension('booking__is_instant')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "two_metrics_null_dim_values",
        spec: r#"{"metrics": ["bookings", "views"], "group_by": ["TimeDimension('metric_time', 'day')", "Dimension('listing__is_lux_latest')", "Dimension('listing__country_latest')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "three_metrics_null_dim_values",
        spec: r#"{"metrics": ["bookings", "views", "listings"], "group_by": ["Dimension('listing__is_lux_latest')", "Dimension('listing__country_latest')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "multi_metrics_no_dims",
        spec: r#"{"metrics": ["bookings", "listings"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "multi_metrics_ordered_by_metric",
        spec: r#"{"metrics": ["bookings", "booking_value"], "group_by": ["TimeDimension('metric_time', 'day')"], "order_by": ["-booking_value"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "multi_metrics_ordered_by_dimension",
        spec: r#"{"metrics": ["bookings", "booking_value"], "group_by": ["TimeDimension('metric_time', 'day')"], "order_by": ["-metric_time"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "multiple_cumulative_metrics",
        spec: r#"{"metrics": ["revenue_all_time", "trailing_2_months_revenue"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ═══════════════════════════════════════════════════════════════════
    // Ported from itest_metrics.yaml — single-metric tests
    // ═══════════════════════════════════════════════════════════════════
    TestCase {
        name: "constrained_metric_with_where",
        spec: r#"{"metrics": ["instant_booking_value"], "group_by": ["TimeDimension('metric_time', 'day')"], "where": ["{{ Dimension('booking__is_instant') }}"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "constrained_metric_with_user_constraint",
        spec: r#"{"metrics": ["instant_booking_value"], "group_by": ["TimeDimension('metric_time', 'day')"], "where": ["{{ Dimension('listing__is_lux_latest') }}"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "count_distinct_by_is_instant",
        spec: r#"{"metrics": ["bookers"], "group_by": ["TimeDimension('metric_time', 'day')", "Dimension('booking__is_instant')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "count_distinct_with_constraint",
        spec: r#"{"metrics": ["bookers"], "group_by": ["TimeDimension('metric_time', 'day')"], "where": ["{{ Dimension('booking__is_instant') }}"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "ratio_sort_by_metric_time",
        spec: r#"{"metrics": ["bookings_per_booker"], "group_by": ["TimeDimension('metric_time', 'day')"], "order_by": ["+metric_time"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "booking_payments_by_metric_time",
        spec: r#"{"metrics": ["booking_payments"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "derived_non_referred_bookings_pct_by_day",
        spec: r#"{"metrics": ["non_referred_bookings_pct"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "offset_to_grain_by_day",
        spec: r#"{"metrics": ["bookings_growth_since_start_of_month"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "derived_booking_value_sub_instant_by_day",
        spec: r#"{"metrics": ["booking_value_sub_instant"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "nested_derived_booking_value_sub_instant_add_10_by_day",
        spec: r#"{"metrics": ["booking_value_sub_instant_add_10"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "offset_to_grain_by_week",
        spec: r#"{"metrics": ["bookings_growth_since_start_of_month"], "group_by": ["TimeDimension('metric_time', 'week')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "offset_to_grain_by_month_and_week",
        spec: r#"{"metrics": ["bookings_growth_since_start_of_month"], "group_by": ["TimeDimension('metric_time', 'month')", "TimeDimension('metric_time', 'week')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "offset_to_grain_with_agg_time_dim",
        spec: r#"{"metrics": ["bookings_growth_since_start_of_month"], "group_by": ["TimeDimension('booking__ds', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "join_to_time_spine_by_metric_time",
        spec: r#"{"metrics": ["bookings_join_to_time_spine"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "join_to_time_spine_with_where_filter",
        spec: r#"{"metrics": ["bookings_join_to_time_spine"], "group_by": ["TimeDimension('metric_time', 'day')"], "where": ["{{ Dimension('booking__is_instant') }}"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ═══════════════════════════════════════════════════════════════════
    // Ported from itest_metrics.yaml — tests using NEW metric definitions
    // ═══════════════════════════════════════════════════════════════════
    TestCase {
        name: "identifier_constrained_metric",
        spec: r#"{"metrics": ["booking_value_for_non_null_listing_id"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "lux_listings_filtered",
        spec: r#"{"metrics": ["lux_listings"], "group_by": ["Dimension('listing__country_latest')"], "where": ["{{ Dimension('listing__country_latest') }} = 'us'"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "derived_metrics_with_null_dimension_values",
        spec: r#"{"metrics": ["booking_value_per_view"], "group_by": ["Dimension('listing__is_lux_latest')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "derived_offset_window_2_weeks",
        spec: r#"{"metrics": ["bookings_growth_2_weeks"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "derived_offset_5_day_lag",
        spec: r#"{"metrics": ["bookings_5_day_lag"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "derived_offset_5_day_lag_by_month",
        spec: r#"{"metrics": ["bookings_5_day_lag"], "group_by": ["TimeDimension('metric_time', 'month')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "derived_offset_5_day_lag_by_month_and_week",
        spec: r#"{"metrics": ["bookings_5_day_lag"], "group_by": ["TimeDimension('metric_time', 'month')", "TimeDimension('metric_time', 'week')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "derived_offset_window_and_offset_to_grain",
        spec: r#"{"metrics": ["bookings_month_start_compared_to_1_month_prior"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "derived_offset_window_and_offset_to_grain_by_year",
        spec: r#"{"metrics": ["bookings_month_start_compared_to_1_month_prior"], "group_by": ["TimeDimension('metric_time', 'year')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "nested_derived_instant_plus_non_referred",
        spec: r#"{"metrics": ["instant_plus_non_referred_bookings_pct"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "derived_bookings_per_lux_listing",
        spec: r#"{"metrics": ["bookings_per_lux_listing_derived"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "derived_offset_cumulative_metric",
        spec: r#"{"metrics": ["every_2_days_bookers_2_days_ago"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "offset_window_with_agg_time_dim",
        spec: r#"{"metrics": ["bookings_growth_2_weeks"], "group_by": ["TimeDimension('booking__ds', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "fill_nulls_with_0_by_metric_time",
        spec: r#"{"metrics": ["bookings_fill_nulls_with_0"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "fill_nulls_with_0_by_month",
        spec: r#"{"metrics": ["bookings_fill_nulls_with_0"], "group_by": ["TimeDimension('metric_time', 'month')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "fill_nulls_without_time_spine_by_day",
        spec: r#"{"metrics": ["bookings_fill_nulls_with_0_without_time_spine"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "cumulative_fill_nulls_with_0",
        spec: r#"{"metrics": ["every_two_days_bookers_fill_nulls_with_0"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "derived_fill_nulls_for_one_input_metric",
        spec: r#"{"metrics": ["bookings_growth_2_weeks_fill_nulls_with_0_for_non_offset"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "nested_derived_outer_offset",
        spec: r#"{"metrics": ["bookings_offset_twice"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "nested_derived_offset_multiple_inputs",
        spec: r#"{"metrics": ["booking_fees_since_start_of_month"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "metric_with_metric_in_where_filter",
        spec: r#"{"metrics": ["active_listings"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ═══════════════════════════════════════════════════════════════════
    // Metric-in-where-filter: query-level {{ Metric() }} in WHERE clause
    // ═══════════════════════════════════════════════════════════════════
    TestCase {
        name: "query_metric_in_where_filter",
        spec: r#"{"metrics": ["listings"], "where": ["{{ Metric('bookings', ['listing']) }} > 2"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "cumulative_metric_in_where_filter",
        spec: r#"{"metrics": ["listings"], "where": ["{{ Metric('revenue_all_time', ['user']) }} > 2"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "multiple_metrics_in_where_filter",
        spec: r#"{"metrics": ["listings"], "where": ["{{ Metric('bookings', ['listing']) }} > 2 AND {{ Metric('bookers', ['listing']) }} > 1"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ═══════════════════════════════════════════════════════════════════
    // Fill nulls: additional variations
    // ═══════════════════════════════════════════════════════════════════
    TestCase {
        name: "fill_nulls_with_0_multi_metric",
        spec: r#"{"metrics": ["bookings_fill_nulls_with_0", "views"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "fill_nulls_derived_multi_metric",
        spec: r#"{"metrics": ["bookings_growth_2_weeks_fill_nulls_with_0_for_non_offset", "booking_value"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "fill_nulls_multi_metric_with_categorical_dim",
        spec: r#"{"metrics": ["bookings_fill_nulls_with_0_without_time_spine", "views"], "group_by": ["TimeDimension('metric_time', 'day')", "Dimension('listing__is_lux_latest')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ═══════════════════════════════════════════════════════════════════
    // Nested offset with constraints and agg_time_dim
    // ═══════════════════════════════════════════════════════════════════
    TestCase {
        name: "nested_offset_with_agg_time_dim",
        spec: r#"{"metrics": ["bookings_offset_twice"], "group_by": ["TimeDimension('booking__ds', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "ratio_with_metric_filter_on_metric",
        spec: r#"{"metrics": ["popular_listing_bookings_per_booker"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ═══════════════════════════════════════════════════════════════════
    // Join to time spine with filters
    // ═══════════════════════════════════════════════════════════════════
    TestCase {
        name: "join_to_time_spine_with_queried_filter_and_dim",
        spec: r#"{"metrics": ["bookings_join_to_time_spine"], "group_by": ["TimeDimension('metric_time', 'day')", "Dimension('booking__is_instant')"], "where": ["{{ Dimension('booking__is_instant') }}"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ═══════════════════════════════════════════════════════════════════
    // Metric filter with entity prefix
    // ═══════════════════════════════════════════════════════════════════
    TestCase {
        name: "metric_filter_with_entity_prefix",
        spec: r#"{"metrics": ["listings"], "where": ["{{ Metric('views', ['view__listing']) }} > 2"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ═══════════════════════════════════════════════════════════════════
    // Date part: EXTRACT(part FROM ...) instead of DATE_TRUNC
    // ═══════════════════════════════════════════════════════════════════
    TestCase {
        name: "date_part_year",
        spec: r#"{"metrics": ["bookings"], "group_by": ["TimeDimension('metric_time', 'day', date_part='year')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "date_part_year_non_metric_time",
        spec: r#"{"metrics": ["bookings"], "group_by": ["TimeDimension('booking__ds', 'day', date_part='year')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "date_part_multiple",
        spec: r#"{"metrics": ["bookings"], "group_by": ["TimeDimension('metric_time', 'day', date_part='quarter')", "TimeDimension('metric_time', 'day', date_part='dow')", "TimeDimension('metric_time', 'day', date_part='doy')", "TimeDimension('metric_time', 'day', date_part='day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "date_part_derived_offset_month",
        spec: r#"{"metrics": ["bookings_5_day_lag"], "group_by": ["TimeDimension('metric_time', 'day', date_part='month')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "date_part_count_distinct_dow",
        spec: r#"{"metrics": ["bookers"], "group_by": ["TimeDimension('metric_time', 'day', date_part='dow')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ═══════════════════════════════════════════════════════════════════
    // Time constraint: WHERE metric_time BETWEEN start AND end
    // ═══════════════════════════════════════════════════════════════════
    TestCase {
        name: "time_constraint_nested_offset",
        spec: r#"{"metrics": ["bookings_offset_twice"], "group_by": ["TimeDimension('metric_time', 'day')"], "time_constraint": ["2019-12-05", "2019-12-10"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "time_constraint_offset_window",
        spec: r#"{"metrics": ["bookings_5_day_lag"], "group_by": ["TimeDimension('metric_time', 'day')"], "time_constraint": ["2019-12-05", "2019-12-10"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "time_constraint_cumulative_offset",
        spec: r#"{"metrics": ["every_2_days_bookers_2_days_ago"], "group_by": ["TimeDimension('metric_time', 'day')"], "time_constraint": ["2019-12-01", "2019-12-10"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "time_constraint_join_to_time_spine",
        spec: r#"{"metrics": ["bookings_join_to_time_spine"], "group_by": ["TimeDimension('metric_time', 'day')"], "time_constraint": ["2019-12-19", "2019-12-19"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ═══════════════════════════════════════════════════════════════════
    // Remaining STANDARD tests from itest_metrics.yaml
    // ═══════════════════════════════════════════════════════════════════
    TestCase {
        name: "lux_listings_by_country_filtered",
        spec: r#"{"metrics": ["lux_listings"], "group_by": ["Dimension('listing__country_latest')"], "where": ["{{ Dimension('listing__country_latest') }} = 'us'"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "same_measure_constrained_and_unconstrained",
        spec: r#"{"metrics": ["booking_value", "instant_booking_value"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "fill_nulls_with_0_non_metric_time",
        spec: r#"{"metrics": ["bookings_fill_nulls_with_0"], "group_by": ["TimeDimension('booking__paid_at', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "fill_nulls_with_0_agg_time_dim",
        spec: r#"{"metrics": ["bookings_fill_nulls_with_0"], "group_by": ["TimeDimension('booking__ds', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "fill_nulls_with_0_categorical_dim",
        spec: r#"{"metrics": ["bookings_fill_nulls_with_0"], "group_by": ["Dimension('booking__is_instant')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "ratio_with_zero_denominator",
        spec: r#"{"metrics": ["bookings_per_dollar"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ═══════════════════════════════════════════════════════════════════
    // Remaining DATE_PART tests
    // ═══════════════════════════════════════════════════════════════════
    TestCase {
        name: "date_part_offset_agg_time_dim_doy",
        spec: r#"{"metrics": ["bookings_5_day_lag"], "group_by": ["TimeDimension('booking__ds', 'day', date_part='doy')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ═══════════════════════════════════════════════════════════════════
    // Remaining TIME_CONSTRAINT tests
    // ═══════════════════════════════════════════════════════════════════
    TestCase {
        name: "time_constraint_join_to_time_spine_queried",
        spec: r#"{"metrics": ["bookings_join_to_time_spine"], "group_by": ["TimeDimension('metric_time', 'day')"], "time_constraint": ["2019-12-19", "2019-12-19"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ═══════════════════════════════════════════════════════════════════
    // Subdaily tests: archived_users with hour-level agg_time_dimension
    // ═══════════════════════════════════════════════════════════════════
    TestCase {
        name: "simple_subdaily_metric_default_day",
        spec: r#"{"metrics": ["archived_users"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: Some(7.0),
    },
    TestCase {
        name: "simple_subdaily_metric_default_hour",
        spec: r#"{"metrics": ["archived_users"], "group_by": ["TimeDimension('metric_time', 'hour')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "subdaily_offset_window",
        spec: r#"{"metrics": ["subdaily_offset_window_metric"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "subdaily_offset_grain_to_date",
        spec: r#"{"metrics": ["subdaily_offset_grain_to_date_metric"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "subdaily_join_to_time_spine",
        spec: r#"{"metrics": ["subdaily_join_to_time_spine_metric"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "subdaily_metric_by_dimension",
        spec: r#"{"metrics": ["archived_users"], "group_by": ["TimeDimension('metric_time', 'day')", "Dimension('user__home_state')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "subdaily_metric_hour_by_dimension",
        spec: r#"{"metrics": ["archived_users"], "group_by": ["TimeDimension('metric_time', 'hour')", "Dimension('user__home_state')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ═══════════════════════════════════════════════════════════════════
    // Derived/ratio metric in WHERE filter
    // ═══════════════════════════════════════════════════════════════════
    TestCase {
        name: "derived_metric_in_where_filter",
        spec: r#"{"metrics": ["bookings"], "where": ["{{ Metric('views_times_booking_value', ['listing']) }} > 1000"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "ratio_metric_in_where_filter",
        spec: r#"{"metrics": ["bookings"], "where": ["{{ Metric('bookings_per_view', ['listing']) }} > 0.5"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ═══════════════════════════════════════════════════════════════════
    // WHERE-pushdown-on-offset
    // ═══════════════════════════════════════════════════════════════════
    TestCase {
        name: "offset_with_where_on_grouped_dim",
        spec: r#"{"metrics": ["bookings_5_day_lag"], "group_by": ["TimeDimension('metric_time', 'day')", "Dimension('booking__is_instant')"], "where": ["NOT {{ Dimension('booking__is_instant') }}"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "nested_offset_with_where_constraint",
        spec: r#"{"metrics": ["bookings_growth_2_weeks"], "group_by": ["TimeDimension('metric_time', 'day')", "Dimension('booking__is_instant')"], "where": ["{{ Dimension('booking__is_instant') }}"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ═══════════════════════════════════════════════════════════════════
    // Third-hop join tests
    // ═══════════════════════════════════════════════════════════════════
    TestCase {
        name: "third_hop_bookings_by_region",
        spec: r#"{"metrics": ["bookings"], "group_by": ["Dimension('region__region_name')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "third_hop_revenue_by_region",
        spec: r#"{"metrics": ["revenue"], "group_by": ["Dimension('region__region_name')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ═══════════════════════════════════════════════════════════════════
    // 3+ input derived metric: COALESCE join keys
    // ═══════════════════════════════════════════════════════════════════
    TestCase {
        name: "three_input_derived_by_listing",
        spec: r#"{"metrics": ["bookings_plus_views_plus_listings"], "group_by": ["Entity('listing')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "three_input_derived_by_day",
        spec: r#"{"metrics": ["bookings_plus_views_plus_listings"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "three_input_derived_no_dims",
        spec: r#"{"metrics": ["bookings_plus_views_plus_listings"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ═══════════════════════════════════════════════════════════════════
    // NULL-safe join: IS NOT DISTINCT FROM
    // ═══════════════════════════════════════════════════════════════════
    TestCase {
        name: "null_safe_join_multi_metric_nullable_dim",
        spec: r#"{"metrics": ["bookings", "views"], "group_by": ["Dimension('listing__is_lux_latest')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "null_safe_join_ratio_nullable_dim",
        spec: r#"{"metrics": ["bookings_per_view"], "group_by": ["Dimension('listing__is_lux_latest')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    // ═══════════════════════════════════════════════════════════════════
    // fill_nulls_with in derived expression
    // ═══════════════════════════════════════════════════════════════════
    TestCase {
        name: "fill_nulls_in_derived_by_day",
        spec: r#"{"metrics": ["fill_nulls_derived_test"], "group_by": ["TimeDimension('metric_time', 'day')"]}"#,
        min_rows: 1,
        expected_value: None,
    },
    TestCase {
        name: "fill_nulls_in_derived_no_dims",
        spec: r#"{"metrics": ["fill_nulls_derived_test"]}"#,
        min_rows: 1,
        expected_value: None,
    },
];

// ═══════════════════════════════════════════════════════════════════════════
// Shared scorecard runner
// ═══════════════════════════════════════════════════════════════════════════

fn setup_metric_store(database: &str, schema: &str) -> InMemoryMetricStore {
    let manifest = build_semantic_manifest(database, schema);
    InMemoryMetricStore::from_manifest(&manifest)
}

#[allow(clippy::type_complexity)]
fn run_scorecard(
    store: &mut InMemoryMetricStore,
    execute: &mut dyn FnMut(&str, &str) -> Result<Option<Vec<RecordBatch>>, String>,
) {
    let mut pass = 0u32;
    let mut fail = 0u32;
    let mut skip = 0u32;
    let mut results: Vec<(String, bool, String)> = Vec::new();

    for tc in TEST_CASES {
        let spec = match parse_query_spec(tc.spec) {
            Ok(s) => s,
            Err(e) => {
                fail += 1;
                results.push((tc.name.to_string(), false, format!("parse error: {e}")));
                continue;
            }
        };
        let sql = match compile(store, &spec, Dialect::Snowflake) {
            Ok(s) => s,
            Err(e) => {
                fail += 1;
                results.push((tc.name.to_string(), false, format!("compile error: {e}")));
                continue;
            }
        };

        match execute(tc.name, &sql) {
            Ok(Some(batches)) => {
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                if rows < tc.min_rows {
                    fail += 1;
                    results.push((
                        tc.name.to_string(),
                        false,
                        format!("expected >= {} rows, got {rows}\n  SQL: {sql}", tc.min_rows),
                    ));
                } else if let Some(expected) = tc.expected_value {
                    let scalar = extract_scalar(&batches);
                    if let Some(actual) = scalar {
                        if (actual - expected).abs() < 0.01 {
                            pass += 1;
                            results.push((
                                tc.name.to_string(),
                                true,
                                format!("{rows} rows, value={actual}"),
                            ));
                        } else {
                            fail += 1;
                            results.push((tc.name.to_string(), false, format!("value mismatch: expected {expected}, got {actual}\n  SQL: {sql}")));
                        }
                    } else {
                        fail += 1;
                        results.push((tc.name.to_string(), false, format!("expected value {expected} but could not extract scalar\n  SQL: {sql}")));
                    }
                } else {
                    pass += 1;
                    results.push((tc.name.to_string(), true, format!("{rows} rows")));
                }
            }
            Ok(None) => {
                skip += 1;
            }
            Err(msg) => {
                fail += 1;
                results.push((tc.name.to_string(), false, msg));
            }
        }
    }

    let bar = "=".repeat(70);
    eprintln!("\n{bar}");
    eprintln!("Snowflake Compatibility Scorecard");
    eprintln!("{bar}");
    for (name, ok, detail) in &results {
        let icon = if *ok { "PASS" } else { "FAIL" };
        eprintln!("  [{icon}] {name}: {detail}");
    }
    eprintln!("{bar}");
    let total = pass + fail;
    eprintln!(
        "  Total: {total}  Pass: {pass}  Fail: {fail}  Skip: {skip}  ({:.0}%)",
        if total > 0 {
            (pass as f64 / total as f64) * 100.0
        } else {
            0.0
        }
    );
    eprintln!("{bar}\n");

    assert_eq!(
        fail, 0,
        "{fail} of {total} Snowflake compatibility tests failed"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Record test — live Snowflake, writes Arrow IPC recordings
// ═══════════════════════════════════════════════════════════════════════════

#[test]
#[ignore = "requires fusion_tests/snowflake profile in ~/.dbt/profiles.yml"]
fn snowflake_compat_record() {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let schema = format!("MF_TEST_{ts}");
    eprintln!("snowflake_compat [record]: using schema {schema}");

    let mut sf = SnowflakeConn::connect();
    sf.execute_update(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"));
    sf.set_schema(schema.clone());
    eprintln!("snowflake_compat [record]: schema {schema} created");

    for stmt in snowflake_data_ddl(&schema) {
        sf.execute_update(&stmt);
    }
    eprintln!("snowflake_compat [record]: test data loaded");

    let db_batches = sf.execute_query("SELECT CURRENT_DATABASE()");
    let database = {
        use arrow_array::cast::AsArray;
        db_batches
            .first()
            .and_then(|b| {
                b.column(0)
                    .as_string_opt::<i32>()
                    .map(|a| a.value(0).to_string())
            })
            .expect("failed to get current database")
    };

    let mut store = setup_metric_store(&database, &schema);

    run_scorecard(&mut store, &mut |name, sql| match std::panic::catch_unwind(
        std::panic::AssertUnwindSafe(|| sf.execute_query(sql)),
    ) {
        Ok(batches) => {
            write_recording(name, &batches);
            Ok(Some(batches))
        }
        Err(_) => Err(format!("execution panicked\n  SQL: {sql}")),
    });

    eprintln!(
        "snowflake_compat [record]: recordings written to {:?}",
        recordings_dir()
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Replay test — reads Arrow IPC recordings, no Snowflake needed
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn snowflake_compat_replay() {
    let dir = recordings_dir();
    if !dir.exists() || fs::read_dir(&dir).map_or(true, |mut d| d.next().is_none()) {
        eprintln!(
            "snowflake_compat [replay]: no recordings at {dir:?} — \
             run snowflake_compat_record to generate them"
        );
        return;
    }

    let mut store = setup_metric_store("RECORDED_DB", "RECORDED_SCHEMA");

    run_scorecard(&mut store, &mut |name, _sql| Ok(read_recording(name)));
}

fn extract_scalar(batches: &[RecordBatch]) -> Option<f64> {
    let b = batches.first()?;
    if b.num_rows() == 0 || b.num_columns() == 0 {
        return None;
    }
    let col_idx = b.num_columns() - 1;
    let col = b.column(col_idx);
    use arrow_array::{Decimal128Array, Float64Array, Int32Array, Int64Array};
    if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
        Some(arr.value(0))
    } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        Some(arr.value(0) as f64)
    } else if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
        Some(arr.value(0) as f64)
    } else if let Some(arr) = col.as_any().downcast_ref::<Decimal128Array>() {
        let scale = arr.scale() as f64;
        Some(arr.value(0) as f64 / 10f64.powf(scale))
    } else {
        None
    }
}
