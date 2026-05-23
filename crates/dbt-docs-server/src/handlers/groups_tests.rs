//! Tests for `GET /api/v1/groups/:id`.
//!
//! Schema anchoring (#10255): the `RecordBatch` fixtures below are hand-
//! rolled and not enforced against the production parquet schemas. A column
//! rename or type change in `dbt-index` will pass these tests while
//! silently breaking the handler. Once #10255 lands typed row builders,
//! replace these schemas to get compile-time coverage.

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use axum::extract::{Path, Query, State};
use axum::response::Response;

use super::*;
use crate::providers::{Backend, BackendError, Providers};
use crate::state::AppState;

// ---------------------------------------------------------------------------
// Mock backend
// ---------------------------------------------------------------------------

/// Routes Arrow queries to fixture batches. The member-models query and the
/// `model_count` query both hit `dbt.nodes`; they're disambiguated by the
/// `count(*)` projection.
struct GroupDetailMockBackend {
    group_batches: Vec<RecordBatch>,
    model_batches: Vec<RecordBatch>,
    count_batches: Vec<RecordBatch>,
}

impl Backend for GroupDetailMockBackend {
    fn is_available(&self) -> bool {
        true
    }

    fn query_scalar(&self, _sql: &str) -> Option<String> {
        Some("0".to_owned())
    }

    fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
        // count(*) projection lands first; member-list query follows.
        if sql.contains("count(*)") && sql.contains("dbt.nodes") {
            return Ok(self.count_batches.clone());
        }
        if sql.contains("dbt.nodes") {
            return Ok(self.model_batches.clone());
        }
        if sql.contains("dbt.groups") {
            return Ok(self.group_batches.clone());
        }
        Err(BackendError::Query(format!("unrouted query: {sql}")))
    }
}

fn make_state(backend: GroupDetailMockBackend) -> Arc<AppState> {
    let providers = Providers {
        backend: Arc::new(backend),
        ..Providers::default()
    };
    Arc::new(AppState {
        index_dir: PathBuf::from("/tmp"),
        providers,
    })
}

// ---------------------------------------------------------------------------
// Batch builders
// ---------------------------------------------------------------------------

/// Schema for `dbt.groups` rows as queried by `GROUP_DETAIL_NODE_SQL`.
fn group_row_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("package_name", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("original_file_path", DataType::Utf8, true),
        Field::new("owner_name", DataType::Utf8, true),
        Field::new("owner_email", DataType::Utf8, true),
        Field::new("config", DataType::Utf8, true),
        Field::new("ingested_at", DataType::Utf8, true),
    ]))
}

#[allow(clippy::too_many_arguments)]
fn make_group_row(
    unique_id: &str,
    name: &str,
    package_name: Option<&str>,
    description: Option<&str>,
    owner_name: Option<&str>,
    owner_email: Option<&str>,
    config_json: Option<&str>,
    ingested_at: Option<&str>,
) -> RecordBatch {
    RecordBatch::try_new(
        group_row_schema(),
        vec![
            Arc::new(StringArray::from(vec![unique_id])),
            Arc::new(StringArray::from(vec![name])),
            Arc::new(StringArray::from(vec![package_name])),
            Arc::new(StringArray::from(vec![description])),
            Arc::new(StringArray::from(vec![Some("models/_groups.yml")])),
            Arc::new(StringArray::from(vec![owner_name])),
            Arc::new(StringArray::from(vec![owner_email])),
            Arc::new(StringArray::from(vec![config_json])),
            Arc::new(StringArray::from(vec![ingested_at])),
        ],
    )
    .expect("valid group row batch")
}

/// Build a member-models batch. Each tuple is
/// `(unique_id, name, database_name, schema_name, contract_enforced)`.
#[allow(clippy::type_complexity)]
fn model_members_batch(
    rows: &[(&str, &str, Option<&str>, Option<&str>, Option<bool>)],
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("database_name", DataType::Utf8, true),
        Field::new("schema_name", DataType::Utf8, true),
        Field::new("contract_enforced", DataType::Boolean, true),
    ]));
    let uids: Vec<&str> = rows.iter().map(|r| r.0).collect();
    let names: Vec<&str> = rows.iter().map(|r| r.1).collect();
    let dbs: Vec<Option<&str>> = rows.iter().map(|r| r.2).collect();
    let schemas: Vec<Option<&str>> = rows.iter().map(|r| r.3).collect();
    let contracts: Vec<Option<bool>> = rows.iter().map(|r| r.4).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(uids)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(dbs)),
            Arc::new(StringArray::from(schemas)),
            Arc::new(BooleanArray::from(contracts)),
        ],
    )
    .expect("valid model member batch")
}

fn count_batch(n: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "count_star()",
        DataType::Int64,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![n]))])
        .expect("valid count batch")
}

async fn response_body(response: Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    serde_json::from_slice(&bytes).expect("valid json")
}

fn default_params() -> Query<GroupDetailParams> {
    Query(GroupDetailParams { first: None })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn invalid_unique_id_returns_400() {
    let backend = GroupDetailMockBackend {
        group_batches: vec![],
        model_batches: vec![],
        count_batches: vec![],
    };
    let state = make_state(backend);
    let r = get_group(State(state), Path("bad'id".to_owned()), default_params()).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn missing_group_returns_404() {
    let backend = GroupDetailMockBackend {
        group_batches: vec![],
        model_batches: vec![],
        count_batches: vec![],
    };
    let state = make_state(backend);
    let r = get_group(
        State(state),
        Path("group.x.missing".to_owned()),
        default_params(),
    )
    .await;
    assert_eq!(r.status(), 404);
}

#[tokio::test]
async fn all_fields_hydrated() {
    let backend = GroupDetailMockBackend {
        group_batches: vec![make_group_row(
            "group.jaffle.finance",
            "finance",
            Some("jaffle"),
            Some("Finance domain"),
            Some("Finance Data Team"),
            Some("finance-data@jaffle.example"),
            Some(r#"{"tags":["finance","core"],"meta":{"domain":"finance","tier":"gold"}}"#),
            Some("2026-05-19T08:30:00Z"),
        )],
        model_batches: vec![model_members_batch(&[
            (
                "model.jaffle.orders",
                "orders",
                Some("prod"),
                Some("dbt_prod"),
                Some(true),
            ),
            (
                "model.jaffle.payments",
                "payments",
                Some("prod"),
                Some("dbt_prod"),
                Some(false),
            ),
            (
                "model.jaffle.revenue",
                "revenue",
                Some("prod"),
                Some("dbt_prod"),
                None,
            ),
        ])],
        count_batches: vec![count_batch(3)],
    };
    let state = make_state(backend);
    let r = get_group(
        State(state),
        Path("group.jaffle.finance".to_owned()),
        default_params(),
    )
    .await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;

    // NodeBase fields flatten into top-level; resource_type is hardcoded.
    assert_eq!(body["unique_id"], "group.jaffle.finance");
    assert_eq!(body["name"], "finance");
    assert_eq!(body["resource_type"], "group");
    assert_eq!(body["package_name"], "jaffle");
    assert_eq!(body["description"], "Finance domain");
    assert_eq!(body["original_file_path"], "models/_groups.yml");
    // file_path is omitted entirely (Class B per the contract).
    assert!(body.get("file_path").is_none());

    // tags lifted from config.tags.
    assert_eq!(body["tags"], serde_json::json!(["finance", "core"]));

    // Owner is a nested object — slack/github are null when config lacks them.
    assert_eq!(body["owner"]["name"], "Finance Data Team");
    assert_eq!(body["owner"]["email"], "finance-data@jaffle.example");
    assert_eq!(body["owner"]["slack"], serde_json::Value::Null);
    assert_eq!(body["owner"]["github"], serde_json::Value::Null);

    // meta lifted from config.meta, parsed as nested JSON.
    assert_eq!(
        body["meta"],
        serde_json::json!({"domain": "finance", "tier": "gold"})
    );

    // models inline with the richer per-member shape.
    assert_eq!(body["models"].as_array().expect("array").len(), 3);
    assert_eq!(body["models"][0]["unique_id"], "model.jaffle.orders");
    assert_eq!(body["models"][0]["name"], "orders");
    assert_eq!(body["models"][0]["database_name"], "prod");
    assert_eq!(body["models"][0]["schema_name"], "dbt_prod");
    assert_eq!(body["models"][0]["contract_enforced"], true);
    assert_eq!(body["models"][1]["contract_enforced"], false);
    assert_eq!(
        body["models"][2]["contract_enforced"],
        serde_json::Value::Null
    );

    // model_count + truncated.
    assert_eq!(body["model_count"], 3);
    assert_eq!(body["truncated"], false);

    // Definition-only: depends_on, execution_info, catalog all absent.
    assert!(body.get("depends_on").is_none());
    assert!(body.get("execution_info").is_none());
    assert!(body.get("catalog").is_none());

    // ingested_at as ISO 8601 string.
    assert_eq!(body["ingested_at"], "2026-05-19T08:30:00Z");
}

#[tokio::test]
async fn owner_with_slack_and_github_from_config() {
    let backend = GroupDetailMockBackend {
        group_batches: vec![make_group_row(
            "group.pkg.g",
            "g",
            Some("pkg"),
            None,
            Some("Team"),
            Some("team@example.com"),
            Some(r##"{"owner":{"slack":"#team","github":"org/team"}}"##),
            Some("2026-05-19T08:30:00Z"),
        )],
        model_batches: vec![],
        count_batches: vec![count_batch(0)],
    };
    let state = make_state(backend);
    let r = get_group(
        State(state),
        Path("group.pkg.g".to_owned()),
        default_params(),
    )
    .await;
    let body = response_body(r).await;
    assert_eq!(body["owner"]["name"], "Team");
    assert_eq!(body["owner"]["email"], "team@example.com");
    assert_eq!(body["owner"]["slack"], "#team");
    assert_eq!(body["owner"]["github"], "org/team");
}

#[tokio::test]
async fn owner_null_when_all_owner_fields_absent() {
    let backend = GroupDetailMockBackend {
        group_batches: vec![make_group_row(
            "group.pkg.g",
            "g",
            Some("pkg"),
            None,
            None,
            None,
            None,
            Some("2026-05-19T08:30:00Z"),
        )],
        model_batches: vec![],
        count_batches: vec![count_batch(0)],
    };
    let state = make_state(backend);
    let r = get_group(
        State(state),
        Path("group.pkg.g".to_owned()),
        default_params(),
    )
    .await;
    let body = response_body(r).await;
    assert_eq!(body["owner"], serde_json::Value::Null);
}

#[tokio::test]
async fn tags_empty_when_config_absent() {
    let backend = GroupDetailMockBackend {
        group_batches: vec![make_group_row(
            "group.pkg.g",
            "g",
            Some("pkg"),
            None,
            Some("Team"),
            None,
            None, // config absent
            Some("2026-05-19T08:30:00Z"),
        )],
        model_batches: vec![],
        count_batches: vec![count_batch(0)],
    };
    let state = make_state(backend);
    let r = get_group(
        State(state),
        Path("group.pkg.g".to_owned()),
        default_params(),
    )
    .await;
    let body = response_body(r).await;
    assert_eq!(body["tags"], serde_json::json!([]));
}

#[tokio::test]
async fn meta_null_when_config_absent() {
    let backend = GroupDetailMockBackend {
        group_batches: vec![make_group_row(
            "group.pkg.g",
            "g",
            Some("pkg"),
            None,
            Some("Team"),
            None,
            None, // config absent
            Some("2026-05-19T08:30:00Z"),
        )],
        model_batches: vec![],
        count_batches: vec![count_batch(0)],
    };
    let state = make_state(backend);
    let r = get_group(
        State(state),
        Path("group.pkg.g".to_owned()),
        default_params(),
    )
    .await;
    let body = response_body(r).await;
    assert_eq!(body["meta"], serde_json::Value::Null);
}

#[tokio::test]
async fn meta_null_when_config_has_no_meta_key() {
    let backend = GroupDetailMockBackend {
        group_batches: vec![make_group_row(
            "group.pkg.g",
            "g",
            Some("pkg"),
            None,
            Some("Team"),
            None,
            Some(r#"{"other":"value"}"#),
            Some("2026-05-19T08:30:00Z"),
        )],
        model_batches: vec![],
        count_batches: vec![count_batch(0)],
    };
    let state = make_state(backend);
    let r = get_group(
        State(state),
        Path("group.pkg.g".to_owned()),
        default_params(),
    )
    .await;
    let body = response_body(r).await;
    assert_eq!(body["meta"], serde_json::Value::Null);
}

#[tokio::test]
async fn meta_null_when_config_malformed() {
    // Malformed config JSON must surface as meta=null, not 500.
    let backend = GroupDetailMockBackend {
        group_batches: vec![make_group_row(
            "group.pkg.g",
            "g",
            Some("pkg"),
            None,
            Some("Team"),
            None,
            Some("not{valid:json"),
            Some("2026-05-19T08:30:00Z"),
        )],
        model_batches: vec![],
        count_batches: vec![count_batch(0)],
    };
    let state = make_state(backend);
    let r = get_group(
        State(state),
        Path("group.pkg.g".to_owned()),
        default_params(),
    )
    .await;
    assert_eq!(
        r.status(),
        200,
        "malformed config must not 500 the response"
    );
    let body = response_body(r).await;
    assert_eq!(body["meta"], serde_json::Value::Null);
}

#[tokio::test]
async fn models_empty_when_no_members() {
    let backend = GroupDetailMockBackend {
        group_batches: vec![make_group_row(
            "group.pkg.g",
            "g",
            Some("pkg"),
            None,
            Some("Team"),
            None,
            None,
            Some("2026-05-19T08:30:00Z"),
        )],
        model_batches: vec![],
        count_batches: vec![count_batch(0)],
    };
    let state = make_state(backend);
    let r = get_group(
        State(state),
        Path("group.pkg.g".to_owned()),
        default_params(),
    )
    .await;
    let body = response_body(r).await;
    assert_eq!(body["models"], serde_json::json!([]));
    assert_eq!(body["model_count"], 0);
    assert_eq!(body["truncated"], false);
}

#[tokio::test]
async fn models_truncated_flag_when_at_limit() {
    // Pass first=2, return 2 rows; count(*) reports 5 total.
    // model_count > models.len() → truncated must be true.
    let backend = GroupDetailMockBackend {
        group_batches: vec![make_group_row(
            "group.pkg.g",
            "g",
            Some("pkg"),
            None,
            Some("Team"),
            None,
            None,
            Some("2026-05-19T08:30:00Z"),
        )],
        model_batches: vec![model_members_batch(&[
            ("model.pkg.a", "a", Some("prod"), Some("dbt"), Some(true)),
            ("model.pkg.b", "b", Some("prod"), Some("dbt"), None),
        ])],
        count_batches: vec![count_batch(5)],
    };
    let state = make_state(backend);
    let r = get_group(
        State(state),
        Path("group.pkg.g".to_owned()),
        Query(GroupDetailParams { first: Some(2) }),
    )
    .await;
    let body = response_body(r).await;
    assert_eq!(body["models"].as_array().expect("array").len(), 2);
    assert_eq!(body["model_count"], 5);
    assert_eq!(body["truncated"], true);
}
