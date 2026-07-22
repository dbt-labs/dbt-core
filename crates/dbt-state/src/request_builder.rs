//! Stable request-builder DTOs and helpers for the dbt State service protocol.
//!
//! This module accepts normalized inputs from higher-level crates, derives
//! service-specific fields such as execution type and semantic extras, and
//! converts those inputs into generated protobuf request messages.

use std::collections::{BTreeMap, HashMap};
use std::io::{self, Read};

use dbt_telemetry::NodeType;
use serde_json::Value;
use thiserror::Error;

use crate::proto::query_cache::{
    CloneRequest, ExecutionOutcome, ExecutionRecord, ModelExecutionType, QueryDependency,
    StaleUpstreamPolicy, SubmitEnrichedSqlRequest, SubmitValuesRequest, TableModifiedInfo,
    TableProperties, ValuesExecution, execution_record,
};

pub const SQL_SEMANTIC_EXTRA_KEYS: &[&str] = &[
    "on_schema_change",
    "incremental_predicates",
    "merge_update_columns",
    "merge_exclude_columns",
    "severity",
    "limit",
    "where",
    "fail_calc",
    "warn_if",
    "error_if",
    "store_failures",
    "store_failures_as",
    // Databricks attributes
    "auto_liquid_cluster",
    "databricks_tags",
];

pub const SEED_SEMANTIC_EXTRA_KEYS: &[&str] = &["column_types", "quote_columns", "delimiter"];

const HASH_READ_CHUNK_SIZE: usize = 64 * 1024;

#[derive(Debug, Error)]
pub enum RequestBuildError {
    #[error("failed to serialize dbt State semantic extra: {0}")]
    SemanticExtra(#[from] serde_json::Error),
    #[error("failed to read seed data for dbt State hash: {0}")]
    SeedHash(#[from] io::Error),
    #[error("unsupported dbt State execution type: {0}")]
    UnsupportedExecutionType(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeIdentity {
    pub name: String,
    pub fqn: Vec<String>,
    pub unique_id: String,
}

impl NodeIdentity {
    pub fn labels(&self) -> HashMap<String, String> {
        HashMap::from([
            ("dbt_node_name".to_string(), self.name.clone()),
            ("dbt_node_fqn".to_string(), self.fqn.join(".")),
            ("dbt_node_unique_id".to_string(), self.unique_id.clone()),
        ])
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionTypeInput {
    pub resource_type: NodeType,
    pub is_view: bool,
    pub is_custom_materialization: bool,
    pub is_incremental: bool,
    pub full_refresh: bool,
    pub incremental_strategy: Option<String>,
    pub has_unique_key: bool,
}

pub fn execution_type_from_input(
    input: &ExecutionTypeInput,
) -> Result<ModelExecutionType, RequestBuildError> {
    if input.resource_type == NodeType::Test {
        return Ok(ModelExecutionType::DbtDataTest);
    }
    if input.is_view {
        return Ok(ModelExecutionType::View);
    }
    if input.is_custom_materialization {
        return Ok(ModelExecutionType::DbtCustom);
    }
    if input.resource_type == NodeType::Snapshot {
        return Ok(ModelExecutionType::Snapshot);
    }
    if input.resource_type == NodeType::Model && input.is_incremental && !input.full_refresh {
        let strategy = input
            .incremental_strategy
            .as_deref()
            .unwrap_or("append")
            .replace('+', "_")
            .to_ascii_uppercase();

        if strategy == "MERGE" && !input.has_unique_key {
            return Ok(ModelExecutionType::Append);
        }

        return ModelExecutionType::from_str_name(&strategy)
            .ok_or(RequestBuildError::UnsupportedExecutionType(strategy));
    }

    Ok(ModelExecutionType::Full)
}

pub type SemanticExtraConfig = BTreeMap<String, Option<Value>>;
pub type SemanticExtras = HashMap<String, String>;

pub fn sql_semantic_extras(
    config: &SemanticExtraConfig,
) -> Result<SemanticExtras, RequestBuildError> {
    semantic_extras_from_keys(config, SQL_SEMANTIC_EXTRA_KEYS)
}

pub fn seed_semantic_extras(
    config: &SemanticExtraConfig,
) -> Result<SemanticExtras, RequestBuildError> {
    semantic_extras_from_keys(config, SEED_SEMANTIC_EXTRA_KEYS)
}

pub fn semantic_extras_from_keys(
    config: &SemanticExtraConfig,
    keys: &[&str],
) -> Result<SemanticExtras, RequestBuildError> {
    let mut extras = HashMap::new();

    for key in keys {
        if let Some(value) = config.get(*key) {
            let serialized = match value {
                Some(value) => serde_json::to_string(value)?,
                None => String::new(),
            };
            extras.insert((*key).to_string(), serialized);
        }
    }

    Ok(extras)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubmitEnrichedSqlRequestInput {
    pub target_table: Option<String>,
    pub dialect: String,
    pub default_catalog: String,
    pub execution_type: ModelExecutionType,
    pub sql: String,
    pub tables: Vec<TableModifiedInfo>,
    pub query_dependencies: Vec<QueryDependency>,
    pub semantic_extras: SemanticExtras,
    pub freshness_tolerance_seconds: i64,
    pub lenient_dependencies: Vec<String>,
    pub tolerate_nondeterminism: bool,
    pub labels: HashMap<String, String>,
    pub clone_time_travel_limit: Option<i64>,
    pub clone_table_properties: Option<TableProperties>,
    pub stale_upstream_policy: StaleUpstreamPolicy,
}

impl SubmitEnrichedSqlRequestInput {
    pub fn into_proto(self) -> SubmitEnrichedSqlRequest {
        SubmitEnrichedSqlRequest {
            target_table: self.target_table,
            dialect: self.dialect,
            default_catalog: self.default_catalog,
            execution_type: self.execution_type as i32,
            sql: self.sql,
            tables: self.tables,
            query_dependencies: self.query_dependencies,
            semantic_extras: self.semantic_extras,
            freshness_tolerance_seconds: self.freshness_tolerance_seconds,
            lenient_dependencies: self.lenient_dependencies,
            tolerate_nondeterminism: self.tolerate_nondeterminism,
            labels: self.labels,
            clone_time_travel_limit: self.clone_time_travel_limit,
            clone_table_properties: self.clone_table_properties,
            stale_upstream_policy: self.stale_upstream_policy as i32,
            clone_chain_depth_limit: None,  //todo: implement
            dbt_node_state: None,           //todo: implement
            default_schema: None,           //todo: implement
            compare_unrendered_code: false, //todo: implement
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubmitValuesRequestInput {
    pub target_table: String,
    pub dialect: String,
    pub default_catalog: String,
    pub values_hash: String,
    pub semantic_extras: SemanticExtras,
    pub last_modified_epoch: Option<i64>,
    pub labels: HashMap<String, String>,
    pub clone_time_travel_limit: Option<i64>,
    pub clone_table_properties: Option<TableProperties>,
}

impl SubmitValuesRequestInput {
    pub fn into_proto(self) -> SubmitValuesRequest {
        SubmitValuesRequest {
            target_table: self.target_table,
            dialect: self.dialect,
            default_catalog: self.default_catalog,
            values_hash: self.values_hash,
            semantic_extras: self.semantic_extras,
            last_modified_epoch: self.last_modified_epoch,
            labels: self.labels,
            clone_time_travel_limit: self.clone_time_travel_limit,
            clone_table_properties: self.clone_table_properties,
            clone_chain_depth_limit: None, //todo: implement
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionOutcomeInput {
    pub last_modified_epoch: Option<i64>,
    pub table_type: Option<String>,
    pub execution_runtime_ms: Option<i64>,
}

impl ExecutionOutcomeInput {
    pub fn into_proto(self) -> ExecutionOutcome {
        ExecutionOutcome {
            last_modified_epoch: self.last_modified_epoch,
            table_type: self.table_type,
            execution_results: None,
            execution_runtime_ms: self.execution_runtime_ms,
        }
    }
}

pub fn sql_execution_record_from_submit_request(
    request: SubmitEnrichedSqlRequest,
    outcome: ExecutionOutcomeInput,
) -> ExecutionRecord {
    ExecutionRecord {
        outcome: Some(outcome.into_proto()),
        input: Some(execution_record::Input::EnrichedSql(Box::new(
            crate::proto::query_cache::SqlExecution {
                target_table: request.target_table,
                dialect: request.dialect,
                default_catalog: request.default_catalog,
                execution_type: request.execution_type,
                sql: request.sql,
                tables: request.tables,
                query_dependencies: request.query_dependencies,
                semantic_extras: request.semantic_extras,
                labels: request.labels,
                default_schema: None, //todo: implement
                dbt_node_state: None, //todo: implement
            },
        ))),
    }
}

pub fn values_execution_record_from_submit_request(
    request: SubmitValuesRequest,
    outcome: ExecutionOutcomeInput,
) -> ExecutionRecord {
    ExecutionRecord {
        outcome: Some(outcome.into_proto()),
        input: Some(execution_record::Input::Values(Box::new(ValuesExecution {
            target_table: request.target_table,
            dialect: request.dialect,
            default_catalog: request.default_catalog,
            values_hash: request.values_hash,
            semantic_extras: request.semantic_extras,
            labels: request.labels,
        }))),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloneRequestInput {
    pub target_table: String,
    pub dialect: String,
    pub default_catalog: String,
    pub execution_type: ModelExecutionType,
    pub clone_source_table: String,
    pub clone_source_last_modified_epoch: Option<i64>,
    pub labels: HashMap<String, String>,
    pub clone_source_table_type: Option<String>,
    pub table_properties: Option<TableProperties>,
}

impl CloneRequestInput {
    pub fn into_proto(self) -> CloneRequest {
        CloneRequest {
            target_table: self.target_table,
            dialect: self.dialect,
            default_catalog: self.default_catalog,
            execution_type: self.execution_type as i32,
            clone_source_table: self.clone_source_table,
            clone_source_last_modified_epoch: self.clone_source_last_modified_epoch,
            labels: self.labels,
            clone_source_table_type: self.clone_source_table_type,
            table_properties: self.table_properties,
            clone_chain_depth_limit: None, //todo: implement
        }
    }
}

pub fn seed_values_hash(bytes: impl AsRef<[u8]>) -> String {
    format!("{:x}", md5::compute(bytes))
}

pub fn seed_values_hash_reader(mut reader: impl Read) -> Result<String, RequestBuildError> {
    let mut context = md5::Context::new();
    let mut buffer = [0; HASH_READ_CHUNK_SIZE];

    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        context.consume(&buffer[..read]);
    }

    Ok(format!("{:x}", context.compute()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn labels_match_python_client_keys() {
        let labels = NodeIdentity {
            name: "orders".to_string(),
            fqn: vec![
                "jaffle_shop".to_string(),
                "marts".to_string(),
                "orders".to_string(),
            ],
            unique_id: "model.jaffle_shop.orders".to_string(),
        }
        .labels();

        assert_eq!(labels.get("dbt_node_name").unwrap(), "orders");
        assert_eq!(
            labels.get("dbt_node_fqn").unwrap(),
            "jaffle_shop.marts.orders"
        );
        assert_eq!(
            labels.get("dbt_node_unique_id").unwrap(),
            "model.jaffle_shop.orders"
        );
        assert_eq!(labels.len(), 3);
    }

    #[test]
    fn sql_semantic_extras_match_python_key_surface_and_json_values() {
        let mut config = SemanticExtraConfig::new();
        config.insert(
            "on_schema_change".to_string(),
            Some(json!("sync_all_columns")),
        );
        config.insert(
            "incremental_predicates".to_string(),
            Some(json!(["updated_at >= current_date", "deleted_at is null"])),
        );
        config.insert("merge_update_columns".to_string(), None);
        config.insert("unique_key".to_string(), Some(json!("id")));

        let extras = sql_semantic_extras(&config).unwrap();

        assert_eq!(
            extras.get("on_schema_change").unwrap(),
            "\"sync_all_columns\""
        );
        assert_eq!(
            extras.get("incremental_predicates").unwrap(),
            "[\"updated_at >= current_date\",\"deleted_at is null\"]"
        );
        assert_eq!(extras.get("merge_update_columns").unwrap(), "");
        assert!(!extras.contains_key("unique_key"));
    }

    #[test]
    fn sql_semantic_extras_include_databricks_attributes() {
        let mut config = SemanticExtraConfig::new();
        config.insert("auto_liquid_cluster".to_string(), Some(json!(true)));
        config.insert(
            "databricks_tags".to_string(),
            Some(json!({"team": "analytics"})),
        );

        let extras = sql_semantic_extras(&config).unwrap();

        assert_eq!(extras.get("auto_liquid_cluster").unwrap(), "true");
        assert_eq!(
            extras.get("databricks_tags").unwrap(),
            "{\"team\":\"analytics\"}"
        );
    }

    #[test]
    fn seed_semantic_extras_match_python_key_surface() {
        let mut config = SemanticExtraConfig::new();
        config.insert("column_types".to_string(), Some(json!({"id": "integer"})));
        config.insert("quote_columns".to_string(), Some(json!(true)));
        config.insert("delimiter".to_string(), Some(json!("|")));
        config.insert("on_schema_change".to_string(), Some(json!("ignored")));

        let extras = seed_semantic_extras(&config).unwrap();

        assert_eq!(extras.get("column_types").unwrap(), "{\"id\":\"integer\"}");
        assert_eq!(extras.get("quote_columns").unwrap(), "true");
        assert_eq!(extras.get("delimiter").unwrap(), "\"|\"");
        assert_eq!(extras.len(), 3);
    }

    #[test]
    fn execution_type_mapping_matches_python_client() {
        assert_eq!(
            execution_type_from_input(&ExecutionTypeInput {
                resource_type: NodeType::Model,
                is_view: true,
                is_custom_materialization: false,
                is_incremental: false,
                full_refresh: false,
                incremental_strategy: None,
                has_unique_key: false,
            })
            .unwrap(),
            ModelExecutionType::View
        );
        assert_eq!(
            execution_type_from_input(&ExecutionTypeInput {
                resource_type: NodeType::Snapshot,
                is_view: false,
                is_custom_materialization: false,
                is_incremental: false,
                full_refresh: false,
                incremental_strategy: None,
                has_unique_key: false,
            })
            .unwrap(),
            ModelExecutionType::Snapshot
        );
        assert_eq!(
            execution_type_from_input(&ExecutionTypeInput {
                resource_type: NodeType::Model,
                is_view: false,
                is_custom_materialization: false,
                is_incremental: true,
                full_refresh: false,
                incremental_strategy: Some("delete+insert".to_string()),
                has_unique_key: true,
            })
            .unwrap(),
            ModelExecutionType::DeleteInsert
        );
        assert_eq!(
            execution_type_from_input(&ExecutionTypeInput {
                resource_type: NodeType::Model,
                is_view: false,
                is_custom_materialization: false,
                is_incremental: true,
                full_refresh: false,
                incremental_strategy: Some("merge".to_string()),
                has_unique_key: false,
            })
            .unwrap(),
            ModelExecutionType::Append
        );
        assert_eq!(
            execution_type_from_input(&ExecutionTypeInput {
                resource_type: NodeType::Model,
                is_view: false,
                is_custom_materialization: false,
                is_incremental: true,
                full_refresh: true,
                incremental_strategy: Some("merge".to_string()),
                has_unique_key: true,
            })
            .unwrap(),
            ModelExecutionType::Full
        );
        assert_eq!(
            execution_type_from_input(&ExecutionTypeInput {
                resource_type: NodeType::Test,
                is_view: false,
                is_custom_materialization: false,
                is_incremental: false,
                full_refresh: false,
                incremental_strategy: None,
                has_unique_key: false,
            })
            .unwrap(),
            ModelExecutionType::DbtDataTest
        );
    }

    #[test]
    fn unknown_incremental_strategy_is_an_error_for_fail_open_callers() {
        let err = execution_type_from_input(&ExecutionTypeInput {
            resource_type: NodeType::Model,
            is_view: false,
            is_custom_materialization: false,
            is_incremental: true,
            full_refresh: false,
            incremental_strategy: Some("custom_strategy".to_string()),
            has_unique_key: true,
        })
        .unwrap_err();

        assert!(err.to_string().contains("CUSTOM_STRATEGY"));
    }

    #[test]
    fn request_inputs_convert_to_proto_messages() {
        let labels = NodeIdentity {
            name: "orders".to_string(),
            fqn: vec!["pkg".to_string(), "orders".to_string()],
            unique_id: "model.pkg.orders".to_string(),
        }
        .labels();
        let request = SubmitEnrichedSqlRequestInput {
            target_table: Some("analytics.orders".to_string()),
            dialect: "snowflake".to_string(),
            default_catalog: "analytics".to_string(),
            execution_type: ModelExecutionType::Merge,
            sql: "select * from raw.orders".to_string(),
            tables: vec![TableModifiedInfo {
                name: "raw.orders".to_string(),
                last_modified_epoch: Some(123),
            }],
            query_dependencies: vec![QueryDependency {
                name: "raw.order_view".to_string(),
                query: "select * from raw.orders".to_string(),
                default_catalog: "analytics".to_string(),
                default_schema: "raw".to_string(),
            }],
            semantic_extras: HashMap::from([(
                "on_schema_change".to_string(),
                "\"sync_all_columns\"".to_string(),
            )]),
            freshness_tolerance_seconds: 2700,
            lenient_dependencies: vec!["raw.orders".to_string()],
            tolerate_nondeterminism: true,
            labels: labels.clone(),
            clone_time_travel_limit: None,
            clone_table_properties: Some(TableProperties {
                hours_to_expiration: Some(12),
                partition_expiration_days: None,
            }),
            stale_upstream_policy: StaleUpstreamPolicy::Any,
        }
        .into_proto();

        assert_eq!(request.target_table.as_deref(), Some("analytics.orders"));
        assert_eq!(request.execution_type, ModelExecutionType::Merge as i32);
        assert_eq!(request.labels, labels);
        assert_eq!(request.tables[0].last_modified_epoch, Some(123));
        assert_eq!(
            request.clone_table_properties.unwrap().hours_to_expiration,
            Some(12)
        );
        assert_eq!(
            request.stale_upstream_policy,
            StaleUpstreamPolicy::Any as i32
        );
    }

    #[test]
    fn values_request_input_converts_to_proto_message() {
        let request = SubmitValuesRequestInput {
            target_table: "analytics.seed_orders".to_string(),
            dialect: "bigquery".to_string(),
            default_catalog: "analytics".to_string(),
            values_hash: seed_values_hash(b"id,name\n1,Ada\n"),
            semantic_extras: HashMap::from([("delimiter".to_string(), "\",\"".to_string())]),
            last_modified_epoch: Some(456),
            labels: HashMap::new(),
            clone_time_travel_limit: Some(3600),
            clone_table_properties: None,
        }
        .into_proto();

        assert_eq!(request.target_table, "analytics.seed_orders");
        assert_eq!(request.values_hash, "6c1abef29d8c78fb8e696f94546a3918");
        assert_eq!(request.last_modified_epoch, Some(456));
        assert_eq!(request.clone_time_travel_limit, Some(3600));
    }

    #[test]
    fn sql_record_drops_cache_decision_only_fields() {
        let request = SubmitEnrichedSqlRequestInput {
            target_table: Some("analytics.orders".to_string()),
            dialect: "snowflake".to_string(),
            default_catalog: "analytics".to_string(),
            execution_type: ModelExecutionType::Merge,
            sql: "select * from raw.orders".to_string(),
            tables: vec![TableModifiedInfo {
                name: "raw.orders".to_string(),
                last_modified_epoch: Some(123),
            }],
            query_dependencies: Vec::new(),
            semantic_extras: HashMap::new(),
            freshness_tolerance_seconds: 2700,
            lenient_dependencies: vec!["raw.orders".to_string()],
            tolerate_nondeterminism: true,
            labels: HashMap::from([("dbt_node_name".to_string(), "orders".to_string())]),
            clone_time_travel_limit: Some(3600),
            clone_table_properties: None,
            stale_upstream_policy: StaleUpstreamPolicy::Any,
        }
        .into_proto();

        let record = sql_execution_record_from_submit_request(
            request,
            ExecutionOutcomeInput {
                last_modified_epoch: Some(456),
                table_type: Some("TABLE".to_string()),
                execution_runtime_ms: Some(789),
            },
        );

        assert_eq!(record.outcome.unwrap().last_modified_epoch, Some(456));
        let Some(execution_record::Input::EnrichedSql(sql)) = record.input else {
            panic!("expected SQL execution input");
        };
        assert_eq!(sql.target_table.as_deref(), Some("analytics.orders"));
        assert_eq!(sql.execution_type, ModelExecutionType::Merge as i32);
        assert_eq!(sql.labels.get("dbt_node_name").unwrap(), "orders");
    }

    #[test]
    fn values_record_moves_last_modified_to_outcome() {
        let request = SubmitValuesRequestInput {
            target_table: "analytics.seed_orders".to_string(),
            dialect: "bigquery".to_string(),
            default_catalog: "analytics".to_string(),
            values_hash: "abc123".to_string(),
            semantic_extras: HashMap::new(),
            last_modified_epoch: Some(123),
            labels: HashMap::new(),
            clone_time_travel_limit: Some(3600),
            clone_table_properties: None,
        }
        .into_proto();

        let record = values_execution_record_from_submit_request(
            request,
            ExecutionOutcomeInput {
                last_modified_epoch: Some(456),
                table_type: None,
                execution_runtime_ms: None,
            },
        );

        assert_eq!(record.outcome.unwrap().last_modified_epoch, Some(456));
        let Some(execution_record::Input::Values(values)) = record.input else {
            panic!("expected values execution input");
        };
        assert_eq!(values.target_table, "analytics.seed_orders");
        assert_eq!(values.values_hash, "abc123");
    }

    #[test]
    fn clone_request_input_converts_to_proto_message() {
        let labels = HashMap::from([(
            "dbt_node_unique_id".to_string(),
            "model.pkg.orders".to_string(),
        )]);
        let properties = TableProperties {
            hours_to_expiration: Some(12),
            partition_expiration_days: None,
        };
        let request = CloneRequestInput {
            target_table: "dev.analytics.orders".to_string(),
            dialect: "snowflake".to_string(),
            default_catalog: "dev".to_string(),
            execution_type: ModelExecutionType::Merge,
            clone_source_table: "prod.analytics.orders".to_string(),
            clone_source_last_modified_epoch: Some(123),
            labels: labels.clone(),
            clone_source_table_type: Some("table".to_string()),
            table_properties: Some(properties),
        }
        .into_proto();

        assert_eq!(request.target_table, "dev.analytics.orders");
        assert_eq!(request.execution_type, ModelExecutionType::Merge as i32);
        assert_eq!(request.clone_source_table, "prod.analytics.orders");
        assert_eq!(request.clone_source_last_modified_epoch, Some(123));
        assert_eq!(request.labels, labels);
        assert_eq!(request.clone_source_table_type.as_deref(), Some("table"));
        assert_eq!(request.table_properties, Some(properties));
    }

    #[test]
    fn seed_values_hash_matches_md5_hex_and_streaming_reader() {
        let seed_bytes = b"id,name\n1,Ada\n2,Grace\n";
        let expected = "e146991e1c07585745c5a65f06a517e9";

        assert_eq!(seed_values_hash(seed_bytes), expected);
        assert_eq!(seed_values_hash_reader(&seed_bytes[..]).unwrap(), expected);
    }
}
