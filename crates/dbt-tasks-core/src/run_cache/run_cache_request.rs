//! Translates `dbt-tasks` runtime state into dbt State service requests.
//!
//! This module owns extraction from dbt schema/task types, adapter-specific
//! relation rendering, seed file hashing, and task-layer context assembly before
//! lowering into the stable request-builder inputs in `dbt-state`.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;

use dbt_adapter::relation::create_relation_from_node;
use dbt_adapter_core::AdapterType;
use dbt_common::{ErrorCode, FsResult, fs_err};
use dbt_schemas::schemas::common::{DbtMaterialization, OnSchemaChange};
use dbt_schemas::schemas::project::{ModelConfig, SeedConfig, SnapshotConfig};
use dbt_schemas::schemas::{
    DbtModel, DbtSeed, DbtSnapshot, DbtTest, InternalDbtNode, InternalDbtNodeAttributes,
};
use dbt_state::proto::query_cache::{
    QueryDependency, StaleUpstreamPolicy, SubmitEnrichedSqlRequest, SubmitValuesRequest,
    TableModifiedInfo, TableProperties,
};
use dbt_state::request_builder::{
    ExecutionTypeInput, NodeIdentity, RequestBuildError, SemanticExtraConfig, SemanticExtras,
    SubmitEnrichedSqlRequestInput, SubmitValuesRequestInput, execution_type_from_input,
    seed_semantic_extras, seed_values_hash_reader, sql_semantic_extras,
};
use dbt_telemetry::NodeType;
use serde::Serialize;
use serde_json::Value;

#[derive(Clone, Debug)]
/// Execution-time SQL request inputs assembled by `dbt-tasks`.
///
/// This stays separate from the `dbt-state` request-builder input so the
/// `dbt-state` crate only sees normalized values and does not depend on task/schema
/// runtime types.
pub struct SqlRunCacheRequestContext {
    pub adapter_type: AdapterType,
    pub dialect: String,
    pub sql: String,
    pub tables: Vec<TableModifiedInfo>,
    pub query_dependencies: Vec<QueryDependency>,
    pub freshness_tolerance_seconds: i64,
    pub lenient_dependencies: Vec<String>,
    pub tolerate_nondeterminism: bool,
    pub full_refresh: bool,
    pub clone_time_travel_limit: Option<i64>,
    pub clone_table_properties: Option<TableProperties>,
    /// How the service should aggregate per-dependency freshness checks for
    /// this request. Derived from the model's
    /// `freshness.build_after.updates_on` config (defaults to ANY).
    pub stale_upstream_policy: StaleUpstreamPolicy,
}

#[derive(Clone, Debug)]
/// Execution-time seed request inputs assembled by `dbt-tasks`.
///
/// This context may include task-layer details, such as the project root, before
/// being lowered into the stable `dbt-state` request-builder input.
pub struct SeedRunCacheRequestContext<'a> {
    pub adapter_type: AdapterType,
    pub dialect: String, // TODO: remove because redundant when adapter_type is present in context
    pub project_root: &'a Path,
    pub last_modified_epoch: Option<i64>,
    pub clone_time_travel_limit: Option<i64>,
    pub clone_table_properties: Option<TableProperties>,
}

pub fn build_model_sql_request(
    model: &DbtModel,
    context: SqlRunCacheRequestContext,
) -> FsResult<SubmitEnrichedSqlRequest> {
    let execution_type =
        execution_type_from_input(&model_execution_type_input(model, context.full_refresh))
            .map_err(request_build_error)?;
    let semantic_extras =
        sql_semantic_extras(&model_sql_semantic_extra_config(model).map_err(request_build_error)?)
            .map_err(request_build_error)?;

    Ok(build_sql_request_input(model, context, execution_type, semantic_extras)?.into_proto())
}

pub fn build_snapshot_sql_request(
    snapshot: &DbtSnapshot,
    context: SqlRunCacheRequestContext,
) -> FsResult<SubmitEnrichedSqlRequest> {
    let execution_type = execution_type_from_input(&snapshot_execution_type_input(
        snapshot,
        context.full_refresh,
    ))
    .map_err(request_build_error)?;

    Ok(
        build_sql_request_input(snapshot, context, execution_type, Default::default())?
            .into_proto(),
    )
}

pub fn build_test_sql_request(
    test: &DbtTest,
    context: SqlRunCacheRequestContext,
) -> FsResult<SubmitEnrichedSqlRequest> {
    let execution_type =
        execution_type_from_input(&test_execution_type_input()).map_err(request_build_error)?;

    // Mirrors dbt-core's `_build_submit_enriched_sql_request` (run_cache.py
    // L992-996): data tests submit with `target_table=None` so the service's
    // `_no_target_table_expr` branch fires and Skip can be returned without
    // requiring the audit relation to exist in the warehouse. With a
    // populated target_table here, the service would require it to be in
    // the request's `tables` list with a `last_modified_epoch` to mark the
    // target as "existing" — which is not possible when
    // `store_failures_as=None` skips the audit relation materialization.
    let mut input = build_sql_request_input(test, context, execution_type, Default::default())?;
    input.target_table = None;
    Ok(input.into_proto())
}

pub fn build_seed_values_request(
    seed: &DbtSeed,
    context: SeedRunCacheRequestContext<'_>,
) -> FsResult<SubmitValuesRequest> {
    let seed_file = context
        .project_root
        .join(seed.common().original_file_path.as_path());
    let values_hash = seed_values_hash_reader(File::open(&seed_file).map_err(|e| {
        fs_err!(
            ErrorCode::IoError,
            "Failed to open seed file for dbt State request {}: {}",
            seed_file.display(),
            e
        )
    })?)
    .map_err(request_build_error)?;

    let semantic_extras =
        seed_semantic_extras(&seed_semantic_extra_config(seed).map_err(request_build_error)?)
            .map_err(request_build_error)?;

    Ok(SubmitValuesRequestInput {
        target_table: target_table_for_node(context.adapter_type, seed)?,
        dialect: context.dialect,
        default_catalog: seed.database(),
        values_hash,
        semantic_extras,
        last_modified_epoch: context.last_modified_epoch,
        labels: node_identity(seed).labels(),
        clone_time_travel_limit: context.clone_time_travel_limit,
        clone_table_properties: context.clone_table_properties,
    }
    .into_proto())
}

pub fn node_identity(node: &dyn InternalDbtNodeAttributes) -> NodeIdentity {
    NodeIdentity {
        name: node.common().name.clone(),
        fqn: node.common().fqn.clone(),
        unique_id: node.common().unique_id.clone(),
    }
}

pub fn target_table_for_node(
    adapter_type: AdapterType,
    node: &dyn InternalDbtNodeAttributes,
) -> FsResult<String> {
    Ok(create_relation_from_node(adapter_type, node, None)?.semantic_fqn())
}

pub fn model_execution_type_input(model: &DbtModel, full_refresh: bool) -> ExecutionTypeInput {
    let materialized = &model.base().materialized;
    ExecutionTypeInput {
        resource_type: NodeType::Model,
        is_view: materialized == &DbtMaterialization::View,
        is_custom_materialization: matches!(materialized, DbtMaterialization::Unknown(_)),
        is_incremental: materialized == &DbtMaterialization::Incremental,
        full_refresh,
        incremental_strategy: model_incremental_strategy(model),
        has_unique_key: model.deprecated_config.unique_key.is_some(),
    }
}

pub fn snapshot_execution_type_input(
    _snapshot: &DbtSnapshot,
    full_refresh: bool,
) -> ExecutionTypeInput {
    ExecutionTypeInput {
        resource_type: NodeType::Snapshot,
        is_view: false,
        is_custom_materialization: false,
        is_incremental: false,
        full_refresh,
        incremental_strategy: None,
        has_unique_key: false,
    }
}

pub fn test_execution_type_input() -> ExecutionTypeInput {
    ExecutionTypeInput {
        resource_type: NodeType::Test,
        is_view: false,
        is_custom_materialization: false,
        is_incremental: false,
        full_refresh: false,
        incremental_strategy: None,
        has_unique_key: false,
    }
}

pub fn model_sql_semantic_extra_config(
    model: &DbtModel,
) -> Result<SemanticExtraConfig, RequestBuildError> {
    let mut extras = model_config_sql_semantic_extra_config(&model.deprecated_config)?;

    // The Python plugin emits `on_schema_change` only for `table` and
    // `incremental` materializations, falling back to the dbt-core default
    // ("ignore") when unset. Mirror that here so semantic hashes match —
    // other materializations don't carry this field over the wire even when
    // the user sets it explicitly.
    let materialized = &model.base().materialized;
    let emits_on_schema_change = matches!(
        materialized,
        DbtMaterialization::Table | DbtMaterialization::Incremental
    );
    if emits_on_schema_change {
        if !extras.contains_key("on_schema_change") {
            extras.insert(
                "on_schema_change".to_string(),
                Some(serde_json::to_value(OnSchemaChange::default())?),
            );
        }
    } else {
        extras.remove("on_schema_change");
    }

    Ok(extras)
}

pub fn seed_semantic_extra_config(
    seed: &DbtSeed,
) -> Result<SemanticExtraConfig, RequestBuildError> {
    seed_config_semantic_extra_config(&seed.deprecated_config)
}

pub fn model_clone_table_properties(model: &DbtModel) -> Option<TableProperties> {
    model_config_clone_table_properties(&model.deprecated_config)
}

pub fn snapshot_clone_table_properties(snapshot: &DbtSnapshot) -> Option<TableProperties> {
    snapshot_config_clone_table_properties(&snapshot.deprecated_config)
}

pub fn seed_clone_table_properties(seed: &DbtSeed) -> Option<TableProperties> {
    seed_config_clone_table_properties(&seed.deprecated_config)
}

fn build_sql_request_input(
    node: &dyn InternalDbtNodeAttributes,
    context: SqlRunCacheRequestContext,
    execution_type: dbt_state::proto::query_cache::ModelExecutionType,
    semantic_extras: SemanticExtras,
) -> FsResult<SubmitEnrichedSqlRequestInput> {
    Ok(SubmitEnrichedSqlRequestInput {
        target_table: Some(target_table_for_node(context.adapter_type, node)?),
        dialect: context.dialect,
        default_catalog: node.database(),
        execution_type,
        sql: context.sql,
        tables: context.tables,
        query_dependencies: context.query_dependencies,
        semantic_extras,
        freshness_tolerance_seconds: context.freshness_tolerance_seconds,
        lenient_dependencies: context.lenient_dependencies,
        tolerate_nondeterminism: context.tolerate_nondeterminism,
        labels: node_identity(node).labels(),
        clone_time_travel_limit: context.clone_time_travel_limit,
        clone_table_properties: context.clone_table_properties,
        stale_upstream_policy: context.stale_upstream_policy,
    })
}

fn model_incremental_strategy(model: &DbtModel) -> Option<String> {
    model
        .__model_attr__
        .incremental_strategy
        .as_ref()
        .or(model.deprecated_config.incremental_strategy.as_ref())
        .map(ToString::to_string)
}

fn model_config_sql_semantic_extra_config(
    config: &ModelConfig,
) -> Result<SemanticExtraConfig, RequestBuildError> {
    let mut extras = SemanticExtraConfig::new();

    insert_json(
        &mut extras,
        "on_schema_change",
        config.on_schema_change.as_ref(),
    )?;
    insert_json(
        &mut extras,
        "incremental_predicates",
        config.incremental_predicates.as_ref(),
    )?;
    insert_json(
        &mut extras,
        "merge_update_columns",
        config.merge_update_columns.as_ref(),
    )?;
    insert_json(
        &mut extras,
        "merge_exclude_columns",
        config.merge_exclude_columns.as_ref(),
    )?;

    Ok(extras)
}

fn seed_config_semantic_extra_config(
    config: &SeedConfig,
) -> Result<SemanticExtraConfig, RequestBuildError> {
    let mut extras = SemanticExtraConfig::new();

    if let Some(column_types) = config.column_types.as_ref() {
        let plain_column_types: BTreeMap<String, String> = column_types
            .iter()
            .map(|(name, data_type)| (name.clone().into_inner(), data_type.clone()))
            .collect();
        extras.insert(
            "column_types".to_string(),
            Some(serde_json::to_value(plain_column_types)?),
        );
    }
    insert_json(&mut extras, "quote_columns", config.quote_columns.as_ref())?;
    if let Some(delimiter) = config.delimiter.as_ref() {
        extras.insert(
            "delimiter".to_string(),
            Some(Value::String(delimiter.clone().into_inner())),
        );
    }

    Ok(extras)
}

fn model_config_clone_table_properties(config: &ModelConfig) -> Option<TableProperties> {
    clone_table_properties_from_values(
        config.__warehouse_specific_config__.hours_to_expiration,
        config
            .__warehouse_specific_config__
            .partition_expiration_days,
    )
}

fn snapshot_config_clone_table_properties(config: &SnapshotConfig) -> Option<TableProperties> {
    clone_table_properties_from_values(
        config.__warehouse_specific_config__.hours_to_expiration,
        config
            .__warehouse_specific_config__
            .partition_expiration_days,
    )
}

fn seed_config_clone_table_properties(config: &SeedConfig) -> Option<TableProperties> {
    clone_table_properties_from_values(
        config.__warehouse_specific_config__.hours_to_expiration,
        config
            .__warehouse_specific_config__
            .partition_expiration_days,
    )
}

fn clone_table_properties_from_values(
    hours_to_expiration: Option<u64>,
    partition_expiration_days: Option<u64>,
) -> Option<TableProperties> {
    let hours_to_expiration = positive_i32(hours_to_expiration);
    let partition_expiration_days = positive_i32(partition_expiration_days);

    if hours_to_expiration.is_none() && partition_expiration_days.is_none() {
        return None;
    }

    Some(TableProperties {
        hours_to_expiration,
        partition_expiration_days,
    })
}

fn positive_i32(value: Option<u64>) -> Option<i32> {
    value
        .and_then(|value| i32::try_from(value).ok())
        .filter(|value| *value > 0)
}

fn insert_json<T: Serialize>(
    extras: &mut SemanticExtraConfig,
    key: &str,
    value: Option<&T>,
) -> Result<(), RequestBuildError> {
    if let Some(value) = value {
        extras.insert(key.to_string(), Some(serde_json::to_value(value)?));
    }
    Ok(())
}

fn request_build_error(error: RequestBuildError) -> Box<dbt_common::FsError> {
    fs_err!(
        ErrorCode::Generic,
        "Failed to build dbt State request: {}",
        error
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_common::io_args::StaticAnalysisKind;
    use dbt_common::path::DbtPath;
    use dbt_schemas::schemas::common::{
        Access, DbtIncrementalStrategy, DbtMaterialization, DbtUniqueKey, OnSchemaChange,
        ResolvedQuoting,
    };
    use dbt_schemas::schemas::nodes::AdapterAttr;
    use dbt_schemas::schemas::serde::StringOrArrayOfStrings;
    use dbt_schemas::schemas::{
        CommonAttributes, DbtModelAttr, DbtSeedAttr, DbtSnapshotAttr, IntrospectionKind,
        NodeBaseAttributes,
    };
    use dbt_state::proto::query_cache::ModelExecutionType;
    use dbt_state::request_builder::{execution_type_from_input, seed_values_hash};
    use dbt_yaml::Spanned;
    use indexmap::IndexMap;
    use std::collections::BTreeMap;

    fn make_common(unique_id: &str, name: &str) -> CommonAttributes {
        CommonAttributes {
            unique_id: unique_id.to_string(),
            name: name.to_string(),
            package_name: "jaffle_shop".to_string(),
            fqn: vec!["jaffle_shop".to_string(), name.to_string()],
            original_file_path: DbtPath::from(format!("models/{name}.sql")),
            tags: vec![],
            meta: IndexMap::new(),
            ..Default::default()
        }
    }

    fn make_base(materialized: DbtMaterialization, alias: &str) -> NodeBaseAttributes {
        NodeBaseAttributes {
            database: "analytics".to_string(),
            schema: "marts".to_string(),
            alias: alias.to_string(),
            materialized,
            quoting: ResolvedQuoting::trues(),
            static_analysis: Spanned::new(StaticAnalysisKind::On),
            enabled: true,
            ..Default::default()
        }
    }

    fn make_model(materialized: DbtMaterialization) -> DbtModel {
        DbtModel {
            __common_attr__: make_common("model.jaffle_shop.orders", "orders"),
            __base_attr__: make_base(materialized, "orders"),
            __model_attr__: DbtModelAttr {
                access: Access::default(),
                introspection: IntrospectionKind::None,
                incremental_strategy: Some(DbtIncrementalStrategy::Merge),
                ..Default::default()
            },
            __adapter_attr__: AdapterAttr::default(),
            deprecated_config: ModelConfig {
                incremental_strategy: Some(DbtIncrementalStrategy::Merge),
                incremental_predicates: Some(vec![
                    "updated_at >= current_date".to_string(),
                    "deleted_at is null".to_string(),
                ]),
                unique_key: Some(DbtUniqueKey::Single("id".to_string())),
                on_schema_change: Some(OnSchemaChange::SyncAllColumns),
                merge_update_columns: Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                    "status".to_string(),
                    "updated_at".to_string(),
                ])),
                ..Default::default()
            },
            __other__: BTreeMap::new(),
        }
    }

    fn make_snapshot() -> DbtSnapshot {
        DbtSnapshot {
            __common_attr__: make_common("snapshot.jaffle_shop.orders_snapshot", "orders_snapshot"),
            __base_attr__: make_base(DbtMaterialization::Snapshot, "orders_snapshot"),
            __snapshot_attr__: DbtSnapshotAttr::default(),
            __adapter_attr__: AdapterAttr::default(),
            ..Default::default()
        }
    }

    fn make_seed() -> DbtSeed {
        let mut column_types = BTreeMap::new();
        column_types.insert(Spanned::new("id".to_string()), "integer".to_string());

        DbtSeed {
            __common_attr__: CommonAttributes {
                original_file_path: DbtPath::from("seeds/cities.csv"),
                ..make_common("seed.jaffle_shop.cities", "cities")
            },
            __base_attr__: make_base(DbtMaterialization::Seed, "cities"),
            __seed_attr__: DbtSeedAttr::default(),
            deprecated_config: SeedConfig {
                column_types: Some(column_types),
                quote_columns: Some(true),
                delimiter: Some(Spanned::new("|".to_string())),
                ..Default::default()
            },
            __other__: BTreeMap::new(),
        }
    }

    fn sql_context(full_refresh: bool) -> SqlRunCacheRequestContext {
        SqlRunCacheRequestContext {
            adapter_type: AdapterType::Snowflake,
            dialect: "snowflake".to_string(),
            sql: "select * from raw.orders".to_string(),
            tables: vec![TableModifiedInfo {
                name: "raw.orders".to_string(),
                last_modified_epoch: 123,
            }],
            query_dependencies: vec![],
            freshness_tolerance_seconds: 2700,
            lenient_dependencies: vec![],
            tolerate_nondeterminism: true,
            full_refresh,
            clone_time_travel_limit: None,
            clone_table_properties: None,
            stale_upstream_policy: StaleUpstreamPolicy::Any,
        }
    }

    #[test]
    fn model_request_uses_fusion_node_identity_target_and_semantic_extras() {
        let model = make_model(DbtMaterialization::Incremental);
        let request = build_model_sql_request(&model, sql_context(false)).unwrap();

        assert_eq!(
            request.target_table.as_deref(),
            Some(r#""analytics"."marts"."orders""#)
        );
        assert_eq!(request.default_catalog, "analytics");
        assert_eq!(request.execution_type, ModelExecutionType::Merge as i32);
        assert_eq!(
            request.labels.get("dbt_node_unique_id").unwrap(),
            "model.jaffle_shop.orders"
        );
        assert_eq!(
            request.labels.get("dbt_node_fqn").unwrap(),
            "jaffle_shop.orders"
        );
        assert_eq!(
            request.semantic_extras.get("on_schema_change").unwrap(),
            "\"sync_all_columns\""
        );
        assert_eq!(
            request
                .semantic_extras
                .get("incremental_predicates")
                .unwrap(),
            "[\"updated_at >= current_date\",\"deleted_at is null\"]"
        );
        assert_eq!(
            request.semantic_extras.get("merge_update_columns").unwrap(),
            "[\"status\",\"updated_at\"]"
        );
        assert!(!request.semantic_extras.contains_key("unique_key"));
    }

    #[test]
    fn table_and_incremental_default_on_schema_change_to_ignore_when_unset() {
        for materialization in [DbtMaterialization::Table, DbtMaterialization::Incremental] {
            let mut model = make_model(materialization.clone());
            model.deprecated_config.on_schema_change = None;
            model.deprecated_config.incremental_predicates = None;
            model.deprecated_config.merge_update_columns = None;

            let request = build_model_sql_request(&model, sql_context(false)).unwrap();

            assert_eq!(
                request.semantic_extras.get("on_schema_change").unwrap(),
                "\"ignore\"",
                "{materialization:?}"
            );
            assert!(
                !request
                    .semantic_extras
                    .contains_key("incremental_predicates"),
                "{materialization:?}"
            );
            assert!(
                !request.semantic_extras.contains_key("merge_update_columns"),
                "{materialization:?}"
            );
        }
    }

    #[test]
    fn view_model_omits_on_schema_change_even_when_set() {
        let mut model = make_model(DbtMaterialization::View);
        model.deprecated_config.on_schema_change = Some(OnSchemaChange::SyncAllColumns);

        let request = build_model_sql_request(&model, sql_context(false)).unwrap();

        assert!(!request.semantic_extras.contains_key("on_schema_change"));
    }

    #[test]
    fn model_execution_type_maps_merge_without_unique_key_to_append() {
        let mut model = make_model(DbtMaterialization::Incremental);
        model.deprecated_config.unique_key = None;

        let execution_type =
            execution_type_from_input(&model_execution_type_input(&model, false)).unwrap();

        assert_eq!(execution_type, ModelExecutionType::Append);
    }

    #[test]
    fn model_full_refresh_suppresses_incremental_skip_type() {
        let model = make_model(DbtMaterialization::Incremental);
        let request = build_model_sql_request(&model, sql_context(true)).unwrap();

        assert_eq!(request.execution_type, ModelExecutionType::Full as i32);
    }

    #[test]
    fn model_full_refresh_keeps_view_execution_type() {
        let model = make_model(DbtMaterialization::View);
        let request = build_model_sql_request(&model, sql_context(true)).unwrap();

        assert_eq!(request.execution_type, ModelExecutionType::View as i32);
    }

    #[test]
    fn custom_materialization_maps_to_custom_execution_type() {
        let model = make_model(DbtMaterialization::Unknown(
            "my_materialization".to_string(),
        ));

        let execution_type =
            execution_type_from_input(&model_execution_type_input(&model, false)).unwrap();

        assert_eq!(execution_type, ModelExecutionType::DbtCustom);
    }

    #[test]
    fn snapshot_request_uses_snapshot_execution_type_during_full_refresh() {
        let snapshot = make_snapshot();

        let request = build_snapshot_sql_request(&snapshot, sql_context(false)).unwrap();
        assert_eq!(request.execution_type, ModelExecutionType::Snapshot as i32);
        assert_eq!(
            request.target_table.as_deref(),
            Some(r#""analytics"."marts"."orders_snapshot""#)
        );

        let request = build_snapshot_sql_request(&snapshot, sql_context(true)).unwrap();
        assert_eq!(request.execution_type, ModelExecutionType::Snapshot as i32);
    }

    #[test]
    fn seed_request_uses_md5_file_hash_and_seed_semantic_extras() {
        let tempdir = tempfile::tempdir().unwrap();
        let seeds_dir = tempdir.path().join("seeds");
        std::fs::create_dir(&seeds_dir).unwrap();
        let seed_bytes = b"id|city\n1|Chicago\n";
        std::fs::write(seeds_dir.join("cities.csv"), seed_bytes).unwrap();

        let seed = make_seed();
        let request = build_seed_values_request(
            &seed,
            SeedRunCacheRequestContext {
                adapter_type: AdapterType::Snowflake,
                dialect: "snowflake".to_string(),
                project_root: tempdir.path(),
                last_modified_epoch: Some(456),
                clone_time_travel_limit: Some(3600),
                clone_table_properties: None,
            },
        )
        .unwrap();

        assert_eq!(request.target_table, r#""analytics"."marts"."cities""#);
        assert_eq!(request.default_catalog, "analytics");
        assert_eq!(request.values_hash, seed_values_hash(seed_bytes));
        assert_eq!(request.last_modified_epoch, Some(456));
        assert_eq!(request.clone_time_travel_limit, Some(3600));
        assert_eq!(
            request.semantic_extras.get("column_types").unwrap(),
            "{\"id\":\"integer\"}"
        );
        assert_eq!(
            request.semantic_extras.get("quote_columns").unwrap(),
            "true"
        );
        assert_eq!(request.semantic_extras.get("delimiter").unwrap(), "\"|\"");
        assert_eq!(request.labels.get("dbt_node_name").unwrap(), "cities");
    }
}
