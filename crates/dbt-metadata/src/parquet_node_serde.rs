use dbt_common::constants::DBT_COMPILED_DIR_NAME;
use dbt_common::hashing::code_hash;
use dbt_common::io_args::{EvalArgs, FsCommand, IoArgs};
use dbt_common::path::get_target_write_path;
use dbt_common::serde_utils::Omissible;
use dbt_common::{CodeLocationWithFile, FsResult, stdfs};
use dbt_schemas::schemas::ResolvedCloudConfig;
use dbt_schemas::schemas::common::{
    DbtChecksum, DbtMaterialization, DbtQuoting, DbtUniqueKey, Expect, FreshnessDefinition,
    FreshnessPeriod, FreshnessRules, Given, IncludeExclude, ModelFreshnessRules, ResolvedQuoting,
    UpdatesOn,
};
use dbt_schemas::schemas::macros::{DbtDocsMacro, DbtMacro, MacroDependsOn};
use dbt_schemas::schemas::manifest::DbtOperation;
use dbt_schemas::schemas::manifest::{DbtSavedQuery, DbtSemanticModel};
use dbt_schemas::schemas::nodes::DbtGroup;
use dbt_schemas::schemas::project::SnapshotMetaColumnNames;
use dbt_schemas::schemas::project::{
    DataTestConfig, ModelConfig, SeedConfig, SnapshotConfig, SourceConfig, UnitTestConfig,
    WarehouseSpecificNodeConfig,
};
use dbt_schemas::schemas::properties::ModelFreshness;
use dbt_schemas::schemas::serde::{StringOrArrayOfStrings, StringOrInteger};
use dbt_schemas::schemas::{
    CommonAttributes, DbtExposure, DbtModel, DbtSeed, DbtSeedAttr, DbtSnapshot, DbtSnapshotAttr,
    DbtSource, DbtSourceAttr, DbtTest, DbtUnitTest, IntrospectionKind, NodeBaseAttributes, Nodes,
    TestMetadata,
};
use dbt_schemas::state::{NodeExecutionState, ResolvedNodes, ResolverState};
use dbt_yaml::Spanned;
use minijinja::machinery::Span;

// for representing dbt metadata in Rust
use indexmap::IndexMap;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use serde::Serialize;
use serde_json;

use crate::file_registry::CompleteStateWithKind;
use crate::hashing::{add_input_file_if_exists, add_session_input_files};
use crate::parquet_column::{ParquetColumnLookup, deserialize_columns};
use crate::parquet_node::{
    DefinedAtSpan, ExposureDetails, GroupDetails, InputFile, MacroArgumentDetails, MetaFields,
    ModelDetails, ModelFreshnessDetails, ParquetNode, ParquetNodeRef, ResourceType,
    SavedQueryDetails, SeedDetails, SemanticModelDetails, SessionDetails, SnapshotDetails,
    SourceDetails, SourceFreshnessDetails, TestDetails, UnitTestDetails, UserConfigs,
    WarehouseDetails, WriteContext,
};

const PARQUET_VERSION: &str = "1.0.0"; // hardcoded version of the parquet format used

// Helper function to serialize WarehouseSpecificNodeConfig to JSON string
fn serialize_warehouse_specific_config(
    config: &Option<WarehouseSpecificNodeConfig>,
) -> Option<String> {
    config.as_ref().and_then(|c| serde_json::to_string(c).ok())
}

// Helper function to deserialize WarehouseSpecificNodeConfig from JSON string
fn deserialize_warehouse_specific_config(
    json_str: &Option<String>,
) -> Option<WarehouseSpecificNodeConfig> {
    json_str.as_ref().and_then(|s| serde_json::from_str(s).ok())
}

// Helper functions to convert between different unique key types
fn dbt_unique_key_to_string_or_array(
    unique_key: &Option<DbtUniqueKey>,
) -> Option<StringOrArrayOfStrings> {
    unique_key.as_ref().map(|key| match key {
        DbtUniqueKey::Single(s) => StringOrArrayOfStrings::String(s.clone()),
        DbtUniqueKey::Multiple(arr) => StringOrArrayOfStrings::ArrayOfStrings(arr.clone()),
    })
}

fn string_or_array_to_dbt_unique_key(
    unique_key: &Option<StringOrArrayOfStrings>,
) -> Option<DbtUniqueKey> {
    unique_key.as_ref().map(|key| match key {
        StringOrArrayOfStrings::String(s) => DbtUniqueKey::Single(s.clone()),
        StringOrArrayOfStrings::ArrayOfStrings(arr) => DbtUniqueKey::Multiple(arr.clone()),
    })
}

fn string_or_array_option_to_vec(value: &Option<StringOrArrayOfStrings>) -> Vec<String> {
    match value {
        Some(StringOrArrayOfStrings::String(s)) => vec![s.clone()],
        Some(StringOrArrayOfStrings::ArrayOfStrings(v)) => v.clone(),
        None => Vec::new(),
    }
}

fn serialize_option_to_json<T: Serialize>(value: &Option<T>) -> Option<String> {
    value
        .as_ref()
        .and_then(|inner| serde_json::to_string(inner).ok())
}

fn serialize_vec_to_json_if_not_empty<T: Serialize>(items: &[T]) -> Option<String> {
    if items.is_empty() {
        None
    } else {
        serde_json::to_string(items).ok()
    }
}

fn serialize_map_to_json_if_not_empty<V: Serialize>(map: &BTreeMap<String, V>) -> Option<String> {
    if map.is_empty() {
        None
    } else {
        serde_json::to_string(map).ok()
    }
}

fn serialize_indexmap_to_json_if_not_empty<V: Serialize>(
    map: &IndexMap<String, V>,
) -> Option<String> {
    if map.is_empty() {
        None
    } else {
        serde_json::to_string(map).ok()
    }
}

// Helper functions to convert between local Hooks and CommonHooks
fn common_hooks_to_hooks(
    common_hooks: &Option<dbt_schemas::schemas::common::Hooks>,
) -> Option<crate::parquet_node::Hooks> {
    use dbt_schemas::schemas::common::Hooks as CommonHooks;
    common_hooks.as_ref().map(|h| {
        crate::parquet_node::Hooks {
            run_before: match h {
                CommonHooks::String(s) => vec![s.clone()],
                CommonHooks::ArrayOfStrings(arr) => arr.clone(),
                CommonHooks::HookConfig(config) => {
                    config.sql.clone().map(|s| vec![s]).unwrap_or_default()
                }
                CommonHooks::HookConfigArray(configs) => {
                    configs.iter().filter_map(|c| c.sql.clone()).collect()
                }
            },
            run_before_as_transaction: None, // This info is not available in simplified format
            run_after: Vec::new(), // CommonHooks only has one field, so we put everything in run_before
            run_after_as_transaction: None,
        }
    })
}

fn hooks_to_common_hooks(
    hooks: &Option<crate::parquet_node::Hooks>,
) -> Option<dbt_schemas::schemas::common::Hooks> {
    use dbt_schemas::schemas::common::Hooks as CommonHooks;
    hooks.as_ref().map(|h| {
        if h.run_before.len() == 1 {
            CommonHooks::String(h.run_before[0].clone())
        } else {
            CommonHooks::ArrayOfStrings(h.run_before.clone())
        }
    })
}

// Helper function to convert quoting string back to DbtQuoting
fn deserialize_quoting_to_dbt_quoting(quoting_str: &Option<String>) -> Option<DbtQuoting> {
    quoting_str.as_ref().map(|s| {
        let resolved = deserialize_quoting(s);
        DbtQuoting {
            database: Some(resolved.database),
            identifier: Some(resolved.identifier),
            schema: Some(resolved.schema),
            snowflake_ignore_case: None, // This information is not stored in the serialized format
        }
    })
}

// Helper functions to convert specific config types to UserConfigs
fn model_config_to_user_configs(config: &ModelConfig) -> UserConfigs {
    UserConfigs {
        enabled: config.enabled,
        compute: config.compute,
        tags: config.tags.clone(),
        meta: config.meta.clone(),
        group: config.group.clone(),
        event_time: config.event_time.clone(),
        // Note: database, schema, alias, materialized, quoting, static_analysis are in WarehouseDetails
        catalog_name: config.catalog_name.clone(),
        incremental_strategy: config.incremental_strategy.clone(),
        incremental_predicates: config.incremental_predicates.clone(),
        batch_size: config.batch_size.clone(),
        lookback: config.lookback,
        begin: config.begin.clone(),
        persist_docs: config.persist_docs.clone(),
        post_hook: common_hooks_to_hooks(&config.post_hook),
        pre_hook: common_hooks_to_hooks(&config.pre_hook),
        column_types: config.column_types.clone(),
        full_refresh: config.full_refresh,
        unique_key: config.unique_key.clone(),
        on_schema_change: config.on_schema_change.clone(),
        on_configuration_change: config.on_configuration_change.clone(),
        on_error: config.on_error.clone(),
        grants: config.grants.clone(),
        packages: config.packages.clone(),
        python_version: config.python_version.clone(),
        imports: config.imports.clone(),
        docs: config.docs.clone(),
        contract: config.contract.clone(),
        concurrent_batches: config.concurrent_batches,
        merge_update_columns: config.merge_update_columns.clone(),
        merge_exclude_columns: config.merge_exclude_columns.clone(),
        access: config.access.clone(),
        table_format: config.table_format.clone(),
        freshness: config.freshness.clone(),
        sql_header: config.sql_header.clone(),
        location: config.location.clone(),
        predicates: config.predicates.clone(),
        sync: config.sync.clone(),
        submission_method: config.submission_method.clone(),
        job_cluster_config: config.job_cluster_config.clone(),
        python_job_config: config.python_job_config.clone(),
        cluster_id: config.cluster_id.clone(),
        http_path: config.http_path.clone(),
        create_notebook: config.create_notebook,
        index_url: config.index_url.clone(),
        additional_libs: config.additional_libs.clone(),
        user_folder_for_python: config.user_folder_for_python,
        warehouse_specific_config: serialize_warehouse_specific_config(&Some(
            config.__warehouse_specific_config__.clone(),
        )),
        ..Default::default()
    }
}

fn seed_config_to_user_configs(config: &SeedConfig) -> UserConfigs {
    UserConfigs {
        enabled: config.enabled,
        compute: None,
        tags: config.tags.clone(),
        meta: config.meta.clone(),
        group: config.group.clone(),
        event_time: config.event_time.clone(),
        // Note: database, schema, alias, quoting, static_analysis are in WarehouseDetails
        quote_columns: config.quote_columns,
        delimiter: config.delimiter.clone().map(|s| s.into_inner()),
        column_types: config.column_types.clone(),
        full_refresh: config.full_refresh,
        grants: config.grants.clone(),
        persist_docs: config.persist_docs.clone(),
        post_hook: common_hooks_to_hooks(&config.post_hook),
        pre_hook: common_hooks_to_hooks(&config.pre_hook),
        docs: config.docs.clone(),
        warehouse_specific_config: serialize_warehouse_specific_config(&Some(
            config.__warehouse_specific_config__.clone(),
        )),
        ..Default::default()
    }
}

fn snapshot_config_to_user_configs(config: &SnapshotConfig) -> UserConfigs {
    UserConfigs {
        enabled: config.enabled,
        compute: config.compute,
        tags: config.tags.clone(),
        meta: config.meta.clone(),
        group: config.group.clone(),
        event_time: config.event_time.clone(),
        // Note: database, schema, alias, materialized, quoting, static_analysis are in WarehouseDetails
        strategy: config.strategy.clone(),
        unique_key: string_or_array_to_dbt_unique_key(&config.unique_key),
        check_cols: config.check_cols.clone(),
        updated_at: config.updated_at.clone(),
        dbt_valid_to_current: config.dbt_valid_to_current.clone(),
        snapshot_meta_column_names: config.snapshot_meta_column_names.clone(),
        hard_deletes: config.hard_deletes.clone(),
        quote_columns: config.quote_columns,
        invalidate_hard_deletes: config.invalidate_hard_deletes,
        persist_docs: config.persist_docs.clone(),
        grants: config.grants.clone(),
        post_hook: common_hooks_to_hooks(&config.post_hook),
        pre_hook: common_hooks_to_hooks(&config.pre_hook),
        docs: config.docs.clone(),
        sync: config.sync.clone(),
        warehouse_specific_config: serialize_warehouse_specific_config(&Some(
            config.__warehouse_specific_config__.clone(),
        )),
        ..Default::default()
    }
}

fn source_config_to_user_configs(config: &SourceConfig) -> UserConfigs {
    UserConfigs {
        enabled: config.enabled,
        tags: config.tags.clone(),
        meta: config.meta.clone(),
        event_time: config.event_time.clone(),
        // Note: quoting, static_analysis are in WarehouseDetails
        source_freshness: match &config.freshness {
            Omissible::Present(v) => v.clone(),
            Omissible::Omitted => None,
        },
        loaded_at_field: config.loaded_at_field.clone(),
        loaded_at_query: config.loaded_at_query.0.clone(),
        warehouse_specific_config: serialize_warehouse_specific_config(&Some(
            config.__warehouse_specific_config__.clone(),
        )),
        ..Default::default()
    }
}

fn data_test_config_to_user_configs(config: &DataTestConfig) -> UserConfigs {
    UserConfigs {
        enabled: config.enabled,
        compute: config.compute,
        tags: config.tags.clone(),
        meta: config.meta.clone(),
        group: config.group.clone(),
        // Note: database, schema, alias, quoting, static_analysis are in WarehouseDetails
        error_if: config.error_if.clone(),
        fail_calc: config.fail_calc.clone(),
        limit: config.limit,
        severity: config.severity.clone(),
        store_failures: config.store_failures,
        store_failures_as: config.store_failures_as.clone(),
        sql_header: config.sql_header.clone(),
        warn_if: config.warn_if.clone(),
        where_: config.where_.clone(),
        warehouse_specific_config: serialize_warehouse_specific_config(&Some(
            config.__warehouse_specific_config__.clone(),
        )),
        ..Default::default()
    }
}

fn unit_test_config_to_user_configs(config: &UnitTestConfig) -> UserConfigs {
    UserConfigs {
        enabled: config.enabled,
        compute: config.compute,
        tags: config.tags.clone(),
        meta: config.meta.clone(),
        // Note: static_analysis is in WarehouseDetails
        warehouse_specific_config: serialize_warehouse_specific_config(&Some(
            config.__warehouse_specific_config__.clone(),
        )),
        ..Default::default()
    }
}

// Helper functions to convert UserConfigs back to specific config types
fn user_configs_to_model_config(
    user_config: &UserConfigs,
    warehouse_details: &Option<WarehouseDetails>,
) -> ModelConfig {
    use dbt_common::serde_utils::Omissible;
    use dbt_yaml::Verbatim;

    ModelConfig {
        enabled: user_config.enabled,
        compute: user_config.compute,
        alias: warehouse_details.as_ref().and_then(|w| w.alias.clone()),
        schema: warehouse_details
            .as_ref()
            .and_then(|w| w.schema.clone())
            .map(|s| Omissible::Present(Some(s)))
            .unwrap_or_default(),
        database: warehouse_details
            .as_ref()
            .and_then(|w| w.database.clone())
            .map(|s| Omissible::Present(Some(s)))
            .unwrap_or_default(),
        tags: user_config.tags.clone(),
        meta: user_config.meta.clone(),
        group: user_config.group.clone(),
        static_analysis: warehouse_details
            .as_ref()
            .and_then(|w| w.static_analysis.clone()),
        event_time: user_config.event_time.clone(),
        quoting: deserialize_quoting_to_dbt_quoting(
            &warehouse_details.as_ref().and_then(|w| w.quoting.clone()),
        ),
        catalog_name: user_config.catalog_name.clone(),
        materialized: warehouse_details
            .as_ref()
            .and_then(|w| w.materialized.clone()),
        incremental_strategy: user_config.incremental_strategy.clone(),
        incremental_predicates: user_config.incremental_predicates.clone(),
        batch_size: user_config.batch_size.clone(),
        lookback: user_config.lookback,
        begin: user_config.begin.clone(),
        persist_docs: user_config.persist_docs.clone(),
        post_hook: Verbatim::from(hooks_to_common_hooks(&user_config.post_hook)),
        pre_hook: Verbatim::from(hooks_to_common_hooks(&user_config.pre_hook)),
        column_types: user_config.column_types.clone(),
        full_refresh: user_config.full_refresh,
        unique_key: user_config.unique_key.clone(),
        on_schema_change: user_config.on_schema_change.clone(),
        on_configuration_change: user_config.on_configuration_change.clone(),
        on_error: user_config.on_error.clone(),
        grants: user_config.grants.clone(),
        packages: user_config.packages.clone(),
        python_version: user_config.python_version.clone(),
        imports: user_config.imports.clone(),
        secrets: user_config.secrets.clone(),
        external_access_integrations: user_config.external_access_integrations.clone(),
        use_anonymous_sproc: user_config.use_anonymous_sproc,
        docs: user_config.docs.clone(),
        contract: user_config.contract.clone(),
        concurrent_batches: user_config.concurrent_batches,
        merge_update_columns: user_config.merge_update_columns.clone(),
        merge_exclude_columns: user_config.merge_exclude_columns.clone(),
        access: user_config.access.clone(),
        table_format: user_config.table_format.clone(),
        freshness: user_config.freshness.clone(),
        sql_header: user_config.sql_header.clone(),
        location: user_config.location.clone(),
        predicates: user_config.predicates.clone(),
        sync: user_config.sync.clone(),
        submission_method: user_config.submission_method.clone(),
        job_cluster_config: user_config.job_cluster_config.clone(),
        python_job_config: user_config.python_job_config.clone(),
        cluster_id: user_config.cluster_id.clone(),
        http_path: user_config.http_path.clone(),
        create_notebook: user_config.create_notebook,
        index_url: user_config.index_url.clone(),
        additional_libs: user_config.additional_libs.clone(),
        user_folder_for_python: user_config.user_folder_for_python,
        config_keys_used: None,
        config_keys_defaults: None,
        meta_keys_used: None,
        meta_keys_defaults: None,
        __warehouse_specific_config__: deserialize_warehouse_specific_config(
            &user_config.warehouse_specific_config,
        )
        .unwrap_or_default(),
    }
}

fn user_configs_to_seed_config(
    user_config: &UserConfigs,
    warehouse_details: &Option<WarehouseDetails>,
) -> SeedConfig {
    use dbt_yaml::{Spanned, Verbatim};

    SeedConfig {
        enabled: user_config.enabled,
        alias: warehouse_details.as_ref().and_then(|w| w.alias.clone()),
        schema: warehouse_details.as_ref().and_then(|w| w.schema.clone()),
        database: warehouse_details.as_ref().and_then(|w| w.database.clone()),
        catalog_name: user_config.catalog_name.clone(),
        tags: user_config.tags.clone(),
        meta: user_config.meta.clone(),
        group: user_config.group.clone(),
        event_time: user_config.event_time.clone(),
        quoting: deserialize_quoting_to_dbt_quoting(
            &warehouse_details.as_ref().and_then(|w| w.quoting.clone()),
        ),
        quote_columns: user_config.quote_columns,
        delimiter: user_config.delimiter.clone().map(Spanned::new),
        column_types: user_config.column_types.clone(),
        full_refresh: user_config.full_refresh,
        grants: user_config.grants.clone(),
        persist_docs: user_config.persist_docs.clone(),
        post_hook: Verbatim::from(hooks_to_common_hooks(&user_config.post_hook)),
        pre_hook: Verbatim::from(hooks_to_common_hooks(&user_config.pre_hook)),
        static_analysis: warehouse_details
            .as_ref()
            .and_then(|w| w.static_analysis.clone()),
        docs: user_config.docs.clone(),
        materialized: Some(DbtMaterialization::Seed),
        __warehouse_specific_config__: deserialize_warehouse_specific_config(
            &user_config.warehouse_specific_config,
        )
        .unwrap_or_default(),
    }
}

fn user_configs_to_snapshot_config(
    user_config: &UserConfigs,
    warehouse_details: &Option<WarehouseDetails>,
) -> SnapshotConfig {
    use dbt_yaml::Verbatim;

    SnapshotConfig {
        enabled: user_config.enabled,
        compute: user_config.compute,
        alias: warehouse_details.as_ref().and_then(|w| w.alias.clone()),
        schema: warehouse_details.as_ref().and_then(|w| w.schema.clone()),
        target_schema: user_config.target_database.clone(),
        database: warehouse_details.as_ref().and_then(|w| w.database.clone()),
        target_database: user_config.target_schema.clone(),
        tags: user_config.tags.clone(),
        meta: user_config.meta.clone(),
        group: user_config.group.clone(),
        static_analysis: warehouse_details
            .as_ref()
            .and_then(|w| w.static_analysis.clone()),
        event_time: user_config.event_time.clone(),
        quoting: deserialize_quoting_to_dbt_quoting(
            &warehouse_details.as_ref().and_then(|w| w.quoting.clone()),
        ),
        full_refresh: user_config.full_refresh,
        materialized: warehouse_details
            .as_ref()
            .and_then(|w| w.materialized.clone()),
        strategy: user_config.strategy.clone(),
        unique_key: dbt_unique_key_to_string_or_array(&user_config.unique_key),
        check_cols: user_config.check_cols.clone(),
        updated_at: user_config.updated_at.clone(),
        dbt_valid_to_current: user_config.dbt_valid_to_current.clone(),
        snapshot_meta_column_names: user_config.snapshot_meta_column_names.clone(),
        hard_deletes: user_config.hard_deletes.clone(),
        quote_columns: user_config.quote_columns,
        invalidate_hard_deletes: user_config.invalidate_hard_deletes,
        persist_docs: user_config.persist_docs.clone(),
        grants: user_config.grants.clone(),
        post_hook: Verbatim::from(hooks_to_common_hooks(&user_config.post_hook)),
        pre_hook: Verbatim::from(hooks_to_common_hooks(&user_config.pre_hook)),
        docs: user_config.docs.clone(),
        sync: user_config.sync.clone(),
        __warehouse_specific_config__: deserialize_warehouse_specific_config(
            &user_config.warehouse_specific_config,
        )
        .unwrap_or_default(),
    }
}

fn user_configs_to_source_config(
    user_config: &UserConfigs,
    warehouse_details: &Option<WarehouseDetails>,
) -> SourceConfig {
    SourceConfig {
        enabled: user_config.enabled,
        tags: user_config.tags.clone(),
        meta: user_config.meta.clone(),
        static_analysis: warehouse_details
            .as_ref()
            .and_then(|w| w.static_analysis.clone()),
        event_time: user_config.event_time.clone(),
        quoting: deserialize_quoting_to_dbt_quoting(
            &warehouse_details.as_ref().and_then(|w| w.quoting.clone()),
        ),
        freshness: Omissible::Present(user_config.source_freshness.clone()),
        loaded_at_field: user_config.loaded_at_field.clone(),
        loaded_at_query: user_config.loaded_at_query.clone().into(),
        schema_origin: user_config.schema_origin,
        sync: user_config.sync.clone(),
        __warehouse_specific_config__: deserialize_warehouse_specific_config(
            &user_config.warehouse_specific_config,
        )
        .unwrap_or_default(),
    }
}

fn user_configs_to_data_test_config(
    user_config: &UserConfigs,
    warehouse_details: &Option<WarehouseDetails>,
) -> DataTestConfig {
    DataTestConfig {
        enabled: user_config.enabled,
        compute: user_config.compute,
        alias: warehouse_details.as_ref().and_then(|w| w.alias.clone()),
        schema: warehouse_details.as_ref().and_then(|w| w.schema.clone()),
        database: warehouse_details.as_ref().and_then(|w| w.database.clone()),
        tags: user_config.tags.clone(),
        meta: user_config.meta.clone(),
        group: user_config.group.clone(),
        static_analysis: warehouse_details
            .as_ref()
            .and_then(|w| w.static_analysis.clone()),
        quoting: deserialize_quoting_to_dbt_quoting(
            &warehouse_details.as_ref().and_then(|w| w.quoting.clone()),
        ),
        full_refresh: user_config.full_refresh,
        error_if: user_config.error_if.clone(),
        fail_calc: user_config.fail_calc.clone(),
        limit: user_config.limit,
        severity: user_config.severity.clone(),
        store_failures: user_config.store_failures,
        store_failures_as: user_config.store_failures_as.clone(),
        sql_header: user_config.sql_header.clone(),
        warn_if: user_config.warn_if.clone(),
        where_: user_config.where_.clone(),
        materialized: Some(DataTestConfig::default_materialized()),
        __warehouse_specific_config__: deserialize_warehouse_specific_config(
            &user_config.warehouse_specific_config,
        )
        .unwrap_or_default(),
    }
}

fn user_configs_to_unit_test_config(
    user_config: &UserConfigs,
    warehouse_details: &Option<WarehouseDetails>,
) -> UnitTestConfig {
    UnitTestConfig {
        enabled: user_config.enabled,
        compute: user_config.compute,
        tags: user_config.tags.clone(),
        meta: user_config.meta.clone(),
        static_analysis: warehouse_details
            .as_ref()
            .and_then(|w| w.static_analysis.clone()),
        __warehouse_specific_config__: deserialize_warehouse_specific_config(
            &user_config.warehouse_specific_config,
        )
        .unwrap_or_default(),
    }
}

// Collects all input files for a dbt node, handling the various path types:
// - path: The primary source file
// - original_file_path: The original location if moved/copied
// - patch_path: Additional configuration patches (usually from schema.yml)
fn collect_input_files(
    io: &IoArgs,
    cas: &mut HashMap<String, String>,
    common: &CommonAttributes,
    registry: &CompleteStateWithKind,
) -> FsResult<Vec<InputFile>> {
    let mut input_files = Vec::new();

    // Add primary path
    add_input_file_if_exists(io, cas, &common.path, &mut input_files, registry)?;

    // Add original file path if different
    if common.original_file_path != common.path {
        add_input_file_if_exists(
            io,
            cas,
            &common.original_file_path,
            &mut input_files,
            registry,
        )?;
    }

    // Add patch path if different from both primary and original
    if let Some(patch_path) = &common.patch_path
        && patch_path != &common.path
        && patch_path != &common.original_file_path
    {
        add_input_file_if_exists(io, cas, patch_path, &mut input_files, registry)?;
    }

    Ok(input_files)
}

//-------------------------------------------------------------------------------------------------
// serialize

pub fn serialize_resolver_state_to_parquet_nodes(
    io: &IoArgs,
    ctx: &WriteContext,
    cas: &mut HashMap<String, String>,
    eval_args: &EvalArgs,
    resolver_state: &ResolverState,
    registry: &CompleteStateWithKind,
    cloud_config: &Option<ResolvedCloudConfig>,
) -> FsResult<Vec<ParquetNode>> {
    let tiny_session = create_tiny_session(eval_args, resolver_state);
    // comp[u]
    // session node

    let session_node = serialize_to_session_pnode(
        io,
        &ctx.with_enabled(true),
        cas,
        &tiny_session,
        registry,
        cloud_config,
    )?
    .with_ctx(ctx.clone());

    // enabled nodes
    let enabled_nodes = serialize_to_pnodes(
        eval_args.command,
        io,
        &ctx.with_enabled(true),
        cas,
        &resolver_state.nodes,
        registry,
        resolver_state.adapter_type.as_ref(),
    )?;

    // disabled nodes
    let disabled_nodes = serialize_to_pnodes(
        eval_args.command,
        io,
        &ctx.with_enabled(false),
        cas,
        &resolver_state.disabled_nodes,
        registry,
        resolver_state.adapter_type.as_ref(),
    )?;

    // macros & operations
    let mut other_nodes = Vec::new();
    // macros, last state is always just parse
    {
        let ctx = WriteContext {
            phase: NodeExecutionState::Parsed,
            ..(*ctx).clone()
        };
        for node in resolver_state.macros.macros.values() {
            other_nodes.push(
                serialize_to_macro_pnode(io, &ctx, cas, node, registry)?.with_ctx(ctx.clone()),
            );
        }
        for node in resolver_state.macros.docs_macros.values() {
            other_nodes.push(
                serialize_to_docs_macro_pnode(io, &ctx, cas, node, registry)?.with_ctx(ctx.clone()),
            );
        }
    }
    // on_run_start
    for (i, node) in resolver_state.operations.on_run_start.iter().enumerate() {
        other_nodes.push(
            serialize_to_operation_pnode(io, ctx, cas, "on-run-start", i, node, registry)?
                .with_ctx(ctx.clone()),
        );
    }
    // on_run_end
    for (i, node) in resolver_state.operations.on_run_end.iter().enumerate() {
        other_nodes.push(
            serialize_to_operation_pnode(
                io,
                &ctx.with_enabled(true),
                cas,
                "on-run-end",
                i,
                node,
                registry,
            )?
            .with_ctx(ctx.clone()),
        );
    }

    // combine results
    let mut result = vec![session_node];
    result.extend(enabled_nodes);
    result.extend(disabled_nodes);
    result.extend(other_nodes);

    Ok(result)
}

fn serialize_to_pnodes(
    cmd: FsCommand,
    io: &IoArgs,
    ctx: &WriteContext,
    cas: &mut HashMap<String, String>,
    nodes: &Nodes,
    registry: &CompleteStateWithKind,
    adapter_type: &str,
) -> FsResult<Vec<ParquetNode>> {
    let mut result = Vec::new();

    for node in nodes.models.values() {
        result.push(serialize_to_model_pnode(io, ctx, cas, node, registry)?._with_ctx(ctx, cmd));
    }
    for node in nodes.tests.values() {
        result.push(serialize_to_test_pnode(io, ctx, cas, node, registry)?._with_ctx(ctx, cmd));
    }
    for node in nodes.snapshots.values() {
        result.push(
            serialize_to_snapshot_pnode(io, ctx, cas, node, registry, adapter_type)?
                ._with_ctx(ctx, cmd),
        );
    }
    for node in nodes.seeds.values() {
        result.push(serialize_to_seed_pnode(io, ctx, cas, node, registry)?._with_ctx(ctx, cmd));
    }
    for node in nodes.sources.values() {
        result.push(serialize_to_source_pnode(io, ctx, cas, node, registry)?._with_ctx(ctx, cmd));
    }
    for node in nodes.unit_tests.values() {
        result
            .push(serialize_to_unit_test_pnode(io, ctx, cas, node, registry)?._with_ctx(ctx, cmd));
    }
    for node in nodes.exposures.values() {
        result.push(serialize_to_exposure_pnode(io, ctx, cas, node, registry)?._with_ctx(ctx, cmd));
    }
    for node in nodes.semantic_models.values() {
        result.push(
            serialize_to_semantic_model_pnode(io, ctx, cas, node, registry)?._with_ctx(ctx, cmd),
        );
    }
    for node in nodes.saved_queries.values() {
        result.push(
            serialize_to_saved_query_pnode(io, ctx, cas, node, registry)?._with_ctx(ctx, cmd),
        );
    }
    for node in nodes.groups.values() {
        result.push(serialize_to_group_pnode(io, ctx, cas, node, registry)?._with_ctx(ctx, cmd));
    }

    Ok(result)
}
// -------------------------------------------------------------------------------------------------
// serialize on node at a time

pub struct TinySession {
    pub project_name: String,
    pub profile_name: String,
    pub target: String,
    pub packages_install_path: Option<String>, // relative path to packages installed by dbt, relative to the project root
    pub adapter_type: String,
    pub quoting: Option<ResolvedQuoting>,
    pub dbt_version: String,
    pub generated_at: Option<String>, // ISO8601
    pub invocation_id: String,
    pub invocation_started_at: Option<String>, // ISO8601
    pub env_vars: Option<HashMap<String, String>>,
    pub command: String,       // the dbt command that was run
    pub cli_args: Vec<String>, // the CLI arguments that were passed to dbt
    pub target_path: String,   // relative path to the target directory
    pub profile_hash: String,
    pub env_vars_hash: Option<String>, // Hash of relevant env vars
}

pub fn create_tiny_session(eval_args: &EvalArgs, resolver_state: &ResolverState) -> TinySession {
    let project_name = resolver_state.root_project_name.clone();
    let profile_name = resolver_state.dbt_profile.profile.clone();
    let target = resolver_state.dbt_profile.target.clone();
    TinySession {
        project_name,
        profile_name,
        target,
        packages_install_path: eval_args
            .packages_install_path
            .as_ref()
            .map(|p| p.to_string_lossy().to_string()),
        adapter_type: resolver_state.adapter_type.to_string(),
        quoting: Some(resolver_state.root_project_quoting),

        dbt_version: env!("CARGO_PKG_VERSION").to_string(),
        generated_at: None,
        invocation_id: eval_args.io.invocation_id.to_string(),
        invocation_started_at: Some(resolver_state.run_started_at.to_rfc3339()),
        // todo: have to identify the env_vars that are relevant for the run
        env_vars: None,
        command: eval_args.command.as_str().to_string(),
        cli_args: vec![], //eval_args.cli_args.clone(),
        target_path: eval_args
            .target_path
            .as_ref()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default(), // relative path to the target directory
        profile_hash: resolver_state.dbt_profile.blake3_hash(),
        env_vars_hash: Some(String::new()),
    }
}

pub fn create_unique_session_id(resolver_state: &ResolverState) -> String {
    let project_name = resolver_state.root_project_name.clone();
    let target = resolver_state.dbt_profile.target.clone();
    format!("session.{project_name}.{target}")
}

// todo: ManifestMetadata is te wrong type here
pub fn serialize_to_session_pnode(
    io: &IoArgs,
    _ctx: &WriteContext,
    cas: &mut HashMap<String, String>,
    tiny_session: &TinySession,
    registry: &CompleteStateWithKind,
    cloud_config: &Option<ResolvedCloudConfig>,
) -> FsResult<ParquetNode> {
    // Try to get from cloud_config, else from manifest_metadata

    // Issuer
    let account_id = cloud_config
        .as_ref()
        .and_then(|c| c.account_identifier.clone());
    let project_id = cloud_config.as_ref().and_then(|c| c.project_id.clone());
    let environment_id = cloud_config.as_ref().and_then(|c| c.environment_id.clone());

    // todo: add vars, they should go into common_details..meta.meta_blob, since they can be arbitrarily structured values

    let session_details = SessionDetails {
        // todo fill in all nones..
        // Accounting metadata
        org_id: None,
        account_id,
        project_id,
        environment_id,
        user_id: None,

        org_name: None,
        account_name: None,
        project_name: Some(tiny_session.project_name.clone()),
        environment_name: None,
        user_name: None,

        // Invocation metadata
        invocation_id: Some(tiny_session.invocation_id.clone()),
        invocation_started_at: tiny_session.invocation_started_at.clone(),
        generated_at: tiny_session.generated_at.clone(),

        // Arguments for the run
        command: Some(tiny_session.command.clone()),
        cli_args: Some(tiny_session.cli_args.clone()),
        packages_install_path: tiny_session.packages_install_path.clone(),
        target_dir: Some(tiny_session.target_path.clone()),

        // Classic dbt config context
        profile_name: Some(tiny_session.profile_name.clone()), // todo: fill in from manifest_metadata
        target: Some(tiny_session.target.clone()), // todo: fill in from manifest_metadata
        profile_hash: Some(tiny_session.profile_hash.clone()), // Hash of rendered/selected profile block
        adapter_type: Some(tiny_session.adapter_type.clone()),
        quoting: tiny_session.quoting.as_ref().map(serialize_quoting),

        // vars -- User-provided vars -- stored in meta

        // environment
        env_vars: tiny_session.env_vars.clone(),
        env_vars_hash: tiny_session.env_vars_hash.clone(), // Hash of relevant env vars

        // profile stuff

        // Versioning
        dbt_version: Some(tiny_session.dbt_version.clone()),
        parquet_version: Some(PARQUET_VERSION.to_string()), // vars are not stored directly here
    };

    // // Versioning
    // pub dbt_schema_version: Option<String
    let mut pnode = ParquetNode::new(
        ResourceType::Session,
        // todo determine unique id for session
        format!(
            "session.{}.{}",
            tiny_session.project_name.clone(),
            tiny_session.target.clone()
        ),
        format!("{}.{}", tiny_session.project_name, tiny_session.target),
        None,
    );

    let input_files = add_session_input_files(io, cas, registry)?;
    pnode.input_files = input_files;
    pnode.session_details = Some(session_details);
    Ok(pnode)
}

pub fn serialize_to_source_pnode(
    io: &IoArgs,
    __ctx: &WriteContext,
    cas: &mut HashMap<String, String>,
    node: &DbtSource,
    registry: &CompleteStateWithKind,
) -> FsResult<ParquetNode> {
    let common: &CommonAttributes = &node.__common_attr__;
    let resource_type = ResourceType::Source;
    let mut pnode = serialize_common_attributes(io, cas, common, resource_type, registry)?;
    pnode.warehouse_details = Some(serialize_base_attr(&node.__base_attr__));
    if !node.__base_attr__.depends_on.nodes.is_empty() {
        pnode.depends_on_nodes = node.__base_attr__.depends_on.nodes.clone();
    }
    if !node.__base_attr__.depends_on.macros.is_empty() {
        pnode.depends_on_macros = node.__base_attr__.depends_on.macros.clone();
    }
    pnode.source_details = Some(dbt_source_attr_to_details(&node.__source_attr__));

    // Serialize the deprecated_config to user_config_details
    pnode.user_config_details = Some(source_config_to_user_configs(&node.deprecated_config));

    Ok(pnode)
}

pub fn serialize_to_macro_pnode(
    io: &IoArgs,
    __ctx: &WriteContext,
    cas: &mut HashMap<String, String>,
    node: &DbtMacro,
    registry: &CompleteStateWithKind,
) -> FsResult<ParquetNode> {
    // Build ParquetNode
    let mut pnode = ParquetNode::new(
        ResourceType::Macro,
        node.unique_id.to_string(),
        node.name.clone(),
        None,
    );
    pnode.name = node.name.clone();
    pnode.package_name = Some(node.package_name.clone());
    pnode.description = Some(node.description.clone());

    // Create a temporary CommonAttributes for reuse
    let common = CommonAttributes {
        path: node.path.clone(),
        original_file_path: node.original_file_path.clone(),
        patch_path: node.patch_path.clone(),
        ..Default::default()
    };
    pnode.input_files = collect_input_files(io, cas, &common, registry)?;

    //add parse cas entry for macro // all macros only get a parse entry

    let hash = code_hash(&node.macro_sql);
    cas.insert(hash.clone(), node.macro_sql.clone());
    pnode.macro_details = Some(crate::parquet_node::MacroDetails {
        macro_sql_cas_hash: Some(hash),
        func_signature: node.funcsign.clone(),
        arguments: node
            .args
            .iter()
            .map(|arg| MacroArgumentDetails {
                name: arg.name.clone(),
                is_optional: arg.is_optional,
            })
            .collect(),
    });
    pnode.depends_on_macros = node.depends_on.macros.clone();

    Ok(pnode)
}

pub fn serialize_to_docs_macro_pnode(
    io: &IoArgs,
    __ctx: &WriteContext,
    cas: &mut HashMap<String, String>,
    node: &DbtDocsMacro,
    registry: &CompleteStateWithKind,
) -> FsResult<ParquetNode> {
    let mut pnode = ParquetNode::new(
        ResourceType::DocsMacro,
        node.unique_id.to_string(),
        node.name.clone(),
        None,
    );
    pnode.name = node.name.clone();
    pnode.package_name = Some(node.package_name.clone());

    // Create a temporary CommonAttributes for reuse
    let common = CommonAttributes {
        path: node.path.clone(),
        original_file_path: node.original_file_path.clone(),
        ..Default::default()
    };
    pnode.input_files = collect_input_files(io, cas, &common, registry)?;

    //add parse cas entry for macro // all macros only get a parse entry

    let hash = code_hash(&node.block_contents);
    cas.insert(hash.clone(), node.block_contents.clone());
    pnode.macro_details = Some(crate::parquet_node::MacroDetails {
        macro_sql_cas_hash: Some(hash),
        func_signature: None,
        arguments: Vec::new(),
    });

    Ok(pnode)
}

pub fn serialize_to_operation_pnode(
    io: &IoArgs,
    _ctx: &WriteContext,
    cas: &mut HashMap<String, String>,
    _label: &str,
    _index: usize,
    node: &DbtOperation,
    registry: &CompleteStateWithKind,
) -> FsResult<ParquetNode> {
    let common: &CommonAttributes = &node.__common_attr__;
    let resource_type = ResourceType::Operation;
    serialize_common_attributes(io, cas, common, resource_type, registry)
}

pub fn serialize_to_seed_pnode(
    io: &IoArgs,
    _ctx: &WriteContext,
    cas: &mut HashMap<String, String>,
    node: &DbtSeed,
    registry: &CompleteStateWithKind,
) -> FsResult<ParquetNode> {
    let resource_type = ResourceType::Seed;
    let common: &CommonAttributes = &node.__common_attr__;
    let mut pnode = serialize_common_attributes(io, cas, common, resource_type, registry)?;
    pnode.warehouse_details = Some(serialize_base_attr(&node.__base_attr__));
    if !node.__base_attr__.depends_on.nodes.is_empty() {
        pnode.depends_on_nodes = node.__base_attr__.depends_on.nodes.clone();
    }
    if !node.__base_attr__.depends_on.macros.is_empty() {
        pnode.depends_on_macros = node.__base_attr__.depends_on.macros.clone();
    }

    pnode.seed_details = Some(from_dbt_seed_attr_to_details(&node.__seed_attr__));
    // todo: where are the seeds? They should be come part of the inputs...

    // Serialize the deprecated_config to user_config_details
    pnode.user_config_details = Some(seed_config_to_user_configs(&node.deprecated_config));

    Ok(pnode)
}

pub fn serialize_to_snapshot_pnode(
    io: &IoArgs,
    _ctx: &WriteContext,
    cas: &mut HashMap<String, String>,

    node: &DbtSnapshot,
    registry: &CompleteStateWithKind,
    adapter_type: &str,
) -> FsResult<ParquetNode> {
    let resource_type = ResourceType::Snapshot;
    let common: &CommonAttributes = &node.__common_attr__;
    let mut pnode = serialize_common_attributes(io, cas, common, resource_type, registry)?;
    pnode.warehouse_details = Some(serialize_base_attr(&node.__base_attr__));
    if !node.__base_attr__.depends_on.nodes.is_empty() {
        pnode.depends_on_nodes = node.__base_attr__.depends_on.nodes.clone();
    }
    if !node.__base_attr__.depends_on.macros.is_empty() {
        pnode.depends_on_macros = node.__base_attr__.depends_on.macros.clone();
    }

    let compiled_sql_cas_hash = node.compiled_code.as_ref().map(|compiled_sql| {
        let hash = code_hash(compiled_sql);
        cas.insert(hash.clone(), compiled_sql.clone());
        hash
    });

    let mut snapshot_details =
        from_dbt_snapshot_attr_to_details(&node.__snapshot_attr__, adapter_type);
    snapshot_details.compiled_sql_cas_hash = compiled_sql_cas_hash;
    pnode.snapshot_details = Some(snapshot_details);

    // Serialize the deprecated_config to user_config_details
    pnode.user_config_details = Some(snapshot_config_to_user_configs(&node.deprecated_config));

    Ok(pnode)
}
pub fn serialize_to_test_pnode(
    io: &IoArgs,
    _ctx: &WriteContext,
    cas: &mut HashMap<String, String>,

    node: &DbtTest,
    registry: &CompleteStateWithKind,
) -> FsResult<ParquetNode> {
    let resource_type = ResourceType::Test;
    let common: &CommonAttributes = &node.__common_attr__;
    let mut pnode = serialize_common_attributes(io, cas, common, resource_type, registry)?;
    pnode.warehouse_details = Some(serialize_base_attr(&node.__base_attr__));
    if !node.__base_attr__.depends_on.nodes.is_empty() {
        pnode.depends_on_nodes = node.__base_attr__.depends_on.nodes.clone();
    }
    if !node.__base_attr__.depends_on.macros.is_empty() {
        pnode.depends_on_macros = node.__base_attr__.depends_on.macros.clone();
    }
    let attr = &node.__test_attr__;
    pnode.test_details = Some(TestDetails {
        column_name: attr.column_name.clone(),
        attached_node: attr.attached_node.clone(),
        test_metadata_name: attr.test_metadata.as_ref().map(|tm| tm.name.clone()),
        test_metadata_namespace: attr
            .test_metadata
            .as_ref()
            .and_then(|tm| tm.namespace.clone()),
        file_key_name: attr.file_key_name.clone(),
        introspection: attr.introspection,
        defined_at: node.defined_at.as_ref().map(DefinedAtSpan::from),
    });

    if let Some(test_metadata) = &attr.test_metadata {
        let test_meta_args =
            serde_json::to_string(test_metadata.kwargs.iter().collect::<Vec<_>>().as_slice())
                .unwrap_or_else(|_| "{}".to_string());
        pnode.meta = extend_meta_blob(
            pnode.meta.take(),
            vec![("__test_metadata_args", test_meta_args)],
        );
    }

    // Serialize the deprecated_config to user_config_details
    pnode.user_config_details = Some(data_test_config_to_user_configs(&node.deprecated_config));

    Ok(pnode)
}

pub fn serialize_to_unit_test_pnode(
    io: &IoArgs,
    _ctx: &WriteContext,
    cas: &mut HashMap<String, String>,
    node: &DbtUnitTest,
    registry: &CompleteStateWithKind,
) -> FsResult<ParquetNode> {
    let explode = |value: &Option<StringOrArrayOfStrings>| -> Vec<String> {
        match value {
            Some(StringOrArrayOfStrings::String(s)) => vec![s.clone()],
            Some(StringOrArrayOfStrings::ArrayOfStrings(arr)) => arr.clone(),
            None => vec![],
        }
    };

    let resource_type = ResourceType::UnitTest;
    let common: &CommonAttributes = &node.__common_attr__;
    let mut pnode = serialize_common_attributes(io, cas, common, resource_type, registry)?;
    pnode.warehouse_details = Some(serialize_base_attr(&node.__base_attr__));
    if !node.__base_attr__.depends_on.nodes.is_empty() {
        pnode.depends_on_nodes = node.__base_attr__.depends_on.nodes.clone();
    }
    if !node.__base_attr__.depends_on.macros.is_empty() {
        pnode.depends_on_macros = node.__base_attr__.depends_on.macros.clone();
    }
    let field_event_status = node
        .field_event_status
        .as_ref()
        .and_then(|status| serde_json::to_string(status).ok());
    let field_pre_injected_sql = node.field_pre_injected_sql.clone();
    let overrides_json = node
        .__unit_test_attr__
        .overrides
        .as_ref()
        .and_then(|overrides| serde_json::to_string(overrides).ok());
    let given_json = serde_json::to_string(&node.__unit_test_attr__.given).ok();
    let expect_json = serde_json::to_string(&node.__unit_test_attr__.expect).ok();

    let unit_test_details = UnitTestDetails {
        model: node.__unit_test_attr__.model.clone(),
        given_count: node.__unit_test_attr__.given.len(),
        expect_format: Some(node.__unit_test_attr__.expect.format.to_string()),
        expect_fixture: node.__unit_test_attr__.expect.fixture.clone(),
        version: node
            .__unit_test_attr__
            .version
            .as_ref()
            .map(|v| v.to_string()),
        versions_include: node
            .__unit_test_attr__
            .versions
            .as_ref()
            .map(|v| explode(&v.include))
            .unwrap_or_default(),
        versions_exclude: node
            .__unit_test_attr__
            .versions
            .as_ref()
            .map(|v| explode(&v.exclude))
            .unwrap_or_default(),
        field_event_status,
        field_pre_injected_sql,
        overrides: overrides_json,
        tested_node_unique_id: node.tested_node_unique_id.clone(),
        this_input_node_unique_id: node.this_input_node_unique_id.clone(),
        given: given_json,
        expect: expect_json,
    };
    pnode.unit_test_details = Some(unit_test_details);

    // Serialize the deprecated_config to user_config_details
    pnode.user_config_details = Some(unit_test_config_to_user_configs(&node.deprecated_config));

    Ok(pnode)
}

pub fn serialize_to_exposure_pnode(
    io: &IoArgs,
    _ctx: &WriteContext,
    cas: &mut HashMap<String, String>,
    node: &DbtExposure,
    registry: &CompleteStateWithKind,
) -> FsResult<ParquetNode> {
    let resource_type = ResourceType::Exposure;
    let common: &CommonAttributes = &node.__common_attr__;
    let mut pnode = serialize_common_attributes(io, cas, common, resource_type, registry)?;
    pnode.warehouse_details = Some(serialize_base_attr(&node.__base_attr__));
    if !node.__base_attr__.depends_on.nodes.is_empty() {
        pnode.depends_on_nodes = node.__base_attr__.depends_on.nodes.clone();
    }
    if !node.__base_attr__.depends_on.macros.is_empty() {
        pnode.depends_on_macros = node.__base_attr__.depends_on.macros.clone();
    }

    let attr = &node.__exposure_attr__;
    let root_path = node
        .__common_attr__
        .path
        .parent()
        .map(|p| p.display().to_string());
    let depends_on = if node.__base_attr__.depends_on.macros.is_empty()
        && node.__base_attr__.depends_on.nodes.is_empty()
        && node
            .__base_attr__
            .depends_on
            .nodes_with_ref_location
            .is_empty()
    {
        None
    } else {
        serde_json::to_string(&node.__base_attr__.depends_on).ok()
    };
    let refs = serialize_vec_to_json_if_not_empty(&node.__base_attr__.refs);
    let sources = serialize_vec_to_json_if_not_empty(&node.__base_attr__.sources);
    let meta = serialize_indexmap_to_json_if_not_empty(&common.meta);
    let tags = if common.tags.is_empty() {
        None
    } else {
        Some(
            common
                .tags
                .iter()
                .map(|tag| Some(tag.clone()))
                .collect::<Vec<Option<String>>>(),
        )
    };
    let exposure_details = ExposureDetails {
        owner_name: attr.owner.name.clone(),
        owner_emails: string_or_array_option_to_vec(&attr.owner.email),
        label: attr.label.clone(),
        maturity: attr.maturity.clone(),
        exposure_type: serde_json::to_string(&attr.type_)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string(),
        url: attr.url.clone(),
        unrendered_config: serialize_map_to_json_if_not_empty(&attr.unrendered_config),
        created_at: attr.created_at,
        root_path,
        original_file_path: common.original_file_path.display().to_string(),
        name: common.name.clone(),
        description: common.description.clone(),
        depends_on,
        refs,
        sources,
        meta,
        tags,
    };
    pnode.exposure_details = Some(exposure_details);

    Ok(pnode)
}

pub fn serialize_to_group_pnode(
    io: &IoArgs,
    _ctx: &WriteContext,
    cas: &mut HashMap<String, String>,
    node: &DbtGroup,
    registry: &CompleteStateWithKind,
) -> FsResult<ParquetNode> {
    let resource_type = ResourceType::Group;
    let common: &CommonAttributes = &node.__common_attr__;
    let mut pnode = serialize_common_attributes(io, cas, common, resource_type, registry)?;
    pnode.warehouse_details = Some(serialize_base_attr(&node.__base_attr__));
    if !node.__base_attr__.depends_on.nodes.is_empty() {
        pnode.depends_on_nodes = node.__base_attr__.depends_on.nodes.clone();
    }
    if !node.__base_attr__.depends_on.macros.is_empty() {
        pnode.depends_on_macros = node.__base_attr__.depends_on.macros.clone();
    }

    let group_details = GroupDetails {
        owner_name: node.__group_attr__.owner.name.clone(),
        owner_emails: string_or_array_option_to_vec(&node.__group_attr__.owner.email),
    };
    pnode.group_details = Some(group_details);

    Ok(pnode)
}

pub fn serialize_to_semantic_model_pnode(
    io: &IoArgs,
    _ctx: &WriteContext,
    cas: &mut HashMap<String, String>,
    node: &DbtSemanticModel,
    registry: &CompleteStateWithKind,
) -> FsResult<ParquetNode> {
    let resource_type = ResourceType::SemanticModel;
    let common: &CommonAttributes = &node.__common_attr__;
    let base = &node.__base_attr__;
    let mut pnode = serialize_common_attributes(io, cas, common, resource_type, registry)?;
    let attr = &node.__semantic_model_attr__;

    if !base.depends_on.nodes.is_empty() {
        pnode.depends_on_nodes = base.depends_on.nodes.clone();
    }
    if !base.depends_on.macros.is_empty() {
        pnode.depends_on_macros = base.depends_on.macros.clone();
    }

    if attr.group.is_some() {
        pnode
            .common_details
            .get_or_insert_with(Default::default)
            .group = attr.group.clone();
    }

    let semantic_details = SemanticModelDetails {
        resource_type,
        package_name: common.package_name.clone(),
        model: attr.model.clone(),
        label: attr.label.clone(),
        defaults: serialize_option_to_json(&attr.defaults),
        entities: serialize_vec_to_json_if_not_empty(&attr.entities),
        dimensions: serialize_vec_to_json_if_not_empty(&attr.dimensions),
        measures: serialize_vec_to_json_if_not_empty(&attr.measures),
        metadata: serialize_option_to_json(&attr.metadata),
        primary_entity: attr.primary_entity.clone(),
        node_relation: serialize_option_to_json(&attr.node_relation),
        time_spine: None,
        refs: serialize_vec_to_json_if_not_empty(&base.refs),
        group: attr.group.clone(),
        created_at: Some(attr.created_at),
        unrendered_config: serialize_map_to_json_if_not_empty(&attr.unrendered_config),
        description: common.description.clone(),
        depends_on: if base.depends_on.macros.is_empty()
            && base.depends_on.nodes.is_empty()
            && base.depends_on.nodes_with_ref_location.is_empty()
        {
            None
        } else {
            serde_json::to_string(&base.depends_on).ok()
        },
    };
    pnode.semantic_model_details = Some(semantic_details);

    Ok(pnode)
}

pub fn serialize_to_saved_query_pnode(
    io: &IoArgs,
    _ctx: &WriteContext,
    cas: &mut HashMap<String, String>,
    node: &DbtSavedQuery,
    registry: &CompleteStateWithKind,
) -> FsResult<ParquetNode> {
    let resource_type = ResourceType::SavedQuery;
    let common: &CommonAttributes = &node.__common_attr__;
    let base = &node.__base_attr__;
    let mut pnode = serialize_common_attributes(io, cas, common, resource_type, registry)?;

    let attr = &node.__saved_query_attr__;
    if !base.depends_on.nodes.is_empty() {
        pnode.depends_on_nodes = base.depends_on.nodes.clone();
    }
    if !base.depends_on.macros.is_empty() {
        pnode.depends_on_macros = base.depends_on.macros.clone();
    }

    if let Some(group) = &attr.group {
        pnode
            .common_details
            .get_or_insert_with(Default::default)
            .group = Some(group.clone());
    }

    let saved_query_details = SavedQueryDetails {
        label: attr.label.clone(),
        group: attr.group.clone(),
        description: common.description.clone(),
        query_params: serde_json::to_string(&attr.query_params).ok(),
        exports: serialize_vec_to_json_if_not_empty(&attr.exports),
        metadata: serialize_option_to_json(&attr.metadata),
        depends_on: if base.depends_on.macros.is_empty()
            && base.depends_on.nodes.is_empty()
            && base.depends_on.nodes_with_ref_location.is_empty()
        {
            None
        } else {
            serde_json::to_string(&base.depends_on).ok()
        },
        refs: serialize_vec_to_json_if_not_empty(&base.refs),
        created_at: Some(attr.created_at),
        unrendered_config: serialize_map_to_json_if_not_empty(&attr.unrendered_config),
        config: serde_json::to_string(&node.deprecated_config).ok(),
    };
    pnode.saved_query_details = Some(saved_query_details);

    Ok(pnode)
}

pub fn serialize_to_model_pnode(
    io: &IoArgs,
    ctx: &WriteContext,
    cas: &mut HashMap<String, String>,
    node: &DbtModel,
    registry: &CompleteStateWithKind,
) -> FsResult<ParquetNode> {
    let resource_type = ResourceType::Model;
    let common: &CommonAttributes = &node.__common_attr__;
    let mut pnode = serialize_common_attributes(io, cas, common, resource_type, registry)?;
    pnode.warehouse_details = Some(serialize_base_attr(&node.__base_attr__));
    if !node.__base_attr__.depends_on.nodes.is_empty() {
        pnode.depends_on_nodes = node.__base_attr__.depends_on.nodes.clone();
    }
    if !node.__base_attr__.depends_on.macros.is_empty() {
        pnode.depends_on_macros = node.__base_attr__.depends_on.macros.clone();
    }

    let compiled_sql_cas_hash = if matches!(
        ctx.phase,
        NodeExecutionState::Compiled | NodeExecutionState::Run
    ) {
        // todo: read this from manifest_model
        let absolute_path = get_target_write_path(
            &io.in_dir,
            &io.out_dir.join(DBT_COMPILED_DIR_NAME),
            &node.__common_attr__.package_name,
            &node.__common_attr__.path,
            &node.__common_attr__.original_file_path,
        );
        if let Ok(compiled_sql) = stdfs::read_to_string(&absolute_path) {
            let hash = code_hash(&compiled_sql);
            cas.insert(hash.clone(), compiled_sql);
            Some(hash)
        } else {
            None
        }
    } else {
        None
    };
    let model_details = ModelDetails {
        introspection: Some(node.__model_attr__.introspection),
        compiled_sql_cas_hash,
        contract_alias_types: node.__model_attr__.contract.as_ref().map(|c| c.alias_types),
        contract_checksum: None, // todo: this is a value, must go into metablob..node.model_attr.contract.and_then(|c| c.checksum.clone()),
        contract_enforced: node.__model_attr__.contract.as_ref().map(|c| c.enforced),
        incremental_strategy: node.__model_attr__.incremental_strategy.clone(),
        version: node.__model_attr__.version.as_ref().map(|v| v.to_string()),
        latest_version: node
            .__model_attr__
            .latest_version
            .as_ref()
            .map(|v| v.to_string()),
        constraints: node.__model_attr__.constraints.clone(),
        deprecation_date: node.__model_attr__.deprecation_date.clone(),
        primary_key: node.__model_attr__.primary_key.clone(),
        event_time: node.__model_attr__.event_time.clone(),
        freshness: from_model_freshness(&node.__model_attr__.freshness),
    };
    pnode.model_details = Some(model_details);

    let common_details = pnode.common_details.get_or_insert_with(Default::default);
    common_details.access = Some(node.__model_attr__.access.clone());
    common_details.group = node.__model_attr__.group.clone();
    if node.__model_attr__.time_spine.is_some() {
        pnode.meta = extend_meta_blob(
            pnode.meta.take(),
            vec![(
                "__time_spine",
                serde_json::to_string(&node.__model_attr__.time_spine)
                    .unwrap_or_else(|_| "null".to_string()),
            )],
        );
    }

    // Serialize the deprecated_config to user_config_details
    pnode.user_config_details = Some(model_config_to_user_configs(&node.deprecated_config));

    Ok(pnode)
}

fn serialize_base_attr(base_attr: &NodeBaseAttributes) -> WarehouseDetails {
    WarehouseDetails {
        catalog: None, // fill if you have it
        database: Some(base_attr.database.clone()),
        schema: Some(base_attr.schema.clone()),
        alias: Some(base_attr.alias.clone()),
        relation_name: base_attr.relation_name.clone(),
        materialized: Some(base_attr.materialized.clone()),
        quoting: Some(serialize_quoting(&base_attr.quoting)),
        static_analysis: Some(base_attr.static_analysis.clone()),
        extended_model: Some(base_attr.extended_model),
    }
}

fn serialize_common_attributes(
    io: &IoArgs,
    cas: &mut HashMap<String, String>,
    common: &CommonAttributes,
    resource_type: ResourceType,
    registry: &CompleteStateWithKind,
) -> FsResult<ParquetNode> {
    // Meta fields
    let mut pnode = ParquetNode::new(
        resource_type,
        common.unique_id.clone(),
        common.name.clone(),
        Some(common.fqn.clone()),
    );

    pnode.input_files = collect_input_files(io, cas, common, registry)?;
    pnode.package_name = Some(common.package_name.clone());
    pnode.description = common.description.clone();
    pnode.tags = common.tags.clone();
    pnode.meta = if !common.meta.is_empty() {
        Some(MetaFields {
            meta_blob: Some(
                common
                    .meta
                    .iter()
                    .filter_map(|(k, v)| serde_json::to_string(v).ok().map(|s| (k.clone(), s)))
                    .collect(),
            ),
            ..Default::default()
        })
    } else {
        None
    };
    Ok(pnode)
}

pub fn extend_meta_blob<'a, I>(meta_option: Option<MetaFields>, pairs: I) -> Option<MetaFields>
where
    I: IntoIterator<Item = (&'a str, String)>,
{
    let new_blob: HashMap<String, String> =
        pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect();

    if new_blob.is_empty() {
        return meta_option;
    }

    match meta_option {
        Some(mut meta) => {
            match &mut meta.meta_blob {
                Some(existing_blob) => existing_blob.extend(new_blob),
                None => meta.meta_blob = Some(new_blob),
            }
            Some(meta)
        }
        None => Some(MetaFields {
            meta_blob: Some(new_blob),
            ..Default::default()
        }),
    }
}

// ------------------------------------------------------------------------------------------------
// deserialize

/// Inverse of serialize_resolver_state_to_parquet_nodes.
/// Reconstructs ResolverState and ManifestMetadata from a list of ParquetNode.
pub fn deserialize_parquet_nodes_to_resolver_state(
    pnodes: HashMap<String, ParquetNodeRef>,
    cas: &HashMap<String, String>,
    columns_lookup: &ParquetColumnLookup,
) -> ResolvedNodes {
    let mut resolved_nodes = ResolvedNodes::default();
    // let mut manifest_metadata = ManifestMetadata::default();
    // improve this. Find only the first session node
    let mut the_session = None;
    for pnode in pnodes.values() {
        if pnode.resource_type == ResourceType::Session {
            the_session = Some(pnode.clone());
            break;
        }
    }
    let the_session = match the_session {
        Some(session) => session,
        None => return ResolvedNodes::default(),
    };
    let pkg_install_path: Option<PathBuf> = the_session
        .session_details
        .as_ref()
        .and_then(|sd| sd.packages_install_path.as_deref())
        .map(PathBuf::from);

    // Partition nodes by type and enabled/disabled
    for node in pnodes.values() {
        match node.resource_type {
            ResourceType::Model => {
                let dbt_model =
                    deserialize_parquet_node_to_dbt_model(node, &pkg_install_path, columns_lookup);
                if node.is_enabled.unwrap_or(true) {
                    resolved_nodes
                        .nodes
                        .models
                        .insert(node.unique_id.clone(), Arc::new(dbt_model));
                } else {
                    resolved_nodes
                        .disabled_nodes
                        .models
                        .insert(node.unique_id.clone(), Arc::new(dbt_model));
                }
            }
            ResourceType::Test => {
                let dbt_test =
                    deserialize_parquet_node_to_dbt_test(node, &pkg_install_path, columns_lookup);
                if node.is_enabled.unwrap_or(true) {
                    resolved_nodes
                        .nodes
                        .tests
                        .insert(node.unique_id.clone(), Arc::new(dbt_test));
                } else {
                    resolved_nodes
                        .disabled_nodes
                        .tests
                        .insert(node.unique_id.clone(), Arc::new(dbt_test));
                }
            }
            ResourceType::Snapshot => {
                let dbt_snapshot = deserialize_parquet_node_to_dbt_snapshot(
                    node,
                    &pkg_install_path,
                    columns_lookup,
                );
                if node.is_enabled.unwrap_or(true) {
                    resolved_nodes
                        .nodes
                        .snapshots
                        .insert(node.unique_id.clone(), Arc::new(dbt_snapshot));
                } else {
                    resolved_nodes
                        .disabled_nodes
                        .snapshots
                        .insert(node.unique_id.clone(), Arc::new(dbt_snapshot));
                }
            }
            ResourceType::Seed => {
                let dbt_seed =
                    deserialize_parquet_node_to_dbt_seed(node, &pkg_install_path, columns_lookup);
                if node.is_enabled.unwrap_or(true) {
                    resolved_nodes
                        .nodes
                        .seeds
                        .insert(node.unique_id.clone(), Arc::new(dbt_seed));
                } else {
                    resolved_nodes
                        .disabled_nodes
                        .seeds
                        .insert(node.unique_id.clone(), Arc::new(dbt_seed));
                }
            }
            ResourceType::Source => {
                let dbt_source =
                    deserialize_parquet_node_to_dbt_source(node, &pkg_install_path, columns_lookup);
                if node.is_enabled.unwrap_or(true) {
                    resolved_nodes
                        .nodes
                        .sources
                        .insert(node.unique_id.clone(), Arc::new(dbt_source));
                } else {
                    resolved_nodes
                        .disabled_nodes
                        .sources
                        .insert(node.unique_id.clone(), Arc::new(dbt_source));
                }
            }
            ResourceType::UnitTest => {
                let dbt_unit_test = deserialize_parquet_node_to_dbt_unit_test(
                    node,
                    &pkg_install_path,
                    columns_lookup,
                );
                if node.is_enabled.unwrap_or(true) {
                    resolved_nodes
                        .nodes
                        .unit_tests
                        .insert(node.unique_id.clone(), Arc::new(dbt_unit_test));
                } else {
                    resolved_nodes
                        .disabled_nodes
                        .unit_tests
                        .insert(node.unique_id.clone(), Arc::new(dbt_unit_test));
                }
            }
            ResourceType::Macro => {
                let dbt_macro = deserialize_parquet_node_to_dbt_macro(node, cas);
                resolved_nodes
                    .macros
                    .macros
                    .insert(node.unique_id.clone(), dbt_macro);
            }
            ResourceType::DocsMacro => {
                let dbt_docs_macro = deserialize_parquet_node_to_dbt_docs_macro(node, cas);
                resolved_nodes
                    .macros
                    .docs_macros
                    .insert(node.unique_id.clone(), dbt_docs_macro);
            }
            ResourceType::Operation => {
                let dbt_operation = Spanned::new(deserialize_parquet_node_to_dbt_operation(
                    node,
                    &pkg_install_path,
                ));
                // Determine if it's on_run_start or on_run_end by node.id or other marker
                if node.unique_id.contains("on-run-start") {
                    resolved_nodes.operations.on_run_start.push(dbt_operation);
                } else if node.unique_id.contains("on-run-end") {
                    resolved_nodes.operations.on_run_end.push(dbt_operation);
                }
            }
            ResourceType::Session => {
                // skip session nodes, they are not part of the resolver state
            }
            _ => {}
        }
    }
    resolved_nodes
}

//     (resolver_state, manifest_metadata)
// }
// pub fn deserialize_parquet_node_to_dbt_command(node: &ParquetNode) -> CommandContext {
//     let binding = HashMap::new();
//     let meta_str = node.meta_str.as_ref().unwrap_or(&binding);

//     // Helper to extract Option<String>
//     let get = |k: &str| meta_str.get(k).cloned();

//     // Extract env_var and var maps
//     let env_var = meta_str
//         .iter()
//         .filter(|(k, _)| k.starts_with("env_var."))
//         .map(|(k, v)| (k.trim_start_matches("env_var.").to_string(), v.clone()))
//         .collect::<BTreeMap<_, _>>();
//     let var = meta_str
//         .iter()
//         .filter(|(k, _)| k.starts_with("var."))
//         .map(|(k, v)| (k.trim_start_matches("var.").to_string(), v.clone()))
//         .collect::<BTreeMap<_, _>>();

//     CommandContext {
//         account_id: get("account_id"),
//         project_id: get("project_id"),
//         environment_id: get("environment_id"),
//         user_id: get("user_id"),
//         invocation_id: get("invocation_id").unwrap_or_default(),
//         project_name: get("project_name").unwrap_or_default(),
//         input_files: node.input_files.clone(),
//         schema_version: get("schema_version").unwrap_or_default(),
//         tool_version: get("tool_version").unwrap_or_default(),
//         command: get("command").unwrap_or_default(),
//         generated_at: get("generated_at")
//             .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
//             .map(|dt| dt.with_timezone(&Utc))
//             .unwrap_or_else(Utc::now),
//         adapter_type: get("adapter_type").unwrap_or_default(),
//         env_var: if env_var.is_empty() {
//             None
//         } else {
//             Some(env_var)
//         },
//         var: if var.is_empty() { None } else { Some(var) },
//     }
// }
pub fn deserialize_parquet_node_to_dbt_source(
    node: &ParquetNode,
    pkg_install_path: &Option<PathBuf>,
    columns_lookup: &ParquetColumnLookup,
) -> DbtSource {
    // 1. Common attributes
    let common_attr = deserialize_common_attr(node, pkg_install_path);

    // 2. Base attributes (warehouse details)
    let base_attr = if let Some(ref warehouse_details) = node.warehouse_details {
        deserialize_base_attr_from_warehouse_details(
            &node.unique_id,
            warehouse_details,
            columns_lookup,
        )
    } else {
        NodeBaseAttributes::default()
    };

    // 3. Source-specific details
    let source_attr = if let Some(ref details) = node.source_details {
        dbt_source_details_to_attr(details)
    } else {
        DbtSourceAttr::default()
    };

    // 4. Depends on
    let base_attr = add_depends_on(node, base_attr);

    // 5. Restore deprecated_config from user_config_details and warehouse_details
    let deprecated_config = node
        .user_config_details
        .as_ref()
        .map(|config| user_configs_to_source_config(config, &node.warehouse_details))
        .unwrap_or_default();

    DbtSource {
        __common_attr__: common_attr,
        __base_attr__: base_attr,
        __source_attr__: source_attr,
        deprecated_config,
        ..Default::default()
    }
}
fn deserialize_common_attr(
    node: &ParquetNode,
    pkg_install_path: &Option<PathBuf>,
) -> CommonAttributes {
    // Path: first input file if present, else default
    let path: PathBuf = node
        .input_files
        .first()
        .map(|f| f.path.clone().into())
        .unwrap_or_default();
    // Patch path: second input file if present
    let patch_path: Option<PathBuf> = node.input_files.get(1).map(|f| f.path.clone().into());

    // Compute original_file_path robustly using pkg_install_path
    // if pkg_install_path is None, then the original_file_path is the same as path

    let original_file_path = if pkg_install_path.is_none() {
        path.clone()
    } else if let Some(pkg_install_path) = pkg_install_path {
        let pkg_last = pkg_install_path
            .components()
            .next_back()
            .map(|c| c.as_os_str().to_os_string());
        let mut comps = path.components();
        if let (Some(pkg_last), Some(first_comp)) = (pkg_last, comps.next()) {
            if first_comp.as_os_str() == pkg_last {
                comps.as_path().to_path_buf()
            } else {
                path.clone()
            }
        } else {
            path.clone()
        }
    } else {
        path.clone()
    };

    let meta = node
        .meta
        .as_ref()
        .and_then(|mf| mf.meta_blob.as_ref())
        .map(|blob| {
            blob.iter()
                .filter(|(k, _)| !k.starts_with("__") && !k.starts_with('$'))
                .filter_map(|(k, v)| serde_json::from_str(v).ok().map(|val| (k.clone(), val)))
                .collect()
        })
        .unwrap_or_default();
    CommonAttributes {
        unique_id: node.unique_id.clone(),
        name: node.name.clone(),
        name_span: dbt_common::Span::default(),
        package_name: node.package_name.clone().unwrap_or_default(),
        fqn: node.fqn.clone().unwrap_or_default(),
        path,
        original_file_path,
        patch_path,
        description: node.description.clone(),
        tags: node.tags.clone(),
        meta,
        checksum: DbtChecksum::default(),
        raw_code: None,
        language: None,
    }
}
// Converts WarehouseDetails back to NodeBaseAttributes
fn deserialize_base_attr_from_warehouse_details(
    unique_id: &str,
    details: &WarehouseDetails,
    columns_lookup: &ParquetColumnLookup,
) -> NodeBaseAttributes {
    let empty_pcolumns = Vec::new();
    let pcolumns = columns_lookup.get(unique_id).unwrap_or(&empty_pcolumns);
    NodeBaseAttributes {
        database: details.database.clone().unwrap_or_default(),
        schema: details.schema.clone().unwrap_or_default(),
        alias: details.alias.clone().unwrap_or_default(),
        relation_name: details.relation_name.clone(),
        materialized: details.materialized.clone().unwrap_or_default(),
        quoting: details
            .quoting
            .as_ref()
            .map(|q| deserialize_quoting(q))
            .unwrap_or_default(),
        static_analysis: details.static_analysis.clone().unwrap_or_default(),
        extended_model: details.extended_model.unwrap_or(false),
        columns: deserialize_columns(pcolumns),
        ..Default::default()
    }
}

// Converts SourceDetails back to your source_attr type
fn dbt_source_details_to_attr(details: &SourceDetails) -> DbtSourceAttr {
    // Fill this out as needed for your schema
    DbtSourceAttr {
        identifier: details.identifier.clone(),
        source_name: details.source_name.clone(),
        loader: details.loader.clone(),
        loaded_at_field: details.loaded_at_field.clone(),
        loaded_at_query: details.loaded_at_query.clone(),
        freshness: to_source_freshness(&details.freshness_details),
        ..Default::default()
    }
}
pub fn to_source_freshness(
    details: &Option<SourceFreshnessDetails>,
) -> Option<FreshnessDefinition> {
    details.as_ref().map(|d| {
        let error_after = if d.error_after_count.is_some() || d.error_after_period.is_some() {
            Some(FreshnessRules {
                count: d.error_after_count,
                period: d
                    .error_after_period
                    .as_ref()
                    .and_then(|p| FreshnessPeriod::from_str(p).ok()),
            })
        } else {
            None
        };
        let warn_after = if d.warn_after_count.is_some() || d.warn_after_period.is_some() {
            Some(FreshnessRules {
                count: d.warn_after_count,
                period: d
                    .warn_after_period
                    .clone()
                    .and_then(|p| FreshnessPeriod::from_str(&p).ok()),
            })
        } else {
            None
        };
        FreshnessDefinition {
            error_after,
            warn_after,
            filter: d.filter.clone(),
        }
    })
}

pub fn to_model_freshness(details: &Option<ModelFreshnessDetails>) -> Option<ModelFreshness> {
    details.as_ref().map(|d| {
        let build_after = if d.build_after_count.is_some()
            || d.build_after_period.is_some()
            || d.build_after_updates_on.is_some()
        {
            Some(ModelFreshnessRules {
                count: d.build_after_count,
                period: d
                    .build_after_period
                    .clone()
                    .and_then(|p| FreshnessPeriod::from_str(&p).ok()),
                updates_on: d
                    .build_after_updates_on
                    .as_ref()
                    .and_then(|u| UpdatesOn::from_str(u).ok()),
            })
        } else {
            None
        };
        ModelFreshness { build_after }
    })
}
/// Reconstruct a DbtMacro from a ParquetNode.
/// Note: macro_sql is not recoverable from CAS here; you must provide a lookup if needed.
pub fn deserialize_parquet_node_to_dbt_macro(
    node: &ParquetNode,
    cas: &HashMap<String, String>,
) -> DbtMacro {
    // Extract macro_sql from the CAS using the hash in the parse phase
    let macro_sql = node
        .macro_details
        .as_ref()
        .and_then(|details| details.macro_sql_cas_hash.as_ref())
        .and_then(|hash| cas.get(hash))
        .cloned()
        .unwrap_or_default();

    // Robustly extract input file paths
    let path = node
        .input_files
        .first()
        .map(|f| f.path.clone().into())
        .unwrap_or_default();
    let original_file_path = node
        .input_files
        .first()
        .map(|f| f.path.clone().into())
        .unwrap_or_default();
    let patch_path = node.input_files.get(2).map(|f| f.path.clone().into());

    DbtMacro {
        unique_id: node.unique_id.clone(),
        name: node.name.clone(),
        package_name: node.package_name.clone().unwrap_or_default(),
        description: node.description.clone().unwrap_or_default(),
        path,
        original_file_path,
        patch_path,
        macro_sql,
        depends_on: MacroDependsOn {
            macros: node
                .depends_on_macros
                .iter()
                .map(|m| m.to_string())
                .collect::<Vec<_>>(),
        },
        // todo: store spans as well, e.g. else where spans are required, but we forget them
        span: Some(Span::default()),
        // Fill in other fields as needed, or use Default::default()
        ..Default::default()
    }
}

pub fn deserialize_parquet_node_to_dbt_docs_macro(
    node: &ParquetNode,
    cas: &HashMap<String, String>,
) -> DbtDocsMacro {
    let block_contents = node
        .macro_details
        .as_ref()
        .and_then(|details| details.macro_sql_cas_hash.as_ref())
        .and_then(|hash| cas.get(hash))
        .cloned()
        .expect("block_contents is required");
    // Robustly extract input file paths
    let path = node
        .input_files
        .first()
        .map(|f| f.path.clone().into())
        .unwrap_or_default();
    let original_file_path = node
        .input_files
        .first()
        .map(|f| f.path.clone().into())
        .unwrap_or_default();

    DbtDocsMacro {
        unique_id: node.unique_id.clone(),
        name: node.name.clone(),
        package_name: node.package_name.clone().unwrap_or_default(),
        path,
        original_file_path,
        block_contents,
    }
}

/// Deserialize a ParquetNode into a DbtOperation.
/// Note: If you use a CAS for code, you may want to pass it in as an argument.
pub fn deserialize_parquet_node_to_dbt_operation(
    node: &ParquetNode,
    pkg_install_path: &Option<PathBuf>,
) -> DbtOperation {
    let common_attr = deserialize_common_attr(node, pkg_install_path);

    DbtOperation {
        __common_attr__: common_attr,
        ..Default::default()
    }
}
/// Deserialize a ParquetNode into a DbtSeed.
/// If you need CAS for code, add it as an argument.
pub fn deserialize_parquet_node_to_dbt_seed(
    node: &ParquetNode,
    pkg_install_path: &Option<PathBuf>,
    columns_lookup: &ParquetColumnLookup,
) -> DbtSeed {
    // 1. Common attributes
    let mut common_attr = deserialize_common_attr(node, pkg_install_path);
    // todo: Check. dbt_seed use original_file_path (and not path for seeds), so we are simulating this here, but why?
    common_attr.original_file_path = common_attr.path.clone();

    // 2. Base attributes (warehouse details)
    let base_attr = node
        .warehouse_details
        .as_ref()
        .map(|details| {
            deserialize_base_attr_from_warehouse_details(&node.unique_id, details, columns_lookup)
        })
        .unwrap_or_default();

    // 3. Seed-specific details
    let seed_attr_from_details = node
        .seed_details
        .as_ref()
        .map(from_seed_details_to_dbt_seed_attr)
        .unwrap_or_default();
    let seed_attr = DbtSeedAttr {
        catalog_name: node
            .user_config_details
            .as_ref()
            .and_then(|config| config.catalog_name.clone()),
        ..seed_attr_from_details
    };

    // 4. Depends on
    let base_attr = add_depends_on(node, base_attr);

    // 5. Restore deprecated_config from user_config_details and warehouse_details
    let deprecated_config = node
        .user_config_details
        .as_ref()
        .map(|config| user_configs_to_seed_config(config, &node.warehouse_details))
        .unwrap_or_default();

    DbtSeed {
        __common_attr__: common_attr,
        __base_attr__: base_attr,
        __seed_attr__: seed_attr,
        deprecated_config,
        ..Default::default()
    }
}

fn add_depends_on(node: &ParquetNode, base_attr: NodeBaseAttributes) -> NodeBaseAttributes {
    let mut base_attr = base_attr;
    if !node.depends_on_nodes.is_empty() {
        base_attr.depends_on.nodes = node.depends_on_nodes.clone();
        // todo: set depends_on.nodes_with_ref_location, but
        base_attr.depends_on.nodes_with_ref_location = node
            .depends_on_nodes
            .iter()
            // todo: we currenetly drop the location, maybe we should keep it? But for now we have to store the depends on ina ref_location as well.
            .map(|n| (n.to_string(), CodeLocationWithFile::default()))
            .collect::<Vec<_>>();
    }
    if !node.depends_on_macros.is_empty() {
        base_attr.depends_on.macros = node.depends_on_macros.clone();
    }
    base_attr
}

pub fn deserialize_parquet_node_to_dbt_snapshot(
    node: &ParquetNode,
    pkg_install_path: &Option<PathBuf>,
    columns_lookup: &ParquetColumnLookup,
) -> DbtSnapshot {
    // 1. Common attributes
    let common_attr = deserialize_common_attr(node, pkg_install_path);

    // 2. Base attributes (warehouse details)
    let base_attr = node
        .warehouse_details
        .as_ref()
        .map(|details| {
            deserialize_base_attr_from_warehouse_details(&node.unique_id, details, columns_lookup)
        })
        .unwrap_or_default();

    // 3. Snapshot-specific details
    let snapshot_attr = node
        .snapshot_details
        .as_ref()
        .map(snapshot_details_to_dbt_snapshot_attr)
        .unwrap_or_default();

    // 4. Depends on
    let base_attr = add_depends_on(node, base_attr);

    // 5. Restore deprecated_config from user_config_details and warehouse_details
    let deprecated_config = node
        .user_config_details
        .as_ref()
        .map(|config| user_configs_to_snapshot_config(config, &node.warehouse_details))
        .unwrap_or_default();

    DbtSnapshot {
        __common_attr__: common_attr,
        __base_attr__: base_attr,
        __snapshot_attr__: snapshot_attr,
        deprecated_config,
        ..Default::default()
    }
}

pub fn deserialize_parquet_node_to_dbt_test(
    node: &ParquetNode,
    pkg_install_path: &Option<PathBuf>,
    columns_lookup: &ParquetColumnLookup,
) -> DbtTest {
    // 1. Common attributes
    let mut common_attr = deserialize_common_attr(node, pkg_install_path);

    // For generic tests, override the path to match the format generated in persist_inner
    // Path should be: generic_tests/{test_name}.sql
    // TODO: we might need to thread IO args through to get the output dir.
    common_attr.path = PathBuf::from("generic_tests").join(format!("{}.sql", node.name));
    common_attr.original_file_path = PathBuf::from("target/").join(common_attr.path.clone());

    // 2. Base attributes (warehouse details)
    let base_attr = node
        .warehouse_details
        .as_ref()
        .map(|details| {
            deserialize_base_attr_from_warehouse_details(&node.unique_id, details, columns_lookup)
        })
        .unwrap_or_default();

    // 3. Test-specific
    let mut test_metadata: Option<TestMetadata> = None;
    if let Some(ref meta) = node.meta
        && let Some(ref metablob) = meta.meta_blob
        && let Some(test_meta) = metablob.get("__test_metadata")
        && let Ok(value) = serde_json::from_str::<TestMetadata>(test_meta)
    {
        test_metadata = Some(value);
    }
    let test_details = node.test_details.as_ref();
    let test_attr = dbt_schemas::schemas::DbtTestAttr {
        column_name: test_details.and_then(|d| d.column_name.clone()),
        attached_node: test_details.and_then(|d| d.attached_node.clone()),
        test_metadata,
        file_key_name: test_details.and_then(|d| d.file_key_name.clone()),
        introspection: test_details
            .map(|d| d.introspection)
            .unwrap_or(IntrospectionKind::None),
        original_name: None, // Not stored in parquet, will be re-populated during parsing if needed
        group: None,
    };

    // 4. Depends on
    let base_attr = add_depends_on(node, base_attr);

    // 5. Restore deprecated_config from user_config_details and warehouse_details
    let deprecated_config = node
        .user_config_details
        .as_ref()
        .map(|config| user_configs_to_data_test_config(config, &node.warehouse_details))
        .unwrap_or_default();

    let defined_at =
        test_details.and_then(|d| d.defined_at.as_ref().map(CodeLocationWithFile::from));

    DbtTest {
        __common_attr__: common_attr,
        __base_attr__: base_attr,
        __test_attr__: test_attr,
        deprecated_config,
        defined_at,
        ..Default::default()
    }
}

pub fn deserialize_parquet_node_to_dbt_unit_test(
    node: &ParquetNode,
    pkg_install_path: &Option<PathBuf>,
    columns_lookup: &ParquetColumnLookup,
) -> DbtUnitTest {
    // 1. Common attributes
    let common_attr = deserialize_common_attr(node, pkg_install_path);

    // 2. Base attributes (warehouse details)
    let base_attr = node
        .warehouse_details
        .as_ref()
        .map(|details| {
            deserialize_base_attr_from_warehouse_details(&node.unique_id, details, columns_lookup)
        })
        .unwrap_or_default();

    // 3. Unit test details
    let utd = node.unit_test_details.as_ref();

    // 4. Deserialize given and expect from details (or legacy meta_blob)
    let given_from_details = utd
        .and_then(|d| d.given.as_ref())
        .and_then(|json| serde_json::from_str::<Vec<Given>>(json).ok());
    let given_from_meta = node
        .meta
        .as_ref()
        .and_then(|mf| mf.meta_blob.as_ref())
        .and_then(|mb| mb.get("__given"))
        .and_then(|s| serde_json::from_str::<Vec<Given>>(s).ok());
    let given = given_from_details.or(given_from_meta).unwrap_or_default();

    let expect_from_details = utd
        .and_then(|d| d.expect.as_ref())
        .and_then(|json| serde_json::from_str::<Expect>(json).ok());
    let expect_from_meta = node
        .meta
        .as_ref()
        .and_then(|mf| mf.meta_blob.as_ref())
        .and_then(|mb| mb.get("__expect"))
        .and_then(|s| serde_json::from_str::<Expect>(s).ok());
    let expect = expect_from_details.or(expect_from_meta).unwrap_or_default();

    // 5. Versions include/exclude as Option<StringOrArrayOfStrings>
    let to_string_or_array = |v: &Vec<String>| {
        if v.is_empty() {
            None
        } else if v.len() == 1 {
            Some(StringOrArrayOfStrings::String(v[0].clone()))
        } else {
            Some(StringOrArrayOfStrings::ArrayOfStrings(v.clone()))
        }
    };

    let versions: Option<IncludeExclude> = utd.and_then(|details| {
        if !details.versions_include.is_empty() || !details.versions_exclude.is_empty() {
            Some(IncludeExclude {
                include: to_string_or_array(&details.versions_include),
                exclude: to_string_or_array(&details.versions_exclude),
            })
        } else {
            None
        }
    });

    let unit_test_attr = dbt_schemas::schemas::DbtUnitTestAttr {
        model: utd.map(|d| d.model.clone()).unwrap_or_default(),
        given,
        expect,
        version: utd.and_then(|d| d.version.clone().map(StringOrInteger::String)),
        versions,
        overrides: utd
            .and_then(|d| d.overrides.as_ref())
            .and_then(|json| serde_json::from_str(json).ok()),
    };

    let field_event_status = utd
        .and_then(|d| d.field_event_status.as_ref())
        .and_then(|json| serde_json::from_str::<BTreeMap<String, dbt_yaml::Value>>(json).ok());
    let field_pre_injected_sql = utd.and_then(|d| d.field_pre_injected_sql.clone());
    let tested_node_unique_id = utd.and_then(|d| d.tested_node_unique_id.clone());
    let this_input_node_unique_id = utd.and_then(|d| d.this_input_node_unique_id.clone());

    // 6. Depends on
    let base_attr = add_depends_on(node, base_attr);

    // 7. Restore deprecated_config from user_config_details and warehouse_details
    let deprecated_config = node
        .user_config_details
        .as_ref()
        .map(|config| user_configs_to_unit_test_config(config, &node.warehouse_details))
        .unwrap_or_default();

    DbtUnitTest {
        __common_attr__: common_attr,
        __base_attr__: base_attr,
        __unit_test_attr__: unit_test_attr,
        field_event_status,
        field_pre_injected_sql,
        tested_node_unique_id,
        this_input_node_unique_id,
        // Not yet persisted across warm-start parquet rounds; analyzer falls
        // back to its (1,1,0) default location for unit tests reloaded from
        // parquet. Populating this is a deliberate follow-up.
        defined_at: None,
        deprecated_config,
    }
}

// Helper function
fn _to_string_or_array(v: &[String]) -> Option<StringOrArrayOfStrings> {
    match v.len() {
        0 => None,
        1 => Some(StringOrArrayOfStrings::String(v[0].clone())),
        _ => Some(StringOrArrayOfStrings::ArrayOfStrings(v.to_vec())),
    }
}

fn deserialize_parquet_node_to_dbt_model(
    node: &ParquetNode,
    pkg_install_path: &Option<PathBuf>,
    columns_lookup: &ParquetColumnLookup,
) -> DbtModel {
    // 1. Common attributes
    let common_attr = deserialize_common_attr(node, pkg_install_path);
    // 2. Base attributes (warehouse details)
    let base_attr = node
        .warehouse_details
        .as_ref()
        .map(|details| {
            deserialize_base_attr_from_warehouse_details(&node.unique_id, details, columns_lookup)
        })
        .unwrap_or_default();
    // 3. Model details
    let model_details = node.model_details.as_ref();

    // 4. Access and group from common_details
    let (access, group) = node
        .common_details
        .as_ref()
        .map(|cd| (cd.access.clone(), cd.group.clone()))
        .unwrap_or((None, None));

    // 5. Time spine from meta_blob
    let time_spine = node
        .meta
        .as_ref()
        .and_then(|mf| mf.meta_blob.as_ref())
        .and_then(|blob| blob.get("__time_spine"))
        .and_then(|s| serde_json::from_str(s).ok());

    // 6. Model freshness
    let freshness = model_details
        .and_then(|d| d.freshness.as_ref())
        .and_then(|fd| to_model_freshness(&Some(fd.clone())));

    // 7. Contract
    let contract = if model_details
        .and_then(|d| d.contract_alias_types.as_ref())
        .is_some()
        || model_details
            .and_then(|d| d.contract_enforced.as_ref())
            .is_some()
    {
        Some(dbt_schemas::schemas::common::DbtContract {
            alias_types: model_details
                .and_then(|d| d.contract_alias_types)
                .unwrap_or(false),
            enforced: model_details
                .and_then(|d| d.contract_enforced)
                .unwrap_or(false),
            checksum: None, // If you store checksum in meta_blob, extract here
        })
    } else {
        None
    };

    // 8. ModelAttr
    let model_attr = dbt_schemas::schemas::DbtModelAttr {
        introspection: model_details
            .and_then(|d| d.introspection)
            .unwrap_or_default(),
        contract,
        incremental_strategy: model_details.and_then(|d| d.incremental_strategy.clone()),
        version: model_details.and_then(|d| d.version.clone().map(StringOrInteger::String)),
        latest_version: model_details
            .and_then(|d| d.latest_version.clone().map(StringOrInteger::String)),
        constraints: model_details
            .map(|d| d.constraints.clone())
            .unwrap_or_default(),
        deprecation_date: model_details.and_then(|d| d.deprecation_date.clone()),
        primary_key: model_details
            .map(|d| d.primary_key.clone())
            .unwrap_or_default(),
        event_time: model_details.and_then(|d| d.event_time.clone()),
        freshness,
        access: access.unwrap_or_default(),
        group,
        time_spine,
        catalog_name: node
            .user_config_details
            .as_ref()
            .and_then(|config| config.catalog_name.clone()),
        table_format: node
            .user_config_details
            .as_ref()
            .and_then(|config| config.table_format.clone()),
        sync: node
            .user_config_details
            .as_ref()
            .and_then(|config| config.sync.clone()),
    };

    // 9. Depends on
    let base_attr = add_depends_on(node, base_attr);

    // 10. Restore deprecated_config from user_config_details and warehouse_details
    let deprecated_config = node
        .user_config_details
        .as_ref()
        .map(|config| user_configs_to_model_config(config, &node.warehouse_details))
        .unwrap_or_default();

    DbtModel {
        __common_attr__: common_attr,
        __base_attr__: base_attr,
        __model_attr__: model_attr,
        deprecated_config,
        ..Default::default()
    }
}
// ------------------------------------------------------------------------------------------------
// helper

pub fn from_dbt_snapshot_attr_to_details(
    attr: &DbtSnapshotAttr,
    adapter_type: &str,
) -> SnapshotDetails {
    let meta = &attr.snapshot_meta_column_names;
    SnapshotDetails {
        dbt_scd_id: meta.get_dbt_scd_id(adapter_type),
        dbt_updated_at: meta.get_dbt_updated_at(adapter_type),
        dbt_valid_from: meta.get_dbt_valid_from(adapter_type),
        dbt_valid_to: meta.get_dbt_valid_to(adapter_type),
        dbt_is_deleted: meta.get_dbt_is_deleted(adapter_type),
        compiled_sql_cas_hash: None,
        introspection: attr.introspection,
    }
}

pub fn snapshot_details_to_dbt_snapshot_attr(details: &SnapshotDetails) -> DbtSnapshotAttr {
    DbtSnapshotAttr {
        snapshot_meta_column_names: SnapshotMetaColumnNames::new(
            Some(details.dbt_scd_id.clone()),
            Some(details.dbt_updated_at.clone()),
            Some(details.dbt_valid_from.clone()),
            Some(details.dbt_valid_to.clone()),
            Some(details.dbt_is_deleted.clone()),
        ),
        introspection: details.introspection,
        sync: None, // Sync config is populated from deprecated_config via user_config_details
    }
}

pub fn from_dbt_seed_attr_to_details(attr: &DbtSeedAttr) -> SeedDetails {
    SeedDetails {
        quote_columns: if attr.quote_columns { Some(true) } else { None },
        column_types: attr.column_types.clone(),
        delimiter: attr.delimiter.clone(),
        root_path: attr.root_path.as_ref().map(|p| p.display().to_string()),
    }
}

pub fn from_seed_details_to_dbt_seed_attr(details: &SeedDetails) -> DbtSeedAttr {
    DbtSeedAttr {
        quote_columns: details.quote_columns.unwrap_or(false),
        column_types: details.column_types.clone(),
        delimiter: details.delimiter.clone(),
        root_path: details.root_path.as_ref().map(PathBuf::from),
        catalog_name: None,
    }
}

pub fn from_source_freshness(
    fresh: &Option<FreshnessDefinition>,
) -> Option<SourceFreshnessDetails> {
    let fresh = fresh.as_ref()?;
    let (error_after_count, error_after_period) = match &fresh.error_after {
        Some(rules) => (rules.count, rules.period.as_ref().map(|p| p.to_string())),
        None => (None, None),
    };
    let (warn_after_count, warn_after_period) = match &fresh.warn_after {
        Some(rules) => (rules.count, rules.period.as_ref().map(|p| p.to_string())),
        None => (None, None),
    };

    Some(SourceFreshnessDetails {
        error_after_count,
        error_after_period,
        warn_after_count,
        warn_after_period,
        filter: fresh.filter.clone(),
    })
}
pub fn from_model_freshness(fresh: &Option<ModelFreshness>) -> Option<ModelFreshnessDetails> {
    let fresh = fresh.as_ref()?;
    let (build_after_count, build_after_period, build_after_updates_on) = match &fresh.build_after {
        Some(rules) => (
            rules.count,
            rules.period.as_ref().map(|p| p.to_string()),
            rules.updates_on.as_ref().map(|u| u.to_string()),
        ),
        None => (None, None, None),
    };

    Some(ModelFreshnessDetails {
        build_after_count,
        build_after_period,
        build_after_updates_on,
    })
}
pub fn dbt_source_attr_to_details(attr: &DbtSourceAttr) -> SourceDetails {
    SourceDetails {
        identifier: attr.identifier.clone(),
        source_name: attr.source_name.clone(),
        loader: attr.loader.clone(),
        loaded_at_field: attr.loaded_at_field.clone(),
        loaded_at_query: attr.loaded_at_query.clone(),
        freshness_details: from_source_freshness(&attr.freshness),
    }
}

pub fn serialize_quoting(q: &ResolvedQuoting) -> String {
    fn enc(opt: bool) -> char {
        match opt {
            true => 'q',
            false => 'n',
            // None => '_',
        }
    }
    [enc(q.database), enc(q.schema), enc(q.identifier)]
        .iter()
        .collect()
}

pub fn deserialize_quoting(s: &str) -> ResolvedQuoting {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() != 3 {
        panic!("Invalid quoting string: '{s}'. Expected 3 characters.");
    }
    fn dec(c: char) -> bool {
        match c {
            'q' => true,
            'n' => false,
            _ => unreachable!("Unexpected quoting character, must be 'q', 'n'"), // not used in this context
        }
    }
    ResolvedQuoting {
        database: dec(chars[0]),
        schema: dec(chars[1]),
        identifier: dec(chars[2]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_schemas::schemas::common::{DbtIncrementalStrategy, DbtUniqueKey, Severity};
    use dbt_schemas::schemas::serde::StringOrArrayOfStrings;
    use dbt_schemas::schemas::{DbtTest, IntrospectionKind};
    use dbt_test_primitives::assert_contains;
    use std::path::PathBuf;

    #[test]
    fn test_generic_test_path_preservation() {
        // Create a minimal ParquetNode that simulates a generic test
        let test_name = "not_null";
        let node = ParquetNode {
            name: test_name.to_string(),
            unique_id: format!("test.test_project.{test_name}"),
            resource_type: ResourceType::Test,
            package_name: Some("test_project".to_string()),
            test_details: Some(TestDetails {
                test_metadata_name: Some("not_null".to_string()),
                test_metadata_namespace: None,
                column_name: None,
                attached_node: None,
                file_key_name: None,
                introspection: IntrospectionKind::None,
                defined_at: None,
            }),
            tags: Vec::new(),
            depends_on_macros: Vec::new(),
            depends_on_nodes: Vec::new(),
            input_files: Vec::new(),
            ..Default::default()
        };

        // Call the function being tested
        let dbt_test: DbtTest = deserialize_parquet_node_to_dbt_test(&node, &None, &HashMap::new());

        // Verify the paths are set correctly
        assert_eq!(
            dbt_test.__common_attr__.path,
            PathBuf::from("generic_tests").join(format!("{test_name}.sql")),
            "Path should be set to generic_tests/<test_name>.sql"
        );

        assert_eq!(
            dbt_test.__common_attr__.original_file_path,
            PathBuf::from("target/")
                .join(PathBuf::from("generic_tests").join(format!("{test_name}.sql"))),
            "Original file path should be set to target/generic_tests/<test_name>.sql"
        );
    }

    #[test]
    fn test_generic_test_defined_at_round_trip() {
        // A generic test deserialized from a warm-start parquet must keep
        // `defined_at` (file + line + col + index) so that the analyzer's
        // `start_location` lands at the test's declaration in the YAML on
        // warm starts, not at line 0.
        let yaml_path = "models/marts/schema.yml";
        let node = ParquetNode {
            name: "not_null".to_string(),
            unique_id: "test.test_project.not_null".to_string(),
            resource_type: ResourceType::Test,
            package_name: Some("test_project".to_string()),
            test_details: Some(TestDetails {
                test_metadata_name: Some("not_null".to_string()),
                test_metadata_namespace: None,
                column_name: None,
                attached_node: None,
                file_key_name: None,
                introspection: IntrospectionKind::None,
                defined_at: Some(DefinedAtSpan {
                    file: yaml_path.to_string(),
                    line: 18,
                    col: 11,
                    index: 247,
                }),
            }),
            tags: Vec::new(),
            depends_on_macros: Vec::new(),
            depends_on_nodes: Vec::new(),
            input_files: Vec::new(),
            ..Default::default()
        };

        let dbt_test: DbtTest = deserialize_parquet_node_to_dbt_test(&node, &None, &HashMap::new());

        let restored = dbt_test
            .defined_at
            .expect("defined_at must round-trip from TestDetails.defined_at");
        assert_eq!(restored.file.as_path(), PathBuf::from(yaml_path).as_path());
        assert_eq!(restored.line, 18);
        assert_eq!(restored.col, 11);
        assert_eq!(restored.index, 247);
    }

    #[test]
    fn test_dbt_unique_key_conversions() {
        // Test single string conversion
        let single_key = Some(DbtUniqueKey::Single("id".to_string()));
        let converted = dbt_unique_key_to_string_or_array(&single_key);
        assert_eq!(
            converted,
            Some(StringOrArrayOfStrings::String("id".to_string()))
        );

        let back_converted = string_or_array_to_dbt_unique_key(&converted);
        assert_eq!(back_converted, single_key);

        // Test multiple keys conversion
        let multi_key = Some(DbtUniqueKey::Multiple(vec![
            "id".to_string(),
            "name".to_string(),
        ]));
        let converted = dbt_unique_key_to_string_or_array(&multi_key);
        assert_eq!(
            converted,
            Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "id".to_string(),
                "name".to_string()
            ]))
        );

        let back_converted = string_or_array_to_dbt_unique_key(&converted);
        assert_eq!(back_converted, multi_key);

        // Test None conversion
        let none_key: Option<DbtUniqueKey> = None;
        let converted = dbt_unique_key_to_string_or_array(&none_key);
        assert_eq!(converted, None);

        let back_converted = string_or_array_to_dbt_unique_key(&converted);
        assert_eq!(back_converted, None);
    }

    #[test]
    fn test_model_config_to_user_configs() {
        let model_config = ModelConfig {
            enabled: Some(true),
            tags: Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "tag1".to_string(),
                "tag2".to_string(),
            ])),
            materialized: Some(DbtMaterialization::Table),
            incremental_strategy: Some(DbtIncrementalStrategy::Append),
            full_refresh: Some(false),
            unique_key: Some(DbtUniqueKey::Single("id".to_string())),
            ..Default::default()
        };

        let user_configs = model_config_to_user_configs(&model_config);

        assert_eq!(user_configs.enabled, Some(true));
        assert_eq!(
            user_configs.tags,
            Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "tag1".to_string(),
                "tag2".to_string()
            ]))
        );
        assert_eq!(
            user_configs.incremental_strategy,
            Some(DbtIncrementalStrategy::Append)
        );
        assert_eq!(user_configs.full_refresh, Some(false));
        assert_eq!(
            user_configs.unique_key,
            Some(DbtUniqueKey::Single("id".to_string()))
        );
    }

    #[test]
    fn test_seed_config_to_user_configs() {
        let seed_config = SeedConfig {
            enabled: Some(false),
            quote_columns: Some(true),
            delimiter: Some(Spanned::new("|".to_string())),
            ..Default::default()
        };

        let user_configs = seed_config_to_user_configs(&seed_config);

        assert_eq!(user_configs.enabled, Some(false));
        assert_eq!(user_configs.quote_columns, Some(true));
        assert_eq!(user_configs.delimiter, Some("|".to_string()));
    }

    #[test]
    fn test_snapshot_config_to_user_configs() {
        let snapshot_config = SnapshotConfig {
            enabled: Some(true),
            strategy: Some("timestamp".to_string()),
            updated_at: Some("updated_at".to_string()),
            unique_key: Some(StringOrArrayOfStrings::String("id".to_string())),
            check_cols: Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "col1".to_string(),
            ])),
            ..Default::default()
        };

        let user_configs = snapshot_config_to_user_configs(&snapshot_config);

        assert_eq!(user_configs.enabled, Some(true));
        assert_eq!(user_configs.strategy, Some("timestamp".to_string()));
        assert_eq!(user_configs.updated_at, Some("updated_at".to_string()));
        assert_eq!(
            user_configs.unique_key,
            Some(DbtUniqueKey::Single("id".to_string()))
        );
        assert_eq!(
            user_configs.check_cols,
            Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "col1".to_string()
            ]))
        );
    }

    #[test]
    fn test_source_config_to_user_configs() {
        let source_config = SourceConfig {
            enabled: Some(true),
            loaded_at_field: Some("loaded_at".to_string()),
            ..Default::default()
        };

        let user_configs = source_config_to_user_configs(&source_config);

        assert_eq!(user_configs.enabled, Some(true));
        assert_eq!(user_configs.loaded_at_field, Some("loaded_at".to_string()));
    }

    #[test]
    fn test_data_test_config_to_user_configs() {
        let test_config = DataTestConfig {
            enabled: Some(false),
            severity: Some(Severity::Error),
            store_failures: Some(true),
            limit: Some(100),
            ..Default::default()
        };

        let user_configs = data_test_config_to_user_configs(&test_config);

        assert_eq!(user_configs.enabled, Some(false));
        assert!(matches!(user_configs.severity, Some(Severity::Error)));
        assert_eq!(user_configs.store_failures, Some(true));
        assert_eq!(user_configs.limit, Some(100));
    }

    #[test]
    fn test_unit_test_config_to_user_configs() {
        let unit_test_config = UnitTestConfig {
            enabled: Some(true),
            tags: Some(StringOrArrayOfStrings::String("unit-test".to_string())),
            ..Default::default()
        };

        let user_configs = unit_test_config_to_user_configs(&unit_test_config);

        assert_eq!(user_configs.enabled, Some(true));
        assert_eq!(
            user_configs.tags,
            Some(StringOrArrayOfStrings::String("unit-test".to_string()))
        );
    }

    #[test]
    fn test_user_configs_round_trip_model() {
        // Create a ModelConfig with various fields set
        let original_config = ModelConfig {
            enabled: Some(true),
            tags: Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "model".to_string(),
                "important".to_string(),
            ])),
            materialized: Some(DbtMaterialization::Incremental),
            incremental_strategy: Some(DbtIncrementalStrategy::DeleteInsert),
            unique_key: Some(DbtUniqueKey::Multiple(vec![
                "id".to_string(),
                "created_at".to_string(),
            ])),
            full_refresh: Some(false),
            ..Default::default()
        };

        // Create warehouse details that would normally come from the warehouse_details field
        let warehouse_details = Some(WarehouseDetails {
            catalog: Some("test_catalog".to_string()),
            database: Some("test_db".to_string()),
            schema: Some("test_schema".to_string()),
            alias: Some("test_alias".to_string()),
            relation_name: Some("test_catalog.test_db.test_schema.test_alias".to_string()),
            materialized: Some(DbtMaterialization::Incremental),
            quoting: None,
            static_analysis: None,
            extended_model: Some(false),
        });

        // Convert to UserConfigs
        let user_configs = model_config_to_user_configs(&original_config);

        // Convert back to ModelConfig
        let restored_config = user_configs_to_model_config(&user_configs, &warehouse_details);

        // Verify key fields are preserved
        assert_eq!(restored_config.enabled, original_config.enabled);
        assert_eq!(restored_config.tags, original_config.tags);
        assert_eq!(
            restored_config.incremental_strategy,
            original_config.incremental_strategy
        );
        assert_eq!(restored_config.unique_key, original_config.unique_key);
        assert_eq!(restored_config.full_refresh, original_config.full_refresh);
    }

    #[test]
    fn test_user_configs_round_trip_seed() {
        let original_config = SeedConfig {
            enabled: Some(false),
            quote_columns: Some(true),
            delimiter: Some(Spanned::new(",".to_string())),
            ..Default::default()
        };

        let warehouse_details = Some(WarehouseDetails {
            catalog: Some("seed_catalog".to_string()),
            database: Some("seed_db".to_string()),
            schema: Some("seed_schema".to_string()),
            alias: Some("seed_table".to_string()),
            relation_name: Some("seed_catalog.seed_db.seed_schema.seed_table".to_string()),
            materialized: None,
            quoting: None,
            static_analysis: None,
            extended_model: None,
        });

        let user_configs = seed_config_to_user_configs(&original_config);
        let restored_config = user_configs_to_seed_config(&user_configs, &warehouse_details);

        assert_eq!(restored_config.enabled, original_config.enabled);
        assert_eq!(restored_config.quote_columns, original_config.quote_columns);
        assert_eq!(
            restored_config.delimiter.map(|s| s.into_inner()),
            original_config.delimiter.map(|s| s.into_inner())
        );
    }

    #[test]
    fn test_user_configs_round_trip_snapshot() {
        let original_config = SnapshotConfig {
            enabled: Some(true),
            strategy: Some("check".to_string()),
            unique_key: Some(StringOrArrayOfStrings::String("user_id".to_string())),
            check_cols: Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "email".to_string(),
                "status".to_string(),
            ])),
            ..Default::default()
        };

        let warehouse_details = Some(WarehouseDetails {
            catalog: Some("snap_catalog".to_string()),
            database: Some("snap_db".to_string()),
            schema: Some("snap_schema".to_string()),
            alias: None,
            relation_name: Some("snap_catalog.snap_db.snap_schema".to_string()),
            materialized: Some(DbtMaterialization::Snapshot),
            quoting: None,
            static_analysis: None,
            extended_model: None,
        });

        let user_configs = snapshot_config_to_user_configs(&original_config);
        let restored_config = user_configs_to_snapshot_config(&user_configs, &warehouse_details);

        assert_eq!(restored_config.enabled, original_config.enabled);
        assert_eq!(restored_config.strategy, original_config.strategy);
        assert_eq!(restored_config.unique_key, original_config.unique_key);
        assert_eq!(restored_config.check_cols, original_config.check_cols);
    }

    #[test]
    fn test_user_configs_empty_values() {
        // Test that empty/default UserConfigs can be converted without panicking
        let empty_user_configs = UserConfigs::default();
        let empty_warehouse_details = None;

        let model_config =
            user_configs_to_model_config(&empty_user_configs, &empty_warehouse_details);
        assert_eq!(model_config.enabled, None);
        assert_eq!(model_config.tags, None);

        let seed_config =
            user_configs_to_seed_config(&empty_user_configs, &empty_warehouse_details);
        assert_eq!(seed_config.enabled, None);
        assert_eq!(seed_config.quote_columns, None);

        let snapshot_config =
            user_configs_to_snapshot_config(&empty_user_configs, &empty_warehouse_details);
        assert_eq!(snapshot_config.enabled, None);
        assert_eq!(snapshot_config.strategy, None);
    }

    #[test]
    fn test_quoting_serialization_round_trip() {
        let original = ResolvedQuoting {
            database: true,
            schema: false,
            identifier: true,
        };

        let serialized = serialize_quoting(&original);
        let deserialized = deserialize_quoting(&serialized);

        assert_eq!(original, deserialized);
        assert_eq!(serialized, "qnq"); // true=q, false=n, true=q
    }

    #[test]
    fn test_quoting_all_combinations() {
        let test_cases = [
            (
                ResolvedQuoting {
                    database: true,
                    schema: true,
                    identifier: true,
                },
                "qqq",
            ),
            (
                ResolvedQuoting {
                    database: false,
                    schema: false,
                    identifier: false,
                },
                "nnn",
            ),
            (
                ResolvedQuoting {
                    database: true,
                    schema: false,
                    identifier: true,
                },
                "qnq",
            ),
            (
                ResolvedQuoting {
                    database: false,
                    schema: true,
                    identifier: false,
                },
                "nqn",
            ),
        ];

        for (original, expected_str) in test_cases.iter() {
            let serialized = serialize_quoting(original);
            assert_eq!(serialized, *expected_str);

            let deserialized = deserialize_quoting(&serialized);
            assert_eq!(*original, deserialized);
        }
    }

    #[test]
    fn test_complex_model_config_round_trip() {
        // Test a complex ModelConfig with many fields populated
        // Set up complex configuration
        let original_config = ModelConfig {
            enabled: Some(false),
            tags: Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "finance".to_string(),
                "daily".to_string(),
                "critical".to_string(),
            ])),
            materialized: Some(DbtMaterialization::Incremental),
            incremental_strategy: Some(DbtIncrementalStrategy::Merge),
            unique_key: Some(DbtUniqueKey::Multiple(vec![
                "user_id".to_string(),
                "event_id".to_string(),
                "timestamp".to_string(),
            ])),
            full_refresh: Some(true),
            sql_header: Some(
                "-- Custom SQL header\nSET search_path = finance, public;".to_string(),
            ),
            location: Some("s3://data-lake/finance/user_events/".to_string()),
            predicates: Some(vec![
                "event_date >= current_date - interval '7 days'".to_string(),
                "user_id IS NOT NULL".to_string(),
            ]),
            ..Default::default()
        };

        let warehouse_details = Some(WarehouseDetails {
            catalog: Some("production".to_string()),
            database: Some("analytics".to_string()),
            schema: Some("finance_marts".to_string()),
            alias: Some("user_event_summary".to_string()),
            relation_name: Some(
                "production.analytics.finance_marts.user_event_summary".to_string(),
            ),
            materialized: Some(DbtMaterialization::Incremental),
            quoting: Some("qnq".to_string()),
            static_analysis: None,
            extended_model: Some(true),
        });

        // Convert to UserConfigs and back
        let user_configs = model_config_to_user_configs(&original_config);
        let restored_config = user_configs_to_model_config(&user_configs, &warehouse_details);

        // Verify complex fields are preserved
        assert_eq!(restored_config.enabled, original_config.enabled);
        assert_eq!(restored_config.tags, original_config.tags);
        assert_eq!(
            restored_config.incremental_strategy,
            original_config.incremental_strategy
        );
        assert_eq!(restored_config.unique_key, original_config.unique_key);
        assert_eq!(restored_config.full_refresh, original_config.full_refresh);
        assert_eq!(restored_config.sql_header, original_config.sql_header);
        assert_eq!(restored_config.location, original_config.location);
        assert_eq!(restored_config.predicates, original_config.predicates);
    }

    #[test]
    fn test_special_characters_in_config() {
        // Test handling of special characters, unicode, and edge case strings
        // Test special characters and unicode in various string fields
        let model_config = ModelConfig {
            sql_header: Some(
                "/* 特殊字符测试 */ -- Comment with émojis 🚀\nSET timezone = 'UTC';".to_string(),
            ),
            location: Some("s3://bucket/path with spaces/special-chars_123/ñoël/".to_string()),
            ..Default::default()
        };

        // Test empty and whitespace-only strings
        let empty_config = ModelConfig {
            sql_header: Some("".to_string()),
            location: Some("   ".to_string()),
            ..Default::default()
        };

        let user_configs1 = model_config_to_user_configs(&model_config);
        let user_configs2 = model_config_to_user_configs(&empty_config);

        // Verify special characters are preserved
        assert_eq!(user_configs1.sql_header, model_config.sql_header);
        assert_eq!(user_configs1.location, model_config.location);

        // Verify empty/whitespace strings are preserved
        assert_eq!(user_configs2.sql_header, Some("".to_string()));
        assert_eq!(user_configs2.location, Some("   ".to_string()));
    }

    #[test]
    fn test_hooks_conversion_edge_cases() {
        use dbt_schemas::schemas::common::Hooks as CommonHooks;

        // Test single string hook
        let single_hook = Some(CommonHooks::String("pre_hook_sql".to_string()));
        let converted = common_hooks_to_hooks(&single_hook);
        assert!(converted.is_some());
        let hooks = converted.unwrap();
        assert_eq!(hooks.run_before, vec!["pre_hook_sql"]);

        // Test array of strings hook
        let array_hook = Some(CommonHooks::ArrayOfStrings(vec![
            "hook1".to_string(),
            "hook2".to_string(),
        ]));
        let converted = common_hooks_to_hooks(&array_hook);
        assert!(converted.is_some());
        let hooks = converted.unwrap();
        assert_eq!(hooks.run_before, vec!["hook1", "hook2"]);

        // Test conversion back to CommonHooks
        let hooks = crate::parquet_node::Hooks {
            run_before: vec!["test_hook".to_string()],
            run_before_as_transaction: Some(false),
            run_after: vec![],
            run_after_as_transaction: None,
        };
        let converted_back = hooks_to_common_hooks(&Some(hooks));
        assert!(matches!(converted_back, Some(CommonHooks::String(_))));
    }

    #[test]
    fn test_large_configuration_handling() {
        // Test handling of large configurations with many tags, predicates, etc.
        let mut config = ModelConfig::default();

        // Large number of tags
        let many_tags: Vec<String> = (0..100).map(|i| format!("tag_{i}")).collect();
        config.tags = Some(StringOrArrayOfStrings::ArrayOfStrings(many_tags.clone()));

        // Large number of predicates
        let many_predicates: Vec<String> = (0..50)
            .map(|i| format!("column_{} IS NOT NULL AND column_{} > {}", i, i, i * 10))
            .collect();
        config.predicates = Some(many_predicates.clone());

        // Long SQL header
        let long_sql_header = "-- ".to_string() + &"Very long comment ".repeat(100);
        config.sql_header = Some(long_sql_header.clone());

        let user_configs = model_config_to_user_configs(&config);

        // Verify large collections are preserved
        assert_eq!(
            user_configs.tags,
            Some(StringOrArrayOfStrings::ArrayOfStrings(many_tags))
        );
        assert_eq!(user_configs.predicates, Some(many_predicates));
        assert_eq!(user_configs.sql_header, Some(long_sql_header));
    }

    #[test]
    fn test_mixed_none_some_values() {
        // Test configurations with mix of None and Some values across different node types
        let model_config = ModelConfig {
            enabled: Some(true),
            tags: None,
            full_refresh: Some(false),
            unique_key: None,
            ..Default::default()
        };

        let seed_config = SeedConfig {
            enabled: None,
            quote_columns: Some(true),
            delimiter: None,
            ..Default::default()
        };

        let user_configs1 = model_config_to_user_configs(&model_config);
        let user_configs2 = seed_config_to_user_configs(&seed_config);

        // Verify None values remain None and Some values are preserved
        assert_eq!(user_configs1.enabled, Some(true));
        assert_eq!(user_configs1.tags, None);
        assert_eq!(user_configs1.full_refresh, Some(false));
        assert_eq!(user_configs1.unique_key, None);

        assert_eq!(user_configs2.enabled, None);
        assert_eq!(user_configs2.quote_columns, Some(true));
        assert_eq!(user_configs2.delimiter, None);
    }

    #[test]
    fn test_extreme_unique_key_cases() {
        // Test very long unique key arrays
        let many_keys: Vec<String> = (0..20).map(|i| format!("key_column_{i}")).collect();
        let multi_key = Some(DbtUniqueKey::Multiple(many_keys.clone()));

        let converted = dbt_unique_key_to_string_or_array(&multi_key);
        assert_eq!(
            converted,
            Some(StringOrArrayOfStrings::ArrayOfStrings(many_keys))
        );

        let back_converted = string_or_array_to_dbt_unique_key(&converted);
        assert_eq!(back_converted, multi_key);

        // Test single key with special characters
        let special_key = Some(DbtUniqueKey::Single("user_id_with_émojis_🔑".to_string()));
        let converted = dbt_unique_key_to_string_or_array(&special_key);
        let back_converted = string_or_array_to_dbt_unique_key(&converted);
        assert_eq!(back_converted, special_key);
    }

    #[test]
    fn test_warehouse_details_edge_cases() {
        // Test with completely empty warehouse details
        let empty_warehouse_details: Option<WarehouseDetails> = None;
        let empty_user_configs = UserConfigs::default();

        let model_config =
            user_configs_to_model_config(&empty_user_configs, &empty_warehouse_details);
        assert_eq!(model_config.alias, None);
        assert_eq!(model_config.database, Omissible::Omitted);

        // Test with partially filled warehouse details
        let partial_warehouse_details = Some(WarehouseDetails {
            catalog: None,
            database: Some("only_db".to_string()),
            schema: None,
            alias: Some("only_alias".to_string()),
            relation_name: None,
            materialized: None,
            quoting: None,
            static_analysis: None,
            extended_model: None,
        });

        let model_config =
            user_configs_to_model_config(&empty_user_configs, &partial_warehouse_details);
        assert_eq!(
            model_config.database,
            Omissible::Present(Some("only_db".to_string()))
        );
        assert_eq!(model_config.alias, Some("only_alias".to_string()));
    }

    #[test]
    fn test_parquet_node_with_user_configs() {
        // Test that ParquetNode can be created with user_config_details populated
        let mut parquet_node = ParquetNode::default();

        // Create a UserConfigs instance with some basic fields
        let user_configs = UserConfigs {
            enabled: Some(true),
            tags: Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "test".to_string(),
            ])),
            description: Some("Test model description".to_string()),
            full_refresh: Some(false),
            unique_key: Some(DbtUniqueKey::Single("id".to_string())),
            // Avoid warehouse_specific_config for now due to serialization issues
            warehouse_specific_config: None,
            ..Default::default()
        };

        parquet_node.user_config_details = Some(user_configs);

        // Verify the user config details are properly stored
        assert!(parquet_node.user_config_details.is_some());
        let stored_configs = parquet_node.user_config_details.as_ref().unwrap();
        assert_eq!(stored_configs.enabled, Some(true));
        assert_eq!(
            stored_configs.tags,
            Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "test".to_string()
            ]))
        );
        assert_eq!(
            stored_configs.description,
            Some("Test model description".to_string())
        );
        assert_eq!(stored_configs.full_refresh, Some(false));
        assert_eq!(
            stored_configs.unique_key,
            Some(DbtUniqueKey::Single("id".to_string()))
        );
    }

    #[test]
    fn test_user_configs_schema_compatibility() {
        // Test that the UserConfigs struct has all the expected fields for the schema
        let user_configs = UserConfigs::default();

        // Verify all major field categories exist by checking they can be set
        let mut test_configs = user_configs;

        // Common fields
        test_configs.enabled = Some(true);
        test_configs.tags = Some(StringOrArrayOfStrings::String("test".to_string()));
        test_configs.meta = None; // BTreeMap can be None
        test_configs.group = Some("test_group".to_string());
        test_configs.description = Some("test description".to_string());

        // ModelConfig specific fields
        test_configs.incremental_strategy = Some(DbtIncrementalStrategy::Append);
        test_configs.full_refresh = Some(true);
        test_configs.unique_key = Some(DbtUniqueKey::Single("test_key".to_string()));

        // SeedConfig specific fields
        test_configs.quote_columns = Some(false);
        test_configs.delimiter = Some(",".to_string());

        // SnapshotConfig specific fields
        test_configs.strategy = Some("timestamp".to_string());
        test_configs.updated_at = Some("updated_at_col".to_string());

        // SourceConfig specific fields
        test_configs.loaded_at_field = Some("loaded_at".to_string());

        // DataTestConfig specific fields
        test_configs.limit = Some(1000);
        test_configs.store_failures = Some(true);

        // Verify all fields were set successfully (basic structural test)
        assert_eq!(test_configs.enabled, Some(true));
        assert_eq!(test_configs.quote_columns, Some(false));
        assert_eq!(test_configs.strategy, Some("timestamp".to_string()));
        assert_eq!(test_configs.loaded_at_field, Some("loaded_at".to_string()));
        assert_eq!(test_configs.limit, Some(1000));
    }

    #[test]
    fn test_warehouse_specific_config_serialization() {
        // Test that WarehouseSpecificNodeConfig can be properly serialized/deserialized
        let warehouse_config = WarehouseSpecificNodeConfig {
            partition_by: None,
            cluster_by: None,
            hours_to_expiration: Some(24),
            labels: Some(IndexMap::from([
                ("env".to_string(), "test".to_string()),
                ("team".to_string(), "data".to_string()),
            ])),
            labels_from_meta: Some(true),
            ..Default::default()
        };

        // Test serialization
        let serialized = serialize_warehouse_specific_config(&Some(warehouse_config));
        assert!(serialized.is_some());
        let json_string = serialized.unwrap();

        // Verify it's valid JSON and contains expected data
        assert_contains!(json_string, "hours_to_expiration");
        assert_contains!(json_string, "24");
        assert_contains!(json_string, "labels");
        assert_contains!(json_string, "env");
        assert_contains!(json_string, "test");

        // Test deserialization
        let deserialized = deserialize_warehouse_specific_config(&Some(json_string));
        assert!(deserialized.is_some());
        let restored_config = deserialized.unwrap();

        // Verify round-trip correctness
        assert_eq!(restored_config.hours_to_expiration, Some(24));
        assert_eq!(restored_config.labels_from_meta, Some(true));
        assert!(restored_config.labels.is_some());
        let labels = restored_config.labels.unwrap();
        assert_eq!(labels.get("env"), Some(&"test".to_string()));
        assert_eq!(labels.get("team"), Some(&"data".to_string()));
    }

    #[test]
    fn test_warehouse_specific_config_edge_cases() {
        // Test with None
        let serialized = serialize_warehouse_specific_config(&None);
        assert_eq!(serialized, None);

        let deserialized = deserialize_warehouse_specific_config(&None);
        assert_eq!(deserialized, None);

        // Test with empty config
        let empty_config = WarehouseSpecificNodeConfig::default();
        let serialized = serialize_warehouse_specific_config(&Some(empty_config));
        assert!(serialized.is_some());

        let deserialized = deserialize_warehouse_specific_config(&serialized);
        assert!(deserialized.is_some());
        let restored = deserialized.unwrap();
        assert_eq!(restored.hours_to_expiration, None);
        assert_eq!(restored.labels, None);

        // Test with invalid JSON (should return None gracefully)
        let invalid_json = Some("{ invalid json".to_string());
        let deserialized = deserialize_warehouse_specific_config(&invalid_json);
        assert_eq!(deserialized, None);
    }

    #[test]
    fn test_model_config_with_warehouse_specific_config() {
        // Test full round-trip with warehouse-specific config included
        let warehouse_config = WarehouseSpecificNodeConfig {
            hours_to_expiration: Some(72),
            labels: Some(IndexMap::from([(
                "project".to_string(),
                "analytics".to_string(),
            )])),
            ..Default::default()
        };

        let model_config = ModelConfig {
            enabled: Some(true),
            materialized: Some(DbtMaterialization::Table),
            __warehouse_specific_config__: warehouse_config,
            ..Default::default()
        };

        // Convert to UserConfigs
        let user_configs = model_config_to_user_configs(&model_config);

        // Verify warehouse config was serialized
        assert!(user_configs.warehouse_specific_config.is_some());
        let json_str = user_configs.warehouse_specific_config.as_ref().unwrap();
        assert_contains!(json_str, "hours_to_expiration");
        assert_contains!(json_str, "72");
        assert_contains!(json_str, "project");

        // Convert back to ModelConfig
        let warehouse_details = Some(WarehouseDetails {
            catalog: Some("test_catalog".to_string()),
            database: Some("test_db".to_string()),
            schema: Some("test_schema".to_string()),
            alias: Some("test_model".to_string()),
            relation_name: Some("test_catalog.test_db.test_schema.test_model".to_string()),
            materialized: Some(DbtMaterialization::Table),
            quoting: None,
            static_analysis: None,
            extended_model: None,
        });

        let restored_config = user_configs_to_model_config(&user_configs, &warehouse_details);

        // Verify warehouse-specific config was restored
        assert_eq!(
            restored_config
                .__warehouse_specific_config__
                .hours_to_expiration,
            Some(72)
        );
        assert!(
            restored_config
                .__warehouse_specific_config__
                .labels
                .is_some()
        );
        let labels = restored_config
            .__warehouse_specific_config__
            .labels
            .unwrap();
        assert_eq!(labels.get("project"), Some(&"analytics".to_string()));
    }
}
