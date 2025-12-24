use std::collections::BTreeMap;

use dbt_common::serde_utils::convert_yml_to_value_map;
use dbt_common::{AdapterError, AdapterErrorKind, AdapterResult};
use dbt_schemas::schemas::project::ModelConfig;
use dbt_schemas::schemas::{CommonAttributes, InternalDbtNodeWrapper};
use minijinja::Value;
use minijinja::value::mutable_vec::MutableVec;

use crate::catalog_relation::CatalogRelation;
use crate::{AdapterType, load_catalogs};

/// Shared, pure helper to compute common table options for BigQuery without DB access.
pub(crate) fn get_common_table_options_value(
    state: &minijinja::State,
    config: ModelConfig,
    common_attr: &CommonAttributes,
    temporary: bool,
) -> BTreeMap<String, Value> {
    let _ = state;
    let mut result = BTreeMap::new();

    if let Some(hours) = config.__warehouse_specific_config__.hours_to_expiration
        && !temporary
    {
        let expiration = format!("TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {hours} hour)");
        result.insert("expiration_timestamp".to_string(), Value::from(expiration));
    }

    // Handle description if persist_docs is enabled
    if let Some(persist_docs) = &config.persist_docs
        && persist_docs.relation.unwrap_or(false)
        && let Some(description) = &common_attr.description
    {
        let escaped_description = description.replace('\\', "\\\\").replace('"', "\\\"");
        result.insert(
            "description".to_string(),
            Value::from(format!("\"\"\"{escaped_description}\"\"\"")),
        );
    }

    let mut labels = config
        .__warehouse_specific_config__
        .labels
        .clone()
        .unwrap_or_default()
        .into_iter()
        .map(|(key, value)| Value::from_iter(vec![Value::from(key), Value::from(value)]))
        .collect::<Vec<_>>();

    // https://github.com/dbt-labs/dbt-adapters/pull/890
    // Merge with priority to labels
    if config
        .__warehouse_specific_config__
        .labels_from_meta
        .unwrap_or_default()
        && let Some(meta) = &config.meta
    {
        // Convert meta values to strings
        for (key, value) in meta {
            labels.push(Value::from_iter(vec![
                Value::from(key),
                value.as_str().map(Value::from).unwrap_or_default(),
            ]));
        }
    }

    // Add labels to opts if any exist
    if !labels.is_empty() {
        result.insert(
            "labels".to_string(),
            Value::from_object(MutableVec::from_iter(labels)),
        );
    }

    let resource_tags = config
        .__warehouse_specific_config__
        .resource_tags
        .unwrap_or_default()
        .into_iter()
        .map(|(key, value)| Value::from_iter(vec![Value::from(key), Value::from(value)]))
        .collect::<Vec<_>>();

    // Add resource_tags to opts if any exist
    if !resource_tags.is_empty() {
        result.insert(
            "tags".to_string(),
            Value::from_object(MutableVec::from_iter(resource_tags)),
        );
    }

    result
}

/// Shared, pure helper to compute full table options for BigQuery without DB access.
pub(crate) fn get_table_options_value(
    state: &minijinja::State,
    config: ModelConfig,
    node: &InternalDbtNodeWrapper,
    temporary: bool,
    adapter_type: AdapterType,
) -> AdapterResult<BTreeMap<String, Value>> {
    // Common options
    let common_attr = node.as_internal_node().common();
    let mut opts = get_common_table_options_value(state, config.clone(), common_attr, temporary);

    // Node serialization and catalogs lookup are in-memory/pure
    // TODO(anna): Ideally from_model_config_and_catalogs would just take in an InternalDbtNodeWrapper instead of a Value. This is blocked by a Snowflake hack in `snowflake__drop_table`.
    let node_yml = node.as_internal_node().serialize();
    let catalog_relation = CatalogRelation::from_model_config_and_catalogs(
        &adapter_type,
        &Value::from_object(convert_yml_to_value_map(node_yml)),
        load_catalogs::fetch_catalogs(),
    )?;

    // KMS key name if present
    if let Some(kms_key_name) = config.__warehouse_specific_config__.kms_key_name {
        opts.insert(
            "kms_key_name".to_string(),
            Value::from(format!("'{kms_key_name}'")),
        );
    }

    if temporary {
        // For temporary tables, set 12-hour expiration
        opts.insert(
            "expiration_timestamp".to_string(),
            Value::from("TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 hour)"),
        );
    } else {
        // Partition filter requirements for non-temporary tables
        if config
            .__warehouse_specific_config__
            .require_partition_filter
            .unwrap_or(false)
            && config.__warehouse_specific_config__.partition_by.is_some()
        {
            opts.insert(
                "require_partition_filter".to_string(),
                Value::from(
                    config
                        .__warehouse_specific_config__
                        .require_partition_filter,
                ),
            );
        }

        if catalog_relation.table_format == "iceberg" {
            opts.insert(
                "table_format".to_string(),
                Value::from(format!("'{}'", catalog_relation.table_format)),
            );
            let file_format = catalog_relation.file_format.ok_or_else(|| {
                AdapterError::new(
                    AdapterErrorKind::Internal,
                    "file_format is not set in catalog",
                )
            })?;
            opts.insert(
                "file_format".to_string(),
                Value::from(format!("'{}'", file_format)),
            );
            let storage_uri = catalog_relation
                .adapter_properties
                .get("storage_uri")
                .ok_or_else(|| {
                    AdapterError::new(
                        AdapterErrorKind::Internal,
                        "storage_uri is not set in catalog",
                    )
                })?;
            opts.insert(
                "storage_uri".to_string(),
                Value::from(format!("'{}'", storage_uri)),
            );
        }
    }

    // Partition expiration if specified
    if let Some(days) = config
        .__warehouse_specific_config__
        .partition_expiration_days
    {
        opts.insert("partition_expiration_days".to_string(), Value::from(days));
    }

    Ok(opts)
}
