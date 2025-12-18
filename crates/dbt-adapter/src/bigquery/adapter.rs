use crate::adapter_engine::AdapterEngine;
use crate::base_adapter::{AdapterType, AdapterTyping};
use crate::catalog_relation::CatalogRelation;
use crate::errors::{AdapterError, AdapterErrorKind, AdapterResult, adbc_error_to_adapter_error};
use crate::funcs::none_value;
use crate::load_catalogs;
use crate::metadata::*;
use crate::typed_adapter::TypedBaseAdapter;
use arrow_schema::Schema;
use dbt_common::serde_utils::convert_yml_to_value_map;
use dbt_schemas::schemas::manifest::PartitionConfig;
use dbt_schemas::schemas::project::ModelConfig;
use dbt_schemas::schemas::relations::base::BaseRelation;
use dbt_schemas::schemas::serde::minijinja_value_to_typed_struct;
use dbt_schemas::schemas::{CommonAttributes, InternalDbtNodeWrapper};
use dbt_xdbc::Connection;
use indexmap::IndexMap;
use minijinja::value::mutable_vec::MutableVec;
use minijinja::{Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, State, Value};

use std::collections::BTreeMap;
use std::fmt::{self, Debug};
use std::sync::Arc;

pub const ADBC_EXECUTE_INVOCATION_OPTION: &str = "dbt_invocation_id";

/// Shared, pure parser for BigQuery `partition_by` config that does not require DB access.
pub(crate) fn parse_partition_by_value(raw_partition_by: Value) -> AdapterResult<Value> {
    if raw_partition_by.is_none() {
        return Ok(none_value());
    }

    let partition_by = minijinja_value_to_typed_struct::<PartitionConfig>(raw_partition_by.clone())
        .map_err(|e| {
            MinijinjaError::new(
                MinijinjaErrorKind::SerdeDeserializeError,
                format!("adapter.parse_partition_by failed on {raw_partition_by:?}: {e}"),
            )
        })?;

    let validated_config = partition_by.into_bigquery().ok_or_else(|| {
        MinijinjaError::new(
            MinijinjaErrorKind::InvalidArgument,
            "Expect a BigqueryPartitionConfigStruct",
        )
    })?;

    Ok(Value::from_object(validated_config))
}

/// Shared, pure helper to compute common table options for BigQuery without DB access.
pub(crate) fn get_common_table_options_value(
    state: &State,
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
    state: &State,
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

/// An adapter for interacting with Bigquery.
#[derive(Clone)]
pub struct BigqueryAdapter {
    engine: Arc<AdapterEngine>,
}

impl Debug for BigqueryAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.adapter_type())
    }
}

impl BigqueryAdapter {
    pub fn new(engine: Arc<AdapterEngine>) -> Self {
        Self { engine }
    }
}

impl AdapterTyping for BigqueryAdapter {
    fn metadata_adapter(&self) -> Option<Box<dyn MetadataAdapter>> {
        Some(Box::new(self.clone()))
    }

    fn as_typed_base_adapter(&self) -> &dyn TypedBaseAdapter {
        self
    }

    fn engine(&self) -> &Arc<AdapterEngine> {
        &self.engine
    }
}

impl TypedBaseAdapter for BigqueryAdapter {}

pub fn get_table_schema(
    conn: &'_ mut dyn Connection,
    relation: Arc<dyn BaseRelation>,
) -> AdapterResult<Schema> {
    conn.get_table_schema(
        Some(&relation.database_as_str()?),
        Some(&relation.schema_as_str()?),
        &relation.identifier_as_str()?,
    )
    .map_err(adbc_error_to_adapter_error)
}

/// Represent nested data types (struct/array) for BigQuery
/// Leaf nodes are primitive types
/// For example column names "a.b", "a.c", "a.c.d" will be
///  a (struct)
///  /\
/// b  c (struct)
///     \
///      d
#[derive(Debug, Default)]
pub(crate) struct NestedColumnDataTypes {
    root: TrieNode,
}

#[derive(Debug, Default)]
struct TrieNode {
    pub children: IndexMap<String, TrieNode>,
    pub data_type: Option<String>,
}

impl NestedColumnDataTypes {
    pub fn insert(&mut self, column_name: &str, column_type: Option<&String>) {
        let names = column_name.split(".");
        let mut node = &mut self.root;
        for name in names {
            node = node.children.entry(name.to_owned()).or_default();
        }
        node.data_type = column_type.map(String::from);
    }

    pub fn format_top_level_columns_data_types(&self) -> IndexMap<String, String> {
        let mut result = IndexMap::new();
        for (column_name, node) in &self.root.children {
            let data_type = match &node.data_type {
                None => {
                    let inner_data_type = node.format_data_type();
                    format!("struct<{inner_data_type}>")
                }
                Some(data_type) => match data_type.as_str() {
                    "struct" => {
                        let inner_data_type = node.format_data_type();
                        format!("struct<{inner_data_type}>")
                    }
                    "array" => {
                        let inner_data_type = node.format_data_type();
                        format!("array<struct<{inner_data_type}>>")
                    }
                    // assume any struct or array type is a primitive type
                    _ => {
                        // ensure no sub fields
                        if node.children.is_empty() {
                            data_type.to_owned()
                        }
                        // sub fields exist -> it's actually not a primitive type -> default to struct
                        // this is to be consistent with dbt compile behavior
                        else {
                            let inner_data_type = node.format_data_type();
                            format!("struct<{inner_data_type}>")
                        }
                    }
                },
            };
            result.insert(column_name.to_owned(), data_type);
        }
        result
    }
}

impl TrieNode {
    // TODO: refactor since this method is very much overlapped with `format_top_level_columns_data_types`
    fn format_data_type(&self) -> String {
        let mut result = vec![];
        for (column_name, node) in &self.children {
            let data_type = match &node.data_type {
                None => {
                    let inner_data_type = node.format_data_type();
                    if inner_data_type.is_empty() {
                        column_name.to_owned()
                    } else {
                        format!("{column_name} struct<{inner_data_type}>")
                    }
                }
                Some(data_type) => match data_type.as_str() {
                    "struct" => {
                        let inner_data_type = node.format_data_type();
                        format!("{column_name} struct<{inner_data_type}>")
                    }
                    "array" => {
                        let inner_data_type = node.format_data_type();
                        format!("{column_name} array<struct<{inner_data_type}>>")
                    }
                    _ => {
                        if node.children.is_empty() {
                            format!("{column_name} {data_type}")
                        } else {
                            let inner_data_type = node.format_data_type();
                            format!("{column_name} struct<{inner_data_type}>")
                        }
                    }
                },
            };
            result.push(data_type);
        }
        result.join(", ")
    }
}

#[cfg(test)] // win ADBC
mod tests {
    use super::*;
    use crate::stmt_splitter::NaiveStmtSplitter;

    use crate::config::AdapterConfig;
    use crate::query_comment::QueryCommentConfig;
    use crate::sql_types::NaiveTypeOpsImpl;
    use dbt_auth::auth_for_backend;
    use dbt_common::cancellation::never_cancels;
    use dbt_schemas::schemas::relations::DEFAULT_RESOLVED_QUOTING;
    use dbt_serde_yaml::Mapping;
    use dbt_xdbc::Backend;

    fn engine() -> Arc<AdapterEngine> {
        let config = Mapping::default();
        let auth = auth_for_backend(Backend::BigQuery);
        AdapterEngine::new(
            AdapterType::Bigquery,
            auth.into(),
            AdapterConfig::new(config),
            DEFAULT_RESOLVED_QUOTING,
            Arc::new(NaiveStmtSplitter), // XXX: may cause bugs if these tests run SQL
            None,
            QueryCommentConfig::from_query_comment(None, AdapterType::Bigquery, false),
            Box::new(NaiveTypeOpsImpl::new(AdapterType::Postgres)),
            never_cancels(),
        )
    }

    #[test]
    fn test_quote() {
        let adapter = BigqueryAdapter::new(engine());
        assert_eq!(adapter.quote("abc"), "`abc`");
    }

    #[test]
    fn test_format_top_level_columns_data_types() {
        // Test case 1: Simple primitive types
        {
            let mut nested = NestedColumnDataTypes::default();
            nested.insert("id", Some(&"integer".to_string()));
            nested.insert("name", Some(&"string".to_string()));

            let result = nested.format_top_level_columns_data_types();
            assert_eq!(result.get("id").unwrap(), "integer");
            assert_eq!(result.get("name").unwrap(), "string");
        }

        // Test case 2: Nested struct
        {
            let mut nested = NestedColumnDataTypes::default();
            nested.insert("user.id", Some(&"integer".to_string()));
            nested.insert("user.name", Some(&"string".to_string()));

            let result = nested.format_top_level_columns_data_types();
            assert_eq!(
                result.get("user").unwrap(),
                "struct<id integer, name string>"
            );
        }

        // Test case 3: Array of structs
        {
            let mut nested = NestedColumnDataTypes::default();
            nested.insert("addresses", Some(&"array".to_string()));
            nested.insert("addresses.street", Some(&"string".to_string()));
            nested.insert("addresses.city", Some(&"string".to_string()));

            let result = nested.format_top_level_columns_data_types();
            assert_eq!(
                result.get("addresses").unwrap(),
                "array<struct<street string, city string>>"
            );
        }

        // Test case 4: Mixed types with deep nesting
        {
            let mut nested = NestedColumnDataTypes::default();
            nested.insert("id", Some(&"integer".to_string()));
            nested.insert("user.name", Some(&"string".to_string()));
            nested.insert("user.contact.email", Some(&"string".to_string()));
            nested.insert("user.contact.phone", Some(&"string".to_string()));

            let result = nested.format_top_level_columns_data_types();
            assert_eq!(result.get("id").unwrap(), "integer");
            assert_eq!(
                result.get("user").unwrap(),
                "struct<name string, contact struct<email string, phone string>>"
            );
        }

        // Test case 5: Empty struct (no data type)
        {
            let mut nested = NestedColumnDataTypes::default();
            nested.insert("empty_struct", None);
            nested.insert("empty_struct.field1", Some(&"string".to_string()));

            let result = nested.format_top_level_columns_data_types();
            assert_eq!(result.get("empty_struct").unwrap(), "struct<field1 string>");
        }

        // Test case 6: Struct marked as primitive but has children
        {
            let mut nested = NestedColumnDataTypes::default();
            nested.insert("metadata", Some(&"json".to_string()));
            nested.insert("metadata.key1", Some(&"string".to_string()));
            nested.insert("metadata.key2", Some(&"integer".to_string()));

            let result = nested.format_top_level_columns_data_types();
            assert_eq!(
                result.get("metadata").unwrap(),
                "struct<key1 string, key2 integer>"
            );
        }
    }
}
