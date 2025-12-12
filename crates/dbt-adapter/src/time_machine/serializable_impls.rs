//! TimeMachineSerializable implementations for Object types.

use std::sync::Arc;

use dbt_schemas::dbt_types::RelationType;

use crate::relation::{RelationObject, create_relation};

use super::serde::ReplayContext;
use super::serializable::{JsonExtractor, TimeMachineSerializable};

/// Defensively strip `__type__` field before passing to serde deserializer.
fn strip_type_field(json: &serde_json::Value) -> serde_json::Value {
    if let Some(obj) = json.as_object() {
        serde_json::Value::Object(
            obj.iter()
                .filter(|(k, _)| *k != "__type__")
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        )
    } else {
        json.clone()
    }
}

impl TimeMachineSerializable for dbt_agate::AgateTable {
    const TYPE_ID: &'static str = "AgateTable";

    fn to_time_machine_json(&self) -> serde_json::Value {
        if let Some(ipc_base64) = table_to_ipc_base64(self) {
            serde_json::json!({
                "__format__": "arrow_ipc_base64",
                "__ipc__": ipc_base64
            })
        } else {
            serde_json::json!({
                "__format__": "metadata_only",
                "num_rows": self.num_rows(),
                "num_columns": self.num_columns(),
                "column_names": self.column_names(),
            })
        }
    }

    fn from_time_machine_json(
        json: &serde_json::Value,
        _ctx: &ReplayContext,
    ) -> Option<minijinja::Value> {
        let ext = JsonExtractor::new(json)?;
        if ext.opt_str("__format__")? != "arrow_ipc_base64" {
            return None;
        }
        let table = ipc_base64_to_table(&ext.opt_str("__ipc__")?)?;
        Some(minijinja::Value::from_object(table))
    }
}

fn table_to_ipc_base64(table: &dbt_agate::AgateTable) -> Option<String> {
    use arrow_ipc::CompressionType;
    use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
    use base64::Engine;

    let batch = table.to_record_batch();
    let options = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::LZ4_FRAME))
        .ok()?;

    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new_with_options(&mut buf, &batch.schema(), options).ok()?;
    writer.write(&batch).ok()?;
    writer.finish().ok()?;

    Some(base64::engine::general_purpose::STANDARD.encode(&buf))
}

/// Deserialize an AgateTable from base64-encoded Arrow IPC bytes.
fn ipc_base64_to_table(ipc_base64: &str) -> Option<dbt_agate::AgateTable> {
    use arrow_ipc::reader::StreamReader;
    use base64::Engine;

    let ipc_bytes = base64::engine::general_purpose::STANDARD
        .decode(ipc_base64)
        .ok()?;

    let cursor = std::io::Cursor::new(ipc_bytes);
    let mut reader = StreamReader::try_new(cursor, None).ok()?;

    // Read the single batch
    let batch = reader.next()?.ok()?;
    Some(dbt_agate::AgateTable::from_record_batch(Arc::new(batch)))
}

impl TimeMachineSerializable for crate::response::AdapterResponse {
    const TYPE_ID: &'static str = "AdapterResponse";

    fn to_time_machine_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or_default()
    }

    fn from_time_machine_json(
        json: &serde_json::Value,
        _ctx: &ReplayContext,
    ) -> Option<minijinja::Value> {
        let response: crate::response::AdapterResponse =
            serde_json::from_value(strip_type_field(json)).ok()?;
        Some(minijinja::Value::from_object(response))
    }
}

impl TimeMachineSerializable for RelationObject {
    const TYPE_ID: &'static str = "RelationObject";

    fn to_time_machine_json(&self) -> serde_json::Value {
        let quote_policy = self.quote_policy();
        serde_json::json!({
            "adapter_type": self.adapter_type(),
            "database": self.database().as_str().unwrap_or_default(),
            "schema": self.schema().as_str().unwrap_or_default(),
            "identifier": self.identifier().as_str(),
            "is_table": self.is_table(),
            "is_view": self.is_view(),
            "is_materialized_view": self.is_materialized_view(),
            "is_cte": self.is_cte(),
            "is_dynamic_table": self.is_dynamic_table(),
            "is_streaming_table": self.is_streaming_table(),
            "quote_policy": {
                "database": quote_policy.database,
                "schema": quote_policy.schema,
                "identifier": quote_policy.identifier,
            },
        })
    }

    fn from_time_machine_json(
        json: &serde_json::Value,
        ctx: &ReplayContext,
    ) -> Option<minijinja::Value> {
        use dbt_common::adapter::AdapterType;
        use dbt_schemas::schemas::common::ResolvedQuoting;
        use dbt_schemas::schemas::relations::{
            DEFAULT_RESOLVED_QUOTING, SNOWFLAKE_RESOLVED_QUOTING,
        };

        let ext = JsonExtractor::new(json)?;

        // Get adapter type from serialized data, falling back to context
        let adapter_type = ext
            .opt_str("adapter_type")
            .and_then(|s| s.parse::<AdapterType>().ok())
            .unwrap_or(ctx.adapter_type);

        // Get quote_policy from serialized data, falling back to adapter-specific defaults.
        let default_quoting = match adapter_type {
            AdapterType::Snowflake => SNOWFLAKE_RESOLVED_QUOTING,
            _ => DEFAULT_RESOLVED_QUOTING,
        };

        let quote_policy = ext
            .opt_object("quote_policy")
            .map(|qp| ResolvedQuoting {
                database: qp
                    .get("database")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(default_quoting.database),
                schema: qp
                    .get("schema")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(default_quoting.schema),
                identifier: qp
                    .get("identifier")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(default_quoting.identifier),
            })
            .unwrap_or(default_quoting);

        let relation_type = if ext.bool_or("is_view", false) {
            Some(RelationType::View)
        } else if ext.bool_or("is_table", false) {
            Some(RelationType::Table)
        } else if ext.bool_or("is_materialized_view", false) {
            Some(RelationType::MaterializedView)
        } else if ext.bool_or("is_cte", false) {
            Some(RelationType::CTE)
        } else if ext.bool_or("is_dynamic_table", false) {
            Some(RelationType::DynamicTable)
        } else if ext.bool_or("is_streaming_table", false) {
            Some(RelationType::StreamingTable)
        } else {
            None
        };

        let relation = create_relation(
            adapter_type,
            ext.str_or("database", ""),
            ext.str_or("schema", ""),
            ext.opt_str("identifier"),
            relation_type,
            quote_policy,
        )
        .ok()?;

        Some(RelationObject::new(relation).into_value())
    }
}

impl TimeMachineSerializable for crate::catalog_relation::CatalogRelation {
    const TYPE_ID: &'static str = "CatalogRelation";

    fn to_time_machine_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or_default()
    }

    fn from_time_machine_json(
        json: &serde_json::Value,
        ctx: &ReplayContext,
    ) -> Option<minijinja::Value> {
        let ext = JsonExtractor::new(json)?;

        let adapter_type = ext
            .opt_str("adapter_type")
            .and_then(|s| s.parse().ok())
            .unwrap_or(ctx.adapter_type);

        let adapter_properties = ext
            .opt_object("adapter_properties")
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        let catalog = crate::catalog_relation::CatalogRelation {
            adapter_type,
            catalog_name: ext.opt_str("catalog_name"),
            integration_name: ext.opt_str("integration_name"),
            catalog_type: ext.str_or("catalog_type", ""),
            table_format: ext.str_or("table_format", ""),
            adapter_properties,
            is_transient: ext.opt_bool("is_transient"),
            external_volume: ext.opt_str("external_volume"),
            base_location: ext.opt_str("base_location"),
            file_format: ext.opt_str("file_format"),
        };

        Some(minijinja::Value::from_object(catalog))
    }
}

impl TimeMachineSerializable for crate::column::Column {
    const TYPE_ID: &'static str = "Column";

    fn to_time_machine_json(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name(),
            "dtype": self.dtype(),
            "data_type": self.data_type(),
            "char_size": self.char_size(),
            "numeric_precision": self.numeric_precision(),
            "numeric_scale": self.numeric_scale(),
        })
    }

    fn from_time_machine_json(
        json: &serde_json::Value,
        ctx: &ReplayContext,
    ) -> Option<minijinja::Value> {
        let ext = JsonExtractor::new(json)?;
        let column = crate::column::Column::new(
            ctx.adapter_type,
            ext.opt_str("name")?,
            ext.str_or("dtype", ""),
            ext.opt_u32("char_size"),
            ext.opt_u64("numeric_precision"),
            ext.opt_u64("numeric_scale"),
        );
        Some(minijinja::Value::from_object(column))
    }
}

#[cfg(test)]
mod tests {
    use crate::relation::RelationObject;

    use super::*;
    use dbt_common::adapter::AdapterType;
    use dbt_schemas::schemas::common::ResolvedQuoting;

    fn ctx() -> ReplayContext {
        ReplayContext {
            adapter_type: AdapterType::Snowflake,
            quoting: ResolvedQuoting::default(),
        }
    }

    #[test]
    fn test_type_ids_are_stable() {
        assert_eq!(dbt_agate::AgateTable::TYPE_ID, "AgateTable");
        assert_eq!(crate::response::AdapterResponse::TYPE_ID, "AdapterResponse");
        assert_eq!(RelationObject::TYPE_ID, "RelationObject");
        assert_eq!(
            crate::catalog_relation::CatalogRelation::TYPE_ID,
            "CatalogRelation"
        );
        assert_eq!(crate::column::Column::TYPE_ID, "Column");
    }

    #[test]
    fn test_adapter_response_roundtrip() {
        let original = crate::response::AdapterResponse {
            message: "SUCCESS 42".to_string(),
            code: "SUCCESS".to_string(),
            rows_affected: 42,
            query_id: Some("query-123".to_string()),
        };

        let json = original.to_time_machine_json();
        assert_eq!(json["message"], "SUCCESS 42");
        assert_eq!(json["rows_affected"], 42);

        let value =
            crate::response::AdapterResponse::from_time_machine_json(&json, &ctx()).unwrap();
        let response = value
            .downcast_object::<crate::response::AdapterResponse>()
            .unwrap();
        assert_eq!(response.message, original.message);
        assert_eq!(response.rows_affected, original.rows_affected);
    }

    #[test]
    fn test_catalog_relation_roundtrip() {
        use std::collections::BTreeMap;

        let original = crate::catalog_relation::CatalogRelation {
            adapter_type: AdapterType::Snowflake,
            catalog_name: Some("my_catalog".to_string()),
            integration_name: Some("my_integration".to_string()),
            catalog_type: "BUILT_IN".to_string(),
            table_format: "ICEBERG".to_string(),
            adapter_properties: BTreeMap::from([("key1".to_string(), "value1".to_string())]),
            is_transient: Some(false),
            external_volume: Some("my_volume".to_string()),
            base_location: Some("/path/to/data".to_string()),
            file_format: None,
        };

        let json = original.to_time_machine_json();
        assert_eq!(json["catalog_name"], "my_catalog");
        assert_eq!(json["table_format"], "ICEBERG");

        let value = crate::catalog_relation::CatalogRelation::from_time_machine_json(&json, &ctx())
            .unwrap();
        let catalog = value
            .downcast_object::<crate::catalog_relation::CatalogRelation>()
            .unwrap();
        assert_eq!(catalog.catalog_name, original.catalog_name);
        assert_eq!(catalog.table_format, original.table_format);
    }

    #[test]
    fn test_column_roundtrip() {
        let original = crate::column::Column::new(
            AdapterType::Snowflake,
            "my_column".to_string(),
            "VARCHAR".to_string(),
            Some(255),
            None,
            None,
        );

        let json = original.to_time_machine_json();
        assert_eq!(json["name"], "my_column");
        assert_eq!(json["dtype"], "VARCHAR");

        let value = crate::column::Column::from_time_machine_json(&json, &ctx()).unwrap();
        let column = value.downcast_object::<crate::column::Column>().unwrap();
        assert_eq!(column.name(), original.name());
        assert_eq!(column.dtype(), original.dtype());
    }

    #[test]
    fn test_relation_object_roundtrip_with_quoting() {
        use dbt_schemas::dbt_types::RelationType;

        // Create a relation with custom quoting
        let custom_quoting = ResolvedQuoting {
            database: false,
            schema: false,
            identifier: true,
        };

        let relation = create_relation(
            AdapterType::Snowflake,
            "MY_DB".to_string(),
            "MY_SCHEMA".to_string(),
            Some("my_table".to_string()),
            Some(RelationType::Table),
            custom_quoting,
        )
        .unwrap();

        let original = RelationObject::new(relation);

        let json = original.to_time_machine_json();

        // Verify quoting is serialized
        assert_eq!(json["quote_policy"]["database"], false);
        assert_eq!(json["quote_policy"]["schema"], false);
        assert_eq!(json["quote_policy"]["identifier"], true);
        assert_eq!(json["adapter_type"], "snowflake");

        // Deserialize with a DIFFERENT context quoting - should use serialized quoting
        let different_ctx = ReplayContext {
            adapter_type: AdapterType::Snowflake,
            quoting: ResolvedQuoting {
                database: true, // Different quoting
                schema: true,
                identifier: false,
            },
        };

        let value = RelationObject::from_time_machine_json(&json, &different_ctx).unwrap();
        let restored = value.downcast_object::<RelationObject>().unwrap();

        // Verify the restored relation uses the serialized quoting
        assert!(!restored.quote_policy().database);
        assert!(!restored.quote_policy().schema);
        assert!(restored.quote_policy().identifier);

        // Verify adapter type is also restored from serialized data
        assert_eq!(restored.adapter_type(), Some("snowflake".to_string()));
    }

    #[test]
    fn test_relation_object_backward_compat_postgres_defaults() {
        // Same test but for Postgres, which has different default quoting (all true)
        let old_format_json = serde_json::json!({
            "database": "mydb",
            "schema": "public",
            "identifier": "users",
            "is_table": true,
        });

        let ctx = ReplayContext {
            adapter_type: AdapterType::Postgres,
            quoting: ResolvedQuoting {
                database: false, // Context has different quoting
                schema: false,
                identifier: false,
            },
        };

        let value = RelationObject::from_time_machine_json(&old_format_json, &ctx).unwrap();
        let restored = value.downcast_object::<RelationObject>().unwrap();

        // Should use Postgres defaults (all true), NOT context quoting (all false)
        assert!(restored.quote_policy().database);
        assert!(restored.quote_policy().schema);
        assert!(restored.quote_policy().identifier);
    }
}
