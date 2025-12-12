//! Recording and replay helpers for MetadataAdapter async methods.
//!
//! This module provides utilities for transparently recording and replaying
//! MetadataAdapter method calls using the global time machine recorder/replayer.
//!
//! # Recording
//!
//! When recording is active (`global_recorder()` returns Some), method calls
//! are captured with their arguments and results.
//!
//! # Replay
//!
//! When replay is active (`global_replayer()` returns Some), recorded results
//! are returned instead of executing the actual implementation.
//!
//! # Usage
//!
//! Use `with_metadata_recording` to wrap any async MetadataAdapter method:
//!
//! ```ignore
//! fn list_relations_schemas(
//!     &self,
//!     unique_id: Option<String>,
//!     phase: Option<ExecutionPhase>,
//!     relations: &[Arc<dyn BaseRelation>],
//! ) -> AsyncAdapterResult<'_, HashMap<String, AdapterResult<Arc<Schema>>>> {
//!     with_metadata_recording(
//!         unique_id.clone().unwrap_or_else(|| "global".to_string()),
//!         "list_relations_schemas",
//!         args_list_relations_schemas(
//!             unique_id.clone(),
//!             phase.map(|p| p.to_string()),
//!             relations.iter().map(|r| r.semantic_fqn()),
//!         ),
//!         self.list_relations_schemas_impl(unique_id, phase, relations),
//!     )
//! }
//! ```

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use arrow_schema::{Field, Schema};
use chrono::{DateTime, Utc};
use dbt_common::cancellation::Cancellable;

use crate::errors::{AdapterError, AdapterErrorKind, AdapterResult};
use crate::metadata::{CatalogAndSchema, MetadataFreshness, RelationSchemaPair, RelationVec, UDF};

use super::event::{CatalogSchema, CatalogSchemas, MetadataCallArgs};
use super::{global_recorder, global_replayer};

// -----------------------------------------------------------------------------
// Recording/Replay wrapper
// -----------------------------------------------------------------------------

/// Wrap an async MetadataAdapter call with automatic recording and replay.
///
/// This function handles both recording and replay modes:
///
/// **Replay mode** (when `global_replayer()` returns Some):
/// 1. Looks up the recorded result for this call
/// 2. Deserializes and returns it without executing the actual implementation
///
/// **Recording mode** (when `global_recorder()` returns Some):
/// 1. Executes the provided future
/// 2. Records the call with its result
/// 3. Returns the result
///
/// **Normal mode** (neither recording nor replay):
/// 1. Executes the provided future and returns the result
///
/// # Type Parameters
///
/// - `T`: The success type that must implement both serialization and deserialization
/// - `F`: The future type
///
/// # Arguments
///
/// - `caller_id`: Identifier for the caller (usually unique_id or "global")
/// - `method`: Name of the MetadataAdapter method being called
/// - `args`: Structured arguments for recording
/// - `fut`: The actual async operation to execute
pub fn with_time_machine_metadata_wrapper<'a, T, F>(
    caller_id: impl Into<String> + Send + 'a,
    method: &'static str,
    args: MetadataCallArgs,
    fut: F,
) -> Pin<Box<dyn Future<Output = Result<T, Cancellable<AdapterError>>> + Send + 'a>>
where
    T: MetadataResultSerialize + MetadataResultDeserialize + Send + 'a,
    F: Future<Output = Result<T, Cancellable<AdapterError>>> + Send + 'a,
{
    let caller_id = caller_id.into();

    Box::pin(async move {
        // Check for replay mode first
        if let Some(replayer) = global_replayer() {
            // In replay mode - look up the recorded result
            match replayer.get_metadata_result(&caller_id, method, &args) {
                Some(Ok(json)) => {
                    // Found a recorded result - deserialize and return it
                    return T::from_recording_json(&json).map_err(|e| {
                        Cancellable::Error(AdapterError::new(
                            AdapterErrorKind::Internal,
                            format!("Failed to deserialize replay result: {}", e),
                        ))
                    });
                }
                Some(Err(e)) => {
                    // Recorded call failed - return the recorded error
                    return Err(Cancellable::Error(AdapterError::new(
                        AdapterErrorKind::Internal,
                        format!("Replayed call failed: {}", e),
                    )));
                }
                None => {
                    // No matching recorded event found in replay mode.
                    // This is an error - we should not execute real network calls during replay.
                    return Err(Cancellable::Error(AdapterError::new(
                        AdapterErrorKind::Internal,
                        format!(
                            "No recorded metadata event for method '{}' with caller '{}'. \
                             Recording may be incomplete.",
                            method, caller_id
                        ),
                    )));
                }
            }
        }

        // Not in replay mode - execute the actual implementation
        let start = Instant::now();
        let result = fut.await;
        let duration_ms = start.elapsed().as_millis() as u64;

        // Record if recording is active (with backpressure support)
        if let Some(recorder) = global_recorder() {
            let (result_json, success, error) = match &result {
                Ok(value) => (value.to_recording_json(), true, None),
                Err(Cancellable::Error(e)) => (serde_json::Value::Null, false, Some(e.to_string())),
                Err(Cancellable::Cancelled) => (
                    serde_json::Value::Null,
                    false,
                    Some("Cancelled".to_string()),
                ),
            };

            recorder
                .record_metadata_call(
                    caller_id,
                    method,
                    args,
                    result_json,
                    success,
                    error,
                    duration_ms,
                )
                .await;
        }

        result
    })
}

// -----------------------------------------------------------------------------
// Result serialization traits
// -----------------------------------------------------------------------------

/// Trait for serializing MetadataAdapter result types to JSON for recording.
pub trait MetadataResultSerialize {
    /// Serialize this result to JSON for recording.
    fn to_recording_json(&self) -> serde_json::Value;
}

/// Trait for deserializing MetadataAdapter result types from JSON for replay.
pub trait MetadataResultDeserialize: Sized {
    /// Deserialize this result from JSON for replay.
    fn from_recording_json(json: &serde_json::Value) -> Result<Self, String>;
}

// Implementations for common MetadataAdapter result types

impl MetadataResultSerialize for HashMap<String, AdapterResult<Arc<Schema>>> {
    fn to_recording_json(&self) -> serde_json::Value {
        let entries: serde_json::Map<String, serde_json::Value> = self
            .iter()
            .map(|(k, v)| {
                let value = match v {
                    Ok(schema) => {
                        // Serialize Arrow fields using serde
                        let fields: Vec<serde_json::Value> = schema
                            .fields()
                            .iter()
                            .filter_map(|f| serde_json::to_value(f.as_ref()).ok())
                            .collect();
                        serde_json::json!({ "ok": fields })
                    }
                    Err(e) => serde_json::json!({ "error": e.to_string() }),
                };
                (k.clone(), value)
            })
            .collect();
        serde_json::Value::Object(entries)
    }
}

impl MetadataResultDeserialize for HashMap<String, AdapterResult<Arc<Schema>>> {
    fn from_recording_json(json: &serde_json::Value) -> Result<Self, String> {
        let obj = json.as_object().ok_or("Expected object")?;
        let mut result = HashMap::new();

        for (key, value) in obj {
            let schema_result = if let Some(fields) = value.get("ok") {
                let fields_arr = fields.as_array().ok_or("Expected fields array")?;
                let mut arrow_fields = Vec::new();

                for field_json in fields_arr {
                    // Deserialize Arrow Field using serde
                    let field: Field = serde_json::from_value(field_json.clone())
                        .map_err(|e| format!("Failed to deserialize field: {}", e))?;
                    arrow_fields.push(field);
                }

                Ok(Arc::new(Schema::new(arrow_fields)))
            } else if let Some(error) = value.get("error") {
                let error_msg = error.as_str().unwrap_or("Unknown error");
                Err(AdapterError::new(AdapterErrorKind::Internal, error_msg))
            } else {
                return Err("Invalid schema entry".to_string());
            };

            result.insert(key.clone(), schema_result);
        }

        Ok(result)
    }
}

impl MetadataResultSerialize for BTreeMap<String, MetadataFreshness> {
    fn to_recording_json(&self) -> serde_json::Value {
        let entries: serde_json::Map<String, serde_json::Value> = self
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    serde_json::json!({
                        "last_altered": v.last_altered.to_rfc3339(),
                        "is_view": v.is_view,
                    }),
                )
            })
            .collect();
        serde_json::Value::Object(entries)
    }
}

impl MetadataResultDeserialize for BTreeMap<String, MetadataFreshness> {
    fn from_recording_json(json: &serde_json::Value) -> Result<Self, String> {
        let obj = json.as_object().ok_or("Expected object")?;
        let mut result = BTreeMap::new();

        for (key, value) in obj {
            let last_altered_str = value
                .get("last_altered")
                .and_then(|v| v.as_str())
                .ok_or("Missing last_altered")?;
            let last_altered: DateTime<Utc> = last_altered_str
                .parse()
                .map_err(|e| format!("Invalid timestamp: {}", e))?;
            let is_view = value
                .get("is_view")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            result.insert(
                key.clone(),
                MetadataFreshness {
                    last_altered,
                    is_view,
                },
            );
        }

        Ok(result)
    }
}

impl MetadataResultSerialize for BTreeMap<CatalogAndSchema, AdapterResult<RelationVec>> {
    fn to_recording_json(&self) -> serde_json::Value {
        let entries: Vec<serde_json::Value> = self
            .iter()
            .map(|(db_schema, result)| {
                let value = match result {
                    Ok(relations) => {
                        let rel_names: Vec<String> =
                            relations.iter().map(|r| r.semantic_fqn()).collect();
                        serde_json::json!({ "ok": rel_names })
                    }
                    Err(e) => serde_json::json!({ "error": e.to_string() }),
                };
                serde_json::json!({
                    "resolved_catalog": db_schema.resolved_catalog,
                    "resolved_schema": db_schema.resolved_schema,
                    "rendered_catalog": db_schema.rendered_catalog,
                    "rendered_schema": db_schema.rendered_schema,
                    "result": value,
                })
            })
            .collect();
        serde_json::Value::Array(entries)
    }
}

impl MetadataResultDeserialize for BTreeMap<CatalogAndSchema, AdapterResult<RelationVec>> {
    fn from_recording_json(json: &serde_json::Value) -> Result<Self, String> {
        let arr = json.as_array().ok_or("Expected array")?;
        let mut result = BTreeMap::new();

        for entry in arr {
            let resolved_catalog = entry
                .get("resolved_catalog")
                .and_then(|v| v.as_str())
                .ok_or("Missing resolved_catalog")?
                .to_string();
            let resolved_schema = entry
                .get("resolved_schema")
                .and_then(|v| v.as_str())
                .ok_or("Missing resolved_schema")?
                .to_string();
            let rendered_catalog = entry
                .get("rendered_catalog")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let rendered_schema = entry
                .get("rendered_schema")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let db_schema = CatalogAndSchema {
                resolved_catalog,
                resolved_schema,
                rendered_catalog,
                rendered_schema,
            };

            let relations_result = if let Some(res) = entry.get("result") {
                if res.get("ok").is_some() {
                    // For replay, return empty relations - the actual relation objects
                    // cannot be reconstructed from JSON without adapter context
                    Ok(Vec::new())
                } else if let Some(error) = res.get("error") {
                    let error_msg = error.as_str().unwrap_or("Unknown error");
                    Err(AdapterError::new(AdapterErrorKind::Internal, error_msg))
                } else {
                    return Err("Invalid result entry".to_string());
                }
            } else {
                return Err("Missing result field".to_string());
            };

            result.insert(db_schema, relations_result);
        }

        Ok(result)
    }
}

impl MetadataResultSerialize for Vec<(String, AdapterResult<RelationSchemaPair>)> {
    fn to_recording_json(&self) -> serde_json::Value {
        let entries: Vec<serde_json::Value> = self
            .iter()
            .map(|(name, result)| {
                let value = match result {
                    Ok((relation, schema)) => {
                        let fields: Vec<serde_json::Value> = schema
                            .fields()
                            .iter()
                            .map(|f| {
                                serde_json::json!({
                                    "name": f.name(),
                                    "type": format!("{:?}", f.data_type()),
                                    "nullable": f.is_nullable(),
                                })
                            })
                            .collect();
                        serde_json::json!({
                            "ok": {
                                "relation": relation.semantic_fqn(),
                                "schema": fields,
                            }
                        })
                    }
                    Err(e) => serde_json::json!({ "error": e.to_string() }),
                };
                serde_json::json!({
                    "name": name,
                    "result": value,
                })
            })
            .collect();
        serde_json::Value::Array(entries)
    }
}

impl MetadataResultDeserialize for Vec<(String, AdapterResult<RelationSchemaPair>)> {
    fn from_recording_json(_json: &serde_json::Value) -> Result<Self, String> {
        // RelationSchemaPair contains a BaseRelation which cannot be reconstructed
        // without adapter context. For replay, this method should not be called
        // as list_relations_schemas_by_patterns is not commonly used.
        Err("Replay of list_relations_schemas_by_patterns is not supported".to_string())
    }
}

impl MetadataResultSerialize for Vec<UDF> {
    fn to_recording_json(&self) -> serde_json::Value {
        let udfs: Vec<serde_json::Value> = self
            .iter()
            .map(|udf| {
                serde_json::json!({
                    "name": udf.name,
                    "signature": udf.signature,
                    "kind": format!("{:?}", udf.kind),
                    "description": udf.description,
                    "adapter_type": format!("{:?}", udf.adapter_type),
                })
            })
            .collect();
        serde_json::Value::Array(udfs)
    }
}

impl MetadataResultDeserialize for Vec<UDF> {
    fn from_recording_json(json: &serde_json::Value) -> Result<Self, String> {
        use crate::metadata::UDFKind;
        use dbt_common::adapter::AdapterType;

        let arr = json.as_array().ok_or("Expected array")?;
        let mut result = Vec::new();

        for udf_json in arr {
            let name = udf_json
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or("Missing UDF name")?
                .to_string();
            let signature = udf_json
                .get("signature")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let description = udf_json
                .get("description")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let kind_str = udf_json
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or("Scalar");
            let adapter_type_str = udf_json
                .get("adapter_type")
                .and_then(|v| v.as_str())
                .unwrap_or("Snowflake");

            let kind = match kind_str {
                "Aggregate" => UDFKind::Aggregate,
                "Table" => UDFKind::Table,
                _ => UDFKind::Scalar,
            };

            let adapter_type = adapter_type_str.parse().unwrap_or(AdapterType::Snowflake);

            result.push(UDF {
                name,
                description,
                signature,
                adapter_type,
                kind,
            });
        }

        Ok(result)
    }
}

impl MetadataResultSerialize for Vec<(String, String, AdapterResult<()>)> {
    fn to_recording_json(&self) -> serde_json::Value {
        let entries: Vec<serde_json::Value> = self
            .iter()
            .map(|(catalog, schema, result)| {
                let status = match result {
                    Ok(()) => serde_json::json!({ "ok": true }),
                    Err(e) => serde_json::json!({ "error": e.to_string() }),
                };
                serde_json::json!({
                    "catalog": catalog,
                    "schema": schema,
                    "result": status,
                })
            })
            .collect();
        serde_json::Value::Array(entries)
    }
}

impl MetadataResultDeserialize for Vec<(String, String, AdapterResult<()>)> {
    fn from_recording_json(json: &serde_json::Value) -> Result<Self, String> {
        let arr = json.as_array().ok_or("Expected array")?;
        let mut result = Vec::new();

        for entry in arr {
            let catalog = entry
                .get("catalog")
                .and_then(|v| v.as_str())
                .ok_or("Missing catalog")?
                .to_string();
            let schema = entry
                .get("schema")
                .and_then(|v| v.as_str())
                .ok_or("Missing schema")?
                .to_string();

            let op_result = if let Some(res) = entry.get("result") {
                if res.get("ok").is_some() {
                    Ok(())
                } else if let Some(error) = res.get("error") {
                    let error_msg = error.as_str().unwrap_or("Unknown error");
                    Err(AdapterError::new(AdapterErrorKind::Internal, error_msg))
                } else {
                    return Err("Invalid result entry".to_string());
                }
            } else {
                return Err("Missing result field".to_string());
            };

            result.push((catalog, schema, op_result));
        }

        Ok(result)
    }
}

// -----------------------------------------------------------------------------
// Argument builder helpers
// -----------------------------------------------------------------------------

/// Create MetadataCallArgs for list_relations_schemas.
pub fn args_list_relations_schemas(
    unique_id: Option<String>,
    phase: Option<String>,
    relations: impl IntoIterator<Item = impl AsRef<str>>,
) -> MetadataCallArgs {
    MetadataCallArgs::ListRelationsSchemas {
        unique_id,
        phase,
        relations: relations
            .into_iter()
            .map(|r| r.as_ref().to_string())
            .collect(),
    }
}

/// Create MetadataCallArgs for list_relations_in_parallel.
pub fn args_list_relations_in_parallel(
    db_schemas: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
) -> MetadataCallArgs {
    MetadataCallArgs::ListRelationsInParallel {
        db_schemas: db_schemas
            .into_iter()
            .map(|(c, s)| CatalogSchema {
                catalog: c.into(),
                schema: s.into(),
            })
            .collect(),
    }
}

/// Create MetadataCallArgs for freshness.
pub fn args_freshness(relations: impl IntoIterator<Item = impl AsRef<str>>) -> MetadataCallArgs {
    MetadataCallArgs::Freshness {
        relations: relations
            .into_iter()
            .map(|r| r.as_ref().to_string())
            .collect(),
    }
}

/// Create MetadataCallArgs for list_user_defined_functions.
pub fn args_list_udfs(catalog_schemas: &BTreeMap<String, BTreeSet<String>>) -> MetadataCallArgs {
    MetadataCallArgs::ListUserDefinedFunctions {
        catalog_schemas: catalog_schemas
            .iter()
            .map(|(c, schemas)| CatalogSchemas {
                catalog: c.clone(),
                schemas: schemas.iter().cloned().collect(),
            })
            .collect(),
    }
}

/// Create MetadataCallArgs for list_relations_schemas_by_patterns.
pub fn args_list_relations_schemas_by_patterns(
    patterns: impl IntoIterator<Item = impl AsRef<str>>,
) -> MetadataCallArgs {
    MetadataCallArgs::ListRelationsSchemasByPatterns {
        patterns: patterns
            .into_iter()
            .map(|p| p.as_ref().to_string())
            .collect(),
    }
}

/// Create MetadataCallArgs for create_schemas_if_not_exists.
pub fn args_create_schemas_if_not_exists(
    catalog_schemas: &BTreeMap<String, BTreeSet<String>>,
) -> MetadataCallArgs {
    MetadataCallArgs::CreateSchemasIfNotExists {
        catalog_schemas: catalog_schemas
            .iter()
            .map(|(c, schemas)| CatalogSchemas {
                catalog: c.clone(),
                schemas: schemas.iter().cloned().collect(),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, TimeUnit};

    #[test]
    fn test_schema_result_serialization() {
        let mut map = HashMap::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        map.insert("test.schema.table".to_string(), Ok(schema));
        map.insert(
            "test.schema.error".to_string(),
            Err(AdapterError::new(AdapterErrorKind::Internal, "test error")),
        );

        let json = map.to_recording_json();
        assert!(json.is_object());
        let obj = json.as_object().unwrap();
        assert!(obj.contains_key("test.schema.table"));
        assert!(obj.contains_key("test.schema.error"));

        // Check successful entry has schema
        let table_result = &obj["test.schema.table"];
        assert!(table_result.get("ok").is_some());

        // Check error entry
        let error_result = &obj["test.schema.error"];
        assert!(error_result.get("error").is_some());
    }

    #[test]
    fn test_schema_roundtrip_with_complex_types() {
        // Test various Arrow DataTypes that should properly roundtrip
        let mut map: HashMap<String, AdapterResult<Arc<Schema>>> = HashMap::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("balance", DataType::Decimal128(38, 9), true),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(
                "updated_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new("is_active", DataType::Boolean, false),
            Field::new("score", DataType::Float64, true),
            Field::new("date", DataType::Date32, true),
            Field::new(
                "nested",
                DataType::FixedSizeList(
                    Arc::new(Field::new(
                        "item",
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        true,
                    )),
                    1,
                ),
                true,
            ),
        ]));
        map.insert("test.schema.complex".to_string(), Ok(schema));

        // Serialize to JSON
        let json = map.to_recording_json();

        // Deserialize back
        let deserialized: HashMap<String, AdapterResult<Arc<Schema>>> =
            HashMap::from_recording_json(&json).expect("Failed to deserialize");

        // Verify roundtrip
        assert_eq!(map.len(), deserialized.len());
        let original_schema = map.get("test.schema.complex").unwrap().as_ref().unwrap();
        let result_schema = deserialized
            .get("test.schema.complex")
            .unwrap()
            .as_ref()
            .unwrap();

        // Compare field by field
        assert_eq!(original_schema.fields().len(), result_schema.fields().len());
        for (orig, res) in original_schema.fields().iter().zip(result_schema.fields()) {
            assert_eq!(orig.name(), res.name(), "Field name mismatch");
            assert_eq!(
                orig.data_type(),
                res.data_type(),
                "DataType mismatch for field {}",
                orig.name()
            );
            assert_eq!(orig.is_nullable(), res.is_nullable(), "Nullable mismatch");
        }
    }

    #[test]
    fn test_args_list_relations_schemas() {
        let args = args_list_relations_schemas(
            Some("model.test".to_string()),
            Some("compile".to_string()),
            ["db.schema.table1", "db.schema.table2"],
        );

        match args {
            MetadataCallArgs::ListRelationsSchemas {
                unique_id,
                phase,
                relations,
            } => {
                assert_eq!(unique_id, Some("model.test".to_string()));
                assert_eq!(phase, Some("compile".to_string()));
                assert_eq!(relations.len(), 2);
            }
            _ => panic!("Expected ListRelationsSchemas"),
        }
    }

    #[test]
    fn test_args_freshness() {
        let args = args_freshness(["source.a.b", "source.c.d"]);

        match args {
            MetadataCallArgs::Freshness { relations } => {
                assert_eq!(relations.len(), 2);
                assert_eq!(relations[0], "source.a.b");
            }
            _ => panic!("Expected Freshness"),
        }
    }
}
