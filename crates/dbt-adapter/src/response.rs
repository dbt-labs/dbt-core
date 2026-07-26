use dbt_agate::AgateTable;
use minijinja::listener::RenderingEventListener;
use minijinja::value::{Enumerator, Object};
use minijinja::{State, Value};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

/// Response from adapter statement execution.
///
/// Field names and serialization shape match dbt Core's `AdapterResponse` /
/// `SnowflakeAdapterResponse` as written to `run_results.json`.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdapterResponse {
    /// Mantle compare_results historically emits `_message`
    #[serde(default, alias = "_message")]
    pub message: String,
    /// Status code from adapter
    #[serde(default)]
    pub code: String,
    /// Rows affected by statement
    #[serde(default)]
    pub rows_affected: i64,
    /// Query ID of executed statement, if available
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_id: Option<String>,
    /// Snowflake DML: rows inserted (CTAS / INSERT / MERGE)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows_inserted: Option<i64>,
    /// Snowflake DML: rows deleted
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows_deleted: Option<i64>,
    /// Snowflake DML: rows updated
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows_updated: Option<i64>,
    /// Snowflake DML: duplicate / multi-joined rows
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows_duplicates: Option<i64>,
}

impl AdapterResponse {
    pub fn new(rows_affected: i64, query_id: Option<String>) -> Self {
        Self {
            message: format!("SUCCESS {}", rows_affected),
            code: "SUCCESS".to_string(),
            rows_affected,
            query_id,
            ..Default::default()
        }
    }

    /// Attach Snowflake-specific DML statistics from the result metadata row.
    pub fn with_snowflake_dml(mut self, dml: SnowflakeDmlStats) -> Self {
        self.rows_inserted = dml.rows_inserted;
        self.rows_deleted = dml.rows_deleted;
        self.rows_updated = dml.rows_updated;
        self.rows_duplicates = dml.rows_duplicates;
        self
    }

    /// Serialize into the Core-compatible `adapter_response` map for `run_results.json`.
    ///
    /// Core uses `_message` (not `message`). Optional Snowflake DML fields and
    /// `query_id` are omitted when absent.
    pub fn to_run_results_map(&self) -> BTreeMap<String, dbt_yaml::Value> {
        let mut map = BTreeMap::new();
        map.insert(
            "_message".to_string(),
            dbt_yaml::Value::string(self.message.clone()),
        );
        map.insert(
            "code".to_string(),
            dbt_yaml::Value::string(self.code.clone()),
        );
        map.insert(
            "rows_affected".to_string(),
            dbt_yaml::to_value(self.rows_affected).expect("i64 serialises to YAML"),
        );
        if let Some(qid) = &self.query_id {
            map.insert("query_id".to_string(), dbt_yaml::Value::string(qid.clone()));
        }
        insert_optional_i64(&mut map, "rows_inserted", self.rows_inserted);
        insert_optional_i64(&mut map, "rows_deleted", self.rows_deleted);
        insert_optional_i64(&mut map, "rows_updated", self.rows_updated);
        insert_optional_i64(&mut map, "rows_duplicates", self.rows_duplicates);
        map
    }
}

fn insert_optional_i64(
    map: &mut BTreeMap<String, dbt_yaml::Value>,
    key: &str,
    value: Option<i64>,
) {
    if let Some(v) = value
        && let Ok(yml) = dbt_yaml::to_value(v)
    {
        map.insert(key.to_string(), yml);
    }
}

/// Granular Snowflake DML statistics from Arrow result columns.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SnowflakeDmlStats {
    pub rows_inserted: Option<i64>,
    pub rows_deleted: Option<i64>,
    pub rows_updated: Option<i64>,
    pub rows_duplicates: Option<i64>,
}

impl Object for AdapterResponse {
    fn call(
        self: &Arc<Self>,
        _state: &State,
        _args: &[Value],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, minijinja::Error> {
        unimplemented!("Is response from 'execute' callable?")
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str()? {
            "message" => Some(Value::from(self.message.clone())),
            "code" => Some(Value::from(self.code.clone())),
            "rows_affected" => Some(Value::from(self.rows_affected)),
            "query_id" => Some(Value::from(self.query_id.clone())),
            "rows_inserted" => Some(Value::from(self.rows_inserted)),
            "rows_deleted" => Some(Value::from(self.rows_deleted)),
            "rows_updated" => Some(Value::from(self.rows_updated)),
            "rows_duplicates" => Some(Value::from(self.rows_duplicates)),
            _ => None,
        }
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        Enumerator::Str(&[
            "message",
            "code",
            "rows_affected",
            "query_id",
            "rows_inserted",
            "rows_deleted",
            "rows_updated",
            "rows_duplicates",
        ])
    }
}

impl TryFrom<Value> for AdapterResponse {
    type Error = minijinja::Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        if let Some(response) = value.downcast_object::<AdapterResponse>() {
            Ok((*response).clone())
        } else if let Some(message_str) = value.as_str() {
            Ok(AdapterResponse {
                message: message_str.to_string(),
                code: "".to_string(),
                rows_affected: 0,
                ..Default::default()
            })
        } else {
            Err(minijinja::Error::new(
                minijinja::ErrorKind::CannotDeserialize,
                "Failed to downcast response",
            ))
        }
    }
}

/// load_result response object
#[derive(Debug)]
pub struct ResultObject {
    pub response: AdapterResponse,
    pub table: Option<AgateTable>,
    #[allow(unused)]
    pub data: Option<Value>,
}

impl ResultObject {
    pub fn new(response: AdapterResponse, table: Option<AgateTable>) -> Self {
        let data = if let Some(table) = &table {
            Some(Value::from_object(table.rows()))
        } else {
            Some(Value::UNDEFINED)
        };
        Self {
            response,
            table,
            data,
        }
    }
}

impl Object for ResultObject {
    fn call_method(
        self: &Arc<Self>,
        _state: &State<'_, '_>,
        method: &str,
        _args: &[Value],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, minijinja::Error> {
        // NOTE: the `keys` method is used by the `stage_external_sources` macro in
        // `dbt-external-table`. Don't delete this unless the external package is fixed.
        if method == "keys" {
            Ok(Value::from_iter(["response", "table", "data"]))
        } else {
            Err(minijinja::Error::new(
                minijinja::ErrorKind::UnknownMethod,
                format!("Unknown method on ResultObject: '{method}'"),
            ))
        }
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str()? {
            "table" => self
                .table
                .as_ref()
                .map(|t| Value::from_object((*t).clone())),
            "data" => self.data.clone(),
            "response" => Some(Value::from_object(self.response.clone())),
            _ => Some(Value::UNDEFINED), // Only return empty at Parsetime TODO fix later
        }
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        Enumerator::Str(&["table", "data", "response"])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_adapter_response_map_matches_core_format() {
        let resp = AdapterResponse {
            message: "SUCCESS 42".to_string(),
            code: "SUCCESS".to_string(),
            rows_affected: 42,
            query_id: Some("01c2f954-abc".to_string()),
            ..Default::default()
        };
        let map = resp.to_run_results_map();

        // Core uses `_message`, not `message`
        assert_eq!(
            map.get("_message").and_then(|v| v.as_str()),
            Some("SUCCESS 42")
        );
        assert_eq!(map.get("code").and_then(|v| v.as_str()), Some("SUCCESS"));
        assert_eq!(map.get("rows_affected").and_then(|v| v.as_i64()), Some(42));
        assert_eq!(
            map.get("query_id").and_then(|v| v.as_str()),
            Some("01c2f954-abc")
        );
        // `message` key should NOT be present (Core uses `_message`)
        assert!(!map.contains_key("message"));
    }

    #[test]
    fn test_to_adapter_response_map_omits_null_query_id() {
        let resp = AdapterResponse {
            message: "SUCCESS 0".to_string(),
            code: "SUCCESS".to_string(),
            rows_affected: 0,
            query_id: None,
            ..Default::default()
        };
        let map = resp.to_run_results_map();
        assert!(!map.contains_key("query_id"));
        assert_eq!(map.len(), 3); // _message, code, rows_affected
    }

    #[test]
    fn test_to_adapter_response_map_includes_snowflake_dml() {
        let resp = AdapterResponse::new(3, Some("qid".to_string())).with_snowflake_dml(
            SnowflakeDmlStats {
                rows_inserted: Some(1),
                rows_deleted: Some(0),
                rows_updated: Some(2),
                rows_duplicates: Some(0),
            },
        );
        let map = resp.to_run_results_map();
        assert_eq!(map.get("rows_inserted").and_then(|v| v.as_i64()), Some(1));
        assert_eq!(map.get("rows_deleted").and_then(|v| v.as_i64()), Some(0));
        assert_eq!(map.get("rows_updated").and_then(|v| v.as_i64()), Some(2));
        assert_eq!(map.get("rows_duplicates").and_then(|v| v.as_i64()), Some(0));
        assert_eq!(map.get("query_id").and_then(|v| v.as_str()), Some("qid"));
    }
}
