use dbt_agate::AgateTable;
use minijinja::listener::RenderingEventListener;
use minijinja::value::{Enumerator, Object};
use minijinja::{State, Value};
use serde::{Deserialize, Serialize};
use std::rc::Rc;
use std::sync::Arc;

/// Response from adapter statement execution
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
    #[serde(default)]
    pub query_id: Option<String>,
}

impl AdapterResponse {
    pub fn new(rows_affected: i64, query_id: Option<String>) -> Self {
        Self {
            message: format!("SUCCESS {}", rows_affected),
            code: "SUCCESS".to_string(),
            rows_affected,
            query_id,
        }
    }
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
            _ => None,
        }
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        Enumerator::Str(&["message", "code", "rows_affected", "query_id"])
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
                query_id: None,
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
    use std::collections::BTreeMap;

    fn metadata_to_yaml(resp: &AdapterResponse) -> BTreeMap<String, dbt_yaml::Value> {
        let mut map = BTreeMap::new();
        map.insert(
            "_message".to_string(),
            dbt_yaml::Value::string(resp.message.clone()),
        );
        map.insert(
            "code".to_string(),
            dbt_yaml::Value::string(resp.code.clone()),
        );
        map.insert(
            "rows_affected".to_string(),
            dbt_yaml::to_value(resp.rows_affected).expect("i64 serialises to YAML"),
        );
        if let Some(qid) = &resp.query_id {
            map.insert("query_id".to_string(), dbt_yaml::Value::string(qid.clone()));
        }
        map
    }

    #[test]
    fn test_to_adapter_response_map_matches_core_format() {
        let resp = AdapterResponse {
            message: "SUCCESS 42".to_string(),
            code: "SUCCESS".to_string(),
            rows_affected: 42,
            query_id: Some("01c2f954-abc".to_string()),
        };
        let map = metadata_to_yaml(&resp);

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
        };
        let map = metadata_to_yaml(&resp);
        assert!(!map.contains_key("query_id"));
        assert_eq!(map.len(), 3); // _message, code, rows_affected
    }
}
