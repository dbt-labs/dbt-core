use dbt_agate::AgateTable;
use minijinja::listener::RenderingEventListener;
use minijinja::value::{Enumerator, Object, ValueMap};
use minijinja::{State, Value};
use serde::de::Deserializer;
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use std::rc::Rc;
use std::sync::Arc;

use crate::value::none_value;

const KEY_MESSAGE: &str = "message";
const KEY_MESSAGE_ALIAS: &str = "_message";
const KEY_CODE: &str = "code";
const KEY_ROWS_AFFECTED: &str = "rows_affected";
const KEY_QUERY_ID: &str = "query_id";

const KNOWN_KEYS: &[&str] = &[KEY_MESSAGE, KEY_CODE, KEY_ROWS_AFFECTED, KEY_QUERY_ID];

/// Response from adapter statement execution.
///
/// A transparent map keyed by string values. Well-known keys (`message`,
/// `code`, `rows_affected`, `query_id`) are exposed via accessors with defaults
/// matching the historical field layout; arbitrary extra keys are preserved.
#[derive(Debug, Clone, PartialEq)]
pub struct AdapterResponse(ValueMap);

impl AdapterResponse {
    /// Start building an empty response.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        let m = message.into();
        self.0
            .insert(Value::from(KEY_MESSAGE), Value::from(m.clone()));
        self.0
            .insert(Value::from(KEY_MESSAGE_ALIAS), Value::from(m));
        self
    }

    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.0
            .insert(Value::from(KEY_CODE), Value::from(code.into()));
        self
    }

    pub fn with_rows_affected(mut self, rows_affected: i64) -> Self {
        self.0
            .insert(Value::from(KEY_ROWS_AFFECTED), Value::from(rows_affected));
        self
    }

    pub fn with_query_id(mut self, query_id: impl Into<String>) -> Self {
        self.0
            .insert(Value::from(KEY_QUERY_ID), Value::from(query_id.into()));
        self
    }

    /// Insert an arbitrary key/value pair.
    pub fn with(mut self, key: impl Into<Value>, value: impl Into<Value>) -> Self {
        self.0.insert(key.into(), value.into());
        self
    }

    fn get_str(&self, key: &str) -> Option<String> {
        self.0
            .get(&Value::from(key))
            .and_then(|v| v.as_str().map(String::from))
    }

    pub fn message(&self) -> String {
        self.get_str(KEY_MESSAGE).unwrap_or_default()
    }

    pub fn code(&self) -> String {
        self.get_str(KEY_CODE).unwrap_or_default()
    }

    pub fn query_id(&self) -> Option<String> {
        self.get_str(KEY_QUERY_ID)
    }

    /// Rows affected, clamping the `-1` unknown sentinel (and any negative) to 0.
    pub fn rows_affected(&self) -> u64 {
        u64::try_from(self.rows_affected_i64()).unwrap_or(0)
    }

    /// Raw rows-affected value; negative (e.g. `-1`) means unknown.
    pub fn rows_affected_i64(&self) -> i64 {
        self.0
            .get(&Value::from(KEY_ROWS_AFFECTED))
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
    }
}

impl Default for AdapterResponse {
    /// Build a response with the canonical field layout, adding defaults for
    /// `message`, `code`, and `rows_affected`.
    fn default() -> AdapterResponse {
        let mut map = ValueMap::default();
        map.insert(Value::from(KEY_MESSAGE), Value::from(String::default()));
        map.insert(Value::from(KEY_CODE), Value::from(String::default()));
        map.insert(Value::from(KEY_ROWS_AFFECTED), Value::from(0i64));
        Self(map)
    }
}

impl Serialize for AdapterResponse {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for AdapterResponse {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(AdapterResponse(ValueMap::deserialize(deserializer)?))
    }
}

impl Object for AdapterResponse {
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        Some(self.0.get(key).cloned().unwrap_or_else(none_value))
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        let mut keys: Vec<Value> = KNOWN_KEYS.iter().map(|k| Value::from(*k)).collect();
        for key in self.0.keys() {
            let is_known = key.as_str().is_some_and(|s| KNOWN_KEYS.contains(&s));
            if !is_known {
                keys.push(key.clone());
            }
        }
        Enumerator::Iter(Box::new(keys.into_iter()))
    }
}

impl TryFrom<Value> for AdapterResponse {
    type Error = minijinja::Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        if let Some(response) = value.downcast_object::<AdapterResponse>() {
            Ok((*response).clone())
        } else if let Some(message_str) = value.as_str() {
            Ok(AdapterResponse::new().with_message(message_str))
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
    use minijinja_contrib::testing::jinja_assert;
    use std::collections::BTreeMap;

    fn metadata_to_yaml(resp: &AdapterResponse) -> BTreeMap<String, dbt_yaml::Value> {
        let mut map = BTreeMap::new();
        map.insert(
            "_message".to_string(),
            dbt_yaml::Value::string(resp.message()),
        );
        map.insert("code".to_string(), dbt_yaml::Value::string(resp.code()));
        map.insert(
            "rows_affected".to_string(),
            dbt_yaml::to_value(resp.rows_affected_i64()).expect("i64 serialises to YAML"),
        );
        if let Some(qid) = resp.query_id() {
            map.insert("query_id".to_string(), dbt_yaml::Value::string(qid));
        }
        map
    }

    #[test]
    fn test_to_adapter_response_map_matches_core_format() {
        let resp = AdapterResponse::new()
            .with_message("SUCCESS 42")
            .with_code("SUCCESS")
            .with_rows_affected(42)
            .with_query_id("01c2f954-abc");
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
        let resp = AdapterResponse::new()
            .with_message("SUCCESS 0")
            .with_code("SUCCESS")
            .with_rows_affected(0);
        let map = metadata_to_yaml(&resp);
        assert!(!map.contains_key("query_id"));
        assert_eq!(map.len(), 3); // _message, code, rows_affected
    }

    #[test]
    fn test_json_roundtrip_preserves_extra_keys() {
        let resp = AdapterResponse::new()
            .with_message("SUCCESS 42")
            .with_code("SUCCESS")
            .with_rows_affected(42)
            .with("foo", "bar");

        let json = serde_json::to_string(&resp).expect("serializes to JSON");
        let roundtripped: AdapterResponse =
            serde_json::from_str(&json).expect("deserializes from JSON");

        assert_eq!(roundtripped, resp);
        assert_eq!(
            roundtripped
                .0
                .get(&Value::from("foo"))
                .and_then(|v| v.as_str()),
            Some("bar")
        );
    }

    fn sample_response() -> AdapterResponse {
        AdapterResponse::new()
            .with_message("SUCCESS 42")
            .with_code("SUCCESS")
            .with_rows_affected(42)
            .with_query_id("01c2f954-abc")
            .with("foo", "bar")
    }

    #[test]
    fn test_jinja_renders_all_keys() {
        jinja_assert(
            sample_response(),
            "{{ obj.message }}|{{ obj.code }}|{{ obj.rows_affected }}|{{ obj.query_id }}|{{ obj.foo }}",
            "SUCCESS 42|SUCCESS|42|01c2f954-abc|bar",
        );
    }

    #[test]
    fn test_jinja_renders_as_dict() {
        jinja_assert(
            sample_response(),
            "{{ obj }}",
            "{'message': 'SUCCESS 42', 'code': 'SUCCESS', 'rows_affected': 42, 'query_id': '01c2f954-abc', '_message': 'SUCCESS 42', 'foo': 'bar'}",
        );
    }

    #[test]
    fn test_default_constructor() {
        jinja_assert(
            AdapterResponse::default(),
            "{{ obj.message }}|{{ obj.code }}|{{ obj.rows_affected }}|{{ obj.query_id }}|{{ obj.foo }}",
            "||0|None|None",
        );
    }

    #[test]
    fn test_jinja_tojson() {
        jinja_assert(
            sample_response(),
            "{{ obj | tojson }}",
            r#"{"_message": "SUCCESS 42", "code": "SUCCESS", "foo": "bar", "message": "SUCCESS 42", "query_id": "01c2f954-abc", "rows_affected": 42}"#,
        );
    }
}
