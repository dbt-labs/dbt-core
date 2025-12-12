use crate::AdapterType;

use arrow::array::RecordBatch;
use dbt_agate::AgateTable;
use minijinja::Value;
use minijinja::listener::RenderingEventListener;
use minijinja::value::{Enumerator, Object};
use minijinja::{Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, State};
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
    //! TODO: find references via searching for keywords `def get_response` in the dbt-adapters repository.
    //! These components all differ across adapters, implement as they're when necessary
    pub fn new(batch: &RecordBatch, adapter_type: AdapterType) -> Self {
        Self {
            message: Self::message(batch, adapter_type),
            code: Self::code(batch, adapter_type),
            rows_affected: batch.num_rows() as i64,
            query_id: Self::query_id(batch, adapter_type),
        }
    }

    /// Get the message for the response from the batch.
    fn message(batch: &RecordBatch, _adapter_type: AdapterType) -> String {
        format!("{} {}", "SUCCESS", batch.num_rows())
    }

    /// Get the code for the response from the batch.
    fn code(_batch: &RecordBatch, _adapter_type: AdapterType) -> String {
        "SUCCESS".to_string()
    }

    /// Get the query ID for the response from the batch.
    pub(crate) fn query_id(batch: &RecordBatch, adapter_type: AdapterType) -> Option<String> {
        match adapter_type {
            AdapterType::Snowflake => batch
                .schema()
                .metadata()
                .get("SNOWFLAKE_QUERY_ID")
                .map(|query_id: &String| query_id.to_string()),
            AdapterType::Bigquery => batch
                .schema()
                .metadata()
                .get("BIGQUERY:query_id")
                .map(|query_id: &String| query_id.to_string()),
            AdapterType::Databricks => batch
                .schema()
                .metadata()
                .get("DATABRICKS_QUERY_ID")
                .map(|query_id: &String| query_id.to_string()),
            _ => None,
        }
    }
}

impl Object for AdapterResponse {
    fn call(
        self: &Arc<Self>,
        _state: &State,
        _args: &[Value],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, MinijinjaError> {
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
    type Error = MinijinjaError;

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
            Err(MinijinjaError::new(
                MinijinjaErrorKind::CannotDeserialize,
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
    ) -> Result<Value, MinijinjaError> {
        // NOTE: the `keys` method is used by the `stage_external_sources` macro in
        // `dbt-external-table`. Don't delete this unless the external package is fixed.
        if method == "keys" {
            Ok(Value::from_iter(["response", "table", "data"]))
        } else {
            Err(minijinja::Error::new(
                minijinja::ErrorKind::InvalidOperation,
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
