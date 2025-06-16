use minijinja::listener::RenderingEventListener;
use minijinja::value::{Enumerator, Object};
use minijinja::Value;
use minijinja::{Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, State};
use std::rc::Rc;
use std::sync::Arc;

use dbt_agate::AgateTable;

/// Response from adapter statement execution
#[derive(Debug, Default, Clone, PartialEq)]
pub struct AdapterResponse {
    /// Message from adapter
    pub message: String,
    /// Status code from adapter
    pub code: String,
    /// Rows affected by statement
    pub rows_affected: i64,
    /// Query ID of executed statement, if available
    pub query_id: Option<String>,
}

impl Object for AdapterResponse {
    fn call(
        self: &Arc<Self>,
        _state: &State,
        _args: &[Value],
        _listener: Rc<dyn RenderingEventListener>,
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
