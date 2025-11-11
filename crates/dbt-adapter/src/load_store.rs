use minijinja::arg_utils::ArgsIter;
use minijinja::value::Value;
use minijinja::{Error as MinijinjaError, ErrorKind as MinijinjaErrorKind};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::response::{AdapterResponse, ResultObject};
use dbt_agate::AgateTable;

use super::funcs::none_value;

/// A store for DBT query results that provides callable functions to access the store
#[derive(Clone, Default)]
pub struct ResultStore {
    results: Arc<Mutex<HashMap<String, Value>>>,
}

impl ResultStore {
    /// Clear all results from the store
    pub fn clear(&self) {
        let mut results = self.results.lock().unwrap();
        results.clear();
    }

    /// Returns a callable function that stores results in the internal map
    /// https://github.com/dbt-labs/dbt-core/blob/34bb3f94dde716a3f9c36481d2ead85c211075dd/core/dbt/context/providers.py#L1043
    pub fn store_result(
        &self,
    ) -> impl Fn(&[Value]) -> Result<Value, MinijinjaError> + Clone + use<> {
        let store = self.clone();
        move |args: &[Value]| {
            // name: str,
            // response: Any,
            // agate_table: Optional["agate.Table"] = None
            let iter = ArgsIter::new("store_result", &["name", "response"], args);
            let name: String = iter.next_arg::<&str>()?.to_string();
            let response = AdapterResponse::try_from(iter.next_arg::<Value>()?)?;

            let table: Option<Value> = iter.next_kwarg::<Option<Value>>("agate_table")?;
            let table = if let Some(t) = table {
                if !t.is_none() {
                    Some((*t.downcast_object::<AgateTable>().expect("agate_table")).clone())
                } else {
                    Some(AgateTable::default())
                }
            } else {
                Some(AgateTable::default())
            };
            let value = Value::from_object(ResultObject::new(response, table));
            iter.finish()?;

            let mut results = store.results.lock().unwrap();
            results.insert(name, value);

            Ok(Value::from(""))
        }
    }

    /// Returns a callable function that loads results from the internal map
    /// https://github.com/dbt-labs/dbt-core/blob/34bb3f94dde716a3f9c36481d2ead85c211075dd/core/dbt/context/providers.py#L1022
    pub fn load_result(
        &self,
    ) -> impl Fn(&[Value]) -> Result<Value, MinijinjaError> + Clone + use<> {
        let store = self.clone();
        move |args: &[Value]| {
            // name: str,
            let iter = ArgsIter::new("load_result", &["name"], args);
            let name: String = iter.next_arg::<&str>()?.to_string();
            iter.finish()?;

            let mut results = store.results.lock().unwrap();

            if let Some(value) = results.get_mut(&name) {
                if name == "main" {
                    Ok(value.clone())
                } else if *value == none_value() {
                    Err(MinijinjaError::new(
                        MinijinjaErrorKind::MacroResultAlreadyLoadedError,
                        format!(
                            "The 'statement' result named '{name}' has already been loaded into a variable"
                        ),
                    ))
                } else {
                    let result = value.clone();
                    *value = none_value();
                    Ok(result)
                }
            } else {
                Ok(none_value())
            }
        }
    }

    /// Returns a callable function that stores raw results in the internal map
    /// https://github.com/dbt-labs/dbt-core/blob/34bb3f94dde716a3f9c36481d2ead85c211075dd/core/dbt/context/providers.py#L1043
    pub fn store_raw_result(
        &self,
    ) -> impl Fn(&[Value]) -> Result<Value, MinijinjaError> + Clone + use<> {
        let store = self.clone();
        move |args: &[Value]| {
            // name: str,
            // message=Optional[str],
            // code=Optional[str],
            // rows_affected=Optional[str],
            // agate_table: Optional["agate.Table"] = None,
            let iter = ArgsIter::new("store_raw_result", &[], args);
            let name: String = iter.next_kwarg::<String>("name")?;
            let message: Option<String> = iter.next_kwarg::<Option<String>>("message")?;
            let code: Option<String> = iter.next_kwarg::<Option<String>>("code")?;
            let rows_affected: Option<String> =
                iter.next_kwarg::<Option<String>>("rows_affected")?;
            let agate_table: Option<Value> = iter.next_kwarg::<Option<Value>>("agate_table")?;

            // Create adapter response
            let response = AdapterResponse {
                message: message.unwrap_or_default(),
                code: code.unwrap_or_default(),
                rows_affected: rows_affected
                    .unwrap_or_default()
                    .parse::<i64>()
                    .unwrap_or(0),
                query_id: None,
            };

            // Call store_result directly instead of using function
            let mut results = store.results.lock().unwrap();
            let value = Value::from_object(ResultObject::new(
                response,
                agate_table
                    .map(|t| {
                        if !t.is_none() {
                            (*t.downcast_object::<AgateTable>().expect("agate_table")).clone()
                        } else {
                            AgateTable::default()
                        }
                    })
                    .or(Some(AgateTable::default())),
            ));

            results.insert(name, value);
            Ok(Value::from(true))
        }
    }
}
