use minijinja::arg_utils::ArgParser;
use minijinja::value::Value;
use minijinja::{
    missing_argument, too_many_arguments, Error as MinijinjaError, ErrorKind as MinijinjaErrorKind,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::adapters::response::{AdapterResponse, ResultObject};
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
    pub fn store_result(&self) -> impl Fn(&[Value]) -> Result<Value, MinijinjaError> + Clone {
        let store = self.clone();
        move |args: &[Value]| {
            let mut args = ArgParser::new(args, None);
            let num_args = args.positional_len() + args.kwargs_len();
            match num_args {
                0..=2 => return missing_argument!("store_result requires at least two arguments"),
                3 => {}
                _ => return too_many_arguments!("store_result takes up to three arguments"),
            };

            let name: String = args
                .next_positional::<Value>()?
                .as_str()
                .unwrap()
                .to_string();

            let response = args.get::<Value>("response")?;
            let response = AdapterResponse::try_from(response)?;

            let table: Option<Value> = args.get_optional::<Value>("agate_table");
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

            let mut results = store.results.lock().unwrap();
            results.insert(name, value);

            Ok(Value::from(""))
        }
    }

    /// Returns a callable function that loads results from the internal map
    pub fn load_result(&self) -> impl Fn(&[Value]) -> Result<Value, MinijinjaError> + Clone {
        let store = self.clone();
        move |args: &[Value]| {
            let mut args: ArgParser = ArgParser::new(args, None);

            let num_args = args.positional_len() + args.kwargs_len();
            let error_msg = "load_result requires one argument";
            match num_args {
                0 => return missing_argument!(error_msg),
                1 => {}
                _ => return too_many_arguments!(error_msg),
            };

            let name: String = args
                .next_positional::<Value>()?
                .as_str()
                .unwrap()
                .to_string();

            let mut results = store.results.lock().unwrap();

            if let Some(value) = results.get_mut(&name) {
                if name == "main" {
                    Ok(value.clone())
                } else if *value == none_value() {
                    Err(MinijinjaError::new(
                        MinijinjaErrorKind::MacroResultAlreadyLoadedError(name),
                        "name",
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
    pub fn store_raw_result(&self) -> impl Fn(&[Value]) -> Result<Value, MinijinjaError> + Clone {
        let store = self.clone();
        move |args: &[Value]| {
            let mut args = ArgParser::new(args, None);

            let num_args = args.positional_len() + args.kwargs_len();
            if num_args < 1 {
                return missing_argument!("store_raw_result requires at least a name argument");
            }

            let name: String = args.get::<String>("name")?;

            if name.is_empty() {
                return Err(MinijinjaError::new(
                    MinijinjaErrorKind::InvalidOperation,
                    "name cannot be empty",
                ));
            }

            let message: Option<String> = args.get_optional::<String>("message");
            let code: Option<String> = args.get_optional::<String>("code");
            let rows_affected: Option<String> = args.get_optional::<String>("rows_affected");
            let agate_table: Option<Value> = args.get_optional::<Value>("agate_table");

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
