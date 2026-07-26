use minijinja::arg_utils::ArgsIter;
use minijinja::value::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::response::{AdapterResponse, ResultObject};
use crate::value::none_value;
use dbt_agate::AgateTable;
use dbt_common::adapter_response_store::record_adapter_response;
use dbt_common::tracing::span_info::find_and_update_span_attrs;
use dbt_telemetry::NodeEvaluated;

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

    /// https://github.com/dbt-labs/dbt-core/blob/34bb3f94dde716a3f9c36481d2ead85c211075dd/core/dbt/context/providers.py#L1043
    pub fn store_result(
        &self,
    ) -> impl Fn(&[Value]) -> Result<Value, minijinja::Error> + Clone + use<> {
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

            // Persist adapter metadata for run_results.json. Only the
            // materialization's "main" statement should populate the node's
            // adapter_response — helper queries (run_query, etc.) must not
            // overwrite query_id / rows_affected / DML stats.
            if name == "main" {
                publish_adapter_response(&response);
            }

            let value = Value::from_object(ResultObject::new(response, table));
            iter.finish()?;

            let mut results = store.results.lock().unwrap();
            results.insert(name, value);

            Ok(Value::from(""))
        }
    }

    /// https://github.com/dbt-labs/dbt-core/blob/34bb3f94dde716a3f9c36481d2ead85c211075dd/core/dbt/context/providers.py#L1022
    pub fn load_result(
        &self,
    ) -> impl Fn(&[Value]) -> Result<Value, minijinja::Error> + Clone + use<> {
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
                    Err(minijinja::Error::new(
                        minijinja::ErrorKind::MacroResultAlreadyLoadedError,
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

    /// https://github.com/dbt-labs/dbt-core/blob/34bb3f94dde716a3f9c36481d2ead85c211075dd/core/dbt/context/providers.py#L1043
    pub fn store_raw_result(
        &self,
    ) -> impl Fn(&[Value]) -> Result<Value, minijinja::Error> + Clone + use<> {
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

            // Parse rows_affected only if string value is present and valid
            let rows_affected = if let Some(rows_affected) = rows_affected
                && let Some(rows) = rows_affected.parse::<i64>().ok()
            {
                rows
            } else {
                0
            };

            // Create adapter response (keep original semantics: default to 0 if not present)
            let response = AdapterResponse {
                message: message.unwrap_or_default(),
                code: code.unwrap_or_default(),
                rows_affected,
                query_id: None,
                ..Default::default()
            };

            if name == "main" {
                publish_adapter_response(&response);
            }

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

/// Write rows_affected onto the NodeEvaluated span and stash the full
/// Core-compatible adapter_response map for later inclusion in run_results.
fn publish_adapter_response(response: &AdapterResponse) {
    // dbt-core uses -1 to indicate unknown rows affected. Telemetry uses `None` for unknown.
    if response.rows_affected >= 0 {
        find_and_update_span_attrs::<_, NodeEvaluated>(|attrs| {
            attrs.rows_affected = Some(response.rows_affected as u64);
            record_adapter_response(attrs.unique_id.clone(), response.to_run_results_map());
        });
    } else {
        // Still record metadata (query_id / code / message / DML) even when
        // rows_affected is unknown, so QUERY_HISTORY correlation still works.
        find_and_update_span_attrs::<_, NodeEvaluated>(|attrs| {
            record_adapter_response(attrs.unique_id.clone(), response.to_run_results_map());
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use minijinja::value::Kwargs;

    fn store_named(store: &ResultStore, name: &str) -> Result<Value, minijinja::Error> {
        let store_raw = store.store_raw_result();
        store_raw(&[Value::from(Kwargs::from_iter([(
            "name",
            Value::from(name),
        )]))])
    }

    fn load_named(store: &ResultStore, name: &str) -> Result<Value, minijinja::Error> {
        let load = store.load_result();
        load(&[Value::from(name)])
    }

    /// The `statement(...)` macro pattern stores a named result and then loads
    /// (consumes) it. Two microbatch batches that share one registry interleave
    /// as store(A)/store(B)/load(A)/load(B); B's load then sees the consumed
    /// sentinel and raises `MacroResultAlreadyLoadedError`. This reproduces the
    /// `concurrent_batches=true` microbatch bug (fs#11019 follow-up), where the
    /// collision is on `get_columns_in_relation` (Postgres) or
    /// `run_query_statement` (Snowflake).
    #[test]
    fn shared_result_store_collides_across_batches() {
        let shared = ResultStore::default();
        store_named(&shared, "get_columns_in_relation").unwrap(); // batch A stores
        store_named(&shared, "get_columns_in_relation").unwrap(); // batch B overwrites
        load_named(&shared, "get_columns_in_relation").unwrap(); // batch A consumes
        let err = load_named(&shared, "get_columns_in_relation").unwrap_err(); // batch B
        assert_eq!(
            err.kind(),
            minijinja::ErrorKind::MacroResultAlreadyLoadedError
        );
    }

    /// The fix gives each batch its own `ResultStore` (see
    /// `reset_result_store`), so the same interleaving no longer collides.
    #[test]
    fn isolated_result_stores_do_not_collide_across_batches() {
        let batch_a = ResultStore::default();
        let batch_b = ResultStore::default();
        store_named(&batch_a, "get_columns_in_relation").unwrap();
        store_named(&batch_b, "get_columns_in_relation").unwrap();
        load_named(&batch_a, "get_columns_in_relation").unwrap();
        // batch B loads its own (still-present) result, not A's consumed one.
        let v = load_named(&batch_b, "get_columns_in_relation").unwrap();
        assert!(!v.is_none());
    }
}
