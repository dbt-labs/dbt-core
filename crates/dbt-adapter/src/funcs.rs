use crate::errors::AdapterResult;
use crate::errors::{AdapterError, AdapterErrorKind};
use crate::formatter::SqlLiteralFormatter;
use crate::relation::factory::create_static_relation;
use crate::response::ResultObject;
use crate::{Adapter, AdapterType};

use arrow::array::RecordBatch;
use dbt_agate::AgateTable;
use minijinja::value::ValueKind;
use minijinja::value::mutable_vec::MutableVec;
use minijinja::{State, Value};
use minijinja_contrib::modules::py_datetime::date::PyDate;
use minijinja_contrib::modules::py_datetime::datetime::PyDateTime;

use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;

/// Helper to call [Adapter::call_method_impl] with jinja-valued arguments.
#[cfg(test)]
fn dispatch_test(
    adapter: &Arc<Adapter>,
    name: &str,
    args: &[Value],
) -> Result<Value, minijinja::Error> {
    let env = minijinja::Environment::new();
    let state = State::new_for_env(&env);
    adapter.call_method_impl(&state, name, args, &[])
}

pub fn dispatch_adapter_get_value(adapter: &Adapter, key: &Value) -> Option<Value> {
    match key.as_str() {
        Some("behavior") => Some(adapter.behavior()),
        // NOTE(serramatutu): BigQuery adapter calls `Relation` from `adapter.Relation`
        // instead of `api.Relation` when executing materialized views
        Some("Relation") => create_static_relation(adapter.adapter_type(), adapter.quoting()),
        _ => None,
    }
}

/// Execute a macro under the `dbt` package.
/// Unlike [`execute_macro`] that returns a `Value`,
/// this function returns a `RecordBatch` which may become handy when the result manipulation is necessary.
///
/// # Panics
///
/// This function will panic if the macro named `dbt.{macro_name}` does not exist in the template state.
pub fn execute_macro_wrapper(
    state: &State,
    args: &[Value],
    macro_name: &str,
) -> Result<Arc<RecordBatch>, AdapterError> {
    execute_macro_wrapper_with_package(state, args, macro_name, "dbt")
}

/// Execute a macro under "dbt" package.
/// If you need to manipulate the result, checkout [`execute_macro_wrapper`]
///
/// # Panics
///
/// This function will panic if the macro named `dbt.{macro_name}` does not exist in the template state.
pub fn execute_macro(
    state: &State,
    args: &[Value],
    macro_name: &str,
) -> Result<Value, AdapterError> {
    execute_macro_with_package(state, args, macro_name, "dbt")
}

/// Execute a macro under a given package.
/// Unlike [`execute_macro_with_package`] that returns a `Value`,
/// this function returns a `RecordBatch` which may become handy when the result manipulation is necessary.
///
/// # Panics
///
/// This function will panic if the macro named `{package}.{macro_name}` does not exist in the template state.
pub fn execute_macro_wrapper_with_package(
    state: &State,
    args: &[Value],
    macro_name: &str,
    package: &str,
) -> Result<Arc<RecordBatch>, AdapterError> {
    let result: Value = execute_macro_with_package(state, args, macro_name, package)?;
    convert_macro_result_to_record_batch(&result)
}

pub fn convert_macro_result_to_record_batch(
    result: &Value,
) -> Result<Arc<RecordBatch>, AdapterError> {
    // Depending on the macro impl, result can be either ResultObject or AgateTable
    let table = if let Some(result) = result.downcast_object::<ResultObject>() {
        result.table.as_ref().expect("AgateTable exists").to_owned()
    } else if let Some(result) = result.downcast_object::<AgateTable>() {
        result.as_ref().to_owned()
    } else {
        return Err(AdapterError::new(
            AdapterErrorKind::UnexpectedResult,
            format!("Unexpected result type {result}"),
        ));
    };

    let record_batch = table.original_record_batch();
    Ok(record_batch)
}

/// Execute a macro under a given package.
/// If you need to manipulate the result, checkout [`execute_macro_wrapper_with_package`]
///
/// # Panics
///
/// This function will panic if the macro named `{package_name}.{macro_name}` does not exist in the template state.
pub fn execute_macro_with_package(
    state: &State,
    args: &[Value],
    macro_name: &str,
    package: &str,
) -> Result<Value, AdapterError> {
    let template_name = format!("{package}.{macro_name}");
    let template = state.env().get_template(&template_name)?;
    let base_ctx = state.get_base_context();
    let state = template.eval_to_state(base_ctx, &[])?;
    let func = state
        .lookup(macro_name, &[])
        .unwrap_or_else(|| panic!("{macro_name} exists"));
    func.call(&state, args, &[]).map_err(|err| {
        if let Some(source) = err.source() {
            if let Some(adapter_err) = source.downcast_ref::<AdapterError>() {
                return adapter_err.clone();
            }
        }
        AdapterError::new(AdapterErrorKind::UnexpectedResult, err.to_string())
    })
}

/// Returns a value that represents the absence of a value of a Object method return.
pub fn none_value() -> Value {
    Value::from(())
}

pub fn empty_string_value() -> Value {
    Value::from("")
}

pub fn empty_vec_value() -> Value {
    Value::from(Vec::<Value>::new())
}

pub fn empty_mutable_vec_value() -> Value {
    Value::from(MutableVec::<Value>::new())
}

pub fn empty_map_value() -> Value {
    Value::from(BTreeMap::<Value, Value>::new())
}

// Helper function to format SQL with bindings
pub fn format_sql_with_bindings(
    adapter_type: AdapterType,
    sql: &str,
    bindings: &Value,
) -> AdapterResult<String> {
    let formatter = SqlLiteralFormatter::new(adapter_type);
    let mut result = String::with_capacity(sql.len());
    // this placeholder char is seen from `get_binding_char` macro
    let binding_char = if adapter_type == AdapterType::Fabric {
        "?"
    } else {
        "%s"
    };
    let mut parts = sql.split(binding_char);
    let mut binding_iter = bindings.as_object().unwrap().try_iter().unwrap();

    // Add the first part (before any %s)
    if let Some(first) = parts.next() {
        result.push_str(first);
    }

    // For each remaining part, insert a binding value before it
    for part in parts {
        match binding_iter.next() {
            Some(value) => {
                // Convert minijinja::Value to a SQL-safe string
                match value.kind() {
                    ValueKind::String => {
                        result.push_str(&formatter.format_str(value.as_str().unwrap()))
                    }
                    ValueKind::Bytes => result.push_str(&formatter.format_bytes(&value)),
                    ValueKind::None => result.push_str(&formatter.none_value()),
                    ValueKind::Bool => result.push_str(&formatter.format_bool(value.is_true())),
                    _ => {
                        // TODO: handle the SQL escaping of more data types
                        if let Some(date) = value.downcast_object::<PyDate>() {
                            result.push_str(&formatter.format_date(date.as_ref().clone()));
                        } else if let Some(datetime) = value.downcast_object::<PyDateTime>() {
                            result.push_str(&formatter.format_datetime(datetime.as_ref().clone()));
                        } else {
                            result.push_str(&value.to_string())
                        }
                    }
                }
            }
            None => {
                return Err(AdapterError::new(
                    AdapterErrorKind::Configuration,
                    "Not enough bindings provided for SQL template".to_string(),
                ));
            }
        }
        result.push_str(part);
    }

    // Check if we used all bindings
    if binding_iter.next().is_some() {
        return Err(AdapterError::new(
            AdapterErrorKind::Configuration,
            "Too many bindings provided for SQL template".to_string(),
        ));
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::Adapter;
    use crate::adapter::adapter_impl::AdapterImpl;
    use crate::sql_types::SATypeOpsImpl;
    use crate::stmt_splitter::NaiveStmtSplitter;
    use dbt_adapter_core::AdapterType;
    use dbt_common::cancellation::never_cancels;
    use dbt_schemas::schemas::relations::{DEFAULT_DBT_QUOTING, DEFAULT_RESOLVED_QUOTING};
    use indexmap::IndexMap;

    /// Create a Typed-phase DuckDB adapter backed by MockEngine.
    fn make_duckdb_adapter() -> Arc<Adapter> {
        let concrete = AdapterImpl::new_mock(
            AdapterType::DuckDB,
            BTreeMap::new(),
            DEFAULT_RESOLVED_QUOTING,
            Box::new(SATypeOpsImpl::new(AdapterType::DuckDB)),
            Arc::new(NaiveStmtSplitter),
        );
        let adapter = Adapter::new(Arc::new(concrete), None, None, never_cancels());
        Arc::new(adapter)
    }

    /// Create a parse-phase DuckDB adapter (returns defaults, no real execution).
    fn make_duckdb_parse_adapter() -> Arc<Adapter> {
        let adapter = Adapter::new_parse_phase_adapter(
            AdapterType::DuckDB,
            dbt_yaml::Mapping::new(),
            DEFAULT_DBT_QUOTING,
            Box::new(SATypeOpsImpl::new(AdapterType::DuckDB)),
            None,
        );
        Arc::new(adapter)
    }

    /// Helper to build a minijinja dict Value from key-value pairs.
    fn dict(pairs: &[(&str, &str)]) -> Value {
        let map: IndexMap<String, Value> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), Value::from(*v)))
            .collect();
        Value::from(map)
    }

    // -- external_root tests --------------------------------------------------

    #[test]
    fn test_external_root_default() {
        let adapter = make_duckdb_adapter();
        let result = dispatch_test(&adapter, "external_root", &[]).unwrap();
        assert_eq!(result.as_str().unwrap(), ".");
    }

    // TODO: test external_root with custom config once MockAdapter supports custom AdapterConfig

    // -- external_write_options tests (ported from dbt-duckdb test_external_utils.py) --

    #[test]
    fn test_external_write_options_csv_inferred() {
        let adapter = make_duckdb_adapter();
        let result = dispatch_test(
            &adapter,
            "external_write_options",
            &[Value::from("/tmp/test.csv"), dict(&[])],
        )
        .unwrap();
        assert_eq!(result.as_str().unwrap(), "format csv, header 1");
    }

    #[test]
    fn test_external_write_options_parquet_with_codec() {
        let adapter = make_duckdb_adapter();
        let result = dispatch_test(
            &adapter,
            "external_write_options",
            &[Value::from("./foo.parquet"), dict(&[("codec", "zstd")])],
        )
        .unwrap();
        assert_eq!(result.as_str().unwrap(), "codec zstd, format parquet");
    }

    #[test]
    fn test_external_write_options_delimiter_infers_csv() {
        let adapter = make_duckdb_adapter();
        let result = dispatch_test(
            &adapter,
            "external_write_options",
            &[
                Value::from("bar"),
                dict(&[("delimiter", "|"), ("header", "0")]),
            ],
        )
        .unwrap();
        assert_eq!(
            result.as_str().unwrap(),
            "delimiter '|', header 0, format csv"
        );
    }

    #[test]
    fn test_external_write_options_partition_by_single() {
        let adapter = make_duckdb_adapter();
        let result = dispatch_test(
            &adapter,
            "external_write_options",
            &[Value::from("a.parquet"), dict(&[("partition_by", "ds")])],
        )
        .unwrap();
        assert_eq!(result.as_str().unwrap(), "partition_by ds, format parquet");
    }

    #[test]
    fn test_external_write_options_partition_by_multi_adds_parens() {
        let adapter = make_duckdb_adapter();
        let result = dispatch_test(
            &adapter,
            "external_write_options",
            &[
                Value::from("b.csv"),
                dict(&[("partition_by", "ds,category")]),
            ],
        )
        .unwrap();
        assert_eq!(
            result.as_str().unwrap(),
            "partition_by (ds,category), format csv, header 1"
        );
    }

    #[test]
    fn test_external_write_options_null_quoted() {
        let adapter = make_duckdb_adapter();
        let result = dispatch_test(
            &adapter,
            "external_write_options",
            &[Value::from("/path/to/c.csv"), dict(&[("null", "\\N")])],
        )
        .unwrap();
        assert_eq!(result.as_str().unwrap(), "null '\\N', format csv, header 1");
    }

    // -- external_read_location tests (ported from dbt-duckdb test_external_utils.py) --

    #[test]
    fn test_external_read_location_no_partition() {
        let adapter = make_duckdb_adapter();
        let result = dispatch_test(
            &adapter,
            "external_read_location",
            &[
                Value::from("bar"),
                dict(&[("format", "csv"), ("delimiter", "|"), ("header", "0")]),
            ],
        )
        .unwrap();
        assert_eq!(result.as_str().unwrap(), "bar");
    }

    #[test]
    fn test_external_read_location_single_partition() {
        let adapter = make_duckdb_adapter();
        let result = dispatch_test(
            &adapter,
            "external_read_location",
            &[
                Value::from("/tmp/a"),
                dict(&[("partition_by", "ds"), ("format", "parquet")]),
            ],
        )
        .unwrap();
        assert_eq!(result.as_str().unwrap(), "/tmp/a/*/*.parquet");
    }

    #[test]
    fn test_external_read_location_multi_partition() {
        let adapter = make_duckdb_adapter();
        let result = dispatch_test(
            &adapter,
            "external_read_location",
            &[Value::from("b"), dict(&[("partition_by", "ds,category")])],
        )
        .unwrap();
        assert_eq!(result.as_str().unwrap(), "b/*/*/*.parquet");
    }

    // -- location_exists tests ------------------------------------------------

    #[test]
    fn test_location_exists_parse_mode_returns_false() {
        let adapter = make_duckdb_parse_adapter();
        let result = dispatch_test(
            &adapter,
            "location_exists",
            &[Value::from("/nonexistent/path")],
        )
        .unwrap();
        // Parse-mode adapter always returns false
        assert_eq!(result, Value::from(false));
    }
}
