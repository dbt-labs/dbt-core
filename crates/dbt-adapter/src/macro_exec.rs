use crate::errors::{AdapterError, AdapterErrorKind};
use crate::response::ResultObject;

use arrow::array::RecordBatch;
use dbt_agate::AgateTable;
use minijinja::{State, Value};

use std::error::Error;
use std::sync::Arc;

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
