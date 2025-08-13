//! Util methods for creating query context.

use crate::errors::{AdapterError, AdapterErrorKind, AdapterResult};

use dbt_schemas::schemas::{DbtModel, DbtSeed, DbtSnapshot, DbtTest, DbtUnitTest};
use dbt_xdbc::QueryCtx;
use minijinja::State;
use serde::Deserialize;

/// Create a new instance from the current jinja state.
pub fn query_ctx_from_state(state: &State) -> AdapterResult<QueryCtx> {
    let dialect_val = state.lookup("dialect").ok_or_else(|| {
        AdapterError::new(
            AdapterErrorKind::Configuration,
            "Missing dialect in the state",
        )
    })?;

    let dialect_str = dialect_val.as_str().ok_or_else(|| {
        AdapterError::new(
            AdapterErrorKind::Configuration,
            "Cannot cast dialect to a string",
        )
    })?;

    // TODO: The following should really be an error, but
    // our tests (functional tests in particular) do not
    // set anything about model in the state.
    //
    // TODO: The following should be an error but there
    // are tests that do not include model.
    //return Err(AdapterError::new(
    //AdapterErrorKind::Configuration,
    //"Missing model in the state",
    //));
    let query = QueryCtx::new(dialect_str);
    // TODO: use node_metadata_from_state
    if let Some(node_id) = node_id_from_state(state) {
        Ok(query.with_node_id(node_id))
    } else {
        Ok(query)
    }
}

pub fn node_id_from_state(state: &State) -> Option<String> {
    let node = state.lookup("model").as_ref()?.clone();
    // Try to deserialize as different node types
    if let Ok(model) = DbtModel::deserialize(&node) {
        Some(model.common_attr.unique_id)
    } else if let Ok(test) = DbtTest::deserialize(&node) {
        Some(test.common_attr.unique_id)
    } else if let Ok(snapshot) = DbtSnapshot::deserialize(&node) {
        Some(snapshot.common_attr.unique_id)
    } else if let Ok(seed) = DbtSeed::deserialize(&node) {
        Some(seed.common_attr.unique_id)
    } else if let Ok(unit_test) = DbtUnitTest::deserialize(&node) {
        Some(unit_test.common_attr.unique_id)
    } else {
        None
    }
}

/// Create a new instance from the current jinja state and given
/// sql.
pub fn query_ctx_from_state_with_sql(
    state: &State,
    sql: impl Into<String>,
) -> AdapterResult<QueryCtx> {
    match query_ctx_from_state(state) {
        Ok(query_ctx) => Ok(query_ctx.with_sql(sql)),
        Err(err) => Err(err),
    }
}
