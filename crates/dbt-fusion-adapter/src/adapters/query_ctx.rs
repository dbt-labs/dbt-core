//! Util methods for creating query context.

use crate::adapters::errors::{AdapterError, AdapterErrorKind, AdapterResult};

use dbt_schemas::schemas::manifest::{DbtModel, DbtSeed, DbtSnapshot, DbtTest, DbtUnitTest};
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

    let query = QueryCtx::new(dialect_str);
    // TODO: use node_metadata_from_state
    match state.lookup("model") {
        // TODO: This is a hack to get the node id from the state, but for now good enough
        Some(node) => {
            // Try to deserialize as different node types
            if let Ok(model) = DbtModel::deserialize(&node) {
                Ok(query.with_node_id(model.common_attr.unique_id))
            } else if let Ok(test) = DbtTest::deserialize(&node) {
                Ok(query.with_node_id(test.common_attr.unique_id))
            } else if let Ok(snapshot) = DbtSnapshot::deserialize(&node) {
                Ok(query.with_node_id(snapshot.common_attr.unique_id))
            } else if let Ok(seed) = DbtSeed::deserialize(&node) {
                Ok(query.with_node_id(seed.common_attr.unique_id))
            } else if let Ok(unit_test) = DbtUnitTest::deserialize(&node) {
                Ok(query.with_node_id(unit_test.common_attr.unique_id))
            } else {
                // TODO: The following should really be an error, but
                // our tests (functional tests in particular) do not
                // set anything about model in the state.
                Ok(query)
            }
        }
        None => {
            // TODO: The following should be an error but there
            // are tests that do not include model.
            //return Err(AdapterError::new(
            //AdapterErrorKind::Configuration,
            //"Missing model in the state",
            //));
            Ok(query)
        }
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
