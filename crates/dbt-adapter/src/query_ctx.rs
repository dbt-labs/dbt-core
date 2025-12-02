//! Util methods for creating query context.

use crate::errors::AdapterResult;

use dbt_common::adapter::DBT_EXECUTION_PHASES;
use dbt_schemas::schemas::{DbtModel, DbtSeed, DbtSnapshot, DbtTest, DbtUnitTest};
use dbt_xdbc::QueryCtx;
use minijinja::{
    Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, State,
    constants::CURRENT_EXECUTION_PHASE,
};
use serde::Deserialize;

/// Create a new instance from the current jinja state.
pub fn query_ctx_from_state(state: &State) -> AdapterResult<QueryCtx> {
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
    let mut query = QueryCtx::default();
    // TODO: use node_metadata_from_state
    if let Some(node_id) = node_id_from_state(state) {
        query = query.with_node_id(node_id);
    }
    if let Some(phase) = execution_phase_from_state(state) {
        query = query.with_phase(phase);
    }
    Ok(query)
}

pub fn node_id_from_state(state: &State) -> Option<String> {
    let node = state.lookup("model").as_ref()?.clone();
    // all deserialization must go through yaml value
    // should this be a .ok?
    let yaml_node = dbt_serde_yaml::to_value(&node)
        .map_err(|e| MinijinjaError::new(MinijinjaErrorKind::SerdeDeserializeError, e.to_string()))
        .ok()?;

    // Try to deserialize as different node types
    if let Ok(model) = DbtModel::deserialize(&yaml_node) {
        Some(model.__common_attr__.unique_id)
    } else if let Ok(test) = DbtTest::deserialize(&yaml_node) {
        Some(test.__common_attr__.unique_id)
    } else if let Ok(snapshot) = DbtSnapshot::deserialize(&yaml_node) {
        Some(snapshot.__common_attr__.unique_id)
    } else if let Ok(seed) = DbtSeed::deserialize(&yaml_node) {
        Some(seed.__common_attr__.unique_id)
    } else if let Ok(unit_test) = DbtUnitTest::deserialize(&yaml_node) {
        Some(unit_test.__common_attr__.unique_id)
    } else {
        None
    }
}

pub fn execution_phase_from_state(state: &State) -> Option<&'static str> {
    let value = state.lookup(CURRENT_EXECUTION_PHASE)?;
    let s = value.as_str()?;
    DBT_EXECUTION_PHASES
        .iter()
        .position(|&p| p == s)
        .map(|idx| DBT_EXECUTION_PHASES[idx])
}
