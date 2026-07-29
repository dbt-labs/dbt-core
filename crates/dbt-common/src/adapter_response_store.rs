//! Side-channel for adapter responses flowing from Jinja `store_result` into
//! `Stat` / `run_results.json`.
//!
//! `store_result` runs inside the Minijinja materialization macro and only has
//! access to the current `NodeEvaluated` span. The full Core-compatible
//! `adapter_response` map (query_id, code, `_message`, Snowflake DML stats) is
//! richer than the few fields on that span, so we stash it here keyed by
//! `unique_id` and drain it when building the node's `Stat`.

use std::collections::BTreeMap;

use dashmap::DashMap;
use once_cell::sync::Lazy;

use dbt_yaml::Value as YmlValue;

static PENDING_ADAPTER_RESPONSES: Lazy<DashMap<String, BTreeMap<String, YmlValue>>> =
    Lazy::new(DashMap::new);

/// Record (or overwrite) the adapter_response map for a node.
///
/// Prefer calling this only for the materialization's `"main"` statement so
/// helper queries (e.g. `run_query`) do not clobber the real DML response.
pub fn record_adapter_response(unique_id: impl Into<String>, response: BTreeMap<String, YmlValue>) {
    PENDING_ADAPTER_RESPONSES.insert(unique_id.into(), response);
}

/// Take the pending adapter_response for a node, if any.
pub fn take_adapter_response(unique_id: &str) -> BTreeMap<String, YmlValue> {
    PENDING_ADAPTER_RESPONSES
        .remove(unique_id)
        .map(|(_, v)| v)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_and_take_roundtrip() {
        let mut map = BTreeMap::new();
        map.insert(
            "rows_affected".to_string(),
            dbt_yaml::to_value(1i64).unwrap(),
        );
        record_adapter_response("model.x.y", map.clone());
        assert_eq!(take_adapter_response("model.x.y"), map);
        assert!(take_adapter_response("model.x.y").is_empty());
    }
}
