use std::collections::BTreeMap;

use dbt_adapter_core::AdapterType;
use minijinja::Value;

use crate::macro_test_harness::MacroTestHarness;

fn harness() -> MacroTestHarness {
    MacroTestHarness::for_adapter(AdapterType::MotherDuck)
        .load_all_macros()
        .with_stub_functions()
        .build()
        .expect("MotherDuck macro harness should build")
}

#[test]
fn dispatch_uses_motherduck_override_then_duckdb_parent() {
    let harness = harness();
    let batch_size = harness
        .render(
            "{{ adapter.dispatch('get_batch_size', 'dbt')() }}",
            BTreeMap::<String, Value>::new(),
        )
        .expect("MotherDuck seed override should resolve");
    assert_eq!(batch_size.trim(), "500");

    let timestamp = harness
        .render(
            "{{ adapter.dispatch('current_timestamp', 'dbt')() }}",
            BTreeMap::<String, Value>::new(),
        )
        .expect("DuckDB parent macro should resolve");
    assert_eq!(timestamp.trim(), "now()");
}

#[test]
fn create_table_as_rejects_python_models() {
    let err = harness()
        .render(
            "{{ motherduck__create_table_as(false, none, 'pass', 'python') }}",
            BTreeMap::<String, Value>::new(),
        )
        .expect_err("Python models must be rejected");
    assert!(err.to_string().contains("Python models are not supported"));
}
