use std::collections::BTreeMap;

use dbt_adapter::relation::RelationObject;
use dbt_adapter_core::AdapterType;
use dbt_schemas::dbt_types::RelationType;
use minijinja::Value;

use crate::macro_test_harness::MacroTestHarness;

fn build_harness() -> MacroTestHarness {
    MacroTestHarness::for_adapter(AdapterType::Bigquery)
        .load_all_macros()
        .with_stub_functions()
        .build()
        .expect("harness should build")
}

fn base_relation_ctx(harness: &MacroTestHarness) -> BTreeMap<String, Value> {
    let relation = harness.relation(
        "test-db",
        "test_schema",
        "member_snapshot",
        Some(RelationType::Table),
    );
    BTreeMap::from([
        (
            "base_relation".to_string(),
            RelationObject::new(relation).into_value(),
        ),
        // `make_temp_relation` (dbt-adapters/macros/adapters/relation.sql) reads
        // `model.batch` before dispatching; a real dbt `model` always has this key
        // (None for non-microbatch models), so mirror that shape here.
        (
            "model".to_string(),
            Value::from_serialize(BTreeMap::from([("batch", Value::from(()))])),
        ),
    ])
}

#[test]
fn make_temp_relation_appends_unique_suffix() {
    // dbt Core's dbt-bigquery adapter appends a `strftime("%H%M%S%f")` suffix to
    // `__dbt_tmp` temp relations (via `bigquery__make_relation_with_suffix`) to avoid
    // collisions across concurrent runs. Fusion previously fell back to the generic
    // `default__make_temp_relation`, which reused the bare `__dbt_tmp` identifier and
    // caused a conformance SQL mismatch against dbt Core (#8247).
    let harness = build_harness();
    let ctx = base_relation_ctx(&harness);

    let rendered = harness
        .render("{{ make_temp_relation(base_relation).identifier }}", ctx)
        .expect("render should succeed");
    let rendered = rendered.trim();

    let suffix = rendered
        .strip_prefix("member_snapshot__dbt_tmp")
        .unwrap_or_else(|| panic!("expected __dbt_tmp-prefixed identifier, got: {rendered:?}"));
    assert!(
        !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit()),
        "BigQuery temp relation should append an all-digit timestamp suffix, got: {rendered:?}"
    );
}

#[test]
fn make_intermediate_relation_does_not_append_suffix() {
    // Unlike `make_temp_relation`, the intermediate/backup relation paths
    // (`dstring=False`) must keep the bare suffix untouched.
    let harness = build_harness();
    let ctx = base_relation_ctx(&harness);

    let rendered = harness
        .render(
            "{{ make_intermediate_relation(base_relation).identifier }}",
            ctx,
        )
        .expect("render should succeed");

    assert_eq!(rendered.trim(), "member_snapshot__dbt_tmp");
}
