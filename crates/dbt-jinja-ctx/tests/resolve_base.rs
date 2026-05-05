//! Tests covering `ResolveBaseCtx` end-to-end:
//!
//! 1. The typed ctx serializes to the same key set today's hand-built
//!    `build_resolve_context` BTreeMap produces — six fixed keys plus one
//!    top-level entry per `dbt_namespace` (via `#[serde(flatten)]`).
//! 2. The `dbt_namespaces` flatten emits each namespace as its own
//!    top-level Jinja key (not as a nested `dbt_namespaces.foo` path).
//! 3. The `ResolveBaseCtx` JsonSchema has stable shape (snapshot test).

use std::collections::BTreeMap;

use dbt_jinja_ctx::{ResolveBaseCtx, to_jinja_btreemap};
use minijinja::Value as MinijinjaValue;

fn fixture_resolve_base_ctx() -> ResolveBaseCtx {
    let mut macro_dispatch_order: BTreeMap<String, MinijinjaValue> = BTreeMap::new();
    macro_dispatch_order.insert(
        "dbt".to_string(),
        MinijinjaValue::from(vec!["dbt".to_string()]),
    );

    let mut dbt_namespaces: BTreeMap<String, MinijinjaValue> = BTreeMap::new();
    dbt_namespaces.insert("dbt".to_string(), MinijinjaValue::from("dbt-ns-stub"));
    dbt_namespaces.insert(
        "snowflake".to_string(),
        MinijinjaValue::from("snowflake-ns-stub"),
    );

    ResolveBaseCtx {
        doc: MinijinjaValue::from("doc-stub"),
        macro_dispatch_order,
        target_package_name: "my_project".to_string(),
        execute: false,
        node: MinijinjaValue::NONE,
        connection_name: String::new(),
        dbt_namespaces,
    }
}

#[test]
fn resolve_base_ctx_serializes_to_expected_keys() {
    let ctx = fixture_resolve_base_ctx();
    let registered = to_jinja_btreemap(&ctx);
    let mut keys: Vec<&str> = registered.keys().map(|s| s.as_str()).collect();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            "MACRO_DISPATCH_ORDER",
            "TARGET_PACKAGE_NAME",
            "connection_name",
            "dbt",
            "doc",
            "execute",
            "node",
            "snowflake",
        ],
        "resolve-base ctx must produce the six base keys plus one entry per \
         dbt_namespace via #[serde(flatten)]"
    );
}

#[test]
fn dbt_namespaces_flatten_to_top_level_keys() {
    let ctx = fixture_resolve_base_ctx();
    let registered = to_jinja_btreemap(&ctx);

    // The map field name `dbt_namespaces` MUST NOT appear — it's flattened
    // into per-namespace top-level keys.
    assert!(
        !registered.contains_key("dbt_namespaces"),
        "`dbt_namespaces` flatten leaked the field name as a top-level key"
    );
    assert_eq!(
        registered
            .get("dbt")
            .and_then(|v| v.as_str().map(|s| s.to_string())),
        Some("dbt-ns-stub".to_string()),
    );
    assert_eq!(
        registered
            .get("snowflake")
            .and_then(|v| v.as_str().map(|s| s.to_string())),
        Some("snowflake-ns-stub".to_string()),
    );
}

#[test]
fn execute_is_false_at_resolve_base() {
    let ctx = fixture_resolve_base_ctx();
    let registered = to_jinja_btreemap(&ctx);
    let execute = registered
        .get("execute")
        .expect("execute must be present at resolve-base scope");
    assert!(
        !execute.is_true(),
        "execute must be false at resolve-base; flips to true at compile/run"
    );
}

#[test]
fn node_is_none_at_resolve_base() {
    let ctx = fixture_resolve_base_ctx();
    let registered = to_jinja_btreemap(&ctx);
    let node = registered
        .get("node")
        .expect("node must be present at resolve-base scope");
    assert!(
        node.is_none(),
        "node must be Value::NONE at base; populated per-model"
    );
}

#[test]
fn resolve_base_ctx_json_schema_snapshot() {
    let schema = schemars::schema_for!(ResolveBaseCtx);
    insta::assert_json_snapshot!("resolve_base_ctx_schema", schema);
}

/// Regression test: minijinja's dispatch lookup
/// (`fs/sa/crates/dbt-jinja/minijinja/src/dispatch_object.rs`) does
/// `order.downcast_object::<Vec<String>>()` to read the per-namespace search
/// order from `MACRO_DISPATCH_ORDER`. The underlying Object type therefore
/// MUST be exactly `Vec<String>` — not the `MutableVec<Value>` that
/// serde-serializing a `Vec<String>` would produce. This test asserts the
/// downcast still works after the typed-ctx round-trip; if it ever flips
/// back to a serde-serialized `Vec<String>` field, this fails fast and
/// the dispatch_logic_test goldie wouldn't have to.
#[test]
fn macro_dispatch_order_values_downcast_to_vec_string() {
    let ctx = fixture_resolve_base_ctx();
    let registered = to_jinja_btreemap(&ctx);
    let order_value = registered
        .get("MACRO_DISPATCH_ORDER")
        .expect("MACRO_DISPATCH_ORDER must be registered");
    let order_obj = order_value
        .as_object()
        .expect("MACRO_DISPATCH_ORDER must be an Object map");
    let dbt_order = order_obj
        .get_value(&MinijinjaValue::from("dbt"))
        .expect("dbt namespace must have an entry in MACRO_DISPATCH_ORDER");
    let downcast = dbt_order
        .as_object()
        .and_then(|obj| obj.downcast::<Vec<String>>());
    assert!(
        downcast.is_some(),
        "MACRO_DISPATCH_ORDER per-namespace value must downcast to \
         `Vec<String>` (the type minijinja's dispatch lookup expects)"
    );
    assert_eq!(downcast.unwrap().as_ref(), &vec!["dbt".to_string()]);
}
