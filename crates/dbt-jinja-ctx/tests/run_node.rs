//! Tests covering `RunNodeCtx` end-to-end:
//!
//! 1. The typed ctx serializes to exactly the same key set today's
//!    `build_run_node_context<S>` BTreeMap produces — 18 base keys plus the
//!    optional `pre_hooks` / `post_hooks` / `load_agate_table` (skipped via
//!    `#[serde(skip_serializing_if = "Option::is_none")]` when absent).
//! 2. `Option<MinijinjaValue>` fields with `None` value MUST be omitted
//!    from the registered map (matching today's "only insert if present"
//!    semantic).
//! 3. `model` / `builtins` downcast contracts hold (same as compile-node).
//! 4. The `RunNodeCtx` JsonSchema has stable shape (snapshot test).

use std::collections::{BTreeMap, BTreeSet};

use dbt_jinja_ctx::{JinjaObject, MacroLookupContext, RunNodeCtx, to_jinja_btreemap};
use minijinja::Value as MinijinjaValue;
use minijinja::machinery::Span;

fn fixture_run_node_ctx(
    pre_hooks: Option<MinijinjaValue>,
    post_hooks: Option<MinijinjaValue>,
    load_agate_table: Option<MinijinjaValue>,
) -> RunNodeCtx {
    let mut model_inner: BTreeMap<String, MinijinjaValue> = BTreeMap::new();
    model_inner.insert(
        "name".to_string(),
        MinijinjaValue::from("dbt_columns".to_string()),
    );

    let mut builtins_inner: BTreeMap<String, MinijinjaValue> = BTreeMap::new();
    builtins_inner.insert("ref".to_string(), MinijinjaValue::from("ref-fn-stub"));
    builtins_inner.insert("config".to_string(), MinijinjaValue::from("config-stub"));

    RunNodeCtx {
        this: MinijinjaValue::from("this-stub"),
        database: "analytics".to_string(),
        schema: "public".to_string(),
        identifier: "dbt_columns".to_string(),
        pre_hooks,
        post_hooks,
        config: MinijinjaValue::from("run-config-stub"),
        model: MinijinjaValue::from_object(model_inner.clone()),
        node: MinijinjaValue::from_object(model_inner),
        connection_name: String::new(),
        store_result: MinijinjaValue::from("store-result-stub"),
        load_result: MinijinjaValue::from("load-result-stub"),
        store_raw_result: MinijinjaValue::from("store-raw-result-stub"),
        submit_python_job: MinijinjaValue::from("submit-python-job-stub"),
        context: JinjaObject::new(MacroLookupContext {
            root_project_name: "my_project".to_string(),
            current_project_name: None,
            packages: BTreeSet::new(),
        }),
        write: MinijinjaValue::from("write-stub"),
        load_agate_table,
        builtins: MinijinjaValue::from_object(builtins_inner),
        target_package_name: "my_project".to_string(),
        current_path: "run/dbt_columns.sql".to_string(),
        current_span: MinijinjaValue::from_serialize(Span::default()),
    }
}

#[test]
fn run_node_ctx_serializes_to_expected_keys_minimal() {
    let ctx = fixture_run_node_ctx(None, None, None);
    let registered = to_jinja_btreemap(&ctx);
    let mut keys: Vec<&str> = registered.keys().map(|s| s.as_str()).collect();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            "TARGET_PACKAGE_NAME",
            "__minijinja_current_path",
            "__minijinja_current_span",
            "builtins",
            "config",
            "connection_name",
            "context",
            "database",
            "identifier",
            "load_result",
            "model",
            "node",
            "schema",
            "store_raw_result",
            "store_result",
            "submit_python_job",
            "this",
            "write",
        ],
        "run-node ctx must omit pre_hooks/post_hooks/load_agate_table when None \
         (matches the 'insert only if present' semantic of the BTreeMap-based \
         build_run_node_context)"
    );
}

#[test]
fn run_node_ctx_serializes_with_optionals() {
    let ctx = fixture_run_node_ctx(
        Some(MinijinjaValue::from("pre-hooks-stub")),
        Some(MinijinjaValue::from("post-hooks-stub")),
        Some(MinijinjaValue::from("load-agate-table-stub")),
    );
    let registered = to_jinja_btreemap(&ctx);
    assert!(registered.contains_key("pre_hooks"));
    assert!(registered.contains_key("post_hooks"));
    assert!(registered.contains_key("load_agate_table"));
}

#[test]
fn builtins_downcasts_to_btreemap_string_value() {
    let ctx = fixture_run_node_ctx(None, None, None);
    let registered = to_jinja_btreemap(&ctx);
    let builtins = registered.get("builtins").unwrap();
    let downcast = builtins
        .as_object()
        .and_then(|obj| obj.downcast::<BTreeMap<String, MinijinjaValue>>());
    assert!(downcast.is_some());
}

#[test]
fn run_node_ctx_json_schema_snapshot() {
    let schema = schemars::schema_for!(RunNodeCtx);
    insta::assert_json_snapshot!("run_node_ctx_schema", schema);
}
