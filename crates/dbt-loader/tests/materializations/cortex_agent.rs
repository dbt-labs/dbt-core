use std::collections::BTreeMap;

use dbt_adapter_core::AdapterType;
use minijinja::Value;

use crate::macro_test_harness::{
    MacroTestHarness, assert_executed_contains, default_mock_config, executed_sql,
};

const ADAPTER: AdapterType = AdapterType::Snowflake;
const MACRO: &str = "materialization_cortex_agent_snowflake";
const SPEC: &str = "models:\n  orchestration: claude-4-sonnet\n";

fn build_harness() -> MacroTestHarness {
    MacroTestHarness::for_adapter(ADAPTER)
        .load_all_macros()
        .with_stub_functions()
        .build()
        .expect("harness should build")
}

/// A `config` mock that answers `config.meta_get('comment'|'profile')`.
///
/// `None` is returned as Jinja `none` (`Value::from(())`) so the
/// materialization's `is none` fallback to `config.get(...)` is exercised.
/// `config.get` itself comes from [`default_mock_config`] (returns the
/// caller-supplied default).
fn agent_config(comment: Option<Value>, profile: Option<Value>) -> Value {
    let mock = default_mock_config();
    mock.on("meta_get", move |args| {
        let key = args.first().and_then(|v| v.as_str());
        match key {
            Some("comment") => Ok(comment.clone().unwrap_or(Value::from(()))),
            Some("profile") => Ok(profile.clone().unwrap_or(Value::from(()))),
            _ => Ok(Value::from(())),
        }
    });
    Value::from_dyn_object(mock)
}

fn render(harness: &MacroTestHarness, ctx: BTreeMap<String, Value>) -> dbt_common::FsResult<String> {
    harness.render(&format!("{{{{ {MACRO}() }}}}"), ctx)
}

/// With meta.comment and meta.profile set, the emitted DDL carries both
/// optional clauses plus the FROM SPECIFICATION body.
#[test]
fn create_agent_with_comment_and_profile() {
    let harness = build_harness();
    let profile = Value::from_serialize(BTreeMap::from([
        ("display_name".to_string(), Value::from("Analyst")),
        ("color".to_string(), Value::from("blue")),
    ]));
    let ctx = harness
        .materialization_context("my_agent", SPEC)
        .config(agent_config(Some(Value::from("Domain analyst")), Some(profile)))
        .build();

    render(&harness, ctx).expect("cortex_agent materialization should render");

    let mock = harness.mock();
    assert_executed_contains(mock, "create or replace agent");
    assert_executed_contains(mock, "comment = 'domain analyst'");
    assert_executed_contains(mock, "profile = '");
    assert_executed_contains(mock, "display_name");
    assert_executed_contains(mock, "from specification $$");
}

/// Single quotes in the comment are doubled so the SQL string literal is valid.
#[test]
fn comment_single_quotes_are_escaped() {
    let harness = build_harness();
    let ctx = harness
        .materialization_context("my_agent", SPEC)
        .config(agent_config(Some(Value::from("dbt's agent")), None))
        .build();

    render(&harness, ctx).expect("cortex_agent materialization should render");

    assert_executed_contains(harness.mock(), "comment = 'dbt''s agent'");
}

/// Without meta, the optional COMMENT / PROFILE clauses are omitted entirely.
#[test]
fn create_agent_without_meta_omits_optional_clauses() {
    let harness = build_harness();
    let ctx = harness
        .materialization_context("my_agent", SPEC)
        .config(agent_config(None, None))
        .build();

    render(&harness, ctx).expect("cortex_agent materialization should render");

    let sqls = executed_sql(harness.mock()).join("\n").to_lowercase();
    assert!(
        sqls.contains("create or replace agent"),
        "expected CREATE OR REPLACE AGENT, got: {sqls}"
    );
    assert!(
        !sqls.contains("comment ="),
        "COMMENT clause should be omitted when no comment is set, got: {sqls}"
    );
    assert!(
        !sqls.contains("profile ="),
        "PROFILE clause should be omitted when no profile is set, got: {sqls}"
    );
}

/// An empty model body is a hard compiler error (no agent has an empty spec).
#[test]
fn empty_specification_raises() {
    let harness = build_harness();
    let ctx = harness
        .materialization_context("my_agent", "   ")
        .config(agent_config(None, None))
        .build();

    let result = render(&harness, ctx);
    assert!(
        result.is_err(),
        "empty specification should raise a compiler error, got: {result:?}"
    );
}
