use std::rc::Rc;
use std::sync::Arc;

use dbt_adapter_core::AdapterType;
use minijinja::listener::RenderingEventListener;
use minijinja::value::Object;
use minijinja::{Environment, Error, ErrorKind, State, Value, context};
use minijinja_contrib::testing::jinja_assert;
use serde_json::{Value as JsonValue, json};

use super::QueryCommentConfig;

#[derive(Debug)]
struct Target;

impl Object for Target {
    fn call_method(
        self: &Arc<Self>,
        _state: &State<'_, '_>,
        name: &str,
        args: &[Value],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, Error> {
        if name != "get" {
            return Err(Error::new(
                ErrorKind::UnknownMethod,
                format!("Unknown method on Target: {name}"),
            ));
        }
        Ok(match args.first().and_then(Value::as_str) {
            Some("profile_name") => Value::from("invocation_id_query_comment"),
            Some("target_name") => Value::from("conformance"),
            _ => Value::UNDEFINED,
        })
    }
}

#[test]
fn test_target_jinja_contract() {
    jinja_assert(
        Target,
        "
        profile_name: {{ obj.get('profile_name') }}
        target_name: {{ obj.get('target_name') }}
        missing_is_undefined: {{ obj.get('missing') is undefined }}
        ",
        "
        profile_name: invocation_id_query_comment
        target_name: conformance
        missing_is_undefined: True
        ",
    );

    let mut env = Environment::new();
    env.add_global("obj", Value::from_object(Target));
    let error = env
        .render_str("{{ obj.unsupported() }}", (), &[])
        .expect_err("unsupported Target methods should fail in Jinja");
    assert_eq!(error.kind(), ErrorKind::UnknownMethod);
    assert!(
        error
            .to_string()
            .contains("Unknown method on Target: unsupported")
    );
}

fn render_default_comment(
    invocation_id: &str,
    node_id: Option<&str>,
    connection_name: &str,
) -> JsonValue {
    let config = QueryCommentConfig::from_query_comment(None, AdapterType::Databricks, true, None);
    let mut env = Environment::new();
    env.add_function("return", |value: Value| value);
    env.add_function("tojson", |value: Value| -> Result<String, Error> {
        serde_json::to_string(&value)
            .map_err(|error| Error::new(ErrorKind::InvalidOperation, error.to_string()))
    });
    let node = node_id.map(|unique_id| json!({ "unique_id": unique_id }));
    let rendered = env
        .render_str(
            &config.comment,
            context! {
                dbt_version => "2.0.0",
                target => Value::from_object(Target),
                invocation_id => invocation_id,
                node => node,
                connection_name => connection_name,
            },
            &[],
        )
        .expect("Databricks default query comment should render");
    serde_json::from_str(rendered.trim())
        .expect("Databricks default query comment should render valid JSON")
}

#[test]
fn test_databricks_default_query_comment_includes_invocation_id_with_node() {
    let comment = render_default_comment(
        "11111111-2222-4333-8444-555555555555",
        Some("model.invocation_id_query_comment.probe"),
        "model.invocation_id_query_comment.probe",
    );

    assert_eq!(
        comment["invocation_id"],
        "11111111-2222-4333-8444-555555555555"
    );
    assert_eq!(
        comment["node_id"],
        "model.invocation_id_query_comment.probe"
    );
    assert!(!comment.as_object().unwrap().contains_key("connection_name"));
}

#[test]
fn test_databricks_default_query_comment_includes_invocation_id_without_node() {
    let comment = render_default_comment(
        "11111111-2222-4333-8444-555555555555",
        None,
        "macro_invocation_id_query_comment_non_node",
    );

    assert_eq!(
        comment["invocation_id"],
        "11111111-2222-4333-8444-555555555555"
    );
    assert_eq!(
        comment["connection_name"],
        "macro_invocation_id_query_comment_non_node"
    );
    assert!(!comment.as_object().unwrap().contains_key("node_id"));
}

#[test]
fn test_databricks_default_query_comment_reuses_context_invocation_id() {
    let invocation_id = "11111111-2222-4333-8444-555555555555";
    let first = render_default_comment(
        invocation_id,
        None,
        "macro_invocation_id_query_comment_non_node",
    );
    let second = render_default_comment(
        invocation_id,
        None,
        "macro_invocation_id_query_comment_non_node",
    );

    assert_eq!(first["invocation_id"], second["invocation_id"]);
}
