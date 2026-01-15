use insta::assert_snapshot;
use minijinja::{
    constants::MACRO_NAMESPACE_REGISTRY,
    context,
    dispatch_object::DispatchObject,
    listener::RenderingEventListener,
    value::{mutable_vec::MutableVec, Object, ValueMap},
    Environment, Error as MinijinjaError, State, Value,
};
use std::rc::Rc;
use std::sync::Arc;

/// Test namespace object that looks up macros in the namespace registry
#[derive(Debug)]
struct TestNamespace {
    name: String,
}

impl Object for TestNamespace {
    fn get_property(
        self: &Arc<Self>,
        state: &State<'_, '_>,
        name: &str,
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, MinijinjaError> {
        let ns_name = Value::from(self.name.clone());
        let namespace_registry = state
            .env()
            .get_macro_namespace_registry()
            .unwrap_or_default();
        if namespace_registry.get(&ns_name).is_some_and(|val| {
            val.try_iter()
                .map(|mut iter| iter.any(|v| v.as_str() == Some(name)))
                .unwrap_or(false)
        }) {
            Ok(Value::from_object(DispatchObject {
                macro_name: (*name).to_string(),
                package_name: Some(self.name.clone()),
                strict: true,
                auto_execute: false,
                context: Some(state.get_base_context()),
            }))
        } else {
            Ok(Value::UNDEFINED)
        }
    }
}

#[test]
fn test_set_unwarp() {
    let env = Environment::new();
    let rv = env
        .render_str(
            r#"
    {% set fqn = ["one","two","three"] %}
    {%- set a, b, c = fqn[0], fqn[1], fqn[2] %}
    {{ a }}|{{ b }}|{{ c }}
    "#,
            context! {},
            &[],
        )
        .unwrap();
    assert_snapshot!(rv, @"one|two|three");
}

#[test]
fn test_set_append() {
    let env = Environment::new();
    let rv = env
        .render_str(
            r#"
    {%- set my_list = ['x'] -%}
{{ my_list.append('y') }}
{{ my_list }}
    "#,
            context! {},
            &[],
        )
        .unwrap();
    // would be None in dbt-core but this should be just cosmetic
    assert_snapshot!(rv, @r"
    None
    ['x', 'y']
    ");
}

#[test]
fn test_macro_namespace_lookup() {
    let mut env = Environment::new();
    let mut macro_namespace_registry = ValueMap::new();
    macro_namespace_registry.insert(
        Value::from("test_2"),
        Value::from_object(MutableVec::from(vec![Value::from("two")])),
    );
    macro_namespace_registry.insert(
        Value::from("test_1"),
        Value::from_object(MutableVec::from(vec![Value::from("another")])),
    );

    env.add_global(
        MACRO_NAMESPACE_REGISTRY,
        Value::from_object(macro_namespace_registry),
    );
    let _ = env.add_template("test_2.two", "{% macro two() %}two{% endmacro %}");

    // Add namespace objects to context
    let test_1 = Value::from_object(TestNamespace {
        name: "test_1".to_string(),
    });
    let test_2 = Value::from_object(TestNamespace {
        name: "test_2".to_string(),
    });

    let rv = env
        .render_str(
            r#"
    {% set m = test_1.one or test_2.two %}
    {{ m() }}
        "#,
            context! { test_1, test_2 },
            &[],
        )
        .unwrap();
    assert_snapshot!(rv, @"two");

    let test_1 = Value::from_object(TestNamespace {
        name: "test_1".to_string(),
    });
    let test_2 = Value::from_object(TestNamespace {
        name: "test_2".to_string(),
    });
    let rv = env
        .render_str(
            r#"
    {% set m = test_2.two or test_1.one %}
    {{ m() }}
        "#,
            context! { test_1, test_2 },
            &[],
        )
        .unwrap();
    assert_snapshot!(rv, @"two");
}
#[test]
fn test_indent_filter_with_width_zero() {
    let env = Environment::new();
    let rv = env
        .render_str(
            r#"
{%- filter indent(width=2) -%}
here
i
am
writing
{%- endfilter -%}
            "#,
            context! {},
            &[],
        )
        .unwrap();
    assert_snapshot!(rv, @"here
  i
  am
  writing");
}
