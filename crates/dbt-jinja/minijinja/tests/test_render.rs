use std::collections::BTreeMap;

use insta::assert_snapshot;
use minijinja::{
    constants::MACRO_NAMESPACE_REGISTRY, context, value::mutable_vec::MutableVec, Environment,
    Value,
};

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
    assert_snapshot!(rv, @r"none
['x', 'y']");
}

#[test]
fn test_macro_namespace_lookup() {
    let mut env = Environment::new();
    let mut macro_namespace_registry: BTreeMap<Value, Value> = BTreeMap::new();
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
    let _ = env.add_template("test_2.two", "{% macro two() %}two{% endmacro %}", &[]);
    let rv = env
        .render_str(
            r#"
    {% set m = test_1.one or test_2.two %}
    {{ m() }}
        "#,
            context! {},
            &[],
        )
        .unwrap();
    assert_snapshot!(rv, @"two");
    let rv = env
        .render_str(
            r#"
    {% set m = test_2.two or test_1.one %}
    {{ m() }}
        "#,
            context! {},
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
