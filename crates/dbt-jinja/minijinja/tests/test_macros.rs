#![cfg(feature = "macros")]
use minijinja::listener::DefaultRenderingEventListener;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use insta::assert_snapshot;
use similar_asserts::assert_eq;

use minijinja::arg_utils::ArgParser;
use minijinja::value::{Kwargs, Object, Value};
use minijinja::{args, context, render, Environment, ErrorKind};

#[test]
fn test_context() {
    let var1 = 23;
    let ctx = context!(var1, var2 => 42);
    assert_eq!(ctx.get_attr("var1").unwrap(), Value::from(23));
    assert_eq!(ctx.get_attr("var2").unwrap(), Value::from(42));
}

#[test]
fn test_context_merge() {
    let one = context!(a => 1);
    let two = context!(b => 2, a => 42);
    let ctx = context![..one, ..two];
    assert_eq!(ctx.get_attr("a").unwrap(), Value::from(1));
    assert_eq!(ctx.get_attr("b").unwrap(), Value::from(2));

    let two = context!(b => 2, a => 42);
    let ctx = context!(a => 1, ..two);
    assert_eq!(ctx.get_attr("a").unwrap(), Value::from(1));
    assert_eq!(ctx.get_attr("b").unwrap(), Value::from(2));
}

#[test]
fn test_context_merge_custom() {
    #[derive(Debug, Clone)]
    struct X;

    impl Object for X {
        fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
            match key.as_str()? {
                "a" => Some(Value::from(1)),
                "b" => Some(Value::from(2)),
                _ => None,
            }
        }
    }

    let x = Value::from_object(X);
    let ctx = context! { a => 42, ..x };

    assert_eq!(ctx.get_attr("a").unwrap(), Value::from(42));
    assert_eq!(ctx.get_attr("b").unwrap(), Value::from(2));
}

#[test]
fn test_render() {
    let env = Environment::new();
    let rv = render!(in env, "Hello {{ name }}!", name => "World");
    assert_eq!(rv, "Hello World!");

    let rv = render!("Hello {{ name }}!", name => "World");
    assert_eq!(rv, "Hello World!");

    let rv = render!("Hello World!");
    assert_eq!(rv, "Hello World!");
}

#[test]
fn test_args() {
    fn type_name_of_val<T: ?Sized>(_val: &T) -> &str {
        std::any::type_name::<T>()
    }

    let args = args!();
    assert_eq!(args.len(), 0);
    assert_eq!(type_name_of_val(args), "[minijinja::value::Value]");

    let args = args!(1, 2);
    assert_eq!(args[0], Value::from(1));
    assert_eq!(args[1], Value::from(2));
    assert_eq!(type_name_of_val(args), "[minijinja::value::Value]");

    let args = args!(1, 2,);
    assert_eq!(args[0], Value::from(1));
    assert_eq!(args[1], Value::from(2));

    let args = args!(1, 2, foo => 42, bar => 23);
    assert_eq!(args[0], Value::from(1));
    assert_eq!(args[1], Value::from(2));
    let kwargs = Kwargs::try_from(args[2].clone()).unwrap();
    assert_eq!(kwargs.get::<i32>("foo").unwrap(), 42);
    assert_eq!(kwargs.get::<i32>("bar").unwrap(), 23);

    let args = args!(1, 2, foo => 42, bar => 23,);
    assert_eq!(args[0], Value::from(1));
    assert_eq!(args[1], Value::from(2));
    let kwargs = Kwargs::try_from(args[2].clone()).unwrap();
    assert_eq!(kwargs.get::<i32>("foo").unwrap(), 42);
    assert_eq!(kwargs.get::<i32>("bar").unwrap(), 23);
    assert_eq!(type_name_of_val(args), "[minijinja::value::Value]");
}

#[test]
fn test_macro_passing() {
    let env = Environment::new();
    let tmpl = env
        .template_from_str("{% macro m(a) %}{{ a }}{% endmacro %}")
        .unwrap();
    let (_, _, state) = tmpl
        .render_and_return_state((), Rc::new(DefaultRenderingEventListener))
        .unwrap();
    let m = state.lookup("m").unwrap();
    assert_eq!(m.get_attr("name").unwrap().as_str(), Some("m"));
    let rv = m
        .call(&state, args!(42), Rc::new(DefaultRenderingEventListener))
        .unwrap();
    assert_eq!(rv.as_str(), Some("42"));

    // if we call the macro on an empty state it errors
    let empty_state = env.empty_state();
    let err = m
        .call(
            &empty_state,
            args!(42),
            Rc::new(DefaultRenderingEventListener),
        )
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidOperation);
    assert_eq!(
        err.detail(),
        Some("cannot call this macro. template state went away.")
    );
}

#[test]
fn test_no_leak() {
    let dropped = Arc::new(AtomicBool::new(false));

    #[derive(Debug, Clone)]
    struct X(Arc<AtomicBool>);

    impl Object for X {
        fn get_value(self: &Arc<Self>, _name: &Value) -> Option<Value> {
            None
        }
    }

    impl Drop for X {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }

    let ctx = context! {
        x => Value::from_object(X(dropped.clone())),
    };
    let mut env = Environment::new();
    env.add_template("x", "{% macro meh() %}{{ x }}{{ meh }}{% endmacro %}")
        .unwrap();
    let rv = env
        .render_str(
            r#"
        {%- from 'x' import meh %}
        {{- meh() }}
        {%- set closure = x %}
        {%- macro foo() %}{{ foo }}{{ closure }}{% endmacro %}
        {{- foo() -}}

        {%- for y in range(3) %}
            {%- set closure = x %}
            {%- macro foo() %}{{ foo }}{{ closure }}{% endmacro %}
            {{- foo() -}}
        {%- endfor -%}
    "#,
            ctx,
            None,
        )
        .unwrap()
        .0;

    assert!(dropped.load(std::sync::atomic::Ordering::Relaxed));
    assert_eq!(
        rv,
        "{}<macro meh><macro foo>{}<macro foo>{}<macro foo>{}<macro foo>{}"
    );
}

/// https://github.com/mitsuhiko/minijinja/issues/434
#[test]
fn test_nested_macro_bug() {
    let rv = render!(
        r#"
    {% set a = 42 %}
    {% macro m1(var) -%}
      {{ var }}
    {%- endmacro %}

    {% macro m2(x=a) -%}
      {{ m1(x) }}
    {%- endmacro %}

    {{ m2() }}
    "#
    );
    assert_snapshot!(rv.trim(), @"42");
}

/// https://github.com/mitsuhiko/minijinja/issues/434
#[test]
fn test_caller_bug() {
    let rv = render!(
        r#"
    {% set a = 42 %}
    {% set b = 23 %}

    {% macro m1(var) -%}
      {{ caller(var) }}
    {%- endmacro %}

    {% macro m2(x=a) -%}
      {% call(var) m1(x) %}{{ var }}|{{ b }}{% endcall %}
    {%- endmacro %}

    {{ m2() }}
    "#
    );
    assert_snapshot!(rv.trim(), @"42|23");
}

/// https://github.com/mitsuhiko/minijinja/issues/535
#[test]
fn test_unenclosed_resolve() {
    // the current intended logic here is that a the state can
    // observe real globals and the initial template context, but
    // no other modifications.  Normally the call block can only
    // see what it encloses explicitly, but since it does not
    // refer to anything here it in fact has an empty closure.

    fn resolve(state: &minijinja::State, var: &str) -> Value {
        state.lookup(var).unwrap_or_default()
    }

    let mut env = Environment::new();
    env.add_global("ctx_global", "ctx global");
    env.add_function("resolve", resolve);
    let rv = env
        .render_str(
            r#"
    {%- set template_global = 'template global' %}
    {%- macro wrapper() %}{{ caller() }}{% endmacro %}
    {%- call wrapper() %}
        {{- resolve('render_global') }}|
        {{- resolve('ctx_global') }}|
        {{- resolve('template_global') }}
    {%- endcall -%}
    "#,
            context! { render_global => "render global" },
            None,
        )
        .unwrap()
        .0;
    assert_snapshot!(rv, @"render global|ctx global|");
}

#[test]
fn test_unenclosed_resolve_with_template() {
    // the current intended logic here is that a the state can
    // observe real globals and the initial template context, but
    // no other modifications.  Normally the call block can only
    // see what it encloses explicitly, but since it does not
    // refer to anything here it in fact has an empty closure.

    fn resolve(state: &minijinja::State, var: &str) -> Value {
        state.lookup(var).unwrap_or_default()
    }

    let mut env = Environment::new();
    env.add_global("ctx_global", "ctx global");
    env.add_function("resolve", resolve);
    // Note (Ani): we will never register a template un-namespaced (see environment_builder.rs try_with_macros)
    // nor will we ever look up a non namespaced template based on dbt namespace resolution (see dispatch_object.rs macro_namespace_template_resolver)
    env.add_template(
        "dbt.wrapper",
        "{%- macro wrapper(a, b) %}{{a}}|{{ caller() }}|{{b}}{% endmacro %}",
    )
    .unwrap();
    let rv = env
        .render_str(
            r#"
    {%- set template_global = 'template global' %}
    {%- set a_variable = 'a variable' %}
    {%- call wrapper(a_variable, b=1) %}
        {{- resolve('render_global') }}|
        {{- resolve('ctx_global') }}|
        {{- resolve('template_global') }}
    {%- endcall -%}
    "#,
            context! { render_global => "render global" },
            None,
        )
        .unwrap()
        .0;
    // different from not using template, but it should okay for dbt usecase
    assert_snapshot!(rv, @"a variable|render global|ctx global||1");
}

#[test]
fn test_unenclosed_resolve_with_template_with_args() {
    // the current intended logic here is that a the state can
    // observe real globals and the initial template context, but
    // no other modifications.  Normally the call block can only
    // see what it encloses explicitly, but since it does not
    // refer to anything here it in fact has an empty closure.

    fn resolve(state: &minijinja::State, var: &str) -> Value {
        state.lookup(var).unwrap_or_default()
    }

    let mut env = Environment::new();
    env.add_global("ctx_global", "ctx global");
    env.add_function("resolve", resolve);
    // Note (Ani): we will never register a template un-namespaced (see environment_builder.rs try_with_macros)
    // nor will we ever look up a non namespaced template based on dbt namespace resolution (see dispatch_object.rs macro_namespace_template_resolver)
    env.add_template(
        "dbt.wrapper",
        "{%- macro wrapper(a, b) %}{{a}}|{{ caller(a) }}|{{b}}{% endmacro %}",
    )
    .unwrap();
    let rv = env
        .render_str(
            r#"
    {%- set template_global = 'template global' %}
    {%- set a_variable = 'a variable' %}
    {%- call(x) wrapper(a_variable, b=1) %}
        {{x}}
        {{- resolve('render_global') }}|
        {{- resolve('ctx_global') }}|
        {{- resolve('template_global') }}
    {%- endcall -%}
    "#,
            context! { render_global => "render global" },
            None,
        )
        .unwrap()
        .0;
    // different from not using template, but it should okay for dbt usecase
    assert_snapshot!(rv, @r"
    a variable|
            a variablerender global|ctx global||1
    ");
}

fn ref_function(args: &[Value]) -> Result<Value, minijinja::Error> {
    // Use ArgParser for argument parsing
    let mut parser = ArgParser::new(args, None);

    // Attempt to get the model name and version
    let model_name = parser.get::<String>("model_name").unwrap_or_default();
    let packagename = parser.get::<String>("packagename").unwrap_or_default();
    let version = parser.get_optional::<String>("version").unwrap_or_default();

    // Log the parsed arguments
    println!(
        "Parsed arguments: model_name = {}, version = {}",
        model_name, version
    );

    if !packagename.is_empty() {
        Ok(Value::from(format!(
            "ref({}, package={}, version={})",
            model_name, packagename, version
        )))
    } else {
        Ok(Value::from(format!(
            "ref({}, version={})",
            model_name, version
        )))
    }
}

#[test]
fn test_ref_override() {
    use minijinja_contrib::pycompat::unknown_method_callback;
    let mut env = Environment::new();

    env.set_unknown_method_callback(unknown_method_callback);
    env.add_template(
        "ref",
        r#"{% macro ref() %}
            {% if kwargs is not undefined %}
                {% set version = kwargs.get('version') %}
            {% endif %}
            {% set packagename = none %}
            {%- if (varargs | length) == 1 -%}
                {% set modelname = varargs[0] %}
            {%- else -%}
                {% set modelname = varargs[0] %}
                {% set packagename = varargs[1] %}
            {% endif %}

            {%- set version_override = 2 -%}
            {%- set packagename_override = 'test' -%}

            {% if packagename is not none %}
                {{ builtins.ref(modelname, packagename, version=version_override) }}
            {% else %}
                {{ builtins.ref(modelname, packagename_override, version=version_override) }}
            {% endif %}
        {% endmacro %}"#,
    )
    .unwrap();

    let mut builtins = BTreeMap::new();
    builtins.insert(Value::from("ref"), Value::from_function(ref_function));
    env.add_global("builtins", Value::from_object(builtins));

    // Test single argument case
    let rv = env
        .render_str(r#"{{ ref('my_model') }}"#, context! {}, None)
        .unwrap()
        .0;
    assert_snapshot!(rv.trim(), @"ref(my_model, package=test, version=2)");

    // Test two argument case
    let rv = env
        .render_str(r#"{{ ref('my_model','my_package') }}"#, context! {}, None)
        .unwrap()
        .0;
    assert_snapshot!(rv.trim(), @"ref(my_model, package=my_package, version=2)");
}

#[test]
fn test_unary_operator_with_function() {
    let mut env = Environment::new();

    // Add a function that returns a number
    env.add_function("get_value", |_args: &[Value]| -> Result<Value, _> {
        Ok(Value::from(42))
    });

    // Test negation with function call
    let rv = env
        .render_str("{{ -get_value() }}", context! {}, None)
        .unwrap()
        .0;
    assert_snapshot!(rv.trim(), @"-42");

    // Test negation with function call and parentheses
    let rv = env
        .render_str("{{ -(get_value()) }}", context! {}, None)
        .unwrap()
        .0;
    assert_snapshot!(rv.trim(), @"-42");

    // Test with var function (common in DBT)
    env.add_function("var", |_args: &[Value]| -> Result<Value, _> {
        Ok(Value::from(90))
    });

    let rv = env
        .render_str("{{ -var('some_var') }}", context! {}, None)
        .unwrap()
        .0;
    assert_snapshot!(rv.trim(), @"-90");
}

#[test]
fn test_macro_default_arg_referencing_other_arg() {
    let env = Environment::new();

    // Define a macro with a default argument that references another argument
    let template = r#"
    {% macro basic_macro(input_dict, name_from_dict=input_dict["name"]) %}
        {{ name_from_dict }}
    {% endmacro %}
    
    {% set dict = {"datatype": "TEXT", "is_rename": false, "name": "account_number", "renamed_column_name": "AccountNumber"} %}
    
    {# This should work - explicit argument #}
    {{ basic_macro(dict, "something") }}
    
    {# This should also work - using default that references first argument #}
    {{ basic_macro(dict) }}
    "#;

    let rv = env.render_str(template, context! {}, None).unwrap().0;
    let lines: Vec<&str> = rv.lines().filter(|l| !l.trim().is_empty()).collect();

    assert_eq!(lines.len(), 2);
    assert_snapshot!(lines[0].trim(), @"something");
    assert_snapshot!(lines[1].trim(), @"account_number");
}
