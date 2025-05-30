#![cfg(feature = "builtins")]
use minijinja::value::Value;
use minijinja::{args, Environment};
use similar_asserts::assert_eq;

use minijinja::filters::{abs, indent};

#[test]
fn test_filter_with_non() {
    fn filter(value: Option<String>) -> String {
        format!("[{}]", value.unwrap_or_default())
    }

    let mut env = Environment::new();
    env.add_filter("filter", filter);
    let state = env.empty_state();

    let rv = state
        .apply_filter("filter", args!(Value::UNDEFINED))
        .unwrap();
    assert_eq!(rv, Value::from("[]"));

    let rv = state
        .apply_filter("filter", args!(Value::from(())))
        .unwrap();
    assert_eq!(rv, Value::from("[]"));

    let rv = state
        .apply_filter("filter", args!(Value::from("wat")))
        .unwrap();
    assert_eq!(rv, Value::from("[wat]"));
}

#[test]
fn test_indent_one_empty_line() {
    let teststring = String::from("\n");
    let args = vec![Value::from(teststring), Value::from(2)];
    assert_eq!(indent(&args).unwrap(), String::from(""));
}

#[test]
fn test_indent_one_line() {
    let teststring = String::from("test\n");
    let args = vec![Value::from(teststring), Value::from(2)];
    assert_eq!(indent(&args).unwrap(), String::from("test"));
}

#[test]
fn test_indent() {
    let teststring = String::from("test\ntest1\n\ntest2\n");
    let args = vec![Value::from(teststring), Value::from(2)];
    assert_eq!(
        indent(&args).unwrap(),
        String::from("test\n  test1\n\n  test2")
    );
}

#[test]
fn test_indent_with_indented_first_line() {
    let teststring = String::from("test\ntest1\n\ntest2\n");
    let args = vec![Value::from(teststring), Value::from(2), Value::from(true)];
    assert_eq!(
        indent(&args).unwrap(),
        String::from("  test\n  test1\n\n  test2")
    );
}

#[test]
fn test_indent_with_indented_blank_line() {
    let teststring = String::from("test\ntest1\n\ntest2\n");
    let args = vec![
        Value::from(teststring),
        Value::from(2),
        Value::from(()),
        Value::from(true),
    ];
    assert_eq!(
        indent(&args).unwrap(),
        String::from("test\n  test1\n  \n  test2")
    );
}

#[test]
fn test_indent_with_all_indented() {
    let teststring = String::from("test\ntest1\n\ntest2\n");
    let args = vec![
        Value::from(teststring),
        Value::from(2),
        Value::from(true),
        Value::from(true),
    ];
    assert_eq!(
        indent(&args).unwrap(),
        String::from("  test\n  test1\n  \n  test2")
    );
}

#[test]
fn test_abs_overflow() {
    let ok = abs(Value::from(i64::MIN)).unwrap();
    assert_eq!(ok, Value::from(-(i64::MIN as i128)));
    let err = abs(Value::from(i128::MIN)).unwrap_err();
    assert_eq!(err.to_string(), "invalid operation: overflow on abs");
}
