#![cfg(feature = "pycompat")]

use minijinja::{Environment, Value};
use minijinja_contrib::pycompat::unknown_method_callback;
use similar_asserts::assert_eq;

fn eval_expr(expr: &str) -> Value {
    let mut env = Environment::new();
    env.set_unknown_method_callback(unknown_method_callback);
    env.compile_expression(expr, &[])
        .unwrap()
        .eval((), &[])
        .unwrap()
}

#[test]
#[allow(clippy::cognitive_complexity)]
fn test_string_methods() {
    assert_eq!(eval_expr("'foo'.upper()").as_str(), Some("FOO"));
    assert_eq!(eval_expr("'FoO'.lower()").as_str(), Some("foo"));
    assert_eq!(eval_expr("' foo '.strip()").as_str(), Some("foo"));
    assert_eq!(eval_expr("'!foo?!!!'.strip('!?')").as_str(), Some("foo"));
    assert_eq!(
        eval_expr("'!!!foo?!!!'.rstrip('!?')").as_str(),
        Some("!!!foo")
    );
    assert_eq!(
        eval_expr("'!!!foo?!!!'.lstrip('!?')").as_str(),
        Some("foo?!!!")
    );
    assert!(eval_expr("'foobar'.islower()").is_true());
    assert!(eval_expr("'FOOBAR'.isupper()").is_true());
    assert!(eval_expr("' \\n'.isspace()").is_true());
    assert!(eval_expr("'abc'.isalpha()").is_true());
    assert!(eval_expr("'abc123'.isalnum()").is_true());
    assert!(eval_expr("'abc%@#'.isascii()").is_true());
    assert_eq!(
        eval_expr("'foobar'.replace('o', 'x')").as_str(),
        Some("fxxbar")
    );
    assert_eq!(
        eval_expr("'foobar'.replace('o', 'x', 1)").as_str(),
        Some("fxobar")
    );
    assert_eq!(eval_expr("'foo bar'.title()").as_str(), Some("Foo Bar"));
    assert_eq!(
        eval_expr("'foo bar'.capitalize()").as_str(),
        Some("Foo bar")
    );
    assert_eq!(eval_expr("'foo barooo'.count('oo')").as_usize(), Some(2));
    assert_eq!(eval_expr("'foo barooo'.find('oo')").as_usize(), Some(1));
    assert_eq!(eval_expr("'foo barooo'.rfind('oo')").as_usize(), Some(8));
    assert!(eval_expr("'a b c'.split() == ['a', 'b', 'c']").is_true());
    assert!(eval_expr("'a  b  c'.split() == ['a', 'b', 'c']").is_true());
    assert!(eval_expr("'a  b  c'.split(none, 1) == ['a', 'b  c']").is_true());
    assert!(eval_expr("'abcbd'.split('b', 1) == ['a', 'cbd']").is_true());
    assert!(eval_expr("'a\\nb\\r\\nc'.splitlines() == ['a', 'b', 'c']").is_true());
    assert!(eval_expr("'a\\nb\\r\\nc'.splitlines(true) == ['a\\n', 'b\\r\\n', 'c']").is_true());
    assert!(eval_expr("'foobarbaz'.startswith('foo')").is_true());
    assert!(eval_expr("'foobarbaz'.startswith(('foo', 'bar'))").is_true());
    assert!(!eval_expr("'barfoobaz'.startswith(('foo', 'baz'))").is_true());
    assert!(eval_expr("'foobarbaz'.endswith('baz')").is_true());
    assert!(eval_expr("'foobarbaz'.endswith(('baz', 'bar'))").is_true());
    assert!(!eval_expr("'foobarbazblah'.endswith(('baz', 'bar'))").is_true());
    assert_eq!(eval_expr("'|'.join([1, 2, 3])").as_str(), Some("1|2|3"));
    assert_eq!(
        eval_expr("'My name is {fname}, I\\'m {age}'.format({'fname': 'John', 'age': 36})")
            .as_str(),
        Some("My name is John, I'm 36")
    );
    assert_eq!(
        eval_expr("'My name is {0}, I\\'m {1}'.format('John', 36)").as_str(),
        Some("My name is John, I'm 36")
    );
    assert_eq!(
        eval_expr("'My name is {}, I\\'m {}'.format('John', 36)").as_str(),
        Some("My name is John, I'm 36")
    );
    assert_eq!(
        eval_expr("'foo_bar'.removesuffix('_bar')").as_str(),
        Some("foo")
    );
}

#[test]
fn test_dict_methods() {
    assert!(eval_expr("{'x': 42}.keys()|list == ['x']").is_true());
    assert!(eval_expr("{'x': 42}.values()|list == [42]").is_true());
    assert!(eval_expr("{'x': 42}.items()|list == [('x', 42)]").is_true());
    assert!(eval_expr("{'x': 42}.get('x') == 42").is_true());
    assert!(eval_expr("{'x': 42}.get('y') is none").is_true());
}

#[test]
fn test_list_methods() {
    assert!(eval_expr("[1, 2, 2, 3].count(2) == 2").is_true());

    // Test basic union with two lists
    assert!(eval_expr("[1, 2, 3].union([3, 4, 5]) | sort == [1, 2, 3, 4, 5]").is_true());

    // Test union with duplicates (should be removed)
    assert!(eval_expr("[1, 2, 2].union([2, 3, 3]) | sort == [1, 2, 3]").is_true());

    // Test union with multiple arguments
    assert!(eval_expr("[1, 2].union([3, 4], [5, 6]) | sort == [1, 2, 3, 4, 5, 6]").is_true());

    // Test union with empty list
    assert!(eval_expr("[1, 2].union([]) | sort == [1, 2]").is_true());

    // Test union of empty list with non-empty
    assert!(eval_expr("[].union([1, 2]) | sort == [1, 2]").is_true());

    // Test union with string elements
    assert!(eval_expr("['a', 'b'].union(['b', 'c']) | sort == ['a', 'b', 'c']").is_true());

    // Test index method - basic usage
    assert_eq!(eval_expr("[1, 2, 3, 4].index(3)").as_i64(), Some(2));
    assert_eq!(eval_expr("['a', 'b', 'c'].index('b')").as_i64(), Some(1));

    // Test index with start parameter
    assert_eq!(eval_expr("[1, 2, 3, 2, 4].index(2, 2)").as_i64(), Some(3));

    // Test index with start and end parameters
    assert_eq!(
        eval_expr("[1, 2, 3, 2, 4].index(2, 1, 3)").as_i64(),
        Some(1)
    );

    // Test index with negative start
    assert_eq!(eval_expr("[1, 2, 3, 4].index(3, -3)").as_i64(), Some(2));

    // Test index with negative end
    assert_eq!(eval_expr("[1, 2, 3, 4].index(2, 0, -1)").as_i64(), Some(1));
}

#[test]
fn test_list_index_errors() {
    let mut env = Environment::new();
    env.set_unknown_method_callback(unknown_method_callback);

    // Test index method error when item not found
    let result = env
        .compile_expression("[1, 2, 3].index(5)", &[])
        .unwrap()
        .eval((), &[]);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.to_string().contains("5 is not in list"));

    // Test index method error when item not in range
    let result = env
        .compile_expression("[1, 2, 3, 4].index(1, 2)", &[])
        .unwrap()
        .eval((), &[]);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.to_string().contains("1 is not in list"));
}
