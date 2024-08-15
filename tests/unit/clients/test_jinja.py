from contextlib import contextmanager

import pytest
import yaml

from dbt.clients.jinja import get_rendered, get_template
from dbt_common.exceptions import JinjaRenderingError


@contextmanager
def returns(value):
    yield value


@contextmanager
def raises(value):
    with pytest.raises(value) as exc:
        yield exc


def expected_id(arg):
    if isinstance(arg, list):
        return "_".join(arg)


jinja_tests = [
    # strings
    (
        """foo: bar""",
        returns("bar"),
        returns("bar"),
    ),
    (
        '''foo: "bar"''',
        returns("bar"),
        returns("bar"),
    ),
    (
        '''foo: "'bar'"''',
        returns("'bar'"),
        returns("'bar'"),
    ),
    (
        """foo: '"bar"'""",
        returns('"bar"'),
        returns('"bar"'),
    ),
    (
        '''foo: "{{ 'bar' | as_text }}"''',
        returns("bar"),
        returns("bar"),
    ),
    (
        '''foo: "{{ 'bar' | as_bool }}"''',
        returns("bar"),
        raises(JinjaRenderingError),
    ),
    (
        '''foo: "{{ 'bar' | as_number }}"''',
        returns("bar"),
        raises(JinjaRenderingError),
    ),
    (
        '''foo: "{{ 'bar' | as_native }}"''',
        returns("bar"),
        returns("bar"),
    ),
    # ints
    (
        """foo: 1""",
        returns("1"),
        returns("1"),
    ),
    (
        '''foo: "1"''',
        returns("1"),
        returns("1"),
    ),
    (
        '''foo: "'1'"''',
        returns("'1'"),
        returns("'1'"),
    ),
    (
        """foo: '"1"'""",
        returns('"1"'),
        returns('"1"'),
    ),
    (
        '''foo: "{{ 1 }}"''',
        returns("1"),
        returns("1"),
    ),
    (
        '''foo: "{{ '1' }}"''',
        returns("1"),
        returns("1"),
    ),
    (
        '''foo: "'{{ 1 }}'"''',
        returns("'1'"),
        returns("'1'"),
    ),
    (
        '''foo: "'{{ '1' }}'"''',
        returns("'1'"),
        returns("'1'"),
    ),
    (
        '''foo: "{{ 1 | as_text }}"''',
        returns("1"),
        returns("1"),
    ),
    (
        '''foo: "{{ 1 | as_bool }}"''',
        returns("1"),
        raises(JinjaRenderingError),
    ),
    (
        '''foo: "{{ 1 | as_number }}"''',
        returns("1"),
        returns(1),
    ),
    (
        '''foo: "{{ 1 | as_native }}"''',
        returns("1"),
        returns(1),
    ),
    (
        '''foo: "{{ '1' | as_text }}"''',
        returns("1"),
        returns("1"),
    ),
    (
        '''foo: "{{ '1' | as_bool }}"''',
        returns("1"),
        raises(JinjaRenderingError),
    ),
    (
        '''foo: "{{ '1' | as_number }}"''',
        returns("1"),
        returns(1),
    ),
    (
        '''foo: "{{ '1' | as_native }}"''',
        returns("1"),
        returns(1),
    ),
    # booleans.
    # Note the discrepancy with true vs True: `true` is recognized by jinja but
    # not literal_eval, but `True` is recognized by ast.literal_eval.
    # For extra fun, yaml recognizes both.
    # unquoted true
    (
        '''foo: "{{ True }}"''',
        returns("True"),
        returns("True"),
    ),
    (
        '''foo: "{{ True | as_text }}"''',
        returns("True"),
        returns("True"),
    ),
    (
        '''foo: "{{ True | as_bool }}"''',
        returns("True"),
        returns(True),
    ),
    (
        '''foo: "{{ True | as_number }}"''',
        returns("True"),
        raises(JinjaRenderingError),
    ),
    (
        '''foo: "{{ True | as_native }}"''',
        returns("True"),
        returns(True),
    ),
    # unquoted true
    (
        '''foo: "{{ true }}"''',
        returns("True"),
        returns("True"),
    ),
    (
        '''foo: "{{ true | as_text }}"''',
        returns("True"),
        returns("True"),
    ),
    (
        '''foo: "{{ true | as_bool }}"''',
        returns("True"),
        returns(True),
    ),
    (
        '''foo: "{{ true | as_number }}"''',
        returns("True"),
        raises(JinjaRenderingError),
    ),
    (
        '''foo: "{{ true | as_native }}"''',
        returns("True"),
        returns(True),
    ),
    (
        '''foo: "{{ 'true' | as_text }}"''',
        returns("true"),
        returns("true"),
    ),
    # quoted 'true'
    (
        '''foo: "'{{ true }}'"''',
        returns("'True'"),
        returns("'True'"),
    ),  # jinja true -> python True -> str(True) -> "True" -> quoted
    (
        '''foo: "'{{ true | as_text }}'"''',
        returns("'True'"),
        returns("'True'"),
    ),
    (
        '''foo: "'{{ true | as_bool }}'"''',
        returns("'True'"),
        returns("'True'"),
    ),
    (
        '''foo: "'{{ true | as_number }}'"''',
        returns("'True'"),
        returns("'True'"),
    ),
    (
        '''foo: "'{{ true | as_native }}'"''',
        returns("'True'"),
        returns("'True'"),
    ),
    # unquoted True
    (
        '''foo: "{{ True }}"''',
        returns("True"),
        returns("True"),
    ),
    (
        '''foo: "{{ True | as_text }}"''',
        returns("True"),
        returns("True"),
    ),  # True -> string 'True' -> text -> str('True') -> 'True'
    (
        '''foo: "{{ True | as_bool }}"''',
        returns("True"),
        returns(True),
    ),
    (
        '''foo: "{{ True | as_number }}"''',
        returns("True"),
        raises(JinjaRenderingError),
    ),
    (
        '''foo: "{{ True | as_native }}"''',
        returns("True"),
        returns(True),
    ),
    # quoted 'True' within rendering
    (
        '''foo: "{{ 'True' | as_text }}"''',
        returns("True"),
        returns("True"),
    ),
    # 'True' -> string 'True' -> text -> str('True') -> 'True'
    (
        '''foo: "{{ 'True' | as_bool }}"''',
        returns("True"),
        returns(True),
    ),
    # quoted 'True' outside rendering
    (
        '''foo: "'{{ True }}'"''',
        returns("'True'"),
        returns("'True'"),
    ),
    (
        '''foo: "'{{ True | as_bool }}'"''',
        returns("'True'"),
        returns("'True'"),
    ),
    # yaml turns 'yes' into a boolean true
    (
        """foo: yes""",
        returns("True"),
        returns("True"),
    ),
    (
        '''foo: "yes"''',
        returns("yes"),
        returns("yes"),
    ),
    # concatenation
    (
        '''foo: "{{ (a_int + 100) | as_native }}"''',
        returns("200"),
        returns(200),
    ),
    (
        '''foo: "{{ (a_str ~ 100) | as_native }}"''',
        returns("100100"),
        returns(100100),
    ),
    (
        '''foo: "{{( a_int ~ 100) | as_native }}"''',
        returns("100100"),
        returns(100100),
    ),
    # multiple nodes -> always str
    (
        '''foo: "{{ a_str | as_native }}{{ a_str | as_native }}"''',
        returns("100100"),
        returns("100100"),
    ),
    (
        '''foo: "{{ a_int | as_native }}{{ a_int | as_native }}"''',
        returns("100100"),
        returns("100100"),
    ),
    (
        '''foo: "'{{ a_int | as_native }}{{ a_int | as_native }}'"''',
        returns("'100100'"),
        returns("'100100'"),
    ),
    (
        """foo:""",
        returns("None"),
        returns("None"),
    ),
    (
        """foo: null""",
        returns("None"),
        returns("None"),
    ),
    (
        '''foo: ""''',
        returns(""),
        returns(""),
    ),
    (
        '''foo: "{{ '' | as_native }}"''',
        returns(""),
        returns(""),
    ),
    # very annoying, but jinja 'none' is yaml 'null'.
    (
        '''foo: "{{ none | as_native }}"''',
        returns("None"),
        returns(None),
    ),
    # make sure we don't include comments in the output (see #2707)
    (
        '''foo: "{# #}hello"''',
        returns("hello"),
        returns("hello"),
    ),
    (
        '''foo: "{% if false %}{% endif %}hello"''',
        returns("hello"),
        returns("hello"),
    ),
]


@pytest.mark.parametrize("value,text_expectation,native_expectation", jinja_tests, ids=expected_id)
def test_jinja_rendering_string(value, text_expectation, native_expectation):
    foo_value = yaml.safe_load(value)["foo"]
    ctx = {"a_str": "100", "a_int": 100, "b_str": "hello"}
    with text_expectation as text_result:
        assert text_result == get_rendered(foo_value, ctx, native=False)

    with native_expectation as native_result:
        assert native_result == get_rendered(foo_value, ctx, native=True)


def test_do():
    s = "{% set my_dict = {} %}\n{% do my_dict.update(a=1) %}"

    template = get_template(s, {})
    mod = template.make_module()
    assert mod.my_dict == {"a": 1}


def test_regular_render():
    s = '{{ "some_value" | as_native }}'
    value = get_rendered(s, {}, native=False)
    assert value == "some_value"
    s = "{{ 1991 | as_native }}"
    value = get_rendered(s, {}, native=False)
    assert value == "1991"

    s = '{{ "some_value" | as_text }}'
    value = get_rendered(s, {}, native=False)
    assert value == "some_value"
    s = "{{ 1991 | as_text }}"
    value = get_rendered(s, {}, native=False)
    assert value == "1991"


def test_native_render():
    s = '{{ "some_value" | as_native }}'
    value = get_rendered(s, {}, native=True)
    assert value == "some_value"
    s = "{{ 1991 | as_native }}"
    value = get_rendered(s, {}, native=True)
    assert value == 1991

    s = '{{ "some_value" | as_text }}'
    value = get_rendered(s, {}, native=True)
    assert value == "some_value"
    s = "{{ 1991 | as_text }}"
    value = get_rendered(s, {}, native=True)
    assert value == "1991"
