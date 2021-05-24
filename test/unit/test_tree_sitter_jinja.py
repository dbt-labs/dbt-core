from functools import reduce
from pprint import pprint
import dbt.tree_sitter_jinja.extractor as extractor

# tree-sitter parser
parser = extractor.parser

#----- helper functions -----#

def extraction(input, expected):
    got = extractor.extract_from_source(input)
    passed = expected == got
    if not passed:
        source_bytes = bytes(input, "utf8")
        tree = parser.parse(source_bytes)
        count = extractor.error_count(tree.root_node)
        print(f"parser error count: {count}")
        print("TYPE CHECKER OUTPUT")
        pprint(extractor.type_check(source_bytes, tree.root_node))
        print(":: EXPECTED ::")
        pprint(expected)
        print("::   GOT    ::")
        pprint(got)
    return passed

def exctracted(refs=[], sources=[], configs=[], python_jinja=False):
    return {
        'refs': refs,
        'sources': set(sources),
        'configs': configs,
        'python_jinja': python_jinja
    }

# runs the parser and type checker and prints debug messaging if it fails
def type_checks(source_text):
    source_bytes = bytes(source_text, "utf8")
    tree = parser.parse(source_bytes)
    # If we couldn't parse the source we can't typecheck it.
    if extractor.error_count(tree.root_node) > 0:
        print("parser failed")
        return False
    res = extractor.type_check(source_bytes, tree.root_node)
    # if it returned a list of errors, it didn't typecheck
    if isinstance(res, extractor.TypeCheckFailure):
        print(res)
        return False
    else:
        return True

def type_check_fails(source_text):
    return not type_checks(source_text)

# same as `type_checks` but operates on a list of source strings
def all_type_check(l):
    return reduce(lambda x, y: x and y, map(type_checks, l))

# same as `type_checks_all` but returns true iff none of the strings typecheck
def none_type_check(l):
    return reduce(lambda x, y: x and y, map(type_check_fails, l))
    
def produces_tree(source_text, ast):
    source_bytes = bytes(source_text, "utf8")
    tree = parser.parse(source_bytes)
    # If we couldn't parse the source we can't typecheck it.
    if extractor.error_count(tree.root_node) > 0:
        print("parser failed")
        return False
    res = extractor.type_check(source_bytes, tree.root_node)
    # if it returned a list of errors, it didn't typecheck
    if isinstance(res, extractor.TypeCheckFailure):
        print(res)
        return False
    elif res != ast:
        print(":: EXPECTED ::")
        print(ast)
        print(":: GOT ::")
        print(res)
        return False
    else:
        return True

def fails_with(source_text, msg):
    source_bytes = bytes(source_text, "utf8")
    tree = parser.parse(source_bytes)
    # If we couldn't parse the source we can't typecheck it.
    if extractor.error_count(tree.root_node) > 0:
        print("parser failed")
        return False
    res = extractor.type_check(source_bytes, tree.root_node)
    # if it returned a list of errors, it didn't typecheck
    if isinstance(res, extractor.TypeCheckFailure):
        if msg == res.msg:
            return True
    print(":: EXPECTED ::")
    print(extractor.TypeCheckFailure(msg))
    print(":: GOT ::")
    print(res)
    return False

#---------- Type Checker Tests ----------#

def test_recognizes_ref_source_config():
    assert all_type_check([
        "select * from {{ ref('my_table') }}",
        "{{ config(key='value') }}",
        "{{ source('a', 'b') }}"
    ])

def test_recognizes_multiple_jinja_calls():
    assert all_type_check([
        "{{ ref('x') }} {{ ref('y') }}",
        "{{ config(key='value') }} {{ config(k='v') }}",
        "{{ source('a', 'b') }} {{ source('c', 'd') }}"
    ])

def test_fails_on_other_fn_names():
    assert none_type_check([
        "select * from {{ reff('my_table') }}",
        "{{ fn(key='value') }}",
        "{{ REF('a', 'b') }}"
    ])

def test_config_all_inputs():
    assert all_type_check([
        "{{ config(key='value') }}",
        "{{ config(key=True) }}",
        "{{ config(key=False) }}",
        "{{ config(key=['v1,','v2']) }}",
        "{{ config(key={'k': 'v'}) }}",
        "{{ config(key=[{'k':['v', {'x': 'y'}]}, ['a', 'b', 'c']]) }}"
    ])

def test_config_fails_non_kwarg_inputs():
    assert none_type_check([
        "{{ config('value') }}",
        "{{ config(True) }}",
        "{{ config(['v1,','v2']) }}",
        "{{ config({'k': 'v'}) }}"
    ])

def test_source_keyword_args():
    assert all_type_check([
        "{{ source(source_name='src', table_name='table') }}",
        "{{ source('src', table_name='table') }}",
        "{{ source(source_name='src', 'table') }}",
        "{{ source('src', 'table') }}"
    ])

def test_source_keyword_args():
    assert none_type_check([
        "{{ source(source_name='src', BAD_NAME='table') }}",
        "{{ source(BAD_NAME='src', table_name='table') }}",
        "{{ source(BAD_NAME='src', BAD_NAME='table') }}"
    ])

def test_source_must_have_2_args():
    assert none_type_check([
        "{{ source('one isnt enough') }}",
        "{{ source('three', 'is', 'too many') }}",
        "{{ source('one', 'two', 'three', 'four') }}",
        "{{ source(source_name='src', table_name='table', 'extra') }}",
    ])

def test_source_args_must_be_strings():
    assert none_type_check([
        "{{ source(True, False) }}",
        "{{ source(key='str', key2='str2') }}",
        "{{ source([], []) }}",
        "{{ source({}, {}) }}",
    ])

def test_ref_accepts_one_and_two_strings():
    assert all_type_check([
        "{{ ref('two', 'args') }}",
        "{{ ref('one arg') }}"
    ])

def test_ref_bad_inputs_fail():
    assert none_type_check([
        "{{ ref('too', 'many', 'strings') }}",
        "{{ ref() }}",
        "{{ ref(kwarg='is_wrong') }}",
        "{{ ref(['list is wrong']) }}"
    ])

def test_nested_fn_calls_fail():
    assert none_type_check([
        "{{ [ref('my_table')] }}",
        "{{ [config(x='y')] }}",
        "{{ config(x=ref('my_table')) }}",
        "{{ source(ref('my_table')) }}"
    ])

def test_config_excluded_kwargs():
    assert none_type_check([
        "{{ config(pre_hook='x') }}",
        "{{ config(pre-hook='x') }}",
        "{{ config(post_hook='x') }}",
        "{{ config(post-hook='x') }}"
    ])

def test_jinja_expressions_fail_everywhere():
    assert none_type_check([
        "{% config(x='y') %}",
        "{% if(whatever) do_something() %}",
        "doing stuff {{ ref('str') }} stuff {% expression %}",
        "{{ {% psych! nested expression %} }}"
    ])

def test_top_level_kwargs_are_rejected():
    assert none_type_check([
        "{{ kwarg='value' }}"
    ])

# this triggers "missing" not "error" nodes from tree-sitter
def test_fails_on_open_jinja_brackets():
    assert none_type_check([
        "{{ ref()",
        "{{ True",
        "{{",
        "{{ 'str' "
    ])

def test_ref_ast():
    assert produces_tree(
        "{{ ref('my_table') }}"
        ,
        ('root', ('ref', 'my_table'))
    )

def test_buried_refs_ast():
    assert produces_tree(
        """
        select
            field1,
            field2,
            field3
        from {{ ref('x') }}
        join {{ ref('y') }}
        """
        ,
        ('root',
            ('ref', 'x'),
            ('ref', 'y')
        )
    )

def test_config_ast():
    assert produces_tree(
        "{{ config(k1={'dict': ['value']}, k2='str') }}"
        ,
        ('root',
            ('config',
                ('kwarg', 
                    'k1', 
                    ('dict', 
                        ('dict', 
                            ('list', 
                                'value'
                            )
                        )
                    )
                ), 
                ('kwarg', 
                    'k2', 
                    'str'
                )
            )
        )
    )

def test_source_ast():
    assert produces_tree(
        "{{ source('x', table_name='y') }}"
        ,
        ('root',
            ('source',
                'x',
                'y'
            )
        )
    )

def test_jinja_expression_ast():
    assert fails_with(
        "{% expression %}"
        ,
        "jinja expressions are unsupported: {% syntax like this %}"
    )

def test_kwarg_order():
    assert fails_with(
        "{{ source(source_name='kwarg', 'positional') }}"
        ,
        "keyword arguments must all be at the end"
    )

#---------- Extractor Tests ----------#

def test_ref():
    assert extraction(
        "{{ ref('my_table') }} {{ ref('other_table')}}"
        ,
        exctracted(
            refs=[['my_table'], ['other_table']]
        )
    )

def test_config():
    assert extraction(
        "{{ config(key='value') }}"
        ,
        exctracted(
            configs=[('key', 'value')]
        )
    )

def test_source():
    assert extraction(
        "{{ source('package', 'table') }} {{ source('x', 'y') }}"
        ,
        exctracted(
            sources=[('package', 'table'), ('x', 'y')]
        )
    )

def test_all():
    assert extraction(
        "{{ source('package', 'table') }} {{ ref('x') }} {{ config(k='v', x=True) }}"
        ,
        exctracted(
            sources=[('package', 'table')],
            refs=[['x']],
            configs=[('k', 'v'), ('x', True)]
        )
    )

def test_deeply_nested_config():
    assert extraction(
        "{{ config(key=[{'k':['v', {'x': 'y'}]}, ['a', 'b', 'c']]) }}"
        ,
        exctracted(
            configs=[('key', [{'k':['v', {'x': 'y'}]}, ['a', 'b', 'c']])]
        )
    )

def test_extracts_dict_with_multiple_keys():
    assert extraction(
        "{{ config(dict={'a':'x', 'b': 'y', 'c':'z'}) }}"
        ,
        exctracted(
            configs=[('dict', {'a': 'x', 'b': 'y', 'c':'z'})]
        )
    )
