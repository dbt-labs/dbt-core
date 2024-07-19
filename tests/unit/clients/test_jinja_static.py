import pytest

from dbt.clients.jinja_static import statically_extract_macro_calls
from dbt.context.base import generate_base_context


@pytest.mark.parametrize(
    "macro_string,expected_possible_macro_calls",
    [
        (
            "{% macro parent_macro() %} {% do return(nested_macro()) %} {% endmacro %}",
            ["nested_macro"],
        ),
        (
            "{% macro lr_macro() %} {{ return(load_result('relations').table) }} {% endmacro %}",
            ["load_result"],
        ),
        (
            "{% macro get_snapshot_unique_id() -%} {{ return(adapter.dispatch('get_snapshot_unique_id')()) }} {%- endmacro %}",
            ["get_snapshot_unique_id"],
        ),
        (
            "{% macro get_columns_in_query(select_sql) -%} {{ return(adapter.dispatch('get_columns_in_query')(select_sql)) }} {% endmacro %}",
            ["get_columns_in_query"],
        ),
        (
            """{% macro test_mutually_exclusive_ranges(model) %}
            with base as (
                select {{ get_snapshot_unique_id() }} as dbt_unique_id,
                *
                from {{ model }} )
            {% endmacro %}""",
            ["get_snapshot_unique_id"],
        ),
        (
            "{% macro test_my_test(model) %} select {{ current_timestamp_backcompat() }} {% endmacro %}",
            ["current_timestamp_backcompat"],
        ),
        (
            "{% macro some_test(model) -%} {{ return(adapter.dispatch('test_some_kind4', 'foo_utils4')) }} {%- endmacro %}",
            ["test_some_kind4", "foo_utils4.test_some_kind4"],
        ),
        (
            "{% macro some_test(model) -%} {{ return(adapter.dispatch('test_some_kind5', macro_namespace = 'foo_utils5')) }} {%- endmacro %}",
            ["test_some_kind5", "foo_utils5.test_some_kind5"],
        ),
    ],
)
def test_extract_macro_calls(self, macro_string, expected_possible_macro_calls):
    cli_vars = {"local_utils_dispatch_list": ["foo_utils4"]}
    ctx = generate_base_context(cli_vars)

    possible_macro_calls = statically_extract_macro_calls(macro_string, ctx)
    assert possible_macro_calls == expected_possible_macro_calls
