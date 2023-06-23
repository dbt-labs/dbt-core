import unittest

from dbt.clients.jinja_static import statically_extract_macro_calls
from dbt.context.base import generate_base_context


class MacroCalls(unittest.TestCase):
    def setUp(self):
        self.macro_strings = [
            "{% macro parent_macro() %} {% do return(nested_macro()) %} {% endmacro %}",
            "{% macro lr_macro() %} {{ return(load_result('relations').table) }} {% endmacro %}",
            "{% macro get_snapshot_unique_id() -%} {{ return(adapter.dispatch('get_snapshot_unique_id')()) }} {%- endmacro %}",
            "{% macro get_columns_in_query(select_sql) -%} {{ return(adapter.dispatch('get_columns_in_query')(select_sql)) }} {% endmacro %}",
            """{% macro test_mutually_exclusive_ranges(model) %}
                with base as (
                    select {{ get_snapshot_unique_id() }} as dbt_unique_id,
                    *
                    from {{ model }} )
                {% endmacro %}""",
            "{% macro test_my_test(model) %} select {{ current_timestamp_backcompat() }} {% endmacro %}",
            "{% macro some_test(model) -%} {{ return(adapter.dispatch('test_some_kind4', 'foo_utils4')) }} {%- endmacro %}",
            "{% macro some_test(model) -%} {{ return(adapter.dispatch('test_some_kind5', macro_namespace = 'foo_utils5')) }} {%- endmacro %}",
        ]

        self.possible_macro_calls = [
            ["nested_macro"],
            ["load_result"],
            ["get_snapshot_unique_id"],
            ["get_columns_in_query"],
            ["get_snapshot_unique_id"],
            ["current_timestamp_backcompat"],
            ["test_some_kind4", "foo_utils4.test_some_kind4"],
            ["test_some_kind5", "foo_utils5.test_some_kind5"],
        ]

    def test_macro_calls(self):
        cli_vars = {"local_utils_dispatch_list": ["foo_utils4"]}
        ctx = generate_base_context(cli_vars)

        index = 0
        for macro_string in self.macro_strings:
            possible_macro_calls = statically_extract_macro_calls(macro_string, ctx)
            self.assertEqual(self.possible_macro_calls[index], possible_macro_calls)
            index += 1
