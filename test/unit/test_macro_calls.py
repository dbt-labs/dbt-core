import os
import unittest
from unittest.mock import MagicMock, patch

from dataclasses import dataclass, field
from typing import Dict, Any

from dbt.clients.jinja import statically_extract_macro_calls
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
            "{% macro test_my_test(model) %} select {{ dbt_utils.current_timestamp() }} {% endmacro %}"
        ] 

        self.possible_macro_calls = [
            ['nested_macro'],
            ['load_result'],
            ['get_snapshot_unique_id'],
            ['get_columns_in_query'],
            ['get_snapshot_unique_id'],
            ['dbt_utils.current_timestamp'],
        ]

    def test_macro_calls(self):
        ctx = generate_base_context({})

        index = 0
        for macro_string in self.macro_strings:
            possible_macro_calls = statically_extract_macro_calls(macro_string, ctx)
            self.assertEqual(possible_macro_calls, self.possible_macro_calls[index])
            index = index + 1


