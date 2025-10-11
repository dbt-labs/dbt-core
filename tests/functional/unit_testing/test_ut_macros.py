import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture

my_model_without_composition_sql = """
{{ config(materialized='table') }}

{% set one = macro_one() %}
{% set two = macro_two() %}

select 1 as id
"""

my_model_with_composition_sql = """
{{ config(materialized='table') }}

{% set one = macro_one() %}
{% set two = macro_two() %}
{% set one_plus_two = one + two %}

select 1 as id
"""

my_macro_sql = """
{% macro macro_one() -%}
    {{ return(1) }}
{%- endmacro %}


{% macro macro_two() -%}
    {{ return(2) }}
{%- endmacro %}
"""

my_unit_test_yml = """
unit_tests:
  - name: my_unit_test
    model: my_model
    given: []
    expect:
      rows:
        - {id: 1}
"""


class TestMacroWithoutComposition:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_without_composition_sql,
            "my_unit_test.yml": my_unit_test_yml,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"my_macros.sql": my_macro_sql}

    def test_macro_in_unit_test(self, project):
        # Test that a model without macro composition properly resolves macro names in unit tests
        run_dbt_and_capture(["test"], expect_pass=True)


class TestMacroComposition:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_with_composition_sql,
            "my_unit_test.yml": my_unit_test_yml,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"my_macros.sql": my_macro_sql}

    def test_macro_composition_in_unit_test(self, project):
        # Verify model works fine outside of unit testing
        results = run_dbt(["run"])
        assert len(results) == 1

        # Test that a model with macro composition properly resolves macro names in unit tests
        run_dbt_and_capture(["test"], expect_pass=True)
