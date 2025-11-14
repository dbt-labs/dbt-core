import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture

dbt_project_yml = """
vars:
  columns_list_one:
    - column_a
    - column_b

  columns_list_two:
    - column_c
"""


my_model_one_variable_sql = """
{{ config(materialized='table') }}
-- {{ get_columns(include=var('columns_list_one'))}}
select 1 as id
"""


my_model_two_variables_sql = """
{{ config(materialized='table') }}
-- {{ get_columns(include=var('columns_list_one') + var('columns_list_two'))}}
select 1 as id
"""


my_macro_sql = """
{%- macro get_columns(include=[]) -%}
    {%- for col in include -%}
        {{ col }}{% if not loop.last %}, {% endif %}
    {%- endfor -%}
{%- endmacro -%}
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


class TestUnitTestOneVariables:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return dbt_project_yml

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_one_variable_sql,
            "my_unit_test.yml": my_unit_test_yml,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"my_macros.sql": my_macro_sql}

    def test_one_variable_as_input_to_macro(self, project):
        run_dbt_and_capture(["test"], expect_pass=True)


class TestUnitTestTwoVariables:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return dbt_project_yml

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_two_variables_sql,
            "my_unit_test.yml": my_unit_test_yml,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"my_macros.sql": my_macro_sql}

    def test_two_variables_as_input_to_macro(self, project):
        # Verify model works fine outside of unit testing
        results = run_dbt(["run"])
        assert len(results) == 1

        run_dbt(["test"])
