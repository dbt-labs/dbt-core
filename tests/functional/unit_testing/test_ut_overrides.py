import pytest

from dbt.tests.util import run_dbt

my_model_with_macros = """
SELECT
{{ current_timestamp() }} as global_current_timestamp,
{{ dbt.current_timestamp() }} as dbt_current_timestamp,
{{ dbt.type_int() }} as dbt_type_int,
{{ my_macro() }} as user_defined_my_macro,
{{ dbt_utils.generate_surrogate_key() }} as package_defined_macro
"""

test_my_model_with_macros = """
unit_tests:
  - name: test_macro_overrides
    model: my_model_with_macros
    overrides:
      macros:
        current_timestamp: "'current_timestamp_override'"
        dbt.type_int: "'dbt_macro_override'"
        my_macro: "'global_user_defined_macro_override'"
        dbt_utils.generate_surrogate_key: "'package_macro_override'"
    given: []
    expect:
      rows:
        - global_current_timestamp: "current_timestamp_override"
          dbt_current_timestamp: "current_timestamp_override"
          dbt_type_int: "dbt_macro_override"
          user_defined_my_macro: "global_user_defined_macro_override"
          package_defined_macro: "package_macro_override"
"""

MY_MACRO_SQL = """
{% macro my_macro() -%}
  {{ test }}
{%- endmacro %}
"""


class TestUnitTestingMacroOverrides:
    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "package": "dbt-labs/dbt_utils",
                    "version": "1.1.1",
                },
            ]
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_with_macros.sql": my_model_with_macros,
            "test_my_model_with_macros.yml": test_my_model_with_macros,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"my_macro.sql": MY_MACRO_SQL}

    def test_macro_overrides(self, project):
        run_dbt(["deps"])

        # Select by model name
        results = run_dbt(["test", "--select", "my_model_with_macros"], expect_pass=True)
        assert len(results) == 1
