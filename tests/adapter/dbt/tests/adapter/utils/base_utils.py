import pytest
from dbt.tests.util import run_dbt

macros__equals_sql = """
{% macro equals(actual, expected) %}
{# -- actual is not distinct from expected #}
(({{ actual }} = {{ expected }}) or ({{ actual }} is null and {{ expected }} is null))
{% endmacro %}
"""

macros__test_assert_equal_sql = """
{% test assert_equal(model, actual, expected) %}
select * from {{ model }}
where not {{ equals(actual, expected) }}
{% endtest %}
"""


class BaseUtils:
    # setup
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "equals.sql": macros__equals_sql,
            "test_assert_equal.sql": macros__test_assert_equal_sql,
        }

    # make it possible to dynamically update the macro call with a namespace
    # (e.g.) 'dateadd', 'dbt.dateadd', 'dbt_utils.dateadd'
    def macro_namespace(self):
        return ""

    def interpolate_macro_namespace(self, model_sql, macro_name):
        macro_namespace = self.macro_namespace()
        return (
            model_sql.replace(f"{macro_name}(", f"{macro_namespace}.{macro_name}(")
            if macro_namespace
            else model_sql
        )

    # actual test sequence
    def test_build_assert_equal(self, project):
        run_dbt(["build"])  # seed, model, test
