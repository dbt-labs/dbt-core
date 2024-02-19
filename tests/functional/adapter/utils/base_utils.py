import pytest
from dbt.tests.util import run_dbt

macros__equals_sql = """
{% macro equals(expr1, expr2) -%}
case when (({{ expr1 }} = {{ expr2 }}) or ({{ expr1 }} is null and {{ expr2 }} is null))
    then 0
    else 1
end = 0
{% endmacro %}
"""

macros__test_assert_equal_sql = """
{% test assert_equal(model, actual, expected) %}
select * from {{ model }}
where not {{ equals(actual, expected) }}
{% endtest %}
"""

macros__replace_empty_sql = """
{% macro replace_empty(expr) -%}
case
    when {{ expr }} = 'EMPTY' then ''
    else {{ expr }}
end
{% endmacro %}
"""


class BaseUtils:
    # setup
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "equals.sql": macros__equals_sql,
            "test_assert_equal.sql": macros__test_assert_equal_sql,
            "replace_empty.sql": macros__replace_empty_sql,
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
