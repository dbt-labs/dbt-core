import pytest

from dbt.contracts.results import NodeStatus
from dbt.tests.util import run_dbt

macros_sql = """
{% macro test_array_results() %}

    {% set sql %}
        select ARRAY[1, 2, 3, 4] as mydata
    {% endset %}

    {% set result = run_query(sql) %}
    {% set value = result.columns['mydata'][0] %}

    {# This will be json-stringified #}
    {% if value != "[1, 2, 3, 4]" %}
        {% do exceptions.raise_compiler_error("Value was " ~ value) %}
    {% endif %}

{% endmacro %}
"""


class TestTypes:
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "macros.sql": macros_sql,
        }

    def test_nested_types(self, project):
        result = run_dbt(["run-operation", "test_array_results"])
        assert result.results[0].status == NodeStatus.Success
