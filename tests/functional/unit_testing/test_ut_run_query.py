import pytest

from dbt.tests.util import run_dbt

base_table_sql = """
select 1 as id
"""

model_with_run_query_sql = """
{% set query %}
    SELECT max(id)
    from {{ ref('base_table') }}
{% endset %}
{% set max_date = run_query(query) %}

select *
from {{ ref('base_table') }}
"""

unit_test_with_run_query_yml = """
unit_tests:
  - name: test_run_query_model
    model: run_query_model
    overrides:
      macros:
        run_query: 1
    given:
      - input: ref('base_table')
        rows:
          - {id: 1}
    expect:
      rows:
        - {id: 1}
"""


class TestUnitTestWithRunQuery:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "run_query_model.sql": model_with_run_query_sql,
            "base_table.sql": base_table_sql,
            "test_my_model.yml": unit_test_with_run_query_yml,
        }

    def test_unit_test(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2

        results = run_dbt(["test", "--select", "run_query_model"])
