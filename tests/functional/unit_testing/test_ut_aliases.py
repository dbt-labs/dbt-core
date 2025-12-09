import pytest

from dbt.tests.util import run_dbt

model_with_alias_sql = """
{{
    config(
        alias='beautiful_alias',
        schema='events',
        materialized='view'
    )
}}

select
    'foo' as foo
"""

model_tested = """
select * from {{ ref('model_with_alias') }}
"""

unit_test_yml = """
unit_tests:
  - name: test_model_with_alias_input
    model: model_tested
    given:
      - input: ref('model_with_alias')
        rows:
          - {foo: bar }
          - {foo: foo }
    expect:
      rows:
          - {foo: bar }
          - {foo: foo }
"""


class TestUnitTestInputWithAlias:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_with_alias.sql": model_with_alias_sql,
            "model_tested.sql": model_tested,
            "unit_test.yml": unit_test_yml,
        }

    def test_input_with_alias(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2

        results = run_dbt(["test"])
        assert len(results) == 1
