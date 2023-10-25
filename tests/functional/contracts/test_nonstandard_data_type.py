import pytest
from dbt.tests.util import run_dbt


my_money_model_sql = """
select
  cast('12.34' as money) as non_standard
"""

model_schema_money_yml = """
version: 2
models:
  - name: my_numeric_model
    config:
      contract:
        enforced: true
    columns:
      - name: non_standard
        data_type: money
"""


class TestModelContractNumericNoPrecision:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_numeric_model.sql": my_money_model_sql,
            "schema.yml": model_schema_money_yml,
        }

    def test_nonstandard_data_type(self, project):
        run_dbt(["run"], expect_pass=True)
