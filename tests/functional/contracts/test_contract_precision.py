import pytest
from dbt.tests.util import run_dbt_and_capture


my_numeric_model_sql = """
select
  1.234 as non_integer
"""

model_schema_numerics_yml = """
version: 2
models:
  - name: my_numeric_model
    config:
      contract:
        enforced: true
    columns:
      - name: non_integer
        data_type: numeric
"""

model_schema_numerics_precision_yml = """
version: 2
models:
  - name: my_numeric_model
    config:
      contract:
        enforced: true
    columns:
      - name: non_integer
        data_type: numeric(38,3)
"""


class TestModelContractNumericNoPrecision:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_numeric_model.sql": my_numeric_model_sql,
            "schema.yml": model_schema_numerics_yml,
        }

    def test_contracted_numeric_without_precision(self, project):
        expected_msg = "Detected columns with numeric type and unspecified precision/scale, this can lead to unintended rounding: ['non_integer']"
        _, logs = run_dbt_and_capture(["run"], expect_pass=True)
        assert expected_msg in logs
        _, logs = run_dbt_and_capture(["--warn-error", "run"], expect_pass=False)
        assert "Compilation Error in model my_numeric_model" in logs
        assert expected_msg in logs


class TestModelContractNumericPrecision:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_numeric_model.sql": my_numeric_model_sql,
            "schema.yml": model_schema_numerics_precision_yml,
        }

    def test_contracted_numeric_with_precision(self, project):
        expected_msg = "Detected columns with numeric type and unspecified precision/scale, this can lead to unintended rounding: ['non_integer']"
        _, logs = run_dbt_and_capture(["run"], expect_pass=True)
        assert expected_msg not in logs
