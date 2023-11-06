import pytest
from dbt.tests.util import run_dbt_and_capture


my_numeric_model_sql = """
select
  12.34 as price
"""

my_money_model_sql = """
select
  cast('12.34' as money) as price
"""

model_schema_money_yml = """
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: price
        data_type: money
"""

model_schema_numeric_yml = """
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: price
        data_type: numeric
"""


class TestModelContractUnrecognizedTypeCode1:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_money_model_sql,
            "schema.yml": model_schema_money_yml,
        }

    def test_nonstandard_data_type(self, project):
        expected_debug_msg = "The `type_code` 790 was not recognized"
        _, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert expected_debug_msg in logs


class TestModelContractUnrecognizedTypeCodeActualMismatch:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_money_model_sql,
            "schema.yml": model_schema_numeric_yml,
        }

    def test_nonstandard_data_type(self, project):
        expected_msg = "unknown type_code 790 | DECIMAL       | data type mismatch"
        expected_debug_msg = "The `type_code` 790 was not recognized"
        _, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=False)
        assert expected_msg in logs
        assert expected_debug_msg in logs


class TestModelContractUnrecognizedTypeCodeExpectedMismatch:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_numeric_model_sql,
            "schema.yml": model_schema_money_yml,
        }

    def test_nonstandard_data_type(self, project):
        expected_msg = "DECIMAL         | unknown type_code 790 | data type mismatch"
        expected_debug_msg = "The `type_code` 790 was not recognized"
        _, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=False)
        assert expected_msg in logs
        assert expected_debug_msg in logs
